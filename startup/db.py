# startup/db.py
"""
Async Postgres connection pool & database access helpers.

Provides central asyncpg connection management and helper functions for jobs
including gatekeeper autotuning, lifeline engine, trade analytics telemetry,
and account drawdown overrides.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union
import asyncpg

logger = logging.getLogger("startup.db")

_pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> asyncpg.Pool:
    """Initialises the global asyncpg connection pool."""
    global _pool
    database_url = os.environ["DATABASE_URL"]
    _pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=2,
        max_size=20,
        command_timeout=15,
        max_inactive_connection_lifetime=300.0,
        statement_cache_size=0,
    )
    logger.info("Database connection pool initialized.")
    return _pool


async def close_db_pool() -> None:
    """Closes active database pool connection."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")


def get_pool() -> asyncpg.Pool:
    """Returns the active asyncpg database pool."""
    if _pool is None:
        raise RuntimeError(
            "DB pool not initialised - init_db_pool() must run before get_pool()."
        )
    return _pool


async def fetch_strategy_params(asset: str) -> Dict[str, Any]:
    """Fetches strategy parameters for an asset from strategy_db."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM strategy_db WHERE asset = $1", asset
        )
        if not row:
            return {}
        return dict(row)


async def fetch_missed_trade_analytics_rows(limit: int = 1000) -> List[Dict[str, Any]]:
    """Queries recent missed trade analytics or signals for parameter tuning."""
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT * FROM missed_trade_analytics
                ORDER BY COALESCE(created_at, analyzed_at) DESC
                LIMIT $1
                """,
                limit,
            )
        except Exception:
            rows = await conn.fetch(
                """
                SELECT * FROM missed_trade_signals
                ORDER BY id DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]


async def update_strategy_lifeline(payload: Dict[str, Any]) -> bool:
    """Upserts strategy lifeline score and termination status into strategy_lifeline."""
    pool = get_pool()
    asset = payload.get("strategy_id") or payload.get("asset")
    if not asset:
        return False

    score = payload.get("lifeline_score")
    terminated = payload.get("lifeline_terminated", False)
    closed_at = payload.get("closed_at")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO strategy_lifeline (asset, lifeline_score, terminated, updated_at)
            VALUES ($1, COALESCE($2, 100.0), $3, COALESCE($4, NOW()))
            ON CONFLICT (asset) DO UPDATE
            SET lifeline_score = COALESCE(EXCLUDED.lifeline_score, strategy_lifeline.lifeline_score),
                terminated = EXCLUDED.terminated,
                updated_at = NOW()
            """,
            asset, score, terminated, closed_at,
        )
    return True


async def insert_missed_trade_signal(
    row_or_magic: Any = None,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    reason: Optional[str] = None,
    rsi: Optional[float] = None,
    atr: Optional[float] = None,
    threshold: Optional[float] = None,
    simulated_pnl_r: Optional[float] = None,
    outcome: Optional[str] = None,
    **kwargs,
) -> int:
    """
    Inserts a missed trade signal into missed_trade_signals.
    Supports dictionary input payloads or direct keyword parameters.
    """
    pool = get_pool()

    if isinstance(row_or_magic, dict):
        row = row_or_magic
        magic_number = row.get("magic_number", 0)
        symbol = row.get("symbol") or row.get("asset", "")
        side = row.get("side", "")
        reason = row.get("reason", "")
        rsi = row.get("rsi")
        atr = row.get("atr")
        threshold = row.get("threshold")
        simulated_pnl_r = row.get("simulated_pnl_r")
        outcome = row.get("outcome")
    else:
        magic_number = int(row_or_magic) if row_or_magic is not None else 0

    query = """
        INSERT INTO missed_trade_signals (
            magic_number, symbol, side, reason, rsi, atr, threshold,
            simulated_pnl_r, outcome, evaluated
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false)
        RETURNING id;
    """
    async with pool.acquire() as conn:
        return await conn.fetchval(
            query,
            magic_number,
            symbol,
            side,
            reason,
            rsi,
            atr,
            threshold,
            simulated_pnl_r,
            outcome,
        )


async def fetch_account_snapshots_summary(recent_minutes: int = 60) -> List[Dict[str, Any]]:
    """
    Retrieves recent account snapshots using timestamptz columns ('ts' / 'created_at')
    and calculates current drawdown percentages.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        query = """
            SELECT 
                account_type AS account_id,
                balance,
                equity,
                day_start_balance,
                CASE 
                    WHEN day_start_balance > 0 THEN ((day_start_balance - equity) / day_start_balance) * 100.0
                    ELSE 0.0
                END AS drawdown_pct,
                COALESCE(ts, created_at) AS snapshot_time
            FROM account_snapshots
            WHERE COALESCE(ts, created_at) >= NOW() - ($1 || ' minutes')::INTERVAL
            ORDER BY COALESCE(ts, created_at) DESC
        """
        try:
            rows = await conn.fetch(query, recent_minutes)
        except Exception:
            fallback_query = """
                SELECT 
                    account_type AS account_id,
                    balance,
                    equity,
                    day_start_balance,
                    CASE 
                        WHEN day_start_balance > 0 THEN ((day_start_balance - equity) / day_start_balance) * 100.0
                        ELSE 0.0
                    END AS drawdown_pct,
                    created_at AS snapshot_time
                FROM account_snapshots
                WHERE created_at >= NOW() - ($1 || ' minutes')::INTERVAL
                ORDER BY created_at DESC
            """
            rows = await conn.fetch(fallback_query, recent_minutes)
        return [dict(r) for r in rows]


async def get_account_control(account_type: str) -> Dict[str, Any]:
    """
    Fetches the manual drawdown-halt override for one account_type.
    Returns a safe default (not halted) if no row exists yet, so callers
    never need a None-check before reading ["manual_drawdown_halt"].
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at
            FROM account_controls
            WHERE account_type = $1
            """,
            account_type,
        )
    if not row:
        return {
            "account_type": account_type,
            "manual_drawdown_halt": False,
            "halt_reason": None,
            "halted_by": None,
            "updated_at": None,
        }
    return dict(row)


async def list_account_controls() -> List[Dict[str, Any]]:
    """Fetches manual drawdown-halt overrides for every known account_type."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at
            FROM account_controls
            """
        )
    return [dict(r) for r in rows]


async def set_account_control(
    account_type: str,
    halt: bool,
    reason: Optional[str] = None,
    actor: Optional[str] = "dashboard_operator",
) -> Dict[str, Any]:
    """
    Upserts the manual drawdown-halt flag for an account_type. This is the
    single write path used by both:
      - POST /control/drawdown_halt (dashboard Start/Stop button)
      - any future automated caller (e.g. a future server-side equity guard)
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO account_controls (account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (account_type) DO UPDATE
            SET manual_drawdown_halt = EXCLUDED.manual_drawdown_halt,
                halt_reason = EXCLUDED.halt_reason,
                halted_by = EXCLUDED.halted_by,
                updated_at = NOW()
            RETURNING account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at
            """,
            account_type,
            halt,
            reason,
            actor,
        )
    return dict(row)