# startup/db.py  (v12)
"""
Async Postgres pool + shared DB helpers.

v12 additions: run_prune_query (was imported by db_pruner but missing -> ImportError),
insert_guardian_action, ALLOWED_PRUNE whitelist (no string-built SQL from caller input).
"""
import json
import logging
import os
from typing import Any, Dict, List, Optional

import asyncpg

logger = logging.getLogger("startup.db")

_pool: Optional[asyncpg.Pool] = None

ALLOWED_PRUNE = {
    ("account_snapshots", "ts"), ("tick_telemetry", "ts"),
    ("guardian_actions", "created_at"), ("market_context_snapshots", "ts"),
}


async def init_db_pool() -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"], min_size=2, max_size=20, command_timeout=15,
        max_inactive_connection_lifetime=300.0, statement_cache_size=0,
    )
    logger.info("Database connection pool initialized.")
    return _pool


async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised - init_db_pool() must run before get_pool().")
    return _pool


async def run_prune_query(table: str, ts_column: str, days: int) -> int:
    """Delete rows older than `days`. Table/column must be whitelisted; days is a bound parameter."""
    if (table, ts_column) not in ALLOWED_PRUNE:
        raise ValueError(f"prune not permitted for {table}.{ts_column}")
    days = int(days)
    pool = get_pool()
    async with pool.acquire() as conn:
        status = await conn.execute(
            f"DELETE FROM {table} WHERE {ts_column} < NOW() - make_interval(days => $1)", days
        )
    try:
        return int(status.split(" ")[-1])
    except Exception:
        return 0


async def fetch_strategy_params(asset: str) -> Dict[str, Any]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM strategy_db WHERE asset = $1", asset)
    return dict(row) if row else {}


async def fetch_missed_trade_analytics_rows(limit: int = 1000) -> List[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                "SELECT * FROM missed_trade_analytics ORDER BY COALESCE(created_at, analyzed_at) DESC LIMIT $1", limit)
        except Exception:
            rows = await conn.fetch("SELECT * FROM missed_trade_signals ORDER BY id DESC LIMIT $1", limit)
    return [dict(r) for r in rows]


async def update_strategy_lifeline(payload: Dict[str, Any]) -> bool:
    pool = get_pool()
    asset = payload.get("strategy_id") or payload.get("asset")
    if not asset:
        return False
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO strategy_lifeline (asset, lifeline_score, terminated, updated_at)
            VALUES ($1, COALESCE($2, 100.0), $3, COALESCE($4, NOW()))
            ON CONFLICT (asset) DO UPDATE
            SET lifeline_score = COALESCE(EXCLUDED.lifeline_score, strategy_lifeline.lifeline_score),
                terminated = EXCLUDED.terminated, updated_at = NOW()
            """,
            asset, payload.get("lifeline_score"), payload.get("lifeline_terminated", False), payload.get("closed_at"),
        )
    return True


async def insert_missed_trade_signal(row_or_magic: Any = None, symbol: Optional[str] = None, side: Optional[str] = None,
                                     reason: Optional[str] = None, rsi: Optional[float] = None, atr: Optional[float] = None,
                                     threshold: Optional[float] = None, simulated_pnl_r: Optional[float] = None,
                                     outcome: Optional[str] = None, **kwargs) -> int:
    pool = get_pool()
    if isinstance(row_or_magic, dict):
        r = row_or_magic
        magic_number = r.get("magic_number", 0)
        symbol = r.get("symbol") or r.get("asset", "")
        side, reason = r.get("side", ""), r.get("reason", "")
        rsi, atr, threshold = r.get("rsi"), r.get("atr"), r.get("threshold")
        simulated_pnl_r, outcome = r.get("simulated_pnl_r"), r.get("outcome")
    else:
        magic_number = int(row_or_magic) if row_or_magic is not None else 0
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """INSERT INTO missed_trade_signals (magic_number, asset, side, reason, rsi, atr, threshold,
                                                 simulated_pnl_r, outcome, evaluated)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,false) RETURNING id""",
            magic_number, symbol, side, reason, rsi, atr, threshold, simulated_pnl_r, outcome,
        )


async def fetch_account_snapshots_summary(recent_minutes: int = 60) -> List[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT account_type AS account_id, balance, equity, day_start_balance,
                   CASE WHEN day_start_balance > 0 THEN ((day_start_balance - equity) / day_start_balance) * 100.0
                        ELSE COALESCE(drawdown_pct, 0.0) END AS drawdown_pct,
                   ts AS snapshot_time
            FROM account_snapshots
            WHERE ts >= NOW() - make_interval(mins => $1)
            ORDER BY ts DESC
            """, int(recent_minutes))
    return [dict(r) for r in rows]


async def get_account_control(account_type: str) -> Dict[str, Any]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at "
            "FROM account_controls WHERE account_type = $1", account_type)
    if not row:
        return {"account_type": account_type, "manual_drawdown_halt": False, "halt_reason": None,
                "halted_by": None, "updated_at": None}
    return dict(row)


async def list_account_controls() -> List[Dict[str, Any]]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at FROM account_controls")
    return [dict(r) for r in rows]


async def set_account_control(account_type: str, halt: bool, reason: Optional[str] = None,
                              actor: Optional[str] = "dashboard_operator") -> Dict[str, Any]:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO account_controls (account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at)
            VALUES ($1,$2,$3,$4,NOW())
            ON CONFLICT (account_type) DO UPDATE
            SET manual_drawdown_halt = EXCLUDED.manual_drawdown_halt, halt_reason = EXCLUDED.halt_reason,
                halted_by = EXCLUDED.halted_by, updated_at = NOW()
            RETURNING account_type, manual_drawdown_halt, halt_reason, halted_by, updated_at
            """, account_type, halt, reason, actor)
    return dict(row)


async def insert_guardian_action(asset: str, report: Dict[str, Any], decision: Dict[str, Any]) -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO guardian_actions
               (asset, ticket, side, action, reason, r_multiple, peak_r, slope_r_min, against_votes, market_bias, details)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)""",
            asset, report.get("ticket"), report.get("side"), decision["action"], decision["reason"],
            decision.get("r_multiple"), decision.get("peak_r"), decision.get("slope_r_min"),
            decision.get("against_votes"), decision.get("market_bias"), json.dumps({"report": report, "decision": decision}, default=str),
        )
