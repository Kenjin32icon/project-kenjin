# startup/jobs/lifeline_engine.py
"""
Strategic Profit Lifeline & Active Profit Engine (v11.08+ Combined Patch)

Integrates:
 - Direct updates to `strategy_db` and `strategy_lifeline` tables.
 - Abstraction helper methods (`update_strategy_lifeline`, `fetch_account_snapshots_summary`, `get_pool`).
 - Flexible `record_trade_outcome` supporting dictionary payloads and parameter-based calls.
 - Multi-tier account drawdown and dynamic profit-locking checks.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any, Dict, Optional, Tuple, Union

import asyncpg

from startup.db import (
    fetch_account_snapshots_summary,
    get_pool,
    update_strategy_lifeline,
)

logger = logging.getLogger("orchestrator.lifeline")
log = logger  # Alias for backward compatibility across modules

# --- Tunable "Survival Economy" Constants -----------------------------
LIFELINE_BASELINE = 100.0          # Neutral starting score for every asset
LIFELINE_MAX = 200.0               # Maximum score cap
LIFELINE_TERMINATION_FLOOR = 0.0   # Score <= 0.0 triggers hard termination

LOSS_AVERSION_FACTOR = 2.5         # Asymmetric loss penalty multiplier
DEFAULT_RISK_UNIT = 10.0           # Fallback baseline risk per trade

STREAK_BONUS_STEP = 0.10           # +10% amplification per consecutive win/loss
STREAK_BONUS_CAP = 0.50            # Maximum +/-50% streak bonus cap


@dataclass
class LifelineState:
    asset: str
    lifeline_score: float
    win_streak: int
    loss_streak: int
    terminated: bool


# ---------------------------------------------------------------------------
# Global Account Equity & Protection Monitoring
# ---------------------------------------------------------------------------
async def run_lifeline_protection_cycle() -> bool:
    """
    Evaluates open account equity and drawdowns using `fetch_account_snapshots_summary`
    and direct pool fallback to apply multi-tier risk mitigation.
    """
    pool = get_pool()
    snaps = await fetch_account_snapshots_summary(recent_minutes=60)

    if snaps:
        for snap in snaps:
            dd = float(snap.get("drawdown_pct", 0.0))
            account_id = snap.get("account_id", "default")
            if dd >= 10.0:
                log.warning("Lifeline emergency triggered for account %s: drawdown=%.2f%%", account_id, dd)
            elif dd >= 4.20:
                log.warning("Lifeline warning for account %s: drawdown=%.2f%%. Contracting exposure.", account_id, dd)

    async with pool.acquire() as conn:
        snapshot = await conn.fetchrow(
            "SELECT * FROM account_snapshots ORDER BY created_at DESC LIMIT 1"
        )
        if not snapshot:
            return True

        equity = float(snapshot["equity"])
        balance = float(snapshot["balance"])
        day_start = float(snapshot.get("day_start_balance", balance))

        drawdown_pct = ((day_start - equity) / day_start) * 100.0 if day_start > 0 else 0.0
        profit_pct = ((equity - balance) / balance) * 100.0 if balance > 0 else 0.0

        # Dynamic Trailing Profit Lock
        if profit_pct >= 1.50:
            logger.info("LIFELINE: Equity target reached (+%.2f%%). Triggering global profit lock.", profit_pct)
            await conn.execute(
                """
                UPDATE strategy_db 
                SET opt_threshold = GREATEST(0.50, opt_threshold - 0.05) 
                WHERE live_approved = TRUE
                """
            )

        # Early Emergency Exposure Reduction
        if drawdown_pct >= 4.20:
            logger.warning("LIFELINE WARNING: High drawdown (%.2f%%). Raising entry thresholds.", drawdown_pct)
            await conn.execute(
                "UPDATE strategy_db SET opt_threshold = 0.85 WHERE live_approved = TRUE"
            )

    return True


# ---------------------------------------------------------------------------
# Per-Asset Survival Economy & Outcome Recording
# ---------------------------------------------------------------------------
async def _get_or_create_state(conn, asset: str) -> LifelineState:
    row = await conn.fetchrow(
        """
        SELECT asset, lifeline_score, win_streak, loss_streak, terminated 
        FROM strategy_lifeline WHERE asset = $1
        """,
        asset,
    )
    if row is None:
        await conn.execute(
            """
            INSERT INTO strategy_lifeline (asset, lifeline_score, win_streak, loss_streak, terminated, updated_at)
            VALUES ($1, $2, 0, 0, false, NOW())
            ON CONFLICT (asset) DO NOTHING
            """,
            asset, LIFELINE_BASELINE,
        )
        return LifelineState(asset, LIFELINE_BASELINE, 0, 0, False)

    return LifelineState(
        asset=row["asset"],
        lifeline_score=float(row["lifeline_score"]),
        win_streak=int(row["win_streak"]),
        loss_streak=int(row["loss_streak"]),
        terminated=bool(row["terminated"]),
    )


def _stimulus(profit: float, risk_unit: float, win_streak: int, loss_streak: int) -> float:
    """Calculates asymmetric lifeline score delta based on profit, risk unit, and streaks."""
    risk_unit = risk_unit if risk_unit and risk_unit > 0 else DEFAULT_RISK_UNIT
    r_multiple = profit / risk_unit

    if profit >= 0:
        streak_amp = 1.0 + min(STREAK_BONUS_CAP, win_streak * STREAK_BONUS_STEP)
        return r_multiple * streak_amp
    else:
        streak_amp = 1.0 + min(STREAK_BONUS_CAP, loss_streak * STREAK_BONUS_STEP)
        return r_multiple * LOSS_AVERSION_FACTOR * streak_amp


async def record_trade_outcome(
    trade_or_asset: Union[Dict[str, Any], str],
    profit: Optional[float] = None,
    risk_unit: float = DEFAULT_RISK_UNIT,
) -> LifelineState:
    """
    Records trade results into `strategy_lifeline` and updates asset survival state.
    Supports either a dictionary payload or direct parameters.
    """
    if isinstance(trade_or_asset, dict):
        trade_record = trade_or_asset
        asset = trade_record.get("asset") or trade_record.get("strategy_id", "UNKNOWN")
        profit_val = float(trade_record.get("profit", profit if profit is not None else 0.0))
        risk_unit_val = float(trade_record.get("risk_unit", risk_unit))

        payload = {
            "strategy_id": asset,
            "lifeline_score": trade_record.get("lifeline_score"),
            "lifeline_terminated": trade_record.get("lifeline_terminated", False),
            "closed_at": trade_record.get("closed_at", datetime.now(timezone.utc)),
        }
        try:
            await update_strategy_lifeline(payload)
        except Exception as e:
            log.warning("Could not execute update_strategy_lifeline helper: %s", e)
    else:
        asset = str(trade_or_asset)
        profit_val = float(profit) if profit is not None else 0.0
        risk_unit_val = float(risk_unit)

    pool = get_pool()
    async with pool.acquire() as conn:
        state = await _get_or_create_state(conn, asset)

        if state.terminated:
            log.info("Lifeline: %s is TERMINATED - outcome logged without applying delta.", asset)
            return state

        delta = _stimulus(profit_val, risk_unit_val, state.win_streak, state.loss_streak)
        new_score = max(0.0, min(LIFELINE_MAX, state.lifeline_score + delta))

        if profit_val >= 0:
            win_streak, loss_streak = state.win_streak + 1, 0
        else:
            win_streak, loss_streak = 0, state.loss_streak + 1

        now_terminated = new_score <= LIFELINE_TERMINATION_FLOOR

        await conn.execute(
            """
            UPDATE strategy_lifeline
            SET lifeline_score = $2, win_streak = $3, loss_streak = $4,
                terminated = $5, last_delta = $6, updated_at = NOW(),
                terminated_at = CASE WHEN $5 AND NOT terminated THEN NOW() ELSE terminated_at END
            WHERE asset = $1
            """,
            asset, new_score, win_streak, loss_streak, now_terminated, delta,
        )

        if now_terminated:
            await conn.execute(
                """
                UPDATE strategy_db
                SET live_approved = false, lifeline_score = $2, lifeline_terminated = true, updated_at = NOW()
                WHERE asset = $1
                """,
                asset, new_score,
            )
            await conn.execute(
                """
                INSERT INTO risk_incidents (account_type, asset, reason, details, created_at)
                VALUES ('live', $1, 'lifeline_termination', $2, NOW())
                """,
                asset,
                f"lifeline_score dropped to {new_score:.1f} (loss={profit_val:.2f}) - auto-terminated.",
            )
            log.warning("LIFELINE TERMINATION: %s hit termination floor (score=%.1f).", asset, new_score)
        else:
            await conn.execute(
                "UPDATE strategy_db SET lifeline_score = $2, updated_at = NOW() WHERE asset = $1",
                asset, new_score,
            )
            log.info("Lifeline outcome recorded for %s: delta=%.2f -> score=%.1f", asset, delta, new_score)

        return LifelineState(asset, new_score, win_streak, loss_streak, now_terminated)


async def get_lifeline_multiplier(asset: str) -> Tuple[float, bool]:
    """Returns position scaling multiplier and termination state for dynamic trade sizing."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT lifeline_score, terminated FROM strategy_lifeline WHERE asset = $1", asset
        )
    if row is None:
        return 1.0, False
    if row["terminated"]:
        return 0.0, True

    multiplier = max(0.2, min(1.5, float(row["lifeline_score"]) / LIFELINE_BASELINE))
    return multiplier, False


async def manual_reset(asset: str, operator_note: str = "Manual operator re-arm") -> None:
    """Restores a terminated asset back to baseline score."""
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE strategy_lifeline
            SET lifeline_score = $2, win_streak = 0, loss_streak = 0,
                terminated = false, terminated_at = NULL, updated_at = NOW()
            WHERE asset = $1
            """,
            asset, LIFELINE_BASELINE,
        )
        await conn.execute(
            """
            UPDATE strategy_db 
            SET lifeline_terminated = false, lifeline_score = $2, updated_at = NOW() 
            WHERE asset = $1
            """,
            asset, LIFELINE_BASELINE,
        )
        await conn.execute(
            """
            INSERT INTO risk_incidents (account_type, asset, reason, details, created_at)
            VALUES ('live', $1, 'lifeline_manual_reset', $2, NOW())
            """,
            asset, operator_note,
        )
    log.warning("Lifeline: %s manually reset to baseline (%.1f) by operator.", asset, LIFELINE_BASELINE)
