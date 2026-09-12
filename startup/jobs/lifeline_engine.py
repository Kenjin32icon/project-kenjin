"""
Strategic Profit Lifeline Engine.

Project Kenjin's Triple Neural Map (ml_tier2.py) already predicts P(loss),
P(profit) and an expected reward per trade. This module is a second,
independent layer on top of that prediction: a *consequence* layer that
treats realised trade outcomes as survival-relevant events rather than
neutral PnL entries.

Design (per-asset "survival economy"):
  - A profitable, correctly-called trade is a strong positive stimulus:
    the asset's lifeline_score rises, and the rise compounds on a winning
    streak (momentum reward).
  - A loss is treated as a threat to the asset's ability to keep trading,
    not a neutral data point: it costs MORE lifeline than an equal-sized
    win grants (LOSS_AVERSION_FACTOR), and it compounds on a losing streak.
  - If lifeline_score is driven to zero, the asset is hard-terminated:
    live_approved is forced false, a risk_incidents row is written, and
    evaluate_tier2_signal() is forced to HOLD/zero-size regardless of what
    the EV model says, until a human operator calls manual_reset().

This module only gates and scales the output of ml_tier2.py — it does not
re-derive win/loss probabilities itself. Wire it in at exactly two points:
  1. main.py's /telemetry route, right after a CLOSE trade is inserted:
         await record_trade_outcome(payload.asset, payload.profit)
  2. main.py's get_strategy_params(), before calling evaluate_tier2_signal():
         lifeline_mult, terminated = await get_lifeline_multiplier(asset)
"""
import logging
from dataclasses import dataclass

from startup.db import get_pool

log = logging.getLogger("lifeline_engine")

# --- Tunable "survival economy" constants -----------------------------
LIFELINE_BASELINE = 100.0          # neutral starting score for every asset
LIFELINE_MAX = 200.0                # hard cap on how far a hot streak can push it
LIFELINE_TERMINATION_FLOOR = 0.0    # score <= this triggers hard termination

# Loss aversion: a loss of magnitude X removes MORE lifeline than a profit
# of magnitude X adds. 2.5 == "losses hurt 2.5x as much as an equal win
# feels good" (prospect-theory-style asymmetry, deliberately punitive).
LOSS_AVERSION_FACTOR = 2.5

# Per-trade stimulus is normalised by a risk unit (an ATR/SL-distance based
# "1R" equivalent) so a $2 move on a micro-lot and a $200 move on a standard
# lot both read as "roughly 1R", keeping the lifeline economy asset-agnostic.
DEFAULT_RISK_UNIT = 10.0  # fallback $ risk-per-trade if none is supplied

# Streak compounding: consecutive wins/losses amplify the stimulus in both
# directions, mirroring momentum in confidence and in caution.
STREAK_BONUS_STEP = 0.10   # +10% per consecutive win/loss
STREAK_BONUS_CAP = 0.50    # capped at +/-50% amplification


@dataclass
class LifelineState:
    asset: str
    lifeline_score: float
    win_streak: int
    loss_streak: int
    terminated: bool


async def _get_or_create_state(conn, asset: str) -> LifelineState:
    row = await conn.fetchrow(
        "SELECT asset, lifeline_score, win_streak, loss_streak, terminated "
        "FROM strategy_lifeline WHERE asset = $1",
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
    """
    Signed lifeline delta for one closed trade.
      profit >= 0 -> pleasure stimulus, amplified by the current win streak.
      profit <  0 -> pain penalty, scaled by LOSS_AVERSION_FACTOR and
                     amplified by the current loss streak.
    """
    risk_unit = risk_unit if risk_unit and risk_unit > 0 else DEFAULT_RISK_UNIT
    r_multiple = profit / risk_unit

    if profit >= 0:
        streak_amp = 1.0 + min(STREAK_BONUS_CAP, win_streak * STREAK_BONUS_STEP)
        return r_multiple * streak_amp
    else:
        streak_amp = 1.0 + min(STREAK_BONUS_CAP, loss_streak * STREAK_BONUS_STEP)
        return r_multiple * LOSS_AVERSION_FACTOR * streak_amp  # already negative


async def record_trade_outcome(asset: str, profit: float, risk_unit: float = DEFAULT_RISK_UNIT) -> LifelineState:
    """
    Call once per CLOSED trade, immediately after the trade_telemetry INSERT
    succeeds. Updates the per-asset lifeline score and, if the floor is
    breached, hard-terminates the asset (live trading revoked).
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        state = await _get_or_create_state(conn, asset)

        if state.terminated:
            log.info("Lifeline: %s is TERMINATED - outcome recorded, no stimulus applied.", asset)
            return state

        delta = _stimulus(profit, risk_unit, state.win_streak, state.loss_streak)
        new_score = max(0.0, min(LIFELINE_MAX, state.lifeline_score + delta))

        if profit >= 0:
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
                f"lifeline_score reached {new_score:.1f} after a loss of {profit:.2f} "
                f"(loss_streak={loss_streak}) - asset auto-terminated, manual re-arm required.",
            )
            log.warning(
                "LIFELINE TERMINATION: %s hit the floor (score=%.1f) - live trading halted, "
                "call lifeline_engine.manual_reset() to re-arm.", asset, new_score,
            )
        else:
            await conn.execute(
                "UPDATE strategy_db SET lifeline_score = $2, updated_at = NOW() WHERE asset = $1",
                asset, new_score,
            )
            verdict = "PLEASURE(+)" if delta >= 0 else "PAIN(-)"
            log.info(
                "Lifeline: %s %s delta=%.2f -> score=%.1f (win_streak=%d, loss_streak=%d)",
                asset, verdict, delta, new_score, win_streak, loss_streak,
            )

        return LifelineState(asset, new_score, win_streak, loss_streak, now_terminated)


async def get_lifeline_multiplier(asset: str) -> tuple:
    """
    Returns (multiplier, terminated) for evaluate_tier2_signal() to use as a
    position-size gate:
      - terminated=True  -> caller must force HOLD / zero size.
      - multiplier scales lot_multiplier: > 1.0 on a hot streak (up to 1.5x),
        < 1.0 once the asset has taken damage (down to 0.2x), well before
        it actually reaches the termination floor.
    """
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
    """Re-arms a terminated asset back to baseline. Never automatic by design."""
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
            "UPDATE strategy_db SET lifeline_terminated = false, lifeline_score = $2, updated_at = NOW() WHERE asset = $1",
            asset, LIFELINE_BASELINE,
        )
        await conn.execute(
            "INSERT INTO risk_incidents (account_type, asset, reason, details, created_at) "
            "VALUES ('live', $1, 'lifeline_manual_reset', $2, NOW())",
            asset, operator_note,
        )
    log.warning("Lifeline: %s manually reset to baseline (%.1f) by operator.", asset, LIFELINE_BASELINE)
