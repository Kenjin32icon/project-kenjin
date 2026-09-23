# startup/jobs/gatekeeper.py
"""
Gatekeeper Autotuning & Signal Evaluation Job (v11.08+ Combined Patch)

Integrates:
 - Table reference updates (`strategy_db` primary references).
 - Helper function abstractions from `startup.db`.
 - UTC Midnight daily balance session reset.
 - Complete risk, window, score, and RSI veto execution gates.
 - Simulated and triple-barrier performance autotuning cycle.
 - Dynamic RSI veto tuning from analytics with safety clamping.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import asyncpg

from startup.db import (
    fetch_missed_trade_analytics_rows,
    fetch_strategy_params,
    get_pool,
)

logger = logging.getLogger("orchestrator.gatekeeper")
log = logger  # Alias for backward compatibility across autotuning job functions

# --- Tunable Environment Defaults ------------------------------------
MIN_SAMPLE_SIZE = int(os.environ.get("GATEKEEPER_MIN_SAMPLE_OVERRIDE", 30))
MIN_WIN_RATE = float(os.environ.get("GATEKEEPER_MIN_WIN_RATE", 55.0))
MIN_PROFIT_FACTOR = float(os.environ.get("GATEKEEPER_MIN_PROFIT_FACTOR", 1.3))
LOOKBACK_DAYS = int(os.environ.get("GATEKEEPER_LOOKBACK_DAYS", 7))

ALLOW_SIMULATED = str(os.environ.get("GATEKEEPER_ALLOW_SIMULATED", "true")).lower() in ("1", "true", "yes")
INCLUDE_DEMO_TRADES = str(os.environ.get("GATEKEEPER_INCLUDE_DEMO_TRADES", "false")).lower() in ("1", "true", "yes")
RELAX_MIN_SAMPLE = str(os.environ.get("GATEKEEPER_RELAX_MIN_SAMPLE", "false")).lower() in ("1", "true", "yes")
RELAXED_MIN_SAMPLE = int(os.environ.get("GATEKEEPER_RELAXED_MIN_SAMPLE", "15"))


# ---------------------------------------------------------------------------
# Dynamic Baseline & Gatekeeper Signal Validation
# ---------------------------------------------------------------------------
async def get_or_reset_daily_baseline(pool: asyncpg.Pool, account_type: str, current_balance: float) -> float:
    """Fetches daily baseline balance. Resets at UTC 00:00 to prevent lockout loops."""
    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT day_start_balance, created_at 
            FROM account_snapshots 
            WHERE account_type = $1 
            ORDER BY created_at DESC LIMIT 1
            """,
            account_type,
        )

        if not row or row["created_at"].astimezone(timezone.utc) < today_start:
            logger.info(f"UTC Midnight Reset: Baseline updated for {account_type} to ${current_balance:,.2f}")
            return current_balance

        return float(row["day_start_balance"])


async def evaluate_gatekeeper_signal(
    asset: str,
    account_type: str = "live",
    side: Optional[str] = None,
    score: float = 0.0,
    rsi: Optional[float] = None,
    current_balance: float = 0.0,
    current_equity: float = 0.0,
) -> Dict[str, Any]:
    """
    Validates execution gates against live strategy_db parameters:
    Daily Loss Limit, Asset Live Approval, Hourly Schedule, RSI Veto, and Score Threshold.
    """
    pool = get_pool()

    # 1. Daily Loss Cap Check
    if current_balance > 0 and current_equity > 0:
        day_start_balance = await get_or_reset_daily_baseline(pool, account_type, current_balance)
        day_loss_pct = ((day_start_balance - current_equity) / day_start_balance) * 100.0 if day_start_balance > 0 else 0.0

        HARD_CAP_PCT = 5.00
        if day_loss_pct >= HARD_CAP_PCT:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO risk_incidents (account_type, asset, incident_type, day_loss_pct, cap)
                    VALUES ($1, $2, 'daily_loss_limit', $3, $4)
                    """,
                    account_type, asset, day_loss_pct, HARD_CAP_PCT
                )
            return {"approved": False, "action": "veto", "reason": "daily_loss_limit"}

    # 2. Strategy Parameters Fetching
    params = await fetch_strategy_params(asset)
    if not params:
        log.warning("No strategy params found in strategy_db for asset: %s", asset)
        return {"approved": False, "action": "hold", "reason": "no_params"}

    if not params.get("live_approved", True):
        return {"approved": False, "action": "hold", "reason": "asset_not_approved", "params": params}

    opt_threshold = float(params.get("opt_threshold", 0.60))
    rsi_buy_max = float(params.get("rsi_buy_max", 70.0))
    rsi_sell_min = float(params.get("rsi_sell_min", 30.0))
    start_hr = int(params.get("scheduled_start_hour", 0))
    end_hr = int(params.get("scheduled_end_hour", 23))

    eval_rsi = rsi if rsi is not None else params.get("rsi")

    # 3. Schedule Window Verification
    current_hour = datetime.now(timezone.utc).hour
    if start_hr <= end_hr:
        in_window = start_hr <= current_hour <= end_hr
    else:
        in_window = current_hour >= start_hr or current_hour <= end_hr

    if not in_window:
        return {"approved": False, "action": "hold", "reason": "outside_scheduled_hours", "params": params}

    # 4. RSI Veto Bounds Check
    if eval_rsi is not None:
        if (side == "BUY" or side is None) and eval_rsi >= rsi_buy_max:
            return {"approved": False, "action": "veto_buy", "reason": f"rsi>={rsi_buy_max}", "params": params}
        if (side == "SELL" or side is None) and eval_rsi <= rsi_sell_min:
            return {"approved": False, "action": "veto_sell", "reason": f"rsi<={rsi_sell_min}", "params": params}

    # 5. ML Score Threshold Check
    if score != 0.0 and abs(score) < opt_threshold:
        return {"approved": False, "action": "hold", "reason": "score_below_threshold", "params": params}

    return {"approved": True, "action": "approve", "reason": "APPROVED", "params": params}


# ---------------------------------------------------------------------------
# SQL Helpers for Autotuning Operations
# ---------------------------------------------------------------------------
async def _fetch_tracked_assets(conn) -> list:
    rows = await conn.fetch("SELECT asset FROM strategy_db")
    return [r["asset"] for r in rows]


async def _fetch_recent_closed_trades(conn, asset: str, since: datetime, include_demo: bool = False):
    account_filter = "AND account_type IN ('live', 'demo')" if include_demo else "AND account_type = 'live'"
    query = f"""
        SELECT profit, created_at
        FROM trade_telemetry
        WHERE asset = $1
          AND created_at >= $2
          {account_filter}
          AND profit IS NOT NULL
          AND type LIKE '%CLOSE%'
        ORDER BY created_at ASC
    """
    return await conn.fetch(query, asset, since)


async def _fetch_simulated_counts(conn, asset: str, since: datetime) -> Tuple[int, int, int]:
    query = """
        SELECT
          SUM(CASE WHEN (COALESCE(would_have_won::int, 0) = 1) THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN (COALESCE(would_have_lost::int, 0) = 1) THEN 1 ELSE 0 END) AS losses,
          COUNT(*) AS total
        FROM missed_trade_analytics
        WHERE asset = $1
          AND COALESCE(created_at, analyzed_at) >= $2
    """
    try:
        row = await conn.fetchrow(query, asset, since)
        if not row:
            return 0, 0, 0
        return int(row["wins"] or 0), int(row["losses"] or 0), int(row["total"] or 0)
    except Exception:
        log.debug("Simulated augmentation query failed for %s", asset, exc_info=True)
        return 0, 0, 0


def _compute_stats_from_rows(rows) -> Tuple[float, float, int]:
    sample_size = len(rows)
    if sample_size == 0:
        return 0.0, 0.0, 0

    wins = [r["profit"] for r in rows if r["profit"] is not None and r["profit"] > 0]
    losses = [abs(r["profit"]) for r in rows if r["profit"] is not None and r["profit"] < 0]

    win_rate = (len(wins) / sample_size) * 100.0
    gross_profit = float(sum(wins)) if wins else 0.0
    gross_loss = float(sum(losses)) if losses else 0.0

    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
    return round(win_rate, 1), round(profit_factor, 2), sample_size


def _compute_stats_with_simulation(real_rows, sim_wins: int, sim_losses: int) -> Tuple[float, float, int]:
    real_win_losses = [r for r in real_rows if r["profit"] is not None]
    real_wins = [r["profit"] for r in real_win_losses if r["profit"] > 0]
    real_losses = [abs(r["profit"]) for r in real_win_losses if r["profit"] < 0]

    total_wins = len(real_wins) + sim_wins
    total_losses = len(real_losses) + sim_losses
    total_sample = len(real_rows) + sim_wins + sim_losses

    win_rate = (total_wins / total_sample) * 100.0 if total_sample > 0 else 0.0

    gross_profit_approx = float(sum(real_wins)) + float(sim_wins)
    gross_loss_approx = float(sum(real_losses)) + float(sim_losses)

    profit_factor = (gross_profit_approx / gross_loss_approx) if gross_loss_approx > 0 else (gross_profit_approx if gross_profit_approx > 0 else 0.0)
    return round(win_rate, 1), round(profit_factor, 2), total_sample


async def _log_risk_incident_if_revoked(conn, asset, was_approved, now_approved, win_rate, profit_factor, sample_size):
    if was_approved and not now_approved:
        await conn.execute(
            """
            INSERT INTO risk_incidents (account_type, asset, reason, details, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            """,
            "live",
            asset,
            "gatekeeper_revoked_live_approval",
            f"win_rate={win_rate:.1f}% profit_factor={profit_factor:.2f} sample_size={sample_size}",
        )


async def _fetch_lifeline_terminated_assets(conn) -> set:
    rows = await conn.fetch("SELECT asset FROM strategy_lifeline WHERE terminated = true")
    return {r["asset"] for r in rows}


# ---------------------------------------------------------------------------
# Main Autotuning Execution Cycle
# ---------------------------------------------------------------------------
async def run_gatekeeper_cycle() -> None:
    """Evaluates rolling trade performance and autotunes strategy parameters per asset."""
    pool = get_pool()
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    async with pool.acquire() as conn:
        assets = await _fetch_tracked_assets(conn)
        terminated_assets = await _fetch_lifeline_terminated_assets(conn)

    evaluated = 0
    approved = 0
    lifeline_blocked = 0

    for asset in assets:
        try:
            async with pool.acquire() as conn:
                current_db = await conn.fetchrow(
                    "SELECT live_approved, opt_threshold, opt_sl_mult, opt_tp_mult FROM strategy_db WHERE asset = $1",
                    asset,
                )
                if not current_db:
                    log.debug("Gatekeeper: no strategy_db row for %s, skipping", asset)
                    continue

                current_live_approved = bool(current_db["live_approved"]) if current_db["live_approved"] is not None else False
                current_thresh = float(current_db["opt_threshold"]) if current_db["opt_threshold"] is not None else 0.60
                current_sl = float(current_db["opt_sl_mult"]) if current_db["opt_sl_mult"] is not None else 1.50
                current_tp = float(current_db["opt_tp_mult"]) if current_db["opt_tp_mult"] is not None else 3.00

                rows = await _fetch_recent_closed_trades(conn, asset, since, include_demo=INCLUDE_DEMO_TRADES)
                real_win_rate, real_pf, real_sample = _compute_stats_from_rows(rows)

                sim_wins = sim_losses = sim_total = 0
                augmented_win_rate, augmented_pf, augmented_sample = real_win_rate, real_pf, real_sample
                used_simulation = False

                if ALLOW_SIMULATED and real_sample < MIN_SAMPLE_SIZE:
                    sim_wins, sim_losses, sim_total = await _fetch_simulated_counts(conn, asset, since)
                    if sim_total > 0:
                        used_simulation = True
                        augmented_win_rate, augmented_pf, augmented_sample = _compute_stats_with_simulation(
                            rows, sim_wins, sim_losses
                        )
                        log.info(
                            "Gatekeeper: %s augmented with %d simulated rows. real_sample=%d -> augmented_sample=%d",
                            asset, sim_total, real_sample, augmented_sample,
                        )

                effective_sample = augmented_sample if used_simulation else real_sample
                effective_win_rate = augmented_win_rate if used_simulation else real_win_rate
                effective_pf = augmented_pf if used_simulation else real_pf

                min_sample_for_decision = RELAXED_MIN_SAMPLE if RELAX_MIN_SAMPLE else MIN_SAMPLE_SIZE

                qualifies = (
                    effective_sample >= min_sample_for_decision
                    and effective_win_rate >= MIN_WIN_RATE
                    and effective_pf >= MIN_PROFIT_FACTOR
                )

                if effective_sample < min_sample_for_decision:
                    live_approved = current_live_approved
                else:
                    live_approved = qualifies

                if asset in terminated_assets:
                    if live_approved:
                        log.warning(
                            "Gatekeeper: %s qualifies statistically but is LIFELINE-TERMINATED - forcing live_approved=False.",
                            asset,
                        )
                    live_approved = False
                    lifeline_blocked += 1

                # Dynamic Threshold Tuning
                if effective_sample >= 10:
                    if effective_win_rate < 45.0:
                        current_thresh = min(0.95, current_thresh + 0.03)
                        current_sl = min(2.5, current_sl + 0.1)
                    elif effective_win_rate > 60.0 and effective_pf > 1.4:
                        current_thresh = max(0.30, current_thresh - 0.02)
                        current_tp = min(6.0, current_tp + 0.2)

                await conn.execute(
                    """
                    UPDATE strategy_db
                    SET win_rate = $2, profit_factor = $3, sample_size = $4,
                        live_approved = $5, opt_threshold = $6, opt_sl_mult = $7, opt_tp_mult = $8, updated_at = NOW()
                    WHERE asset = $1
                    """,
                    asset, effective_win_rate, effective_pf, effective_sample, live_approved, current_thresh, current_sl, current_tp,
                )

                await conn.execute(
                    """
                    INSERT INTO autotune_log
                    (asset, win_rate, profit_factor, sample_size, old_threshold, new_threshold, old_sl, new_sl, old_tp, new_tp, notes, ts)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
                    """,
                    asset, effective_win_rate, effective_pf, effective_sample,
                    current_db["opt_threshold"], current_thresh,
                    current_db["opt_sl_mult"], current_sl,
                    current_db["opt_tp_mult"], current_tp,
                    json.dumps({
                        "used_simulation": used_simulation,
                        "sim_wins": sim_wins,
                        "sim_losses": sim_losses,
                        "real_sample": real_sample,
                    }),
                )

                await _log_risk_incident_if_revoked(
                    conn, asset, current_live_approved, live_approved, effective_win_rate, effective_pf, effective_sample
                )

                evaluated += 1
                approved += int(live_approved)

        except Exception:
            log.exception("Gatekeeper: failed to evaluate asset %s, skipping", asset)
            continue

    log.info(
        "Gatekeeper cycle complete: %d/%d assets evaluated, %d approved for live trading, %d blocked by lifeline",
        evaluated, len(assets), approved, lifeline_blocked,
    )

    try:
        await tune_rsi_veto_from_analytics()
    except Exception:
        log.exception("Gatekeeper: tune_rsi_veto_from_analytics failed", exc_info=True)


async def tune_rsi_veto_from_analytics(limit: int = 5000) -> None:
    """Parses missed_trade_analytics via startup.db helper to adjust strategy_db RSI boundaries."""
    pool = get_pool()
    rows = await fetch_missed_trade_analytics_rows(limit=limit)

    if not rows:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (asset) asset, suggested_logic_tweak
                FROM missed_trade_analytics
                WHERE COALESCE(created_at, analyzed_at) >= NOW() - INTERVAL '24 hours'
                ORDER BY asset, COALESCE(created_at, analyzed_at) DESC
                """
            )

    log.info("tune_rsi_veto_from_analytics: loaded %d rows for tuning", len(rows or []))

    async with pool.acquire() as conn:
        for row in rows:
            asset = row.get("asset") if isinstance(row, dict) or hasattr(row, "get") else row["asset"]
            tweak_raw = row.get("suggested_logic_tweak") if isinstance(row, dict) or hasattr(row, "get") else row["suggested_logic_tweak"]

            if not asset or not tweak_raw:
                continue

            try:
                tweak_data = json.loads(tweak_raw) if isinstance(tweak_raw, str) else tweak_raw
                new_rsi_buy_max = float(tweak_data.get("suggested_rsi_buy_max", 70.0))
                new_rsi_sell_min = float(tweak_data.get("suggested_rsi_sell_min", 30.0))

                clamped_rsi_buy_max = max(60.0, min(85.0, new_rsi_buy_max))
                clamped_rsi_sell_min = max(15.0, min(40.0, new_rsi_sell_min))

                await conn.execute(
                    """
                    UPDATE strategy_db
                    SET rsi_buy_max = $2,
                        rsi_sell_min = $3,
                        updated_at = NOW()
                    WHERE asset = $1
                    """,
                    asset, clamped_rsi_buy_max, clamped_rsi_sell_min,
                )
                log.info(
                    "RSI Veto Tuned for %s: BUY Max = %.1f, SELL Min = %.1f",
                    asset, clamped_rsi_buy_max, clamped_rsi_sell_min,
                )
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                log.warning("Could not parse tweak JSON for %s: %s", asset, e)