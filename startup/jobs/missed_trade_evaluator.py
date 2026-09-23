# startup/jobs/missed_trade_evaluator.py
"""
Missed-Trade Evaluator & Counterfactual Generator.

Combines live counterfactual trade evaluation using tick telemetry with
exportable labeled missed-trade counterfactual dataset generation for ML training
and automated strategy tuning via missed_trade_analytics.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from startup.db import (
    fetch_missed_trade_analytics_rows,
    get_pool,
    insert_missed_trade_signal,
)

log = logging.getLogger("missed_trade_evaluator")

# --- Tunables -----------------------------------------------------------
MAX_HOLD_MINUTES = 60          # must match the EA's InpMaxHoldMinutes default
MIN_AGE_MINUTES = MAX_HOLD_MINUTES + 5   # only evaluate signals old enough to resolve
MAX_BACKLOG_HOURS = 48          # don't evaluate signals older than this
EVAL_BATCH_SIZE = 500

MIN_RSI_VETO_SAMPLE = 8         # minimum evaluated rsi_veto rows before trusting a suggestion
RSI_VETO_WIN_RATE_LOOSEN_TRIGGER = 55.0   # loosen if missed rsi_veto trades won >= 55%
RSI_VETO_STEP = 2.0             # degrees to relax rsi_buy_max / rsi_sell_min per cycle
RSI_BUY_MAX_BOUNDS = (60.0, 85.0)
RSI_SELL_MIN_BOUNDS = (15.0, 40.0)


# -----------------------
# ML Training & Dataset Export Helper
# -----------------------
def evaluate_and_label_missed_trades(limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Exports labeled missed-trade counterfactuals for ML training and offline analytics.
    Ensures asset column normalization and persists signals via insert_missed_trade_signal.
    """
    rows: List[Dict[str, Any]] = []
    try:
        analytics_rows = fetch_missed_trade_analytics_rows(limit=limit)
        if analytics_rows:
            rows = [dict(r) if hasattr(r, "_asdict") or isinstance(r, dict) else dict(r) for r in analytics_rows]
    except Exception as exc:
        log.warning("Could not fetch existing analytics rows, falling back to empty list: %s", exc)
        rows = []

    for r in rows:
        # ensure asset column exists
        r["asset"] = r.get("asset") or r.get("symbol")
        try:
            insert_missed_trade_signal(r)
        except Exception as exc:
            log.exception("Error inserting missed trade signal for asset %s: %s", r.get("asset"), exc)

    log.info("Labeled %d missed trades", len(rows))
    return rows


# -----------------------
# Counterfactual Evaluation Engine
# -----------------------
async def _fetch_pending_signals(conn):
    lower_bound = datetime.now(timezone.utc) - timedelta(hours=MAX_BACKLOG_HOURS)
    upper_bound = datetime.now(timezone.utc) - timedelta(minutes=MIN_AGE_MINUTES)
    return await conn.fetch(
        """
        SELECT id, asset, side, price, atr, sl_distance, tp_distance, created_at
        FROM missed_trade_signals
        WHERE evaluated = false
          AND created_at BETWEEN $1 AND $2
          AND price IS NOT NULL
          AND sl_distance IS NOT NULL AND sl_distance > 0
          AND tp_distance IS NOT NULL AND tp_distance > 0
        ORDER BY created_at ASC
        LIMIT $3
        """,
        lower_bound, upper_bound, EVAL_BATCH_SIZE,
    )


async def _fetch_forward_ticks(conn, asset: str, since: datetime, until: datetime):
    return await conn.fetch(
        """
        SELECT ts, bid, ask
        FROM tick_telemetry
        WHERE asset = $1 AND ts > $2 AND ts <= $3
        ORDER BY ts ASC
        """,
        asset, since, until,
    )


def _simulate_outcome(side: str, entry_price: float, sl_distance: float,
                       tp_distance: float, forward_ticks: list) -> tuple:
    """
    Walk forward_ticks (chronological) and return (outcome, simulated_pnl_r).

    A BUY exits at the tick's bid; a SELL exits at the tick's ask.
    Whichever level (TP or SL) is crossed first in time wins;
    if neither is hit before the window/data runs out, the trade is marked
    'inconclusive' with a partial R computed off the last available price.
    """
    if not forward_ticks:
        return "inconclusive", 0.0

    if side == "BUY":
        tp_price = entry_price + tp_distance
        sl_price = entry_price - sl_distance
        for row in forward_ticks:
            px = float(row["bid"]) if row["bid"] is not None else None
            if px is None:
                continue
            if px >= tp_price:
                return "would_have_won", round(tp_distance / sl_distance, 3)
            if px <= sl_price:
                return "would_have_lost", -1.0
        last_px = next((float(r["bid"]) for r in reversed(forward_ticks) if r["bid"] is not None), None)
        if last_px is None:
            return "inconclusive", 0.0
        partial_r = (last_px - entry_price) / sl_distance
        return "inconclusive", round(max(-1.0, min(tp_distance / sl_distance, partial_r)), 3)

    else:  # SELL
        tp_price = entry_price - tp_distance
        sl_price = entry_price + sl_distance
        for row in forward_ticks:
            px = float(row["ask"]) if row["ask"] is not None else None
            if px is None:
                continue
            if px <= tp_price:
                return "would_have_won", round(tp_distance / sl_distance, 3)
            if px >= sl_price:
                return "would_have_lost", -1.0
        last_px = next((float(r["ask"]) for r in reversed(forward_ticks) if r["ask"] is not None), None)
        if last_px is None:
            return "inconclusive", 0.0
        partial_r = (entry_price - last_px) / sl_distance
        return "inconclusive", round(max(-1.0, min(tp_distance / sl_distance, partial_r)), 3)


async def _evaluate_pending_signals() -> int:
    pool = get_pool()
    evaluated_count = 0

    async with pool.acquire() as conn:
        pending = await _fetch_pending_signals(conn)

    for row in pending:
        try:
            since = row["created_at"]
            until = since + timedelta(minutes=MAX_HOLD_MINUTES)

            async with pool.acquire() as conn:
                forward_ticks = await _fetch_forward_ticks(conn, row["asset"], since, until)

            outcome, simulated_r = _simulate_outcome(
                row["side"], float(row["price"]), float(row["sl_distance"]),
                float(row["tp_distance"]), [dict(r) for r in forward_ticks],
            )

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE missed_trade_signals
                    SET evaluated = true, evaluated_at = NOW(),
                        outcome = $2, simulated_pnl_r = $3
                    WHERE id = $1
                    """,
                    row["id"], outcome, simulated_r,
                )
            evaluated_count += 1

        except Exception:
            log.exception("Failed to evaluate missed_trade_signals.id=%s (asset=%s)", row["id"], row["asset"])
            continue

    if evaluated_count:
        log.info("Missed-trade evaluator: labeled %d pending signal(s).", evaluated_count)
    return evaluated_count


# -----------------------
# Auto-Tuning Strategy Suggestions
# -----------------------
async def _fetch_rsi_veto_stats(conn, since: datetime):
    return await conn.fetch(
        """
        SELECT asset,
               COUNT(*) AS sample_size,
               COUNT(*) FILTER (WHERE outcome = 'would_have_won') AS wins,
               AVG(simulated_pnl_r) AS avg_r
        FROM missed_trade_signals
        WHERE reason = 'rsi_veto'
          AND evaluated = true
          AND created_at >= $1
        GROUP BY asset
        """,
        since,
    )


async def _write_rsi_veto_suggestions_from_evaluated_data() -> int:
    """
    Aggregates evaluated rsi_veto rows per asset. If the RSI exhaustion
    veto has been blocking signals that would mostly have won, suggest
    relaxing rsi_buy_max/rsi_sell_min by writing into the EXISTING
    missed_trade_analytics table.
    """
    pool = get_pool()
    since = datetime.now(timezone.utc) - timedelta(days=3)
    written = 0

    async with pool.acquire() as conn:
        stats = await _fetch_rsi_veto_stats(conn, since)

        for row in stats:
            asset = row["asset"]
            sample_size = int(row["sample_size"] or 0)
            wins = int(row["wins"] or 0)
            avg_r = float(row["avg_r"]) if row["avg_r"] is not None else 0.0

            if sample_size < MIN_RSI_VETO_SAMPLE:
                continue

            win_rate = (wins / sample_size) * 100.0
            if win_rate < RSI_VETO_WIN_RATE_LOOSEN_TRIGGER:
                continue

            current = await conn.fetchrow(
                "SELECT rsi_buy_max, rsi_sell_min FROM strategy_db WHERE asset = $1", asset
            )
            cur_buy_max = float(current["rsi_buy_max"]) if current and current["rsi_buy_max"] is not None else 70.0
            cur_sell_min = float(current["rsi_sell_min"]) if current and current["rsi_sell_min"] is not None else 30.0

            new_buy_max = max(RSI_BUY_MAX_BOUNDS[0], min(RSI_BUY_MAX_BOUNDS[1], cur_buy_max + RSI_VETO_STEP))
            new_sell_min = max(RSI_SELL_MIN_BOUNDS[0], min(RSI_SELL_MIN_BOUNDS[1], cur_sell_min - RSI_VETO_STEP))

            identified_issue = (
                f"[live_missed_trade_evaluator] {asset}: {sample_size} live rsi_veto-blocked signals "
                f"evaluated over the last 3 days, {win_rate:.1f}% would have won (avg simulated R="
                f"{avg_r:+.2f}). Current veto rsi_buy_max={cur_buy_max:.1f} rsi_sell_min={cur_sell_min:.1f} "
                f"looks too strict for current conditions."
            )

            await conn.execute(
                """
                INSERT INTO missed_trade_analytics (asset, groq_analysis, suggested_logic_tweak, analyzed_at, created_at)
                VALUES ($1, $2, $3, NOW(), NOW())
                """,
                asset,
                identified_issue,
                json.dumps({
                    "tweak": "relax RSI exhaustion veto based on live missed-trade evidence",
                    "suggested_rsi_buy_max": new_buy_max,
                    "suggested_rsi_sell_min": new_sell_min,
                }),
            )
            written += 1
            log.info(
                "Missed-trade evaluator: suggested RSI veto relaxation for %s "
                "(win_rate=%.1f%% sample=%d) -> buy_max=%.1f sell_min=%.1f",
                asset, win_rate, sample_size, new_buy_max, new_sell_min,
            )

    return written


# -----------------------
# Scheduler & API Reporting Entries
# -----------------------
async def run_missed_trade_evaluation_cycle() -> None:
    """Main APScheduler entry point."""
    try:
        await _evaluate_pending_signals()
    except Exception:
        log.exception("Missed-trade evaluator: evaluation pass failed.")

    try:
        await _write_rsi_veto_suggestions_from_evaluated_data()
    except Exception:
        log.exception("Missed-trade evaluator: RSI-veto suggestion pass failed.")


async def get_missed_trade_summary(days: int = 7) -> dict:
    """
    Aggregated trend view for GET /missed_trades/summary: per asset/reason
    counts, how many have been judged so far, the win rate of the
    counterfactual trade, and the average simulated R-multiple.
    """
    pool = get_pool()
    since = datetime.now(timezone.utc) - timedelta(days=days)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                asset,
                reason,
                COUNT(*) AS blocked_count,
                COUNT(*) FILTER (WHERE evaluated) AS evaluated_count,
                COUNT(*) FILTER (WHERE outcome = 'would_have_won') AS would_have_won,
                COUNT(*) FILTER (WHERE outcome = 'would_have_lost') AS would_have_lost,
                AVG(simulated_pnl_r) FILTER (WHERE evaluated) AS avg_simulated_r
            FROM missed_trade_signals
            WHERE created_at >= $1
            GROUP BY asset, reason
            ORDER BY blocked_count DESC
            """,
            since,
        )

    breakdown = []
    total_blocked = 0
    total_would_have_won = 0
    total_evaluated = 0

    for r in rows:
        blocked = int(r["blocked_count"] or 0)
        evaluated = int(r["evaluated_count"] or 0)
        would_win = int(r["would_have_won"] or 0)
        total_blocked += blocked
        total_evaluated += evaluated
        total_would_have_won += would_win

        breakdown.append({
            "asset": r["asset"],
            "reason": r["reason"],
            "blocked_count": blocked,
            "evaluated_count": evaluated,
            "would_have_won": would_win,
            "would_have_lost": int(r["would_have_lost"] or 0),
            "missed_win_rate_pct": round(100.0 * would_win / evaluated, 1) if evaluated > 0 else None,
            "avg_simulated_r": round(float(r["avg_simulated_r"]), 3) if r["avg_simulated_r"] is not None else None,
        })

    return {
        "window_days": days,
        "summary": {
            "total_blocked_signals": total_blocked,
            "total_evaluated": total_evaluated,
            "total_would_have_won": total_would_have_won,
            "overall_missed_win_rate_pct": (
                round(100.0 * total_would_have_won / total_evaluated, 1) if total_evaluated > 0 else None
            ),
        },
        "by_asset_and_reason": breakdown,
    }