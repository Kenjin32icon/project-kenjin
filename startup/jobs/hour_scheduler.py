"""
Predictive Session Scheduler.

strategy_db.scheduled_start_hour / scheduled_end_hour have existed in the
schema since early on but nothing ever wrote to them, and nothing ever read
them - the EA only used its static InpSessionStartHour/EndHour inputs.

trade_telemetry.session_hour has been logged on every trade since v10. This
job closes that loop: it finds, per asset, the most profitable contiguous
block of hours over a rolling 30-day window and writes it back to
strategy_db, which /strategy_params now exposes and which MAPSAR v11 now
reads (falling back to its static inputs if no scheduled window is set,
or if the asset doesn't yet have enough trade history to compute one).
"""
import logging
from typing import Optional
from startup.db import get_pool

log = logging.getLogger("hour_scheduler")

MIN_SAMPLE_PER_ASSET = 40      # don't touch scheduling until an asset has this many closed trades total
MIN_WINDOW_HOURS = 6
MAX_WINDOW_HOURS = 16


def _best_window(hourly: dict) -> Optional[tuple]:
    """
    hourly: {hour(0-23): {"count": int, "profit": float}}
    Returns (start_hour, end_hour) with end exclusive, or None if nothing
    qualifies. Non-wrapping window only (kept simple/predictable to reason
    about and to match the EA's own non-wrapping IsWithinTradingSession()
    comparison when start <= end).
    """
    profits = [hourly.get(h, {}).get("profit", 0.0) for h in range(24)]
    counts = [hourly.get(h, {}).get("count", 0) for h in range(24)]

    best = None
    best_score = None
    for length in range(MIN_WINDOW_HOURS, MAX_WINDOW_HOURS + 1):
        for start in range(0, 24 - length + 1):
            end = start + length
            window_profit = sum(profits[start:end])
            window_count = sum(counts[start:end])
            # require reasonable density so a single lucky hour can't dominate
            if window_count < max(10, length * 2):
                continue
            if best_score is None or window_profit > best_score:
                best_score = window_profit
                best = (start, end)
    return best


async def run_hour_scheduler_cycle() -> None:
    """Runs on a ~12h cadence: recompute the best trading-hour window per asset."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT asset, session_hour, COUNT(*) AS n, COALESCE(SUM(profit), 0) AS total_profit
            FROM trade_telemetry
            WHERE type LIKE '%CLOSE%'
              AND session_hour IS NOT NULL
              AND created_at >= NOW() - INTERVAL '30 days'
            GROUP BY asset, session_hour
            """
        )

        by_asset: dict = {}
        totals: dict = {}
        for r in rows:
            asset = r["asset"]
            by_asset.setdefault(asset, {})[int(r["session_hour"])] = {
                "count": int(r["n"]),
                "profit": float(r["total_profit"]),
            }
            totals[asset] = totals.get(asset, 0) + int(r["n"])

        for asset, hourly in by_asset.items():
            if totals.get(asset, 0) < MIN_SAMPLE_PER_ASSET:
                log.info(
                    "Hour-scheduler: %s has only %d closed trades (need %d) - leaving schedule untouched.",
                    asset, totals.get(asset, 0), MIN_SAMPLE_PER_ASSET,
                )
                continue

            window = _best_window(hourly)
            if window is None:
                log.info("Hour-scheduler: %s - no contiguous window met the density bar, leaving as-is.", asset)
                continue

            start_hour, end_hour = window
            await conn.execute(
                """
                UPDATE strategy_db
                SET scheduled_start_hour = $2, scheduled_end_hour = $3, updated_at = NOW()
                WHERE asset = $1
                """,
                asset, start_hour, end_hour,
            )
            log.info(
                "Hour-scheduler: %s -> scheduled window %02d:00-%02d:00 (from %d closed trades).",
                asset, start_hour, end_hour, totals[asset],
            )