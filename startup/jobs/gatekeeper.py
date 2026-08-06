"""
Gatekeeper Autotuning Job: Evaluates rolling trade performance on a ~4-hour cadence,
computing win rate, profit factor, and sample size from trade_telemetry[cite: 16].
Dynamically updates approval flags (`live_approved`) and self-tunes entry 
thresholds (`opt_threshold`) to adapt to changing market conditions.
"""
import logging
from startup.db import get_pool

log = logging.getLogger("gatekeeper")

MIN_SAMPLE_SIZE = 30
MIN_WIN_RATE = 55.0
MIN_PROFIT_FACTOR = 1.3


async def run_gatekeeper_cycle() -> None:
    """Evaluates rolling trade performance and autotunes strategy parameters[cite: 16]."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              asset,
              COUNT(*) FILTER (WHERE type LIKE '%CLOSE%') AS closed_trades,
              ROUND(100.0 * COUNT(*) FILTER (WHERE profit > 0)
                    / NULLIF(COUNT(*) FILTER (WHERE type LIKE '%CLOSE%'), 0), 1) AS win_rate,
              ROUND((SUM(profit) FILTER (WHERE profit > 0)
                    / NULLIF(ABS(SUM(profit) FILTER (WHERE profit < 0)), 0))::numeric, 2) AS profit_factor
            FROM trade_telemetry
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY asset
            """
        )

        for row in rows:
            asset = row["asset"]
            closed = row["closed_trades"] or 0
            win_rate = float(row["win_rate"]) if row["win_rate"] is not None else 0.0
            profit_factor = float(row["profit_factor"]) if row["profit_factor"] is not None else 0.0

            qualifies = (
                closed >= MIN_SAMPLE_SIZE
                and win_rate >= MIN_WIN_RATE
                and profit_factor >= MIN_PROFIT_FACTOR
            )

            # Fetch current entry threshold for dynamic autotuning adjustment
            current_db = await conn.fetchrow("SELECT opt_threshold FROM strategy_db WHERE asset = $1", asset)
            current_thresh = float(current_db["opt_threshold"]) if current_db and current_db["opt_threshold"] is not None else 0.60

            # Dynamic threshold autotuning logic based on rolling performance
            if closed >= 10:
                if win_rate < 45.0:
                    current_thresh = min(0.85, current_thresh + 0.03)  # Be more conservative
                elif win_rate > 60.0 and profit_factor > 1.4:
                    current_thresh = max(0.45, current_thresh - 0.02)  # Take more trades

            await conn.execute(
                """
                UPDATE strategy_db
                SET win_rate = $2, profit_factor = $3, sample_size = $4,
                    live_approved = $5, opt_threshold = $6, updated_at = NOW()
                WHERE asset = $1
                """,
                asset, win_rate, profit_factor, closed, qualifies, current_thresh
            )
            log.info(
                "Gatekeeper: %s -> closed=%d win_rate=%.1f%% pf=%.2f live_approved=%s opt_threshold=%.2f",
                asset, closed, win_rate, profit_factor, qualifies, current_thresh,
            )