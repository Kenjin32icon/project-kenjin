"""
Runs on a ~4-hour cadence. Computes rolling performance per asset from
trade_telemetry and updates strategy_db's win_rate/profit_factor/sample_size.

live_approved is set here too, but per the local-alpha-training plan this
should stay conservative: v10 only ENFORCES live_approved when
InpEnforceLiveApproval=true AND the account is real, so during demo-only
testing this job is safe to run continuously - it's just building the
decision log, not gating anything yet.
"""
import logging
from startup.db import get_pool

log = logging.getLogger("gatekeeper")

MIN_SAMPLE_SIZE = 30
MIN_WIN_RATE = 55.0
MIN_PROFIT_FACTOR = 1.3


async def run_gatekeeper_cycle() -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              asset,
              COUNT(*) FILTER (WHERE type LIKE '%CLOSE%')                                   AS closed_trades,
              ROUND(100.0 * COUNT(*) FILTER (WHERE profit > 0)
                    / NULLIF(COUNT(*) FILTER (WHERE type LIKE '%CLOSE%'), 0), 1)             AS win_rate,
              ROUND((SUM(profit) FILTER (WHERE profit > 0)
                    / NULLIF(ABS(SUM(profit) FILTER (WHERE profit < 0)), 0))::numeric, 2)    AS profit_factor
            FROM trade_telemetry
            WHERE created_at >= now() - INTERVAL '7 days'
            GROUP BY asset
            """
        )

        for row in rows:
            closed = row["closed_trades"] or 0
            win_rate = float(row["win_rate"]) if row["win_rate"] is not None else 0.0
            profit_factor = float(row["profit_factor"]) if row["profit_factor"] is not None else 0.0

            qualifies = (
                closed >= MIN_SAMPLE_SIZE
                and win_rate >= MIN_WIN_RATE
                and profit_factor >= MIN_PROFIT_FACTOR
            )

            await conn.execute(
                """
                UPDATE strategy_db
                SET win_rate = $2, profit_factor = $3, sample_size = $4,
                    live_approved = $5, updated_at = now()
                WHERE asset = $1
                """,
                row["asset"], win_rate, profit_factor, closed, qualifies,
            )
            log.info(
                "Gatekeeper: %s -> closed=%d win_rate=%.1f%% pf=%.2f live_approved=%s",
                row["asset"], closed, win_rate, profit_factor, qualifies,
            )
