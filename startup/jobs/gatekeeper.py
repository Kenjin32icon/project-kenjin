"""
Gatekeeper Autotuning Job: Evaluates rolling trade performance on a ~4-hour cadence,
computing win rate, profit factor, and sample size from trade_telemetry.
Dynamically updates approval flags (`live_approved`), self-tunes entry 
thresholds (`opt_threshold`), stop-loss (`opt_sl_mult`), and take-profit (`opt_tp_mult`)
multipliers, and parses analytics to adjust RSI veto thresholds.
"""
import json
import logging
from startup.db import get_pool

log = logging.getLogger("gatekeeper")

MIN_SAMPLE_SIZE = 30
MIN_WIN_RATE = 55.0
MIN_PROFIT_FACTOR = 1.3


async def tune_rsi_veto_from_analytics() -> None:
    """Parses missed_trade_analytics to adjust strategy_db RSI limits."""
    pool = get_pool()
    async with pool.acquire() as conn:
        # Fetch latest analysis per asset using created_at or analyzed_at fallback
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (asset) asset, suggested_logic_tweak
            FROM missed_trade_analytics
            WHERE COALESCE(created_at, analyzed_at) >= NOW() - INTERVAL '24 hours'
            ORDER BY asset, COALESCE(created_at, analyzed_at) DESC
            """
        )

        for row in rows:
            asset = row["asset"]
            try:
                tweak_data = json.loads(row["suggested_logic_tweak"])
                new_rsi_buy_max = float(tweak_data.get("suggested_rsi_buy_max", 70.0))
                new_rsi_sell_min = float(tweak_data.get("suggested_rsi_sell_min", 30.0))

                # Clamp parameters to safe boundaries
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
                    asset, clamped_rsi_buy_max, clamped_rsi_sell_min
                )
                log.info(
                    f"RSI Veto Tuned for {asset}: BUY Max = {clamped_rsi_buy_max}, SELL Min = {clamped_rsi_sell_min}"
                )
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                log.warning(f"Could not parse tweak JSON for {asset}: {e}")


async def run_gatekeeper_cycle() -> None:
    """Evaluates rolling trade performance and autotunes strategy parameters."""
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
              AND account_type = 'live'  # FIX: Prevent demo leak into Gatekeeper
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

            # Fetch current approval state and parameters for dynamic autotuning adjustment
            current_db = await conn.fetchrow(
                "SELECT live_approved, opt_threshold, opt_sl_mult, opt_tp_mult FROM strategy_db WHERE asset = $1", asset
            )
            current_live_approved = bool(current_db["live_approved"]) if current_db and current_db["live_approved"] is not None else False
            current_thresh = float(current_db["opt_threshold"]) if current_db and current_db["opt_threshold"] is not None else 0.60
            current_sl = float(current_db["opt_sl_mult"]) if current_db and current_db["opt_sl_mult"] is not None else 1.50
            current_tp = float(current_db["opt_tp_mult"]) if current_db and current_db["opt_tp_mult"] is not None else 3.00

            # Preserve current live_approved state if sample size is insufficient
            if closed < MIN_SAMPLE_SIZE:
                live_approved = current_live_approved
            else:
                live_approved = qualifies

            # Dynamic threshold autotuning logic based on rolling performance
            if closed >= 10:
                if win_rate < 45.0:
                    current_thresh = min(0.85, current_thresh + 0.03)  # Be more conservative
                    current_sl = min(2.0, current_sl + 0.1)  # Widen stop loss slightly against whipsaws
                elif win_rate > 60.0 and profit_factor > 1.4:
                    current_thresh = max(0.45, current_thresh - 0.02)  # Take more trades
                    current_tp = min(4.0, current_tp + 0.2)  # Let runners run

            await conn.execute(
                """
                UPDATE strategy_db
                SET win_rate = $2, profit_factor = $3, sample_size = $4,
                    live_approved = $5, opt_threshold = $6, opt_sl_mult = $7, opt_tp_mult = $8, updated_at = NOW()
                WHERE asset = $1
                """,
                asset, win_rate, profit_factor, closed, live_approved, current_thresh, current_sl, current_tp
            )
            log.info(
                "Gatekeeper: %s -> closed=%d win_rate=%.1f%% pf=%.2f live_approved=%s opt_threshold=%.2f opt_sl_mult=%.2f opt_tp_mult=%.2f",
                asset, closed, win_rate, profit_factor, live_approved, current_thresh, current_sl, current_tp
            )

    # Autotune RSI veto limits from recent analytics
    await tune_rsi_veto_from_analytics()