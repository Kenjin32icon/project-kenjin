"""
Confidence Calibration Job.

evaluate_tier2_signal() (ml_tier2.py) computes a `confidence` per trade and
scales lot size with it - but until now nothing ever checked whether that
confidence was actually predictive of outcomes. A model can report high
confidence and still be wrong; "trusting the model more" should mean trusting
it more once there's evidence it's earned that, not just because it says a
big number.

This job closes that loop:
  1. Pulls closed trades from the last 30 days where tier2_confidence was
     recorded (requires MAPSAR EA v11.01+ - earlier v11 builds captured the
     value but never actually sent it in the /telemetry payload).
  2. Computes, per asset, a Brier score: mean((confidence - outcome)^2) where
     outcome is 1 for a win, 0 for a loss. Lower is better; 0.25 is the score
     an uninformative constant 0.5 "confidence" would get, so scores above
     that are actively worse than a shrug.
  3. Writes calibration_score + calibration_n to strategy_db per asset.

main.py's get_strategy_params() reads these back and derives a
calibration_multiplier that evaluate_tier2_signal() uses to derate
lot_multiplier for any asset that doesn't yet have enough validated history,
or whose confidence has been running poorly calibrated. See ml_tier2.py's
evaluate_tier2_signal() docstring for the exact multiplier rule.
"""
import logging
from startup.db import get_pool

log = logging.getLogger("confidence_calibration")

MIN_SAMPLE_FOR_CALIBRATION = 30
# Brier score an uninformative constant-0.5 forecast would get - used as the
# "worse than a shrug" cutoff callers can compare calibration_score against.
UNINFORMATIVE_BRIER_BASELINE = 0.25


async def run_calibration_cycle() -> None:
    """Runs on a daily cadence - see main.py's scheduler registration."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT asset, tier2_confidence, profit
            FROM trade_telemetry
            WHERE type LIKE '%CLOSE%'
              AND tier2_confidence IS NOT NULL
              AND created_at >= NOW() - INTERVAL '30 days'
            """
        )

        by_asset: dict = {}
        for r in rows:
            conf = float(r["tier2_confidence"])
            outcome = 1.0 if float(r["profit"]) > 0 else 0.0
            by_asset.setdefault(r["asset"], []).append((conf, outcome))

        for asset, samples in by_asset.items():
            n = len(samples)
            if n < MIN_SAMPLE_FOR_CALIBRATION:
                log.info(
                    "Calibration: %s has %d confidence-tagged trades (need %d) - leaving as-is.",
                    asset, n, MIN_SAMPLE_FOR_CALIBRATION,
                )
                continue

            brier = sum((conf - outcome) ** 2 for conf, outcome in samples) / n

            await conn.execute(
                """
                UPDATE strategy_db
                SET calibration_score = $2, calibration_n = $3, calibration_updated_at = NOW()
                WHERE asset = $1
                """,
                asset, round(brier, 4), n,
            )
            verdict = "well-calibrated" if brier <= UNINFORMATIVE_BRIER_BASELINE else "POORLY calibrated"
            log.info("Calibration: %s -> brier=%.4f n=%d (%s)", asset, brier, n, verdict)
