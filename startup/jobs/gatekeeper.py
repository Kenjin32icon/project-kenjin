"""
startup/jobs/gatekeeper.py

Gatekeeper Autotuning Job: Evaluates rolling trade performance on a ~4-hour cadence,
computing win rate, profit factor, and sample size from trade_telemetry.
Dynamically updates approval flags (`live_approved`), self-tunes entry 
thresholds (`opt_threshold`), stop-loss (`opt_sl_mult`), and take-profit (`opt_tp_mult`)
multipliers, and parses analytics to adjust RSI veto thresholds[cite: 15].
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from startup.db import get_pool

logger = logging.getLogger("gatekeeper")

# --- Tunable thresholds -----------------------------------------------
MIN_SAMPLE_SIZE = 30
MIN_WIN_RATE = 55.0
MIN_PROFIT_FACTOR = 1.3
LOOKBACK_DAYS = 7


async def _fetch_tracked_assets(conn):
    """All assets currently configured in strategy_db."""
    rows = await conn.fetch("SELECT asset FROM strategy_db")
    return [r["asset"] for r in rows]


async def _fetch_recent_closed_trades(conn, asset: str, since: datetime):
    """Closed trades for a single asset since a cutoff timestamp."""
    return await conn.fetch(
        """
        SELECT profit, created_at, type
        FROM trade_telemetry
        WHERE asset = $1
          AND created_at >= $2
          AND account_type = 'live'
        ORDER BY created_at ASC
        """,
        asset,
        since,
    )


def _compute_stats(rows):
    """Return (win_rate, profit_factor, sample_size) from trade rows."""
    closed_rows = [r for r in rows if r["type"] and "CLOSE" in r["type"]]
    sample_size = len(closed_rows)
    if sample_size == 0:
        return 0.0, 0.0, 0

    wins = [r["profit"] for r in closed_rows if r["profit"] is not None and r["profit"] > 0]
    losses = [abs(r["profit"]) for r in closed_rows if r["profit"] is not None and r["profit"] < 0]

    win_rate = (len(wins) / sample_size) * 100.0
    gross_profit = float(sum(wins))
    gross_loss = float(sum(losses))

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = gross_profit  
    else:
        profit_factor = 0.0

    return win_rate, profit_factor, sample_size


async def tune_rsi_veto_from_analytics() -> None:
    """Parses missed_trade_analytics to adjust strategy_db RSI limits[cite: 15]."""
    pool = get_pool()
    async with pool.acquire() as conn:
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
                logger.info(
                    f"RSI Veto Tuned for {asset}: BUY Max = {clamped_rsi_buy_max}, SELL Min = {clamped_rsi_sell_min}"
                )
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logger.warning(f"Could not parse tweak JSON for {asset}: {e}")


async def run_gatekeeper_cycle(pool=None) -> None:
    """Evaluates rolling trade performance and autotunes strategy parameters[cite: 15]."""
    if pool is None:
        pool = get_pool()

    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    async with pool.acquire() as conn:
        prior_status = {
            r["asset"]: r["live_approved"]
            for r in await conn.fetch("SELECT asset, live_approved FROM strategy_db")
        }

    assets = []
    async with pool.acquire() as conn:
        assets = await _fetch_tracked_assets(conn)

    evaluated = 0
    approved = 0

    for asset in assets:
        try:
            async with pool.acquire() as conn:
                rows = await _fetch_recent_closed_trades(conn, asset, since)
                win_rate, profit_factor, sample_size = _compute_stats(rows)

                qualifies = (
                    sample_size >= MIN_SAMPLE_SIZE
                    and win_rate >= MIN_WIN_RATE
                    and profit_factor >= MIN_PROFIT_FACTOR
                )

                current_db = await conn.fetchrow(
                    "SELECT live_approved, opt_threshold, opt_sl_mult, opt_tp_mult FROM strategy_db WHERE asset = $1", asset
                )
                current_live_approved = bool(current_db["live_approved"]) if current_db and current_db["live_approved"] is not None else False
                current_thresh = float(current_db["opt_threshold"]) if current_db and current_db["opt_threshold"] is not None else 0.60
                current_sl = float(current_db["opt_sl_mult"]) if current_db and current_db["opt_sl_mult"] is not None else 1.50
                current_tp = float(current_db["opt_tp_mult"]) if current_db and current_db["opt_tp_mult"] is not None else 3.00

                if sample_size < MIN_SAMPLE_SIZE:
                    live_approved = current_live_approved
                else:
                    live_approved = qualifies

                if sample_size >= 10:
                    if win_rate < 45.0:
                        current_thresh = min(0.85, current_thresh + 0.03)
                        current_sl = min(2.0, current_sl + 0.1)
                    elif win_rate > 60.0 and profit_factor > 1.4:
                        current_thresh = max(0.45, current_thresh - 0.02)
                        current_tp = min(4.0, current_tp + 0.2)

                await conn.execute(
                    """
                    UPDATE strategy_db
                    SET win_rate = $2, profit_factor = $3, sample_size = $4,
                        live_approved = $5, opt_threshold = $6, opt_sl_mult = $7, opt_tp_mult = $8, updated_at = NOW()
                    WHERE asset = $1
                    """,
                    asset, win_rate, profit_factor, sample_size, live_approved, current_thresh, current_sl, current_tp
                )

                await conn.execute(
                    """
                    INSERT INTO autotune_log 
                    (asset, win_rate, profit_factor, sample_size, old_threshold, new_threshold, old_sl, new_sl, old_tp, new_tp, ts)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
                    """,
                    asset, win_rate, profit_factor, sample_size, 
                    current_db["opt_threshold"] if current_db else 0.60, current_thresh, 
                    current_db["opt_sl_mult"] if current_db else 1.50, current_sl, 
                    current_db["opt_tp_mult"] if current_db else 3.00, current_tp
                )

                was_approved = prior_status.get(asset, False)
                if was_approved and not live_approved:
                    await conn.execute(
                        """
                        INSERT INTO risk_incidents (account_type, asset, reason, details)
                        VALUES ($1, $2, $3, $4)
                        """,
                        "live",
                        asset,
                        "gatekeeper_revoked_live_approval",
                        f"win_rate={win_rate:.3f} profit_factor={profit_factor:.3f} sample_size={sample_size}",
                    )

            evaluated += 1
            approved += int(live_approved)

            logger.info(
                "Gatekeeper: %s -> closed=%d win_rate=%.1f%% pf=%.2f live_approved=%s opt_threshold=%.2f opt_sl_mult=%.2f opt_tp_mult=%.2f",
                asset, sample_size, win_rate, profit_factor, live_approved, current_thresh, current_sl, current_tp
            )

        except Exception:
            logger.exception("Gatekeeper: failed to evaluate asset %s, skipping", asset)
            continue

    await tune_rsi_veto_from_analytics()

    logger.info(
        "Gatekeeper cycle complete: %d/%d assets evaluated, %d approved for live trading",
        evaluated, len(assets), approved,
    )