"""
Gatekeeper Autotuning Job: Evaluates rolling trade performance on a ~4-hour cadence,
computing win rate, profit factor, and sample size from trade_telemetry.
Dynamically updates approval flags (`live_approved`), self-tunes entry 
thresholds (`opt_threshold`), stop-loss (`opt_sl_mult`), and take-profit (`opt_tp_mult`)
multipliers, and parses analytics to adjust RSI veto thresholds.

HARDENED:
  - Uses positional $1/$2 asyncpg bindings instead of named parameters.
  - Implements per-asset try/except isolation to prevent cycle crashes.
  - Replaces invalid SQL comments (#) with Postgres standards (--).
"""
import json
import logging
from datetime import datetime, timedelta, timezone

from startup.db import get_pool

log = logging.getLogger("gatekeeper")

# --- Tunable thresholds -----------------------------------------------
MIN_SAMPLE_SIZE = 30
MIN_WIN_RATE = 55.0
MIN_PROFIT_FACTOR = 1.3
LOOKBACK_DAYS = 7


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


async def _fetch_tracked_assets(conn) -> list:
    """All assets currently configured in strategy_db."""
    rows = await conn.fetch("SELECT asset FROM strategy_db")
    return [r["asset"] for r in rows]


async def _fetch_recent_closed_trades(conn, asset: str, since: datetime):
    """
    Closed LIVE trades for a single asset since a cutoff timestamp.
    Using positional $1/$2 placeholders.
    """
    return await conn.fetch(
        """
        SELECT profit, created_at
        FROM trade_telemetry
        WHERE asset = $1
          AND created_at >= $2
          AND account_type = 'live'  -- FIX: Prevent demo leak into Gatekeeper
          AND profit IS NOT NULL
          AND type LIKE '%CLOSE%'
        ORDER BY created_at ASC
        """,
        asset,
        since,
    )


def _compute_stats(rows) -> tuple:
    """Return (win_rate, profit_factor, sample_size) from trade rows."""
    sample_size = len(rows)
    if sample_size == 0:
        return 0.0, 0.0, 0

    wins = [r["profit"] for r in rows if r["profit"] is not None and r["profit"] > 0]
    losses = [abs(r["profit"]) for r in rows if r["profit"] is not None and r["profit"] < 0]

    # Convert to percentage logic for autotune thresholds
    win_rate = (len(wins) / sample_size) * 100.0
    gross_profit = float(sum(wins))
    gross_loss = float(sum(losses))

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = gross_profit  # no losses at all yet
    else:
        profit_factor = 0.0

    return round(win_rate, 1), round(profit_factor, 2), sample_size


async def _log_risk_incident_if_revoked(conn, asset, was_approved, now_approved, win_rate, profit_factor, sample_size):
    """
    If an asset that WAS approved just got revoked, drop a
    row into risk_incidents so it's visible without grepping logs.
    """
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


async def run_gatekeeper_cycle() -> None:
    """Evaluates rolling trade performance and autotunes strategy parameters per asset."""
    pool = get_pool()
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    
    async with pool.acquire() as conn:
        assets = await _fetch_tracked_assets(conn)

    evaluated = 0
    approved = 0

    for asset in assets:
        try:
            async with pool.acquire() as conn:
                # 1. Fetch historical DB states & closed trades
                current_db = await conn.fetchrow(
                    "SELECT live_approved, opt_threshold, opt_sl_mult, opt_tp_mult FROM strategy_db WHERE asset = $1", 
                    asset
                )
                if not current_db:
                    continue
                
                current_live_approved = bool(current_db["live_approved"]) if current_db["live_approved"] is not None else False
                current_thresh = float(current_db["opt_threshold"]) if current_db["opt_threshold"] is not None else 0.60
                current_sl = float(current_db["opt_sl_mult"]) if current_db["opt_sl_mult"] is not None else 1.50
                current_tp = float(current_db["opt_tp_mult"]) if current_db["opt_tp_mult"] is not None else 3.00

                rows = await _fetch_recent_closed_trades(conn, asset, since)
                win_rate, profit_factor, sample_size = _compute_stats(rows)

                # 2. Gatekeeper Approval Logic
                qualifies = (
                    sample_size >= MIN_SAMPLE_SIZE
                    and win_rate >= MIN_WIN_RATE
                    and profit_factor >= MIN_PROFIT_FACTOR
                )

                if sample_size < MIN_SAMPLE_SIZE:
                    live_approved = current_live_approved
                else:
                    live_approved = qualifies

                # 3. Dynamic Threshold Autotuning Logic
                if sample_size >= 10:
                    if win_rate < 45.0:
                        current_thresh = min(0.85, current_thresh + 0.03)  # Be more conservative
                        current_sl = min(2.0, current_sl + 0.1)  # Widen stop loss slightly against whipsaws
                    elif win_rate > 60.0 and profit_factor > 1.4:
                        current_thresh = max(0.45, current_thresh - 0.02)  # Take more trades
                        current_tp = min(4.0, current_tp + 0.2)  # Let runners run

                # 4. Persist Updates & Log Operations
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
                    current_db["opt_threshold"], current_thresh, 
                    current_db["opt_sl_mult"], current_sl, 
                    current_db["opt_tp_mult"], current_tp
                )

                await _log_risk_incident_if_revoked(
                    conn, asset, current_live_approved, live_approved, win_rate, profit_factor, sample_size
                )

                log.info(
                    "Gatekeeper: %s -> closed=%d win_rate=%.1f%% pf=%.2f live_approved=%s opt_threshold=%.2f opt_sl_mult=%.2f opt_tp_mult=%.2f",
                    asset, sample_size, win_rate, profit_factor, live_approved, current_thresh, current_sl, current_tp
                )

                evaluated += 1
                approved += int(live_approved)

        except Exception:
            # Isolation: one bad asset calculation must never kill the whole cycle.
            log.exception("Gatekeeper: failed to evaluate asset %s, skipping", asset)
            continue

    log.info(
        "Gatekeeper cycle complete: %d/%d assets evaluated, %d approved for live trading",
        evaluated, len(assets), approved,
    )

    # 5. Autotune RSI veto limits from recent analytics
    await tune_rsi_veto_from_analytics()