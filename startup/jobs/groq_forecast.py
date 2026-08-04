"""
Builds a compact feature summary from the recent tick window and asks Groq
for a 30-minute directional probability + dynamic SL/TP multiplier
suggestion. Writes the result to `forecasts` and mirrors the SL/TP
multipliers into `strategy_db` so v10 picks them up on its next
/strategy_params poll.

Runs on its own APScheduler cadence (see orchestrator/main.py) - separate
from feature_pull's faster cadence, so a slow/rate-limited Groq call never
blocks tick ingestion.
"""
import os
import json
import logging
import pandas as pd
from groq import AsyncGroq
from startup.db import get_pool
from startup.jobs.feature_pull import get_distinct_assets, pull_feature_window

log = logging.getLogger("groq_forecast")

GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")

SYSTEM_PROMPT = """You are a quantitative FX/CFD analyst. You will be given a
30-minute window of recent price, volume, and indicator data for one asset.
Respond with ONLY a JSON object, no prose, no markdown fences, matching this
exact shape:
{"bullish_prob": <0-100 float>, "bearish_prob": <0-100 float>,
 "suggested_sl_atr_mult": <float>, "suggested_tp_atr_mult": <float>,
 "rationale": "<one short sentence>"}
bullish_prob and bearish_prob must sum to 100. Base suggested_sl_atr_mult and
suggested_tp_atr_mult on the volatility and momentum you observe - widen SL
on high-ATR/choppy windows, don't just repeat generic 1.5/3.0 defaults."""


def summarise_frame(df: pd.DataFrame) -> str:
    if df.empty:
        return "No recent tick data available for this asset."
    latest = df.iloc[-1]
    summary = {
        "n_ticks": len(df),
        "latest_bid": float(latest.get("bid") or 0),
        "latest_ask": float(latest.get("ask") or 0),
        "latest_rsi": float(latest.get("rsi") or 0),
        "latest_adx": float(latest.get("adx") or 0),
        "tema_trend": "rising" if df["tema"].iloc[-1] > df["tema"].iloc[0] else "falling",
        "ma_stack": {
            "ma10": float(latest.get("ma10") or 0), "ma20": float(latest.get("ma20") or 0),
            "ma50": float(latest.get("ma50") or 0), "ma100": float(latest.get("ma100") or 0),
            "ma200": float(latest.get("ma200") or 0),
        },
        "avg_tick_volume": float(df["tick_volume"].mean()) if "tick_volume" in df else None,
    }
    return json.dumps(summary)


async def run_forecast_cycle() -> None:
    """Called on the APScheduler ~30-min cadence for every asset with recent ticks."""
    client = AsyncGroq(api_key=os.environ["GROQ_API_KEY"])
    assets = await get_distinct_assets()

    for asset in assets:
        try:
            df = await pull_feature_window(asset, minutes=30)
            if df.empty:
                log.info("Skipping forecast for %s - no recent ticks.", asset)
                continue

            completion = await client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": summarise_frame(df)},
                ],
                temperature=0.2,
            )
            raw = completion.choices[0].message.content.strip()
            data = json.loads(raw)  # let this raise loudly - a malformed Groq response should not write bad data

            pool = get_pool()
            async with pool.acquire() as conn:
                forecast_id = await conn.fetchval(
                    """
                    INSERT INTO forecasts
                        (asset, horizon_minutes, bullish_prob, bearish_prob,
                         suggested_sl_atr_mult, suggested_tp_atr_mult, rationale, model_used)
                    VALUES ($1, 30, $2, $3, $4, $5, $6, $7)
                    RETURNING id
                    """,
                    asset, data["bullish_prob"], data["bearish_prob"],
                    data["suggested_sl_atr_mult"], data["suggested_tp_atr_mult"],
                    data.get("rationale", ""), GROQ_MODEL,
                )
                await conn.execute(
                    """
                    UPDATE strategy_db
                    SET opt_sl_mult = $2, opt_tp_mult = $3, updated_at = now()
                    WHERE asset = $1
                    """,
                    asset, data["suggested_sl_atr_mult"], data["suggested_tp_atr_mult"],
                )
            log.info("Forecast #%s written for %s: bullish=%.1f%%", forecast_id, asset, data["bullish_prob"])

        except Exception:
            # One asset's failure (bad Groq JSON, rate limit, etc.) must not
            # take down the whole cycle for every other asset.
            log.exception("Forecast cycle failed for asset %s", asset)
