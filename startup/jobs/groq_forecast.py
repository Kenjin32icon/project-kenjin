"""
Tier 1 Macro Engine: Calls Groq API for 15-minute structural direction logic,
incorporating advanced volatility regimes, volume anomalies, and price velocity.
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

SYSTEM_PROMPT = """
You are a quantitative macro forecasting AI. Analyze the provided technical indicators 
and output directional probabilities strictly for the next 15-minute window.
Output strictly in JSON format using keys 'bullish_prob' and 'bearish_prob' (as float values between 0.0 and 100.0, summing to 100.0), and optional 'rationale'.
Alternatively, you may output 'direction' (BULLISH, BEARISH, NEUTRAL) and 'confidence' (0.0 to 1.0).
"""


def summarise_frame(df: pd.DataFrame) -> str:
    """Summarises the advanced feature window for Groq inference."""
    if df.empty or len(df) < 5:
        return json.dumps({"error": "Insufficient tick data."})

    latest = df.iloc[-1]
    summary = {
        "n_ticks": len(df),
        "latest_bid": float(latest.get("bid") or 0),
        "latest_ask": float(latest.get("ask") or 0),
        "rsi": float(latest.get("rsi") or 50.0),
        "adx": float(latest.get("adx") or 0),
        "volatility_regime": float(latest.get("volatility_regime") or 0),
        "rvol": float(latest.get("rvol") or 1.0),
        "ma_spread_delta": float(latest.get("ma_spread_delta") or 0.0),
        "price_velocity_1m": float(latest.get("price_velocity_1m") or 0.0),
        "price_velocity_5m": float(latest.get("price_velocity_5m") or 0.0),
    }
    return json.dumps(summary)


async def run_forecast_cycle() -> None:
    """Called on APScheduler cadence to evaluate and update 15-minute market forecasts."""
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        log.error("GROQ_API_KEY environment variable missing.")
        return

    client = AsyncGroq(api_key=api_key)
    assets = await get_distinct_assets()

    for asset in assets:
        try:
            # Pull 20 minutes of context for a 15-minute prediction window
            df = await pull_feature_window(asset, minutes=20)
            if df.empty or len(df) < 5:
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

            # Clean markdown code fences if output by LLM
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
                raw = raw.strip()

            data = json.loads(raw)
            
            # Map probabilities cleanly; fallback to direction mapping if legacy output format is received
            if "bullish_prob" in data and "bearish_prob" in data:
                bullish_prob = float(data["bullish_prob"])
                bearish_prob = float(data["bearish_prob"])
            else:
                direction = data.get("direction", "NEUTRAL").upper()
                confidence = float(data.get("confidence", 0.5))
                if direction == "BULLISH":
                    bullish_prob = round(confidence * 100.0, 2)
                    bearish_prob = round((1.0 - confidence) * 100.0, 2)
                elif direction == "BEARISH":
                    bearish_prob = round(confidence * 100.0, 2)
                    bullish_prob = round((1.0 - confidence) * 100.0, 2)
                else:
                    bullish_prob = 50.0
                    bearish_prob = 50.0

            rationale = data.get("rationale", "")

            pool = get_pool()
            async with pool.acquire() as conn:
                query = """
                    INSERT INTO forecasts (asset, horizon_minutes, bullish_prob, bearish_prob, rationale, model_used)
                    VALUES ($1, 15, $2, $3, $4, $5)
                """
                await conn.execute(query, asset, bullish_prob, bearish_prob, rationale, GROQ_MODEL)
                
            log.info(f"Forecast updated for {asset}: Bullish={bullish_prob}%, Bearish={bearish_prob}%")
        except Exception as e:
            log.exception(f"Forecast cycle error for asset {asset}: {e}")
