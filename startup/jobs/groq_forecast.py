# startup/jobs/groq_forecast.py
"""
Tier 1 Macro Engine: Calls Groq API for 15-minute structural direction logic,
incorporating advanced volatility regimes, volume anomalies, and price velocity.
Supports synchronous client dispatch and automated scheduled forecast cycles.
"""

import os
import json
import logging
from typing import Any, Dict, Optional

# Keep pandas import for compatibility code paths/tests that expect pandas objects.
import pandas as pd

try:
    from groq import AsyncGroq, GroqClient
except Exception:
    try:
        from groq import AsyncGroq
        GroqClient = None
    except Exception:
        AsyncGroq = None
        GroqClient = None

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


# -----------------------
# Synchronous Request Handler
# -----------------------
def request_groq_forecast(payload: Dict[str, Any]) -> Optional[Any]:
    """Dispatches a direct forecast query using the synchronous GroqClient with safety checks."""
    if GroqClient is None:
        log.warning("groq client not available in environment")
        return None
    try:
        client = GroqClient()
        resp = client.query(payload)
        return resp
    except Exception as exc:
        log.exception("groq_forecast: client error: %s", exc)
        return None


# -----------------------
# DataFrame compatibility helpers (polars/pandas)
# -----------------------
def is_df_empty(df: Any) -> bool:
    """
    Robust emptiness check supporting:
      - polars.DataFrame.is_empty()
      - pandas.DataFrame.empty
      - fall back to len(df) == 0
    """
    try:
        if hasattr(df, "is_empty") and callable(getattr(df, "is_empty")):
            return bool(df.is_empty())
        if hasattr(df, "empty"):
            return bool(getattr(df, "empty"))
        return len(df) == 0
    except Exception as exc:
        log.debug("is_df_empty fallback triggered: %s", exc)
        return True


def last_row_as_dict(df: Any) -> Dict[str, Any]:
    """
    Return the last row as a column->value dict, supporting Polars and Pandas frames.
    """
    if df is None:
        return {}

    # Polars path (named row)
    try:
        if hasattr(df, "row"):
            try:
                out = df.row(-1, named=True)
                if isinstance(out, dict):
                    return out
                if hasattr(df, "columns") and out is not None:
                    cols = list(df.columns)
                    return dict(zip(cols, out))
            except TypeError:
                tup = df.row(-1)
                if tup is None:
                    raise
                if hasattr(df, "columns"):
                    return dict(zip(list(df.columns), tup))
    except Exception:
        log.debug("polars row path failed for last_row_as_dict", exc_info=True)

    # pandas path
    try:
        if hasattr(df, "iloc"):
            last = df.iloc[-1]
            try:
                return last.to_dict()
            except Exception:
                if hasattr(df, "columns") and hasattr(last, "__len__"):
                    return dict(zip(list(df.columns), list(last)))
    except Exception:
        log.debug("pandas iloc path failed for last_row_as_dict", exc_info=True)

    # Fallback: try tail(1) + to_dict('records')
    try:
        if hasattr(df, "tail"):
            tail = df.tail(1)
            if hasattr(tail, "to_dict"):
                try:
                    recs = tail.to_dict(orient="records")
                    if recs:
                        return recs[0]
                except Exception:
                    pass
            try:
                if hasattr(tail, "to_dict") and callable(getattr(tail, "to_dict")):
                    d = tail.to_dict()
                    return {k: v[-1] if isinstance(v, (list, tuple)) and len(v) > 0 else v for k, v in d.items()}
            except Exception:
                pass
    except Exception:
        log.debug("tail fallback path failed for last_row_as_dict", exc_info=True)

    log.debug("Could not extract last row as dict; returning empty dict. Type: %s", type(df))
    return {}


# -----------------------
# Summariser
# -----------------------
def summarise_frame(df: Any) -> str:
    """Summarises the advanced feature window for Groq inference."""
    try:
        if df is None or is_df_empty(df) or (hasattr(df, "__len__") and len(df) < 5):
            return json.dumps({"error": "Insufficient tick data."})
    except Exception:
        return json.dumps({"error": "Insufficient tick data."})

    latest = last_row_as_dict(df)

    if not latest:
        try:
            cols = []
            if hasattr(df, "columns"):
                cols = list(df.columns)
            else:
                try:
                    cols = list(pd.DataFrame(df).columns)
                except Exception:
                    cols = []

            if cols:
                for c in cols:
                    try:
                        if hasattr(df, "iloc"):
                            latest[c] = df[c].iloc[-1]
                        elif hasattr(df, "select"):
                            try:
                                s = df.select(c).to_series()
                                latest[c] = s[-1]
                            except Exception:
                                latest[c] = pd.DataFrame(df)[c].iloc[-1]
                    except Exception:
                        latest[c] = None
        except Exception:
            pass

    def _f(k, default):
        v = latest.get(k) if isinstance(latest, dict) else None
        try:
            return float(v) if v is not None else float(default)
        except Exception:
            try:
                return float(default)
            except Exception:
                return 0.0

    summary = {
        "n_ticks": int(len(df)) if hasattr(df, "__len__") else 0,
        "latest_bid": _f("bid", 0.0),
        "latest_ask": _f("ask", 0.0),
        "rsi": _f("rsi", 50.0),
        "adx": _f("adx", 0.0),
        "volatility_regime": _f("volatility_regime", 0.0),
        "rvol": _f("rvol", 1.0),
        "ma_spread_delta": _f("ma_spread_delta", 0.0),
        "price_velocity_1m": _f("price_velocity_1m", 0.0),
        "price_velocity_5m": _f("price_velocity_5m", 0.0),
    }
    return json.dumps(summary)


# -----------------------
# Automated Forecast Cycle
# -----------------------
async def run_forecast_cycle() -> None:
    """Called on APScheduler cadence to evaluate and update 15-minute market forecasts."""
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        log.error("GROQ_API_KEY environment variable missing.")
        return

    if AsyncGroq is None:
        log.error("AsyncGroq client class not imported or available.")
        return

    client = AsyncGroq(api_key=api_key)
    assets = await get_distinct_assets()

    for asset in assets:
        try:
            df = await pull_feature_window(asset, minutes=20)

            if df is None or is_df_empty(df) or (hasattr(df, "__len__") and len(df) < 5):
                log.debug("Skipping asset %s: insufficient frame rows.", asset)
                continue

            summary_payload = summarise_frame(df)

            completion = await client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": summary_payload},
                ],
                temperature=0.2,
            )
            raw = completion.choices[0].message.content.strip()

            if raw.startswith("```"):
                parts = raw.split("```")
                if len(parts) >= 2:
                    raw = parts[1]
                    if raw.strip().startswith("json"):
                        raw = raw.strip()[4:]
                raw = raw.strip()

            data = json.loads(raw)

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