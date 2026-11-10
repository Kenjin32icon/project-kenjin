# startup/jobs/groq_forecast.py
"""
Tier-1 macro forecast engine (v12).

Provider-agnostic: LLM_PROVIDER=groq (default) or xai (Grok). Both expose an
OpenAI-compatible /chat/completions endpoint, so one httpx client serves both.

    LLM_PROVIDER=groq  GROQ_API_KEY=...  GROQ_MODEL=openai/gpt-oss-20b
    LLM_PROVIDER=xai   XAI_API_KEY=...   XAI_MODEL=grok-4.5

The prompt now contains (a) the broker-side engineered features and (b) the condensed
Yahoo Finance market-context snapshot. LLM probabilities are SHRUNK toward 50/50 and
clamped to [LLM_PROB_MIN, LLM_PROB_MAX]: a language model's raw confidence is not
calibrated, and the downstream EV gate should never treat 99% as 99%.

Fixes vs v11.09: removed the non-existent `GroqClient` import, replaced the Polars
summary path with the canonical pandas feature frame, parses JSON defensively.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, Optional

import httpx

from startup.db import get_pool
from startup.jobs.feature_pull import get_distinct_assets, pull_feature_window
from startup.market_context import service as market_service

log = logging.getLogger("groq_forecast")

PROVIDER = os.environ.get("LLM_PROVIDER", "groq").lower()
ENDPOINTS = {"groq": "https://api.groq.com/openai/v1/chat/completions",
             "xai": "https://api.x.ai/v1/chat/completions"}
GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")
XAI_MODEL = os.environ.get("XAI_MODEL", "grok-4.5")
LLM_PROB_MIN = float(os.environ.get("LLM_PROB_MIN", "25"))
LLM_PROB_MAX = float(os.environ.get("LLM_PROB_MAX", "75"))
LLM_SHRINK = float(os.environ.get("LLM_SHRINK", "0.6"))   # weight on the LLM view vs 50/50
LLM_TIMEOUT = float(os.environ.get("LLM_TIMEOUT_SECONDS", "30"))


def active_model() -> str:
    return XAI_MODEL if PROVIDER == "xai" else GROQ_MODEL


SYSTEM_PROMPT = (
    "You are a quantitative macro forecasting engine for short-horizon trading. "
    "You receive broker-side technical features and a cross-asset market-context summary. "
    "Estimate directional probabilities for the NEXT 15 MINUTES only. Be conservative: when "
    "evidence conflicts, stay near 50/50. Respond with ONE JSON object and nothing else: "
    '{"bullish_prob": <0-100>, "bearish_prob": <0-100>, "rationale": "<max 200 chars>"} '
    "with bullish_prob + bearish_prob = 100."
)


def _api_key() -> str:
    return os.environ.get("XAI_API_KEY" if PROVIDER == "xai" else "GROQ_API_KEY", "").strip()


def summarise_frame(df: Any, asset: str) -> str:
    if df is None or len(df) < 5:
        return json.dumps({"error": "insufficient_ticks"})
    last = df.iloc[-1]

    def f(k: str, d: float = 0.0) -> float:
        try:
            v = float(last.get(k, d))
            return d if v != v else round(v, 5)
        except Exception:
            return d

    summary = {
        "asset": asset, "n_ticks": int(len(df)),
        "bid": f("bid"), "ask": f("ask"), "rsi": f("rsi", 50.0), "adx": f("adx"),
        "volatility_regime_bps": f("volatility_regime"), "rvol": f("rvol", 1.0),
        "ma_spread_delta": f("ma_spread_delta"),
        "price_velocity_short": f("price_velocity_1m"), "price_velocity_long": f("price_velocity_5m"),
        "local_ea_score": f("local_score"),
    }
    ctx = market_service.get_asset_context(asset)
    summary["market_context"] = {
        "fresh": ctx["fresh"], "regime": ctx["regime"], "yahoo_bias": ctx["bias"],
        "macro": ctx["macro"], "summary": ctx["summary"],
    }
    return json.dumps(summary)


def _parse_probs(raw: str) -> Optional[Dict[str, Any]]:
    raw = (raw or "").strip()
    raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()
    m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    if "bullish_prob" in data and "bearish_prob" in data:
        bull, bear = float(data["bullish_prob"]), float(data["bearish_prob"])
    else:
        direction = str(data.get("direction", "NEUTRAL")).upper()
        conf = float(data.get("confidence", 0.5))
        bull = {"BULLISH": conf * 100, "BEARISH": (1 - conf) * 100}.get(direction, 50.0)
        bear = 100.0 - bull
    total = bull + bear
    if total <= 0:
        return None
    bull, bear = 100.0 * bull / total, 100.0 * bear / total
    return {"bull": bull, "bear": bear, "rationale": str(data.get("rationale", ""))[:500]}


def shrink_and_clamp(bull: float) -> float:
    bull = 50.0 + LLM_SHRINK * (bull - 50.0)
    return max(LLM_PROB_MIN, min(LLM_PROB_MAX, bull))


async def call_llm(client: httpx.AsyncClient, user_payload: str) -> Optional[str]:
    key = _api_key()
    if not key:
        log.error("%s API key missing (LLM_PROVIDER=%s).", "XAI_API_KEY" if PROVIDER == "xai" else "GROQ_API_KEY", PROVIDER)
        return None
    body = {"model": active_model(), "temperature": 0.2,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_payload}]}
    # JSON mode is supported by both providers; retry once without it if rejected.
    for use_json_mode in (True, False):
        payload = dict(body)
        if use_json_mode:
            payload["response_format"] = {"type": "json_object"}
        resp = await client.post(ENDPOINTS.get(PROVIDER, ENDPOINTS["groq"]),
                                 headers={"Authorization": f"Bearer {key}"}, json=payload)
        if resp.status_code == 400 and use_json_mode:
            continue
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    return None


async def run_forecast_cycle() -> None:
    """APScheduler entry point."""
    if not _api_key():
        log.error("LLM key not configured - forecast cycle skipped.")
        return
    assets = await get_distinct_assets()
    if not assets:
        return
    pool = get_pool()
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        for asset in assets:
            try:
                market_service.track_asset(asset)
                df = await pull_feature_window(asset, minutes=20, return_pandas=True)
                if df is None or len(df) < 5:
                    log.debug("Skipping %s: insufficient rows.", asset)
                    continue
                raw = await call_llm(client, summarise_frame(df, asset))
                parsed = _parse_probs(raw or "")
                if not parsed:
                    log.warning("Unparseable LLM output for %s: %.120s", asset, raw)
                    continue
                bull = round(shrink_and_clamp(parsed["bull"]), 2)
                bear = round(100.0 - bull, 2)
                direction = "BULLISH" if bull > 55 else ("BEARISH" if bull < 45 else "NEUTRAL")
                async with pool.acquire() as conn:
                    await conn.execute(
                        """INSERT INTO forecasts (asset, horizon_minutes, bullish_prob, bearish_prob,
                                                  rationale, model_used, direction, confidence)
                           VALUES ($1, 15, $2, $3, $4, $5, $6, $7)""",
                        asset, bull, bear, parsed["rationale"], f"{PROVIDER}:{active_model()}",
                        direction, round(abs(bull - 50.0) * 2.0, 2),
                    )
                log.info("Forecast %s via %s: bull=%.1f bear=%.1f", asset, PROVIDER, bull, bear)
            except Exception as exc:
                log.exception("Forecast cycle error for %s: %s", asset, exc)
