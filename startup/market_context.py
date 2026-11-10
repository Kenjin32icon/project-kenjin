# startup/market_context.py
"""
Yahoo Finance market-context service (v12).

ROLE IN THE SYSTEM
------------------
Yahoo is NOT used as an execution price feed. The broker (MT5) price is the only
price a trade fills at. Yahoo is used as a *context* layer:

  * cross-asset drivers (DXY, VIX, US10Y, S&P futures, gold, oil, BTC)
  * an independent read of the traded asset's short-horizon trend
  * a condensed risk-regime label (risk_on / risk_off / neutral)

The condensed snapshot is (a) cached in memory + Redis, (b) pushed to the
dashboard over the existing WebSocket, (c) injected into the LLM (Groq / xAI Grok)
prompt, and (d) used by the Trade Guardian as an extra "against" vote.

DATA PATH
---------
Primary   : 1-minute bars polled with yfinance.download (thread-offloaded).
Optional  : yfinance live WebSocket (KENJIN_YAHOO_WS=1) updates last price between polls.
Both are unofficial endpoints: may be delayed, rate limited or change without notice,
and Yahoo's terms limit use to personal use. Every consumer therefore checks
`fresh` and degrades to "no opinion" (bias 0.0) rather than trading on stale data.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

log = logging.getLogger("market_context")

POLL_SECONDS = int(os.environ.get("KENJIN_YAHOO_POLL_SECONDS", "60"))
STALE_AFTER_SECONDS = int(os.environ.get("KENJIN_YAHOO_STALE_SECONDS", "600"))
USE_WS = os.environ.get("KENJIN_YAHOO_WS", "0").lower() in ("1", "true", "yes")
SNAPSHOT_EVERY_N_POLLS = int(os.environ.get("KENJIN_YAHOO_SNAPSHOT_EVERY", "5"))

# Broker symbol -> Yahoo symbol. Override/extend with KENJIN_YAHOO_MAP='{"US30":"^DJI"}'
DEFAULT_SYMBOL_MAP: Dict[str, str] = {
    "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X", "USDJPY": "JPY=X", "USDCHF": "CHF=X",
    "AUDUSD": "AUDUSD=X", "NZDUSD": "NZDUSD=X", "USDCAD": "CAD=X", "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X", "XAUUSD": "GC=F", "XAGUSD": "SI=F", "USOIL": "CL=F", "UKOIL": "BZ=F",
    "BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD", "SOLUSD": "SOL-USD", "XRPUSD": "XRP-USD",
    "US500": "^GSPC", "US30": "^DJI", "NAS100": "^NDX", "USTEC": "^NDX",
}

# Macro basket: label -> yahoo symbol
MACRO_BASKET: Dict[str, str] = {
    "DXY": "DX-Y.NYB", "VIX": "^VIX", "US10Y": "^TNX", "SPX_FUT": "ES=F",
    "GOLD": "GC=F", "OIL": "CL=F", "BTC": "BTC-USD",
}

# How an asset reacts to the dollar / risk factors: +1 = moves with, -1 = moves against.
# Used only for the blended bias; unknown assets get the asset's own momentum only.
FACTOR_BETAS: Dict[str, Dict[str, float]] = {
    "EURUSD": {"DXY": -1.0}, "GBPUSD": {"DXY": -1.0}, "AUDUSD": {"DXY": -1.0, "SPX_FUT": 0.5},
    "NZDUSD": {"DXY": -1.0, "SPX_FUT": 0.5}, "USDJPY": {"DXY": 1.0, "US10Y": 0.6},
    "USDCHF": {"DXY": 1.0}, "USDCAD": {"DXY": 1.0, "OIL": -0.4},
    "XAUUSD": {"DXY": -0.8, "US10Y": -0.6}, "XAGUSD": {"DXY": -0.7},
    "BTCUSD": {"SPX_FUT": 0.7, "DXY": -0.3}, "ETHUSD": {"SPX_FUT": 0.7, "DXY": -0.3},
    "US500": {"VIX": -1.0}, "NAS100": {"VIX": -1.0}, "USTEC": {"VIX": -1.0}, "US30": {"VIX": -1.0},
    "USOIL": {"OIL": 1.0},
}


def _symbol_map() -> Dict[str, str]:
    m = dict(DEFAULT_SYMBOL_MAP)
    try:
        extra = json.loads(os.environ.get("KENJIN_YAHOO_MAP", "{}") or "{}")
        if isinstance(extra, dict):
            m.update({str(k).upper(): str(v) for k, v in extra.items()})
    except Exception:
        log.warning("KENJIN_YAHOO_MAP is not valid JSON - ignoring.")
    return m


def yahoo_symbol_for(asset: str) -> Optional[str]:
    return _symbol_map().get(asset.upper())


def _clamp(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _series_metrics(close: pd.Series) -> Dict[str, float]:
    """Return/trend/vol read from a 1-minute close series."""
    close = close.dropna()
    if len(close) < 12:
        return {"ok": False}
    last = float(close.iloc[-1])

    def ret(n: int) -> float:
        if len(close) <= n:
            return 0.0
        base = float(close.iloc[-1 - n])
        return (last / base - 1.0) * 1e4 if base else 0.0   # basis points

    ema_fast = close.ewm(span=9, adjust=False).mean().iloc[-1]
    ema_slow = close.ewm(span=21, adjust=False).mean().iloc[-1]
    rets = close.pct_change().dropna().tail(60)
    vol_bps = float(rets.std() * 1e4) if len(rets) > 5 else 0.0

    r5, r15, r60 = ret(5), ret(15), ret(60)
    # Normalise the momentum by recent volatility so it is comparable across assets.
    denom = max(vol_bps * np.sqrt(15), 1e-6)
    mom = _clamp(0.6 * (r15 / denom) + 0.4 * (r5 / max(vol_bps * np.sqrt(5), 1e-6)))
    trend = 1.0 if ema_fast > ema_slow else -1.0
    score = _clamp(0.7 * mom + 0.3 * trend * min(1.0, abs(mom) + 0.3))
    return {
        "ok": True, "last": last, "ret_5m_bps": round(r5, 2), "ret_15m_bps": round(r15, 2),
        "ret_60m_bps": round(r60, 2), "vol_bps": round(vol_bps, 3),
        "trend": "UP" if trend > 0 else "DOWN", "momentum": round(score, 3),
    }


class MarketContextService:
    def __init__(self) -> None:
        self._snapshot: Dict[str, Any] = {"ts": 0.0, "fresh": False, "macro": {}, "assets": {}, "regime": "unknown"}
        self._task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._poll_count = 0
        self._tracked_assets: set = set()
        self._broadcast = None
        self._redis = None
        self._pool_getter = None
        self._live_prices: Dict[str, float] = {}

    # ---------------- wiring -----------------
    def configure(self, broadcast=None, redis_client=None, pool_getter=None) -> None:
        self._broadcast, self._redis, self._pool_getter = broadcast, redis_client, pool_getter

    def track_asset(self, asset: str) -> None:
        if asset:
            self._tracked_assets.add(asset.upper())

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="market_context_loop")
        if USE_WS and self._ws_task is None:
            self._ws_task = asyncio.create_task(self._ws_loop(), name="market_context_ws")

    async def stop(self) -> None:
        for t in (self._task, self._ws_task):
            if t:
                t.cancel()
        self._task = self._ws_task = None

    # ---------------- polling -----------------
    def _yahoo_symbols(self) -> Dict[str, str]:
        """label -> yahoo symbol for macro basket plus all tracked assets."""
        out = dict(MACRO_BASKET)
        for a in self._tracked_assets:
            ys = yahoo_symbol_for(a)
            if ys:
                out[a] = ys
        return out

    @staticmethod
    def _download(symbols: List[str]) -> pd.DataFrame:
        import yfinance as yf  # imported lazily so the orchestrator still boots without it
        return yf.download(
            tickers=symbols, period="1d", interval="1m", group_by="ticker",
            progress=False, threads=False, auto_adjust=False,
        )

    async def refresh_once(self) -> Dict[str, Any]:
        label_to_sym = self._yahoo_symbols()
        symbols = sorted(set(label_to_sym.values()))
        try:
            df = await asyncio.to_thread(self._download, symbols)
        except Exception as exc:
            log.warning("Yahoo download failed: %s", exc)
            self._snapshot["fresh"] = (time.time() - self._snapshot.get("ts", 0)) < STALE_AFTER_SECONDS
            return self._snapshot
        if df is None or df.empty:
            log.warning("Yahoo returned an empty frame (market closed or rate-limited).")
            self._snapshot["fresh"] = (time.time() - self._snapshot.get("ts", 0)) < STALE_AFTER_SECONDS
            return self._snapshot

        metrics_by_symbol: Dict[str, Dict[str, float]] = {}
        newest_bar_ts = 0.0
        for sym in symbols:
            try:
                sub = df[sym] if isinstance(df.columns, pd.MultiIndex) else df
                close = sub["Close"].dropna()
                if close.empty:
                    continue
                m = _series_metrics(close)
                if not m.get("ok"):
                    continue
                if sym in self._live_prices:
                    m["last"] = self._live_prices[sym]
                bar_ts = pd.Timestamp(close.index[-1]).tz_convert("UTC").timestamp() \
                    if pd.Timestamp(close.index[-1]).tzinfo else pd.Timestamp(close.index[-1]).timestamp()
                m["bar_age_s"] = round(time.time() - bar_ts, 1)
                newest_bar_ts = max(newest_bar_ts, bar_ts)
                metrics_by_symbol[sym] = m
            except Exception:
                continue

        macro, assets = {}, {}
        for label, sym in label_to_sym.items():
            m = metrics_by_symbol.get(sym)
            if not m:
                continue
            (macro if label in MACRO_BASKET else assets)[label] = m

        regime = self._regime(macro)
        for a in list(assets.keys()):
            assets[a]["bias"] = self._blended_bias(a, assets[a], macro, regime)
        # Assets with no Yahoo mapping still get a macro-only view lazily in get_asset_context.

        fresh = bool(metrics_by_symbol) and (time.time() - newest_bar_ts) < STALE_AFTER_SECONDS
        self._snapshot = {
            "ts": time.time(), "fresh": fresh, "regime": regime,
            "macro": macro, "assets": assets,
            "summary": self._summary_text(regime, macro, assets),
        }
        await self._publish()
        return self._snapshot

    @staticmethod
    def _regime(macro: Dict[str, Dict[str, float]]) -> str:
        vix = macro.get("VIX", {})
        spx = macro.get("SPX_FUT", {})
        score = 0.0
        if vix:
            score += -1.0 if vix.get("ret_60m_bps", 0.0) > 150 or vix.get("momentum", 0.0) > 0.5 else 0.0
            score += 0.5 if vix.get("momentum", 0.0) < -0.3 else 0.0
        if spx:
            score += 1.0 if spx.get("momentum", 0.0) > 0.3 else (-1.0 if spx.get("momentum", 0.0) < -0.3 else 0.0)
        return "risk_on" if score >= 1.0 else ("risk_off" if score <= -1.0 else "neutral")

    @staticmethod
    def _blended_bias(asset: str, own: Dict[str, float], macro: Dict[str, Dict[str, float]], regime: str) -> float:
        """-1..+1. Positive favours LONG the broker asset."""
        own_mom = float(own.get("momentum", 0.0))
        betas = FACTOR_BETAS.get(asset.upper(), {})
        macro_term, wsum = 0.0, 0.0
        for factor, beta in betas.items():
            fm = macro.get(factor)
            if fm:
                macro_term += beta * float(fm.get("momentum", 0.0))
                wsum += abs(beta)
        macro_term = macro_term / wsum if wsum else 0.0
        risk_term = {"risk_on": 0.3, "risk_off": -0.3}.get(regime, 0.0) if asset.upper() in ("BTCUSD", "ETHUSD", "US500", "NAS100", "USTEC", "US30", "AUDUSD", "NZDUSD") else 0.0
        return round(_clamp(0.6 * own_mom + 0.3 * macro_term + 0.1 * risk_term), 3)

    @staticmethod
    def _summary_text(regime: str, macro: Dict, assets: Dict) -> str:
        parts = [f"regime={regime}"]
        for k in ("DXY", "VIX", "US10Y", "SPX_FUT"):
            m = macro.get(k)
            if m:
                parts.append(f"{k} {m['trend']} {m['ret_15m_bps']:+.0f}bps/15m")
        for a, m in assets.items():
            parts.append(f"{a} {m['trend']} bias={m.get('bias', 0):+.2f}")
        return "; ".join(parts)

    async def _publish(self) -> None:
        snap = self._snapshot
        if self._redis is not None:
            try:
                await self._redis.set("ctx:yahoo", json.dumps(snap), ex=STALE_AFTER_SECONDS * 2)
            except Exception as exc:
                log.debug("Redis publish failed: %s", exc)
        if self._broadcast is not None:
            try:
                await self._broadcast({"type": "MARKET_CONTEXT", "fresh": snap["fresh"], "regime": snap["regime"],
                                       "summary": snap.get("summary", ""), "assets": {k: v.get("bias") for k, v in snap["assets"].items()},
                                       "timestamp": snap["ts"]})
            except Exception:
                pass
        self._poll_count += 1
        if self._pool_getter and self._poll_count % SNAPSHOT_EVERY_N_POLLS == 0:
            try:
                pool = self._pool_getter()
                async with pool.acquire() as conn:
                    await conn.execute("INSERT INTO market_context_snapshots (payload) VALUES ($1::jsonb)", json.dumps(snap))
            except Exception as exc:
                log.debug("snapshot persist failed: %s", exc)

    async def _loop(self) -> None:
        await asyncio.sleep(2)
        while True:
            try:
                await self.refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("market context loop error")
            await asyncio.sleep(POLL_SECONDS)

    # ---------------- optional live websocket -----------------
    async def _ws_loop(self) -> None:
        """Keeps self._live_prices fresh between polls. Reconnects with backoff."""
        backoff = 5
        while True:
            try:
                import yfinance as yf
                symbols = sorted(set(self._yahoo_symbols().values()))

                def handler(msg: dict) -> None:
                    try:
                        sym, px = msg.get("id"), msg.get("price")
                        if sym and px:
                            self._live_prices[sym] = float(px)
                    except Exception:
                        pass

                async with yf.AsyncWebSocket(verbose=False) as ws:
                    await ws.subscribe(symbols)
                    backoff = 5
                    await ws.listen(handler)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Yahoo websocket dropped (%s); retry in %ss", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(120, backoff * 2)

    # ---------------- consumer API -----------------
    def snapshot(self) -> Dict[str, Any]:
        snap = dict(self._snapshot)
        snap["fresh"] = bool(snap.get("ts")) and (time.time() - snap["ts"]) < STALE_AFTER_SECONDS and snap.get("fresh", False)
        return snap

    def get_asset_context(self, asset: str) -> Dict[str, Any]:
        """Return a per-asset freshness decision; stale inputs always become no-opinion."""
        self.track_asset(asset)
        snap = self.snapshot()
        now = time.time()
        a = snap.get("assets", {}).get(asset.upper())
        macro_all = snap.get("macro", {})
        fresh_macro = {
            key: value for key, value in macro_all.items()
            if value.get("bar_age_s") is not None
            and float(value.get("bar_age_s", STALE_AFTER_SECONDS + 1)) <= STALE_AFTER_SECONDS
        }
        if a:
            asset_fresh = bool(snap["fresh"]) and a.get("bar_age_s") is not None and float(a["bar_age_s"]) <= STALE_AFTER_SECONDS
            bias = float(a.get("bias", 0.0))
        else:  # Unmapped asset: macro-only, using only individually fresh macro series.
            asset_fresh = bool(snap["fresh"]) and bool(fresh_macro)
            bias = self._blended_bias(asset, {"momentum": 0.0}, fresh_macro, snap.get("regime", "unknown"))
        return {
            "fresh": bool(asset_fresh),
            "regime": snap.get("regime", "unknown") if asset_fresh else "unknown",
            "bias": max(-1.0, min(1.0, bias)) if asset_fresh else 0.0,
            "asset": a or {}, "summary": snap.get("summary", "") if asset_fresh else "Market context is stale for this asset.",
            "macro": {k: {"trend": v.get("trend"), "ret_15m_bps": v.get("ret_15m_bps"), "bar_age_s": v.get("bar_age_s")} for k, v in fresh_macro.items()},
            "snapshot_age_s": round(now - float(snap.get("ts") or 0.0), 1) if snap.get("ts") else None,
        }


service = MarketContextService()
