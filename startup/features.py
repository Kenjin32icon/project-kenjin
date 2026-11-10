"""
startup/features.py
=============================
SINGLE SOURCE OF TRUTH for feature engineering.
"""
from __future__ import annotations

import os
from typing import Iterable

import numpy as np
import pandas as pd

# --- EA constants mirrored on the server -------------------------------
W_TEMA = float(os.environ.get("EA_W_TEMA", "1.0"))
W_AC = float(os.environ.get("EA_W_AC", "0.8"))
W_SAR = float(os.environ.get("EA_W_SAR", "1.0"))
VOL_WEIGHT = float(os.environ.get("EA_VOL_WEIGHT", "0.6"))
VOL_AVG_BARS = 18          # EA: InpVolMaPeriod(20) -> bars 2..19 => 18 bars

RAW_NUMERIC_COLS = [
    "bid", "ask", "close", "bar_high", "bar_low", "atr", "tick_volume",
    "rsi", "tema", "ac", "sar", "adx", "ma10", "ma20", "ma50", "ma100", "ma200",
    "tf_seconds",
]

# Directional features flip sign with trade side (BUY=+1, SELL=-1).
DIRECTIONAL = [
    "local_score", "ma_spread_n", "ma_spread_d_n", "close_vs_ma200_n",
    "rsi_dev", "rsi_delta", "vel_short", "vel_long", "tema_dist", "sar_dist", "ac_n",
]
NEUTRAL = ["adx", "rvol", "vol_bps", "atr_ratio", "spread_atr", "hour_sin", "hour_cos"]
FEATURE_COLS = [f"{c}_s" for c in DIRECTIONAL] + NEUTRAL
FEATURE_VERSION = 2


def coerce_frame(df) -> pd.DataFrame:
    """
    Coerces input from various backends (pandas, Polars, PySpark, PyArrow, dicts, or arrays)
    into a standardized pandas DataFrame, enforces raw numeric schema types, and formats 
    temporal ordering.
    """
    if df is None:
        return pd.DataFrame()

    out = None

    # 1. Check for native pandas DataFrame
    if isinstance(df, pd.DataFrame):
        out = df.copy()
    # 2. Polars DataFrame or Arrow Table
    elif hasattr(df, 'to_pandas'):
        out = df.to_pandas().copy()
    # 3. PySpark DataFrame
    elif hasattr(df, 'toPandas'):
        out = df.toPandas().copy()
    # 4. Cloneable objects with .to_pandas()
    elif hasattr(df, 'clone'):
        try:
            cloned_df = df.clone()
            if hasattr(cloned_df, 'to_pandas'):
                out = cloned_df.to_pandas()
        except Exception:
            pass

    # 5. Fallback for dicts, arrays, or custom objects
    if out is None:
        try:
            out = pd.DataFrame(df).copy()
        except Exception:
            return pd.DataFrame()

    if out.empty:
        return pd.DataFrame()

    # 6. Schema & Numeric Column Enforcement
    for c in RAW_NUMERIC_COLS:
        if c not in out.columns:
            out[c] = np.nan
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")

    # 7. Timestamp Standardization, Deduplication & Sorting
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
        out = out.dropna(subset=["ts"]).sort_values("ts")
        out = out.drop_duplicates(subset=["ts"], keep="last")

    return out.reset_index(drop=True)


def local_score_from_frame(f: pd.DataFrame) -> pd.Series:
    """
    Vectorised replica of MAPSAR EA ComputeSignalScore(). Row i uses row i-1
    as the "previous bar" for slope votes.
    """
    px, tema, ac, sar = f["px"], f["tema"], f["ac"], f["sar"]
    tema_prev, ac_prev = tema.shift(1), ac.shift(1)

    tema_vote = np.where((px > tema) & (tema > tema_prev), 1.0,
                         np.where((px < tema) & (tema < tema_prev), -1.0, 0.0))
    ac_vote = np.where((ac > 0) & (ac > ac_prev), 1.0,
                       np.where((ac < 0) & (ac < ac_prev), -1.0, 0.0))
    sar_vote = np.where(sar < px, 1.0, -1.0)

    vol_avg = f["tick_volume"].shift(1).rolling(VOL_AVG_BARS, min_periods=5).mean()
    ratio = np.where(vol_avg > 0, f["tick_volume"] / vol_avg, 1.0)
    up, down = 1.0 + 0.15 * VOL_WEIGHT, 1.0 - 0.20 * VOL_WEIGHT
    vol_mult = np.where(ratio >= 1.2, up, np.where(ratio < 0.7, down, 1.0))

    tw = W_TEMA + W_AC + W_SAR
    raw = (tema_vote * W_TEMA + ac_vote * W_AC + sar_vote * W_SAR) / tw
    score = np.clip(raw * vol_mult, -1.0, 1.0)
    bad = ~np.isfinite(px.to_numpy()) | ~np.isfinite(tema.to_numpy()) | ~np.isfinite(sar.to_numpy())
    score = np.where(bad, 0.0, score)
    return pd.Series(score, index=f.index, name="local_score")


def base_features(df: pd.DataFrame) -> pd.DataFrame:
    """Side-agnostic engineered frame (one row per input row, same order)."""
    f = coerce_frame(df)
    if f.empty:
        return f

    mid = (f["bid"] + f["ask"]) / 2.0
    f["px"] = f["close"].where(f["close"] > 0, mid)

    atr_fb = f["px"].diff().abs().rolling(14, min_periods=3).mean()
    atr = f["atr"].where(f["atr"] > 0, atr_fb)
    atr = atr.where(atr > 0, f["px"].abs() * 1e-4)
    f["atr_f"] = atr.replace(0, np.nan).ffill().fillna(1e-6)

    a = f["atr_f"]
    f["ma_spread_n"] = (f["ma10"] - f["ma50"]) / a
    f["ma_spread_d_n"] = f["ma_spread_n"] - f["ma_spread_n"].shift(5)
    ma200 = f["ma200"].where(f["ma200"] > 0)
    f["close_vs_ma200_n"] = (f["px"] - ma200) / a
    f["rsi_dev"] = f["rsi"] - 50.0
    f["rsi_delta"] = f["rsi"] - f["rsi"].shift(3)
    f["vel_short"] = (f["px"] - f["px"].shift(3)) / a
    f["vel_long"] = (f["px"] - f["px"].shift(12)) / a
    f["tema_dist"] = (f["px"] - f["tema"]) / a
    f["sar_dist"] = (f["px"] - f["sar"]) / a
    f["ac_n"] = f["ac"] / a

    vol_avg = f["tick_volume"].shift(1).rolling(VOL_AVG_BARS, min_periods=5).mean()
    f["rvol"] = np.where(vol_avg > 0, f["tick_volume"] / vol_avg, 1.0)
    f["vol_bps"] = a / f["px"].abs().replace(0, np.nan) * 1e4
    f["atr_ratio"] = a / a.rolling(50, min_periods=10).mean()
    f["spread_atr"] = (f["ask"] - f["bid"]).clip(lower=0) / a

    ts = pd.to_datetime(f["ts"], utc=True)
    hour = ts.dt.hour + ts.dt.minute / 60.0
    f["hour_sin"] = np.sin(2 * np.pi * hour / 24.0)
    f["hour_cos"] = np.cos(2 * np.pi * hour / 24.0)

    f["local_score"] = local_score_from_frame(f)
    f["price_velocity_1m"] = f["vel_short"]
    f["price_velocity_5m"] = f["vel_long"]
    f["ma_spread_delta"] = f["ma_spread_d_n"]
    f["volatility_regime"] = f["vol_bps"]
    return f


def side_matrix(base: pd.DataFrame, side: int) -> pd.DataFrame:
    """Feature matrix for a hypothetical trade in direction `side` (+1 BUY, -1 SELL)."""
    out = pd.DataFrame(index=base.index)
    for c in DIRECTIONAL:
        out[f"{c}_s"] = base[c] * float(side)
    for c in NEUTRAL:
        out[c] = base[c]
    out = out[FEATURE_COLS].replace([np.inf, -np.inf], np.nan)
    out = out.clip(lower=-15.0, upper=15.0)
    out["rvol"] = base["rvol"].clip(0, 15)
    out["vol_bps"] = base["vol_bps"].clip(0, 500)
    return out.fillna(0.0).astype("float64")


def latest_side_features(base: pd.DataFrame) -> dict:
    """{+1: 1-row DataFrame, -1: 1-row DataFrame} for the newest row."""
    last = base.iloc[[-1]]
    return {+1: side_matrix(last, +1), -1: side_matrix(last, -1)}


def feature_dict(row: pd.Series, cols: Iterable[str] = FEATURE_COLS) -> dict:
    return {c: float(row[c]) for c in cols}
