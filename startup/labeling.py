# startup/labeling.py
"""
startup/labeling.py  (v11.09)
=============================
Path-aware labels for supervised learning from FULL bar history.

Why
---
v11.08 trained only on CLOSED TRADES (and only live ones). That is
  * survivorship / selection bias  - the model never sees what happened when it
    did NOT trade (missed trades, HOLD decisions),
  * far too little data           - 0 trades => 0 training rows => no model,
  * leaky                         - features were sampled at trade CLOSE.
Here every closed bar is labelled, for BOTH directions, by simulating the EA's
own exit rules forward in time (triple-barrier method, Lopez de Prado AFML ch.3):
    upper barrier  = take-profit  (entry +/- atr*tp_mult)
    lower barrier  = stop-loss    (entry -/+ atr*sl_mult)
    vertical bar   = max holding time (EA InpMaxHoldMinutes)
Outcome is expressed in R-multiples (profit / initial risk), so assets and lot
sizes are comparable.

Conventions (must match how the EA posts rows - see PostTick() in the EA):
  * Row k = decision at open of bar0; `bid`/`ask` = price at decision time;
    `close`,`bar_high`,`bar_low` = the bar that JUST closed (bar1).
  * Therefore the price path AFTER entry at row k is described by rows k+1..k+H
    (row k+1's high/low is the bar that started at row k's time).
  * BUY enters at ask, exits at bid;  SELL enters at bid, exits at ask
    (ask path ~= bid high/low + spread).
  * If TP and SL are both touched inside one bar the SL wins (conservative).
  * Labels spanning a data gap (weekend / EA off) are invalid (NaN).
Not modelled: SAR trailing, profit-peak giveback, ML-reversal exit, slippage,
commission. Simulated R is therefore a PROXY; real closed trades (which carry
true R) override the simulated label when available (see trainer.py).
"""
from __future__ import annotations

import math
from typing import Optional, Tuple

import numpy as np
import pandas as pd


def _epoch_seconds(ts: pd.Series) -> np.ndarray:
    # unit-safe (pandas 3 may infer s/ms/us/ns resolution, so never use astype("int64") // 1e9)
    t = pd.to_datetime(ts, utc=True)
    return ((t - pd.Timestamp("1970-01-01", tz="UTC")) / pd.Timedelta(seconds=1)).to_numpy(dtype="float64")


def infer_tf_seconds(df: pd.DataFrame, default: int = 300) -> int:
    if "tf_seconds" in df.columns:
        v = pd.to_numeric(df["tf_seconds"], errors="coerce").dropna()
        v = v[v > 0]
        if len(v):
            return int(v.median())
    if "ts" in df.columns and len(df) > 5:
        d = np.diff(_epoch_seconds(df["ts"]))
        d = d[d > 0]
        if len(d):
            return int(np.median(d))
    return default


def max_hold_bars(tf_seconds: int, max_hold_minutes: float = 60.0) -> int:
    return max(2, int(math.ceil(max_hold_minutes * 60.0 / max(1, tf_seconds))))


def label_frame(df: pd.DataFrame, sl_mult: float, tp_mult: float, hold_bars: int,
                side: int) -> pd.DataFrame:
    """
    Triple-barrier labels for every row of `df` (time-sorted bar snapshots).
    Returns DataFrame aligned with df.index:
        R, exit_pos, bars_held, exit_reason(tp/sl/time), mfe_r, mae_r, valid
    Rows whose future is not yet known (last `hold_bars` rows without a barrier
    touch) get valid=False and R=NaN.
    """
    n = len(df)
    ask = df["ask"].to_numpy("float64")
    bid = df["bid"].to_numpy("float64")
    hi = df["bar_high"].to_numpy("float64")
    lo = df["bar_low"].to_numpy("float64")
    cl = df["close"].to_numpy("float64")
    atr = df["atr"].to_numpy("float64")
    t = _epoch_seconds(df["ts"])
    spr = np.maximum(ask - bid, 0.0)

    entry = ask if side > 0 else bid
    sl = atr * float(sl_mult)
    tp = atr * float(tp_mult)

    R = np.full(n, np.nan)
    exit_pos = np.full(n, -1, dtype="int64")
    reason = np.full(n, "", dtype=object)
    mfe = np.zeros(n)
    mae = np.zeros(n)

    ok = np.isfinite(entry) & np.isfinite(sl) & np.isfinite(tp) & (sl > 0) & (tp > 0)
    done = ~ok                      # rows that can never be labelled
    tf = infer_tf_seconds(df)
    max_span = hold_bars * tf * 1.5 + tf
    H = int(hold_bars)

    for h in range(1, H + 1):
        m = n - h
        if m <= 0:
            break
        act = ~done[:m]
        if not act.any():
            break
        j_hi, j_lo, j_cl, j_sp = hi[h:], lo[h:], cl[h:], spr[h:]
        e, s_, t_ = entry[:m], sl[:m], tp[:m]
        finite = np.isfinite(j_hi) & np.isfinite(j_lo) & np.isfinite(j_cl)
        gap = (t[h:] - t[:m]) > max_span

        if side > 0:
            fav = (j_hi - e) / s_
            adv = (e - j_lo) / s_
            hit_tp = j_hi >= e + t_
            hit_sl = j_lo <= e - s_
        else:
            fav = (e - (j_lo + j_sp)) / s_
            adv = ((j_hi + j_sp) - e) / s_
            hit_tp = (j_lo + j_sp) <= e - t_
            hit_sl = (j_hi + j_sp) >= e + s_

        # invalidate rows crossing a gap or with bad data (never label them)
        bad = act & (gap | ~finite)
        done[:m] |= bad

        live = act & ~bad
        mfe[:m] = np.where(live, np.maximum(mfe[:m], np.nan_to_num(fav, nan=0.0)), mfe[:m])
        mae[:m] = np.where(live, np.maximum(mae[:m], np.nan_to_num(adv, nan=0.0)), mae[:m])

        sl_first = live & hit_sl                       # includes "both in same bar"
        tp_only = live & hit_tp & ~hit_sl
        idx = np.arange(m)

        R[idx[sl_first]] = -1.0
        reason[idx[sl_first]] = "sl"
        R[idx[tp_only]] = (t_ / s_)[tp_only]
        reason[idx[tp_only]] = "tp"
        resolved = sl_first | tp_only
        exit_pos[idx[resolved]] = (idx + h)[resolved]
        done[:m] |= resolved

        if h == H:
            tex = live & ~resolved
            if side > 0:
                r_t = (j_cl - e) / s_
            else:
                r_t = (e - (j_cl + j_sp)) / s_
            R[idx[tex]] = r_t[tex]
            reason[idx[tex]] = "time"
            exit_pos[idx[tex]] = (idx + h)[tex]
            done[:m] |= tex

    valid = np.isfinite(R) & (exit_pos >= 0)
    out = pd.DataFrame({
        "R": R,
        "exit_pos": exit_pos,
        "bars_held": np.where(valid, exit_pos - np.arange(n), 0),
        "exit_reason": reason,
        "mfe_r": mfe,
        "mae_r": mae,
        "valid": valid,
    }, index=df.index)
    return out


def label_single_path(side: str, entry_price: float, sl_dist: float, tp_dist: float,
                      rows: pd.DataFrame, hold_bars: int, max_span_s: float,
                      t0_epoch: float) -> Tuple[str, float]:
    """
    Label ONE signal (used by the missed-trade evaluator). `rows` are the bar
    snapshots strictly AFTER the signal time, ascending. Returns
    (outcome, r) with outcome in would_have_won/would_have_lost/inconclusive.
    """
    if rows is None or len(rows) == 0 or sl_dist <= 0 or tp_dist <= 0:
        return "inconclusive", 0.0
    hi = rows["bar_high"].to_numpy("float64")
    lo = rows["bar_low"].to_numpy("float64")
    cl = rows["close"].to_numpy("float64")
    sp = np.maximum(rows["ask"].to_numpy("float64") - rows["bid"].to_numpy("float64"), 0.0)
    t = _epoch_seconds(rows["ts"])
    buy = str(side).upper() == "BUY"
    last_r = 0.0
    for i in range(min(len(rows), hold_bars)):
        if not (np.isfinite(hi[i]) and np.isfinite(lo[i])):
            continue
        if t[i] - t0_epoch > max_span_s:
            return "inconclusive", 0.0
        if buy:
            hit_tp = hi[i] >= entry_price + tp_dist
            hit_sl = lo[i] <= entry_price - sl_dist
            last_r = (cl[i] - entry_price) / sl_dist
        else:
            hit_tp = lo[i] + sp[i] <= entry_price - tp_dist
            hit_sl = hi[i] + sp[i] >= entry_price + sl_dist
            last_r = (entry_price - (cl[i] + sp[i])) / sl_dist
        if hit_sl:
            return "would_have_lost", -1.0
        if hit_tp:
            return "would_have_won", round(tp_dist / sl_dist, 3)
    if len(rows) < hold_bars:
        return "inconclusive", round(max(-1.0, min(tp_dist / sl_dist, last_r)), 3)
    return ("would_have_won" if last_r > 0 else "would_have_lost"), round(
        max(-1.0, min(tp_dist / sl_dist, last_r)), 3)


def uniqueness_weights(entry_pos: np.ndarray, exit_pos: np.ndarray, n: int) -> np.ndarray:
    """
    Average uniqueness (AFML ch.4): labels whose holding windows overlap many
    others carry less independent information. weight_i = mean_t(1 / c_t) over
    t in [entry_i, exit_i], c_t = number of labels alive at bar t.
    """
    entry_pos = np.asarray(entry_pos, dtype="int64")
    exit_pos = np.asarray(exit_pos, dtype="int64")
    if len(entry_pos) == 0:
        return np.array([])
    diff = np.zeros(n + 2)
    np.add.at(diff, entry_pos, 1.0)
    np.add.at(diff, exit_pos + 1, -1.0)
    conc = np.cumsum(diff)[:n]
    inv = np.where(conc > 0, 1.0 / np.maximum(conc, 1.0), 0.0)
    cs = np.concatenate([[0.0], np.cumsum(inv)])
    span = (exit_pos - entry_pos + 1).astype("float64")
    return (cs[exit_pos + 1] - cs[entry_pos]) / span


def purged_time_split(t_entry: np.ndarray, t_exit: np.ndarray, val_frac: float = 0.25,
                      embargo_s: float = 0.0) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    Walk-forward split with PURGING: no training label may overlap the
    validation period. Returns (train_mask, val_mask, cut_time).
    """
    t_entry = np.asarray(t_entry, dtype="float64")
    t_exit = np.asarray(t_exit, dtype="float64")
    cut = float(np.quantile(t_entry, 1.0 - val_frac))
    train = t_exit < (cut - embargo_s)
    val = t_entry >= cut
    return train, val, cut


def recency_weights(t_entry: np.ndarray, half_life_days: float) -> np.ndarray:
    """Exponential decay: newest rows weight 1.0, rows `half_life_days` older 0.5."""
    if half_life_days <= 0 or len(t_entry) == 0:
        return np.ones(len(t_entry))
    age_days = (np.max(t_entry) - t_entry) / 86400.0
    return np.power(0.5, age_days / half_life_days)


def compute_recency_weighted_labels(
    df: pd.DataFrame,
    label_col: str = "target",
    half_life: int = 1000,
    weight_col_name: str = "recency_weight",
) -> pd.DataFrame:
    """
    Computes exponential decay weights for historical data to prioritize recent
    market behavior in ML model training.

    Args:
        df: The dataset containing features and labels.
        label_col: The column containing the target variable to be modeled.
        half_life: The number of periods/rows it takes for the weight to halve.
        weight_col_name: The name of the output weight column.

    Returns:
        pd.DataFrame: The dataframe with the new recency weight column appended.
    """
    if df.empty:
        return df

    # Create an array of sequential steps from the oldest (0) to the newest (N)
    steps = np.arange(len(df))

    # Calculate exponential decay weights based on the half-life
    decay_rate = np.log(2) / half_life
    weights = np.exp(decay_rate * (steps - steps.max()))

    # Normalize weights so the maximum weight for the most recent observation is 1.0
    df = df.copy()
    df[weight_col_name] = weights / weights.max()

    return df
