# startup/jobs/trade_guardian.py
"""
Trade Guardian (v12): automated cancellation of trades that are trending into a loss.

The EA reports each open position every few seconds (POST /guardian/evaluate) with
its current R-multiple (profit expressed in units of the initial stop distance).
The Guardian keeps a short history per ticket and answers with one of:

    HOLD      - keep the trade
    CLOSE     - close it now (loss trending worse / thesis invalidated / profit give-back)
    MOVE_SL   - ratchet the stop to break-even once the trade has paid 1R

Decision rules (all thresholds env-tunable, expressed in R):

  hard_stop        r <= -GUARDIAN_HARD_R                       (gap / slippage protection)
  loss_acceleration r <= -GUARDIAN_SOFT_R and R-slope worsening and >=1 independent "against" vote
  thesis_invalidated r < 0 and >=2 independent "against" votes
  time_underwater  held >= 50% of max-hold, r <= -GUARDIAN_TIME_R and not improving
  profit_giveback  peak_r >= GUARDIAN_PEAK_R and gave back >= GUARDIAN_GIVEBACK of it and >=1 against vote
  breakeven        r >= GUARDIAN_BE_R -> MOVE_SL to open +/- 0.1 * initial stop distance

"Against votes" (independent evidence the move is going the wrong way):
  1. orchestrator micro-trend opposes the position (strength >= 0.4)
  2. Tier-2 ML action opposes the position
  3. Yahoo market-context bias opposes the position (|bias| >= 0.3 and data fresh)
  4. EA-reported momentum flip (AC against position)

The EA ALSO carries a local fail-safe (see KenjinGuardian.mqh) that does not depend
on this service, so a dead orchestrator can never leave a losing trade unmanaged.
"""
from __future__ import annotations

import logging
import os
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

import numpy as np

log = logging.getLogger("trade_guardian")

HARD_R = float(os.environ.get("GUARDIAN_HARD_R", "0.85"))
SOFT_R = float(os.environ.get("GUARDIAN_SOFT_R", "0.35"))
SLOPE_SOFT = float(os.environ.get("GUARDIAN_SLOPE_SOFT_R_PER_MIN", "-0.15"))
SLOPE_NOVOTE = float(os.environ.get("GUARDIAN_SLOPE_NOVOTE_R_PER_MIN", "-0.25"))
NOVOTE_R = float(os.environ.get("GUARDIAN_NOVOTE_R", "0.55"))
TIME_R = float(os.environ.get("GUARDIAN_TIME_R", "0.25"))
PEAK_R = float(os.environ.get("GUARDIAN_PEAK_R", "0.8"))
GIVEBACK = float(os.environ.get("GUARDIAN_GIVEBACK", "0.40"))
BE_R = float(os.environ.get("GUARDIAN_BE_R", "1.0"))
ENABLED = os.environ.get("GUARDIAN_ENABLED", "1").lower() in ("1", "true", "yes")
HISTORY_LEN = 80
HISTORY_WINDOW_S = 120.0
PRUNE_AFTER_S = 900.0

_history: Dict[int, Deque[Tuple[float, float]]] = {}
_last_seen: Dict[int, float] = {}
_peak_r: Dict[int, float] = {}


def _norm_dir(s: Optional[str]) -> int:
    """+1 bullish, -1 bearish, 0 neutral. Accepts BUY/SELL and BULLISH/BEARISH."""
    s = (s or "").upper()
    if s in ("BUY", "BULLISH", "UP", "LONG"):
        return 1
    if s in ("SELL", "BEARISH", "DOWN", "SHORT"):
        return -1
    return 0


def _slope_r_per_min(ticket: int, now: float) -> Optional[float]:
    h = _history.get(ticket)
    if not h:
        return None
    pts = [(t, r) for t, r in h if now - t <= HISTORY_WINDOW_S]
    if len(pts) < 4 or (pts[-1][0] - pts[0][0]) < 15.0:
        return None
    t = np.array([p[0] for p in pts]) - pts[0][0]
    r = np.array([p[1] for p in pts])
    slope_per_s = float(np.polyfit(t, r, 1)[0])
    return slope_per_s * 60.0


def _prune(now: float) -> None:
    for tk in [k for k, ts in _last_seen.items() if now - ts > PRUNE_AFTER_S]:
        _history.pop(tk, None)
        _last_seen.pop(tk, None)
        _peak_r.pop(tk, None)


def forget(ticket: int) -> None:
    _history.pop(ticket, None)
    _last_seen.pop(ticket, None)
    _peak_r.pop(ticket, None)


def evaluate(report: Dict[str, Any], tier2_action: str = "HOLD", micro_trend: str = "NEUTRAL",
             micro_strength: float = 0.0, market_bias: float = 0.0, market_fresh: bool = False) -> Dict[str, Any]:
    """Pure decision function (no I/O) so it is unit-testable."""
    now = time.time()
    ticket = int(report.get("ticket") or 0)
    side = str(report.get("side", "")).upper()
    pos_dir = 1 if side == "BUY" else -1
    r = float(report.get("r_multiple", 0.0))
    open_s = float(report.get("open_seconds", 0.0))
    max_hold_s = float(report.get("max_hold_seconds", 3600.0)) or 3600.0
    sl_dist = float(report.get("initial_sl_distance", 0.0))
    open_price = float(report.get("open_price", 0.0))
    cur_sl = float(report.get("sl", 0.0))
    momentum_against = bool(report.get("momentum_against", False))

    base = {"action": "HOLD", "reason": "ok", "new_sl": 0.0, "r_multiple": round(r, 3)}
    if not ENABLED:
        base["reason"] = "guardian_disabled"
        return base

    _prune(now)
    _last_seen[ticket] = now
    h = _history.setdefault(ticket, deque(maxlen=HISTORY_LEN))
    h.append((now, r))
    peak = max(_peak_r.get(ticket, 0.0), float(report.get("peak_r", 0.0)), r)
    _peak_r[ticket] = peak
    slope = _slope_r_per_min(ticket, now)

    # ---- independent "against" votes ----
    votes, vote_names = 0, []
    if _norm_dir(micro_trend) == -pos_dir and micro_strength >= 0.4:
        votes += 1; vote_names.append("micro_trend")
    if _norm_dir(tier2_action) == -pos_dir:
        votes += 1; vote_names.append("tier2")
    if market_fresh and abs(market_bias) >= 0.3 and (1 if market_bias > 0 else -1) == -pos_dir:
        votes += 1; vote_names.append("yahoo_context")
    if momentum_against:
        votes += 1; vote_names.append("ea_momentum")

    base.update({"peak_r": round(peak, 3), "slope_r_min": None if slope is None else round(slope, 3),
                 "against_votes": votes, "votes": vote_names, "market_bias": round(market_bias, 3)})

    def close(reason: str) -> Dict[str, Any]:
        base.update({"action": "CLOSE", "reason": reason})
        return base

    # 1. hard stop
    if r <= -HARD_R:
        return close("hard_stop")

    # 2. loss acceleration
    if slope is not None:
        if r <= -SOFT_R and slope <= SLOPE_SOFT and votes >= 1:
            return close("loss_acceleration")
        if r <= -NOVOTE_R and slope <= SLOPE_NOVOTE:
            return close("loss_acceleration_no_vote")

    # 3. thesis invalidated
    if r < -0.10 and votes >= 2:
        return close("thesis_invalidated")

    # 4. dead money underwater
    if open_s >= 0.5 * max_hold_s and r <= -TIME_R and (slope is None or slope <= 0.0):
        return close("time_underwater")

    # 5. profit give-back
    if peak >= PEAK_R and r > 0 and (peak - r) / peak >= GIVEBACK and votes >= 1:
        return close("profit_giveback")

    # 6. break-even ratchet
    if r >= BE_R and sl_dist > 0 and open_price > 0:
        target = open_price + pos_dir * 0.1 * sl_dist
        improves = (cur_sl == 0.0) or (pos_dir > 0 and target > cur_sl) or (pos_dir < 0 and target < cur_sl)
        if improves:
            base.update({"action": "MOVE_SL", "reason": "breakeven_ratchet", "new_sl": round(target, 6)})
            return base
    return base
