> **Release note:** This file preserves the original v12 patch rationale for audit and traceability. The changes described here have been integrated into the v12 release tree where applicable. It is not a list of steps that must be applied again.

# Exact edits to the remaining v11.09 files

Files not listed here (hour_scheduler, confidence_calibration, feature_pull, features, labeling, auth, ws_manager,
lifeline_engine, redis_client, electron-app/*) are unchanged.

---------------------------------------------------------------------
## A. MQL5 EA: `MAPSAR_ProfitEngine_v11_09.mq5` -> save as `MAPSAR_ProfitEngine_v12_00.mq5`
---------------------------------------------------------------------

### A1. Version + include (put the include right above `// EVENT HANDLERS`)
```mql5
#property version   "12.00"
...
#include "KenjinGuardian.mqh"      // copy KenjinGuardian.mqh into MQL5/Experts/ next to the EA
```

### A2. `HasOpenPosition` - the old code read `posInfo.Magic()` without selecting the position into `posInfo`
```mql5
bool HasOpenPosition(long &type)
  {
   if(posInfo.Select(_Symbol) && posInfo.Magic() == InpMagicNumber)
     { type = posInfo.PositionType(); return(true); }
   return(false);
  }
```

### A3. Schedules are UTC on the server but the EA compared them with broker time (`TimeCurrent`)
Replace `TimeToStruct(TimeCurrent(), dt)` with `TimeToStruct(TimeGMT(), dt)` in **`IsWithinScheduledHours`** and the
`dt.hour` checks of **`IsWithinTradingSession`** (the Friday cutoff may stay on broker time). Same change in
`MAPSAR_TrendVisualizer.mq5::IsLowTrustBar`.

### A4. Micro-trend vocabulary (server now always returns BUY / SELL / NEUTRAL)
In `CheckInFlightMicroTrendReversal` replace `"BEARISH"` with `"SELL"` and `"BULLISH"` with `"BUY"`.
(Before, this early-close never fired because the server never sent BULLISH/BEARISH.)

### A5. `FetchStrategyParams`: ask for the account type and read the v12 fields
```mql5
string url = StringFormat("%s/strategy_params?asset=%s&account_type=%s", InpOrchestratorURL, asset, IsDemoAccount() ? "demo" : "live");
...
// new globals (top of file):  bool db_trading_halted=false; double db_market_bias=0.0; string db_market_regime="unknown";
bool th; if(JsonGetBool(body, "trading_halted", th)) db_trading_halted = th;
if(JsonGetNumber(body, "market_bias", v)) db_market_bias = v;
JsonGetString(body, "market_regime", db_market_regime);
```

### A6. `NewEntriesAllowed()` - honour the operator halt / kill switch (first line of the function)
```mql5
if(db_trading_halted) { lastBlockReason = "operator_manual_halt"; return(false); }
```

### A7. `PostAccountSnapshot` - send the day-start balance the gatekeeper needs
Add `\"day_start_balance\":%.2f,` to the JSON format string and `dayStartEquity` to the argument list (after `dayLossPct`).

### A8. Timer + dispatch
```mql5
// OnInit():   EventSetTimer(InpGuardianSeconds);          // was InpHeartbeatSeconds (30s) - far too slow to protect a trade
// OnTimer():
void OnTimer()
  {
   RefreshDailyCounter();
   UpdateDrawdownState();
   CheckFloatingLossKillSwitch();
   ManageProfitPeakExit();
   ManageEarlyLossCut();
   ManageMaxHoldSafetyNet();
   ManageTrailing();
   RefreshParamsIfDue();
   GuardianTick();                                    // <-- v12 loss-trend auto-cancellation
   if(SnapshotDue()) PostAccountSnapshot();           // throttled
   FlushPostQueue(10);
   SaveEAState();
  }
```
Remove the old `CheckInFlightMicroTrendReversal(params)` call from `OnTimer` and `OnTick` (the Guardian supersedes it and
combines micro-trend with Tier-2, Yahoo context and the R-slope instead of a single 0.6-strength flip).

### A9. Allow WebRequest
MT5 -> Tools -> Options -> Expert Advisors -> *Allow WebRequest for listed URL* -> add `http://127.0.0.1:8000`.

---------------------------------------------------------------------
## B. `startup/jobs/gatekeeper.py`
---------------------------------------------------------------------

1. `risk_incidents` insert used columns that do not exist (`incident_type`, `day_loss_pct`, `cap`). Replace with:
```python
await conn.execute(
    "INSERT INTO risk_incidents (account_type, asset, reason, details) VALUES ($1,$2,'daily_loss_limit',$3)",
    account_type, asset, f"day_loss_pct={day_loss_pct:.2f} cap={HARD_CAP_PCT}")
```
2. `get_or_reset_daily_baseline`: replace `created_at` with `ts` (both in the SELECT and the comparison).
3. `_fetch_simulated_counts` queried columns that do not exist on `missed_trade_analytics`. Use the evaluated signals:
```python
query = """
    SELECT COUNT(*) FILTER (WHERE outcome='would_have_won')  AS wins,
           COUNT(*) FILTER (WHERE outcome='would_have_lost') AS losses,
           COUNT(*) FILTER (WHERE evaluated)                 AS total
    FROM missed_trade_signals WHERE asset = $1 AND created_at >= $2"""
```
4. Threshold clamp: keep `min(0.95, ...)` (the DB CHECK now allows 0.95 via migration 003).
5. `autotune_log` insert passes `json.dumps(...)` into `notes`; migration 003 adds that column.
6. `start_hr = int(params.get("scheduled_start_hour", 0))` crashes on NULL (`int(None)`): use
   `params.get("scheduled_start_hour")` and skip the schedule check when either value is `None`.

---------------------------------------------------------------------
## C. `startup/jobs/missed_trade_evaluator.py`
---------------------------------------------------------------------
Delete `evaluate_and_label_missed_trades` (it calls async functions without `await`, so it never worked, and nothing calls it).
Everything else in the file is correct.

---------------------------------------------------------------------
## D. `startup/jobs/ml_tier2.py`
---------------------------------------------------------------------
* Delete `run_retrain_cycle` (the dead copy; `main.py` has the live one). It passed a list of dicts to
  `compute_recency_weighted_labels` (needs a DataFrame) and trained on columns the table does not have.
* `OnlineTierUpdater.learn` residual: `batch_e_utility` is now the entry score; for a proper residual store the batch model's
  `e_utility` at entry in `trade_telemetry.entry_e_utility` (column already exists) and pass that.

---------------------------------------------------------------------
## E. `startup/jobs/auto_tester.py`
---------------------------------------------------------------------
`Expert=v10_kenjin.ex5` -> `Expert=MAPSAR_ProfitEngine_v12_00.ex5`. The tester also needs the Yahoo/Guardian inputs
disabled (`InpUseOrchestrator=false`) or WebRequest calls will fail inside the tester.

---------------------------------------------------------------------
## F. `orchestrator/static/dashboard.html` (optional, 3 small additions)
---------------------------------------------------------------------
1. Handle the new WebSocket messages inside `socket.onmessage`:
```javascript
if (payload.type === "GUARDIAN_ACTION") {
  logConsole(`[GUARDIAN] ${payload.action} ${payload.asset} #${payload.ticket} r=${payload.r_multiple} (${payload.reason})`, payload.action === "CLOSE" ? "warn" : "info");
  fetchKpis();
}
if (payload.type === "MARKET_CONTEXT") {
  document.getElementById("live-trend").innerText = payload.regime;
  logConsole(`[YAHOO] ${payload.summary}`, payload.fresh ? "info" : "warn");
}
```
2. `data.guardian_actions` and `data.market_context` are now in `/kpis` - render them in two small tables/console lines.
3. `fetchHealth()`: `data.yahoo === "ok"` -> green, `"stale"` -> yellow, else red.
