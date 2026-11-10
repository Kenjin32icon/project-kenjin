# Project Kenjin v12.00 – integration contract

This document records the wire/database contract used by the release. Keep these names in sync when editing the EA, FastAPI schemas, database, or dashboard.

## MT5 → Orchestrator

All REST requests except `GET /health` require `X-API-Key: <ORCH_API_KEY>`. The MT5 terminal must allow WebRequest to `http://127.0.0.1:8000`.

| Method / path | Purpose | Key contract |
|---|---|---|
| `GET /health` | Service health | Returns database, Redis, LLM, Yahoo and last MT5 snapshot status; no API key |
| `GET /strategy_params?asset=EURUSD&account_type=demo` | EA parameter pull | Returns threshold, SL/TP multipliers, per-asset approval, `trading_halted`, tier2 action/confidence/lot multiplier, `micro_trend` (`BUY/SELL/NEUTRAL`), UTC schedule, lifeline and market context |
| `POST /ticks` | Tick/feature snapshot | `asset`, `bid`, `ask`, `tick_volume`, `rsi`, `tema`, `ac`, `sar`, `adx`, `ma10`, `ma20`, `ma50`, `ma100`, `ma200` |
| `POST /telemetry` | Completed trade telemetry | `asset`, `type` (`BUY_CLOSE`/`SELL_CLOSE`), `price`, `lots`, `profit`, plus optional RSI, entry score, SL/TP, magic, account type, forecast ID and Tier-2 confidence |
| `POST /account_snapshot` | Account state | `account_type`, balance/equity, margin, floating P/L, drawdown, day loss, `day_start_balance`, streak/cooldown/halt state |
| `POST /risk_incident` | Local safety incident | `account_type`, `asset`, `reason`, `details` |
| `POST /missed_trade` | Blocked/counterfactual signal | `asset`, `reason`, `side`, score/threshold, RSI, price, ATR, SL/TP distances and account metadata |
| `POST /guardian/evaluate` | Position risk report | ticket, side, open/current price, current SL/TP, lots, P/L, `r_multiple`, `peak_r`, `initial_sl_distance`, held/max-hold seconds and momentum-against flag |

Guardian responds with `action` (`HOLD`, `CLOSE`, `MOVE_SL`), `reason`, `new_sl`, `r_multiple`, `peak_r`, `slope_r_min`, `against_votes`, vote list and market bias. The EA executes the action by position ticket. If the server call is unavailable, local EA fail-safe rules remain active.

## Dashboard → Orchestrator

| Method / path | Dashboard action |
|---|---|
| `GET /kpis` | KPI/account/asset/risk/Guardian tables |
| `GET /missed_trades/summary?days=7` | Missed-signal analytics |
| `POST /control/kill_switch` | Global stop; also revokes existing per-asset live approvals |
| `POST /control/drawdown_halt` | Independent live/demo account halt or resume |
| `PATCH /strategy_params` | Per-asset live approval |
| `POST /jobs/retrain` | Trigger model retraining |
| `POST /jobs/calibrate` | Trigger confidence calibration |
| `GET /market/context` | Market snapshot |
| `GET /market/context/{asset}` | Per-asset context with freshness |

The WebSocket path is `/ws/dashboard`; it authenticates using the `api_key` query parameter and broadcasts `TICK_UPDATE`, `RISK_ALERT`, `ACCOUNT_CONTROL_UPDATE`, `GUARDIAN_ACTION`, `MARKET_CONTEXT`, and model updates.

## PostgreSQL ownership

- `strategy_db`: per-asset threshold, risk multipliers, UTC schedule, live approval, RSI veto and calibration.
- `account_controls`: manual halt per `live`/`demo` account.
- `strategy_lifeline`: per-asset survival score, streaks and termination state.
- `tick_telemetry`: feature history used for forecasts and model inference.
- `trade_telemetry`: completed trade outcomes and optional entry-feature fields.
- `account_snapshots`: account health and UTC/broker snapshot state.
- `forecasts`: Tier-1 probabilities.
- `missed_trade_signals`: blocked signals and evaluated forward outcomes.
- `guardian_actions`: audit records for Guardian close/stop decisions.
- `market_context_snapshots`: sampled market-context payloads.
- `risk_incidents` and `autotune_log`: operator/risk events and Gatekeeper history.

## Fail-safe rules

1. A missing/unhealthy orchestrator or failed parameter fetch blocks new entries when `InpUseOrchestrator=true`.
2. `trading_halted=true` blocks new entries for the matching account type. Live account entry additionally needs the per-asset `live_approved=true` flag by default.
3. The kill switch never re-approves assets when cleared; an operator must explicitly restore asset approval.
4. Existing-position protection (hard stop, floating-loss kill switch, max hold, and local Guardian fallback) stays inside the EA and does not depend on a fresh server response.
5. Yahoo is advisory context only; stale per-asset data returns `fresh=false` and bias zero. Broker quotes remain the only execution prices.
