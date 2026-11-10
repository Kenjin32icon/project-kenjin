# Project Kenjin - system analysis and v12 improvements

## 1. How the engine works today (v11.09)

```
MT5 EA (MAPSAR v11.09)  --ticks/telemetry/snapshots-->  FastAPI orchestrator  --> Postgres + Redis
   TEMA+AC+SAR vote  <---- /strategy_params ----------   Tier-1 LLM (Groq) forecast every 15 min
   RSI veto, ATR risk                                    Tier-2 LightGBM (p_profit, p_loss, E[reward]) + micro-trend
   exits: SAR trail, profit-peak,                        Gatekeeper (live approval/threshold tuning), Lifeline (survival score),
          ML reversal, max hold                          Hour scheduler, Calibration (Brier), Missed-trade evaluator
                                                         WebSocket -> Electron dashboard
```
Decision chain for an entry: local vote score >= effective threshold -> RSI veto -> Tier-2 agreement -> risk/margin gates -> order.
Tier-2 EV: `EV = p_profit * E[utility] - p_loss * risk`; HOLD if `p_loss > 0.40` or `EV <= opt_threshold`.

## 2. Defects found (ordered by impact)

| # | Defect | Effect | v12 fix |
|---|--------|--------|---------|
| 1 | `db_pruner` imported `run_prune_query`, which did not exist in `db.py` | `ImportError` inside the scheduler `try` -> **no background job ever ran** (forecasts, gatekeeper, retrain, calibration, evaluator, hour scheduler) | function added; each job now registered separately |
| 2 | Operator halt / kill switch not enforced: Gatekeeper's `evaluate_gatekeeper_signal` is never called by any route and the EA ignored `live_approved` by default | Dashboard "Halt" and "Kill Switch" buttons did nothing to the EA | `/strategy_params` returns `trading_halted`; EA blocks entries; kill switch also sets the halt |
| 3 | Schedule hours are UTC but the EA compared `TimeCurrent()` (broker time) | Trades in the wrong hours | use `TimeGMT()` |
| 4 | `HasOpenPosition` read `posInfo.Magic()` without `posInfo.Select()` | magic check on stale data, wrong exits | `posInfo.Select(_Symbol)` |
| 5 | Micro-trend strings: server `BUY/SELL`, EA early-close compared `BEARISH/BULLISH` | in-flight micro-trend exit never fired | single vocabulary |
| 6 | Schema drift: `strategy_db.lifeline_*`, `account_controls`, `account_snapshots.day_start_balance/created_at`, `autotune_log.notes` missing; gatekeeper inserted non-existent `risk_incidents` columns; `chk_opt_threshold <= 0.90` while code raises to 0.95 | lifeline termination, daily-loss gate and halt silently erroring | migration `003` + patches |
| 7 | `GroqClient` does not exist in the groq SDK; unreachable code path | dead code / confusion | removed |
| 8 | Online learning called with `features=` (parameter is `features_map`) and looked for `OPEN` telemetry rows that the EA never sends | online model never learned | fixed |
| 9 | Retrain restricted to `account_type='live'` | with demo-only trading no model is ever trained -> permanently on the heuristic fallback | `TRAIN_ACCOUNT_TYPES=live,demo` |
| 10 | 30 s timer is the only place that checks protective exits between bars (`InpTradeOnBarCloseOnly`) | a losing trade can slide most of the way to the stop before being managed | 5 s Guardian loop + local fail-safe |
| 11 | CORS `allow_origins=*` with credentials | unsafe if the port is ever exposed | explicit origins, no credentials |
| 12 | Training labels leak: features sampled at trade **close** (`LATERAL ... ts <= created_at`) | optimistic model | roadmap item R3 |

## 3. What v12 adds

### 3.1 Yahoo Finance data -> "condensed current information" -> LLM -> EA
`startup/market_context.py` polls 1-minute bars for a macro basket (DXY, VIX, US10Y, S&P futures, gold, oil, BTC) plus every
tracked asset, computes return/trend/volatility, a volatility-normalised momentum score, a `risk_on / risk_off / neutral`
regime and a **bias in [-1, +1]** per asset (own momentum 60 %, factor betas e.g. EURUSD vs DXY 30 %, regime 10 %).
The snapshot is cached in Redis, pushed over the WebSocket, stored every 5th poll, injected into the LLM prompt, used to
veto Tier-2 entries that fight a strong bias (`|bias| >= 0.5`), and used as one "against" vote in the Guardian.
An optional live WebSocket (`KENJIN_YAHOO_WS=1`) refreshes last prices between polls.

**Important limits (verified in research):** Yahoo has no official API since 2017; yfinance uses reverse-engineered endpoints
that can break or rate-limit (429) without notice, data can be delayed, and Yahoo's terms restrict the data to personal use
(commercial / income-generating use needs Yahoo's written permission). Therefore Yahoo is **context only**: execution always
uses broker prices; every consumer checks `fresh` and falls back to "no opinion" (bias 0) when data is stale. If you ever run
this commercially, switch the feed to a licensed provider - only `MarketContextService._download` has to change.

### 3.2 Grok / Groq
Your code calls **Groq** (groq.com inference, model `openai/gpt-oss-20b`), not xAI **Grok**. v12 supports both through one
OpenAI-compatible client: `LLM_PROVIDER=groq` or `LLM_PROVIDER=xai` (`https://api.x.ai/v1`, default model `grok-4.5`,
override with `XAI_MODEL`; model names change, so confirm in the xAI console). LLM probabilities are shrunk toward 50/50
(`LLM_SHRINK=0.6`) and clamped to 25-75 % because raw LLM confidence is uncalibrated; the Brier calibration job then measures
whether the downstream Tier-2 confidence actually earned any trust.

### 3.3 Trade Guardian (automated cancellation of trades trending to a loss)
The EA reports each open position every 5 s (`/guardian/evaluate`). The server tracks R-multiple history per ticket and decides:

| Rule | Trigger |
|------|---------|
| hard_stop | r <= -0.85 R (gap/slippage protection before the -1 R stop) |
| loss_acceleration | r <= -0.35 R **and** R-slope <= -0.15 R/min **and** >= 1 against-vote |
| loss_acceleration_no_vote | r <= -0.55 R and slope <= -0.25 R/min |
| thesis_invalidated | r < -0.1 R and >= 2 against-votes (micro-trend, Tier-2, Yahoo bias, EA momentum) |
| time_underwater | held >= 50 % of max-hold, r <= -0.25 R, not improving |
| profit_giveback | peak >= 0.8 R, gave back >= 40 %, >= 1 against-vote |
| breakeven_ratchet | r >= 1 R -> stop moved to entry + 0.1 R |

A simulated BUY sliding -0.05 R ... -0.45 R with an opposing micro-trend is closed at **-0.36 R** instead of riding to -1 R.
**Fail-safe:** if the orchestrator is down, `KenjinGuardian.mqh` applies local rules (-0.85 R hard, -0.45 R with momentum
against). Every Guardian action is stored in `guardian_actions` for review and future learning.

Trade-off to be aware of: cutting losers early raises the loss frequency of "would have recovered" trades. Tune the `GUARDIAN_*`
thresholds against `guardian_actions` + `missed_trade_signals` data on a demo account before going live.

## 4. Further research-backed improvements (roadmap, not yet coded)

* **R1 Triple-barrier labels for every bar** (`startup/labeling.py` already implements it with purging, uniqueness weights and
  recency decay but nothing calls it). Have the EA post `close/bar_high/bar_low/atr` and train on all bars instead of ~dozens of trades.
* **R2 Meta-labeling:** keep the TEMA/AC/SAR vote as the primary signal and train LightGBM only to decide *take / skip* and size.
* **R3 Entry-time features:** store `entry_features` (JSON column exists) at order time; stop sampling features at the close.
* **R4 Purged walk-forward validation + deflated metrics** before any retrained model replaces the live one (champion/challenger,
  promote only if out-of-sample expectancy beats the incumbent).
* **R5 Cost-aware EV:** subtract spread + commission + expected slippage (`spread_atr` feature exists) inside the EV gate.
* **R6 Position sizing:** replace the Kelly-style multiplier with fractional Kelly capped by the daily-loss budget and a portfolio
  correlation cap (EURUSD/GBPUSD/XAUUSD are not independent bets).
* **R7 Execution quality:** log fill price vs signal price per trade; auto-pause an asset when slippage > 0.2 R.
* **R8 News/event blackout:** economic-calendar feed (NFP, CPI, FOMC) -> block entries +/- 15 min, tighten Guardian.
* **R9 Regime-dependent parameters:** key SL/TP/threshold by (volatility regime x Yahoo regime) instead of one value per asset.
* **R10 Operations:** Prometheus metrics, alerting (Telegram) on `risk_incident`, nightly `pg_dump`, API key rotation, bind to
  127.0.0.1 only, secrets out of the Electron preload for any non-local deployment.

## 5. Honest expectations
No code can guarantee profit. The changes above reduce *avoidable* losses and remove bugs that silently disabled risk controls,
but strategy edge must be proven out-of-sample. Run v12 on a **demo account for at least 4 weeks** (>= 100 closed trades),
review `guardian_actions`, the missed-trade summary and the calibration score, and only then consider a small live allocation.
