# Project Kenjin – consolidated system analysis (v12.00)

## Scope and source basis

This release was prepared by comparing the compiled project export with the supplied v12 combined patch document and the MAPSAR v11.09 EA source embedded in the request. It retains the original project architecture and jobs rather than replacing the system with a simplified new implementation.

The uploaded compiled export listed the Python services, PostgreSQL schema, Redis helper, Electron UI, launchers, MQL5 experts and model files. The proposed v12 document supplied the market-context service, Guardian service/client and patch rationale. The three serialized `.pkl` model entries in the compiled export were tiny error-placeholder files, not trained models; they were excluded.

## Architecture retained

- **MAPSAR EA / MT5:** local TEMA, AC, SAR, RSI and ATR signals; risk-based order sizing; local stop/trailing/hold/drawdown exits; authenticated data posts; server Guardian requests.
- **FastAPI orchestrator:** API-key authentication; `/strategy_params`; telemetry ingestion; health and KPI endpoints; operator controls; WebSocket updates; scheduled background jobs.
- **Tier 1:** provider-configurable OpenAI-compatible Groq or xAI forecast request.
- **Tier 2:** LightGBM-based signal evaluation, micro-trend and online utility adjustment.
- **Context layer:** Yahoo Finance cross-asset information, treated as advisory only and discarded when stale.
- **Risk layer:** Gatekeeper, Lifeline, account-level halts, drawdown controls and the Trade Guardian.
- **Persistence/cache:** PostgreSQL is the durable record; Redis is used for transient tick windows and cached state.
- **Operator UI:** Electron-hosted dashboard served by FastAPI static content.

## Major integration faults corrected

1. **Scheduler import dependency:** added the missing safe, table/column-whitelisted pruning helper and retained independent job registration so one failed job import does not prevent all other jobs from starting.
2. **Halt propagation:** `/strategy_params` now conveys the account halt; the EA blocks entries on halt, stale parameters or unhealthy orchestrator. Clearing a global halt does not silently re-approve assets.
3. **Time agreement:** strategy schedule hours use UTC in the EA, while the Friday cutoff remains broker-time based.
4. **Position selection:** the EA selects by ticket after filtering symbol and magic number, then modifies/closes by ticket. This avoids symbol-only ambiguity on hedging accounts.
5. **Micro-trend vocabulary:** the server and EA use the same BUY/SELL/NEUTRAL vocabulary.
6. **Database drift:** rebuilt one canonical PostgreSQL schema and an idempotent v12 migration for the account controls, Guardian audit, market snapshots, lifeline and telemetry fields used by the runtime.
7. **Gatekeeper SQL:** repaired the account snapshot timestamp, risk incident insert fields, optional schedule handling and simulated-signal source query.
8. **Simulation metrics:** counterfactual wins/losses may extend a sample/win-rate calculation when explicitly enabled, but are no longer treated as fictional cash P/L to calculate profit factor. Simulation remains disabled by default.
9. **Asynchronous dead code:** removed an unused wrapper that called async database operations without awaiting them and removed the duplicate Tier-2 retrain routine.
10. **Dependency declarations:** replaced inconsistent duplicate package pins with a single compatible requirements definition and aligned the Conda environment.
11. **Yahoo freshness:** freshness is evaluated for the individual asset (and its macro inputs), not inferred solely from another asset's most recent bar.
12. **EA Guardian state:** original stop distance and Guardian peak-R state are persisted independently of trailing-stop changes. Partial exits are accumulated and only one completed-position telemetry record is emitted.
13. **EA position risk:** lot sizing respects broker min/max/step and order side for margin calculation; the EA refuses to force the minimum lot when that would exceed its requested risk budget.
14. **Launcher safety:** corrected the Redis URL, database preflight Python, process cleanup behaviour and MT5 terminal path configuration.
15. **Legacy SQLite parameter switch:** corrected the old `InpUseDatabase` switch so it cannot masquerade as a working remote parameter source; with the orchestrator disabled, explicit EA input defaults remain active.

## Remaining work – not represented as complete

- **MQL5 compiler verification:** MetaEditor was not available in the build environment. Compile both `MAPSAR_ProfitEngine_v12_00.mq5` and its include on the target MT5 installation. Check warnings, tester behaviour and broker-specific symbol properties.
- **Live integration:** PostgreSQL, Redis, Groq/xAI, Wine, MT5 and Electron services were not available here. The migration and API need execution against a staging database and live local services.
- **Existing DB upgrade:** `CREATE TABLE IF NOT EXISTS` does not automatically convert every unknown legacy schema. Back up the real database and apply to a clone first; reconcile any locally added columns/indexes not represented in the supplied source export.
- **Training artefacts:** no valid existing model binaries were present. Run the retraining pipeline with enough validated rows before expecting trained Tier-2 predictions.
- **Entry-time feature leakage:** the current online learning / closed-trade training path can still sample technical data after entry. A complete EA → API → PostgreSQL entry-feature snapshot and proper entry-time expected-utility value are still needed before describing the model as leakage-free.
- **Guardian R accounting:** floating profit divided by estimated initial cash risk is an approximation. Verify contract specifications, tick values, commissions, swaps and slippage on every broker/symbol, and compare local and server R values during demo forward testing.
- **Tester path:** MT5 tester automation depends on the actual Wine prefix, MT5 executable location, symbol history, expert installation and tester profile. Confirm the `.set` and `.ini` behaviour on the target terminal.
- **Operational secrets:** `.env` is intentionally not included. Generate `ORCH_API_KEY`, configure DB credentials and provider keys locally, keep the host bound to `127.0.0.1`, and do not expose the Electron/API key path to untrusted networks.

## Verification performed for this release tree

- `python -m compileall -q startup orchestrator db tests scripts/run_rsi_tuning.py`
- `python -m pytest -q` – seven passing unit/contract tests.
- `bash -n start_kenjin.sh scripts/apply_migrations.sh scripts/migrate_supabase_to_local.sh scripts/smoke_test_api.sh`
- Checked that EA and Guardian copies in `mql5/` and `Docs/` match byte-for-byte.

These checks are source-level validation only. They do not establish that a broker will accept an order, a database migration will succeed against every existing installation, or the strategy will be profitable. Start in Strategy Tester and a demo account; do not bypass the approval and loss controls to make the software trade.
