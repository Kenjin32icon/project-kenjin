# Project Kenjin v12.00 – release notes and verification record

## Updated in this consolidated release

1. Replaced the invalid, duplicate `sql/migrations/schema.sql` baseline with one executable canonical schema and an idempotent `003_v12_guardian_market_context.sql` delta.
2. Added all v12 tables and data columns used by the runtime: `account_controls`, `guardian_actions`, `market_context_snapshots`, `day_start_balance`, `autotune_log.notes`, lifeline fields, and telemetry training fields.
3. Fixed Gatekeeper queries to use `account_snapshots.ts`, valid `risk_incidents(account_type,asset,reason,details)` columns, evaluated `missed_trade_signals` outcomes, and null-safe optional UTC schedule windows.
4. Removed the dead synchronous missed-trade wrapper which called async database helpers without awaiting them.
5. Removed the unused duplicate Tier-2 retraining function; `orchestrator.main.run_retrain_cycle` is the single scheduler owner.
6. Updated the MT5 tester to use `MT5_TERMINAL_EXE` + `WINEPREFIX` with argument-safe process creation, configurable report paths, and a `.set` that disables orchestrator/Guardian WebRequests in Strategy Tester.
7. Promoted the MAPSAR source to v12.00, added `KenjinGuardian.mqh`, used UTC for UTC strategy windows, fixed position magic selection, parsed account halt/market context, made parameter fetch failure fail-closed, and reduced timer cadence to Guardian cadence.
8. Persisted initial SL and Guardian peak R in MT5 terminal Global Variables to survive terminal restart and trailing-stop changes.
9. Updated the EA to select positions by symbol + magic + ticket, not the ambiguous symbol-only selector; ticket-based modifications/closes avoid acting on another EA's position on hedging accounts.
10. Reworked lot sizing to respect broker volume min/max/step, round down, refuse an unaffordable minimum lot, and calculate margin for the actual BUY/SELL direction.
11. Aggregated partial-close volume and realised P/L until the position is fully closed; fixed BUY_CLOSE/SELL_CLOSE labelling so the counter-side exit deal does not reverse the original position direction.
12. Made Yahoo freshness per asset (not only global snapshot freshness), kept stale per-asset bias neutral, and prevented simulated win/loss counts from being added as fake account-currency profit factor.
13. Updated dashboard WebSocket handling for Guardian actions / market-context updates and added operational setup instructions.
14. Corrected launcher configuration: use a literal Redis URL, venv Python for the database preflight, safer process cleanup, and separated terminal executable from shell-command strings.
15. Reconciled Python and Conda dependency declarations into one compatible direct-dependency set; added market-context unit tests.
16. Corrected the legacy SQLite toggle so it no longer causes uninitialized database defaults to override the EA's explicit local inputs when the orchestrator is disabled.

## Deferred / requires additional design

- Entry-time feature leakage (R3 in `docs/ANALYSIS_AND_IMPROVEMENTS.md`) remains a data-pipeline improvement: the currently wired online learner still derives its lightweight features from recently collected tick history and uses the entry score as its residual baseline. Correcting it properly requires a complete entry-feature snapshot contract from EA → API → PostgreSQL and a verified model e-utility at entry. Do not interpret the current online learner as leakage-free.
- The strategy tester still depends on the MT5 installation, expert `.ex5`, symbol history, broker/tester profile, and path layout configured on the actual target machine.
- MQL5 compilation has not been run in MetaEditor here. The `.mq5` and `.mqh` files need compile validation and demo forward testing.
- The v12 draft and legacy project contain some telemetry/business logic whose columns and semantics are inherited. The canonical schema covers all SQL fields referenced in the supplied code, but a database migration should be validated against a clone of the user's actual PostgreSQL database before a live cutover.
- Model binary files were not present in the upload in a usable form. Training/retraining must produce the `.pkl` artifacts before the learned Tier-2 path can operate; until then its fallback path will return heuristic outputs/HOLD depending on its implementation.

## Verification performed in the build workspace

- Python `compileall` passed on the consolidated Python source.
- `pytest`: 7 tests passed (Guardian decision rules, market-context staleness, and schema contracts).
- `bash -n` passed for the Linux launcher and all shipped shell scripts.
- EA source copies in `mql5/` and `Docs/` are byte-identical; the Guardian include copies are byte-identical.
- Static checks confirmed no imports of the removed sync wrapper and no references to the duplicate `ml_tier2.run_retrain_cycle` remain.
- PostgreSQL, Redis, full dependency installation, HTTP integration, browser/Electron launch and MT5/MetaEditor runtime tests were not available in the build workspace.
