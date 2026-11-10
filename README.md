# Project Kenjin v12.00 – consolidated local release

This release combines the compiled project tree with the v12 market-context and Trade Guardian work, and applies the integration corrections listed in `docs/ANALYSIS_AND_IMPROVEMENTS.md` and `docs/RELEASE_NOTES_v12.md`.

## 1. Included

- FastAPI orchestrator, API schemas, auth, Redis client, WebSocket manager and dashboard.
- PostgreSQL canonical schema plus idempotent v12 compatibility migration.
- Forecast, Tier-2, Gatekeeper, Lifeline, calibration, missed-trade evaluator, pruning and MT5 tester jobs.
- Yahoo Finance context layer (context only; broker/MT5 prices remain the execution source).
- Server Trade Guardian and MQL5 `KenjinGuardian.mqh` client.
- MAPSAR v12 EA source: `mql5/MAPSAR_ProfitEngine_v12_00.mq5`.
- Electron shell and Linux/Windows launchers.
- API Postman collection, integration contract, schema tests and Guardian/market-context unit tests.

## 2. Important source/export limitations

The supplied compiled-system text export did not contain valid binary model files. The three `.pkl` entries were 42-byte error placeholders, so they are intentionally omitted. The model directory is created empty; run the training job to generate valid model artifacts.

The project export also listed an FBS installer `.exe` and a report workbook, but did not provide their usable file bytes in the text export; they are not included. It also did not include a usable `.migration_tmp/kenjin_supabase.dump` file. The migration script that imports a Supabase dump therefore remains a separate, optional migration path and requires that dump to be supplied locally. The release ships a canonical PostgreSQL schema for a clean local database.

Python source has passed `compileall`; the automated suite passes 7 tests and shell scripts pass `bash -n`. No live PostgreSQL/Redis services, Groq/xAI credentials, Wine terminal, broker account or MetaEditor compiler were available in this build environment. Consequently, database migration execution, live API integration, Electron runtime, and MQL5 compilation still require execution on the target machine. Do not treat this as a verified profitable strategy.

## 3. Installation – Linux (recommended for this source bundle)

1. Install Python 3.11+, PostgreSQL 15/16+, Redis, Node.js/npm and (for MT5) Wine. For a headless MT5 terminal, install Xvfb.
2. Copy `.env.example` to `.env` and configure real credentials locally. Generate the orchestrator key:

   ```bash
   openssl rand -hex 32
   ```

   Use the resulting value for `ORCH_API_KEY`; the sample intentionally leaves it blank so the API refuses authenticated requests until you configure it. Never commit `.env`.
3. Create the local PostgreSQL role/database and set the matching `DATABASE_URL`. Use a password URL-encoded if it contains reserved URL characters.
4. Apply the schema and migrations:

   ```bash
   chmod +x scripts/apply_migrations.sh start_kenjin.sh
   ./scripts/apply_migrations.sh
   ```

5. Install the MT5 EA and include in the **same** Experts folder under the Wine MT5 data directory. Compile `MAPSAR_ProfitEngine_v12_00.mq5` with MetaEditor. Add `http://127.0.0.1:8000` to MT5 Tools → Options → Expert Advisors → Allow WebRequest.
6. Configure `MT5_SHORTCUT_PATH` for the interactive launcher. For the tester, configure `MT5_TERMINAL_EXE` to the Windows `terminal64.exe` path inside the same Wine prefix. `MT5_PATH` as a shell command is deprecated and intentionally unused.
7. Start:

   ```bash
   ./start_kenjin.sh
   ```

The launcher creates/activates `orchestrator/.venv` (or the Conda environment), synchronizes requirements, checks PostgreSQL, checks Redis, starts MT5 via Wine, starts Uvicorn, checks `/health`, and launches Electron.

## 4. Windows

`start_kenjin.bat` starts the Python API and Electron application. PostgreSQL and Redis must already be installed and running, and `.env` must be configured. MT5 is started via the configured Windows installation path in the launcher; compile and install the v12 EA separately.

## 5. Environment variables

See `.env.example`. Key values:

- `DATABASE_URL`: local PostgreSQL DSN.
- `REDIS_URL`: use `redis://localhost:6379/0` (do not escape the `://`).
- `ORCH_API_KEY`: strong random secret shared with the EA/dashboard.
- `GROQ_API_KEY` and `GROQ_MODEL`: Tier-1 model connection. The supplied default model is `openai/gpt-oss-20b`.
- `FORECAST_INTERVAL_MIN`, `GATEKEEPER_INTERVAL_HOURS`: job cadences. Simulated missed-trade outcomes are excluded from approval decisions by default (`GATEKEEPER_ALLOW_SIMULATED=false`); only opt in after demo validation.
- `WINEPREFIX`, `WINE_BIN`, `MT5_SHORTCUT_PATH`: interactive Linux Wine launcher.
- `MT5_TERMINAL_EXE`, `MT5_TESTER_EXPERT`, `MT5_REPORT_DIR`: headless Strategy Tester configuration.

The optional `InpUseDatabase` EA input only initializes the legacy SQLite table; it is not a v12 live parameter source. When `InpUseOrchestrator=false`, the EA uses its own input parameters. The original prompt's example `REDIS_URL=redis\://...` is incorrect; use a literal `redis://...`. The original `MT5_PATH="env WINEPREFIX=... wine ..."` is a command string, not a terminal executable path; use the split variables above. Keep `KENJIN_HOST=127.0.0.1` for a local-only setup: Electron preload reads the local API key from `.env`, so do not expose this port to untrusted networks.

## 6. Safety controls and operational behavior

- Live entries are fail-closed by default: the EA requires the orchestrator to be healthy, a successful strategy-parameter fetch, and per-asset live approval. `InpEnforceLiveApproval=true` is the v12 default.
- Manual account halt (`account_controls`) is separate from per-asset live approval and is checked by `/strategy_params`; the EA blocks new entries when `trading_halted=true`.
- The global kill switch revokes existing live approvals when engaged. Clearing it removes the account halt but **does not** automatically re-approve assets.
- Guardian requests are sent at the configured 5-second interval. If the API cannot be reached for the configured number of retries, the EA applies its local fail-safe. The EA's local stop, floating-loss and max-hold protections remain enabled independently.
- Guardian R uses broker floating profit divided by the initial-stop cash risk (a practical approximation); slippage, commission timing and contract-specific cash calculations should be validated on each broker/symbol.
- Yahoo is advisory context only and may be delayed/stale/unavailable. It must not replace broker execution prices.
- Run on a demo account for at least four weeks and at least 100 closed trades before considering any live deployment. This does not guarantee future results.

## 7. API and integration contract

`docs/INTEGRATION_CONTRACT.md` lists the endpoints, request/response field names, PostgreSQL table ownership and fail-safe behaviour shared by the EA, orchestrator and Electron dashboard.

## 8. Tests and verification

```bash
python -m compileall -q startup orchestrator db tests
python -m pytest -q
curl -sS http://127.0.0.1:8000/health
```

The health endpoint reports PostgreSQL, Redis, LLM configuration, Yahoo freshness and last MT5 snapshot age. Test authenticated endpoints using the included `PROJECT_KENJIN.postman_collection.json` after setting its `apiKey` variable to the same secret used in `.env`.

## 9. Existing system update / dump migration

If upgrading a database which already contains data, take a verified `pg_dump` backup first. Apply the canonical schema and migration in a staging database before production. Do not run the Supabase restore script unless the correct dump and credentials are configured. `scripts/migrate_supabase_to_local.sh` remains in the release for the original project migration workflow; inspect its connection variables and dump path before executing.
