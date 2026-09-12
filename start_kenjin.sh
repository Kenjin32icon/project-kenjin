#!/usr/bin/env bash
#
# start_kenjin.sh — Complete launcher for Project Kenjin Orchestrator,
# FBS MT5 (via Wine) & Electron Frontend App.
#
# CHANGES FROM v1:
#   - No more hardcoded /home/infoscience paths - everything is read from
#     .env or sane defaults, so this runs on any Linux Mint account.
#   - Optional Miniconda environment (USE_CONDA=1) as an alternative to the
#     orchestrator/.venv virtualenv - see environment.yml.
#   - Logs go to ./logs/*.log instead of /dev/null, so a crash is
#     debuggable after the terminal closes.
#   - Starts a virtual display (Xvfb) automatically if no X session is
#     detected, so Wine/MT5 can run on a headless Linux Mint box or over SSH.
#   - Verifies the configured DATABASE_URL is actually reachable before
#     starting uvicorn, so a bad local-Postgres migration fails loudly here
#     instead of inside a stack trace three layers deep.

set -euo pipefail

# -----------------------------------------------------------------------------
# 1. Directory Setup & Path Resolution
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"
ORCH_DIR="$ROOT_DIR/orchestrator"
ELECTRON_DIR="$ROOT_DIR/electron-app"
LOG_DIR="$ROOT_DIR/logs"

cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
mkdir -p "$ORCH_DIR/static" "$LOG_DIR"
echo "==> Project Root Directory: $ROOT_DIR"

if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    source ".env"
    set +a
else
    echo "ERROR: .env file missing in $ROOT_DIR!"
    exit 1
fi

# Portable defaults - override any of these in .env instead of editing this script.
: "${WINEPREFIX:=$HOME/.wine-kenjin}"
: "${MT5_SHORTCUT_PATH:=C:\\users\\$(whoami)\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\FBS MetaTrader 5\\FBS MT5.lnk}"
: "${WINE_BIN:=wine}"
: "${KENJIN_HOST:=127.0.0.1}"
: "${KENJIN_PORT:=8000}"
: "${USE_CONDA:=0}"
: "${CONDA_ENV_NAME:=kenjin}"

# -----------------------------------------------------------------------------
# 2. Process Cleanup (Close all past running instances)
# -----------------------------------------------------------------------------
echo "==> Terminating any previously running instances of Kenjin Orchestrator, MT5, and Electron..."

pkill -f "uvicorn orchestrator.main:app" 2>/dev/null || true
if command -v fuser >/dev/null 2>&1; then
    fuser -k "${KENJIN_PORT}/tcp" 2>/dev/null || true
fi

pkill -f "terminal64.exe" 2>/dev/null || true
pkill -f "FBS MT5" 2>/dev/null || true
pkill -f "wine-stable" 2>/dev/null || true
pkill -f "electron" 2>/dev/null || true

sleep 2
echo "==> Cleanup complete."

# -----------------------------------------------------------------------------
# 3. Redis Service Check
# -----------------------------------------------------------------------------
if command -v redis-cli >/dev/null 2>&1 && redis-cli ping >/dev/null 2>&1; then
    echo "==> Redis is running."
elif command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet redis-server 2>/dev/null; then
    echo "==> Redis is running (systemd)."
elif command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files 2>/dev/null | grep -q redis-server; then
    echo "==> Starting Redis via systemd..."
    sudo systemctl start redis-server
elif command -v redis-server >/dev/null 2>&1; then
    echo "==> Starting Redis as background daemon..."
    redis-server --daemonize yes
else
    echo "WARNING: redis-server not found. Orchestrator will default to Postgres fallback."
fi

# -----------------------------------------------------------------------------
# 4. Python Environment Activation (venv OR Miniconda)
# -----------------------------------------------------------------------------
if [ "$USE_CONDA" = "1" ]; then
    if ! command -v conda >/dev/null 2>&1; then
        echo "ERROR: USE_CONDA=1 but 'conda' is not on PATH. Install Miniconda or unset USE_CONDA."
        exit 1
    fi
    # shellcheck disable=SC1091
    source "$(conda info --base)/etc/profile.d/conda.sh"
    if ! conda env list | grep -qE "^\s*${CONDA_ENV_NAME}\s"; then
        echo "==> Creating Miniconda environment '${CONDA_ENV_NAME}' from environment.yml..."
        conda env create -f "$ROOT_DIR/environment.yml"
    fi
    conda activate "$CONDA_ENV_NAME"
    echo "==> Using Miniconda environment: $CONDA_ENV_NAME ($(which python))"
else
    VENV_PATH="orchestrator/.venv"
    if [ ! -d "$VENV_PATH" ]; then
        echo "==> Creating Python virtual environment at $VENV_PATH..."
        python3 -m venv "$VENV_PATH"
        # shellcheck disable=SC1091
        source "$VENV_PATH/bin/activate"
        pip install --upgrade pip
        if [ -f "orchestrator/requirements.txt" ]; then
            pip install -r orchestrator/requirements.txt
        fi
    else
        # shellcheck disable=SC1091
        source "$VENV_PATH/bin/activate"
    fi
    echo "==> Using Python virtualenv: $(which python)"
fi

# -----------------------------------------------------------------------------
# 5. Local Database Reachability Pre-flight
# -----------------------------------------------------------------------------
if [ -z "${DATABASE_URL:-}" ]; then
    echo "ERROR: DATABASE_URL is not set in .env."
    exit 1
fi
echo "==> Checking DATABASE_URL is reachable..."
python3 - <<'PYEOF'
import os, sys
import asyncio
import asyncpg

async def main():
    try:
        conn = await asyncio.wait_for(asyncpg.connect(dsn=os.environ["DATABASE_URL"]), timeout=8)
        await conn.execute("SELECT 1;")
        await conn.close()
        print("==> DATABASE_URL is reachable.")
    except Exception as exc:
        print(f"ERROR: Could not reach DATABASE_URL: {exc}")
        sys.exit(1)

asyncio.run(main())
PYEOF

# -----------------------------------------------------------------------------
# 6. Virtual Display for Wine (headless / SSH-friendly)
# -----------------------------------------------------------------------------
XVFB_PID=""
if [ -z "${DISPLAY:-}" ]; then
    if command -v Xvfb >/dev/null 2>&1; then
        echo "==> No DISPLAY detected - starting a virtual display via Xvfb for Wine/MT5..."
        Xvfb :99 -screen 0 1280x1024x24 >"$LOG_DIR/xvfb.log" 2>&1 &
        XVFB_PID=$!
        export DISPLAY=:99
        sleep 1
    else
        echo "WARNING: No DISPLAY and Xvfb not installed. MT5 under Wine may fail to render."
        echo "         Install with: sudo apt install xvfb"
    fi
fi

# -----------------------------------------------------------------------------
# 7. Launch FBS MetaTrader 5 via Wine
# -----------------------------------------------------------------------------
echo "==> Launching FBS MetaTrader 5 (WINEPREFIX=$WINEPREFIX)..."
env WINEPREFIX="$WINEPREFIX" "$WINE_BIN" "$MT5_SHORTCUT_PATH" >"$LOG_DIR/mt5_wine.log" 2>&1 &
MT5_PID=$!
echo "==> FBS MT5 launched (PID: $MT5_PID). Logs: $LOG_DIR/mt5_wine.log"

# -----------------------------------------------------------------------------
# 8. Launch FastAPI Orchestrator & Electron App
# -----------------------------------------------------------------------------
HOST="$KENJIN_HOST"
PORT="$KENJIN_PORT"

echo "==> Starting Kenjin Orchestrator API (http://${HOST}:${PORT})..."
uvicorn orchestrator.main:app --host "$HOST" --port "$PORT" --env-file .env \
    >"$LOG_DIR/orchestrator.log" 2>&1 &
UVICORN_PID=$!

ELECTRON_PID=""

cleanup() {
    echo ""
    echo "==> Shutting down Kenjin Orchestrator (PID: $UVICORN_PID) & Electron App..."
    kill "$UVICORN_PID" 2>/dev/null || true
    if [ -n "$ELECTRON_PID" ]; then
        kill "$ELECTRON_PID" 2>/dev/null || true
    fi
    pkill -f "electron" 2>/dev/null || true
    if [ -n "$XVFB_PID" ]; then
        kill "$XVFB_PID" 2>/dev/null || true
    fi
    wait "$UVICORN_PID" 2>/dev/null || true
    echo "==> Stopped successfully."
}
trap cleanup INT TERM

echo "==> Waiting for API health check..."
for i in $(seq 1 30); do
    if curl -sf "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
        echo "==> Orchestrator active and healthy."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Orchestrator did not become healthy within 15s. Check $LOG_DIR/orchestrator.log"
        tail -n 40 "$LOG_DIR/orchestrator.log" || true
        exit 1
    fi
    sleep 0.5
done

# Open Frontend through Electron App
if [ -d "$ELECTRON_DIR" ]; then
    echo "==> Launching Electron Frontend App..."
    (
        cd "$ELECTRON_DIR"
        if [ ! -d "node_modules" ]; then
            echo "==> Installing Electron dependencies..."
            npm install
        fi
        npm start >"$LOG_DIR/electron.log" 2>&1 &
    )
    ELECTRON_PID=$!
else
    echo "WARNING: Electron app folder not found at $ELECTRON_DIR"
fi

echo "========================================================================="
echo "  Project Kenjin is live!"
echo "  - API Backend: http://${HOST}:${PORT}"
echo "  - App UI:      Electron Application ($ELECTRON_DIR)"
echo "  - FBS MT5:     Running under Wine prefix $WINEPREFIX"
echo "  - Logs:        $LOG_DIR/{orchestrator,mt5_wine,electron,xvfb}.log"
echo "  Press Ctrl+C to stop the system."
echo "========================================================================="

wait "$UVICORN_PID"
