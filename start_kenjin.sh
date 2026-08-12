#!/usr/bin/env bash
#
# start_kenjin.sh — Complete launcher for Project Kenjin Orchestrator, FBS MT5 & Electron Frontend App.
#

set -euo pipefail

# -----------------------------------------------------------------------------
# 1. Directory Setup & Path Resolution
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"
ORCH_DIR="$ROOT_DIR/orchestrator"
ELECTRON_DIR="$ROOT_DIR/electron-app"

cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
echo "==> Project Root Directory: $ROOT_DIR"

mkdir -p "$ORCH_DIR/static"

# -----------------------------------------------------------------------------
# 2. Process Cleanup (Close all past running instances)
# -----------------------------------------------------------------------------
echo "==> Terminating any previously running instances of Kenjin Orchestrator, MT5, and Electron..."

pkill -f "uvicorn orchestrator.main:app" 2>/dev/null || true
if command -v fuser >/dev/null 2>&1; then
    fuser -k 8000/tcp 2>/dev/null || true
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
# 4. Virtual Environment Activation
# -----------------------------------------------------------------------------
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

echo "==> Using Python environment: $(which python)"

if [ ! -f ".env" ]; then
    echo "ERROR: .env file missing in $ROOT_DIR!"
    exit 1
fi

# -----------------------------------------------------------------------------
# 5. Launch FBS MetaTrader 5 via Wine
# -----------------------------------------------------------------------------
echo "==> Launching FBS MetaTrader 5..."
env WINEPREFIX="/home/infoscience/.wine" wine-stable "C:\users\infoscience\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\FBS MetaTrader 5\FBS MT5.lnk" > /dev/null 2>&1 &
MT5_PID=$!
echo "==> FBS MT5 launched (PID: $MT5_PID)."

# -----------------------------------------------------------------------------
# 6. Launch FastAPI Orchestrator & Electron App
# -----------------------------------------------------------------------------
HOST="127.0.0.1"
PORT="8000"

echo "==> Starting Kenjin Orchestrator API (http://${HOST}:${PORT})..."
uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env &
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
        npm start > /dev/null 2>&1 &
    )
    ELECTRON_PID=$!
else
    echo "WARNING: Electron app folder not found at $ELECTRON_DIR"
fi

echo "========================================================================="
echo "  Project Kenjin is live!"
echo "  - API Backend: http://${HOST}:${PORT}"
echo "  - App UI:      Electron Application ($ELECTRON_DIR)"
echo "  - FBS MT5:     Running under Wine prefix /home/infoscience/.wine"
echo "  Press Ctrl+C to stop the system."
echo "========================================================================="

wait "$UVICORN_PID"