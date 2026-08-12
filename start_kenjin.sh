#!/usr/bin/env bash
#
# start_kenjin.sh — Universal Launcher for Kenjin Orchestrator & Electron Frontend
#

set -euo pipefail

# -----------------------------------------------------------------------------
# 1. Directory Setup & Path Resolution
# -----------------------------------------------------------------------------
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCH_DIR="$ROOT_DIR/orchestrator"

cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
echo "==> Project Root Directory: $ROOT_DIR"

# -----------------------------------------------------------------------------
# 2. Locate Electron App Directory (Fixes missing package.json error)
# -----------------------------------------------------------------------------
ELECTRON_DIR=""
for dir in "$ROOT_DIR/electron-app" "$ROOT_DIR/electron" "$ROOT_DIR"; do
    if [ -f "$dir/package.json" ]; then
        ELECTRON_DIR="$dir"
        break
    fi
done

if [ -z "$ELECTRON_DIR" ]; then
    # Fallback: search 2 levels deep for package.json
    ELECTRON_DIR="$(find "$ROOT_DIR" -maxdepth 2 -name "package.json" -exec dirname {} \; | head -n 1)"
fi

if [ -z "$ELECTRON_DIR" ] || [ ! -f "$ELECTRON_DIR/package.json" ]; then
    echo "ERROR: Could not locate 'package.json' in $ROOT_DIR or any subdirectories."
    exit 1
fi
echo "==> Electron App Directory: $ELECTRON_DIR"

# -----------------------------------------------------------------------------
# 3. Process Cleanup
# -----------------------------------------------------------------------------
echo "==> Cleaning up previous instances..."
pkill -f "uvicorn orchestrator.main:app" 2>/dev/null || true
pkill -f "electron" 2>/dev/null || true
pkill -f "terminal64.exe" 2>/dev/null || true
if command -v fuser >/dev/null 2>&1; then 
    fuser -k 8000/tcp 2>/dev/null || true
fi
sleep 1

# -----------------------------------------------------------------------------
# 4. Service Dependencies Check (Redis & Python Venv)
# -----------------------------------------------------------------------------
# Ensure Redis is responsive
if ! command -v redis-cli >/dev/null 2>&1 || ! redis-cli ping >/dev/null 2>&1; then
    echo "==> Starting Redis daemon..."
    redis-server --daemonize yes 2>/dev/null || true
fi

# Set up Python Virtual Environment
VENV_PATH="$ORCH_DIR/.venv"
if [ ! -d "$VENV_PATH" ]; then
    echo "==> Creating Python virtual environment..."
    python3 -m venv "$VENV_PATH"
fi
source "$VENV_PATH/bin/activate"

if [ -f "$ORCH_DIR/requirements.txt" ]; then
    echo "==> Verifying Python dependencies..."
    pip install -q -r "$ORCH_DIR/requirements.txt"
fi

if [ ! -f ".env" ]; then
    echo "ERROR: .env configuration file missing in $ROOT_DIR!"
    exit 1
fi

# -----------------------------------------------------------------------------
# 5. Electron Dependencies Check
# -----------------------------------------------------------------------------
if [ ! -d "$ELECTRON_DIR/node_modules" ]; then
    echo "==> Installing Node modules for Electron..."
    (cd "$ELECTRON_DIR" && npm install)
fi

# -----------------------------------------------------------------------------
# 6. Launch FBS MetaTrader 5 via Wine
# -----------------------------------------------------------------------------
WINE_DIR="${WINEPREFIX:-$HOME/.wine}"
MT5_LNK=$(find "$WINE_DIR" -name "FBS MT5.lnk" 2>/dev/null | head -n 1)

if [ -n "$MT5_LNK" ]; then
    echo "==> Launching FBS MetaTrader 5..."
    wine "$MT5_LNK" >/dev/null 2>&1 &
else
    echo "WARNING: FBS MT5 shortcut not found under $WINE_DIR. Skipping Wine launch."
fi

# -----------------------------------------------------------------------------
# 7. Start FastAPI Backend & Electron Frontend
# -----------------------------------------------------------------------------
HOST="127.0.0.1"
PORT="8000"

cleanup() {
    echo -e "\n==> Shutting down Kenjin services..."
    pkill -P $$ 2>/dev/null || true
    pkill -f "uvicorn orchestrator.main:app" 2>/dev/null || true
    pkill -f "electron" 2>/dev/null || true
    echo "==> All processes stopped."
}
trap cleanup INT TERM EXIT

echo "==> Starting Orchestrator Backend API (http://${HOST}:${PORT})..."
uvicorn orchestrator.main:app --host "$HOST" --port "$PORT" --env-file .env &

# Wait for API Health
for i in {1..30}; do
    if curl -sf "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
        echo "==> Backend API is active and healthy."
        break
    fi
    sleep 0.5
done

echo "==> Launching Dashboard Frontend via Electron..."
(cd "$ELECTRON_DIR" && npm start -- --api-url="http://${HOST}:${PORT}") &

echo "========================================================================="
echo "  Project Kenjin is live!"
echo "  - Backend API: http://${HOST}:${PORT}"
echo "  - Frontend:    Electron App ($ELECTRON_DIR)"
echo "  Press Ctrl+C to stop all processes."
echo "========================================================================="

wait
