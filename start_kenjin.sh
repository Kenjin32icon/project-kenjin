#!/usr/bin/env bash
#
# start_kenjin.sh — Complete launcher for Project Kenjin Orchestrator & FBS MT5.
#
# Usage:
#   chmod +x start_kenjin.sh
#   ./start_kenjin.sh
#

set -euo pipefail

# -----------------------------------------------------------------------------
# 1. Directory Setup & Path Resolution
# -----------------------------------------------------------------------------
# Absolute root directory: /home/infoscience/Desktop/1. PROJECT KENJIN, SageEyes Predictive Quant Matrix/project-kenjin
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"
ORCH_DIR="$ROOT_DIR/orchestrator"

cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}"
echo "==> Project Root Directory: $ROOT_DIR"

# Ensure orchestrator static directory exists for dashboard.html[cite: 19]
mkdir -p "$ORCH_DIR/static"

# -----------------------------------------------------------------------------
# 2. Process Cleanup (Close all past running instances)
# -----------------------------------------------------------------------------
echo "==> Terminating any previously running instances of Kenjin Orchestrator and MT5..."

# Terminate past Uvicorn / FastAPI orchestrator processes & free port 8000
pkill -f "uvicorn orchestrator.main:app" 2>/dev/null || true
if command -v fuser >/dev/null 2>&1; then
    fuser -k 8000/tcp 2>/dev/null || true
fi

# Terminate past Wine / MetaTrader 5 instances
pkill -f "terminal64.exe" 2>/dev/null || true
pkill -f "FBS MT5" 2>/dev/null || true
pkill -f "wine-stable" 2>/dev/null || true

# Short wait to allow sockets and Wine processes to close down fully
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
# 4. Virtual Environment Activation & Configuration Check
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

# Check for root .env file
if [ ! -f ".env" ]; then
    echo "ERROR: .env file missing in $ROOT_DIR!"
    echo "Ensure DATABASE_URL, ORCH_API_KEY, GROQ_API_KEY, and REDIS_URL are configured in $ROOT_DIR/.env."
    exit 1
fi

# Check for dashboard frontend page
if [ ! -f "$ORCH_DIR/static/dashboard.html" ]; then
    echo "WARNING: Frontend dashboard not found at $ORCH_DIR/static/dashboard.html"
fi

# -----------------------------------------------------------------------------
# 5. Launch FBS MetaTrader 5 via Wine
# -----------------------------------------------------------------------------
echo "==> Launching FBS MetaTrader 5..."
env WINEPREFIX="/home/infoscience/.wine" wine-stable "C:\users\infoscience\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\FBS MetaTrader 5\FBS MT5.lnk" > /dev/null 2>&1 &
MT5_PID=$!
echo "==> FBS MT5 launched (PID: $MT5_PID)."

# -----------------------------------------------------------------------------
# 6. Launch FastAPI Orchestrator & Open Dashboard
# -----------------------------------------------------------------------------
HOST="127.0.0.1"
PORT="8000"

echo "==> Starting Kenjin Orchestrator API (http://${HOST}:${PORT})..."
uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env &
UVICORN_PID=$!

cleanup() {
    echo ""
    echo "==> Shutting down Kenjin Orchestrator (PID: $UVICORN_PID)..."
    kill "$UVICORN_PID" 2>/dev/null || true
    wait "$UVICORN_PID" 2>/dev/null || true
    echo "==> Stopped successfully."
}
trap cleanup INT TERM

# Poll /health endpoint until orchestrator is online (max 15 seconds)
echo "==> Waiting for API health check..."
for i in $(seq 1 30); do
    if curl -sf "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
        echo "==> Orchestrator active and healthy."
        break
    fi
    sleep 0.5
done

# Open dashboard in standard default browser
DASHBOARD_URL="http://${HOST}:${PORT}/static/dashboard.html"
echo "==> Opening KPI Monitor: $DASHBOARD_URL"

if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$DASHBOARD_URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
    open "$DASHBOARD_URL" >/dev/null 2>&1 || true
fi

echo "========================================================================="
echo "  Project Kenjin is live!"
echo "  - API Backend: http://${HOST}:${PORT}"
echo "  - Dashboard:   $DASHBOARD_URL"
echo "  - FBS MT5:     Running under Wine prefix /home/infoscience/.wine"
echo "  Press Ctrl+C to stop the system."
echo "========================================================================="

wait "$UVICORN_PID"