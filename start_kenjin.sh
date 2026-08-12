#!/usr/bin/env bash
#
# start_kenjin.sh — opens and runs Project Kenjin's orchestrator.
#
# Usage:
#   chmod +x start_kenjin.sh
#   ./start_kenjin.sh
#
# What it does, in order:
#   1. Confirms it's being run from (or can find) the orchestrator/ directory
#   2. Starts redis-server if it isn't already running (best-effort, works
#      with either a systemd-managed redis or a plain background process)
#   3. Activates the Python venv (creates one on first run if missing)
#   4. Confirms .env exists (refuses to start without it - better than
#      starting with silently-missing config)
#   5. Launches uvicorn in the foreground
#   6. Opens the KPI dashboard in your default browser once the server is up
#   7. On Ctrl+C, shuts down cleanly
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Allow running this script from the repo root OR from inside orchestrator/
if [ -f "$SCRIPT_DIR/main.py" ]; then
    ORCH_DIR="$SCRIPT_DIR"
elif [ -f "$SCRIPT_DIR/orchestrator/main.py" ]; then
    ORCH_DIR="$SCRIPT_DIR/orchestrator"
else
    echo "ERROR: couldn't find main.py in '$SCRIPT_DIR' or '$SCRIPT_DIR/orchestrator'."
    echo "Place this script either next to main.py, or one level above the orchestrator/ folder."
    exit 1
fi

cd "$ORCH_DIR"
echo "==> Working directory: $ORCH_DIR"

# --- 1. Redis ---------------------------------------------------------
if command -v redis-cli >/dev/null 2>&1 && redis-cli ping >/dev/null 2>&1; then
    echo "==> Redis already running."
elif command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet redis-server 2>/dev/null; then
    echo "==> Redis already running (systemd)."
elif command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files 2>/dev/null | grep -q redis-server; then
    echo "==> Starting Redis via systemd..."
    sudo systemctl start redis-server
elif command -v redis-server >/dev/null 2>&1; then
    echo "==> Starting Redis as a background process (no systemd unit found)..."
    redis-server --daemonize yes
else
    echo "WARNING: redis-server not found on PATH. The Redis-fast-path in ml_tier2.py"
    echo "         will fail and the orchestrator will fall back to the slower Postgres"
    echo "         path automatically - not fatal, but install redis-server for full speed."
fi

# --- 2. Python venv -----------------------------------------------------
if [ ! -d ".venv" ]; then
    echo "==> No .venv found - creating one and installing requirements (first run only)..."
    python3 -m venv .venv
    # shellcheck disable=SC1091
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
else
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi
echo "==> Using Python: $(which python)"

# --- 3. .env check -------------------------------------------------------
if [ ! -f ".env" ]; then
    echo "ERROR: .env not found in $ORCH_DIR."
    echo "Create it with at least DATABASE_URL, ORCH_API_KEY, GROQ_API_KEY, REDIS_URL before running this script."
    exit 1
fi

# --- 4. Ensure static/ exists (for the KPI dashboard) --------------------
mkdir -p static
if [ ! -f "static/dashboard.html" ]; then
    echo "NOTE: static/dashboard.html not found - the /static/dashboard.html KPI"
    echo "      dashboard route will 404 until you place it there. The API itself"
    echo "      will still start and run fine without it."
fi

# --- 5. Launch uvicorn, then open the dashboard once it's reachable ------
HOST="${KENJIN_HOST:-127.0.0.1}"
PORT="${KENJIN_PORT:-8000}"

echo "==> Starting orchestrator on http://${HOST}:${PORT} ..."
uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env &
UVICORN_PID=$!

cleanup() {
    echo ""
    echo "==> Shutting down orchestrator (pid $UVICORN_PID)..."
    kill "$UVICORN_PID" 2>/dev/null || true
    wait "$UVICORN_PID" 2>/dev/null || true
    echo "==> Stopped."
}
trap cleanup INT TERM

# Wait for /health to respond before trying to open the browser (max ~15s)
for i in $(seq 1 30); do
    if curl -sf "http://${HOST}:${PORT}/health" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

DASHBOARD_URL="http://${HOST}:${PORT}/static/dashboard.html"
if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$DASHBOARD_URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
    open "$DASHBOARD_URL" >/dev/null 2>&1 || true
else
    echo "==> Open this in your browser: $DASHBOARD_URL"
fi

echo "==> Kenjin is running. Dashboard: $DASHBOARD_URL"
echo "==> Press Ctrl+C to stop."

wait "$UVICORN_PID"
