#!/usr/bin/env bash
#
# start_kenjin.sh — Complete launcher for Project Kenjin Orchestrator,
# FBS MT5 (via Wine) & Electron Frontend App.
#
# DO NOT hardcode a specific username, WINEPREFIX, or Wine binary (e.g.
# /home/infoscience/.wine-kenjin, wine-stable) into the ": ${VAR:=...}"
# defaults below - a prior revision of this script did exactly that and it
# silently breaks on every machine that isn't the one it was written on:
#   - a fallback WINE_BIN of "wine-stable" fails with "command not found"
#     unless that exact binary name was installed (the standard `sudo apt
#     install wine` package installs a binary named `wine`, not
#     `wine-stable` - that name normally only exists if you added the
#     WineHQ repo and installed its staging/stable split packages).
#   - a fallback WINEPREFIX/MT5_SHORTCUT_PATH under another user's $HOME
#     silently launches nothing (or the wrong install) for everyone else.
# Always set WINEPREFIX / WINE_BIN / MT5_SHORTCUT_PATH in .env instead -
# these ":=" defaults exist only so the script doesn't hard-fail when .env
# is momentarily incomplete during first-time setup.
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

    # Same staleness problem as the venv path below, just for conda: an
    # env that already existed from before environment.yml changed never
    # otherwise picks up the change. Hash-gate an `env update` so this stays
    # fast on unchanged runs instead of re-solving every launch.
    ENV_HASH_FILE="$(conda info --base)/envs/${CONDA_ENV_NAME}/.environment.sha256"
    NEW_ENV_HASH="$(sha256sum "$ROOT_DIR/environment.yml" 2>/dev/null | awk '{print $1}')"
    OLD_ENV_HASH="$(cat "$ENV_HASH_FILE" 2>/dev/null || echo "")"
    if [ "$NEW_ENV_HASH" != "$OLD_ENV_HASH" ]; then
        echo "==> environment.yml changed since last run - updating '${CONDA_ENV_NAME}'..."
        conda env update -f "$ROOT_DIR/environment.yml" --prune
        echo "$NEW_ENV_HASH" > "$ENV_HASH_FILE"
    fi
    echo "==> Using Miniconda environment: $CONDA_ENV_NAME ($(which python))"
else
    VENV_PATH="orchestrator/.venv"
    if [ ! -d "$VENV_PATH" ]; then
        echo "==> Creating Python virtual environment at $VENV_PATH..."
        python3 -m venv "$VENV_PATH"
    fi

    # shellcheck disable=SC1091
    source "$VENV_PATH/bin/activate"

    # A venv directory existing is not the same as a working venv - an
    # interrupted first run (Ctrl+C mid `pip install`, disk full, etc.)
    # leaves the directory in place with an incomplete/broken interpreter,
    # and every future run silently "succeeds" at activation while nothing
    # actually works. Fail loudly here instead of surfacing as a random
    # ModuleNotFoundError three steps later.
    if ! command -v python >/dev/null 2>&1; then
        echo "ERROR: $VENV_PATH exists but has no working 'python' executable - it's likely"
        echo "       left over from an interrupted setup. Fix with:"
        echo "         rm -rf $VENV_PATH && ./start_kenjin.sh"
        exit 1
    fi

    # THE ACTUAL FIX for 'ModuleNotFoundError: No module named asyncpg' and
    # its siblings: previously, requirements.txt was only ever installed at
    # the moment the venv was FIRST created - reusing an existing venv on
    # every later run skipped `pip install` entirely, so any dependency
    # added after that venv existed (asyncpg, for instance) was never
    # actually installed into it. Now: hash-gate a real install/sync on
    # every run, so it's both correct (drift is caught, not just at
    # creation time) and fast (skips the pip resolve entirely when nothing
    # changed, instead of paying that cost on every single launch).
    REQ_FILE="orchestrator/requirements.txt"
    REQ_HASH_FILE="$VENV_PATH/.requirements.sha256"
    NEW_REQ_HASH="$(sha256sum "$REQ_FILE" 2>/dev/null | awk '{print $1}')"
    OLD_REQ_HASH="$(cat "$REQ_HASH_FILE" 2>/dev/null || echo "")"
    if [ "$NEW_REQ_HASH" != "$OLD_REQ_HASH" ] || ! python -c "import asyncpg" >/dev/null 2>&1; then
        echo "==> Installing/syncing Python dependencies from $REQ_FILE..."
        pip install --upgrade pip --quiet
        if [ -f "$REQ_FILE" ]; then
            pip install -r "$REQ_FILE"
        fi
        echo "$NEW_REQ_HASH" > "$REQ_HASH_FILE"
    else
        echo "==> Python dependencies already up to date."
    fi
    echo "==> Using Python virtualenv: $(command -v python)"
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
# v11.07 FIX: previously this redirected uvicorn's entire stdout/stderr
# straight to $LOG_DIR/orchestrator.log with nothing shown in the terminal -
# the backend engine's logs (job cycles, request errors, model retrain
# messages, everything) were only ever visible by separately tailing the
# log file, which is easy to not realize you need to do. `> >(tee file)` is
# process substitution, not a real pipe, so `$!` still captures uvicorn's
# own PID (not tee's) - the cleanup() trap below still kills the right
# process. Output now appears live in this terminal AND is still persisted
# to the log file for after-the-fact debugging.
uvicorn orchestrator.main:app --host "$HOST" --port "$PORT" --env-file .env \
    > >(tee "$LOG_DIR/orchestrator.log") 2>&1 &
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