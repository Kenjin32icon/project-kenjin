#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${KENJIN_BASE_URL:-http://127.0.0.1:8000}"
KEY="${ORCH_API_KEY:-}"
if [[ -z "$KEY" ]]; then echo "Set ORCH_API_KEY in the environment first." >&2; exit 2; fi
curl --fail --silent --show-error "$BASE_URL/health" | python -m json.tool
curl --fail --silent --show-error -H "X-API-Key: $KEY" "$BASE_URL/strategy_params?asset=EURUSD&account_type=demo" | python -m json.tool
