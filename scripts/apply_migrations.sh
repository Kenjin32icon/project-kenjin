#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ ! -f .env ]]; then echo "ERROR: .env is missing; copy .env.example and set DATABASE_URL" >&2; exit 1; fi
set -a; source .env; set +a
: "${DATABASE_URL:?DATABASE_URL must be configured}"
command -v psql >/dev/null 2>&1 || { echo "ERROR: psql is required (PostgreSQL client tools)." >&2; exit 1; }
echo "Applying canonical schema to ${DATABASE_URL%%@*}@<redacted>"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/schema.sql
for migration in sql/migrations/*.sql; do
  [[ -f "$migration" ]] || continue
  echo "Applying $(basename "$migration")"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration"
done
echo "Schema and migrations applied. Verify with: psql \"\$DATABASE_URL\" -c '\\dt public.*'"
