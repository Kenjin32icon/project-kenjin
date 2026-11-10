from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def test_canonical_schema_has_runtime_tables_and_columns():
    sql = (ROOT / "sql/schema.sql").read_text().lower()
    for table in ("strategy_db", "account_snapshots", "forecasts", "tick_telemetry",
                  "trade_telemetry", "missed_trade_signals", "risk_incidents",
                  "account_controls", "guardian_actions", "market_context_snapshots",
                  "strategy_lifeline", "autotune_log"):
        assert f"create table if not exists public.{table}" in sql
    assert "day_start_balance numeric" in sql
    assert "entry_features jsonb" in sql
    assert "entry_e_utility double precision" in sql

def test_duplicate_baseline_not_in_migrations():
    assert not (ROOT / "sql/migrations/schema.sql").exists()
