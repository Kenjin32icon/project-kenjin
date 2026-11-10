# startup/jobs/db_pruner.py
"""
Database pruning (v12). v11.09 imported `run_prune_query` from startup.db, which did not
exist -> ImportError inside main.py's scheduler block -> NO background job was ever scheduled.
`run_prune_query` now exists in startup/db.py and the retention list is data-driven.
"""
import logging
from startup.db import get_pool, run_prune_query

log = logging.getLogger("db_pruner")

# (table, timestamp column, retention days) - day counts are int-validated constants, never user input
RETENTION = [
    ("account_snapshots", "ts", 7),
    ("tick_telemetry", "ts", 30),
    ("guardian_actions", "created_at", 90),
    ("market_context_snapshots", "ts", 14),
]


async def run_snapshot_pruning_cycle() -> None:
    await run_all_pruning_cycles()


async def prune_ticks_retention(days: int = 30) -> None:
    await run_prune_query("tick_telemetry", "ts", int(days))


async def run_all_pruning_cycles() -> None:
    for table, col, days in RETENTION:
        try:
            deleted = await run_prune_query(table, col, int(days))
            log.info("DB Pruner: removed %s rows from %s older than %d days.", deleted, table, days)
        except Exception as exc:
            log.exception("Failed to prune %s: %s", table, exc)
