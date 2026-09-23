# startup/jobs/db_pruner.py
"""
Database Pruning Job: Prevents table bloat by deleting stale telemetry,
ticks, and snapshot records on a recurring schedule.
"""
import logging
from startup.db import get_pool, run_prune_query

log = logging.getLogger("db_pruner")


async def run_snapshot_pruning_cycle() -> None:
    """
    Runs on a daily cadence to remove account snapshots older than 7 days.
    """
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            # Execute deletion for records older than 7 days
            result = await conn.execute(
                "DELETE FROM account_snapshots WHERE ts < NOW() - INTERVAL '7 days'"
            )

            # asyncpg execute() returns a status string like 'DELETE 150'
            deleted_count = result.split(" ")[-1] if result.startswith("DELETE") else "0"
            log.info(f"DB Pruner: Removed {deleted_count} stale account snapshots.")

    except Exception as e:
        log.exception(f"Failed to prune account_snapshots: {e}")


async def prune_ticks_retention(days: int = 30) -> None:
    """
    Prunes records from the 'ticks' table older than the specified retention period (default 30 days).
    """
    try:
        safe_days = int(days)
        sql = f"DELETE FROM ticks WHERE ts < NOW() - INTERVAL '{safe_days} days'"
        await run_prune_query(sql)
        log.info("DB Pruner: Pruned ticks older than %d days.", safe_days)
    except Exception as e:
        log.exception(f"Failed to prune ticks table: {e}")


async def run_all_pruning_cycles() -> None:
    """
    Master APScheduler entry point to execute all database cleanup passes.
    """
    await run_snapshot_pruning_cycle()
    await prune_ticks_retention(days=30)