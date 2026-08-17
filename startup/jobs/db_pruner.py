"""
Database Pruning Job: Prevents table bloat by deleting stale telemetry 
and snapshot records on a recurring schedule.
"""
import logging
from startup.db import get_pool

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