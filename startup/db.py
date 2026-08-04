"""
Async Postgres connection pool.

Points at whatever DATABASE_URL is set to - your Supabase project's direct
Postgres connection string (the same one verified in DBeaver), NOT the
Supabase REST/anon URL. Connecting with that URL's role (normally `postgres`)
is what lets this service satisfy the RLS policies that only allow
`service_role`/`postgres` to touch tick_telemetry and forecasts, and to write
strategy_db - the same tables the EA's old anon key could never fully reach.
"""
import os
import asyncpg
from typing import Optional

_pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> asyncpg.Pool:
    global _pool
    database_url = os.environ["DATABASE_URL"]
    _pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=10,
        command_timeout=10,
    )
    return _pool


async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError(
            "DB pool not initialised - init_db_pool() must run in the FastAPI "
            "startup event before any request handler uses get_pool()."
        )
    return _pool
