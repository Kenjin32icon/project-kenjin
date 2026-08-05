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
        # CRITICAL if DATABASE_URL is Supabase's pooled connection string
        # (Supavisor/pgbouncer, typically port 6543, "Transaction" mode).
        # asyncpg caches server-side prepared statements by default. In
        # transaction-pooling mode, pgbouncer can hand the next query to a
        # different physical Postgres connection than the one that prepared
        # the statement, so the cached statement name no longer exists there
        # -> asyncpg.exceptions.InvalidSQLStatementNameError, intermittently,
        # on whichever request happens to land on a different connection.
        # This is exactly what caused the /strategy_params 500s and the
        # cascading /health 503s in the logs. Setting this to 0 disables
        # server-side statement caching so every query is sent as a fresh
        # simple/extended query each time - slightly more overhead per call,
        # correctness over micro-optimization for a service this size.
        # (If DATABASE_URL is the DIRECT connection, port 5432/session mode,
        # this isn't strictly required - but it's harmless to leave on, and
        # protects you the moment anyone switches the connection string.)
        statement_cache_size=0,
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