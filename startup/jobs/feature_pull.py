"""
Pulls the last N minutes of tick_telemetry for a given asset and returns a
pandas DataFrame ready to be summarised into the Groq prompt.
"""
import pandas as pd
from startup.db import get_pool


async def get_distinct_assets() -> list[str]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT asset FROM tick_telemetry;")
    return [r["asset"] for r in rows]


async def pull_feature_window(asset: str, minutes: int = 30) -> pd.DataFrame:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200
            FROM tick_telemetry
            WHERE asset = $1 AND ts >= now() - ($2 || ' minutes')::interval
            ORDER BY ts ASC
            """,
            asset, str(minutes),
        )
    return pd.DataFrame([dict(r) for r in rows])
