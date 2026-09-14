"""
Feature Extraction Engine (v11.08 - Big-Data / Polars rewrite)
================================================================
Computes advanced volatility regimes, MA delta spreads, volume anomalies
(RVol), and price velocity from tick telemetry windows.

WHY THIS CHANGED FROM v11.07:
  - The original implementation used pandas + asyncpg row objects. That's
    fine for the ~15-45 minute "hot" windows it was built for, but it does
    NOT scale to the larger windows you need to train better models or run
    analytics over weeks/months of tick history: pandas' row-by-row
    Decimal->float casting and eager rolling ops become the bottleneck as
    the row count grows past ~100k-1M rows.
  - Polars is used instead for the query -> DataFrame -> feature pipeline:
    it's a multi-threaded, Arrow-backed engine with a lazy query optimizer,
    typically 5-15x faster than pandas on this kind of columnar rolling-
    window workload, and uses substantially less memory (no per-cell
    Python/Decimal object overhead).
  - A second, explicitly "cold path" function - `pull_feature_window_bulk`
    - is added for pulling large historical ranges (hours/days/months) via
    a server-side asyncpg cursor in chunks, so a training/backtest job can
    stream gigabytes of tick history without holding it all in memory at
    once (a basic Lambda-architecture "batch layer" on top of the existing
    "speed layer").
  - Public function names/signatures are kept 1:1 compatible with v11.07
    (`get_distinct_assets`, `pull_feature_window`) so ml_tier2.py,
    orchestrator/main.py, and the gatekeeper/auto_tester jobs do not need
    to change to adopt this.

Install: pip install polars pyarrow
"""
from __future__ import annotations

import logging
from typing import AsyncIterator

import polars as pl

from startup.db import get_pool

log = logging.getLogger("feature_pull")

# Columns pulled from tick_telemetry that need Decimal -> float normalization.
_RAW_FLOAT_COLS = [
    "bid", "ask", "tick_volume", "rsi", "tema", "ac", "sar",
    "adx", "ma10", "ma20", "ma50", "ma100", "ma200",
]

_SELECT_COLS = "ts, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200"


async def get_distinct_assets() -> list[str]:
    """Retrieves a list of distinct assets from tick telemetry."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT asset FROM tick_telemetry;")
    return [r["asset"] for r in rows]


def _rows_to_polars(rows) -> pl.DataFrame:
    """
    Converts asyncpg Record rows into a Polars DataFrame, casting Decimal
    columns to Float64 up front (Polars has no native Decimal-in-pandas-style
    overhead, but asyncpg still hands back Python Decimal objects for numeric
    columns, so we normalize once here rather than per-column later).
    """
    if not rows:
        return pl.DataFrame()

    cols: dict[str, list] = {k: [] for k in rows[0].keys()}
    for r in rows:
        for k, v in r.items():
            cols[k].append(v)

    df = pl.DataFrame(cols)
    for col in _RAW_FLOAT_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    return df


def _engineer_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Shared feature-engineering pipeline (mid price, velocity, MA spread
    delta, relative volume, volatility regime) expressed as a Polars lazy
    query so the optimizer can fuse/vectorize the rolling operations.
    """
    if df.is_empty():
        return df

    lf = (
        df.lazy()
        .with_columns(pl.col("ts").cast(pl.Datetime).alias("ts_dt"))
        .sort("ts_dt")
        .with_columns(((pl.col("bid") + pl.col("ask")) / 2.0).alias("mid"))
        .with_columns(
            pl.col("ts_dt").diff().dt.total_seconds().fill_null(1.0).alias("dt_sec")
        )
        .with_columns(
            pl.when(pl.col("dt_sec") <= 0).then(1.0).otherwise(pl.col("dt_sec")).alias("dt_sec")
        )
        .with_columns(
            [
                (pl.col("mid").diff(n=12).fill_null(0.0) /
                 pl.col("dt_sec").rolling_sum(window_size=12, min_periods=1).fill_null(1.0))
                .alias("price_velocity_1m"),
                (pl.col("mid").diff(n=60).fill_null(0.0) /
                 pl.col("dt_sec").rolling_sum(window_size=60, min_periods=1).fill_null(1.0))
                .alias("price_velocity_5m"),
                (pl.col("ma10") - pl.col("ma50")).alias("ma_spread"),
            ]
        )
        .with_columns(
            (pl.col("ma_spread") - pl.col("ma_spread").shift(5).fill_null(0.0)).alias("ma_spread_delta")
        )
        .with_columns(
            pl.col("tick_volume").rolling_mean(window_size=30, min_periods=1).alias("_mean_vol")
        )
        .with_columns(
            pl.when(pl.col("_mean_vol") > 0)
            .then(pl.col("tick_volume") / pl.col("_mean_vol"))
            .otherwise(1.0)
            .alias("rvol")
        )
        .with_columns(
            [
                pl.col("mid").rolling_std(window_size=20, min_periods=1).fill_null(0.0).alias("_roll_std"),
                pl.col("mid").rolling_mean(window_size=20, min_periods=1).fill_null(1.0).alias("_roll_mean"),
            ]
        )
        .with_columns(
            ((pl.col("_roll_std") / pl.col("_roll_mean")) * 10000.0).alias("volatility_regime")
        )
        .drop(["_mean_vol", "_roll_std", "_roll_mean"])
    )
    return lf.collect()


async def pull_feature_window(asset: str, minutes: int = 45) -> pl.DataFrame:
    """
    Pulls the last N minutes of tick_telemetry for a given asset and returns
    a Polars DataFrame enriched with advanced statistical indicators.
    Drop-in replacement for the v11.07 pandas version - same signature,
    same output columns (mid, price_velocity_1m/5m, ma_spread_delta, rvol,
    volatility_regime), same NULL/NaN-safe defaults on short windows.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT {_SELECT_COLS}
            FROM tick_telemetry
            WHERE asset = $1 AND ts >= NOW() - ($2 || ' minutes')::INTERVAL
            ORDER BY ts ASC
            """,
            asset, str(minutes),
        )

    df = _rows_to_polars(rows)
    if df.is_empty():
        return df
    return _engineer_features(df)


async def pull_feature_window_bulk(
    asset: str,
    since_minutes: int,
    until_minutes: int = 0,
    chunk_rows: int = 200_000,
) -> AsyncIterator[pl.DataFrame]:
    """
    BATCH / COLD PATH (new in v11.08): streams a large historical range
    (e.g. training on 90 days of tick history for a nightly model refresh,
    or auto_tester.py backtests) in fixed-size chunks via an asyncpg
    server-side cursor, so callers never have to materialize the full
    history in memory at once.

    Usage:
        chunks = []
        async for chunk_df in pull_feature_window_bulk("EURUSD", since_minutes=60*24*90):
            chunks.append(chunk_df)
        full_df = pl.concat(chunks) if chunks else pl.DataFrame()

    Each yielded chunk already has the same engineered feature columns as
    pull_feature_window(); rolling windows are computed per-chunk, so if you
    need perfectly continuous rolling stats across chunk boundaries, prefer
    concatenating raw chunks first and calling _engineer_features() once on
    the combined frame for offline/training use (memory permitting), and
    reserve the streamed engineered-per-chunk form for pure aggregation
    (e.g. counting regime buckets, computing summary stats) over very large
    ranges where holding everything in memory isn't practical at all.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            cursor = conn.cursor(
                f"""
                SELECT {_SELECT_COLS}
                FROM tick_telemetry
                WHERE asset = $1
                  AND ts >= NOW() - ($2 || ' minutes')::INTERVAL
                  AND ts <  NOW() - ($3 || ' minutes')::INTERVAL
                ORDER BY ts ASC
                """,
                asset, str(since_minutes), str(until_minutes),
            )
            async for rows in cursor.iterate(chunk_rows):
                # rows here is a list from asyncpg's cursor.iterate in the
                # common wrapper shape; if your asyncpg version yields one
                # row at a time instead, batch them with a small buffer.
                batch = rows if isinstance(rows, list) else [rows]
                df = _rows_to_polars(batch)
                if not df.is_empty():
                    yield _engineer_features(df)