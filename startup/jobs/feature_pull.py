# startup/jobs/feature_pull.py
"""
Feature Extraction Engine (v11.08 - Polars rewrite, updated to reuse startup.features)
Computes mid, velocity, ma_spread_delta, rvol, volatility_regime and returns a
Polars DataFrame by default. Optionally convert to pandas and run
startup.features.base_features() for exact parity with the server's
single-source-of-truth feature code.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List, Optional

import pandas as pd
import polars as pl

from startup.db import fetch_missed_trade_analytics_rows, get_pool
import startup.features as features  # <-- single source of truth (constants & helpers)

log = logging.getLogger("feature_pull")

# Reuse RAW_NUMERIC_COLS from startup.features to keep decimal->float casting consistent.
_RAW_FLOAT_COLS = getattr(features, "RAW_NUMERIC_COLS", [
    "bid", "ask", "tick_volume", "rsi", "tema", "ac", "sar",
    "adx", "ma10", "ma20", "ma50", "ma100", "ma200",
])

# SELECT columns in DB match the canonical raw numeric names
_SELECT_COLS = "ts, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200"


async def get_distinct_assets() -> list[str]:
    """Retrieves a list of distinct assets from tick_telemetry."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT asset FROM tick_telemetry;")
    return [r["asset"] for r in rows]


def _rows_to_polars(rows) -> pl.DataFrame:
    """
    Converts asyncpg Record rows into a Polars DataFrame and casts numeric
    columns to Float64 up-front using the canonical raw column names.
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
            # cast where possible; strict=False tolerates missing/null-like values
            try:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
            except Exception:
                # defensive fallback if casting fails
                pass
    return df


def _engineer_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Polars-engineered features. Names and definitions match startup.features'
    public expectations (price_velocity_1m/5m, ma_spread_delta, rvol,
    volatility_regime, etc.)
    """
    if df.is_empty():
        return df

    # Build a lazy pipeline and collect at the end
    lf = (
        df.lazy()
        .with_columns(pl.col("ts").cast(pl.Datetime).alias("ts_dt"))
        .sort("ts_dt")
        .with_columns(((pl.col("bid") + pl.col("ask")) / 2.0).alias("mid"))
        .with_columns((pl.col("ts_dt").diff().dt.total_seconds().fill_null(1.0)).alias("dt_sec"))
        .with_columns(pl.when(pl.col("dt_sec") <= 0).then(1.0).otherwise(pl.col("dt_sec")).alias("dt_sec"))
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
        .with_columns((pl.col("ma_spread") - pl.col("ma_spread").shift(5).fill_null(0.0)).alias("ma_spread_delta"))
        .with_columns(pl.col("tick_volume").rolling_mean(window_size=30, min_periods=1).alias("_mean_vol"))
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
        .with_columns(((pl.col("_roll_std") / pl.col("_roll_mean")) * 10000.0).alias("volatility_regime"))
        .drop(["_mean_vol", "_roll_std", "_roll_mean"])
    )
    return lf.collect()


async def pull_feature_window(asset: str, minutes: int = 45, return_pandas: bool = False):
    """
    Pulls the last N minutes of tick_telemetry for a given asset and returns
    a Polars DataFrame enriched with advanced features by default.

    If return_pandas=True, the function converts the Polars engineered frame to
    pandas and then runs startup.features.base_features() to produce the exact
    pandas-based canonical feature frame. This keeps both the fast Polars path
    for analytics and an exact pandas canonical path for any component that
    expects the `startup.features` DataFrame layout.
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
        # Return empty in the requested format
        return pl.DataFrame() if not return_pandas else features.coerce_frame(pd.DataFrame())

    p_df = _engineer_features(df)

    if return_pandas:
        # Convert to pandas and then run canonical base_features (ensures exact parity)
        try:
            pandas_df = p_df.to_pandas()
            # coerce_frame ensures expected columns and dtypes before feature calculations
            pandas_coerced = features.coerce_frame(pandas_df)
            base = features.base_features(pandas_coerced)
            return base
        except Exception:
            log.exception("Failed to convert engineered polars frame to pandas and apply base_features; returning pandas fallback.")
            return p_df.to_pandas()
    else:
        return p_df


async def pull_feature_window_bulk(
    asset: str,
    since_minutes: int,
    until_minutes: int = 0,
    chunk_rows: int = 200_000,
) -> AsyncIterator[pl.DataFrame]:
    """
    Streams large historical ranges in chunks. Each yielded chunk is a Polars
    DataFrame engineered by _engineer_features(). If you need continuous rolling
    stats across boundaries for training, concatenate raw chunks and call
    startup.features._engineer_features once on the combined frame.
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
                batch = rows if isinstance(rows, list) else [rows]
                df = _rows_to_polars(batch)
                if not df.is_empty():
                    yield _engineer_features(df)


def assemble_feature_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converts DB record rows into a pandas DataFrame and normalizes/transforms features."""
    df = pd.DataFrame(rows)
    return df


async def fetch_missed_trade_training_rows(limit: int = 5000) -> pd.DataFrame:
    """Fetches missed-trade analytics rows and structures them into a training feature DataFrame."""
    rows = await fetch_missed_trade_analytics_rows(limit=limit)
    log.info("Fetched %d missed-trade rows for training", len(rows or []))
    return assemble_feature_dataframe(rows)