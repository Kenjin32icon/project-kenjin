"""
Feature Extraction Engine: Computes advanced volatility regimes, 
MA delta spreads, volume anomalies (RVol), and price velocity 
from recent tick telemetry windows.
"""
import pandas as pd
import numpy as np
from startup.db import get_pool


async def get_distinct_assets() -> list[str]:
    """Retrieves a list of distinct assets from tick telemetry."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT asset FROM tick_telemetry;")
    return [r["asset"] for r in rows]


async def pull_feature_window(asset: str, minutes: int = 45) -> pd.DataFrame:
    """
    Pulls the last N minutes of tick_telemetry for a given asset and returns a
    pandas DataFrame enriched with advanced statistical indicators.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200
            FROM tick_telemetry
            WHERE asset = $1 AND ts >= NOW() - ($2 || ' minutes')::INTERVAL
            ORDER BY ts ASC
            """,
            asset, str(minutes),
        )
    
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    
    # Cast all potential Decimal columns from the database to float to prevent type mismatch errors
    cols_to_float = [
        'bid', 'ask', 'tick_volume', 'rsi', 'tema', 'ac', 'sar', 
        'adx', 'ma10', 'ma20', 'ma50', 'ma100', 'ma200'
    ]
    for col in cols_to_float:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Now the math will execute without Decimal vs float type errors
    df['mid'] = (df['bid'] + df['ask']) / 2.0

    # 1. Price Velocity (V = Delta Price / Delta Time in seconds)
    df['ts_dt'] = pd.to_datetime(df['ts'])
    df['dt_sec'] = df['ts_dt'].diff().dt.total_seconds().fillna(1.0)
    df['dt_sec'] = np.where(df['dt_sec'] <= 0, 1.0, df['dt_sec'])
    df['price_velocity_1m'] = df['mid'].diff(periods=12).fillna(0.0) / df['dt_sec'].rolling(12).sum().fillna(1.0)
    df['price_velocity_5m'] = df['mid'].diff(periods=60).fillna(0.0) / df['dt_sec'].rolling(60).sum().fillna(1.0)

    # 2. Moving Average Spread Delta: (MA10 - MA50)_t - (MA10 - MA50)_(t-5)
    df['ma_spread'] = df['ma10'] - df['ma50']
    df['ma_spread_delta'] = df['ma_spread'] - df['ma_spread'].shift(5).fillna(0.0)

    # 3. Relative Volume (RVol = Current Tick Vol / 30-period Mean)
    mean_vol = df['tick_volume'].rolling(window=30, min_periods=1).mean()
    df['rvol'] = np.where(mean_vol > 0, df['tick_volume'] / mean_vol, 1.0)

    # 4. Volatility Regime (Normalized StdDev of High-Low Mid Spreads)
    rolling_std = df['mid'].rolling(window=20, min_periods=1).std().fillna(0.0)
    rolling_mean = df['mid'].rolling(window=20, min_periods=1).mean().fillna(1.0)
    df['volatility_regime'] = (rolling_std / rolling_mean) * 10000.0

    return df