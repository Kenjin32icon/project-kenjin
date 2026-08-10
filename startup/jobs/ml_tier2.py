"""
Tier 2 Micro-Trigger Classifier: Rapid evaluation of tick features
against Macro LLM bias using a LightGBM Triple 'Neural Map' Engine.
Includes an in-memory caching mechanism and a heuristic fallback for cold starts.

v11 change: pull_feature_window() below was previously dead code - it read
raw ticks from Redis but returned them WITHOUT computing ma_spread_delta,
price_velocity_1m, or rvol, so nothing in the codebase actually called it
(main.py used the slower Postgres-backed version in feature_pull.py instead,
even though the Redis cache was being populated on every single tick).

This version computes the same engineered features, using the same formulas
as feature_pull.py's Postgres path, so /strategy_params can now use Redis
(sub-millisecond ZRANGEBYSCORE) as the primary hot path with Postgres kept
only as a cold-start/fallback. See orchestrator/main.py's get_strategy_params().
"""
import os
import time
import json
import joblib
import numpy as np
import pandas as pd
import lightgbm as lgb
from db.redis_client import redis_client

# Global cache for the models to prevent disk I/O bottlenecks
_model_cache = {}

# Columns cast to float for downstream math (mirrors feature_pull.py's Postgres path)
_RAW_FLOAT_COLS = ['bid', 'ask', 'tick_volume', 'rsi', 'tema', 'ac', 'sar', 'adx',
                    'ma10', 'ma20', 'ma50', 'ma100', 'ma200']


async def pull_feature_window(symbol: str, window_minutes: int = 15) -> pd.DataFrame:
    """
    Pulls the rolling tick window from Redis (fast path) and computes the same
    engineered features the Postgres path computes, so callers get identical
    columns regardless of which backend served the window.
    """
    redis_key = f"ticks:{symbol}"
    current_time = time.time()
    start_time = current_time - (window_minutes * 60)

    # ZRANGEBYSCORE is O(log(N)+M) - this is the whole point of the Redis cache.
    raw_ticks = await redis_client.zrangebyscore(redis_key, start_time, current_time)

    if not raw_ticks:
        return pd.DataFrame()

    parsed_ticks = [json.loads(tick) for tick in raw_ticks]
    df = pd.DataFrame(parsed_ticks)
    if df.empty:
        return df

    for col in _RAW_FLOAT_COLS:
        if col in df.columns:
            df[col] = df[col].astype(float)
        else:
            df[col] = 0.0

    # v11: cache_tick_to_redis() in main.py now stamps each cached tick with a
    # unix-seconds 'ts' field at write time, so we can reconstruct real elapsed
    # time between ticks here instead of assuming even spacing.
    if 'ts' in df.columns:
        df['ts_dt'] = pd.to_datetime(df['ts'], unit='s')
    else:
        # Cold-start safety: older cached ticks written before this field existed.
        df['ts_dt'] = pd.to_datetime(pd.Series(range(len(df))), unit='s')
    df = df.sort_values('ts_dt').reset_index(drop=True)

    df['mid'] = (df['bid'] + df['ask']) / 2.0
    df['dt_sec'] = df['ts_dt'].diff().dt.total_seconds().fillna(1.0)
    df['dt_sec'] = np.where(df['dt_sec'] <= 0, 1.0, df['dt_sec'])

    df['price_velocity_1m'] = df['mid'].diff(periods=12).fillna(0.0) / df['dt_sec'].rolling(12).sum().fillna(1.0)
    df['price_velocity_5m'] = df['mid'].diff(periods=60).fillna(0.0) / df['dt_sec'].rolling(60).sum().fillna(1.0)

    df['ma_spread'] = df['ma10'] - df['ma50']
    df['ma_spread_delta'] = df['ma_spread'] - df['ma_spread'].shift(5).fillna(0.0)

    mean_vol = df['tick_volume'].rolling(window=30, min_periods=1).mean()
    df['rvol'] = np.where(mean_vol > 0, df['tick_volume'] / mean_vol, 1.0)

    rolling_std = df['mid'].rolling(window=20, min_periods=1).std().fillna(0.0)
    rolling_mean = df['mid'].rolling(window=20, min_periods=1).mean().fillna(1.0)
    df['volatility_regime'] = (rolling_std / rolling_mean) * 10000.0

    return df


def compute_micro_trend(df: pd.DataFrame) -> dict:
    """
    v11.3: Short-horizon (~1-2 minute) statistical trend read, deliberately
    independent of both the 15-minute Groq macro forecast and the LightGBM
    Tier-2 models - pure arithmetic over the same feature window Tier-2
    already has in memory, so it costs nothing extra to compute on every
    /strategy_params call.

    Purpose: give the EA something to check an ALREADY-OPEN position against
    on a much tighter timeline than either of those two run on. Tier-2's
    'action' answers "would I enter this trade right now" - it doesn't track
    whether a trade already in flight is still going the right way on the
    scale of the last minute or two. This does.
    """
    if df.empty or len(df) < 5:
        return {"micro_trend": "NEUTRAL", "micro_trend_strength": 0.0}

    latest = df.iloc[-1]
    v1m = float(latest.get('price_velocity_1m', 0.0))
    ma_delta = float(latest.get('ma_spread_delta', 0.0))
    rvol = float(latest.get('rvol', 1.0))

    raw = 0.0
    if v1m > 0:
        raw += 0.6
    elif v1m < 0:
        raw -= 0.6
    if ma_delta > 0:
        raw += 0.4
    elif ma_delta < 0:
        raw -= 0.4

    # Thin/quiet tick activity makes a short-horizon read less trustworthy -
    # dampen it rather than let a low-volume blip look like a strong signal.
    raw *= min(1.5, max(0.5, rvol))

    strength = min(1.0, abs(raw))
    if raw >= 0.3:
        trend = "BUY"
    elif raw <= -0.3:
        trend = "SELL"
    else:
        trend = "NEUTRAL"

    return {"micro_trend": trend, "micro_trend_strength": round(strength, 3)}


def get_models():
    """Loads models into memory once and reuses them."""
    global _model_cache
    if not _model_cache:
        try:
            _model_cache['loss'] = joblib.load('models/map_loss.pkl')
            _model_cache['profit'] = joblib.load('models/map_profit.pkl')
            _model_cache['reward'] = joblib.load('models/map_reward.pkl')
        except FileNotFoundError:
            return None
    return _model_cache


def train_neural_maps(telemetry_df: pd.DataFrame, risk_value: float = 1.0, opt_threshold: float = 0.1) -> bool:
    """Trains the Triple Neural Map models on historical tick telemetry."""
    global _model_cache

    if telemetry_df.empty or 'profit' not in telemetry_df.columns:
        return False

    # 1. Prepare Target Variables
    telemetry_df['is_loss'] = (telemetry_df['profit'] < 0).astype(int)
    telemetry_df['is_profit'] = (telemetry_df['profit'] > 0).astype(int)
    telemetry_df['reward'] = telemetry_df['profit'].clip(lower=0)

    features = ['ma_spread_delta', 'price_velocity_1m', 'rsi', 'rvol']

    for f in features:
        if f not in telemetry_df.columns:
            telemetry_df[f] = 0.0
    telemetry_df.fillna(0, inplace=True)

    X = telemetry_df[features]

    # 2. Train Map 1: P_loss (Classifier)
    y_loss = telemetry_df['is_loss']
    clf_loss = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05)
    clf_loss.fit(X, y_loss)

    # 3. Train Map 2: P_profit (Classifier)
    y_profit = telemetry_df['is_profit']
    clf_profit = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05)
    clf_profit.fit(X, y_profit)

    # 4. Train Map 3: E_reward (Regressor) - only on winning trades, to learn magnitude
    win_mask = telemetry_df['is_profit'] == 1
    if win_mask.sum() > 0:
        X_wins = X[win_mask]
        y_reward = telemetry_df.loc[win_mask, 'reward']
        reg_reward = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.05)
        reg_reward.fit(X_wins, y_reward)
    else:
        reg_reward = lgb.LGBMRegressor(n_estimators=10, learning_rate=0.05)
        reg_reward.fit(X, telemetry_df['reward'])

    # 5. Save Models
    os.makedirs('models', exist_ok=True)
    joblib.dump(clf_loss, 'models/map_loss.pkl')
    joblib.dump(clf_profit, 'models/map_profit.pkl')
    joblib.dump(reg_reward, 'models/map_reward.pkl')

    _model_cache.clear()
    return True


def evaluate_tier2_signal(df: pd.DataFrame, bullish_prob: float, bearish_prob: float, opt_threshold: float,
                           risk_value: float = 1.0, calibration_multiplier: float = 1.0) -> dict:
    """
    Evaluates real-time ticks using the Triple Neural Map, falling back to heuristics if untrained.

    calibration_multiplier (v11.1, new): supplied by main.py from
    strategy_db.calibration_score/calibration_n, which confidence_calibration.py
    computes from actual trade outcomes vs. the confidence recorded at entry
    time. Defaults to 1.0 (no change) until an asset has enough validated
    history - see confidence_calibration.py for the exact rule. This is what
    makes "the model trusting itself more" evidence-gated rather than just a
    number the model reports about itself: it can only size up toward the
    2.5x ceiling once its own confidence has actually been checked against
    what happened.
    """
    if df.empty or len(df) < 5:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0}

    latest = df.iloc[-1]

    features_dict = {
        'ma_spread_delta': float(latest.get('ma_spread_delta', 0.0)),
        'price_velocity_1m': float(latest.get('price_velocity_1m', 0.0)),
        'rsi': float(latest.get('rsi', 50.0)),
        'rvol': float(latest.get('rvol', 1.0))
    }

    current_features_df = pd.DataFrame([features_dict])
    models = get_models()

    if models:
        p_loss = models['loss'].predict_proba(current_features_df)[0][1]
        p_profit = models['profit'].predict_proba(current_features_df)[0][1]
        e_reward = models['reward'].predict(current_features_df)[0]

        expected_value = (p_profit * e_reward) - (p_loss * risk_value)

        if p_loss > 0.40 or expected_value <= opt_threshold:
            action = "HOLD"
        else:
            action = "BUY" if bullish_prob > bearish_prob else "SELL"

        confidence = p_profit
        lot_multiplier = max(0.5, min(2.5, 1.0 + (expected_value / risk_value)))
        lot_multiplier = max(0.5, min(2.5, lot_multiplier * calibration_multiplier))

        return {
            "action": action,
            "confidence": float(round(confidence, 3)),
            "lot_multiplier": float(round(lot_multiplier, 2))
        }

    else:
        llm_bias = (bullish_prob - bearish_prob) / 100.0
        micro_momentum = 0.0

        ma_delta = features_dict['ma_spread_delta']
        v_1m = features_dict['price_velocity_1m']
        rsi = features_dict['rsi']
        rvol = features_dict['rvol']

        if ma_delta > 0 and v_1m > 0 and rsi > 50:
            micro_momentum += 0.35
        elif ma_delta < 0 and v_1m < 0 and rsi < 50:
            micro_momentum -= 0.35

        if rvol > 1.25:
            micro_momentum *= 1.2

        combined_score = (llm_bias * 0.6) + (micro_momentum * 0.4)
        confidence = min(abs(combined_score), 1.0)

        action = "HOLD"
        if combined_score >= (opt_threshold - 0.10):
            action = "BUY"
        elif combined_score <= -(opt_threshold - 0.10):
            action = "SELL"

        lot_multiplier = max(0.5, min(2.5, 1.0 + (abs(llm_bias) * 1.5)))
        lot_multiplier = max(0.5, min(2.5, lot_multiplier * calibration_multiplier))

        return {
            "action": action,
            "confidence": float(round(confidence, 3)),
            "lot_multiplier": float(round(lot_multiplier, 2))
        }