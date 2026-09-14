"""
Tier 2 Micro-Trigger Classifier: Rapid evaluation of tick features
against Macro LLM bias using a LightGBM Triple 'Neural Map' Engine.
Includes an in-memory caching mechanism and a heuristic fallback for cold starts.

STRATEGIC PROFIT LIFELINE UPDATE (see startup/jobs/lifeline_engine.py):
  - The reward regressor ("map_reward.pkl") is trained on EVERY closed trade
    using a signed, loss-averse utility target:
        reward_utility = profit                        if profit >= 0
        reward_utility = profit * LOSS_AVERSION_FACTOR  if profit <  0
  - evaluate_tier2_signal() takes lifeline_multiplier / lifeline_terminated
    so a per-asset "survival economy" gates and scales the signal.
  - Safe atomic model writes using tempfile and os.replace, along with
    a clear_model_cache helper.
"""
import os
import time
import json
import joblib
import tempfile
import numpy as np
import pandas as pd
import lightgbm as lgb
from typing import List, Dict, Tuple
from db.redis_client import redis_client

LOSS_AVERSION_FACTOR = 2.5
_model_cache = {}

_RAW_FLOAT_COLS = ['bid', 'ask', 'tick_volume', 'rsi', 'tema', 'ac', 'sar', 'adx',
                   'ma10', 'ma20', 'ma50', 'ma100', 'ma200']


async def pull_feature_window(symbol: str, window_minutes: int = 15) -> pd.DataFrame:
    """
    Pulls the rolling tick window from Redis (fast path) and computes engineered features.
    """
    redis_key = f"ticks:{symbol}"
    current_time = time.time()
    start_time = current_time - (window_minutes * 60)

    raw_ticks = await redis_client.zrange(redis_key, start_time, current_time, byscore=True)

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

    if 'ts' in df.columns:
        df['ts_dt'] = pd.to_datetime(df['ts'], unit='s')
    else:
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
    Short-horizon (~1-2 minute) statistical trend read independent of macro models.
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
    """Load models into memory on-demand and cache them."""
    global _model_cache
    if _model_cache:
        return _model_cache
    required = ['map_loss.pkl', 'map_profit.pkl', 'map_reward.pkl']
    base = 'models'
    try:
        loaded = {}
        for fname in required:
            path = os.path.join(base, fname)
            loaded_key = fname.replace('.pkl', '').split('map_')[-1]
            loaded[loaded_key] = joblib.load(path)
        _model_cache = loaded
        return _model_cache
    except FileNotFoundError:
        return None
    except Exception as e:
        import logging
        logging.getLogger(__name__).exception("Failed to load ML models: %s", e)
        return None


def clear_model_cache():
    """Clear in-memory model cache so subsequent calls to get_models() reload from disk."""
    global _model_cache
    _model_cache.clear()


def compute_reward_utility(profit: pd.Series) -> pd.Series:
    """
    Signed, loss-averse utility used as the reward regression target.
    """
    return np.where(profit >= 0, profit, profit * LOSS_AVERSION_FACTOR)


def train_neural_maps(telemetry_df: pd.DataFrame, risk_value: float = 1.0, opt_threshold: float = 0.1) -> bool:
    """
    Trains the Triple Neural Map models and writes them atomically to disk using temp files.
    Returns True if training completed and files were written successfully.
    """
    global _model_cache

    if telemetry_df.empty or 'profit' not in telemetry_df.columns:
        return False

    telemetry_df['is_loss'] = (telemetry_df['profit'] < 0).astype(int)
    telemetry_df['is_profit'] = (telemetry_df['profit'] > 0).astype(int)
    telemetry_df['reward_utility'] = compute_reward_utility(telemetry_df['profit'])

    features = ['ma_spread_delta', 'price_velocity_1m', 'rsi', 'rvol']

    for f in features:
        if f not in telemetry_df.columns:
            telemetry_df[f] = 0.0
    telemetry_df.fillna(0, inplace=True)

    X = telemetry_df[features]

    y_loss = telemetry_df['is_loss']
    clf_loss = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05, class_weight='balanced')
    clf_loss.fit(X, y_loss)

    y_profit = telemetry_df['is_profit']
    clf_profit = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05, class_weight='balanced')
    clf_profit.fit(X, y_profit)

    reg_reward = lgb.LGBMRegressor(n_estimators=150, learning_rate=0.05)
    reg_reward.fit(X, telemetry_df['reward_utility'])

    os.makedirs('models', exist_ok=True)

    mapping = {
        'map_loss.pkl': clf_loss,
        'map_profit.pkl': clf_profit,
        'map_reward.pkl': reg_reward
    }

    try:
        for fname, model_obj in mapping.items():
            final_path = os.path.join('models', fname)
            fd, tmp_path = tempfile.mkstemp(dir='models', prefix=fname + '.', suffix='.tmp')
            os.close(fd)
            joblib.dump(model_obj, tmp_path)
            os.replace(tmp_path, final_path)

        _model_cache.clear()
        return True
    except Exception as e:
        import logging
        logging.getLogger(__name__).exception("Failed to save ML models atomically: %s", e)
        try:
            for f in os.listdir('models'):
                if f.endswith('.tmp'):
                    try:
                        os.remove(os.path.join('models', f))
                    except Exception:
                        pass
        except Exception:
            pass
        return False


def evaluate_tier2_signal(df: pd.DataFrame, bullish_prob: float, bearish_prob: float, opt_threshold: float,
                          risk_value: float = 1.0, calibration_multiplier: float = 1.0,
                          lifeline_multiplier: float = 1.0, lifeline_terminated: bool = False) -> dict:
    """
    Evaluates real-time ticks using the Triple Neural Map with EV veto logic,
    calibration, and lifeline gating.
    """
    if lifeline_terminated:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 0.0, "lifeline_terminated": True}

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
        e_utility = models['reward'].predict(current_features_df)[0]
        expected_value = (p_profit * max(e_utility, 0.0)) - (p_loss * risk_value)

        if p_loss > 0.40 or expected_value <= opt_threshold:
            action = "HOLD"
        else:
            action = "BUY" if bullish_prob > bearish_prob else "SELL"

        confidence = p_profit
        win_loss_ratio = (max(e_utility, 0.0) / risk_value) if risk_value > 0.0 else 0.01
        if win_loss_ratio > 0:
            kelly_fraction = p_profit - (p_loss / win_loss_ratio)
        else:
            kelly_fraction = 0.0

        raw_multiplier = 1.0 + (kelly_fraction * 2.0)
        lot_multiplier = raw_multiplier * calibration_multiplier * lifeline_multiplier
        lot_multiplier = max(0.1, min(3.0, lot_multiplier))

        return {
            "action": action,
            "confidence": float(round(confidence, 3)),
            "lot_multiplier": float(round(lot_multiplier, 2)),
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
        lot_multiplier = lot_multiplier * calibration_multiplier * lifeline_multiplier
        lot_multiplier = max(0.1, min(2.5, lot_multiplier))

        return {
            "action": action,
            "confidence": float(round(confidence, 3)),
            "lot_multiplier": float(round(lot_multiplier, 2)),
        }