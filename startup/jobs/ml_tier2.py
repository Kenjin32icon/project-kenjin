# startup/jobs/ml_tier2.py
"""
Tier 2 Micro-Trigger Classifier (v11.08 Patched): Uses startup.features as the single
source for engineered features on small windows pulled from Redis (fast path).
Ingests missed-trade counterfactual labels and uses recency weighting for retraining cycles.
"""

import os
import time
import json
import joblib
import tempfile
import logging
import numpy as np
import pandas as pd
import lightgbm as lgb
from typing import Dict, List, Optional, Any, Union

from db.redis_client import redis_client
import startup.features as features
from startup.jobs.feature_pull import pull_feature_window, pull_feature_window_bulk
from startup.db import fetch_missed_trade_analytics_rows
from startup.labeling import compute_recency_weighted_labels

log = logging.getLogger("ml_tier2")

LOSS_AVERSION_FACTOR = 2.5
_model_cache = {}

_RAW_FLOAT_COLS = getattr(features, "RAW_NUMERIC_COLS", [
    'bid', 'ask', 'tick_volume', 'rsi', 'tema', 'ac', 'sar', 'adx',
    'ma10', 'ma20', 'ma50', 'ma100', 'ma200'
])

_ONLINE_FEATURES = ['ma_spread_delta', 'price_velocity_1m', 'rsi', 'rvol']
ONLINE_BLEND_WEIGHT = 0.25


# ---------------------
# Fast Redis-based small-window pull
# ---------------------
async def pull_feature_window_redis(symbol: str, window_minutes: int = 15) -> pd.DataFrame:
    """
    Pulls the rolling tick window from Redis (fast path) and computes engineered features.
    Delegates feature engineering to startup.features.base_features() for exact parity.
    """
    redis_key = f"ticks:{symbol}"
    current_time = time.time()
    start_time = current_time - (window_minutes * 60)

    raw_ticks = await redis_client.zrange(redis_key, start_time, current_time, byscore=True)

    if not raw_ticks:
        return pd.DataFrame()

    parsed_ticks = [json.loads(tick) for tick in raw_ticks]
    df = pd.DataFrame(parsed_ticks)

    if len(df) == 0:
        return df

    # Ensure numeric raw columns present and cast to float using canonical list
    for col in _RAW_FLOAT_COLS:
        if col in df.columns:
            try:
                df[col] = df[col].astype(float)
            except Exception:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float).fillna(0.0)
        else:
            df[col] = 0.0

    # Normalize timestamps and sort
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], unit='s', utc=True, errors='coerce')
    else:
        df['ts'] = pd.to_datetime(pd.Series(range(len(df))), unit='s', utc=True)
    df = df.sort_values('ts').reset_index(drop=True)

    coerced = features.coerce_frame(df)
    engineered = features.base_features(coerced)
    return engineered


# ---------------------
# Micro trend calculation
# ---------------------
def compute_micro_trend(df: pd.DataFrame) -> dict:
    """Short-horizon (~1-2 minute) statistical trend read using base_features."""
    if df is None or len(df) < 5:
        return {"micro_trend": "NEUTRAL", "micro_trend_strength": 0.0}

    base = features.base_features(features.coerce_frame(df))
    if base is None or len(base) < 1:
        return {"micro_trend": "NEUTRAL", "micro_trend_strength": 0.0}

    latest = base.iloc[-1]

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


# ---------------------
# Models loading / caching
# ---------------------
def get_models():
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
        log.exception("Failed to load ML models: %s", e)
        return None


def clear_model_cache():
    global _model_cache
    _model_cache.clear()


def compute_reward_utility(profit: pd.Series) -> pd.Series:
    return np.where(profit >= 0, profit, profit * LOSS_AVERSION_FACTOR)


def train_neural_maps(
    telemetry_df: pd.DataFrame,
    risk_value: float = 1.0,
    opt_threshold: float = 0.1,
    sample_weight: Optional[Union[pd.Series, np.ndarray]] = None,
) -> bool:
    """Batch LightGBM training supporting recency sample weights."""
    global _model_cache

    if len(telemetry_df) == 0 or 'profit' not in telemetry_df.columns:
        return False

    telemetry_df['is_loss'] = (telemetry_df['profit'] < 0).astype(int)
    telemetry_df['is_profit'] = (telemetry_df['profit'] > 0).astype(int)
    telemetry_df['reward_utility'] = compute_reward_utility(telemetry_df['profit'])

    features_list = _ONLINE_FEATURES
    for f in features_list:
        if f not in telemetry_df.columns:
            telemetry_df[f] = 0.0
    telemetry_df.fillna(0, inplace=True)

    X = telemetry_df[features_list]

    y_loss = telemetry_df['is_loss']
    clf_loss = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05, class_weight='balanced')
    clf_loss.fit(X, y_loss, sample_weight=sample_weight)

    y_profit = telemetry_df['is_profit']
    clf_profit = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05, class_weight='balanced')
    clf_profit.fit(X, y_profit, sample_weight=sample_weight)

    reg_reward = lgb.LGBMRegressor(n_estimators=150, learning_rate=0.05)
    reg_reward.fit(X, telemetry_df['reward_utility'], sample_weight=sample_weight)

    os.makedirs('models', exist_ok=True)
    mapping = {'map_loss.pkl': clf_loss, 'map_profit.pkl': clf_profit, 'map_reward.pkl': reg_reward}

    try:
        for fname, model_obj in mapping.items():
            final_path = os.path.join('models', fname)
            fd, tmp_path = tempfile.mkstemp(dir='models', prefix=fname + '.', suffix='.tmp')
            os.close(fd)
            joblib.dump(model_obj, tmp_path)
            os.replace(tmp_path, final_path)
        _model_cache.clear()
        return True
    except Exception:
        log.exception("Failed to save ML models atomically")
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


# ---------------------
# Retrain Execution Cycle
# ---------------------
async def run_retrain_cycle(limit: int = 10000) -> bool:
    """
    Ingests missed-trade counterfactual labels with recency weighting,
    pulls engineered feature windows, and retrains LightGBM models.
    """
    missed = await fetch_missed_trade_analytics_rows(limit=limit)
    labels = compute_recency_weighted_labels(missed) if missed else []
    log.info("ml_tier2: retrain cycle started. missed=%d labels=%d", len(missed), len(labels))

    if not labels:
        log.warning("ml_tier2: no counterfactual labels generated. Retrain cycle skipped.")
        return False

    df_augmented = pd.DataFrame(labels) if isinstance(labels, list) else labels
    sample_weight = df_augmented.get('weight', None) if 'weight' in df_augmented.columns else None

    success = train_neural_maps(df_augmented, sample_weight=sample_weight)
    return success


# ---------------------
# OnlineTierUpdater
# ---------------------
class OnlineTierUpdater:
    def __init__(self, redis_state_key_prefix: str = "online_tier2_model"):
        from river import linear_model, preprocessing, optim
        self._prefix = redis_state_key_prefix
        self.model = preprocessing.StandardScaler() | linear_model.LinearRegression(optimizer=optim.SGD(lr=0.01))

    def predict_adjustment(self, features: Dict[str, float]) -> float:
        try:
            return float(self.model.predict_one(features) or 0.0)
        except Exception:
            log.exception("OnlineTierUpdater predict failed, defaulting adjustment to 0.0")
            return 0.0

    def learn(self, features: Dict[str, float], batch_e_utility: float, actual_reward_utility: float) -> None:
        residual = actual_reward_utility - batch_e_utility
        try:
            self.model.learn_one(features, residual)
        except Exception:
            log.exception("OnlineTierUpdater learn_one failed")

    async def persist(self) -> None:
        import pickle
        await redis_client.set(f"{self._prefix}:state", pickle.dumps(self.model))

    @classmethod
    async def load(cls, redis_state_key_prefix: str = "online_tier2_model") -> "OnlineTierUpdater":
        import pickle
        inst = cls(redis_state_key_prefix)
        try:
            raw = await redis_client.get(f"{inst._prefix}:state")
            if raw:
                inst.model = pickle.loads(raw)
        except Exception:
            log.exception("Failed to load OnlineTierUpdater state from Redis - starting fresh")
        return inst


_online_updater: Optional[OnlineTierUpdater] = None


def set_online_updater(updater: OnlineTierUpdater) -> None:
    global _online_updater
    _online_updater = updater


# ---------------------
# Tier2 Signal Evaluation
# ---------------------
def evaluate_tier2_signal(
    df: pd.DataFrame,
    bullish_prob: float,
    bearish_prob: float,
    opt_threshold: float,
    risk_value: float = 1.0,
    calibration_multiplier: float = 1.0,
    lifeline_multiplier: float = 1.0,
    lifeline_terminated: bool = False,
) -> dict:
    """Evaluates ticks using batch models and startup.features.base_features()."""
    if lifeline_terminated:
        return {
            "action": "HOLD",
            "confidence": 0.0,
            "lot_multiplier": 0.0,
            "lifeline_terminated": True,
            "volatility_regime": 0.0,
        }

    if df is None or len(df) < 5:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0, "volatility_regime": 0.0}

    base = features.base_features(features.coerce_frame(df))
    if base is None or len(base) < 1:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0, "volatility_regime": 0.0}

    latest = base.iloc[-1]
    volatility_regime = float(latest.get('volatility_regime', 0.0))
    features_dict = {
        'ma_spread_delta': float(latest.get('ma_spread_delta', 0.0)),
        'price_velocity_1m': float(latest.get('price_velocity_1m', 0.0)),
        'rsi': float(latest.get('rsi', 50.0)),
        'rvol': float(latest.get('rvol', 1.0)),
    }

    models = get_models()

    if models:
        current_features_df = pd.DataFrame([features_dict])
        p_loss = models['loss'].predict_proba(current_features_df)[0][1]
        p_profit = models['profit'].predict_proba(current_features_df)[0][1]
        e_utility = models['reward'].predict(current_features_df)[0]

        online_adjustment = 0.0
        if _online_updater is not None:
            online_adjustment = _online_updater.predict_adjustment(features_dict)
        blended_e_utility = e_utility + (ONLINE_BLEND_WEIGHT * online_adjustment)

        expected_value = (p_profit * max(blended_e_utility, 0.0)) - (p_loss * risk_value)

        if p_loss > 0.40 or expected_value <= opt_threshold:
            action = "HOLD"
        else:
            action = "BUY" if bullish_prob > bearish_prob else "SELL"

        confidence = p_profit
        win_loss_ratio = (max(blended_e_utility, 0.0) / risk_value) if risk_value > 0.0 else 0.01
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
            "volatility_regime": float(round(volatility_regime, 3)),
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
            "volatility_regime": float(round(volatility_regime, 3)),
        }


# ---------------------
# Online Learning Telemetry Record
# ---------------------
async def record_closed_trade_for_online_learning(
    features_map: Dict[str, float],
    batch_e_utility: float,
    actual_profit: float,
) -> None:
    if _online_updater is None:
        return
    actual_reward_utility = compute_reward_utility(pd.Series([actual_profit]))[0]
    _online_updater.learn(features_map, batch_e_utility, float(actual_reward_utility))
    await _online_updater.persist()