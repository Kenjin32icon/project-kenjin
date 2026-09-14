"""
Tier 2 Micro-Trigger Classifier (v11.08): Rapid evaluation of tick features
against Macro LLM bias using a LightGBM Triple 'Neural Map' Engine, now with
an online-learning layer for faster adaptation and a passthrough of
`volatility_regime` for the EA's adaptive trading-frequency logic.
Includes an in-memory caching mechanism and a heuristic fallback for cold starts.

STRATEGIC PROFIT LIFELINE (from v11.07 - unchanged):
  - The reward regressor ("map_reward.pkl") is trained on EVERY closed trade
    using a signed, loss-averse utility target.
  - evaluate_tier2_signal() takes lifeline_multiplier / lifeline_terminated
    so a per-asset "survival economy" gates and scales the signal.

BIG-DATA / ADAPTIVE-FREQUENCY UPDATE (v11.08):
  - `volatility_regime` (already computed by feature_pull.py) is now carried
    through evaluate_tier2_signal()'s return dict and surfaced by
    orchestrator/main.py's /strategy_params response, so
    MAPSAR_ProfitEngine.mq5's fast-lane cadence logic has a live read on
    whether the market is actually moving enough to justify trading faster.
  - NEW: `OnlineTierUpdater`, a River-based incremental model that updates
    after every closed trade in O(1) time (no LightGBM refit needed). The
    full LightGBM retrain (train_neural_maps, run on gatekeeper's ~4h
    cadence / auto_tester's schedule) remains the source of truth for
    map_loss/map_profit/map_reward.pkl; the online model produces a fast,
    continuously-fresh *adjustment* to expected value that's blended in
    between full retrains, rather than trading raw LightGBM staleness for
    higher frequency.

Install: pip install river   (LightGBM/joblib/pandas/numpy already required)
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
from typing import Dict
from db.redis_client import redis_client

log = logging.getLogger("ml_tier2")

LOSS_AVERSION_FACTOR = 2.5
_model_cache = {}

_RAW_FLOAT_COLS = ['bid', 'ask', 'tick_volume', 'rsi', 'tema', 'ac', 'sar', 'adx',
                   'ma10', 'ma20', 'ma50', 'ma100', 'ma200']

_ONLINE_FEATURES = ['ma_spread_delta', 'price_velocity_1m', 'rsi', 'rvol']

# How much weight the fast online adjustment gets vs the batch LightGBM
# expected_value. Kept modest and configurable - the online model is a
# freshness correction, not a replacement for the validated batch model.
ONLINE_BLEND_WEIGHT = 0.25


async def pull_feature_window(symbol: str, window_minutes: int = 15) -> pd.DataFrame:
    """
    Pulls the rolling tick window from Redis (fast path) and computes engineered features.
    Unchanged from v11.07 - this window is small (~15 min of ticks for one
    asset) so pandas is not the bottleneck here; the big-data rewrite targets
    startup/jobs/feature_pull.py's larger Postgres-backed windows instead.
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
    """Short-horizon (~1-2 minute) statistical trend read independent of macro models. Unchanged."""
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
    """Load models into memory on-demand and cache them. Unchanged."""
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


def train_neural_maps(telemetry_df: pd.DataFrame, risk_value: float = 1.0, opt_threshold: float = 0.1) -> bool:
    """Batch LightGBM training - unchanged from v11.07 (source of truth model)."""
    global _model_cache

    if telemetry_df.empty or 'profit' not in telemetry_df.columns:
        return False

    telemetry_df['is_loss'] = (telemetry_df['profit'] < 0).astype(int)
    telemetry_df['is_profit'] = (telemetry_df['profit'] > 0).astype(int)
    telemetry_df['reward_utility'] = compute_reward_utility(telemetry_df['profit'])

    features = _ONLINE_FEATURES
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


# ---------------------------------------------------------------------------
# NEW v11.08: online / incremental learning layer (River)
# ---------------------------------------------------------------------------
class OnlineTierUpdater:
    """
    Incremental expected-value adjustment model. Updates in O(1) per closed
    trade (no refit of the whole dataset), so the signal can react to intraday
    regime shifts well before the next scheduled LightGBM retrain.

    This deliberately predicts a *residual* against the batch model's
    expected_value (actual reward_utility - batch e_utility), not profit
    directly - it's a correction layer, not a second source of truth. That
    keeps a misbehaving/underfit online model from being able to push the
    combined signal far from what the validated batch model says.
    """

    def __init__(self, redis_state_key_prefix: str = "online_tier2_model"):
        from river import linear_model, preprocessing, optim

        self._prefix = redis_state_key_prefix
        self.model = preprocessing.StandardScaler() | linear_model.LinearRegression(
            optimizer=optim.SGD(lr=0.01)
        )

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
        """Serialize model state to Redis so it survives orchestrator restarts."""
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


# Module-level singleton, lazily populated by orchestrator startup (lifespan
# hook) via `await OnlineTierUpdater.load()`. Kept as a plain global (like
# _model_cache above) to match this file's existing style rather than
# introducing a DI framework for one object.
_online_updater: "OnlineTierUpdater | None" = None


def set_online_updater(updater: "OnlineTierUpdater") -> None:
    global _online_updater
    _online_updater = updater


def evaluate_tier2_signal(df: pd.DataFrame, bullish_prob: float, bearish_prob: float, opt_threshold: float,
                          risk_value: float = 1.0, calibration_multiplier: float = 1.0,
                          lifeline_multiplier: float = 1.0, lifeline_terminated: bool = False) -> dict:
    """
    Evaluates real-time ticks using the Triple Neural Map with EV veto logic,
    calibration, lifeline gating, and (v11.08) an online-learning EV
    adjustment + volatility_regime passthrough for the EA's adaptive
    trading-frequency ("fast lane") logic.
    """
    if lifeline_terminated:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 0.0,
                "lifeline_terminated": True, "volatility_regime": 0.0}

    if df.empty or len(df) < 5:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0, "volatility_regime": 0.0}

    latest = df.iloc[-1]
    volatility_regime = float(latest.get('volatility_regime', 0.0))
    features_dict = {
        'ma_spread_delta': float(latest.get('ma_spread_delta', 0.0)),
        'price_velocity_1m': float(latest.get('price_velocity_1m', 0.0)),
        'rsi': float(latest.get('rsi', 50.0)),
        'rvol': float(latest.get('rvol', 1.0)),
    }

    current_features_df = pd.DataFrame([features_dict])
    models = get_models()

    if models:
        p_loss = models['loss'].predict_proba(current_features_df)[0][1]
        p_profit = models['profit'].predict_proba(current_features_df)[0][1]
        e_utility = models['reward'].predict(current_features_df)[0]

        # v11.08: blend in the fast online-learning residual adjustment, if
        # a persisted online model is available. This is what lets the
        # expected-value gate react to an emerging intraday regime shift
        # without waiting for the next scheduled LightGBM retrain -
        # supporting higher, but still EV-gated, trading frequency.
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


async def record_closed_trade_for_online_learning(features: Dict[str, float], batch_e_utility: float,
                                                    actual_profit: float) -> None:
    """
    Call this from wherever v11.07 already records a closed trade's outcome
    (e.g. the same place trade_telemetry gets the closing profit written).
    Updates the online model in O(1) and persists it to Redis every call -
    cheap enough at trade-close frequency, not per-tick.
    """
    if _online_updater is None:
        return
    actual_reward_utility = compute_reward_utility(pd.Series([actual_profit]))[0]
    _online_updater.learn(features, batch_e_utility, float(actual_reward_utility))
    await _online_updater.persist()