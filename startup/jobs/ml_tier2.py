"""
Tier 2 Micro-Trigger Classifier: Rapid evaluation of tick features 
against Macro LLM bias using a LightGBM Triple 'Neural Map' Engine5].
Includes an in-memory caching mechanism and a heuristic fallback for cold starts5].
"""
import os
import time
import json
import joblib
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from db.redis_client import redis_client

# Global cache for the models to prevent disk I/O bottlenecks5]
_model_cache = {}


async def pull_feature_window(symbol: str, window_minutes: int = 15) -> pd.DataFrame:
    """Pulls the rolling tick window from Redis memory5]."""
    redis_key = f"ticks:{symbol}"
    current_time = time.time()
    start_time = current_time - (window_minutes * 60)
    
    # Fetch all ticks between start_time and current_time
    # ZRANGEBYSCORE is O(log(N)+M) - highly efficient5]
    raw_ticks = await redis_client.zrangebyscore(redis_key, start_time, current_time)
    
    if not raw_ticks:
        return pd.DataFrame()  # Return empty DF if no ticks5]
        
    # Deserialize JSON strings back to dictionaries5]
    parsed_ticks = [json.loads(tick) for tick in raw_ticks]
    
    # Convert to Pandas DataFrame for feature engineering5]
    df = pd.DataFrame(parsed_ticks)
    
    # Ensure standard datetime index if required by downstream logic5]
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('timestamp', inplace=True)
    elif 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'])
        df.set_index('ts', inplace=True)
    
    return df


def get_models():
    """Loads models into memory once and reuses them5]."""
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
    """Trains the Triple Neural Map models on historical tick telemetry5]."""
    global _model_cache
    
    if telemetry_df.empty or 'profit' not in telemetry_df.columns:
        return False
        
    # 1. Prepare Target Variables5]
    # Map 1: Toxic regimens (Loss)5]
    telemetry_df['is_loss'] = (telemetry_df['profit'] < 0).astype(int)
    # Map 2: Momentum breakouts (Profit)5]
    telemetry_df['is_profit'] = (telemetry_df['profit'] > 0).astype(int)
    # Map 3: Expected Reward5]
    telemetry_df['reward'] = telemetry_df['profit'].clip(lower=0)
    
    # Align features with the real-time telemetry extraction5]
    features = ['ma_spread_delta', 'price_velocity_1m', 'rsi', 'rvol']
    
    # Ensure columns exist, fill NaNs5]
    for f in features:
        if f not in telemetry_df.columns:
            telemetry_df[f] = 0.0
    telemetry_df.fillna(0, inplace=True)
    
    X = telemetry_df[features]
    
    # 2. Train Map 1: P_loss (Classifier)5]
    y_loss = telemetry_df['is_loss']
    clf_loss = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05)
    clf_loss.fit(X, y_loss)
    
    # 3. Train Map 2: P_profit (Classifier)5]
    y_profit = telemetry_df['is_profit']
    clf_profit = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.05)
    clf_profit.fit(X, y_profit)
    
    # 4. Train Map 3: E_reward (Regressor)5]
    # Only train regressor on winning trades to learn magnitude5]
    win_mask = telemetry_df['is_profit'] == 1
    if win_mask.sum() > 0:
        X_wins = X[win_mask]
        y_reward = telemetry_df.loc[win_mask, 'reward']
        reg_reward = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.05)
        reg_reward.fit(X_wins, y_reward)
    else:
        # Fallback if no winning trades exist yet5]
        reg_reward = lgb.LGBMRegressor(n_estimators=10, learning_rate=0.05)
        reg_reward.fit(X, telemetry_df['reward'])
    
    # 5. Save Models5]
    os.makedirs('models', exist_ok=True)
    joblib.dump(clf_loss, 'models/map_loss.pkl')
    joblib.dump(clf_profit, 'models/map_profit.pkl')
    joblib.dump(reg_reward, 'models/map_reward.pkl')
    
    # Invalidate cache so the newly trained models are loaded on the next call5]
    _model_cache.clear()
    
    return True


def evaluate_tier2_signal(df: pd.DataFrame, bullish_prob: float, bearish_prob: float, opt_threshold: float, risk_value: float = 1.0) -> dict:
    """Evaluates real-time ticks using the Triple Neural Map, falling back to heuristics if untrained5]."""
    if df.empty or len(df) < 5:
        return {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0}

    latest = df.iloc[-1]

    # Standardize real-time features5]
    features_dict = {
        'ma_spread_delta': float(latest.get('ma_spread_delta', 0.0)),
        'price_velocity_1m': float(latest.get('price_velocity_1m', 0.0)),
        'rsi': float(latest.get('rsi', 50.0)),
        'rvol': float(latest.get('rvol', 1.0))
    }
    
    current_features_df = pd.DataFrame([features_dict])
    
    # Fetch cached models5]
    models = get_models()

    if models:
        # --- PRIMARY LOGIC: Use cached LightGBM Triple Neural Map ---5]
        p_loss = models['loss'].predict_proba(current_features_df)[0][1]
        p_profit = models['profit'].predict_proba(current_features_df)[0][1]
        e_reward = models['reward'].predict(current_features_df)[0]
        
        # Mathematical Framework Execution5]
        expected_value = (p_profit * e_reward) - (p_loss * risk_value)
        
        # Micro-Risk Mitigation: Veto if loss probability is too high5]
        if p_loss > 0.40 or expected_value <= opt_threshold:
            action = "HOLD"
        else:
            # Determine direction driven by macro LLM bias5]
            action = "BUY" if bullish_prob > bearish_prob else "SELL"
            
        confidence = p_profit
        lot_multiplier = max(0.5, min(2.5, 1.0 + (expected_value / risk_value)))
        
        return {
            "action": action,
            "confidence": float(round(confidence, 3)),
            "lot_multiplier": float(round(lot_multiplier, 2))
        }

    else:
        # --- FALLBACK LOGIC: Original Heuristic (Pre-Training Phase) ---5]
        # Calculate micro score combining technical momentum + LLM bias5]
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

        # Lot size scaler base on conviction5]
        lot_multiplier = max(0.5, min(2.5, 1.0 + (abs(llm_bias) * 1.5)))

        return {
            "action": action,
            "confidence": float(round(confidence, 3)),
            "lot_multiplier": float(round(lot_multiplier, 2))
        }