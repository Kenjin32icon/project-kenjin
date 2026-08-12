"""
Pydantic API contract schema for v11 orchestrator integration.
These are the API contract between the MQL5 EA and this
service - field names here must match exactly what the EA's PostTick(),
PostTelemetry(), and FetchStrategyParams() send/expect.
"""
from typing import Optional
from pydantic import BaseModel, Field


class TickIn(BaseModel):
    asset: str
    bid: float
    ask: float
    tick_volume: Optional[float] = None
    rsi: Optional[float] = None
    tema: Optional[float] = None
    ac: Optional[float] = None
    sar: Optional[float] = None
    adx: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma50: Optional[float] = None
    ma100: Optional[float] = None
    ma200: Optional[float] = None


class TelemetryIn(BaseModel):
    asset: str
    type: str  # BUY_CLOSE / SELL_CLOSE / BUY_OPEN / SELL_OPEN
    price: float
    lots: float
    profit: float
    rsi: Optional[float] = None
    entry_score: Optional[float] = Field(default=None, alias="entry_score")
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    magic_number: Optional[int] = None
    account_type: Optional[str] = None  # 'demo' or 'live'
    session_hour: Optional[int] = None
    forecast_id: Optional[int] = None
    tier2_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)  # v11.1: sent by EA

    class Config:
        populate_by_name = True


class StrategyParamsOut(BaseModel):
    asset: str
    opt_threshold: float = Field(..., description="EV threshold required to trigger execution")
    opt_sl_mult: float = Field(..., description="Dynamic Stop Loss multiplier based on rolling win rate")
    opt_tp_mult: float = Field(..., description="Dynamic Take Profit multiplier based on rolling win rate")
    rsi_buy_max: float = Field(70.0, description="Dynamic upper threshold for RSI buys")
    rsi_sell_min: float = Field(30.0, description="Dynamic lower threshold for RSI sells")
    live_approved: bool
    forecast_id: Optional[int] = None
    bullish_prob: Optional[float] = None
    bearish_prob: Optional[float] = None
    tier2_action: str = "HOLD"  # BUY, SELL, or HOLD
    tier2_confidence: float = Field(0.0, ge=0.0, le=1.0)
    recommended_lot_multiplier: float = 1.0
    
    # v11.3: short-horizon (~1-2min) statistical trend, independent of tier2_action
    micro_trend: str = Field("NEUTRAL", description="Short-horizon 1-2 min direction: BULLISH, BEARISH, NEUTRAL")
    micro_trend_strength: float = Field(0.0, ge=0.0, le=1.0, description="Normalized velocity strength (0.0 to 1.0)")
    
    # v11: predictive session scheduling, populated by hour_scheduler.py
    scheduled_start_hour: Optional[int] = Field(None, ge=0, le=23, description="UTC start hour for allowed executions")
    scheduled_end_hour: Optional[int] = Field(None, ge=0, le=23, description="UTC end hour for allowed executions")
    
    # Calibration metrics for risk adjustments
    calibration_score: Optional[float] = Field(None, description="Brier calibration score over rolling 30 days")
    calibration_n: Optional[int] = None


class HealthOut(BaseModel):
    status: str
    db: str