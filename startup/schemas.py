"""
Pydantic API contract schema for v10 orchestrator integration.
These are the API contract between v10 (MQL5) and this
service - field names here must match exactly what v10's PostTick(),
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

    class Config:
        populate_by_name = True


class StrategyParamsOut(BaseModel):
    asset: str
    opt_threshold: float
    opt_sl_mult: float
    opt_tp_mult: float
    rsi_buy_max: float = 70.0    # Dynamic upper RSI ceiling
    rsi_sell_min: float = 30.0   # Dynamic lower RSI floor
    live_approved: bool
    forecast_id: Optional[int] = None
    bullish_prob: Optional[float] = None
    bearish_prob: Optional[float] = None
    tier2_action: str = "HOLD"  # BUY, SELL, or HOLD
    tier2_confidence: float = 0.0
    recommended_lot_multiplier: float = 1.0


class HealthOut(BaseModel):
    status: str
    db: str