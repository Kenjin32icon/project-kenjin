"""
Pydantic API contract (v12) between the MQL5 EA and the orchestrator.
Field names must match what the EA sends/expects exactly.
"""
from typing import Dict, List, Optional
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
    entry_score: Optional[float] = None
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    magic_number: Optional[int] = None
    account_type: Optional[str] = None
    session_hour: Optional[int] = None
    forecast_id: Optional[int] = None
    tier2_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)

    model_config = {"populate_by_name": True}


class StrategyParamsOut(BaseModel):
    asset: str
    opt_threshold: float
    opt_sl_mult: float
    opt_tp_mult: float
    rsi_buy_max: float = 70.0
    rsi_sell_min: float = 30.0
    live_approved: bool
    forecast_id: Optional[int] = None
    bullish_prob: Optional[float] = None
    bearish_prob: Optional[float] = None
    tier2_action: str = "HOLD"
    tier2_confidence: float = Field(0.0, ge=0.0, le=1.0)
    recommended_lot_multiplier: float = 1.0
    micro_trend: str = Field("NEUTRAL", description="BUY / SELL / NEUTRAL")
    micro_trend_strength: float = Field(0.0, ge=0.0, le=1.0)
    scheduled_start_hour: Optional[int] = Field(None, ge=0, le=23)
    scheduled_end_hour: Optional[int] = Field(None, ge=0, le=23)
    calibration_score: Optional[float] = None
    calibration_n: Optional[int] = None
    volatility_regime: float = 0.0
    lifeline_score: Optional[float] = None
    lifeline_terminated: bool = False
    # ---- v12 ----
    trading_halted: bool = Field(False, description="Operator manual halt (dashboard) for this account type")
    halt_reason: Optional[str] = None
    market_bias: float = Field(0.0, ge=-1.0, le=1.0, description="Yahoo context bias, + favours long")
    market_regime: str = "unknown"
    market_fresh: bool = False
    tier2_note: Optional[str] = None


class HealthOut(BaseModel):
    status: str
    db: str
    redis: str = "unknown"
    groq: str = "unknown"
    yahoo: str = "unknown"
    mt5_last_seen_seconds: Optional[float] = None


class AccountSnapshotIn(BaseModel):
    account_type: str
    login: Optional[int] = None
    asset: Optional[str] = None
    balance: float
    equity: float
    margin: Optional[float] = None
    margin_level: Optional[float] = None
    floating_pl: Optional[float] = None
    peak_equity: Optional[float] = None
    drawdown_pct: Optional[float] = None
    day_loss_pct: Optional[float] = None
    day_start_balance: Optional[float] = None   # v12: EA now sends dayStartEquity
    consecutive_losses: Optional[int] = None
    consecutive_wins: Optional[int] = None
    risk_cooldown_active: Optional[bool] = None
    drawdown_halt: Optional[bool] = None


class RiskIncidentIn(BaseModel):
    account_type: str
    asset: Optional[str] = None
    reason: str
    details: Optional[str] = None


class MissedTradeIn(BaseModel):
    asset: str
    account_type: Optional[str] = None
    reason: str
    side: str
    score: Optional[float] = None
    raw_score: Optional[float] = None
    threshold: Optional[float] = None
    rsi: Optional[float] = None
    price: Optional[float] = None
    atr: Optional[float] = None
    sl_distance: Optional[float] = None
    tp_distance: Optional[float] = None
    tier2_action: Optional[str] = None
    tier2_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    session_hour: Optional[int] = None
    magic_number: Optional[int] = None


class PositionReportIn(BaseModel):
    """POST /guardian/evaluate - sent by the EA every few seconds per open position."""
    asset: str
    ticket: int
    side: str                                   # BUY / SELL
    account_type: Optional[str] = None
    open_price: float
    price: float
    sl: float = 0.0
    tp: float = 0.0
    lots: float = 0.0
    profit: float = 0.0
    r_multiple: float                           # profit in units of the INITIAL stop distance
    peak_r: float = 0.0
    initial_sl_distance: float = 0.0
    open_seconds: float = 0.0
    max_hold_seconds: float = 3600.0
    momentum_against: bool = False
    rsi: Optional[float] = None


class GuardianDecisionOut(BaseModel):
    action: str = "HOLD"                        # HOLD | CLOSE | MOVE_SL
    reason: str = "ok"
    new_sl: float = 0.0
    r_multiple: float = 0.0
    against_votes: int = 0
    votes: List[str] = []
    market_bias: float = 0.0
    slope_r_min: Optional[float] = None
    peak_r: float = 0.0


class MarketContextOut(BaseModel):
    fresh: bool
    regime: str
    summary: str = ""
    macro: Dict[str, dict] = {}
    assets: Dict[str, dict] = {}
    ts: float = 0.0
