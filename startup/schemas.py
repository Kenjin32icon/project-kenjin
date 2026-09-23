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
    tier2_action: str = "HOLD"
    tier2_confidence: float = Field(0.0, ge=0.0, le=1.0)
    recommended_lot_multiplier: float = 1.0
    
    micro_trend: str = Field("NEUTRAL", description="Short-horizon 1-2 min direction: BULLISH, BEARISH, NEUTRAL")
    micro_trend_strength: float = Field(0.0, ge=0.0, le=1.0, description="Normalized velocity strength (0.0 to 1.0)")
    
    scheduled_start_hour: Optional[int] = Field(None, ge=0, le=23, description="UTC start hour for allowed executions")
    scheduled_end_hour: Optional[int] = Field(None, ge=0, le=23, description="UTC end hour for allowed executions")
    
    calibration_score: Optional[float] = Field(None, description="Brier calibration score over rolling 30 days")
    calibration_n: Optional[int] = None

    # NEW v11.08: normalized-bps rolling stdev regime read
    volatility_regime: float = 0.0


class HealthOut(BaseModel):
    status: str
    db: str
    redis: str = "unknown"
    groq: str = "unknown"
    mt5_last_seen_seconds: Optional[float] = None


class AccountSnapshotIn(BaseModel):
    account_type: str                                  # 'demo' or 'live'
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
    """
    Contract for POST /missed_trade. Sent by the EA's PostMissedTrade()
    whenever a would-have-traded signal (raw technical score crossed the
    effective threshold, or Tier-2 issued a non-HOLD directive) was NOT
    executed because a risk gate, veto, or precondition blocked it.

    This is deliberately separate from missed_trade_analytics (which is
    populated by the offline Groq backtest pipeline in
    startup/jobs/auto_tester.py) - this table captures REAL live/demo
    missed opportunities, one row per blocked decision, so
    startup/jobs/missed_trade_evaluator.py can later walk tick_telemetry
    forward and label whether the missed trade would have won or lost.
    """
    asset: str
    account_type: Optional[str] = None            # 'demo' or 'live'
    reason: str                                    # e.g. rsi_veto, lifeline_terminated, daily_loss_limit,
                                                     # max_consecutive_losses, orchestrator_unhealthy,
                                                     # live_approval_required, drawdown_halt, margin_buffer,
                                                     # micro_risk_oversized, tier2_local_score_mismatch
    side: str                                       # 'BUY' or 'SELL'
    score: Optional[float] = None                   # final (post-veto) score at decision time
    raw_score: Optional[float] = None               # pre-veto/raw technical score
    threshold: Optional[float] = None               # effective entry threshold at decision time
    rsi: Optional[float] = None
    price: Optional[float] = None                   # bid/ask (side-appropriate) at time of the blocked signal
    atr: Optional[float] = None
    sl_distance: Optional[float] = None             # ATR * sl_mult the trade would have used
    tp_distance: Optional[float] = None             # ATR * tp_mult the trade would have used
    tier2_action: Optional[str] = None
    tier2_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    session_hour: Optional[int] = None
    magic_number: Optional[int] = None