-- Project Kenjin v12.00 canonical PostgreSQL schema.
-- Safe to apply to a new database. Existing installations should run the
-- idempotent migration scripts after taking a backup.
BEGIN;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.strategy_db (
    asset text PRIMARY KEY,
    opt_threshold numeric DEFAULT 0.60 CHECK (opt_threshold IS NULL OR (opt_threshold >= 0.10 AND opt_threshold <= 0.95)),
    opt_sl_mult numeric DEFAULT 1.50 CHECK (opt_sl_mult IS NULL OR (opt_sl_mult >= 0.5 AND opt_sl_mult <= 5.0)),
    opt_tp_mult numeric DEFAULT 3.00 CHECK (opt_tp_mult IS NULL OR (opt_tp_mult >= 0.5 AND opt_tp_mult <= 10.0)),
    scheduled_start_hour integer CHECK (scheduled_start_hour IS NULL OR scheduled_start_hour BETWEEN 0 AND 23),
    scheduled_end_hour integer CHECK (scheduled_end_hour IS NULL OR scheduled_end_hour BETWEEN 0 AND 23),
    live_approved boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    win_rate numeric,
    profit_factor numeric,
    sample_size integer,
    rsi_buy_max numeric(5,2) DEFAULT 70.0 CHECK (rsi_buy_max IS NULL OR rsi_buy_max BETWEEN 50 AND 100),
    rsi_sell_min numeric(5,2) DEFAULT 30.0 CHECK (rsi_sell_min IS NULL OR rsi_sell_min BETWEEN 0 AND 50),
    calibration_score numeric,
    calibration_n integer,
    calibration_updated_at timestamptz,
    lifeline_score numeric DEFAULT 100.0,
    lifeline_terminated boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS public.account_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_type text NOT NULL CHECK (account_type IN ('live','demo')),
    login bigint,
    asset text,
    balance numeric,
    equity numeric,
    margin numeric,
    margin_level numeric,
    floating_pl numeric,
    peak_equity numeric,
    drawdown_pct numeric,
    day_loss_pct numeric,
    day_start_balance numeric,
    consecutive_losses integer,
    consecutive_wins integer,
    risk_cooldown_active boolean,
    drawdown_halt boolean,
    ts timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.forecasts (
    id bigserial PRIMARY KEY,
    asset varchar(20) NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT now(),
    horizon_minutes integer NOT NULL DEFAULT 30,
    bullish_prob numeric(5,2) CHECK (bullish_prob IS NULL OR bullish_prob BETWEEN 0 AND 100),
    bearish_prob numeric(5,2) CHECK (bearish_prob IS NULL OR bearish_prob BETWEEN 0 AND 100),
    suggested_sl_atr_mult numeric(5,2),
    suggested_tp_atr_mult numeric(5,2),
    rationale text,
    model_used varchar(100),
    direction varchar(16),
    confidence numeric(5,2)
);

CREATE TABLE IF NOT EXISTS public.missed_trade_analytics (
    id serial PRIMARY KEY,
    asset varchar(20) NOT NULL,
    groq_analysis text,
    suggested_logic_tweak text,
    analyzed_at timestamptz DEFAULT now(),
    created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.missed_trade_signals (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset text NOT NULL,
    account_type text CHECK (account_type IS NULL OR account_type IN ('live','demo')),
    reason text NOT NULL,
    side text NOT NULL CHECK (side IN ('BUY','SELL')),
    score numeric,
    raw_score numeric,
    threshold numeric,
    rsi numeric CHECK (rsi IS NULL OR rsi BETWEEN 0 AND 100),
    price numeric,
    atr numeric,
    sl_distance numeric,
    tp_distance numeric,
    tier2_action text,
    tier2_confidence numeric CHECK (tier2_confidence IS NULL OR tier2_confidence BETWEEN 0 AND 1),
    session_hour integer,
    magic_number bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    evaluated boolean NOT NULL DEFAULT false,
    evaluated_at timestamptz,
    outcome text CHECK (outcome IS NULL OR outcome IN ('would_have_won','would_have_lost','inconclusive')),
    simulated_pnl_r numeric
);

CREATE TABLE IF NOT EXISTS public.optimization_logs (
    id serial PRIMARY KEY,
    asset varchar(20) NOT NULL,
    timeframe varchar(10) NOT NULL,
    pass_number integer,
    profit numeric,
    total_trades integer,
    win_rate numeric,
    drawdown numeric,
    parameters jsonb,
    tested_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.risk_incidents (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_type text NOT NULL CHECK (account_type IN ('live','demo')),
    asset text,
    reason text NOT NULL,
    details text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.telemetry (
    forecast_id varchar(255) PRIMARY KEY,
    symbol varchar(20) NOT NULL,
    action varchar(50),
    confidence numeric(5,4),
    pnl numeric(10,4),
    brier_score numeric(5,4),
    "timestamp" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.tick_telemetry (
    id bigserial PRIMARY KEY,
    asset varchar(20) NOT NULL,
    ts timestamptz NOT NULL DEFAULT now(),
    bid numeric(18,5),
    ask numeric(18,5),
    tick_volume integer,
    rsi numeric(6,2) CHECK (rsi IS NULL OR rsi BETWEEN 0 AND 100),
    tema numeric(18,5),
    ac numeric(18,5),
    sar numeric(18,5),
    adx numeric(6,2) CHECK (adx IS NULL OR adx BETWEEN 0 AND 100),
    ma10 numeric(18,5),
    ma20 numeric(18,5),
    ma50 numeric(18,5),
    ma100 numeric(18,5),
    ma200 numeric(18,5)
);

CREATE TABLE IF NOT EXISTS public.trading_assets (
    asset_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol varchar(15) NOT NULL UNIQUE,
    asset_class varchar(50) NOT NULL,
    pip_size numeric(10,5) NOT NULL,
    tick_size numeric(10,5) NOT NULL,
    contract_size integer NOT NULL DEFAULT 100000,
    is_active boolean DEFAULT true,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    pip_value numeric
);

CREATE TABLE IF NOT EXISTS public.autotune_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset text NOT NULL REFERENCES public.strategy_db(asset) ON DELETE CASCADE,
    win_rate numeric,
    profit_factor numeric,
    sample_size integer,
    old_threshold numeric,
    new_threshold numeric,
    old_sl numeric,
    new_sl numeric,
    old_tp numeric,
    new_tp numeric,
    notes text,
    ts timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.strategy_lifeline (
    asset text PRIMARY KEY REFERENCES public.strategy_db(asset) ON DELETE CASCADE,
    lifeline_score numeric NOT NULL DEFAULT 100.0 CHECK (lifeline_score BETWEEN 0 AND 200),
    win_streak integer NOT NULL DEFAULT 0,
    loss_streak integer NOT NULL DEFAULT 0,
    last_delta numeric,
    terminated boolean NOT NULL DEFAULT false,
    terminated_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.trade_telemetry (
    id bigserial PRIMARY KEY,
    asset text NOT NULL,
    type text NOT NULL,
    price numeric,
    lots numeric(10,2),
    profit numeric,
    created_at timestamptz NOT NULL DEFAULT now(),
    rsi numeric,
    entry_score numeric,
    sl_price numeric,
    tp_price numeric,
    magic_number bigint,
    account_type text CHECK (account_type IS NULL OR account_type IN ('live','demo')),
    session_hour integer,
    forecast_id bigint REFERENCES public.forecasts(id),
    tier2_confidence numeric,
    micro_trend varchar(10) DEFAULT 'NEUTRAL',
    reward_utility numeric,
    entry_features jsonb,
    entry_e_utility double precision
);

CREATE TABLE IF NOT EXISTS public.account_controls (
    account_type text PRIMARY KEY CHECK (account_type IN ('live','demo')),
    manual_drawdown_halt boolean NOT NULL DEFAULT false,
    halt_reason text,
    halted_by text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.guardian_actions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset text NOT NULL,
    ticket bigint,
    side text,
    action text NOT NULL CHECK (action IN ('CLOSE','MOVE_SL','HOLD')),
    reason text NOT NULL,
    r_multiple numeric,
    peak_r numeric,
    slope_r_min numeric,
    against_votes integer,
    market_bias numeric,
    details jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.market_context_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_snapshots_type_ts ON public.account_snapshots(account_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_forecasts_asset_generated ON public.forecasts(asset, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_tick_telemetry_asset_ts ON public.tick_telemetry(asset, ts DESC);
CREATE INDEX IF NOT EXISTS idx_trade_telemetry_asset_created ON public.trade_telemetry(asset, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trade_telemetry_close_created ON public.trade_telemetry(created_at DESC) WHERE type LIKE '%CLOSE%';
CREATE INDEX IF NOT EXISTS idx_missed_signals_eval ON public.missed_trade_signals(evaluated, created_at);
CREATE INDEX IF NOT EXISTS idx_guardian_actions_created ON public.guardian_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_context_ts ON public.market_context_snapshots(ts DESC);
CREATE INDEX IF NOT EXISTS idx_risk_incidents_created ON public.risk_incidents(created_at DESC);
COMMIT;
