-- Project KENJIN v10 Consolidated Database Schema (Combined, Refined & Indexed)[cite: 15]

-- 1. Trading Assets Catalog
CREATE TABLE IF NOT EXISTS trading_assets (
    asset_id UUID NOT NULL DEFAULT gen_random_uuid(),
    symbol VARCHAR(32) NOT NULL UNIQUE,
    asset_class VARCHAR(64) NOT NULL,
    pip_size NUMERIC(12, 5) NOT NULL,
    tick_size NUMERIC(12, 5) NOT NULL,
    contract_size INT NOT NULL DEFAULT 100000,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT trading_assets_pkey PRIMARY KEY (asset_id)
);

-- 2. Strategy Parameters & Performance Database
CREATE TABLE IF NOT EXISTS strategy_db (
    asset VARCHAR(32) PRIMARY KEY,
    opt_threshold NUMERIC(4,2) DEFAULT 0.60,
    opt_sl_mult NUMERIC(4,2) DEFAULT 1.50,
    opt_tp_mult NUMERIC(4,2) DEFAULT 3.00,
    scheduled_start_hour INT,
    scheduled_end_hour INT,
    live_approved BOOLEAN DEFAULT FALSE,
    win_rate NUMERIC(5,2) DEFAULT 0.00,
    profit_factor NUMERIC(5,2) DEFAULT 0.00,
    sample_size INT DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. AI Forecasts Table
CREATE TABLE IF NOT EXISTS forecasts (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(32) NOT NULL,
    horizon_minutes INT DEFAULT 15,
    bullish_prob NUMERIC(5,2) NOT NULL CHECK (bullish_prob >= 0 AND bullish_prob <= 100),
    bearish_prob NUMERIC(5,2) NOT NULL CHECK (bearish_prob >= 0 AND bearish_prob <= 100),
    suggested_sl_atr_mult NUMERIC(4,2),
    suggested_tp_atr_mult NUMERIC(4,2),
    rationale TEXT,
    model_used VARCHAR(64),
    generated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Optimize fast lookup of latest asset forecast[cite: 15]
CREATE INDEX IF NOT EXISTS idx_forecasts_asset_generated ON forecasts(asset, generated_at DESC);

-- 4. Tick & Indicator Telemetry Feed
CREATE TABLE IF NOT EXISTS tick_telemetry (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(32) NOT NULL,
    bid NUMERIC(12, 5) NOT NULL,
    ask NUMERIC(12, 5) NOT NULL,
    tick_volume NUMERIC(12, 2),
    rsi NUMERIC(6, 2) CHECK (rsi >= 0 AND rsi <= 100),
    tema NUMERIC(12, 5),
    ac NUMERIC(12, 5),
    sar NUMERIC(12, 5),
    adx NUMERIC(6, 2) CHECK (adx >= 0 AND adx <= 100),
    ma10 NUMERIC(12, 5),
    ma20 NUMERIC(12, 5),
    ma50 NUMERIC(12, 5),
    ma100 NUMERIC(12, 5),
    ma200 NUMERIC(12, 5),
    ts TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tick_telemetry_asset_ts ON tick_telemetry(asset, ts DESC);

-- 5. Trade Execution Telemetry
CREATE TABLE IF NOT EXISTS trade_telemetry (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(32) NOT NULL,
    type VARCHAR(16) NOT NULL,
    price NUMERIC(12, 5) NOT NULL,
    lots NUMERIC(6, 2) NOT NULL,
    profit NUMERIC(12, 2) NOT NULL,
    rsi NUMERIC(6, 2),
    entry_score NUMERIC(4, 2),
    sl_price NUMERIC(12, 5),
    tp_price NUMERIC(12, 5),
    magic_number BIGINT,
    account_type VARCHAR(16),
    session_hour INT,
    forecast_id BIGINT REFERENCES forecasts(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    ts TIMESTAMPTZ DEFAULT NOW()
);

-- Optimize Gatekeeper rolling performance evaluations[cite: 15]
CREATE INDEX IF NOT EXISTS idx_trade_telemetry_asset_created ON trade_telemetry(asset, created_at DESC);

-- 6. Optimization Logs (Auto-Tester)
CREATE TABLE IF NOT EXISTS optimization_logs (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(32) NOT NULL,
    timeframe VARCHAR(16) NOT NULL,
    pass_number INT,
    profit NUMERIC(12, 2),
    total_trades INT,
    win_rate NUMERIC(5, 2),
    drawdown NUMERIC(5, 2),
    parameters JSONB,
    tested_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. Missed Trade Analytics
CREATE TABLE IF NOT EXISTS missed_trade_analytics (
    id BIGSERIAL PRIMARY KEY,
    asset VARCHAR(32) NOT NULL,
    groq_analysis TEXT,
    suggested_logic_tweak TEXT,
    analyzed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);