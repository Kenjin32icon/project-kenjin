-- 003_v12_guardian_market_context.sql
-- Idempotent compatibility migration. Run after the canonical baseline schema.
BEGIN;
ALTER TABLE public.strategy_db
    ADD COLUMN IF NOT EXISTS lifeline_score numeric DEFAULT 100.0,
    ADD COLUMN IF NOT EXISTS lifeline_terminated boolean NOT NULL DEFAULT false;
ALTER TABLE public.account_snapshots
    ADD COLUMN IF NOT EXISTS day_start_balance numeric,
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE public.autotune_log ADD COLUMN IF NOT EXISTS notes text;
ALTER TABLE public.trade_telemetry
    ADD COLUMN IF NOT EXISTS entry_features jsonb,
    ADD COLUMN IF NOT EXISTS entry_e_utility double precision;
ALTER TABLE public.strategy_db DROP CONSTRAINT IF EXISTS chk_opt_threshold;
ALTER TABLE public.strategy_db ADD CONSTRAINT chk_opt_threshold
    CHECK (opt_threshold IS NULL OR (opt_threshold >= 0.10 AND opt_threshold <= 0.95));
CREATE TABLE IF NOT EXISTS public.account_controls (
    account_type text PRIMARY KEY CHECK (account_type IN ('live','demo')),
    manual_drawdown_halt boolean NOT NULL DEFAULT false,
    halt_reason text,
    halted_by text,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.guardian_actions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset text NOT NULL, ticket bigint, side text,
    action text NOT NULL CHECK (action IN ('CLOSE','MOVE_SL','HOLD')),
    reason text NOT NULL, r_multiple numeric, peak_r numeric,
    slope_r_min numeric, against_votes integer, market_bias numeric,
    details jsonb, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.market_context_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_account_snapshots_type_ts ON public.account_snapshots(account_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_tick_telemetry_asset_ts ON public.tick_telemetry(asset, ts DESC);
CREATE INDEX IF NOT EXISTS idx_trade_telemetry_asset_created ON public.trade_telemetry(asset, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_missed_signals_eval ON public.missed_trade_signals(evaluated, created_at);
CREATE INDEX IF NOT EXISTS idx_guardian_actions_created ON public.guardian_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcs_ts ON public.market_context_snapshots(ts DESC);
COMMIT;
