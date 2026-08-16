-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.strategy_db (
  asset text NOT NULL UNIQUE,
  opt_threshold numeric,
  opt_sl_mult numeric,
  opt_tp_mult numeric,
  scheduled_start_hour integer,
  scheduled_end_hour integer,
  live_approved boolean NOT NULL DEFAULT false,
  updated_at timestamp with time zone DEFAULT now(),
  win_rate numeric,
  profit_factor numeric,
  sample_size integer,
  rsi_buy_max numeric DEFAULT 70.0,
  rsi_sell_min numeric DEFAULT 30.0,
  CONSTRAINT strategy_db_pkey PRIMARY KEY (asset)
);
CREATE TABLE public.trade_telemetry (
  id bigint NOT NULL DEFAULT nextval('trade_telemetry_id_seq'::regclass),
  asset text NOT NULL,
  type text NOT NULL,
  price numeric,
  lots numeric,
  profit numeric,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  rsi numeric,
  entry_score numeric,
  sl_price numeric,
  tp_price numeric,
  magic_number bigint,
  account_type text,
  session_hour integer,
  forecast_id bigint,
  CONSTRAINT trade_telemetry_pkey PRIMARY KEY (id),
  CONSTRAINT trade_telemetry_forecast_id_fkey FOREIGN KEY (forecast_id) REFERENCES public.forecasts(id),
  CONSTRAINT fk_trade_telemetry_forecast FOREIGN KEY (forecast_id) REFERENCES public.forecasts(id)
);
CREATE TABLE public.trading_assets (
  asset_id uuid NOT NULL DEFAULT gen_random_uuid(),
  symbol character varying NOT NULL UNIQUE,
  asset_class character varying NOT NULL,
  pip_size numeric NOT NULL,
  tick_size numeric NOT NULL,
  contract_size integer NOT NULL DEFAULT 100000,
  is_active boolean DEFAULT true,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT trading_assets_pkey PRIMARY KEY (asset_id)
);
CREATE TABLE public.forecasts (
  id bigint NOT NULL DEFAULT nextval('forecasts_id_seq'::regclass),
  asset character varying NOT NULL,
  generated_at timestamp with time zone NOT NULL DEFAULT now(),
  horizon_minutes integer NOT NULL DEFAULT 30,
  bullish_prob numeric CHECK (bullish_prob >= 0.0 AND bullish_prob <= 100.0),
  bearish_prob numeric CHECK (bearish_prob >= 0.0 AND bearish_prob <= 100.0),
  suggested_sl_atr_mult numeric,
  suggested_tp_atr_mult numeric,
  rationale text,
  model_used character varying,
  direction character varying,
  confidence numeric,
  CONSTRAINT forecasts_pkey PRIMARY KEY (id)
);
CREATE TABLE public.tick_telemetry (
  id bigint NOT NULL DEFAULT nextval('tick_telemetry_id_seq'::regclass),
  asset character varying NOT NULL,
  ts timestamp with time zone NOT NULL DEFAULT now(),
  bid numeric,
  ask numeric,
  tick_volume integer,
  rsi numeric CHECK (rsi >= 0::numeric AND rsi <= 100::numeric),
  tema numeric,
  ac numeric,
  sar numeric,
  adx numeric CHECK (adx >= 0::numeric AND adx <= 100::numeric),
  ma10 numeric,
  ma20 numeric,
  ma50 numeric,
  ma100 numeric,
  ma200 numeric,
  CONSTRAINT tick_telemetry_pkey PRIMARY KEY (id)
);
CREATE TABLE public.optimization_logs (
  id integer NOT NULL DEFAULT nextval('optimization_logs_id_seq'::regclass),
  asset character varying NOT NULL,
  timeframe character varying NOT NULL,
  pass_number integer,
  profit numeric,
  total_trades integer,
  win_rate numeric,
  drawdown numeric,
  parameters jsonb,
  tested_at timestamp with time zone DEFAULT now(),
  CONSTRAINT optimization_logs_pkey PRIMARY KEY (id)
);
CREATE TABLE public.missed_trade_analytics (
  id integer NOT NULL DEFAULT nextval('missed_trade_analytics_id_seq'::regclass),
  asset character varying NOT NULL,
  groq_analysis text,
  suggested_logic_tweak text,
  analyzed_at timestamp with time zone DEFAULT now(),
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT missed_trade_analytics_pkey PRIMARY KEY (id)
);
CREATE TABLE public.account_snapshots (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_type text NOT NULL,          -- 'demo' or 'live'
  login bigint,
  asset text,                          -- which EA instance/chart posted this
  balance numeric,
  equity numeric,
  margin numeric,
  margin_level numeric,
  floating_pl numeric,
  peak_equity numeric,
  drawdown_pct numeric,
  day_loss_pct numeric,
  consecutive_losses integer,
  consecutive_wins integer,
  risk_cooldown_active boolean,
  drawdown_halt boolean,
  ts timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_account_snapshots_type_ts ON public.account_snapshots (account_type, ts DESC);

CREATE TABLE public.risk_incidents (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_type text NOT NULL,
  asset text,
  reason text NOT NULL,                -- 'daily_loss_limit' | 'drawdown_halt' | 'floating_loss_kill_switch' | 'max_consecutive_losses'
  details text,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_risk_incidents_created_at ON public.risk_incidents (created_at DESC);