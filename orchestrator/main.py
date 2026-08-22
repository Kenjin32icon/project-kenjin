"""
Entry point. Run with:
    uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env

(drop --reload for the systemd/Render deployment - it's a dev-only convenience)

IMPORTANT - single source of truth: this file IS the API. The files under
startup/routes/ (health.py, strategy.py, ticks.py, telemetry.py) are NOT
imported anywhere below and are dead code relative to what actually runs.
Delete startup/routes/*.py from the repo, or at minimum stop editing them.
Every route lives here, once.
"""
import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Header, status, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import numpy as np
import pandas as pd

load_dotenv()

from db.redis_client import redis_client
from startup.db import close_db_pool, get_pool, init_db_pool
from startup.schemas import (
    HealthOut, StrategyParamsOut, TelemetryIn, TickIn,
    AccountSnapshotIn, RiskIncidentIn,
)
from startup.jobs.feature_pull import pull_feature_window as pg_pull_feature_window
from startup.jobs.ml_tier2 import evaluate_tier2_signal, train_neural_maps, compute_micro_trend
from startup.jobs.ml_tier2 import pull_feature_window as redis_pull_feature_window
from startup.jobs.auto_tester import continuous_tester_cycle
from startup.auth import verify_api_key
from startup.ws_manager import manager  # [NEW IMPORT]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("orchestrator")

scheduler = AsyncIOScheduler()

_VALID_MODEL_PATTERN = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*(/[a-z0-9]+(-[a-z0-9]+)*)*$")

MIN_REDIS_ROWS_FOR_HOT_PATH = 8

_KPI_CACHE = {"data": None, "timestamp": 0}
KPI_CACHE_TTL = 10.0  # 10 seconds

# -----------------------------------------------------------------------------
# Schema models for admin requests
# -----------------------------------------------------------------------------
class KillSwitchRequest(BaseModel):
    kill_switch: bool
    reason: Optional[str] = "Manual operator override"

class StrategyPatchRequest(BaseModel):
    symbol: str
    live_approved: bool


def validate_groq_model() -> str:
    model = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")
    if not _VALID_MODEL_PATTERN.match(model):
        raise RuntimeError(
            f"GROQ_MODEL='{model}' doesn't look like a valid Groq model id "
            f"(expected lowercase/hyphenated, e.g. 'openai/gpt-oss-20b'). Check .env."
        )
    logger.info("Using Groq model: %s", model)
    return model


async def cache_tick_to_redis(asset: str, tick_data: dict):
    """Saves tick to Redis ZSET and trims data older than 45 minutes."""
    current_time = time.time()
    redis_key = f"ticks:{asset}"

    tick_data = dict(tick_data)
    tick_data["ts"] = current_time

    try:
        await redis_client.zadd(redis_key, {json.dumps(tick_data): current_time})
        cutoff_time = current_time - (45 * 60)
        await redis_client.zremrangebyscore(redis_key, "-inf", cutoff_time)
    except Exception as e:
        logger.warning(f"Redis cache_tick_to_redis failed for {asset}: {e}")


async def insert_tick_row(payload: TickIn):
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tick_telemetry
                (asset, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200, ts)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
                """,
                payload.asset, payload.bid, payload.ask, payload.tick_volume, payload.rsi,
                payload.tema, payload.ac, payload.sar, payload.adx, payload.ma10, payload.ma20,
                payload.ma50, payload.ma100, payload.ma200,
            )
    except Exception:
        logger.exception("Background insert_tick_row failed for %s", payload.asset)


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculates Average True Range for reward normalization."""
    if 'high' not in df.columns or 'low' not in df.columns or 'close' not in df.columns:
        return pd.Series(1e-5, index=df.index)

    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()

    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)

    return true_range.rolling(period).mean()


async def fetch_recent_telemetry() -> pd.DataFrame:
    """Helper function to fetch telemetry records joined with raw tick feature context and compute engineered features."""
    pool = get_pool()
    since = datetime.now(timezone.utc) - timedelta(days=30)
    
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                tt.profit, 
                te.rsi, te.bid, te.ask, te.ma10, te.ma50, te.tick_volume, te.ts
            FROM trade_telemetry tt
            LEFT JOIN LATERAL (
                SELECT rsi, bid, ask, ma10, ma50, tick_volume, ts
                FROM tick_telemetry
                WHERE asset = tt.asset AND ts <= tt.created_at
                ORDER BY ts DESC LIMIT 1
            ) te ON true
            WHERE tt.created_at >= $1
              AND tt.account_type = 'live'  -- FIX: Prevent demo leak into Neural Map
        """, since)
        
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])

    # Convert numeric fields to float
    for col in ['profit', 'rsi', 'bid', 'ask', 'ma10', 'ma50', 'tick_volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Compute engineered features required by train_neural_maps()
    df['ma_spread'] = df['ma10'] - df['ma50']
    df['ma_spread_delta'] = df['ma_spread'] - df['ma_spread'].shift(1).fillna(0.0)
    df['price_velocity_1m'] = df['bid'].diff(1).fillna(0.0)

    mean_vol = df['tick_volume'].rolling(window=30, min_periods=1).mean()
    df['rvol'] = np.where(mean_vol > 0, df['tick_volume'] / mean_vol, 1.0)

    return df


async def get_hot_feature_window(asset: str, minutes: int = 30) -> pd.DataFrame:
    for attempt in range(3):
        try:
            df = await redis_pull_feature_window(asset, window_minutes=minutes)
            if len(df) >= MIN_REDIS_ROWS_FOR_HOT_PATH:
                return df
            return df
            return await pg_pull_feature_window(asset, minutes=minutes)
        except Exception as e:
            if attempt == 2:
                logger.exception("Redis/PG feature window failed entirely for %s.", asset)
            else:
                await asyncio.sleep(0.5 + random.uniform(0, 0.5))  # Jittered backoff

    return pd.DataFrame()


@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_groq_model()

    await init_db_pool()

    try:
        from startup.jobs.groq_forecast import run_forecast_cycle
        from startup.jobs.gatekeeper import run_gatekeeper_cycle
        from startup.jobs.hour_scheduler import run_hour_scheduler_cycle
        from startup.jobs.confidence_calibration import run_calibration_cycle
        from startup.jobs.db_pruner import run_snapshot_pruning_cycle

        async def run_retrain_cycle():
            df = await fetch_recent_telemetry()
            if len(df) >= 50:
                await asyncio.to_thread(train_neural_maps, df)
                logger.info(f"Retrained Triple Neural Map on {len(df)} recent trades.")
            else:
                logger.info(f"[Retrain] Insufficient telemetry sample size ({len(df)}/50). Skipping cycle.")

        scheduler.add_job(run_forecast_cycle, "interval", minutes=15, id="run_forecast_cycle")
        scheduler.add_job(run_gatekeeper_cycle, "interval", hours=1, id="run_gatekeeper_cycle")
        scheduler.add_job(run_retrain_cycle, "interval", hours=6, id="run_retrain_cycle")
        scheduler.add_job(run_hour_scheduler_cycle, "interval", hours=12, id="run_hour_scheduler_cycle")
        scheduler.add_job(run_calibration_cycle, "interval", hours=24, id="run_calibration_cycle")
        scheduler.add_job(continuous_tester_cycle, "interval", hours=4, id="continuous_tester_cycle")
        scheduler.add_job(run_snapshot_pruning_cycle, "interval", hours=24, id="run_snapshot_pruning_cycle")

        scheduler.start()
        logger.info(
            "APScheduler initialized: Forecast (15m), Gatekeeper (1h), ML Retrain (6h), "
            "Hour Scheduler (12h), Confidence Calibration (24h), AutoTester (4h), DB Pruner (24h)."
        )
    except Exception as e:
        logger.warning(f"Failed scheduling background jobs: {e}")

    yield

    scheduler.shutdown(wait=False)
    await close_db_pool()


app = FastAPI(
    title="PROJECT KENJIN - Quant Matrix Orchestrator",
    description="High-frequency EA telemetry, indicator snapshot ingestion, and multi-tier strategy parameter sync API.",
    version="11.4.0",
    lifespan=lifespan,
)

# 1. Enable CORS for Electron desktop app and web preflight (OPTIONS /kpis)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Absolute Path for Static Directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR, check_dir=False), name="static")


# --- ROUTES ---

# ==========================================
# [NEW] WEBSOCKET ROUTE FOR ELECTRON DASHBOARD
# ==========================================
@app.websocket("/ws/dashboard")
async def websocket_dashboard_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and listen for manual dashboard overrides
            data = await websocket.receive_text()
            if data == "PING":
                await websocket.send_text("PONG")
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/health", response_model=HealthOut, tags=["System Health"], summary="Service and Database Health Check")
async def health():
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1;")
        return HealthOut(status="ok", db="ok")
    except Exception as exc:
        logger.error(f"Healthcheck DB failure: {exc}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database unreachable: {exc}")


@app.get(
    "/strategy_params",
    response_model=StrategyParamsOut,
    dependencies=[Depends(verify_api_key)],
    tags=["Strategy"],
    summary="Fetch Optimized Parameters & Tier-2 Signals",
)
async def get_strategy_params(asset: str):
    """
    Fetches strategy configuration, evaluates real-time Redis feature windows,
    computes micro-trends, and returns parameters to MQL5.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT asset, opt_threshold, opt_sl_mult, opt_tp_mult,
                   rsi_buy_max, rsi_sell_min, live_approved,
                   scheduled_start_hour, scheduled_end_hour,
                   calibration_score, calibration_n
            FROM strategy_db WHERE asset = $1
            """,
            asset,
        )
        if not row:
            logger.warning("strategy_db had no row for '%s' - auto-bootstrapping defaults.", asset)
            row = await conn.fetchrow(
                """
                INSERT INTO strategy_db (asset, opt_threshold, opt_sl_mult, opt_tp_mult, rsi_buy_max, rsi_sell_min, live_approved, updated_at)
                VALUES ($1, 0.60, 1.5, 3.0, 70.0, 30.0, false, NOW())
                ON CONFLICT (asset) DO UPDATE SET asset = EXCLUDED.asset
                RETURNING asset, opt_threshold, opt_sl_mult, opt_tp_mult,
                          rsi_buy_max, rsi_sell_min, live_approved,
                          scheduled_start_hour, scheduled_end_hour,
                          calibration_score, calibration_n
                """,
                asset,
            )

        forecast = await conn.fetchrow(
            "SELECT id, bullish_prob, bearish_prob FROM forecasts WHERE asset = $1 ORDER BY generated_at DESC LIMIT 1",
            asset,
        )

    bullish_prob = float(forecast["bullish_prob"]) if forecast and forecast["bullish_prob"] is not None else 50.0
    bearish_prob = float(forecast["bearish_prob"]) if forecast and forecast["bearish_prob"] is not None else 50.0
    opt_threshold = float(row["opt_threshold"]) if row["opt_threshold"] is not None else 0.60

    calibration_n = row["calibration_n"]
    calibration_score = row["calibration_score"]
    if calibration_n is None or int(calibration_n) < 30:
        calibration_multiplier = 0.6
    elif calibration_score is not None and float(calibration_score) > 0.25:
        calibration_multiplier = 0.75
    else:
        calibration_multiplier = 1.0

    try:
        df = await get_hot_feature_window(asset, minutes=30)
        tier2 = await asyncio.to_thread(
            evaluate_tier2_signal, df, bullish_prob, bearish_prob, opt_threshold,
            1.0, calibration_multiplier,
        )
        micro = compute_micro_trend(df)
    except Exception:
        logger.exception("Tier-2 evaluation failed for %s - degrading to HOLD/neutral.", asset)
        tier2 = {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0}
        micro = {"micro_trend": "NEUTRAL", "micro_trend_strength": 0.0}

    return StrategyParamsOut(
        asset=row["asset"],
        opt_threshold=opt_threshold,
        opt_sl_mult=float(row["opt_sl_mult"]) if row["opt_sl_mult"] is not None else 1.5,
        opt_tp_mult=float(row["opt_tp_mult"]) if row["opt_tp_mult"] is not None else 3.0,
        rsi_buy_max=float(row["rsi_buy_max"]) if row["rsi_buy_max"] is not None else 70.0,
        rsi_sell_min=float(row["rsi_sell_min"]) if row["rsi_sell_min"] is not None else 30.0,
        live_approved=bool(row["live_approved"]),
        forecast_id=forecast["id"] if forecast else None,
        bullish_prob=bullish_prob,
        bearish_prob=bearish_prob,
        tier2_action=tier2.get("action", "HOLD"),
        tier2_confidence=tier2.get("confidence", 0.0),
        recommended_lot_multiplier=tier2.get("lot_multiplier", 1.0),
        scheduled_start_hour=row["scheduled_start_hour"],
        scheduled_end_hour=row["scheduled_end_hour"],
        micro_trend=micro.get("micro_trend", "NEUTRAL"),
        micro_trend_strength=micro.get("micro_trend_strength", 0.0),
    )


@app.post(
    "/ticks",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
    tags=["Data Feed"],
    summary="Ingest Bar/Tick Feature Snapshot",
)
async def post_ticks(payload: TickIn, background_tasks: BackgroundTasks):
    background_tasks.add_task(insert_tick_row, payload)
    background_tasks.add_task(cache_tick_to_redis, payload.asset, payload.model_dump())
    
    # [NEW] Broadcast real-time micro-trend and price data to dashboard
    tick_data = payload.model_dump()
    await manager.broadcast({
        "type": "TICK_UPDATE",
        "timestamp": tick_data.get("timestamp", time.time()),
        "price": tick_data.get("bid", tick_data.get("price")),
        "micro_trend": tick_data.get("micro_trend", 0.0),
        "expected_value": tick_data.get("expected_value", 0.0)
    })

    # [NEW] Hook for Future Online Learning:
    # If this tick represents a closed trade, broadcast a trigger
    if tick_data.get("is_trade_close"):
        await manager.broadcast({
            "type": "TRADE_CLOSED",
            "profit": tick_data.get("profit")
        })
        # Future function call: await trigger_incremental_learning(tick_data)

    return {"status": "accepted", "asset": payload.asset}


@app.post(
    "/telemetry",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
    tags=["Telemetry"],
    summary="Log Trade Execution Telemetry",
)
async def post_telemetry(payload: TelemetryIn):
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trade_telemetry
                (asset, type, price, lots, profit, rsi, entry_score, sl_price, tp_price, magic_number, account_type, session_hour, forecast_id, tier2_confidence, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
                """,
                payload.asset, payload.type, payload.price, payload.lots, payload.profit,
                payload.rsi, payload.entry_score, payload.sl_price, payload.tp_price,
                payload.magic_number, payload.account_type, payload.session_hour, payload.forecast_id,
                payload.tier2_confidence,
            )
    except Exception:
        logger.exception("Failed to insert trade_telemetry row for %s", payload.asset)
        raise HTTPException(status_code=500, detail="Failed to store telemetry.")
    return {"status": "logged", "asset": payload.asset}

@app.post(
    "/account_snapshot",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
    tags=["Telemetry"],
    summary="Ingest Live Account Balance/Equity Snapshot",
)
async def post_account_snapshot(payload: AccountSnapshotIn):
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO account_snapshots
                (account_type, login, asset, balance, equity, margin, margin_level, floating_pl,
                 peak_equity, drawdown_pct, day_loss_pct, consecutive_losses, consecutive_wins,
                 risk_cooldown_active, drawdown_halt, ts)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, NOW())
                """,
                payload.account_type, payload.login, payload.asset, payload.balance, payload.equity,
                payload.margin, payload.margin_level, payload.floating_pl, payload.peak_equity,
                payload.drawdown_pct, payload.day_loss_pct, payload.consecutive_losses,
                payload.consecutive_wins, payload.risk_cooldown_active, payload.drawdown_halt,
            )
    except Exception:
        logger.exception("Failed to insert account_snapshots row for %s", payload.account_type)
        raise HTTPException(status_code=500, detail="Failed to store account snapshot.")
    return {"status": "logged", "account_type": payload.account_type}


@app.post(
    "/risk_incident",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
    tags=["Control"],
    summary="Log a Local EA Circuit-Breaker Trip",
)
async def post_risk_incident(payload: RiskIncidentIn):
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO risk_incidents (account_type, asset, reason, details) VALUES ($1,$2,$3,$4)",
                payload.account_type, payload.asset, payload.reason, payload.details,
            )
    except Exception:
        logger.exception("Failed to insert risk_incidents row")
        raise HTTPException(status_code=500, detail="Failed to store risk incident.")
    logger.warning("RISK INCIDENT: %s | %s | %s | %s",
                    payload.account_type, payload.asset, payload.reason, payload.details)
    
    # [NEW] Push high-priority alerts immediately to the UI
    risk_data = payload.model_dump()
    await manager.broadcast({
        "type": "RISK_ALERT",
        "severity": risk_data.get("severity", "HIGH"),
        "message": risk_data.get("reason", risk_data.get("message"))
    })

    return {"status": "logged", "reason": payload.reason}


@app.post(
    "/retrain",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_api_key)],
    tags=["ML Engine"],
    summary="Trigger Async Model Retraining Cycle",
)
async def trigger_retrain(background_tasks: BackgroundTasks):
    telemetry_df = await fetch_recent_telemetry()

    if telemetry_df.empty or len(telemetry_df) < 50:
        raise HTTPException(status_code=400, detail="Insufficient telemetry data found to retrain (< 50 records).")

    background_tasks.add_task(asyncio.to_thread, train_neural_maps, telemetry_df)
    return {"status": "retraining_initiated", "records": len(telemetry_df)}


@app.post(
    "/hour_scheduler/run",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_api_key)],
    tags=["Scheduling"],
    summary="Manually Trigger the Predictive Session Scheduler",
)
async def trigger_hour_scheduler(background_tasks: BackgroundTasks):
    from startup.jobs.hour_scheduler import run_hour_scheduler_cycle
    background_tasks.add_task(run_hour_scheduler_cycle)
    return {"status": "hour_scheduler_triggered"}


@app.get(
    "/kpis",
    dependencies=[Depends(verify_api_key)],
    tags=["Monitoring"],
    summary="Aggregated KPIs for the Dashboard",
)
async def get_kpis():
    """
    v11.4: single endpoint powering static/dashboard.html with CORS support and absolute static pathing.
    Includes in-memory TTL caching with graceful degradation.
    """
    global _KPI_CACHE
    current_time = time.time()

    # Return cached data if valid
    if _KPI_CACHE["data"] and (current_time - _KPI_CACHE["timestamp"]) < KPI_CACHE_TTL:
        return _KPI_CACHE["data"]

    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            per_asset = await conn.fetch(
                """
                SELECT
                    t.asset,
                    COUNT(*) FILTER (WHERE t.type LIKE '%CLOSE%') AS closed_trades,
                    COUNT(*) FILTER (WHERE t.type LIKE '%CLOSE%' AND t.profit > 0) AS wins,
                    COALESCE(SUM(t.profit) FILTER (WHERE t.type LIKE '%CLOSE%'), 0) AS total_profit,
                    s.live_approved,
                    s.opt_threshold,
                    s.opt_sl_mult,
                    s.opt_tp_mult,
                    s.scheduled_start_hour,
                    s.scheduled_end_hour,
                    s.calibration_score,
                    s.calibration_n
                FROM trade_telemetry t
                FULL OUTER JOIN strategy_db s ON s.asset = t.asset
                WHERE t.created_at >= NOW() - INTERVAL '30 days' OR t.created_at IS NULL
                GROUP BY t.asset, s.asset, s.live_approved, s.opt_threshold, s.opt_sl_mult,
                         s.opt_tp_mult, s.scheduled_start_hour, s.scheduled_end_hour,
                         s.calibration_score, s.calibration_n
                ORDER BY COALESCE(t.asset, s.asset)
                """
            )

            equity_curve = await conn.fetch(
                """
                SELECT created_at, profit,
                       SUM(profit) OVER (ORDER BY created_at) AS cumulative_profit
                FROM trade_telemetry
                WHERE type LIKE '%CLOSE%' AND created_at >= NOW() - INTERVAL '30 days'
                ORDER BY created_at ASC
                """
            )

            recent_trades = await conn.fetch(
                """
                SELECT asset, type, price, lots, profit, tier2_confidence, created_at
                FROM trade_telemetry
                WHERE type LIKE '%CLOSE%'
                ORDER BY created_at DESC
                LIMIT 25
                """
            )

            latest_forecasts = await conn.fetch(
                """
                SELECT DISTINCT ON (asset) asset, bullish_prob, bearish_prob, generated_at
                FROM forecasts
                ORDER BY asset, generated_at DESC
                """
            )

            latest_accounts = await conn.fetch(
                """
                SELECT DISTINCT ON (account_type) account_type, login, asset, balance, equity, margin,
                       margin_level, floating_pl, peak_equity, drawdown_pct, day_loss_pct,
                       consecutive_losses, consecutive_wins, risk_cooldown_active, drawdown_halt, ts
                FROM account_snapshots
                ORDER BY account_type, ts DESC
                """
            )

            recent_incidents = await conn.fetch(
                """
                SELECT account_type, asset, reason, details, created_at
                FROM risk_incidents
                ORDER BY created_at DESC
                LIMIT 20
                """
            )

        total_closed = sum(int(r["closed_trades"] or 0) for r in per_asset)
        total_wins = sum(int(r["wins"] or 0) for r in per_asset)
        total_profit = sum(float(r["total_profit"] or 0) for r in per_asset)

        response_data = {
            "summary": {
                "total_closed_trades": total_closed,
                "total_wins": total_wins,
                "overall_win_rate": round(100.0 * total_wins / total_closed, 1) if total_closed > 0 else 0.0,
                "total_profit": round(total_profit, 2),
                "assets_tracked": len(per_asset),
                "assets_live_approved": sum(1 for r in per_asset if r["live_approved"]),
            },
            "per_asset": [
                {
                    "asset": r["asset"],
                    "closed_trades": int(r["closed_trades"] or 0),
                    "wins": int(r["wins"] or 0),
                    "win_rate": round(100.0 * (r["wins"] or 0) / r["closed_trades"], 1) if (r["closed_trades"] or 0) > 0 else 0.0,
                    "total_profit": round(float(r["total_profit"] or 0), 2),
                    "live_approved": bool(r["live_approved"]) if r["live_approved"] is not None else False,
                    "opt_threshold": float(r["opt_threshold"]) if r["opt_threshold"] is not None else None,
                    "opt_sl_mult": float(r["opt_sl_mult"]) if r["opt_sl_mult"] is not None else None,
                    "opt_tp_mult": float(r["opt_tp_mult"]) if r["opt_tp_mult"] is not None else None,
                    "scheduled_start_hour": r["scheduled_start_hour"],
                    "scheduled_end_hour": r["scheduled_end_hour"],
                    "calibration_score": float(r["calibration_score"]) if r["calibration_score"] is not None else None,
                    "calibration_n": r["calibration_n"],
                }
                for r in per_asset if r["asset"] is not None
            ],
            "equity_curve": [
                {
                    "created_at": r["created_at"].isoformat(),
                    "profit": float(r["profit"]),
                    "cumulative_profit": round(float(r["cumulative_profit"]), 2),
                }
                for r in equity_curve
            ],
            "recent_trades": [
                {
                    "asset": r["asset"],
                    "type": r["type"],
                    "price": float(r["price"]) if r["price"] is not None else None,
                    "lots": float(r["lots"]) if r["lots"] is not None else None,
                    "profit": float(r["profit"]) if r["profit"] is not None else None,
                    "tier2_confidence": float(r["tier2_confidence"]) if r["tier2_confidence"] is not None else None,
                    "created_at": r["created_at"].isoformat(),
                }
                for r in recent_trades
            ],
            "latest_forecasts": [
                {
                    "asset": r["asset"],
                    "bullish_prob": float(r["bullish_prob"]) if r["bullish_prob"] is not None else None,
                    "bearish_prob": float(r["bearish_prob"]) if r["bearish_prob"] is not None else None,
                    "generated_at": r["generated_at"].isoformat(),
                }
                for r in latest_forecasts
            ],
            "accounts": [
                {
                    "account_type": r["account_type"],
                    "login": r["login"],
                    "asset": r["asset"],
                    "balance": float(r["balance"]) if r["balance"] is not None else None,
                    "equity": float(r["equity"]) if r["equity"] is not None else None,
                    "floating_pl": float(r["floating_pl"]) if r["floating_pl"] is not None else None,
                    "margin_level": float(r["margin_level"]) if r["margin_level"] is not None else None,
                    "peak_equity": float(r["peak_equity"]) if r["peak_equity"] is not None else None,
                    "drawdown_pct": float(r["drawdown_pct"]) if r["drawdown_pct"] is not None else None,
                    "day_loss_pct": float(r["day_loss_pct"]) if r["day_loss_pct"] is not None else None,
                    "consecutive_losses": r["consecutive_losses"],
                    "risk_cooldown_active": r["risk_cooldown_active"],
                    "drawdown_halt": r["drawdown_halt"],
                    "last_updated": r["ts"].isoformat(),
                    "seconds_since_update": (datetime.now(timezone.utc) - r["ts"]).total_seconds(),
                }
                for r in latest_accounts
            ],
            "risk_incidents": [
                {
                    "account_type": r["account_type"],
                    "asset": r["asset"],
                    "reason": r["reason"],
                    "details": r["details"],
                    "created_at": r["created_at"].isoformat(),
                }
                for r in recent_incidents
            ],
        }

        _KPI_CACHE["data"] = response_data
        _KPI_CACHE["timestamp"] = current_time
        return response_data

    except Exception as e:
        logger.error(f"KPI DB Timeout/Error: {e}. Degrading gracefully.")
        if _KPI_CACHE["data"]:
            return _KPI_CACHE["data"]
        raise HTTPException(status_code=500, detail="Database timeout and no cached data available.")

# -----------------------------------------------------------------------------
# Control & Job Endpoints
# -----------------------------------------------------------------------------

@app.post("/control/kill_switch", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def emergency_kill_switch(payload: KillSwitchRequest):
    """
    Emergency Halt: Deactivates live approval across all symbol strategies in Redis & Postgres.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE strategy_db SET live_approved = FALSE")
        
    return {
        "status": "success", 
        "message": f"Global Emergency Kill Switch executed. Reason: {payload.reason}"
    }

@app.post("/jobs/retrain", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def trigger_model_retrain(background_tasks: BackgroundTasks):
    """
    Triggers LightGBM background retraining.
    """
    telemetry_df = await fetch_recent_telemetry()

    if telemetry_df.empty or len(telemetry_df) < 50:
        raise HTTPException(status_code=400, detail="Insufficient telemetry data found to retrain (< 50 records).")

    background_tasks.add_task(asyncio.to_thread, train_neural_maps, telemetry_df)
    
    return {
        "status": "queued", 
        "message": "LightGBM Triple Neural Map retraining initiated in background."
    }

@app.post("/jobs/calibrate", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def trigger_confidence_calibration(background_tasks: BackgroundTasks):
    """
    Calculates Brier calibration scores over 30-day telemetry.
    """
    from startup.jobs.confidence_calibration import run_calibration_cycle
    background_tasks.add_task(run_calibration_cycle)
    
    return {
        "status": "success", 
        "message": "Brier score confidence calibration calculated and updated."
    }

@app.patch("/strategy_params", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def patch_strategy_params(payload: StrategyPatchRequest):
    """
    Updates live_approved flag for an individual trading pair.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE strategy_db SET live_approved = $1 WHERE asset = $2", 
            payload.live_approved, payload.symbol
        )
        
    return {
        "status": "success", 
        "symbol": payload.symbol, 
        "live_approved": payload.live_approved,
        "message": f"Asset {payload.symbol} live execution updated to {payload.live_approved}."
    }