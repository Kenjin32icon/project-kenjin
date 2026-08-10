"""
Entry point. Run with:
    uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env

(drop --reload for the systemd/Render deployment - it's a dev-only convenience)

IMPORTANT - single source of truth: this file IS the API. The files under
startup/routes/ (health.py, strategy.py, ticks.py, telemetry.py) are NOT
imported anywhere below and are dead code relative to what actually runs.
Delete startup/routes/*.py from the repo, or at minimum stop editing them.
Every route lives here, once.

v11 CHANGES vs the previous version:
  - get_strategy_params() now tries the Redis-backed feature window
    (startup/jobs/ml_tier2.py::pull_feature_window - sub-millisecond
    ZRANGEBYSCORE) FIRST, falling back to the Postgres-backed window
    (startup/jobs/feature_pull.py::pull_feature_window) only when Redis
    doesn't yet have enough ticks cached for this asset (cold start /
    Redis flush / brand-new asset). Previously the Redis cache was being
    populated on every single tick but nothing ever read from it - the
    hot path was always hitting Postgres.
  - cache_tick_to_redis() now stamps each cached tick with a unix-seconds
    'ts' field, which ml_tier2.pull_feature_window() needs to compute
    real elapsed-time-based features (price velocity, MA spread delta).
  - POST /ticks no longer blocks the HTTP response on the Postgres INSERT.
    Both the Postgres write and the Redis cache write are now background
    tasks, and the endpoint acknowledges immediately. Trade-off: a tick
    insert failure is now logged rather than surfaced to the EA as a
    non-2xx response - acceptable for telemetry, but flagged explicitly
    here because it's a real behavior change.
  - get_strategy_params() now also selects and returns scheduled_start_hour
    / scheduled_end_hour from strategy_db (schema already had these
    columns; nothing wrote or read them before).
  - New hour_scheduler job added to the scheduler (~12h cadence) to
    actually populate those two columns.
"""
import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, status
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

from db.redis_client import redis_client
from startup.db import close_db_pool, get_pool, init_db_pool
from startup.schemas import HealthOut, StrategyParamsOut, TelemetryIn, TickIn
from startup.jobs.feature_pull import pull_feature_window as pg_pull_feature_window
from startup.jobs.ml_tier2 import evaluate_tier2_signal, train_neural_maps
from startup.jobs.ml_tier2 import pull_feature_window as redis_pull_feature_window
from startup.auth import verify_api_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("orchestrator")

scheduler = AsyncIOScheduler()

_VALID_MODEL_PATTERN = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*(/[a-z0-9]+(-[a-z0-9]+)*)*$")

# Below this many rows, the Redis window is treated as "cold" and we fall
# back to the slower but more complete Postgres-backed window.
MIN_REDIS_ROWS_FOR_HOT_PATH = 8


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

    # v11: stamp the tick with its own timestamp so ml_tier2.pull_feature_window
    # can compute real elapsed-time features instead of assuming even spacing.
    tick_data = dict(tick_data)
    tick_data["ts"] = current_time

    try:
        await redis_client.zadd(redis_key, {json.dumps(tick_data): current_time})
        cutoff_time = current_time - (45 * 60)
        await redis_client.zremrangebyscore(redis_key, "-inf", cutoff_time)
    except Exception as e:
        logger.warning(f"Redis cache_tick_to_redis failed for {asset}: {e}")


async def insert_tick_row(payload: TickIn):
    """v11: moved off the request/response critical path - runs as a background task."""
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
    """Helper function to fetch telemetry records joined with tick feature context."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tt.profit, te.ma_spread_delta, te.price_velocity_1m, te.rsi, te.rvol
            FROM trade_telemetry tt
            LEFT JOIN LATERAL (
                SELECT * FROM tick_telemetry
                WHERE asset = tt.asset AND ts <= tt.created_at
                ORDER BY ts DESC LIMIT 1
            ) te ON true
            WHERE tt.created_at >= NOW() - INTERVAL '30 days'
        """)
    return pd.DataFrame([dict(r) for r in rows])


async def get_hot_feature_window(asset: str, minutes: int = 30) -> pd.DataFrame:
    """
    v11: Redis-first feature window for the /strategy_params hot path, with a
    Postgres fallback for cold starts (asset just added, Redis flushed, etc).
    """
    try:
        df = await redis_pull_feature_window(asset, window_minutes=minutes)
        if len(df) >= MIN_REDIS_ROWS_FOR_HOT_PATH:
            return df
    except Exception:
        logger.exception("Redis feature window failed for %s - falling back to Postgres.", asset)

    return await pg_pull_feature_window(asset, minutes=minutes)


@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_groq_model()

    await init_db_pool()

    try:
        from startup.jobs.groq_forecast import run_forecast_cycle
        from startup.jobs.gatekeeper import run_gatekeeper_cycle
        from startup.jobs.hour_scheduler import run_hour_scheduler_cycle
        from startup.jobs.confidence_calibration import run_calibration_cycle

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
        # v11: populates strategy_db.scheduled_start_hour/end_hour
        scheduler.add_job(run_hour_scheduler_cycle, "interval", hours=12, id="run_hour_scheduler_cycle")
        # v11.1: NEW - validates tier2_confidence against real outcomes, populates
        # strategy_db.calibration_score/calibration_n
        scheduler.add_job(run_calibration_cycle, "interval", hours=24, id="run_calibration_cycle")

        scheduler.start()
        logger.info(
            "APScheduler initialized: Forecast (15m), Gatekeeper (1h), ML Retrain (6h), "
            "Hour Scheduler (12h), Confidence Calibration (24h). "
            "AutoTester DISABLED - see auto_tester_review.md."
        )
    except Exception as e:
        logger.warning(f"Failed scheduling background jobs: {e}")

    yield

    scheduler.shutdown(wait=False)
    await close_db_pool()


app = FastAPI(
    title="PROJECT KENJIN - Quant Matrix Orchestrator",
    description="High-frequency EA telemetry, indicator snapshot ingestion, and multi-tier strategy parameter sync API.",
    version="11.2.0",
    lifespan=lifespan,
)


# --- ROUTES ---

@app.get("/health", response_model=HealthOut, tags=["System Health"], summary="Service and Database Health Check")
async def health():
    """
    Deliberately NOT behind API-key auth - the EA's OnInit() calls this to
    decide whether to allow trading at all, and it must stay reachable
    even if the key is misconfigured elsewhere.
    """
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
            # v11.2 FIX: previously a hard 404 here, which is what EURUSDw and
            # BCHUSD were hitting in the log - any asset the EA is attached to
            # but that nobody has manually seeded a strategy_db row for was
            # stuck running with no gatekeeper tuning and no Tier-2 signal at
            # all (the EA falls back to its local defaults on a failed fetch,
            # so it wasn't fully blocked, just running blind). Auto-bootstrap
            # a conservative default row instead, so any newly-attached asset
            # onboards itself and starts accumulating gatekeeper/calibration
            # history immediately rather than requiring a manual DB insert.
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

    # v11.1: derive a calibration multiplier from confidence_calibration.py's output.
    # Not enough validated history yet -> derate meaningfully (0.6x). Enough history
    # but the model's stated confidence hasn't tracked real outcomes well (Brier
    # score worse than an uninformative 0.5 guess) -> derate moderately (0.75x).
    # Otherwise leave lot sizing exactly as Tier-2 computed it (1.0x).
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
    except Exception:
        logger.exception("Tier-2 evaluation failed for %s - degrading to HOLD/neutral.", asset)
        tier2 = {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0}

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
    )


@app.post(
    "/ticks",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
    tags=["Data Feed"],
    summary="Ingest Bar/Tick Feature Snapshot",
)
async def post_ticks(payload: TickIn, background_tasks: BackgroundTasks):
    # v11: both the Postgres write and the Redis cache write are now background
    # tasks - the EA's PostTick() gets acknowledged immediately instead of
    # waiting on a database round-trip on its entry-decision critical path.
    background_tasks.add_task(insert_tick_row, payload)
    background_tasks.add_task(cache_tick_to_redis, payload.asset, payload.model_dump())
    return {"status": "accepted", "asset": payload.asset}


@app.post(
    "/telemetry",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_api_key)],
    tags=["Telemetry"],
    summary="Log Trade Execution Telemetry",
)
async def post_telemetry(payload: TelemetryIn):
    # v11.2 BUG FIX: this INSERT previously referenced a column named "ts" on
    # trade_telemetry, which does not exist on that table (it only exists on
    # tick_telemetry - this was a copy/paste from insert_tick_row()). Every
    # trade close was failing with asyncpg.exceptions.UndefinedColumnError
    # and returning 500 to the EA, silently discarding every close: the
    # gatekeeper's sample size could never grow, so live_approved was stuck
    # at False forever regardless of how many trades actually closed.
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
    "/retrain",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(verify_api_key)],
    tags=["ML Engine"],
    summary="Trigger Async Model Retraining Cycle",
)
async def trigger_retrain(background_tasks: BackgroundTasks):
    """Manual or Webhook trigger to run model retraining asynchronously."""
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
    """Manual trigger - useful right after deploying v11 rather than waiting up to 12h."""
    from startup.jobs.hour_scheduler import run_hour_scheduler_cycle
    background_tasks.add_task(run_hour_scheduler_cycle)
    return {"status": "hour_scheduler_triggered"}