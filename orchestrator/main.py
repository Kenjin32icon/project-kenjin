"""
Orchestrator Core Application
Project: KENJIN SageEyes Predictive Quant Matrix
"""
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, HTTPException, Header, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from startup.db import close_db_pool, get_pool, init_db_pool

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("orchestrator")

# Scheduler Initialization
scheduler = AsyncIOScheduler()

# API Key Security Scheme for Swagger UI
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


# Pydantic Schemas
class HealthOut(BaseModel):
    status: str
    db: str


class StrategyParamsOut(BaseModel):
    asset: str
    opt_threshold: float
    opt_sl_mult: float
    opt_tp_mult: float
    live_approved: bool
    forecast_id: Optional[int] = None
    bullish_prob: Optional[float] = None
    bearish_prob: Optional[float] = None


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
    type: str
    price: float
    lots: float
    profit: float
    rsi: Optional[float] = None
    entry_score: Optional[float] = Field(default=None, alias="entry_score")
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    magic_number: Optional[int] = None
    account_type: Optional[str] = None
    session_hour: Optional[int] = None
    forecast_id: Optional[int] = None

    class Config:
        populate_by_name = True


# Security Dependency
async def verify_api_key(key: str = Depends(api_key_header)):
    expected_key = os.environ.get("ORCH_API_KEY", "")
    if not expected_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ORCH_API_KEY environment variable is not configured on server."
        )
    if key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header."
        )


# Unified Lifespan Manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize shared pool in startup.db
    await init_db_pool()

    # Start Background Jobs
    try:
        from startup.jobs.groq_forecast import run_forecast_cycle
        from startup.jobs.gatekeeper import run_gatekeeper_cycle
        from startup.jobs.auto_tester import continuous_tester_cycle

        # Forecast cycle every 30 minutes
        scheduler.add_job(run_forecast_cycle, 'interval', minutes=30, id="run_forecast_cycle")
        # Gatekeeper approval evaluation every 4 hours
        scheduler.add_job(run_gatekeeper_cycle, 'interval', hours=4, id="run_gatekeeper_cycle")
        # Headless MT5 optimization & inefficiency analysis once daily at 02:00
        scheduler.add_job(continuous_tester_cycle, 'cron', hour=2, id="continuous_tester_cycle")

        scheduler.start()
        logger.info("APScheduler initialized with forecast, gatekeeper, and auto-tester jobs.")
    except Exception as e:
        logger.warning(f"Background jobs failed to initialize: {e}")

    yield

    # Teardown
    scheduler.shutdown(wait=False)
    await close_db_pool()


# FastAPI Declaration
app = FastAPI(
    title="PROJECT KENJIN - Quant Matrix Orchestrator",
    description="High-frequency EA telemetry, indicator snapshot ingestion, and strategy parameter sync API.",
    version="10.0.0",
    lifespan=lifespan
)


# --- ROUTES ---

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


@app.get("/strategy_params", response_model=StrategyParamsOut, dependencies=[Depends(verify_api_key)], tags=["Strategy"], summary="Fetch Optimized Parameters")
async def get_strategy_params(asset: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT asset, opt_threshold, opt_sl_mult, opt_tp_mult, live_approved FROM strategy_db WHERE asset = $1",
            asset
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"Asset '{asset}' not configured in strategy_db.")

        forecast = await conn.fetchrow(
            "SELECT id, bullish_prob, bearish_prob FROM forecasts WHERE asset = $1 ORDER BY generated_at DESC LIMIT 1",
            asset
        )

    return StrategyParamsOut(
        asset=row["asset"],
        opt_threshold=float(row["opt_threshold"]) if row["opt_threshold"] is not None else 0.60,
        opt_sl_mult=float(row["opt_sl_mult"]) if row["opt_sl_mult"] is not None else 1.5,
        opt_tp_mult=float(row["opt_tp_mult"]) if row["opt_tp_mult"] is not None else 3.0,
        live_approved=bool(row["live_approved"]),
        forecast_id=forecast["id"] if forecast else None,
        bullish_prob=float(forecast["bullish_prob"]) if forecast and forecast["bullish_prob"] is not None else None,
        bearish_prob=float(forecast["bearish_prob"]) if forecast and forecast["bearish_prob"] is not None else None
    )


@app.post("/ticks", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Data Feed"], summary="Ingest Bar/Tick Feature Snapshot")
async def post_ticks(payload: TickIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tick_telemetry 
            (asset, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200, ts)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
            """,
            payload.asset, payload.bid, payload.ask, payload.tick_volume, payload.rsi,
            payload.tema, payload.ac, payload.sar, payload.adx, payload.ma10, payload.ma20,
            payload.ma50, payload.ma100, payload.ma200
        )
    return {"status": "created", "asset": payload.asset}


@app.post("/telemetry", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Telemetry"], summary="Log Trade Execution Telemetry")
async def post_telemetry(payload: TelemetryIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trade_telemetry 
            (asset, type, price, lots, profit, rsi, entry_score, sl_price, tp_price, magic_number, account_type, session_hour, forecast_id, ts)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
            """,
            payload.asset, payload.type, payload.price, payload.lots, payload.profit,
            payload.rsi, payload.entry_score, payload.sl_price, payload.tp_price,
            payload.magic_number, payload.account_type, payload.session_hour, payload.forecast_id
        )
    return {"status": "logged", "asset": payload.asset}