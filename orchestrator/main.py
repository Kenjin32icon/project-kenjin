"""
Entry point. Run with:
    uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --reload

(drop --reload for the systemd/Render deployment - it's a dev-only convenience)
"""
import os
import json
import logging
import pandas as pd
import asyncpg
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from groq import AsyncGroq

# Load environment variables
load_dotenv()
database_url = os.getenv("DATABASE_URL")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orchestrator")

# ==========================================
# 1. AUTHENTICATION & DATABASE
# ==========================================
def verify_api_key(x_api_key: str = Header(default="")) -> None:
    expected = os.environ.get("ORCH_API_KEY", "")
    if not expected:
        raise HTTPException(status_code=500, detail="ORCH_API_KEY is not configured on the server.")
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key header.")

_pool: Optional[asyncpg.Pool] = None

async def init_db_pool() -> asyncpg.Pool:
    global _pool
    database_url = os.environ["DATABASE_URL"]
    _pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10, command_timeout=10)
    return _pool

async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised.")
    return _pool

# ==========================================
# 2. PYDANTIC SCHEMAS
# ==========================================
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

class StrategyParamsOut(BaseModel):
    asset: str
    opt_threshold: float
    opt_sl_mult: float
    opt_tp_mult: float
    live_approved: bool
    forecast_id: Optional[int] = None
    bullish_prob: Optional[float] = None
    bearish_prob: Optional[float] = None

class HealthOut(BaseModel):
    status: str
    db: str

# ==========================================
# 3. BACKGROUND JOBS
# ==========================================
GROQ_MODEL = os.environ.get("GROQ_MODEL", "GPT OSS 20B")

async def get_distinct_assets() -> list[str]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT asset FROM tick_telemetry;")
    return [r["asset"] for r in rows]

async def pull_feature_window(asset: str, minutes: int = 30) -> pd.DataFrame:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT ts, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200
            FROM tick_telemetry
            WHERE asset = $1 AND ts >= now() - ($2 || ' minutes')::interval
            ORDER BY ts ASC
            """,
            asset, str(minutes),
        )
    return pd.DataFrame([dict(r) for r in rows])

async def run_forecast_cycle() -> None:
    client = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY"))
    assets = await get_distinct_assets()
    for asset in assets:
        try:
            df = await pull_feature_window(asset, minutes=30)
            if df.empty:
                continue
                
            latest = df.iloc[-1]
            summary = json.dumps({
                "n_ticks": len(df), "latest_bid": float(latest.get("bid") or 0),
                "tema_trend": "rising" if df["tema"].iloc[-1] > df["tema"].iloc[0] else "falling",
            })
            
            completion = await client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "Return ONLY JSON: {\"bullish_prob\": 50, \"bearish_prob\": 50, \"suggested_sl_atr_mult\": 1.5, \"suggested_tp_atr_mult\": 3.0, \"rationale\": \"string\"}"},
                    {"role": "user", "content": summary},
                ],
                temperature=0.2,
            )
            data = json.loads(completion.choices[0].message.content.strip())
            
            pool = get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO forecasts (asset, horizon_minutes, bullish_prob, bearish_prob, suggested_sl_atr_mult, suggested_tp_atr_mult, rationale, model_used)
                    VALUES ($1, 30, $2, $3, $4, $5, $6, $7)
                    """,
                    asset, data["bullish_prob"], data["bearish_prob"], data["suggested_sl_atr_mult"], data["suggested_tp_atr_mult"], data.get("rationale", ""), GROQ_MODEL,
                )
                await conn.execute(
                    "UPDATE strategy_db SET opt_sl_mult = $2, opt_tp_mult = $3, updated_at = now() WHERE asset = $1",
                    asset, data["suggested_sl_atr_mult"], data["suggested_tp_atr_mult"],
                )
        except Exception as e:
            log.exception(f"Forecast failed for {asset}")

async def run_gatekeeper_cycle() -> None:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT asset,
              COUNT(*) FILTER (WHERE type LIKE '%CLOSE%') AS closed_trades,
              ROUND(100.0 * COUNT(*) FILTER (WHERE profit > 0) / NULLIF(COUNT(*) FILTER (WHERE type LIKE '%CLOSE%'), 0), 1) AS win_rate,
              ROUND((SUM(profit) FILTER (WHERE profit > 0) / NULLIF(ABS(SUM(profit) FILTER (WHERE profit < 0)), 0))::numeric, 2) AS profit_factor
            FROM trade_telemetry WHERE created_at >= now() - INTERVAL '7 days' GROUP BY asset
            """
        )
        for row in rows:
            closed = row["closed_trades"] or 0
            win_rate = float(row["win_rate"]) if row["win_rate"] else 0.0
            pf = float(row["profit_factor"]) if row["profit_factor"] else 0.0
            qualifies = (closed >= 30 and win_rate >= 55.0 and pf >= 1.3)
            
            await conn.execute(
                "UPDATE strategy_db SET win_rate = $2, profit_factor = $3, sample_size = $4, live_approved = $5, updated_at = now() WHERE asset = $1",
                row["asset"], win_rate, pf, closed, qualifies,
            )

# ==========================================
# 4. FASTAPI APP & ROUTES
# ==========================================
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    scheduler.add_job(run_forecast_cycle, "interval", minutes=30, id="groq_forecast", max_instances=1, coalesce=True)
    scheduler.add_job(run_gatekeeper_cycle, "interval", hours=4, id="gatekeeper", max_instances=1, coalesce=True)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)
    await close_db_pool()

app = FastAPI(title="Project KENJIN Orchestrator", version="10.0.0", lifespan=lifespan)

@app.get("/health", response_model=HealthOut)
async def health():
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1;")
        return HealthOut(status="ok", db="ok")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB check failed: {exc}")

@app.get("/strategy_params", response_model=StrategyParamsOut, dependencies=[Depends(verify_api_key)])
async def get_strategy_params(asset: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT asset, opt_threshold, opt_sl_mult, opt_tp_mult, live_approved FROM strategy_db WHERE asset = $1", asset)
        if not row:
            raise HTTPException(status_code=404, detail="No strategy_db row.")
        forecast = await conn.fetchrow("SELECT id, bullish_prob, bearish_prob FROM forecasts WHERE asset = $1 ORDER BY generated_at DESC LIMIT 1", asset)
        
    return StrategyParamsOut(
        asset=row["asset"], opt_threshold=float(row["opt_threshold"] or 0.60), opt_sl_mult=float(row["opt_sl_mult"] or 1.5),
        opt_tp_mult=float(row["opt_tp_mult"] or 3.0), live_approved=bool(row["live_approved"]),
        forecast_id=forecast["id"] if forecast else None, 
        bullish_prob=float(forecast["bullish_prob"]) if forecast and forecast["bullish_prob"] else None,
        bearish_prob=float(forecast["bearish_prob"]) if forecast and forecast["bearish_prob"] else None,
    )

@app.post("/ticks", dependencies=[Depends(verify_api_key)], status_code=201)
async def post_ticks(data: TickIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tick_telemetry (asset, ts, bid, ask, tick_volume, rsi, tema, ac, sar, adx, ma10, ma20, ma50, ma100, ma200)
            VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            """,
            data.asset, data.bid, data.ask, data.tick_volume, data.rsi, data.tema, data.ac, data.sar, data.adx, data.ma10, data.ma20, data.ma50, data.ma100, data.ma200
        )
    return {"status": "success"}

@app.post("/telemetry", dependencies=[Depends(verify_api_key)], status_code=201)
async def post_telemetry(data: TelemetryIn):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trade_telemetry (asset, type, price, lots, profit, created_at, rsi, entry_score, sl_price, tp_price, magic_number, account_type, session_hour, forecast_id)
            VALUES ($1, $2, $3, $4, $5, now(), $6, $7, $8, $9, $10, $11, $12, $13)
            """,
            data.asset, data.type, data.price, data.lots, data.profit, data.rsi, data.entry_score, data.sl_price, data.tp_price, data.magic_number, data.account_type, data.session_hour, data.forecast_id
        )
    return {"status": "success"}