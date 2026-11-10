"""
PROJECT KENJIN v12 orchestrator. Run with:
    uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --env-file .env

v12 changes vs v11.09 (see docs/ANALYSIS_AND_IMPROVEMENTS.md):
  * scheduler jobs registered independently (one bad import no longer disables every job)
  * Yahoo Finance market-context service started in lifespan, exposed at /market/context
  * Trade Guardian endpoint POST /guardian/evaluate (loss-trend auto-cancellation)
  * /strategy_params now carries trading_halted (operator halt), market_bias/regime, and
    normalised BUY/SELL/NEUTRAL micro-trend; Tier-2 BUY/SELL is vetoed when fresh market
    context strongly opposes it
  * kill switch is enforced: it clears live_approved AND the EA is told to halt
  * online-learning kwarg bug fixed; training uses live+demo by default (TRAIN_ACCOUNT_TYPES)
"""
import asyncio
import json
import logging
import os
import random
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

from db.redis_client import redis_client
from startup.db import (
    close_db_pool, get_account_control, get_pool, init_db_pool, insert_guardian_action,
    list_account_controls, set_account_control,
)
from startup.schemas import (
    AccountSnapshotIn, GuardianDecisionOut, HealthOut, MarketContextOut, MissedTradeIn,
    PositionReportIn, RiskIncidentIn, StrategyParamsOut, TelemetryIn, TickIn,
)
from startup.jobs.ml_tier2 import (
    OnlineTierUpdater, clear_model_cache, compute_micro_trend, evaluate_tier2_signal,
    pull_feature_window_redis as redis_pull_feature_window, record_closed_trade_for_online_learning,
    set_online_updater, train_neural_maps,
)
from startup.jobs import lifeline_engine, trade_guardian
from startup.jobs.missed_trade_evaluator import get_missed_trade_summary, run_missed_trade_evaluation_cycle
from startup.market_context import service as market_service
from startup.auth import verify_api_key
from startup.ws_manager import manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("orchestrator")

scheduler = AsyncIOScheduler()
_VALID_MODEL_PATTERN = re.compile(r"^[A-Za-z0-9._\-]+(/[A-Za-z0-9._\-]+)*$")
_KPI_CACHE = {"data": None, "timestamp": 0}
KPI_CACHE_TTL = 10.0
TRAIN_ACCOUNT_TYPES = [s.strip() for s in os.environ.get("TRAIN_ACCOUNT_TYPES", "live,demo").split(",") if s.strip()]
MARKET_VETO_BIAS = float(os.environ.get("MARKET_VETO_BIAS", "0.5"))


# ----------------------------------------------------------------------------- models
class KillSwitchRequest(BaseModel):
    kill_switch: bool
    reason: Optional[str] = "Manual operator override"


class DrawdownHaltRequest(BaseModel):
    account_type: str
    halt: bool
    reason: Optional[str] = "Manual operator override from admin dashboard"


class StrategyPatchRequest(BaseModel):
    symbol: str
    live_approved: bool


class LifelineResetRequest(BaseModel):
    asset: str
    reason: Optional[str] = "Manual operator re-arm"


# ----------------------------------------------------------------------------- helpers
def validate_llm_config() -> str:
    provider = os.environ.get("LLM_PROVIDER", "groq").lower()
    model = os.environ.get("XAI_MODEL", "grok-4.5") if provider == "xai" else os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")
    if not _VALID_MODEL_PATTERN.match(model):
        raise RuntimeError(f"Model id '{model}' looks invalid. Check .env.")
    logger.info("LLM provider=%s model=%s", provider, model)
    return model


async def cache_tick_to_redis(asset: str, tick_data: dict):
    now = time.time()
    key = f"ticks:{asset}"
    tick_data = dict(tick_data)
    tick_data["ts"] = now
    try:
        await redis_client.zadd(key, {json.dumps(tick_data): now})
        await redis_client.zremrangebyscore(key, "-inf", now - 45 * 60)
    except Exception as e:
        logger.warning("Redis cache_tick_to_redis failed for %s: %s", asset, e)


async def insert_tick_row(payload: TickIn):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(
                """INSERT INTO tick_telemetry (asset,bid,ask,tick_volume,rsi,tema,ac,sar,adx,ma10,ma20,ma50,ma100,ma200,ts)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())""",
                payload.asset, payload.bid, payload.ask, payload.tick_volume, payload.rsi, payload.tema, payload.ac,
                payload.sar, payload.adx, payload.ma10, payload.ma20, payload.ma50, payload.ma100, payload.ma200)
    except Exception:
        logger.exception("Background insert_tick_row failed for %s", payload.asset)


def _features_from_rows(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["profit", "rsi", "bid", "ask", "ma10", "ma50", "tick_volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["ma_spread"] = df["ma10"] - df["ma50"]
    df["ma_spread_delta"] = df["ma_spread"] - df["ma_spread"].shift(1).fillna(0.0)
    df["price_velocity_1m"] = df["bid"].diff(1).fillna(0.0)
    mean_vol = df["tick_volume"].rolling(window=30, min_periods=1).mean()
    df["rvol"] = np.where(mean_vol > 0, df["tick_volume"] / mean_vol, 1.0)
    return df


async def fetch_recent_telemetry() -> pd.DataFrame:
    since = datetime.now(timezone.utc) - timedelta(days=30)
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT tt.profit, te.rsi, te.bid, te.ask, te.ma10, te.ma50, te.tick_volume, te.ts
            FROM trade_telemetry tt
            LEFT JOIN LATERAL (
                SELECT rsi, bid, ask, ma10, ma50, tick_volume, ts FROM tick_telemetry
                WHERE asset = tt.asset AND ts <= tt.created_at ORDER BY ts DESC LIMIT 1
            ) te ON true
            WHERE tt.created_at >= $1 AND tt.type LIKE '%CLOSE%' AND tt.account_type = ANY($2::text[])
            ORDER BY tt.created_at
            """, since, TRAIN_ACCOUNT_TYPES)
    if not rows:
        return pd.DataFrame()
    return _features_from_rows(pd.DataFrame([dict(r) for r in rows]))


async def get_hot_feature_window(asset: str, minutes: int = 30) -> pd.DataFrame:
    for attempt in range(3):
        try:
            return await redis_pull_feature_window(asset, minutes)
        except Exception:
            if attempt == 2:
                logger.exception("Redis feature window failed for %s.", asset)
            else:
                await asyncio.sleep(0.5 + random.uniform(0, 0.5))
    return pd.DataFrame()


async def run_retrain_cycle():
    df = await fetch_recent_telemetry()
    if len(df) < 50:
        logger.info("[Retrain] insufficient sample (%d/50) - skipping.", len(df))
        return
    if await asyncio.to_thread(train_neural_maps, df):
        logger.info("Retrained Triple Neural Map on %d trades.", len(df))
        try:
            await manager.broadcast({"type": "MODEL_UPDATED", "message": "Models retrained", "records": len(df), "timestamp": time.time()})
        except Exception as e:
            logger.warning("Model update broadcast failed: %s", e)


def _add_job(func, trigger: str, job_id: str, **kw):
    """Each job is registered on its own so one failure cannot disable the others."""
    try:
        scheduler.add_job(func, trigger, id=job_id, max_instances=1, coalesce=True, **kw)
    except Exception as e:
        logger.error("Could not schedule %s: %s", job_id, e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_llm_config()
    await init_db_pool()

    try:
        set_online_updater(await OnlineTierUpdater.load())
        logger.info("Online tier-2 EV adjustment model ready.")
    except Exception as e:
        logger.warning("OnlineTierUpdater unavailable (pip install river?): %s", e)

    market_service.configure(broadcast=manager.broadcast, redis_client=redis_client, pool_getter=get_pool)
    try:
        async with get_pool().acquire() as conn:
            for r in await conn.fetch("SELECT asset FROM strategy_db"):
                market_service.track_asset(r["asset"])
    except Exception:
        pass
    if os.environ.get("KENJIN_YAHOO_ENABLED", "1").lower() in ("1", "true", "yes"):
        await market_service.start()

    jobs = [
        ("startup.jobs.groq_forecast", "run_forecast_cycle", "interval", {"minutes": int(os.environ.get("FORECAST_INTERVAL_MIN", "15"))}),
        ("startup.jobs.gatekeeper", "run_gatekeeper_cycle", "interval", {"hours": int(os.environ.get("GATEKEEPER_INTERVAL_HOURS", "1"))}),
        ("startup.jobs.hour_scheduler", "run_hour_scheduler_cycle", "interval", {"hours": 12}),
        ("startup.jobs.confidence_calibration", "run_calibration_cycle", "interval", {"hours": 24}),
        ("startup.jobs.db_pruner", "run_all_pruning_cycles", "interval", {"hours": 24}),
        ("startup.jobs.auto_tester", "continuous_tester_cycle", "interval", {"hours": 4}),
        ("startup.jobs.lifeline_engine", "run_lifeline_protection_cycle", "interval", {"minutes": 5}),
    ]
    import importlib
    for mod_name, fn_name, trig, kw in jobs:
        try:
            fn = getattr(importlib.import_module(mod_name), fn_name)
            _add_job(fn, trig, fn_name, **kw)
        except Exception as e:
            logger.error("Job %s.%s not scheduled: %s", mod_name, fn_name, e)
    _add_job(run_retrain_cycle, "interval", "run_retrain_cycle", hours=6)
    _add_job(run_missed_trade_evaluation_cycle, "interval", "run_missed_trade_evaluation_cycle",
             minutes=int(os.environ.get("MISSED_TRADE_EVAL_INTERVAL_MIN", "15")))
    scheduler.start()
    logger.info("Scheduler started with %d jobs.", len(scheduler.get_jobs()))

    yield

    scheduler.shutdown(wait=False)
    await market_service.stop()
    await close_db_pool()


app = FastAPI(
    title="PROJECT KENJIN - Quant Matrix Orchestrator",
    description="EA telemetry, Yahoo market context, LLM forecasts, Tier-2 ML and Trade Guardian API.",
    version="12.0.0", lifespan=lifespan,
)
# Credentials + wildcard origin is invalid/unsafe; the Electron app is same-origin so none is needed.
app.add_middleware(CORSMiddleware, allow_origins=os.environ.get("CORS_ORIGINS", "http://127.0.0.1:8000").split(","),
                   allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR, check_dir=False), name="static")


# ----------------------------------------------------------------------------- websocket
@app.websocket("/ws/dashboard")
async def websocket_dashboard_endpoint(websocket: WebSocket):
    api_key = websocket.query_params.get("api_key") or websocket.headers.get("x-api-key") or ""
    expected = os.environ.get("ORCH_API_KEY", "")
    if not expected or api_key != expected:
        await websocket.close(code=1008)
        return
    await manager.connect(websocket)
    try:
        while True:
            if await websocket.receive_text() == "PING":
                await websocket.send_text("PONG")
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ----------------------------------------------------------------------------- health
@app.get("/health", response_model=HealthOut, tags=["System Health"])
async def health():
    try:
        async with get_pool().acquire() as conn:
            await conn.fetchval("SELECT 1;")
        db_status = "ok"
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Database unreachable: {exc}")
    try:
        await redis_client.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "down"
    provider = os.environ.get("LLM_PROVIDER", "groq").lower()
    key_name = "XAI_API_KEY" if provider == "xai" else "GROQ_API_KEY"
    llm_status = "configured" if os.environ.get(key_name, "").strip() else "not_configured"
    snap = market_service.snapshot()
    yahoo_status = "ok" if snap.get("fresh") else ("stale" if snap.get("ts") else "no_data")
    mt5_age = None
    try:
        async with get_pool().acquire() as conn:
            row = await conn.fetchrow("SELECT MAX(ts) AS last_ts FROM account_snapshots")
        if row and row["last_ts"]:
            mt5_age = (datetime.now(timezone.utc) - row["last_ts"]).total_seconds()
    except Exception:
        pass
    return HealthOut(status="ok", db=db_status, redis=redis_status, groq=llm_status, yahoo=yahoo_status, mt5_last_seen_seconds=mt5_age)


# ----------------------------------------------------------------------------- strategy params
def _norm_trend(t: str) -> str:
    t = (t or "").upper()
    return "BUY" if t in ("BUY", "BULLISH") else ("SELL" if t in ("SELL", "BEARISH") else "NEUTRAL")


@app.get("/strategy_params", response_model=StrategyParamsOut, dependencies=[Depends(verify_api_key)], tags=["Strategy"])
async def get_strategy_params(asset: str, account_type: Optional[str] = None):
    if account_type is not None:
        account_type = account_type.strip().lower()
        if account_type not in ("live", "demo"):
            raise HTTPException(status_code=400, detail="account_type must be 'live' or 'demo'.")
    market_service.track_asset(asset)
    pool = get_pool()
    base_cols = """s.asset, s.opt_threshold, s.opt_sl_mult, s.opt_tp_mult, s.rsi_buy_max, s.rsi_sell_min, s.live_approved,
                   s.scheduled_start_hour, s.scheduled_end_hour, s.calibration_score, s.calibration_n,
                   sl.lifeline_score, sl.terminated AS lifeline_terminated"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {base_cols} FROM strategy_db s LEFT JOIN strategy_lifeline sl ON s.asset = sl.asset WHERE s.asset = $1", asset)
        if not row:
            await conn.execute(
                """INSERT INTO strategy_db (asset,opt_threshold,opt_sl_mult,opt_tp_mult,rsi_buy_max,rsi_sell_min,live_approved,updated_at)
                   VALUES ($1,0.60,1.5,3.0,70.0,30.0,false,NOW()) ON CONFLICT (asset) DO NOTHING""", asset)
            row = await conn.fetchrow(
                f"SELECT {base_cols} FROM strategy_db s LEFT JOIN strategy_lifeline sl ON s.asset = sl.asset WHERE s.asset = $1", asset)
        forecast = await conn.fetchrow(
            "SELECT id, bullish_prob, bearish_prob FROM forecasts WHERE asset = $1 ORDER BY generated_at DESC LIMIT 1", asset)

    bullish = float(forecast["bullish_prob"]) if forecast and forecast["bullish_prob"] is not None else 50.0
    bearish = float(forecast["bearish_prob"]) if forecast and forecast["bearish_prob"] is not None else 50.0
    opt_threshold = float(row["opt_threshold"]) if row["opt_threshold"] is not None else 0.60

    cal_n, cal_score = row["calibration_n"], row["calibration_score"]
    if cal_n is None or int(cal_n) < 30:
        cal_mult = 0.6
    elif cal_score is not None and float(cal_score) > 0.25:
        cal_mult = 0.75
    else:
        cal_mult = 1.0

    try:
        life_mult, life_term = await lifeline_engine.get_lifeline_multiplier(asset)
    except Exception:
        logger.exception("Lifeline lookup failed for %s - degrading to 1.0/False.", asset)
        life_mult, life_term = 1.0, False

    halted, halt_reason = False, None
    if account_type:
        ctrl = await get_account_control(account_type.strip().lower())
        halted, halt_reason = bool(ctrl.get("manual_drawdown_halt")), ctrl.get("halt_reason")

    try:
        df = await get_hot_feature_window(asset, minutes=30)
        tier2 = await asyncio.to_thread(evaluate_tier2_signal, df, bullish, bearish, opt_threshold, 1.0, cal_mult, life_mult, life_term)
        micro = compute_micro_trend(df)
    except Exception:
        logger.exception("Tier-2 evaluation failed for %s - degrading to HOLD/neutral.", asset)
        tier2 = {"action": "HOLD", "confidence": 0.0, "lot_multiplier": 1.0}
        micro = {"micro_trend": "NEUTRAL", "micro_trend_strength": 0.0}

    ctx = market_service.get_asset_context(asset)
    note = None
    action = tier2.get("action", "HOLD")
    if ctx["fresh"] and action in ("BUY", "SELL"):
        opposed = (action == "BUY" and ctx["bias"] <= -MARKET_VETO_BIAS) or (action == "SELL" and ctx["bias"] >= MARKET_VETO_BIAS)
        if opposed:
            note = f"market_context_veto bias={ctx['bias']:+.2f} regime={ctx['regime']}"
            action = "HOLD"

    try:  # cached for the Trade Guardian's "Tier-2 against" vote
        await redis_client.set(f"tier2:{asset}", json.dumps({"action": action, "ts": time.time()}), ex=600)
    except Exception:
        pass

    return StrategyParamsOut(
        asset=row["asset"], opt_threshold=opt_threshold,
        opt_sl_mult=float(row["opt_sl_mult"]) if row["opt_sl_mult"] is not None else 1.5,
        opt_tp_mult=float(row["opt_tp_mult"]) if row["opt_tp_mult"] is not None else 3.0,
        rsi_buy_max=float(row["rsi_buy_max"]) if row["rsi_buy_max"] is not None else 70.0,
        rsi_sell_min=float(row["rsi_sell_min"]) if row["rsi_sell_min"] is not None else 30.0,
        live_approved=bool(row["live_approved"]) and not life_term,
        forecast_id=forecast["id"] if forecast else None, bullish_prob=bullish, bearish_prob=bearish,
        tier2_action=action, tier2_confidence=tier2.get("confidence", 0.0),
        recommended_lot_multiplier=tier2.get("lot_multiplier", 1.0),
        scheduled_start_hour=row["scheduled_start_hour"], scheduled_end_hour=row["scheduled_end_hour"],
        micro_trend=_norm_trend(micro.get("micro_trend", "NEUTRAL")), micro_trend_strength=micro.get("micro_trend_strength", 0.0),
        calibration_score=float(cal_score) if cal_score is not None else None, calibration_n=cal_n,
        volatility_regime=tier2.get("volatility_regime", 0.0),
        lifeline_score=float(row["lifeline_score"]) if row["lifeline_score"] is not None else 100.0,
        lifeline_terminated=bool(life_term),
        trading_halted=halted, halt_reason=halt_reason,
        market_bias=ctx["bias"], market_regime=ctx["regime"], market_fresh=ctx["fresh"], tier2_note=note,
    )


# ----------------------------------------------------------------------------- ingestion
@app.post("/ticks", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Data Feed"])
async def post_ticks(payload: TickIn, background_tasks: BackgroundTasks):
    background_tasks.add_task(insert_tick_row, payload)
    background_tasks.add_task(cache_tick_to_redis, payload.asset, payload.model_dump())
    market_service.track_asset(payload.asset)
    await manager.broadcast({"type": "TICK_UPDATE", "asset": payload.asset, "timestamp": time.time(), "price": payload.bid,
                             "micro_trend": market_service.get_asset_context(payload.asset)["regime"], "expected_value": 0.0})
    return {"status": "accepted", "asset": payload.asset}


async def trigger_online_learning_update(asset: str, closed_profit: float, entry_score: float):
    """Feeds the online learner with the feature snapshot nearest the close (see docs: entry features roadmap)."""
    try:
        async with get_pool().acquire() as conn:
            rows = await conn.fetch(
                "SELECT rsi, bid, ma10, ma50, tick_volume FROM tick_telemetry WHERE asset=$1 ORDER BY ts DESC LIMIT 30", asset)
        if not rows:
            return
        df = _features_from_rows(pd.DataFrame([dict(r) for r in rows]).iloc[::-1].reset_index(drop=True))
        latest = df.iloc[-1]
        await record_closed_trade_for_online_learning(
            features_map={"ma_spread_delta": float(latest["ma_spread_delta"]), "price_velocity_1m": float(latest["price_velocity_1m"]),
                          "rsi": float(latest["rsi"]), "rvol": float(latest["rvol"])},
            batch_e_utility=float(entry_score) if entry_score else 0.0, actual_profit=closed_profit)
    except Exception as e:
        logger.warning("Online learning update failed for %s: %s", asset, e)


@app.post("/telemetry", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Telemetry"])
async def post_telemetry(payload: TelemetryIn, background_tasks: BackgroundTasks):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(
                """INSERT INTO trade_telemetry (asset,type,price,lots,profit,rsi,entry_score,sl_price,tp_price,magic_number,
                                                account_type,session_hour,forecast_id,tier2_confidence,created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())""",
                payload.asset, payload.type, payload.price, payload.lots, payload.profit, payload.rsi, payload.entry_score,
                payload.sl_price, payload.tp_price, payload.magic_number, payload.account_type, payload.session_hour,
                payload.forecast_id, payload.tier2_confidence)
    except Exception:
        logger.exception("Failed to insert trade_telemetry for %s", payload.asset)
        raise HTTPException(status_code=500, detail="Failed to store telemetry.")
    if "CLOSE" in (payload.type or ""):
        _KPI_CACHE["data"] = None
        if payload.account_type == "live":
            background_tasks.add_task(lifeline_engine.record_trade_outcome, payload.asset, payload.profit)
        background_tasks.add_task(trigger_online_learning_update, payload.asset, payload.profit, payload.entry_score)
        await manager.broadcast({"type": "TRADE_CLOSED", "asset": payload.asset, "profit": payload.profit})
    return {"status": "logged", "asset": payload.asset}


@app.post("/account_snapshot", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Telemetry"])
async def post_account_snapshot(payload: AccountSnapshotIn):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(
                """INSERT INTO account_snapshots (account_type,login,asset,balance,equity,margin,margin_level,floating_pl,peak_equity,
                       drawdown_pct,day_loss_pct,day_start_balance,consecutive_losses,consecutive_wins,risk_cooldown_active,drawdown_halt,ts)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())""",
                payload.account_type, payload.login, payload.asset, payload.balance, payload.equity, payload.margin,
                payload.margin_level, payload.floating_pl, payload.peak_equity, payload.drawdown_pct, payload.day_loss_pct,
                payload.day_start_balance, payload.consecutive_losses, payload.consecutive_wins,
                payload.risk_cooldown_active, payload.drawdown_halt)
    except Exception:
        logger.exception("Failed to insert account_snapshots row")
        raise HTTPException(status_code=500, detail="Failed to store account snapshot.")
    return {"status": "logged", "account_type": payload.account_type}


@app.post("/risk_incident", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Control"])
async def post_risk_incident(payload: RiskIncidentIn):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute("INSERT INTO risk_incidents (account_type,asset,reason,details) VALUES ($1,$2,$3,$4)",
                               payload.account_type, payload.asset, payload.reason, payload.details)
    except Exception:
        logger.exception("Failed to insert risk_incidents row")
        raise HTTPException(status_code=500, detail="Failed to store risk incident.")
    logger.warning("RISK INCIDENT: %s | %s | %s | %s", payload.account_type, payload.asset, payload.reason, payload.details)
    await manager.broadcast({"type": "RISK_ALERT", "severity": "HIGH", "message": payload.reason})
    return {"status": "logged", "reason": payload.reason}


@app.post("/missed_trade", status_code=status.HTTP_201_CREATED, dependencies=[Depends(verify_api_key)], tags=["Telemetry"])
async def post_missed_trade(payload: MissedTradeIn):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(
                """INSERT INTO missed_trade_signals (asset,account_type,reason,side,score,raw_score,threshold,rsi,price,atr,
                       sl_distance,tp_distance,tier2_action,tier2_confidence,session_hour,magic_number,created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())""",
                payload.asset, payload.account_type, payload.reason, payload.side, payload.score, payload.raw_score,
                payload.threshold, payload.rsi, payload.price, payload.atr, payload.sl_distance, payload.tp_distance,
                payload.tier2_action, payload.tier2_confidence, payload.session_hour, payload.magic_number)
    except Exception:
        logger.exception("Failed to insert missed_trade_signals for %s", payload.asset)
        raise HTTPException(status_code=500, detail="Failed to store missed trade signal.")
    await manager.broadcast({"type": "MISSED_TRADE", "asset": payload.asset, "reason": payload.reason, "side": payload.side, "raw_score": payload.raw_score})
    return {"status": "logged", "asset": payload.asset, "reason": payload.reason}


@app.get("/missed_trades/summary", dependencies=[Depends(verify_api_key)], tags=["Monitoring"])
async def missed_trades_summary(days: int = 7):
    return await get_missed_trade_summary(days=max(1, min(days, 90)))


# ----------------------------------------------------------------------------- v12: market context + guardian
@app.get("/market/context", dependencies=[Depends(verify_api_key)], tags=["Market Context"], response_model=MarketContextOut)
async def market_context():
    s = market_service.snapshot()
    return MarketContextOut(fresh=s["fresh"], regime=s.get("regime", "unknown"), summary=s.get("summary", ""),
                            macro=s.get("macro", {}), assets=s.get("assets", {}), ts=s.get("ts", 0.0))


@app.get("/market/context/{asset}", dependencies=[Depends(verify_api_key)], tags=["Market Context"])
async def market_context_asset(asset: str):
    return market_service.get_asset_context(asset)


@app.post("/market/refresh", dependencies=[Depends(verify_api_key)], tags=["Market Context"])
async def market_refresh():
    snap = await market_service.refresh_once()
    return {"fresh": snap.get("fresh"), "regime": snap.get("regime"), "summary": snap.get("summary", "")}


@app.post("/guardian/evaluate", response_model=GuardianDecisionOut, dependencies=[Depends(verify_api_key)], tags=["Guardian"])
async def guardian_evaluate(report: PositionReportIn, background_tasks: BackgroundTasks):
    """Called by the EA every few seconds per open position; answers HOLD / CLOSE / MOVE_SL."""
    ctx = market_service.get_asset_context(report.asset)
    tier2_action, micro_dir, micro_strength = "HOLD", "NEUTRAL", 0.0
    try:
        df = await get_hot_feature_window(report.asset, minutes=15)
        if df is not None and len(df) >= 5:
            micro = compute_micro_trend(df)
            micro_dir, micro_strength = micro["micro_trend"], micro["micro_trend_strength"]
    except Exception:
        pass
    # Tier-2 action comes from the most recent EA-visible evaluation cached in Redis (cheap) when available.
    try:
        cached = await redis_client.get(f"tier2:{report.asset}")
        if cached:
            tier2_action = json.loads(cached).get("action", "HOLD")
    except Exception:
        pass

    decision = trade_guardian.evaluate(report.model_dump(), tier2_action=tier2_action, micro_trend=micro_dir,
                                       micro_strength=micro_strength, market_bias=ctx["bias"], market_fresh=ctx["fresh"])
    if decision["action"] != "HOLD":
        logger.warning("GUARDIAN %s %s #%s r=%.2f reason=%s votes=%s", decision["action"], report.asset, report.ticket,
                       decision["r_multiple"], decision["reason"], decision.get("votes"))
        background_tasks.add_task(insert_guardian_action, report.asset, report.model_dump(), decision)
        await manager.broadcast({"type": "GUARDIAN_ACTION", "asset": report.asset, "ticket": report.ticket,
                                 "action": decision["action"], "reason": decision["reason"], "r_multiple": decision["r_multiple"]})
        if decision["action"] == "CLOSE":
            trade_guardian.forget(report.ticket)
    return GuardianDecisionOut(**{k: decision.get(k) for k in GuardianDecisionOut.model_fields if decision.get(k) is not None})


# ----------------------------------------------------------------------------- ML / jobs
@app.post("/models/reload", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def models_reload():
    clear_model_cache()
    await manager.broadcast({"type": "MODEL_UPDATED", "message": "Admin requested model reload", "timestamp": time.time()})
    return {"status": "ok", "message": "Model cache cleared."}


async def _retrain_response(background_tasks: BackgroundTasks):
    df = await fetch_recent_telemetry()
    if df.empty or len(df) < 50:
        raise HTTPException(status_code=400, detail="Insufficient telemetry data to retrain (< 50 records).")
    background_tasks.add_task(asyncio.to_thread, train_neural_maps, df)
    return {"status": "queued", "records": len(df), "message": "LightGBM Triple Neural Map retraining initiated."}


@app.post("/retrain", dependencies=[Depends(verify_api_key)], tags=["ML Engine"])
async def trigger_retrain(background_tasks: BackgroundTasks):
    return await _retrain_response(background_tasks)


@app.post("/jobs/retrain", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def trigger_model_retrain(background_tasks: BackgroundTasks):
    return await _retrain_response(background_tasks)


@app.post("/jobs/calibrate", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def trigger_confidence_calibration(background_tasks: BackgroundTasks):
    from startup.jobs.confidence_calibration import run_calibration_cycle
    background_tasks.add_task(run_calibration_cycle)
    return {"status": "success", "message": "Calibration queued."}


@app.post("/hour_scheduler/run", dependencies=[Depends(verify_api_key)], tags=["Scheduling"])
async def trigger_hour_scheduler(background_tasks: BackgroundTasks):
    from startup.jobs.hour_scheduler import run_hour_scheduler_cycle
    background_tasks.add_task(run_hour_scheduler_cycle)
    return {"status": "hour_scheduler_triggered"}


@app.post("/jobs/forecast", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def trigger_forecast(background_tasks: BackgroundTasks):
    from startup.jobs.groq_forecast import run_forecast_cycle
    background_tasks.add_task(run_forecast_cycle)
    return {"status": "queued"}


# ----------------------------------------------------------------------------- KPIs
@app.get("/kpis", dependencies=[Depends(verify_api_key)], tags=["Monitoring"])
async def get_kpis():
    now = time.time()
    if _KPI_CACHE["data"] and (now - _KPI_CACHE["timestamp"]) < KPI_CACHE_TTL:
        return _KPI_CACHE["data"]
    try:
        async with get_pool().acquire() as conn:
            per_asset = await conn.fetch(
                """
                SELECT COALESCE(t.asset, s.asset) AS asset,
                       COUNT(*) FILTER (WHERE t.type LIKE '%CLOSE%') AS closed_trades,
                       COUNT(*) FILTER (WHERE t.type LIKE '%CLOSE%' AND t.profit > 0) AS wins,
                       COALESCE(SUM(t.profit) FILTER (WHERE t.type LIKE '%CLOSE%'), 0) AS total_profit,
                       s.live_approved, s.opt_threshold, s.opt_sl_mult, s.opt_tp_mult, s.scheduled_start_hour,
                       s.scheduled_end_hour, s.calibration_score, s.calibration_n, sl.lifeline_score, sl.terminated AS lifeline_terminated
                FROM strategy_db s
                FULL OUTER JOIN trade_telemetry t ON s.asset = t.asset AND t.created_at >= NOW() - INTERVAL '30 days'
                LEFT JOIN strategy_lifeline sl ON sl.asset = COALESCE(s.asset, t.asset)
                GROUP BY COALESCE(t.asset, s.asset), s.live_approved, s.opt_threshold, s.opt_sl_mult, s.opt_tp_mult,
                         s.scheduled_start_hour, s.scheduled_end_hour, s.calibration_score, s.calibration_n, sl.lifeline_score, sl.terminated
                ORDER BY 1""")
            equity_curve = await conn.fetch(
                """SELECT created_at, profit, SUM(profit) OVER (ORDER BY created_at) AS cumulative_profit
                   FROM trade_telemetry WHERE type LIKE '%CLOSE%' AND created_at >= NOW() - INTERVAL '30 days' ORDER BY created_at""")
            recent_trades = await conn.fetch(
                """SELECT asset,type,price,lots,profit,tier2_confidence,created_at FROM trade_telemetry
                   WHERE type LIKE '%CLOSE%' ORDER BY created_at DESC LIMIT 25""")
            latest_forecasts = await conn.fetch(
                "SELECT DISTINCT ON (asset) asset,bullish_prob,bearish_prob,generated_at FROM forecasts ORDER BY asset, generated_at DESC")
            latest_accounts = await conn.fetch(
                """SELECT DISTINCT ON (account_type) account_type,login,asset,balance,equity,margin,margin_level,floating_pl,peak_equity,
                          drawdown_pct,day_loss_pct,consecutive_losses,consecutive_wins,risk_cooldown_active,drawdown_halt,ts
                   FROM account_snapshots ORDER BY account_type, ts DESC""")
            controls = await conn.fetch("SELECT account_type, manual_drawdown_halt, halt_reason, updated_at FROM account_controls")
            incidents = await conn.fetch("SELECT account_type,asset,reason,details,created_at FROM risk_incidents ORDER BY created_at DESC LIMIT 20")
            guardian = await conn.fetch(
                "SELECT asset,ticket,action,reason,r_multiple,created_at FROM guardian_actions ORDER BY created_at DESC LIMIT 15")

        def f(v):
            return float(v) if v is not None else None

        per_asset = [r for r in per_asset if r["asset"] is not None]
        closed = sum(int(r["closed_trades"] or 0) for r in per_asset)
        wins = sum(int(r["wins"] or 0) for r in per_asset)
        total_profit = sum(float(r["total_profit"] or 0) for r in per_asset)
        ctl = {r["account_type"]: dict(r) for r in controls}
        ctx = market_service.snapshot()

        data = {
            "summary": {"total_closed_trades": closed, "total_wins": wins,
                        "overall_win_rate": round(100.0 * wins / closed, 1) if closed else 0.0,
                        "total_profit": round(total_profit, 2), "assets_tracked": len(per_asset),
                        "assets_live_approved": sum(1 for r in per_asset if r["live_approved"])},
            "per_asset": [{
                "asset": r["asset"], "closed_trades": int(r["closed_trades"] or 0), "wins": int(r["wins"] or 0),
                "win_rate": round(100.0 * (r["wins"] or 0) / r["closed_trades"], 1) if (r["closed_trades"] or 0) > 0 else 0.0,
                "total_profit": round(float(r["total_profit"] or 0), 2), "live_approved": bool(r["live_approved"]),
                "opt_threshold": f(r["opt_threshold"]), "opt_sl_mult": f(r["opt_sl_mult"]), "opt_tp_mult": f(r["opt_tp_mult"]),
                "scheduled_start_hour": r["scheduled_start_hour"], "scheduled_end_hour": r["scheduled_end_hour"],
                "calibration_score": f(r["calibration_score"]), "calibration_n": r["calibration_n"],
                "lifeline_score": f(r["lifeline_score"]), "lifeline_terminated": bool(r["lifeline_terminated"]),
                "market_bias": market_service.get_asset_context(r["asset"])["bias"],
            } for r in per_asset],
            "equity_curve": [{"created_at": r["created_at"].isoformat(), "profit": float(r["profit"]),
                              "cumulative_profit": round(float(r["cumulative_profit"]), 2)} for r in equity_curve],
            "recent_trades": [{"asset": r["asset"], "type": r["type"], "price": f(r["price"]), "lots": f(r["lots"]),
                               "profit": f(r["profit"]), "tier2_confidence": f(r["tier2_confidence"]),
                               "created_at": r["created_at"].isoformat()} for r in recent_trades],
            "latest_forecasts": [{"asset": r["asset"], "bullish_prob": f(r["bullish_prob"]), "bearish_prob": f(r["bearish_prob"]),
                                  "generated_at": r["generated_at"].isoformat()} for r in latest_forecasts],
            "accounts": [{
                "account_type": r["account_type"], "login": r["login"], "asset": r["asset"], "balance": f(r["balance"]),
                "equity": f(r["equity"]), "floating_pl": f(r["floating_pl"]), "margin_level": f(r["margin_level"]),
                "peak_equity": f(r["peak_equity"]), "drawdown_pct": f(r["drawdown_pct"]), "day_loss_pct": f(r["day_loss_pct"]),
                "consecutive_losses": r["consecutive_losses"], "risk_cooldown_active": r["risk_cooldown_active"],
                "drawdown_halt": r["drawdown_halt"], "last_updated": r["ts"].isoformat(),
                "seconds_since_update": (datetime.now(timezone.utc) - r["ts"]).total_seconds(),
                "manual_drawdown_halt": bool(ctl.get(r["account_type"], {}).get("manual_drawdown_halt", False)),
                "manual_halt_reason": ctl.get(r["account_type"], {}).get("halt_reason"),
            } for r in latest_accounts],
            "risk_incidents": [{"account_type": r["account_type"], "asset": r["asset"], "reason": r["reason"],
                                "details": r["details"], "created_at": r["created_at"].isoformat()} for r in incidents],
            "guardian_actions": [{"asset": r["asset"], "ticket": r["ticket"], "action": r["action"], "reason": r["reason"],
                                  "r_multiple": f(r["r_multiple"]), "created_at": r["created_at"].isoformat()} for r in guardian],
            "market_context": {"fresh": ctx.get("fresh"), "regime": ctx.get("regime"), "summary": ctx.get("summary", "")},
        }
        _KPI_CACHE.update(data=data, timestamp=now)
        return data
    except Exception as e:
        logger.error("KPI error: %s", e)
        if _KPI_CACHE["data"]:
            return _KPI_CACHE["data"]
        raise HTTPException(status_code=500, detail="Database error and no cached data available.")


# ----------------------------------------------------------------------------- control
@app.post("/control/lifeline_reset", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def lifeline_reset(payload: LifelineResetRequest):
    await lifeline_engine.manual_reset(payload.asset, payload.reason)
    return {"status": "success", "asset": payload.asset, "message": "Lifeline reset to baseline."}


@app.post("/control/kill_switch", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def emergency_kill_switch(payload: KillSwitchRequest):
    """Global halt. Clearing the halt never silently restores per-asset live approval."""
    if payload.kill_switch:
        async with get_pool().acquire() as conn:
            await conn.execute("UPDATE strategy_db SET live_approved = FALSE, updated_at = NOW()")
    for acct in ("live", "demo"):
        await set_account_control(
            acct, bool(payload.kill_switch),
            f"KILL SWITCH: {payload.reason}" if payload.kill_switch else "Global halt cleared; per-asset approvals remain unchanged.",
            "dashboard_operator",
        )
    _KPI_CACHE["data"] = None
    severity = "CRITICAL" if payload.kill_switch else "WARNING"
    message = (f"Global kill switch engaged: {payload.reason}" if payload.kill_switch
               else "Global halt cleared. Live trading still requires individual asset approval.")
    await manager.broadcast({"type": "RISK_ALERT", "severity": severity, "message": message})
    return {"status": "success", "kill_switch": payload.kill_switch, "message": message}


@app.patch("/strategy_params", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def patch_strategy_params(payload: StrategyPatchRequest):
    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE strategy_db SET live_approved = $1 WHERE asset = $2", payload.live_approved, payload.symbol)
    _KPI_CACHE["data"] = None
    return {"status": "success", "symbol": payload.symbol, "live_approved": payload.live_approved,
            "message": f"Asset {payload.symbol} live execution updated to {payload.live_approved}."}


@app.post("/control/drawdown_halt", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def set_drawdown_halt(payload: DrawdownHaltRequest):
    account_type = payload.account_type.strip().lower()
    if account_type not in ("live", "demo"):
        raise HTTPException(status_code=400, detail="account_type must be 'live' or 'demo'.")
    result = await set_account_control(account_type, payload.halt, payload.reason, "dashboard_operator")
    async with get_pool().acquire() as conn:
        await conn.execute("INSERT INTO risk_incidents (account_type,asset,reason,details) VALUES ($1,$2,$3,$4)", account_type, None,
                           "manual_drawdown_halt" if payload.halt else "manual_drawdown_halt_cleared", payload.reason)
    logger.warning("MANUAL DRAWDOWN HALT %s for %s | %s", "ENGAGED" if payload.halt else "CLEARED", account_type, payload.reason)
    await manager.broadcast({"type": "ACCOUNT_CONTROL_UPDATE", "account_type": account_type,
                             "manual_drawdown_halt": payload.halt, "reason": payload.reason, "timestamp": time.time()})
    _KPI_CACHE["data"] = None
    return {"status": "success", "account_type": account_type, "manual_drawdown_halt": result["manual_drawdown_halt"],
            "message": f"Automated trading {'HALTED' if payload.halt else 'RESUMED'} for {account_type} accounts."}


@app.get("/control/drawdown_halt", dependencies=[Depends(verify_api_key)], tags=["Control"])
async def get_drawdown_halt(account_type: Optional[str] = None):
    if account_type:
        normalized = account_type.strip().lower()
        if normalized not in ("live", "demo"):
            raise HTTPException(status_code=400, detail="account_type must be 'live' or 'demo'.")
        return await get_account_control(normalized)
    return {"accounts": await list_account_controls()}
