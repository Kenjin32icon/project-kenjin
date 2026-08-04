from fastapi import APIRouter, Depends, HTTPException
from startup.db import get_pool
from startup.schemas import StrategyParamsOut
from startup.auth import verify_api_key

router = APIRouter()


@router.get("/strategy_params", response_model=StrategyParamsOut, dependencies=[Depends(verify_api_key)])
async def get_strategy_params(asset: str):
    """
    Flat JSON, not PostgREST's array-wrapped row - by design (see v10's
    header comment on why this avoids the "[]-looks-like-success" trap
    that the old direct-Supabase integration had).

    Joins the latest forecast for this asset so the EA gets threshold/SL/TP
    AND live_approved AND the forecast_id to attach to its next trade, in
    one request instead of two.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT asset, opt_threshold, opt_sl_mult, opt_tp_mult, live_approved
            FROM strategy_db
            WHERE asset = $1
            """,
            asset,
        )
        if row is None:
            raise HTTPException(status_code=404, detail=f"No strategy_db row for asset '{asset}'.")

        forecast = await conn.fetchrow(
            """
            SELECT id, bullish_prob, bearish_prob
            FROM forecasts
            WHERE asset = $1
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            asset,
        )

    return StrategyParamsOut(
        asset=row["asset"],
        opt_threshold=float(row["opt_threshold"]) if row["opt_threshold"] is not None else 0.60,
        opt_sl_mult=float(row["opt_sl_mult"]) if row["opt_sl_mult"] is not None else 1.5,
        opt_tp_mult=float(row["opt_tp_mult"]) if row["opt_tp_mult"] is not None else 3.0,
        live_approved=bool(row["live_approved"]),
        forecast_id=forecast["id"] if forecast else None,
        bullish_prob=float(forecast["bullish_prob"]) if forecast and forecast["bullish_prob"] is not None else None,
        bearish_prob=float(forecast["bearish_prob"]) if forecast and forecast["bearish_prob"] is not None else None,
    )
