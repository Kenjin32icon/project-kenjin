from fastapi import APIRouter, HTTPException
from startup.db import get_pool
from startup.schemas import HealthOut

router = APIRouter()


@router.get("/health", response_model=HealthOut)
async def health():
    """
    Deliberately NOT behind the API-key check - v10's OnInit() calls this
    to decide whether to allow trading at all, and a monitoring probe or
    Render's own health check should be able to hit it too.

    Returns a real non-200 on DB failure - v10 treats ANY non-200 as
    "orchestrator unhealthy, block entries", so silently returning 200
    with an error message buried in the body would defeat the whole point
    of this check.
    """
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1;")
        return HealthOut(status="ok", db="ok")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"DB check failed: {exc}")
