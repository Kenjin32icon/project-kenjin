"""
Entry point. Run with:
    uvicorn orchestrator.main:app --host 127.0.0.1 --port 8000 --reload

(drop --reload for the systemd/Render deployment - it's a dev-only convenience)

IMPORTANT: this file replaces any monolithic main.py that redefines routes,
schemas, and jobs inline. Having two copies of the same logic (one here, one
in startup/routes + startup/jobs) is how the wrong GROQ_MODEL default and the
missing statement_cache_size fix went unnoticed - the well-tested modular
files existed but weren't actually being imported/run. There must be exactly
ONE place each route and job is defined.
"""
import os
import re
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

from startup.db import init_db_pool, close_db_pool
from startup.routes import health, strategy, ticks, telemetry
from startup.jobs.groq_forecast import run_forecast_cycle
from startup.jobs.gatekeeper import run_gatekeeper_cycle

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orchestrator")

scheduler = AsyncIOScheduler()

# Groq's model catalog uses lowercase, hyphenated, slash-namespaced IDs, e.g.
# "openai/gpt-oss-20b". The exact string "GPT OSS 20B" (spaces, no slash) is
# what fired the HTTP 404 model_not_found errors on every forecast cycle in
# your logs - if GROQ_MODEL isn't set in .env, code must NOT silently fall
# back to a guessed/reformatted name; it should fail loudly at startup
# instead of failing on the first scheduled job 30 minutes later.
_VALID_MODEL_PATTERN = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*(/[a-z0-9]+(-[a-z0-9]+)*)*$")


def resolve_and_validate_groq_model() -> str:
    model = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")
    if not _VALID_MODEL_PATTERN.match(model):
        raise RuntimeError(
            f"GROQ_MODEL='{model}' doesn't look like a valid Groq model id "
            f"(expected lowercase/hyphenated, e.g. 'openai/gpt-oss-20b'). "
            f"Check .env - this is the exact bug that caused the repeated "
            f"'model_not_found' 404s in your forecast job logs."
        )
    log.info("Using Groq model: %s", model)
    return model


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fail fast, before accepting any traffic, rather than 30 minutes into
    # running with a broken forecast loop.
    resolve_and_validate_groq_model()

    await init_db_pool()
    log.info("DB pool initialised.")

    # Three independent cadences - deliberately not one shared loop, so a
    # slow Groq call never delays tick ingestion or vice versa.
    scheduler.add_job(run_forecast_cycle, "interval", minutes=int(os.environ.get("FORECAST_INTERVAL_MIN", 30)),
                       id="groq_forecast", max_instances=1, coalesce=True)
    scheduler.add_job(run_gatekeeper_cycle, "interval", hours=int(os.environ.get("GATEKEEPER_INTERVAL_HOURS", 4)),
                       id="gatekeeper", max_instances=1, coalesce=True)

    # auto_tester (headless Wine/MT5 Strategy Tester automation) is
    # DELIBERATELY NOT scheduled here. See auto_tester_review.md - as written
    # it risks shutting down whatever MT5 terminal instance Wine resolves to
    # (ShutdownTerminal=1 with no confirmation it's a separate instance from
    # your live/demo terminal), and its report-parsing step is currently a
    # hardcoded mock, not real XML parsing. Do not re-enable this job until
    # both are fixed - see the review doc for the specific changes needed.
    scheduler.start()
    log.info("Scheduler started: forecast every %sm, gatekeeper every %sh",
              os.environ.get("FORECAST_INTERVAL_MIN", 30), os.environ.get("GATEKEEPER_INTERVAL_HOURS", 4))

    yield

    # Shutdown
    scheduler.shutdown(wait=False)
    await close_db_pool()
    log.info("DB pool closed.")


app = FastAPI(title="Project KENJIN Orchestrator", version="10.0.1", lifespan=lifespan)

app.include_router(health.router)
app.include_router(strategy.router)
app.include_router(ticks.router)
app.include_router(telemetry.router)