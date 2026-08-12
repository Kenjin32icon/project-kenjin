import asyncio
import logging
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv

# Load DATABASE_URL and other configuration from .env
load_dotenv()

from startup.db import init_db_pool, close_db_pool
from startup.jobs.gatekeeper import tune_rsi_veto_from_analytics

logging.basicConfig(level=logging.INFO)

async def run_test():
    await init_db_pool()
    await tune_rsi_veto_from_analytics()
    await close_db_pool()

if __name__ == "__main__":
    asyncio.run(run_test())