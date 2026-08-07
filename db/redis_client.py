import redis.asyncio as redis
import os

# Connect to Redis (assuming local or Docker network)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create a global connection pool
redis_pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)
redis_client = redis.Redis(connection_pool=redis_pool)

async def get_redis():
    """Dependency injection for FastAPI routes."""
    return redis_client