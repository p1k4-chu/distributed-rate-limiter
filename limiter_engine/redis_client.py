import redis.asyncio as aioredis
import os

class RedisClientManager:
    def __init__(self):
        # Read Redis location from system environment variables (this is critical for Docker later)
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.pool = None

    async def initialize_pool(self):
        """Initializes a non-blocking asynchronous Redis connection pool."""
        if not self.pool:
            self.pool = aioredis.ConnectionPool(
                host=self.redis_host, 
                port=self.redis_port, 
                decode_responses=True # Automatically converts database bytes to clean strings
            )
            print(f"Async Redis Connection Pool initialized at {self.redis_host}:{self.redis_port}")

    def get_client(self) -> aioredis.Redis:
        """Returns an active async Redis client instance from the pool."""
        if not self.pool:
            raise RuntimeError("Redis connection pool has not been initialized. Call initialize_pool() first.")
        return aioredis.Redis(connection_pool=self.pool)

    async def close_pool(self):
        """Gracefully disconnects and shuts down the connection pool when the server stops."""
        if self.pool:
            await self.pool.disconnect()
            print("Redis Connection Pool safely disconnected.")

# Create a single global instance to be imported across all your rate limiter algorithms
redis_manager = RedisClientManager()