import os
import time
from limiter_engine.algorithms.base_limiter import BaseRateLimiter
from limiter_engine.redis_client import redis_manager

class TokenBucketLimiter(BaseRateLimiter):
    def __init__(self):
        self.lua_sha = None
        self.script_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "storage", "lua_scripts", "token_bucket.lua"
        )

    async def _load_lua_script(self, redis_client):
        """Reads the Lua file and caches it into Redis memory for extreme speed."""
        if self.lua_sha is None:
            with open(self.script_path, "r") as file:
                lua_code = file.read()
            # Register the script with Redis; it returns a unique SHA hash
            self.lua_sha = await redis_client.script_load(lua_code)
            print(f"Token Bucket Lua Script successfully cached. SHA: {self.lua_sha}")

    async def is_allowed(self, key: str, max_tokens: int, refill_rate: float) -> tuple[bool, int, int]:
        """Executes the cached Lua script atomically over the Redis instance."""
        # Get an active client from our global connection pool
        client = redis_manager.get_client()
        
        # Ensure the Lua script is loaded into Redis memory
        await self._load_lua_script(client)
        
        # Current Unix timestamp in seconds
        current_time = int(time.time())
        # Cost per request (1 API call equals 1 token)
        token_cost = 1 
        
        # Format the unique storage key name inside the Redis database
        redis_key = f"ratelimit:token_bucket:{key}"

        try:
            # Run the script using its cached SHA hash instead of passing the whole file text
            result = await client.evalsha(
                self.lua_sha, 
                1,              # Number of keys being sent
                redis_key,      # KEYS[1]
                max_tokens,    # ARGV[1]
                refill_rate,   # ARGV[2]
                current_time,  # ARGV[3]
                token_cost     # ARGV[4]
            )
            
            # Unpack the array returned by Lua: [allowed_int, remaining, reset_time]
            allowed_int, remaining, reset_time = result
            return bool(allowed_int), remaining, reset_time
            
        except Exception as e:
            print(f"Fallback triggered. Error executing Lua script: {e}")
            # Fault tolerance: if Redis fails, fail-safe open so the app doesn't crash for users
            return True, max_tokens, current_time