import os
import time
from algorithms.base_limiter import BaseRateLimiter
from redis_client import redis_manager

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
        import typing
        client = redis_manager.get_client()
        await self._load_lua_script(client)
        
        current_time = int(time.time())
        token_cost = 1 
        redis_key = f"ratelimit:token_bucket:{key}"

        try:
            # Run the script and cast it to a type Pylance understands
            result = await client.evalsha(self.lua_sha, 1, redis_key, max_tokens, refill_rate, current_time, token_cost)  # type: ignore
            typed_result = typing.cast(typing.List[int], result)
            
            allowed_int = typed_result[0]
            remaining = typed_result[1]
            reset_time = typed_result[2]
            
            return bool(allowed_int), remaining, reset_time
            
        except Exception as e:
            print(f"Fallback triggered. Error executing Lua script: {e}")
            return True, max_tokens, current_time