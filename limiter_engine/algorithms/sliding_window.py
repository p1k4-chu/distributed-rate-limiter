import time
from limiter_engine.algorithms.base_limiter import BaseRateLimiter
from limiter_engine.redis_client import redis_manager

class SlidingWindowLimiter(BaseRateLimiter):
    async def is_allowed(self, key: str, max_tokens: int, refill_rate: float) -> tuple[bool, int, int]:
        """
        Implements a sliding window log rate limiter using Redis Sorted Sets (ZSET).
        
        Note: For sliding window, 'refill_rate' defines the window size in seconds 
        implied by the system design (e.g., if window is 60 seconds, max_tokens is capacity).
        """
        client = redis_manager.get_client()
        
        # 1. Setup timestamps and window configurations
        now = time.time()                    # Current precise time with milliseconds
        now_ms = int(now * 1000)             # Convert to milliseconds for unique scores
        window_size_seconds = int(refill_rate) # Time window duration (e.g., 60s)
        clear_before_ts = now - window_size_seconds # Calculate the rolling expiration boundary
        
        redis_key = f"ratelimit:sliding_window:{key}"
        
        try:
            # 2. Use a Redis Pipeline to bundle commands together atomically
            async with client.pipeline(transaction=True) as pipe:
                # Remove all request entries older than our rolling window boundary
                pipe.zremrangebyscore(redis_key, 0, clear_before_ts)
                
                # Fetch the total number of remaining logs inside our current window
                pipe.zcard(redis_key)
                
                # Execute the bundled commands up to this point
                _, current_request_count = await pipe.execute()
            
            # 3. Evaluate if the client has broken the limit threshold
            if current_request_count < max_tokens:
                # Client is allowed through! Add their new request timestamp to the log
                # We use now_ms as a unique value so multiple calls don't overwrite each other
                await client.zadd(redis_key, {str(now_ms): now})
                # Set a 1-hour expiration on the key so old logs automatically drop out of memory
                await client.expire(redis_key, 3600)
                
                allowed = True
                remaining_tokens = max_tokens - (current_request_count + 1)
            else:
                # Client is rate-limited!
                allowed = False
                remaining_tokens = 0
                
            # 4. Calculate when the oldest request drops out of the window to find the reset time
            oldest_logs = await client.zrange(redis_key, 0, 0, withscores=True)
            if oldest_logs:
                oldest_timestamp = oldest_logs[0][1]
                reset_time_seconds = int(oldest_timestamp + window_size_seconds)
            else:
                reset_time_seconds = int(now) + window_size_seconds
                
            return allowed, remaining_tokens, reset_time_seconds
            
        except Exception as e:
            print(f"Sliding Window fallback triggered. Error: {e}")
            # Fail-open safety default
            return True, max_tokens, int(now)