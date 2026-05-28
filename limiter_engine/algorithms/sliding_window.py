import time
from algorithms.base_limiter import BaseRateLimiter
from redis_client import redis_manager

class SlidingWindowLimiter(BaseRateLimiter):
    async def is_allowed(self, key: str, max_tokens: int, refill_rate: float) -> tuple[bool, int, int]:
        """
        Implements a sliding window log rate limiter using Redis Sorted Sets (ZSET).
        
        Note: For sliding window, 'refill_rate' defines the window size in seconds 
        implied by the system design (e.g., if window is 60 seconds, max_tokens is capacity).
        """
        client = redis_manager.get_client()
        
        # 1. FIX: Standardize timeline to clean millisecond-scale integer values
        now_seconds = time.time()
        now_ms = int(now_seconds * 1000)
        
        window_size_ms = int(refill_rate * 1000)
        clear_before_ms = now_ms - window_size_ms
        
        redis_key = f"ratelimit:sliding_window:{key}"
        
        try:
            # 2. Pipeline transaction blocks to evaluate elements cleanly
            async with client.pipeline(transaction=True) as pipe:
                pipe.zremrangebyscore(redis_key, 0, clear_before_ms)
                pipe.zcard(redis_key)
                _, current_request_count = await pipe.execute()
            
            # 3. Evaluate allowance thresholds
            if current_request_count < max_tokens:
                allowed = True
                remaining_tokens = max_tokens - (current_request_count + 1)
                
                # FIX: Bundle write execution using an explicit atomic pipeline transaction block
                async with client.pipeline(transaction=True) as write_pipe:
                    write_pipe.zadd(redis_key, {str(now_ms): now_ms})
                    write_pipe.expire(redis_key, 3600)  
                    await write_pipe.execute()
            else:
                allowed = False
                remaining_tokens = 0
                
            # 4. FIX: Decode the oldest entry's millisecond score and cast safely back to precise seconds
            oldest_logs = await client.zrange(redis_key, 0, 0, withscores=True)
            if oldest_logs:
                oldest_timestamp_ms = oldest_logs[0][1]
                reset_time_seconds = int((oldest_timestamp_ms + window_size_ms) / 1000)
            else:
                reset_time_seconds = int(now_seconds) + int(refill_rate)

            return allowed, remaining_tokens, reset_time_seconds
            
        except Exception as e:
            print(f"Sliding Window fallback triggered. Error: {e}")
            # Fail-open safety default
            return True, max_tokens, int(now_seconds)