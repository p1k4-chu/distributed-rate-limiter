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
            import uuid
            member_id = f"{now_ms}:{uuid.uuid4().hex[:6]}"

            # Execute all pipeline operations atomically in a single network round-trip
            async with client.pipeline(transaction=True) as pipe:
                # 1. Clean out metrics older than the sliding window threshold
                pipe.zremrangebyscore(redis_key, 0, clear_before_ms)
                # 2. Optimistically log the current request element
                pipe.zadd(redis_key, {member_id: now_ms})
                # 3. Request count of active items in the moving window
                pipe.zcount(redis_key, clear_before_ms + 1, "+inf")
                # 4. Fetch the oldest element's score inside the transaction to prevent multi-threaded drift
                pipe.zrange(redis_key, 0, 0, withscores=True)
                # 5. Reset TTL key timeout rules
                pipe.expire(redis_key, 3600)
                
                # Unpack atomic result sets cleanly
                _, _, current_request_count, oldest_logs, _ = await pipe.execute()

            # Evaluate thresholds using the transaction calculation snapshots
            if current_request_count <= max_tokens:
                allowed = True
                remaining_tokens = max_tokens - current_request_count
            else:
                allowed = False
                remaining_tokens = 0
                # Corrective cleanup can happen immediately; we don't block subsequent requests 
                # because the atomic pipeline transaction was the source of truth for tracking.
                await client.zrem(redis_key, member_id)
                
            # Compute reset time directly from the atomic oldest log record
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