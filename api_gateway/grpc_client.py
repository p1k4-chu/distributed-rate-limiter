import time
import asyncio

class MockRateLimiterClient:
    """
    A temporary mock client that simulates high-performance gRPC communication 
    with limiter engine without requiring the engine to be running.
    """
    async def check_limit(self, key: str, max_tokens: int, refill_rate: float, algorithm: str):
        # Simulate a tiny 1ms gRPC asynchronous network round-trip overhead
        await asyncio.sleep(0.001)
        
        # Always return an allowed mock response structured exactly like our Protobuf contract
        return {
            "allowed": True,
            "remaining_tokens": max_tokens - 1,
            "reset_time_seconds": int(time.time()) + 60
        }