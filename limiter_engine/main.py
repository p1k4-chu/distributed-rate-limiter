import asyncio
import grpc
from concurrent import futures
from typing import Any
# Import the auto-generated gRPC stubs that we compiled earlier
from limiter_engine.protobuf import rate_limiter_pb2
from limiter_engine.protobuf import rate_limiter_pb2_grpc

# Import your connection manager and algorithm classes
from limiter_engine.redis_client import redis_manager
from limiter_engine.algorithms.token_bucket import TokenBucketLimiter
from limiter_engine.algorithms.sliding_window import SlidingWindowLimiter

class RateLimiterServicer(rate_limiter_pb2_grpc.RateLimiterServicer):
    """
    Implements the gRPC network interface defined in Sameer's rate_limiter.proto file.
    """
    def __init__(self):
        # Instantiate your algorithm engines
        self.limiters = {
            "token_bucket": TokenBucketLimiter(),
            "sliding_window": SlidingWindowLimiter()
        }
    async def CheckLimit(
        self, 
        request: Any, 
        context: Any
    ) -> Any:
        """
        Invoked over the network by Sameer's C++ Gateway every time an API call hits the system.
        """
        # Unpack the request payload parameters using the exact variables Sameer defined
        key = request.key
        max_tokens = request.max_tokens
        refill_rate = request.refill_rate
        algorithm = request.algorithm.lower()

        # Route the request to the correct algorithm handler dynamically
        limiter = self.limiters.get(algorithm)
        
        if not limiter:
            # Fault tolerance fallback: default to token_bucket if Sameer sends an unknown string
            limiter = self.limiters["token_bucket"]

        # Run the evaluation logic asynchronously over Redis
        allowed, remaining, reset_time = await limiter.is_allowed(key, max_tokens, refill_rate)

        # Pack the calculation output into Sameer's exact gRPC response format
        return rate_limiter_pb2.RateLimitResponse(  # type: ignore
            allowed=allowed,
            remaining_tokens=remaining,
            reset_time_seconds=reset_time
        )

async def serve():
    """Boots up the asynchronous gRPC network server."""
    # 1. Initialize our high-performance global Redis connection pool
    await redis_manager.initialize_pool()

    # 2. Create the async gRPC server instance
    server = grpc.aio.server()
    
    # 3. Register our implementation service layer onto the network server
    rate_limiter_pb2_grpc.add_RateLimiterServicer_to_server(RateLimiterServicer(), server)
    
    # 4. Bind the server to listen on port 50051 (Standard default gRPC port)
    # Using [::] allows the server to accept connections from inside a Docker network later!
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    
    print(f"Gagan's Rate Limiter Engine Server is live and listening on {listen_addr}...")
    await server.start()
    
    try:
        # Keep the server running continuously in the background
        await server.wait_for_termination()
    finally:
        # Ensure database channels close gracefully if the process is killed
        await redis_manager.close_pool()

if __name__ == "__main__":
    # Boot the async main event loop execution environment
    asyncio.run(serve())