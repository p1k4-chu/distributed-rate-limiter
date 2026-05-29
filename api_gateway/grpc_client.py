import grpc
import logging
import time
from typing import Dict, Any

# Import our compiled Protobuf stubs
import rate_limiter_pb2
import rate_limiter_pb2_grpc

logger = logging.getLogger("api_gateway.grpc_client")

class RateLimiterClient:

    # Production-grade asynchronous gRPC client wrapper that communicates
    # directly with stateful rate limiting microservice engine.

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.target = f"{host}:{port}"
        self._channel: grpc.aio.Channel | None = None
        self._stub: rate_limiter_pb2_grpc.RateLimiterStub | None = None

    async def start(self) -> None:
        
        # Initializes a long-lived, high-performance asynchronous HTTP/2 multiplexed
        # gRPC channel pool. Called during FastAPI application startup.
        
        if not self._channel:
            logger.info(f"Initializing asynchronous gRPC channel to Limiter Engine at {self.target}")
            # We open an insecure async channel pool optimized for microservice mesh networks
            self._channel = grpc.aio.insecure_channel(
                self.target,
                options=[
                    ('grpc.max_receive_message_length', 4 * 1024 * 1024), # 4MB limit
                    ('grpc.max_send_message_length', 4 * 1024 * 1024),
                    ('grpc.keepalive_time_ms', 30000),                    # Keep connections alive
                    ('grpc.keepalive_timeout_ms', 5000),
                    ('grpc.dns_enable_srv_queries', 0),                  # Disable intensive SRV lookup
                ]
            )
            self._stub = rate_limiter_pb2_grpc.RateLimiterStub(self._channel)

    async def close(self) -> None:
        
        # Gracefully tears down active channel pools during application shutdown sequence.
        
        if self._channel:
            logger.info("Closing active gRPC microservice channel loops smoothly...")
            await self._channel.close()
            self._channel = None
            self._stub = None

    async def check_limit(self, key: str, max_tokens: int, refill_rate: float, algorithm: str) -> Dict[str, Any]:
        
        # Dials engine over an active gRPC tunnel to evaluate rate-limit allowance.
        # Implements an automatic Fallback Fail-Open strategy if network connections time out.
        
        # Architectural Safetynet: If channel hasn't been explicitly started, attempt emergency boot
        if not self._stub:
            await self.start()
            
        # Marshall internal python variables into a binary-serialized Protobuf message
        request_payload = rate_limiter_pb2.RateLimitRequest(
            key=key,
            max_tokens=max_tokens,
            refill_rate=refill_rate,
            algorithm=algorithm
        )

        try:
            # Fire multiplexed async request across our HTTP/2 tunnel with a strict 250ms deadline
            response = await self._stub.CheckLimit(request_payload, timeout=0.250)
            
            # Return normalized Python dictionary payload to our calling ASGI middleware layer
            return {
                "allowed": response.allowed,
                "remaining_tokens": response.remaining_tokens,
                "reset_time_seconds": response.reset_time_seconds
            }

        except grpc.RpcError as rpc_err:
            logger.error(
                f"CRITICAL: Rate Limiter gRPC microservice communication link broken! "
                f"Code: {rpc_err.code()}, Details: {rpc_err.details()}. Executing Fail-Open procedure."
            )
            
            # FIX: If the channel is dead or unavailable, dismantle the pointers
            # so the next incoming request forces a clean, fresh channel rebuild pool.
            if rpc_err.code() in [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]:
                self._channel = None
                self._stub = None
            
            return {
                "allowed": True,
                "remaining_tokens": max_tokens,
                "reset_time_seconds": int(time.time())
            }