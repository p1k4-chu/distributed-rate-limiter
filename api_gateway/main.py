import time
import grpc
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse

# Import our client engine, global configuration settings, and routers
from grpc_client import RateLimiterClient
from config import settings
from routers import protected_routes  # <-- Import your new modular router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_gateway.main")

# ---------------------------------------------------------
# MICROSERVICE LIFECYCLE MANAGEMENT (Lifespan Context)
# ---------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global limiter_client
    limiter_client = RateLimiterClient(host=settings.ENGINE_HOST, port=settings.ENGINE_PORT)
    await limiter_client.start()
    logger.info("Successfully bound live asynchronous gRPC tunnel to Limiter Engine.")
    
    yield
    
    await limiter_client.close()
    logger.info("Gateway client connections closed completely.")

# Instantiating FastAPI App
app = FastAPI(
    title="Distributed Rate Limiter Gateway",
    description="High-performance edge proxy gateway running over async gRPC.",
    version="1.0.0",
    lifespan=lifespan
)

# Register our sub-router modules dynamically onto the app instance
app.include_router(protected_routes.router)  # <-- Mounts /api/v1/resource cleanly

# ---------------------------------------------------------
# GLOBAL MIDDLEWARE INTERCEPTOR
# ---------------------------------------------------------
@app.middleware("http")
async def rate_limiter_middleware(request: Request, call_next):
    # Pass basic check routes cleanly without querying Redis
    if request.url.path in ["/", "/docs", "/openapi.json"]:
        return await call_next(request)
        
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Missing required security header: X-User-ID"}
        )
        
    rate_limit_key = f"user:{user_id}"
    chosen_algo = request.headers.get("X-Limiter-Algo", "token_bucket").lower()
    if chosen_algo not in ["token_bucket", "sliding_window"]:
        chosen_algo = "token_bucket"
    
    try:
        verdict = await limiter_client.check_limit(
            key=rate_limit_key,
            max_tokens=100,
            refill_rate=2.0,
            algorithm=chosen_algo
        )
        
        if not verdict["allowed"]:
            # FIX: Dynamically calculate the standard cooldown delta instead of using a hardcoded "30"
            current_time = int(time.time())
            cooldown_seconds = max(0, verdict["reset_time_seconds"] - current_time)
            if cooldown_seconds == 0:
                cooldown_seconds = 1  # Fallback minimum safe ceiling
                
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": f"Too Many Requests. Rate limit exceeded via {chosen_algo}."},
                headers={"Retry-After": str(cooldown_seconds)}
            )
            
        response: Response = await call_next(request)
        response.headers["X-RateLimit-Remaining"] = str(verdict["remaining_tokens"])
        response.headers["X-RateLimit-Reset"] = str(verdict["reset_time_seconds"])
        response.headers["X-RateLimit-Algo"] = chosen_algo
        
        return response

    except (grpc.RpcError, RuntimeError) as network_err:
        # FIX: Catch precise network faults to handle fail-open safely without masking syntax/runtime bugs
        logger.warning(f"Middleware Engine Network Fallback Triggered: {network_err}")
        return await call_next(request)

# Basic health status API kept directly in main
@app.get("/")
async def root():
    return {"status": "healthy", "service": "api_gateway"}