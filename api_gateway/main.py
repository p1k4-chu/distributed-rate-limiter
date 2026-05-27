# api_gateway/main.py
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, HTTPException, status, Header, Depends
from fastapi.responses import JSONResponse

# Import our new client engine and global microservice settings
from grpc_client import RateLimiterClient
from config import settings

# Setup standard structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_gateway.main")

# ---------------------------------------------------------
# MICROSERVICE LIFECYCLE MANAGEMENT (Lifespan Context)
# ---------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    
    # Manages the operational readiness lifecycle of the microservice layer.
    # Guarantees long-lived gRPC tunnels boot and shut down atomically.
    
    # Initialize our production gRPC client targeting configuration port
    global limiter_client
    limiter_client = RateLimiterClient(host=settings.ENGINE_HOST, port=settings.ENGINE_PORT)
    
    # Trigger connection pool startup sequence
    await limiter_client.start()
    logger.info("Successfully bound live asynchronous gRPC tunnel to Limiter Engine.")
    
    yield # Execution checkpoint where FastAPI starts accepting public HTTP web requests
    
    # Trigger teardown sequence when worker processes receive termination signals
    await limiter_client.close()
    logger.info("Gateway client connections closed completely.")

# Instantiating FastAPI App utilizing unified lifecycle tracking
app = FastAPI(
    title="Distributed Rate Limiter Gateway",
    description="High-performance edge proxy gateway running over async gRPC.",
    version="1.0.0",
    lifespan=lifespan
)

# ---------------------------------------------------------
# GLOBAL MIDDLEWARE INTERCEPTOR (Slightly altered to reference global client safely)
# ---------------------------------------------------------
@app.middleware("http")
async def rate_limiter_middleware(request: Request, call_next):
    if request.url.path in ["/", "/docs", "/openapi.json"]:
        return await call_next(request)
        
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Missing required security header: X-User-ID"}
        )
        
    rate_limit_key = f"user:{user_id}"
    
    try:
        # Check limit against live gRPC proxy engine client wrapper
        verdict = await limiter_client.check_limit(
            key=rate_limit_key,
            max_tokens=100,
            refill_rate=2.0,
            algorithm="token_bucket"
        )
        
        if not verdict["allowed"]:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Too Many Requests. Rate limit exceeded."},
                headers={"Retry-After": "30"}
            )
            
        response: Response = await call_next(request)
        response.headers["X-RateLimit-Remaining"] = str(verdict["remaining_tokens"])
        response.headers["X-RateLimit-Reset"] = str(verdict["reset_time_seconds"])
        
        return response

    except Exception as e:
        logger.warning(f"Middleware Engine Fallback Warning: {e}")
        return await call_next(request)

# ---------------------------------------------------------
# CORE APIs
# ---------------------------------------------------------
async def verify_security_headers(x_user_id: str = Header(..., description="Unique user identifier")):
    return x_user_id

@app.get("/")
async def root():
    return {"status": "healthy", "service": "api_gateway"}

@app.get("/api/v1/resource", dependencies=[Depends(verify_security_headers)])
async def protected_resource():
    return {"message": "Success! You accessed the protected resource cleanly."}