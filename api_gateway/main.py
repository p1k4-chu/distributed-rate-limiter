import time
from fastapi import FastAPI, Request, Response, HTTPException, status, Header, Depends
from fastapi.responses import JSONResponse
from grpc_client import MockRateLimiterClient
from config import settings

print(f"Gateway configured to route gRPC calls to -> {settings.ENGINE_HOST}:{settings.ENGINE_PORT}")
app = FastAPI(
    title="Distributed Rate Limiter Gateway",
    description="High-performance edge proxy gateway running over async gRPC.",
    version="1.0.0"
)

# Initialize client wrapper
limiter_client = MockRateLimiterClient()

# ---------------------------------------------------------
# GLOBAL MIDDLEWARE INTERCEPTOR
# ---------------------------------------------------------
@app.middleware("http")
async def rate_limiter_middleware(request: Request, call_next):

    # Global interceptor middleware that intercepts all incoming HTTP requests,
    # extracts client identifiers, and verifies compliance against the rate limiter.

    # Skip rate limiting for the basic health check root path
    if request.url.path in ["/", "/docs", "/openapi.json"]:
        return await call_next(request)
        
    # Extract client identifier from custom headers
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Missing required security header: X-User-ID"}
        )
        
    rate_limit_key = f"user:{user_id}"
    
    try:
        # Communicate asynchronously with the rate limiting engine layer
        verdict = await limiter_client.check_limit(
            key=rate_limit_key,
            max_tokens=100,
            refill_rate=2.0,
            algorithm="token_bucket"
        )
        
        # Handle Rate Limited Short-Circuiting Action
        if not verdict["allowed"]:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Too Many Requests. Rate limit exceeded."},
                headers={"Retry-After": "30"}
            )
            
        # If allowed, execute the underlying API route
        response: Response = await call_next(request)
        
        # Inject updated rate limit capacity metrics directly into response headers
        response.headers["X-RateLimit-Remaining"] = str(verdict["remaining_tokens"])
        response.headers["X-RateLimit-Reset"] = str(verdict["reset_time_seconds"])
        
        return response

    except Exception as e:
        # Fallback security handling: Default-Allow if middleware fails internally
        print(f"Middleware Engine Fallback Warning: {e}")
        return await call_next(request)

async def verify_security_headers(x_user_id: str = Header(..., description="Unique user or client identifier")):
    """Helper dependency to force Swagger UI to render the input field box."""
    return x_user_id

# ---------------------------------------------------------
# CLEAN CLEANED ROUTES (No rate limiting code mixed in!)
# ---------------------------------------------------------
@app.get("/")
async def root():
    """Basic health check endpoint."""
    return {"status": "healthy", "service": "api_gateway"}

@app.get("/api/v1/resource", dependencies=[Depends(verify_security_headers)])
async def protected_resource():
    """A high-value endpoint protected transparently by global middleware."""
    return {"message": "Success! You accessed the protected resource cleanly."}

@app.get("/api/v1/other-data", dependencies=[Depends(verify_security_headers)])
async def another_protected_resource():
    """A brand new endpoint that is automatically rate-limited with zero extra code!"""
    return {"message": "Success! This path is also globally protected."}