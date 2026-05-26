from fastapi import FastAPI, Header, HTTPException, status
from grpc_client import MockRateLimiterClient

app = FastAPI(
    title="Distributed Rate Limiter Gateway",
    description="High-performance edge proxy gateway running over async gRPC.",
    version="1.0.0"
)

# Initialize client wrapper
limiter_client = MockRateLimiterClient()

@app.get("/")
async def root():
    """Basic health check endpoint."""
    return {"status": "healthy", "service": "api_gateway"}

@app.get("/api/v1/resource")
async def protected_resource(x_user_id: str = Header(..., alias="X-User-ID")):
    """
    A mock protected API route representing a high-value endpoint 
    (like a ticket checkout, login, or public resource).
    """
    # 1. We extract the client's unique key from the headers
    rate_limit_key = f"user:{x_user_id}"
    
    # 2. Forward the request details to our evaluation engine layer
    # For now, passing standard limits: max 100 requests bursting, refilling at 2/sec
    verdict = await limiter_client.check_limit(
        key=rate_limit_key,
        max_tokens=100,
        refill_rate=2.0,
        algorithm="token_bucket"
    )
    
    # 3. Handle the rate limiting evaluation action
    if not verdict["allowed"]:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too Many Requests. Rate limit exceeded.",
            headers={"Retry-After": "30"}
        )
        
    # 4. If allowed, pass request cleanly to our internal resource
    return {
        "message": "Success! You accessed the protected resource cleanly.",
        "rate_limit_status": {
            "remaining": verdict["remaining_tokens"],
            "reset_at": verdict["reset_time_seconds"]
        }
    }