from fastapi import APIRouter, Header, Depends

# Initialize an isolated router instance specifically for protected resources
router = APIRouter(
    prefix="/api/v1",
    tags=["Protected Resources"]
)

# Shared security dependency reusable across multiple endpoints in this module
async def verify_security_headers(x_user_id: str = Header(..., description="Unique user identifier")):
    return x_user_id

@router.get("/resource", dependencies=[Depends(verify_security_headers)])
async def protected_resource():
    """
    Mock enterprise resource target.
    This route is heavily protected by our global rate limiting middleware layer.
    """
    return {
        "status": "success",
        "message": "Success! You bypassed the gRPC rate limiter and accessed the protected resource cleanly."
    }