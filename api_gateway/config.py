import os

class Settings:
    # Manages application environment variables and configurations 
    # for API Gateway microservice.

    # Define where the API Gateway itself should look for Limiter Engine
    ENGINE_HOST: str = os.getenv("ENGINE_HOST", "127.0.0.1")
    ENGINE_PORT: int = int(os.getenv("ENGINE_PORT", 50051))
    
    # Gateway local application runtime port
    APP_PORT: int = int(os.getenv("APP_PORT", 8000))

settings = Settings()