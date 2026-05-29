from abc import ABC, abstractmethod

class BaseRateLimiter(ABC):
    """
    Abstract Base Class that serves as the blueprint for all rate-limiting 
    algorithms (Token Bucket, Sliding Window Log, etc.).
    """

    @abstractmethod
    async def is_allowed(self, key: str, max_tokens: int, refill_rate: float) -> tuple[bool, int, int]:
        """
        Evaluates if an incoming request should be allowed or rate-limited.

        Args:
            key: The unique identifier (e.g., "user:101" or "ip:192.168.1.1").
            max_tokens: The maximum capacity of the bucket/window.
            refill_rate: How many tokens are added back per second.

        Returns:
            A tuple of (allowed: bool, remaining_tokens: int, reset_time_seconds: int)
        """
        pass