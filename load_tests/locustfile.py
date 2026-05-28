# load_tests/locustfile.py
import random
from locust import HttpUser, task, between

class RateLimiterTrafficSimulator(HttpUser):
    """
    Simulates thousands of concurrent distributed users making rapid, 
    high-volume API calls to stress-test our FastAPI Gateway and gRPC Engine.
    """
    
    # Simulate a realistic network delay between requests (e.g., 0.1 to 0.5 seconds of "think time")
    # To test extreme stress, you can reduce this or comment it out entirely.
    wait_time = between(0.1, 0.5)

    @task(3)
    def hit_protected_resource_token_bucket(self):
        """
        Simulates a user pounding the protected endpoint utilizing the Token Bucket algorithm.
        """
        # Pick from a pool of 50 simulated user IDs to ensure some users exhaust their limits quickly
        simulated_user_id = f"locust_tb_{random.randint(1, 50)}"
        
        headers = {
            "X-User-ID": simulated_user_id,
            "X-Limiter-Algo": "token_bucket"
        }
        
        # We catch responses so that expected 429 Too Many Requests errors 
        # are marked as successful test conditions rather than failures in the dashboard.
        with self.client.get("/api/v1/resource", headers=headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 429:
                response.success()  # 429 means our rate limiter is doing its job perfectly!
            else:
                response.failure(f"Unexpected Gateway Error: {response.status_code}")

    @task(1)
    def hit_protected_resource_sliding_window(self):
        """
        Simulates a user hammering the protected endpoint utilizing the Sliding Window algorithm.
        """
        simulated_user_id = f"locust_sw_{random.randint(1, 50)}"
        
        headers = {
            "X-User-ID": simulated_user_id,
            "X-Limiter-Algo": "sliding_window"
        }
        
        with self.client.get("/api/v1/resource", headers=headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 429:
                response.success()
            else:
                response.failure(f"Unexpected Gateway Error: {response.status_code}")

    @task(1)
    def hit_unprotected_root(self):
        """
        Simulates basic health check calls to verify that unmonitored routes 
        bypass the gRPC tunnel completely and stay instantly available.
        """
        self.client.get("/")