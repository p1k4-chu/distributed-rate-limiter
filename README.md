# ⚡ Distributed Rate-Limiter Infrastructure

A high-performance, low-latency distributed rate-limiting system designed to safeguard backend microservices from traffic surges, API abuse, and brute-force attacks. The architecture implements an asynchronous core engine communicating over high-speed gRPC transport layers, backed by an atomic Redis memory tier for multi-node consistency.

---

## 🚀 Key Architectural Pillars

- **Asynchronous Execution Framework:** Architected using Python's concurrent `asyncio` engine to efficiently handle thousands of inbound checking routines per second with non-blocking I/O.
- **Dual-Algorithmic Memory Protection:** Implements two isolated, industry-standard traffic-shaping strategies:
  - **Atomic Token Bucket:** Engineered via internal **Redis Lua Scripting** to guarantee 100% thread-safe execution and eliminate distributed race conditions under heavy concurrent request spikes.
  - **Sliding Window Log:** Built utilizing **Redis Sorted Sets (`ZSET`)** to dynamically monitor, evaluate, and invalidate precise timestamp windows per client connection.
- **High-Throughput Network Communication:** Uses structured **gRPC (Protocol Buffers)** instead of standard REST text-serialization, minimizing network latency and bandwidth overhead.
- **Production-Ready Container Isolation:** Fully containerized using multi-stage **Docker** build targets, unified across an automated microservice cluster via orchestration setups.

---

## 🛠️ System Workflow Diagram

```text 
[ Client Request ]
       │
       ▼
┌──────────────────────────────┐
│  FastAPI Reverse-Proxy GW    │
└──────────────┬───────────────┘
               │
               │  (High-Speed gRPC / Protobuf)
               ▼
┌──────────────────────────────┐
│  Async Rate-Limiter Engine   │
└──────────────┬───────────────┘
               │
               │  (Atomic Evaluation / Lua Scripts)
               ▼
┌──────────────────────────────┐
│   Distributed Redis Tier     │
└──────────────────────────────┘
```

# Technical Stack & Ecosystem

> ###  Engine Core
> Python 3.12, Asyncio Framework

> ###  Memory/Scripting Layer
> Redis Enterprise Data Structures, Embedded Lua Scripts

> ###  Communication Layer
> gRPC, Protocol Buffers (proto3)

> ###  API Edge Routing
> FastAPI, Uvicorn Server

> ###  Infrastructure Tooling
> Multi-stage Docker, Docker Compose

---

#  Running the Distributed Rate Limiter 

This guide walks you through launching the complete distributed rate limiter system, generating traffic, and monitoring its behavior in real time.

---

## Step 1: Start the Distributed Infrastructure

Build and launch all services using Docker Compose.

### Services Started

* Redis State Tier
* gRPC Evaluation Engine
* FastAPI Edge Gateway

### Commands

Open a terminal in the project root directory:

```bash
cd distributed-rate-limiter
```

Build and start all containers:

```bash
docker compose up --build
```

### Expected Output

Keep this terminal running. You should see logs indicating:

* Redis has started successfully
* gRPC server is listening on port `50051`
* FastAPI gateway is running on port `8000`
* Internal service connections have been established

---

## Step 2: Launch the Traffic Simulator (Locust)

Use Locust to generate high-volume traffic against the API gateway.

Open a second terminal window and run:

```bash
locust -f load_tests/locustfile.py --host=http://localhost:8000
```

### Prerequisites

Ensure Locust is installed:

```bash
pip install locust
```

### Expected Output

Locust will start a local web interface for configuring load tests.

---

## Step 3: Execute the Load Test

Open your browser and navigate to:

```text
http://localhost:8089
```

### Test Configuration

Configure the following parameters:

| Parameter       | Value                 |
| --------------- | --------------------- |
| Number of Users | 100                   |
| Ramp-Up Rate    | 20 users/sec          |
| Host            | http://localhost:8000 |

### Start Testing

Click the **START** button.

### Monitoring

Navigate between the following tabs:

* Statistics
* Charts
* Failures

You will observe thousands of requests being sent to:

```text
/api/v1/resource
```

This simulates a realistic traffic surge against the distributed rate-limiting infrastructure.

---

## Step 4: Launch the Real-Time Dashboard

Visualize how the rate-limiting algorithms react to incoming traffic.

Open a third terminal window and run:

```bash
python rate_limiter_dashboard/dashboard.py
```

### Prerequisites

Install the Rich library:

```bash
pip install rich
```

### Dashboard Features

The live terminal dashboard displays:

* Request throughput
* Allowed requests
* Rejected requests
* Rate limiter decisions
* Real-time system activity

This provides an interactive view of how the distributed rate limiter behaves under load.

---

#  Workflow Summary

1. Start all services with Docker Compose.
2. Launch Locust traffic generation.
3. Configure and start the load test from the Locust UI.
4. Open the live dashboard.
5. Observe how the rate limiter handles concurrent traffic in real time.

🎉 You now have a complete end-to-end demonstration of the Distributed Rate Limiter system running locally.

