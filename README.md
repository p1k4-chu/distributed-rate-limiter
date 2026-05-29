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

# Technical Stack & Ecosystem

## Engine Core

Python 3.12, Asyncio Framework

## Memory/Scripting Layer

Redis Enterprise Data Structures, Embedded Lua Scripts

## Communication Layer

gRPC, Protocol Buffers (proto3)

## API Edge Routing

FastAPI, Uvicorn Server

## Infrastructure Tooling

Multi-stage Docker, Docker Compose

---

# Step-by-Step Deployment & Testing Guide

Follow these exact steps to launch the system cluster and verify that the rate-limiter is actively dropping traffic.

---

## Step 1: Deploy the Project Layout using Docker

Open your terminal inside the main project directory and run this single command to build and boot up the API Gateway, your core Engine, and the Redis instance simultaneously:

#### Bash

```bash id="u9g1a2"
docker compose up --build
```

(Leave this terminal running so you can monitor the live container logging structures).

---

## Step 2: Verify the API Gateway is Running Cleanly

Open a second, completely new terminal window in VS Code and run this command to check if the public entrance channel is active:

#### Bash

```bash id="n8k2p4"
curl -X GET http://localhost:8000/
```

### Expected Response:

#### JSON

```json id="k4m8s1"
{"status": "healthy", "message": "API Gateway is active"}
```

---

## Step 3: Test and Verify the Rate Limiter (The Evaluation)

To simulate a user sending rapid requests and trigger a rate-limit block, copy and paste this command into your terminal and press Enter multiple times very quickly:

#### Bash

```bash id="f2x7q9"
curl -H "X-User-ID: user_123" http://localhost:8000/api/v1/protected
```

### What you will see in the terminal:

#### First 5 Requests (Allowed):

The engine confirms tokens are available in Redis and returns:

```http id="z1v6r3"
HTTP 200 OK -> {"status": "success", "message": "Request processed successfully"}
```

---

#### 6th Request Onward (Blocked & Throttled):

Once you hit the limit inside the time window, your core engine drops the traffic instantly to protect the backend:

```http id="p5t9w2"
HTTP 429 Too Many Requests -> {"error": "Rate limit exceeded. Please try again later."}
```
