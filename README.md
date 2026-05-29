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