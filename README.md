# High-Concurrency Distributed Task Queue & Rate-Limited API Gateway

A production-grade, containerized, fault-tolerant distributed task execution platform built with **Python 3.11+**, **FastAPI**, **gRPC**, **Redis 7**, and **PostgreSQL 16**.

Features an atomic Redis Lua token-bucket rate limiter, consistent-hash worker routing with virtual nodes, strict priority scheduling, exponential backoff retries with jitter, heartbeat-based crash recovery, Dead-Letter Queue (DLQ) replay, and comprehensive Prometheus observability.

---

## Architecture Overview

```text
  [ Clients / wrk / Locust ]
             │
             ▼  HTTP (X-API-Key)
    ┌───────────────────────────────────────────────┐
    │  API Gateway (FastAPI / Uvicorn)              │
    │  - Atomic Token-Bucket Limiter (Lua Script)   │
    │  - Pydantic v2 Payload Validator (64 KB Max)  │
    │  - Idempotency Deduplication                  │
    └──────┬─────────────────────────────────┬──────┘
           │                                 │
           ▼                                 ▼
   PostgreSQL 16 (Durable)            Redis 7 (Ephemeral)
   - jobs table                       - ratelimit:{client_id}
   - job_attempts table               - queue:ready (Sorted Set)
                                      - queue:delayed (Sorted Set)
                                      - queue:dlq (List)
                                      - worker:heartbeat:{id} (TTL 10s)
                                      - workers:active (Hash)
                                      - worker:inflight:{id} (Set)
                                             ▲
                                             │
    ┌────────────────────────────────────────┴──────┐
    │  Consistent-Hash Dispatcher                   │
    │  - 128-bit MD5 Ring (100 vnodes/node)         │
    │  - Strict Priority Poller (ZPOPMIN)           │
    │  - Watchdog (Heartbeat Monitor & Eviction)    │
    │  - Retry Promoter (Delayed Queue -> Ready)    │
    └──────────────────────┬────────────────────────┘
                           │
                           ▼  HTTP/2 gRPC (ExecuteTask)
    ┌───────────────────────────────────────────────┐
    │  Stateless gRPC Worker Pool (Scalable N)      │
    │  - Bounded Concurrency Semaphore              │
    │  - Heartbeat Emitter (Every 3s)               │
    │  - Pluggable Handlers (sleep, cpu_hash, ...)  │
    └───────────────────────────────────────────────┘
```

For detailed architecture diagrams and failover sequences, see [planning/ARCHITECTURE_DIAGRAM.md](planning/ARCHITECTURE_DIAGRAM.md).

---

## Key Features

1. **Atomic Token-Bucket Rate Limiter:** Evaluated inside `scripts/token_bucket.lua` using microsecond Redis timestamps. Guaranteed zero race conditions under concurrent burst traffic. Attaches `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `Retry-After` headers.
2. **Consistent-Hash Dispatcher:** 128-bit MD5 ring with 100 virtual nodes per worker, providing uniform distribution and minimal remapping upon worker join/leave.
3. **Strict Priority Scheduling:** Formula `(-priority * 1e12) + timestamp_ms` stored in a Redis sorted set guarantees Priority 10 jobs pop and execute before Priority 1 jobs, maintaining FIFO ordering within the same priority level.
4. **Heartbeat-Based Crash Recovery:** Workers emit heartbeats with a 10s TTL every 3s. The watchdog detects missing heartbeats within 2s, evicts dead nodes, and re-enqueues stranded in-flight jobs with zero data loss.
5. **Exponential Backoff & Dead-Letter Queue:** Failed retryable tasks undergo exponential backoff with full jitter in `queue:delayed`. Exhausted retries land in `queue:dlq` and can be inspected via `GET /v1/dlq` and safely replayed via `POST /v1/dlq/{id}/replay`.
6. **100% Async / Zero Blocking I/O:** Fully asynchronous architecture across all network layers using `asyncio`, `asyncpg`, `redis.asyncio`, and `grpc.aio`.

---

## Quick Start

### Windows (One-Click Launcher)
Simply double-click `start.bat` or run:
```cmd
start.bat
```

---

### Manual / Cross-Platform Startup
#### Prerequisites
* Docker & Docker Compose (Docker Engine 24+)
* Python 3.11+ (if running tests locally)

#### 1. Configure Environment
```bash
cp .env.example .env
```

#### 2. Boot Cluster with 3 Workers
```bash
docker compose up --build -d --scale worker=3
```

Verify that all 7 containers are healthy and running:
```bash
docker compose ps
```

### 3. Check System Health
```bash
curl -s http://localhost:8000/health
```
Output:
```json
{"status":"healthy","postgres":true,"redis":true,"active_workers":3}
```

---

## API Endpoints

| Method | Endpoint | Description | Auth Required |
|---|---|---|---|
| `POST` | `/v1/jobs` | Submit a new task for distributed execution | Yes (`X-API-Key`) |
| `GET` | `/v1/jobs/{id}` | Query current job status, attempt count, and output | Yes (`X-API-Key`) |
| `GET` | `/v1/dlq` | List dead-lettered jobs pending intervention | Yes (`X-API-Key`) |
| `POST` | `/v1/dlq/{id}/replay` | Reset attempts and re-enqueue a dead-lettered job | Yes (`X-API-Key`) |
| `GET` | `/health` | Liveness and dependency health (Postgres, Redis, Workers) | No |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint | No |
| `GET` | `/docs` | Interactive Swagger / OpenAPI documentation | No |

### Submitting a Job
```bash
curl -X POST http://localhost:8000/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key-1" \
  -d '{
    "task_type": "cpu_hash",
    "payload": {"iterations": 1000, "input": "hello-world"},
    "priority": 8,
    "max_retries": 3,
    "idempotency_key": "custom-unique-key-123"
  }'
```

Response:
```json
{
  "job_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "QUEUED",
  "priority": 8,
  "created_at": "2026-11-19T08:00:00Z"
}
```

---

## Running Automated Tests

The test suite covers unit tests, integration tests against containerized databases, and live failover recovery tests.

```bash
# Run complete test suite (38 tests)
pytest tests/ -v
```

Test Breakdown:
* **`tests/unit/`**: Pydantic schemas, config parsing, Protobuf serialization, Token-Bucket Lua logic, MD5 Hash Ring uniformity, Heartbeat emitter, and Watchdog logic.
* **`tests/integration/`**: PostgreSQL and Redis connectivity, Rate limiting enforcement, Idempotency deduplication, Priority ordering (10 before 1), gRPC execution, and DLQ replay lifecycle.
* **`tests/failover/`**: Worker crash recovery and state preservation.

---

## Benchmarks & Performance Verification

Empirical results verified on a multi-container Docker cluster:

| Benchmark | Target | Verified Result | Status |
|---|---|---|---|
| **wrk Ingestion Throughput** | High Sustained RPS | **140.05 req/s** (full DB insert + Redis push) | **PASSED** |
| **wrk Gateway Direct** | Raw ASGI RPS | **248.87 req/s** (health + Redis ping) | **PASSED** |
| **Locust Stateful Workload** | 20 users / 30s | **2,938 requests**, **105.23 req/s**, median **52ms** | **PASSED** |
| **Worker Failover Recovery** | `< 15.0s` | **8.76s** (from `docker kill` to job `COMPLETED`) | **PASSED** |
| **Data Loss** | 0% | **0 dropped tasks**, all transitions audited in DB | **PASSED** |
| **Test Suite Pass Rate** | 100% | **38 passed / 0 failed** in 17.71s | **PASSED** |

For complete percentile distributions, charts, and raw tool output logs, consult [benchmarks/results.md](benchmarks/results.md).

---

## Project Structure

```text
.
├── proto/
│   ├── task_queue.proto            # gRPC service and message contracts
│   ├── task_queue_pb2.py           # Generated Python protobuf stubs
│   └── task_queue_pb2_grpc.py      # Generated Python gRPC stubs
├── services/
│   ├── common/                     # Config, DB connection, models, hash ring, logging
│   ├── gateway/                    # FastAPI routes, rate limiter, Pydantic schemas, DLQ
│   ├── dispatcher/                 # Core scheduler, consistent-hash router, watchdog, promoter
│   └── worker/                     # gRPC worker server, heartbeat emitter, task handlers
├── scripts/
│   ├── benchmark_failover.py       # Live container kill automated failover test
│   ├── gen_proto.py                # Protobuf code generator script
│   ├── init.sql                    # PostgreSQL schema DDL and indexes
│   └── token_bucket.lua            # Atomic Redis Lua rate limiting script
├── tests/
│   ├── unit/                       # Unit tests (schemas, hash ring, Lua, heartbeat, etc.)
│   ├── integration/                # Integration tests (Postgres, Redis, priority, gRPC, DLQ)
│   └── failover/                   # Worker crash recovery tests
├── benchmarks/
│   ├── post_job.lua                # wrk load script
│   ├── locustfile.py               # Locust stateful benchmark script
│   └── results.md                  # Documented benchmark metrics and test logs
├── docker-compose.yml              # Cluster definition (Gateway, Dispatcher, 3 Workers, Postgres, Redis)
├── Dockerfile                      # Multi-stage production container build
├── requirements.txt                # Pinned dependencies
├── pytest.ini                      # Pytest async configuration
├── .env.example                    # Environment template
└── README.md                       # Project documentation
```
