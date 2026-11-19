# Project Requirements

## 1. Project Overview
Build a **High-Concurrency Distributed Task Queue & Rate-Limited API Gateway** that demonstrates production-grade Distributed Systems, Concurrency, Caching, and Containerization engineering, validated by reproducible load-testing and failover benchmarks.

---

## 2. Confirmed Requirements vs. Proposed Assumptions

### 2.1 Confirmed Requirements (User-Specified)
1. **API Gateway:** Entry point accepting asynchronous job submissions, implemented in **C++ or FastAPI**.
2. **Rate Limiting:** **Redis-backed token-bucket** rate limiting enforced at the API Gateway.
3. **Asynchronous Job Dispatch:** Dispatch jobs to a pool of **stateless worker nodes over gRPC**.
4. **Routing:** **Consistent hashing** across worker nodes.
5. **Fault Tolerance:**
   * **Priority queues** for job ordering.
   * **Automatic retries with exponential backoff**.
   * **Heartbeat monitoring** to detect dead workers.
   * **Dead-Letter Queue (DLQ)** for permanently failed tasks.
6. **Persistence:** Persist job states and lifecycle metadata in **PostgreSQL**.
7. **Deployment:** Containerize the API Gateway, Redis broker, PostgreSQL database, and multiple worker nodes using **Docker Compose** so the entire cluster runs with a single command.
8. **Benchmarking:** Load-test with **`wrk` or Locust** to measure:
   * Peak throughput (requests/sec)
   * p99 latency
   * Worker failover recovery time

### 2.2 Proposed Assumptions (To Be Confirmed)
> **Note for Google Antigravity:** Do not treat these as permanent if the user overrides them. Confirm unresolved items before Phase 1 if required.

1. **Primary Language (Default):** **Python 3.11+ with FastAPI + `grpcio`** for both Gateway and Worker services to maximize development velocity and native async support. *(Alternative: C++20 with Drogon/Crow + gRPC C++).*
2. **Client Identification for Rate Limiting:** Clients authenticate via an `X-API-Key` HTTP header; each key maps to its own Redis token bucket.
3. **Headless System (No Frontend UI):** No web dashboard is required for v1; interaction occurs via REST API, OpenAPI/Swagger UI, and CLI load-test scripts.
4. **Built-in Benchmark Task Handlers:** Workers support deterministic task types (`sleep`, `cpu_hash`, `webhook`, `fail_sim`) so load tests and fault-injection tests are reproducible.
5. **Atomic Rate Limiting:** Token-bucket refill and consumption execute inside a **Redis Lua script** to prevent race conditions under high concurrency.

---

## 3. Unresolved Critical Decisions
| ID | Decision | Options | Default Assumption |
| :--- | :--- | :--- | :--- |
| **UD-01** | Gateway & Worker language | FastAPI (Python) vs. C++20 | FastAPI (Python 3.11+) |
| **UD-02** | Load-testing tool | `wrk` vs. Locust | Both (`wrk` for raw HTTP throughput/p99; Locust for complex job lifecycles) |
| **UD-03** | Client auth mechanism | API Key (`X-API-Key`) vs. IP-based vs. JWT | `X-API-Key` header (fallback to client IP if disabled in dev) |
| **UD-04** | Optional monitoring UI | None vs. Prometheus + Grafana | None in v1 (structured JSON metrics endpoint `/metrics` included) |

---

## 4. Functional Requirements
* **FR-01:** Accept job submission requests (`POST /v1/jobs`) with task type, payload, priority (`1`–`10`), max retries, and idempotency key.
* **FR-02:** Enforce per-client token-bucket rate limiting via Redis Lua script; return `429 Too Many Requests` with `Retry-After` and `X-RateLimit-*` headers when exhausted.
* **FR-03:** Persist initial job record (`QUEUED`) in PostgreSQL before acknowledging `202 Accepted`.
* **FR-04:** Enqueue job IDs into a Redis priority queue (`ZSET`) and dispatch them to healthy workers over gRPC using a consistent hashing ring.
* **FR-05:** Workers execute tasks asynchronously, report heartbeats periodically, and update job status (`RUNNING`, `COMPLETED`, `RETRYING`, `DEAD_LETTERED`) in PostgreSQL.
* **FR-06:** On transient failure, schedule retries using exponential backoff with jitter: `delay = min(max_delay, base_delay * 2^attempt) + jitter`.
* **FR-07:** When `attempt >= max_retries`, move the job to the Dead-Letter Queue (DLQ) and mark status `DEAD_LETTERED` in PostgreSQL.
* **FR-08:** Detect dead workers via missed heartbeats (TTL expiry in Redis), remove them from the consistent hash ring, and re-enqueue their in-flight jobs.
* **FR-09:** Expose job status lookup (`GET /v1/jobs/{job_id}`), DLQ inspection (`GET /v1/dlq`), DLQ replay (`POST /v1/dlq/{job_id}/replay`), and cluster health (`GET /health`).

---

## 5. Non-Functional Requirements
* **Concurrency:** Gateway must handle concurrent async I/O without blocking the event loop; Redis rate limiting must be atomic.
* **Fault Tolerance:** Killing any single worker container (`docker kill`) must not lose acknowledged jobs; in-flight jobs must be recovered automatically.
* **Idempotency:** Duplicate submissions with the same `idempotency_key` must return the existing `job_id` without re-enqueueing.
* **Portability:** Entire stack must boot cleanly via `docker compose up --build`.
* **Observability:** Structured JSON logs with `correlation_id`, `job_id`, and `worker_id`.