# Implementation Roadmap

Develop the project incrementally across **6 phases**. Do not start a phase until all completion criteria of the previous phase pass.

---

## Phase 1: Foundation, Contracts & Infrastructure
* **Objective:** Set up repository structure, Docker Compose stack, PostgreSQL schema, and gRPC Protobuf contracts.
* **Features:** Core config, DB models/migrations, Protobuf stub generation, Docker Compose (`postgres`, `redis`).
* **Modules:** `proto/task_queue.proto`, `scripts/gen_proto.sh`, `services/common/config.py`, `services/common/db.py`, `docker-compose.yml`, `.env.example`.
* **Dependencies:** None.
* **Expected Output:** `docker compose up postgres redis` boots cleanly; Alembic/SQL creates `jobs` and `job_attempts` tables; gRPC Python stubs compile without errors.
* **Testing:** Connection health check script for PostgreSQL and Redis.
* **Completion Criteria:** Schema verified in Postgres; generated gRPC stubs import cleanly.

---

## Phase 2: API Gateway & Redis Token-Bucket Rate Limiter
* **Objective:** Build the FastAPI Gateway with atomic rate limiting, job submission, idempotency, and status endpoints.
* **Features:** `F1` (Rate Limiter), `F2` (Job Submission & Idempotency), `GET /v1/jobs/{id}`, `GET /health`, `GET /metrics`.
* **Modules:** `scripts/token_bucket.lua`, `services/gateway/main.py`, `services/gateway/rate_limiter.py`, `services/gateway/routes.py`.
* **Dependencies:** Phase 1.
* **Expected Output:** Gateway accepts jobs, enforces rate limits with `429` + headers, persists `QUEUED` jobs to Postgres, and enqueues IDs into Redis `queue:ready`.
* **Testing:** Unit tests for Lua token bucket; integration tests for `202`, `200` (idempotent), `422`, and `429`.
* **Completion Criteria:** All gateway unit and integration tests pass.

---

## Phase 3: Stateless gRPC Worker Pool & Heartbeats
* **Objective:** Implement the gRPC worker service with bounded concurrency, pluggable task handlers, and Redis heartbeats.
* **Features:** gRPC `ExecuteTask` & `HealthCheck`, heartbeat emitter (`worker:heartbeat:{id}`), handlers (`sleep`, `cpu_hash`, `webhook`, `fail_sim`), PostgreSQL status updates.
* **Modules:** `services/worker/server.py`, `services/worker/handlers.py`, `services/worker/heartbeat.py`.
* **Dependencies:** Phase 1.
* **Expected Output:** Multiple worker containers run concurrently, register in `workers:active`, refresh heartbeats, and execute gRPC tasks.
* **Testing:** Direct gRPC client test invoking `ExecuteTask` for success and simulated failure.
* **Completion Criteria:** Worker executes tasks, updates `jobs` and `job_attempts` in Postgres, and maintains TTL heartbeat in Redis.

---

## Phase 4: Consistent-Hash Dispatcher & Priority Scheduling
* **Objective:** Connect the Redis priority queue to the gRPC worker pool via a consistent hashing ring.
* **Features:** `F3` (Priority Queue), `F4` (Consistent Hashing & gRPC Dispatch).
* **Modules:** `services/common/hash_ring.py`, `services/dispatcher/main.py`, `services/dispatcher/dispatcher.py`.
* **Dependencies:** Phases 2 & 3.
* **Expected Output:** Jobs submitted to the Gateway are automatically popped by priority, routed via consistent hashing, and completed by workers.
* **Testing:** Unit tests for hash ring distribution; integration test proving priority `10` executes before priority `1`.
* **Completion Criteria:** End-to-end `POST /v1/jobs` -> `COMPLETED` works across 3 scaled worker containers.

---

## Phase 5: Fault Tolerance (Retries, Exponential Backoff, Failover & DLQ)
* **Objective:** Harden the distributed system against task errors and worker crashes.
* **Features:** `F5` (Heartbeat Watchdog & Failover), `F6` (Exponential Backoff & DLQ), `GET /v1/dlq`, `POST /v1/dlq/{id}/replay`.
* **Modules:** `services/dispatcher/watchdog.py`, `services/dispatcher/retry_promoter.py`, `services/gateway/dlq_routes.py`.
* **Dependencies:** Phase 4.
* **Expected Output:** Failed tasks retry with exponential backoff, move to DLQ after `max_retries`, and can be replayed. Killing a worker triggers automatic re-enqueueing of its in-flight jobs.
* **Testing:** Integration tests for retry/DLQ/replay; automated failover test using `docker kill`.
* **Completion Criteria:** Zero lost jobs during `docker kill` failover test; DLQ replay succeeds.

---

## Phase 6: Containerization Polish & Load Benchmarking
* **Objective:** Finalize single-command Docker Compose orchestration and collect verifiable performance benchmarks.
* **Features:** `F7` (Benchmarking Suite), multi-stage `Dockerfile`, final `README.md` with benchmark results.
* **Modules:** `Dockerfile`, `docker-compose.yml`, `benchmarks/post_job.lua`, `benchmarks/locustfile.py`, `benchmarks/results.md`.
* **Dependencies:** Phase 5.
* **Expected Output:** `docker compose up --build --scale worker=3` runs cleanly; `wrk` and Locust produce throughput, p99 latency, and failover recovery metrics.
* **Testing:** Full `pytest` suite + reproducible `wrk` and Locust benchmark runs.
* **Completion Criteria:** All tests pass and `benchmarks/results.md` contains verified throughput, p99 latency, and failover time.