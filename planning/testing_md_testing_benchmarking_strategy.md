# Testing & Benchmarking Strategy

## 1. Unit Tests (`tests/unit/`)
* **Token-Bucket Lua Script:** Verify burst consumption up to `BUCKET_CAPACITY`, rejection at `0` tokens, and accurate refill after `T` seconds.
* **Consistent Hash Ring:** Verify deterministic routing, even key distribution across `N` workers with virtual nodes, and minimal key remapping when a node is added/removed.
* **Exponential Backoff:** Verify `base * 2^attempt + jitter` respects `MAX_BACKOFF_SEC`.
* **Schema Validation:** Verify invalid priorities (`0`, `11`), oversized payloads (`>64KB`), and unknown `task_type` return `422`.

---

## 2. Integration Tests (`tests/integration/`)
Run against containerized Redis + PostgreSQL:
* **End-to-End Job Lifecycle:** Submit job -> verify `202` -> poll until `COMPLETED` -> verify `result` in PostgreSQL.
* **Priority Ordering:** Pause dispatcher, enqueue 10 low-priority (`1`) and 10 high-priority (`10`) jobs, resume dispatcher -> verify high-priority jobs start first.
* **Idempotency:** Send identical `POST /v1/jobs` twice with the same `idempotency_key` -> verify only 1 row in PostgreSQL and 1 execution.
* **Retry & DLQ Flow:** Submit `fail_sim` job with `max_retries=2` -> verify 2 retries with backoff -> verify final status `DEAD_LETTERED` and presence in `GET /v1/dlq` -> call `/replay` and verify re-execution.

---

## 3. Fault-Injection & Failover Tests (`tests/failover/`)
* **Worker Crash Recovery:**
  1. Start cluster with 3 workers (`docker compose up --scale worker=3`).
  2. Submit a `sleep` job (duration `5s`).
  3. Identify assigned worker via `GET /v1/jobs/{id}` and execute `docker kill <worker_container>`.
  4. Measure elapsed time from `docker kill` until a surviving worker picks up and completes the job.
  5. **Pass Criterion:** Job completes on another worker with zero data loss; recovery time recorded (`<= HEARTBEAT_TTL_SEC + 5s`).

---

## 4. Load & Performance Benchmarks (`benchmarks/`)
* **`wrk` Throughput & p99 Latency Test:**
  ```bash
  wrk -t4 -c100 -d30s -s benchmarks/post_job.lua http://localhost:8000/v1/jobs
  ```
  * Capture: Requests/sec, p50, p95, p99 latency, and non-2xx/429 error count.
* **Locust Stateful Benchmark (`benchmarks/locustfile.py`):**
  * Simulates mixed clients submitting prioritized jobs, hitting rate limits, and polling status endpoints.
* **Deliverable:** Save raw benchmark outputs to `benchmarks/results.md` for resume verification.