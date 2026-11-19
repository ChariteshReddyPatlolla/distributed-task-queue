# Feature Specifications

## F1. Redis-Backed Token-Bucket Rate Limiter
* **Description:** Protects the API Gateway from traffic spikes by enforcing per-client request quotas.
* **Mechanism:**
  * Each client (`X-API-Key`) has a Redis hash storing `tokens` and `last_refill_ts`.
  * A single **Redis Lua script** atomically calculates elapsed time, refills tokens up to `BUCKET_CAPACITY` at `REFILL_RATE_PER_SEC`, and decrements 1 token if available.
* **Outputs:**
  * Allowed: Request proceeds; response includes `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.
  * Denied: HTTP `429 Too Many Requests` with `Retry-After` header.
* **Edge Cases:** Redis unavailable -> fail-open or fail-closed configurable via `RATE_LIMIT_FAIL_MODE` (default: `fail_closed` with `503 Service Unavailable`).

---

## F2. Asynchronous Job Submission & Idempotency
* **Description:** Clients submit background jobs and receive immediate `202 Accepted` responses.
* **Inputs:** `task_type`, `payload` (JSON), `priority` (`1` = lowest, `10` = highest, default `5`), `max_retries` (default `3`), optional `idempotency_key`.
* **Behavior:**
  * Validate payload against schema.
  * If `idempotency_key` exists in PostgreSQL, return existing job record (`200 OK`).
  * Insert job into PostgreSQL with `status = QUEUED`, push to Redis priority queue, return `202 Accepted` with `job_id`.

---

## F3. Priority Queue & Delayed Retry Scheduler
* **Description:** Ensures high-priority tasks are dispatched before lower-priority tasks and manages delayed retries.
* **Mechanism:**
  * **Ready Queue:** Redis Sorted Set (`queue:ready`) scored by `(-priority * 1e12) + enqueue_timestamp_ms` so higher priority pops first, with FIFO ordering within the same priority.
  * **Retry Queue:** Redis Sorted Set (`queue:delayed`) scored by `next_retry_unix_ms`. A lightweight scheduler loop moves due jobs from `queue:delayed` to `queue:ready`.

---

## F4. Consistent Hashing & gRPC Worker Dispatch
* **Description:** Routes jobs from the Dispatcher to stateless Worker nodes over gRPC with minimal key remapping during scaling or failover.
* **Mechanism:**
  * Maintain an in-memory **Consistent Hash Ring** with `V` virtual nodes per active worker (default `V = 100`).
  * Hash `routing_key` (default: `job_id` or optional `partition_key`) to select the target worker's gRPC endpoint.
  * Invoke `WorkerService.ExecuteTask(TaskRequest)` over a persistent gRPC channel with deadline/timeout.

---

## F5. Heartbeat Monitoring & Automatic Failover
* **Description:** Detects crashed or partitioned workers and recovers their in-flight jobs.
* **Mechanism:**
  * Each worker sends a heartbeat every `HEARTBEAT_INTERVAL_SEC` (default `3s`) by refreshing a Redis key `worker:heartbeat:{worker_id}` with TTL `HEARTBEAT_TTL_SEC` (default `10s`).
  * The Coordinator/Dispatcher tracks active workers and their in-flight job sets (`worker:inflight:{worker_id}`).
  * If a worker's heartbeat expires, the Coordinator removes it from the hash ring, moves all its in-flight jobs back to `queue:ready` (incrementing `attempt`), and records the failover timestamp for benchmarking.

---

## F6. Exponential Backoff & Dead-Letter Queue (DLQ)
* **Description:** Handles transient failures gracefully and isolates poison-pill jobs.
* **Mechanism:**
  * On task error or gRPC timeout:
    * If `attempt < max_retries`: compute `delay = min(MAX_BACKOFF_SEC, BASE_BACKOFF_SEC * (2 ** attempt)) + random_jitter`, update PostgreSQL (`status = RETRYING`), add to `queue:delayed`.
    * If `attempt >= max_retries`: update PostgreSQL (`status = DEAD_LETTERED`, record `last_error`), push `job_id` to Redis list `queue:dlq`.
  * Operators can inspect (`GET /v1/dlq`) or replay (`POST /v1/dlq/{job_id}/replay`) dead-lettered jobs.

---

## F7. Benchmarking & Load-Testing Suite
* **Description:** Reproducible scripts to quantify system performance for engineering validation.
* **Metrics Captured:**
  * Peak throughput (req/sec) via `wrk` / Locust.
  * Gateway latency distribution (p50, p95, p99).
  * End-to-end job completion latency.
  * Worker failover recovery time (seconds from `docker kill` to job completion on surviving worker).