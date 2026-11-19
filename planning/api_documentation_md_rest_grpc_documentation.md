# API & gRPC Documentation

## 1. REST API (API Gateway — Port `8000`)

### Authentication & Rate-Limit Headers
* **Request Header:** `X-API-Key: <client_key>` (required on `/v1/*` endpoints).
* **Response Headers (all `/v1/*` responses):**
  * `X-RateLimit-Limit`: Max bucket capacity.
  * `X-RateLimit-Remaining`: Tokens left after this request.
  * `X-RateLimit-Reset`: Seconds until bucket is full.
  * `Retry-After`: Present on `429 Too Many Requests`.

---

### `POST /v1/jobs` — Submit Asynchronous Job
**Request Body:**
```json
{
  "task_type": "cpu_hash",
  "payload": { "iterations": 50000, "input": "benchmark" },
  "priority": 8,
  "max_retries": 3,
  "idempotency_key": "req-2026-0001"
}
```
**Responses:**
* `202 Accepted` — Job queued.
  ```json
  {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "QUEUED",
    "priority": 8,
    "created_at": "2026-11-19T00:45:00Z"
  }
  ```
* `200 OK` — Duplicate `idempotency_key`; returns existing job.
* `422 Unprocessable Entity` — Invalid payload or priority out of range (`1`–`10`).
* `429 Too Many Requests` — Rate limit exceeded.

---

### `GET /v1/jobs/{job_id}` — Get Job Status & Result
**Responses:**
* `200 OK` — Full job record including `status`, `attempt`, `assigned_worker`, `result`, `last_error`, and timestamps.
* `404 Not Found` — Unknown `job_id`.

---

### `GET /v1/dlq` — List Dead-Lettered Jobs
* **Query Params:** `limit` (default `20`, max `100`), `offset` (default `0`).
* **Response:** `200 OK` with list of `DEAD_LETTERED` jobs and total count.

---

### `POST /v1/dlq/{job_id}/replay` — Replay Dead-Lettered Job
* Resets `attempt = 0`, sets `status = QUEUED`, removes from `queue:dlq`, and pushes back to `queue:ready`.
* **Response:** `202 Accepted`.

---

### `GET /health` & `GET /metrics`
* `GET /health`: Returns `200 OK` if PostgreSQL and Redis are reachable, plus count of active workers.
* `GET /metrics`: Returns JSON counters (`jobs_submitted`, `jobs_completed`, `jobs_retried`, `jobs_dlq`, `active_workers`, `rate_limited_total`).

---

## 2. Internal gRPC Contract (`proto/task_queue.proto`)

```protobuf
syntax = "proto3";

package taskqueue;

service WorkerService {
  rpc ExecuteTask (TaskRequest) returns (TaskResponse);
  rpc HealthCheck (HealthRequest) returns (HealthResponse);
}

message TaskRequest {
  string job_id = 1;
  string task_type = 2;
  string payload_json = 3;
  int32 attempt = 4;
  int32 max_retries = 5;
}

message TaskResponse {
  string job_id = 1;
  bool success = 2;
  string result_json = 3;
  string error_message = 4;
  int64 duration_ms = 5;
}

message HealthRequest {}

message HealthResponse {
  string worker_id = 1;
  bool healthy = 2;
  int32 inflight_count = 3;
}
```