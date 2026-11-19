# System Architecture

## 1. High-Level Architecture Diagram

```mermaid
flowchart LR
    Client[Client / wrk / Locust] -->|HTTP REST + X-API-Key| GW[API Gateway<br/>FastAPI]
    GW -->|Lua Token Bucket| Redis[(Redis 7<br/>Rate Limit, Priority Queue,<br/>Delayed Queue, Heartbeats)]
    GW -->|Persist QUEUED Job| PG[(PostgreSQL 16<br/>Job State & History)]
    
    subgraph Cluster [Coordinator & Worker Pool]
        Disp[Dispatcher &<br/>Failover Monitor] <-->|ZPOPMIN / Heartbeats| Redis
        Disp -->|Update State| PG
        Disp -->|gRPC ExecuteTask<br/>Consistent Hash Ring| W1[Worker Node 1]
        Disp -->|gRPC ExecuteTask<br/>Consistent Hash Ring| W2[Worker Node 2]
        Disp -->|gRPC ExecuteTask<br/>Consistent Hash Ring| W3[Worker Node N]
    end

    W1 -->|Heartbeat + Result| Redis
    W2 -->|Heartbeat + Result| Redis
    W3 -->|Heartbeat + Result| Redis
    W1 -->|Update Job State| PG
    W2 -->|Update Job State| PG
    W3 -->|Update Job State| PG
```

---

## 2. Core Components

### 2.1 API Gateway (`services/gateway`)
* Exposes REST endpoints (`/v1/jobs`, `/v1/dlq`, `/health`, `/metrics`).
* Middleware executes the **Redis Lua Token-Bucket** check before hitting route handlers.
* Validates payloads, enforces idempotency via PostgreSQL, inserts `QUEUED` jobs, and pushes job IDs to Redis `queue:ready`.

### 2.2 Dispatcher & Failover Monitor (`services/dispatcher`)
* Runs three lightweight async background loops:
  1. **Delayed-to-Ready Promoter:** Moves due jobs from `queue:delayed` (`ZSET`) to `queue:ready` (`ZSET`).
  2. **Consistent-Hash Dispatcher:** Pops highest-priority jobs from `queue:ready`, selects a healthy worker from the **Consistent Hash Ring**, records the job in `worker:inflight:{worker_id}`, and dispatches via gRPC.
  3. **Heartbeat Watchdog:** Scans registered workers every `2s`. If `worker:heartbeat:{worker_id}` has expired, removes the worker from the ring and re-enqueues all jobs in `worker:inflight:{worker_id}`.

### 2.3 Stateless Worker Pool (`services/worker`)
* Exposes a gRPC server (`WorkerService.ExecuteTask`, `WorkerService.HealthCheck`).
* Registers itself in Redis (`workers:active`) on startup and refreshes `worker:heartbeat:{worker_id}` every `3s` (TTL `10s`).
* Executes pluggable task handlers (`sleep`, `cpu_hash`, `webhook`, `fail_sim`) using a bounded concurrency semaphore (`WORKER_MAX_CONCURRENCY`).
* Updates PostgreSQL job status and clears `worker:inflight:{worker_id}` upon completion or failure.

---

## 3. Job Lifecycle & Failover Sequence

```mermaid
sequenceDiagram
    participant C as Client
    participant GW as API Gateway
    participant R as Redis
    participant PG as PostgreSQL
    participant D as Dispatcher
    participant W as Worker (gRPC)

    C->>GW: POST /v1/jobs (X-API-Key, priority=8)
    GW->>R: EVAL token_bucket.lua
    alt Rate Limit Exceeded
        R-->>GW: 0 (denied)
        GW-->>C: 429 Too Many Requests
    else Allowed
        R-->>GW: 1 (remaining tokens)
        GW->>PG: INSERT job (status=QUEUED)
        GW->>R: ZADD queue:ready (score, job_id)
        GW-->>C: 202 Accepted (job_id)
    end

    D->>R: ZPOPMIN queue:ready
    D->>D: ConsistentHashRing.get_node(routing_key)
    D->>R: SADD worker:inflight:{worker_id} job_id
    D->>W: gRPC ExecuteTask(job_id, payload)
    W->>PG: UPDATE status=RUNNING
    alt Success
        W->>PG: UPDATE status=COMPLETED, result
        W->>R: SREM worker:inflight:{worker_id} job_id
        W-->>D: TaskResponse(OK)
    else Transient Error (attempt < max_retries)
        W->>PG: UPDATE status=RETRYING, attempt++
        W->>R: ZADD queue:delayed (now + backoff, job_id)
        W->>R: SREM worker:inflight:{worker_id} job_id
    else Max Retries Exhausted
        W->>PG: UPDATE status=DEAD_LETTERED, last_error
        W->>R: LPUSH queue:dlq job_id
        W->>R: SREM worker:inflight:{worker_id} job_id
    end
```

---

## 4. Security & Error-Handling Architecture
* **Input Validation:** Strict Pydantic v2 schemas; max payload size capped at `64 KB` to prevent memory exhaustion.
* **Rate-Limit Isolation:** Per-API-Key buckets prevent a single noisy client from starving other tenants.
* **Timeouts:** Every gRPC call carries an explicit deadline (`GRPC_TASK_TIMEOUT_SEC`); PostgreSQL and Redis pools enforce connection timeouts.
* **Graceful Shutdown:** Workers handle `SIGTERM` by stopping new gRPC accepts, finishing in-flight tasks up to a grace period, and deregistering cleanly.