# Technology Stack

## 1. Confirmed vs. Proposed Stack

| Layer | Technology | Status | Justification |
| :--- | :--- | :--- | :--- |
| **API Gateway** | **FastAPI** (Python 3.11+) + Uvicorn | Confirmed option (Default) | Async I/O, automatic OpenAPI docs, fast iteration. *(C++20 alternative noted in UD-01).* |
| **RPC Transport** | **gRPC** (`grpcio`, `grpcio-tools`, Protobuf v3) | Confirmed | Low-latency binary serialization, strongly typed contracts, HTTP/2 multiplexing. |
| **Broker / Cache / Rate Limit** | **Redis 7.x** (`redis-py` async + Lua) | Confirmed | Atomic Lua token-bucket rate limiting, `ZSET` priority queues, TTL heartbeats. |
| **Primary Database** | **PostgreSQL 16** (`asyncpg` + SQLAlchemy 2.0) | Confirmed | ACID job-state persistence, indexing on status/idempotency, reliable audit trail. |
| **Containerization** | **Docker & Docker Compose v2** | Confirmed | Single-command multi-node cluster orchestration (`--scale worker=N`). |
| **Load Testing** | **`wrk`** & **Locust** | Confirmed | `wrk` for raw HTTP throughput/p99; Locust for stateful job submission/polling scenarios. |
| **Testing** | **pytest**, `pytest-asyncio`, `httpx` | Proposed | Standard async Python testing ecosystem. |
| **Schema Migrations** | **Alembic** | Proposed | Version-controlled PostgreSQL schema migrations. |
| **Logging** | **structlog** (JSON output) | Proposed | Machine-parsable logs with `job_id`, `worker_id`, and latency fields. |

---

## 2. Compatibility Matrix
* **Python 3.11+** <-> FastAPI 0.115+, SQLAlchemy 2.0 (async), `asyncpg` 0.29+, `redis` 5.x, `grpcio` 1.66+.
* **Redis 7-alpine** <-> Native Lua 5.1 scripting (`EVALSHA`), `ZSET` commands (`ZADD`, `ZPOPMIN`, `ZRANGEBYSCORE`), keyspace TTLs.
* **PostgreSQL 16-alpine** <-> Native `UUID`, `JSONB`, `TIMESTAMPTZ`, partial indexes.
* **Docker Compose v2** <-> Supports `depends_on` with `condition: service_healthy` and replica scaling for stateless workers.

---

## 3. Dependency Management Rules
* Pin exact or minor-bounded versions in `requirements.txt` (or `pyproject.toml`).
* Generate gRPC Python stubs (`*_pb2.py`, `*_pb2_grpc.py`) via a dedicated script (`scripts/gen_proto.sh`) from `proto/task_queue.proto`.
* Never commit compiled `.pyc`, `__pycache__`, or `.env` files.