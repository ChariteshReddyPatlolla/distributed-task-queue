# High-Concurrency Distributed Task Queue & Rate-Limited API Gateway

A containerized, fault-tolerant distributed task execution platform featuring a Redis-backed token-bucket API Gateway, gRPC-based stateless worker pool with consistent hashing, priority queueing, exponential backoff retries, heartbeat-based failover, Dead-Letter Queue (DLQ), and PostgreSQL job-state persistence.

---

## Documentation Index

| File | Purpose |
| :--- | :--- |
| `AGENTS.md` | **Start here for AI agents (Google Antigravity).** Rules, coding standards, architecture guardrails, and workflow. |
| `PROJECT_REQUIREMENTS.md` | Confirmed requirements, proposed assumptions, functional & non-functional requirements, and unresolved decisions. |
| `FEATURES.md` | Detailed functional specifications for every system capability. |
| `TECH_STACK.md` | Confirmed vs. proposed technologies, versions, and compatibility matrix. |
| `ARCHITECTURE.md` | System architecture, Mermaid diagrams, data flows, consistent hashing, and failover design. |
| `UI_UX_GUIDELINES.md` | Explicit scope note (headless backend) + CLI, OpenAPI/Swagger, and observability UX standards. |
| `DATABASE_SCHEMA.md` | PostgreSQL schema, Mermaid ER diagram, indexes, and Redis keyspace design. |
| `API_DOCUMENTATION.md` | REST API endpoints, gRPC `.proto` contracts, error codes, and rate-limit headers. |
| `TESTING.md` | Unit, integration, fault-injection, and load-testing (`wrk` / Locust) specifications. |
| `IMPLEMENTATION_ROADMAP.md` | Incremental 6-phase build plan with dependencies and completion criteria. |
| `.env.example` | Environment variable template for local and Docker Compose execution. |

---

## Quick Start (Target Workflow)

Once implemented according to `IMPLEMENTATION_ROADMAP.md`:

```bash
cp .env.example .env
docker compose up --build --scale worker=3
```

* **API Gateway:** `http://localhost:8000`
* **OpenAPI Docs:** `http://localhost:8000/docs`
* **Health Check:** `http://localhost:8000/health`

## Target Benchmark Metrics
* **Peak Throughput:** Requests/sec sustained under `wrk` / Locust load.
* **Tail Latency:** p50, p95, and p99 gateway response latency (ms).
* **Failover Recovery Time:** Seconds from worker termination (`docker kill`) to job reassignment and completion.