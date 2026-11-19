# AGENTS.md — Instructions for Google Antigravity

This document is the authoritative operating manual for **Google Antigravity** (and any AI coding agent) working on this repository.

---

## 1. How to Interpret This Documentation
1. Read `PROJECT_REQUIREMENTS.md`, `ARCHITECTURE.md`, `DATABASE_SCHEMA.md`, `API_DOCUMENTATION.md`, and `IMPLEMENTATION_ROADMAP.md` **before writing any code**.
2. Follow `IMPLEMENTATION_ROADMAP.md` strictly phase by phase. **Do not generate the entire application in one pass.**
3. Respect the distinction between **Confirmed Requirements** and **Proposed Assumptions** in `PROJECT_REQUIREMENTS.md`.

---

## 2. Folder Structure (Mandatory)
```text
.
├── proto/
│   └── task_queue.proto
├── services/
│   ├── common/              # Shared config, DB models, Redis client, consistent hashing, logging
│   ├── gateway/             # FastAPI app, rate-limit middleware, REST routes
│   ├── dispatcher/          # Priority queue consumer, consistent hash router, heartbeat watchdog
│   └── worker/              # gRPC server, heartbeat emitter, task handlers
├── scripts/
│   ├── gen_proto.sh
│   └── token_bucket.lua
├── tests/
│   ├── unit/
│   ├── integration/
│   └── failover/
├── benchmarks/
│   ├── post_job.lua
│   └── locustfile.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## 3. Coding Standards & Architectural Rules
* **Async Everywhere:** Never use blocking I/O (`time.sleep`, synchronous `psycopg2`, synchronous `redis`) inside FastAPI routes or `asyncio` event loops. Use `asyncio.sleep`, `asyncpg`, and `redis.asyncio`.
* **Atomic Rate Limiting:** Token-bucket logic MUST execute inside a single Redis Lua script (`scripts/token_bucket.lua`), never via separate `GET` + `SET` calls in Python.
* **Stateless Workers:** Workers must not store durable state on local disk. All durable state belongs in PostgreSQL; ephemeral coordination belongs in Redis.
* **Type Safety:** Use Python type hints and Pydantic v2 models for all API inputs/outputs and internal configs.
* **Clean Code:** Keep functions modular, single-purpose, and concise. Avoid unnecessary boilerplate or dead code.

---

## 4. Error Handling & Security Requirements
* Never swallow exceptions silently; log structured errors with `job_id` and `worker_id`.
* Enforce `64 KB` max JSON payload size at the API Gateway.
* Use parameterized queries via SQLAlchemy/asyncpg (no raw string SQL interpolation).
* Never hardcode secrets, DB passwords, or API keys in source files; load exclusively from environment variables (`.env`).
* Every external call (gRPC, Redis, PostgreSQL) must have an explicit timeout.

---

## 5. Testing & Verification Expectations
* Every phase in `IMPLEMENTATION_ROADMAP.md` requires passing tests before moving to the next phase.
* Run `pytest` after every feature implementation.
* Do not mock away Redis Lua or PostgreSQL constraints in integration tests—run them against real Dockerized services.

---

## 6. Dependency Management & Git Practices
* Pin dependencies in `requirements.txt`. Do not add heavy frameworks or unnecessary packages without justification.
* Make small, phase-aligned commits with conventional messages (`feat(gateway): add redis lua token bucket`, `test(failover): add worker crash recovery test`).
* Update `README.md`, `API_DOCUMENTATION.md`, or `.env.example` whenever endpoints or environment variables change.

---

## 7. Handling Incomplete Requirements & Approval Gates
* **Ask for approval BEFORE:**
  * Switching the implementation language (e.g., FastAPI -> C++).
  * Changing the database schema in a breaking way.
  * Adding external infrastructure not listed in `TECH_STACK.md` (e.g., Kafka, RabbitMQ, Kubernetes).
  * Removing any confirmed feature (consistent hashing, priority queues, DLQ, heartbeats, Docker Compose, or benchmarks).
* If a minor implementation detail is unspecified, choose the simplest production-standard approach, document it clearly, and proceed.