import uuid
import pytest
from sqlalchemy import text
from services.common.db import (
    check_db_health,
    close_db,
    get_async_session,
    init_db,
)
from services.common.models import Job, JobAttempt
from services.common.redis_client import check_redis_health, close_redis, get_redis_client


@pytest.mark.asyncio
async def test_redis_connectivity():
    health = await check_redis_health()
    assert health is True, "Redis health check failed"

    client = get_redis_client()
    test_key = f"test:ping:{uuid.uuid4()}"
    await client.set(test_key, "pong", ex=10)
    val = await client.get(test_key)
    assert val == "pong"
    await client.delete(test_key)
    await close_redis()


@pytest.mark.asyncio
async def test_postgres_connectivity_and_schema():
    # Initialize DB tables if not already present
    await init_db()

    health = await check_db_health()
    assert health is True, "PostgreSQL health check failed"

    async for session in get_async_session():
        # Verify tables exist in information_schema
        result = await session.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name IN ('jobs', 'job_attempts')"
            )
        )
        tables = {row[0] for row in result.fetchall()}
        assert "jobs" in tables, "Table 'jobs' was not found in PostgreSQL"
        assert "job_attempts" in tables, "Table 'job_attempts' was not found in PostgreSQL"

        # Verify columns in jobs table
        col_res = await session.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'jobs'"
            )
        )
        job_cols = {row[0]: row[1] for row in col_res.fetchall()}
        expected_cols = [
            "id", "idempotency_key", "client_id", "task_type",
            "payload", "priority", "status", "attempt", "max_retries",
            "assigned_worker", "result", "last_error", "created_at",
            "updated_at", "completed_at"
        ]
        for col in expected_cols:
            assert col in job_cols, f"Column '{col}' missing from 'jobs' table"

        # Test insert and query using SQLAlchemy models
        test_job_id = uuid.uuid4()
        test_job = Job(
            id=test_job_id,
            idempotency_key=f"idem-{test_job_id}",
            client_id="test-client-phase1",
            task_type="sleep",
            payload={"duration_sec": 1},
            priority=8,
            status="QUEUED",
            attempt=0,
            max_retries=3,
        )
        session.add(test_job)
        await session.flush()

        # Add an attempt
        test_attempt = JobAttempt(
            job_id=test_job_id,
            attempt_number=1,
            worker_id="worker-phase1-test",
            status="STARTED",
        )
        session.add(test_attempt)
        await session.commit()

        # Query back
        queried_job = await session.get(Job, test_job_id)
        assert queried_job is not None
        assert queried_job.client_id == "test-client-phase1"
        assert queried_job.priority == 8
        assert queried_job.status == "QUEUED"

        # Clean up test row
        await session.delete(queried_job)
        await session.commit()

    await close_db()
