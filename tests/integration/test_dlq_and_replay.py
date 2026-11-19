import uuid
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport
from services.common.db import close_db, get_session_context, init_db
from services.common.models import Job
from services.common.redis_client import close_redis, get_redis_client
from services.gateway.main import app

VALID_API_KEY = "test-key-1"


@pytest_asyncio.fixture(autouse=True)
async def setup_teardown():
    await init_db()
    yield
    await close_db()
    await close_redis()


@pytest.mark.asyncio
async def test_dlq_inspection_and_replay_lifecycle():
    transport = ASGITransport(app=app)
    headers = {"X-API-Key": VALID_API_KEY}
    redis = get_redis_client()

    job_id = uuid.uuid4()
    job_id_str = str(job_id)

    # 1. Seed a DEAD_LETTERED job in PostgreSQL and Redis DLQ
    async with get_session_context() as session:
        job = Job(
            id=job_id,
            client_id="test-dlq-client",
            task_type="fail_sim",
            payload={"error_message": "Poison pill payload"},
            priority=8,
            status="DEAD_LETTERED",
            attempt=3,
            max_retries=3,
            last_error="Poison pill payload",
        )
        session.add(job)

    await redis.lpush("queue:dlq", job_id_str)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # 2. Inspect DLQ via GET /v1/dlq
        res_list = await client.get("/v1/dlq?limit=10&offset=0", headers=headers)
        assert res_list.status_code == 200
        dlq_data = res_list.json()
        assert dlq_data["total"] >= 1
        listed_ids = [j["id"] for j in dlq_data["jobs"]]
        assert job_id_str in listed_ids

        # 3. Replay the dead-lettered job via POST /v1/dlq/{id}/replay
        res_replay = await client.post(f"/v1/dlq/{job_id_str}/replay", headers=headers)
        assert res_replay.status_code == 202
        replay_body = res_replay.json()
        assert replay_body["job_id"] == job_id_str
        assert replay_body["status"] == "QUEUED"
        assert replay_body["priority"] == 8

        # 4. Verify job is removed from Redis queue:dlq
        dlq_items = await redis.lrange("queue:dlq", 0, -1)
        assert job_id_str not in dlq_items

        # 5. Verify job is back in Redis queue:ready with priority 8 score
        ready_score = await redis.zscore("queue:ready", job_id_str)
        assert ready_score is not None

        # 6. Verify PostgreSQL status reset to QUEUED and attempt reset to 0
        async with get_session_context() as session:
            replayed_job = await session.get(Job, job_id)
            assert replayed_job.status == "QUEUED"
            assert replayed_job.attempt == 0
            assert replayed_job.last_error is None

        # 7. Attempting to replay a job that is already QUEUED should return 400
        res_invalid_replay = await client.post(f"/v1/dlq/{job_id_str}/replay", headers=headers)
        assert res_invalid_replay.status_code == 400
        assert "cannot replay unless DEAD_LETTERED" in res_invalid_replay.json()["error"]["message"]

    # Cleanup ready key
    await redis.zrem("queue:ready", job_id_str)
