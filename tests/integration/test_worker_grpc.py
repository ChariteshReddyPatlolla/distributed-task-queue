import asyncio
import json
import uuid
import grpc
import pytest
import pytest_asyncio
from sqlalchemy import select
from services.common.db import close_db, get_session_context, init_db
from services.common.models import Job, JobAttempt
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc
from services.common.redis_client import close_redis, get_redis_client
from services.worker.server import WorkerServiceServicer

TEST_PORT = 50055
TEST_WORKER_ID = f"test-worker-{uuid.uuid4().hex[:6]}"


@pytest_asyncio.fixture
async def grpc_worker_server():
    await init_db()
    server = grpc.aio.server()
    servicer = WorkerServiceServicer(worker_id=TEST_WORKER_ID, max_concurrency=4)
    task_queue_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"127.0.0.1:{TEST_PORT}")
    await server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{TEST_PORT}")
    stub = task_queue_pb2_grpc.WorkerServiceStub(channel)

    yield stub, servicer

    await channel.close()
    await server.stop(grace=0.5)
    await close_db()
    await close_redis()


@pytest.mark.asyncio
async def test_worker_grpc_health_check(grpc_worker_server):
    stub, servicer = grpc_worker_server
    response = await stub.HealthCheck(task_queue_pb2.HealthRequest())
    assert response.healthy is True
    assert response.worker_id == TEST_WORKER_ID
    assert response.inflight_count == 0


@pytest.mark.asyncio
async def test_worker_grpc_execute_task_success(grpc_worker_server):
    stub, servicer = grpc_worker_server
    job_id = uuid.uuid4()
    job_id_str = str(job_id)

    # 1. Create initial job in PostgreSQL
    async with get_session_context() as session:
        job = Job(
            id=job_id,
            client_id="test-client",
            task_type="cpu_hash",
            payload={"iterations": 500, "input": "test-val"},
            priority=8,
            status="QUEUED",
            attempt=0,
            max_retries=3,
        )
        session.add(job)

    # 2. Invoke gRPC ExecuteTask
    request = task_queue_pb2.TaskRequest(
        job_id=job_id_str,
        task_type="cpu_hash",
        payload_json=json.dumps({"iterations": 500, "input": "test-val"}),
        attempt=1,
        max_retries=3,
    )
    response = await stub.ExecuteTask(request)
    assert response.success is True
    assert response.job_id == job_id_str
    assert response.duration_ms >= 0
    res_data = json.loads(response.result_json)
    assert "hash" in res_data
    assert res_data["iterations"] == 500

    # 3. Verify PostgreSQL state
    async with get_session_context() as session:
        updated_job = await session.get(Job, job_id)
        assert updated_job.status == "COMPLETED"
        assert updated_job.result["iterations"] == 500
        assert updated_job.completed_at is not None
        assert updated_job.assigned_worker == TEST_WORKER_ID

        # Verify JobAttempt
        stmt = select(JobAttempt).where(JobAttempt.job_id == job_id)
        res = await session.execute(stmt)
        attempts = res.scalars().all()
        assert len(attempts) == 1
        attempt = attempts[0]
        assert attempt.attempt_number == 1
        assert attempt.status == "SUCCEEDED"
        assert attempt.duration_ms is not None

    # 4. Verify Redis in-flight set is cleared
    redis = get_redis_client()
    inflight = await redis.sismember(f"worker:inflight:{TEST_WORKER_ID}", job_id_str)
    assert not inflight


@pytest.mark.asyncio
async def test_worker_grpc_execute_task_transient_failure_and_retry(grpc_worker_server):
    stub, servicer = grpc_worker_server
    job_id = uuid.uuid4()
    job_id_str = str(job_id)

    # 1. Create initial job in PostgreSQL
    async with get_session_context() as session:
        job = Job(
            id=job_id,
            client_id="test-client",
            task_type="fail_sim",
            payload={"error_message": "Transient network glitch"},
            priority=5,
            status="QUEUED",
            attempt=0,
            max_retries=3,
        )
        session.add(job)

    # 2. Invoke gRPC ExecuteTask with attempt=1 (< max_retries=3)
    request = task_queue_pb2.TaskRequest(
        job_id=job_id_str,
        task_type="fail_sim",
        payload_json=json.dumps({"error_message": "Transient network glitch"}),
        attempt=1,
        max_retries=3,
    )
    response = await stub.ExecuteTask(request)
    assert response.success is False
    assert "Transient network glitch" in response.error_message

    # 3. Verify PostgreSQL status is RETRYING
    async with get_session_context() as session:
        updated_job = await session.get(Job, job_id)
        assert updated_job.status == "RETRYING"
        assert updated_job.last_error == "Transient network glitch"

        # Verify attempt record
        stmt = select(JobAttempt).where(JobAttempt.job_id == job_id)
        res = await session.execute(stmt)
        attempts = res.scalars().all()
        assert len(attempts) == 1
        attempt = attempts[0]
        assert attempt.status == "FAILED"
        assert attempt.error_message == "Transient network glitch"

    # 4. Verify job is scheduled in Redis queue:delayed
    redis = get_redis_client()
    score = await redis.zscore("queue:delayed", job_id_str)
    assert score is not None
    # Cleanup delayed key
    await redis.zrem("queue:delayed", job_id_str)


@pytest.mark.asyncio
async def test_worker_grpc_execute_task_max_retries_and_dlq(grpc_worker_server):
    stub, servicer = grpc_worker_server
    job_id = uuid.uuid4()
    job_id_str = str(job_id)

    # 1. Create initial job in PostgreSQL
    async with get_session_context() as session:
        job = Job(
            id=job_id,
            client_id="test-client",
            task_type="fail_sim",
            payload={"error_message": "Fatal non-retryable error"},
            priority=5,
            status="RETRYING",
            attempt=2,
            max_retries=3,
        )
        session.add(job)

    # 2. Invoke gRPC ExecuteTask with attempt=3 (>= max_retries=3)
    request = task_queue_pb2.TaskRequest(
        job_id=job_id_str,
        task_type="fail_sim",
        payload_json=json.dumps({"error_message": "Fatal non-retryable error"}),
        attempt=3,
        max_retries=3,
    )
    response = await stub.ExecuteTask(request)
    assert response.success is False

    # 3. Verify PostgreSQL status is DEAD_LETTERED
    async with get_session_context() as session:
        updated_job = await session.get(Job, job_id)
        assert updated_job.status == "DEAD_LETTERED"
        assert updated_job.last_error == "Fatal non-retryable error"

    # 4. Verify job is pushed to Redis queue:dlq
    redis = get_redis_client()
    dlq_items = await redis.lrange("queue:dlq", 0, -1)
    assert job_id_str in dlq_items

    # Cleanup DLQ item
    await redis.lrem("queue:dlq", 0, job_id_str)
