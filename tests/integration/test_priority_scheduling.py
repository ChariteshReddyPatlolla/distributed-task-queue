import asyncio
import json
import time
import uuid
import grpc
import pytest
import pytest_asyncio
from sqlalchemy import select
from services.common.db import close_db, get_session_context, init_db
from services.common.models import Job, JobAttempt
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc
from services.common.redis_client import close_redis, get_redis_client
from services.dispatcher.dispatcher import JobDispatcher
from services.worker.heartbeat import HeartbeatEmitter
from services.worker.server import WorkerServiceServicer

WORKER_PORT = 50060
WORKER_ID = f"priority-worker-{uuid.uuid4().hex[:6]}"


@pytest_asyncio.fixture
async def setup_worker_and_dispatcher():
    await init_db()
    redis = get_redis_client()
    await redis.delete("queue:ready")

    server = grpc.aio.server()
    servicer = WorkerServiceServicer(worker_id=WORKER_ID, max_concurrency=10)
    task_queue_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"127.0.0.1:{WORKER_PORT}")
    await server.start()

    emitter = HeartbeatEmitter(
        worker_id=WORKER_ID,
        grpc_host="127.0.0.1",
        grpc_port=WORKER_PORT,
    )
    await emitter.register()

    dispatcher = JobDispatcher(poll_interval_ms=10)
    await dispatcher.sync_workers()

    yield dispatcher

    await dispatcher.stop()
    await emitter.deregister()
    await server.stop(grace=0.5)
    await close_db()
    await close_redis()


@pytest.mark.asyncio
async def test_priority_10_executes_before_priority_1(setup_worker_and_dispatcher):
    dispatcher = setup_worker_and_dispatcher
    redis = get_redis_client()

    low_priority_ids = []
    high_priority_ids = []

    # 1. Enqueue 5 LOW-priority (priority = 1) jobs FIRST
    async with get_session_context() as session:
        for i in range(5):
            jid = uuid.uuid4()
            low_priority_ids.append(str(jid))
            job = Job(
                id=jid,
                client_id="test-prio-client",
                task_type="sleep",
                payload={"duration_sec": 0.05},
                priority=1,
                status="QUEUED",
                attempt=0,
                max_retries=3,
            )
            session.add(job)
            # Score: (-1 * 1e12) + timestamp
            score = float((-1 * 1_000_000_000_000) + int(time.time() * 1000) + i)
            await redis.zadd("queue:ready", {str(jid): score})

    # Small delay so timestamps advance
    await asyncio.sleep(0.01)

    # 2. Enqueue 5 HIGH-priority (priority = 10) jobs SECOND
    async with get_session_context() as session:
        for i in range(5):
            jid = uuid.uuid4()
            high_priority_ids.append(str(jid))
            job = Job(
                id=jid,
                client_id="test-prio-client",
                task_type="sleep",
                payload={"duration_sec": 0.05},
                priority=10,
                status="QUEUED",
                attempt=0,
                max_retries=3,
            )
            session.add(job)
            # Score: (-10 * 1e12) + timestamp
            score = float((-10 * 1_000_000_000_000) + int(time.time() * 1000) + i)
            await redis.zadd("queue:ready", {str(jid): score})

    # 3. Verify Redis sorted set ordering: all high-priority jobs must pop first
    popped_order = []
    for _ in range(10):
        popped = await redis.zpopmin("queue:ready", count=1)
        assert popped
        popped_order.append(popped[0][0])

    # First 5 popped must be all high-priority jobs
    assert set(popped_order[:5]) == set(high_priority_ids), "High-priority jobs were not at the front of queue:ready"
    assert set(popped_order[5:]) == set(low_priority_ids), "Low-priority jobs were not at the back of queue:ready"

    # Put them back into queue:ready to verify dispatcher execution order
    for jid in popped_order[:5]:
        await redis.zadd("queue:ready", {jid: float((-10 * 1_000_000_000_000))})
    for jid in popped_order[5:]:
        await redis.zadd("queue:ready", {jid: float((-1 * 1_000_000_000_000))})

    # 4. Run dispatcher steps sequentially and record execution order
    execution_order = []
    for _ in range(10):
        # Dispatch one job
        await dispatcher.dispatch_step()
        await asyncio.sleep(0.08)

    # Check database attempt started_at order
    all_test_ids = [uuid.UUID(x) for x in (high_priority_ids + low_priority_ids)]
    async with get_session_context() as session:
        stmt = (
            select(JobAttempt)
            .where(JobAttempt.job_id.in_(all_test_ids))
            .order_by(JobAttempt.started_at.asc())
        )
        res = await session.execute(stmt)
        attempts = res.scalars().all()
        executed_job_ids = [str(a.job_id) for a in attempts]

    # Verify that the first 5 executed jobs are all high-priority
    first_five = executed_job_ids[:5]
    assert set(first_five) == set(high_priority_ids), (
        f"Expected high-priority jobs to execute first, got: {first_five}"
    )
