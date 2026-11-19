import asyncio
import uuid
import grpc
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport
from services.common.db import close_db, get_session_context, init_db
from services.common.models import Job
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc
from services.common.redis_client import close_redis, get_redis_client
from services.dispatcher.dispatcher import JobDispatcher
from services.gateway.main import app
from services.worker.heartbeat import HeartbeatEmitter
from services.worker.server import WorkerServiceServicer

WORKER_PORTS = [50071, 50072, 50073]
WORKER_IDS = [f"e2e-worker-{i}-{uuid.uuid4().hex[:4]}" for i in range(3)]


@pytest_asyncio.fixture
async def setup_cluster():
    await init_db()
    servers = []
    emitters = []

    # Start 3 gRPC worker servers
    for port, wid in zip(WORKER_PORTS, WORKER_IDS):
        server = grpc.aio.server()
        servicer = WorkerServiceServicer(worker_id=wid, max_concurrency=8)
        task_queue_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
        server.add_insecure_port(f"127.0.0.1:{port}")
        await server.start()
        servers.append(server)

        emitter = HeartbeatEmitter(
            worker_id=wid,
            grpc_host="127.0.0.1",
            grpc_port=port,
        )
        await emitter.register()
        emitters.append(emitter)

    dispatcher = JobDispatcher(poll_interval_ms=20)
    await dispatcher.sync_workers()
    assert len(dispatcher.ring) == 3

    dispatcher_task = asyncio.create_task(dispatcher.run())

    yield dispatcher

    dispatcher_task.cancel()
    try:
        await dispatcher_task
    except asyncio.CancelledError:
        pass
    await dispatcher.stop()

    for emitter in emitters:
        await emitter.deregister()

    for server in servers:
        await server.stop(grace=0.2)

    await close_db()
    await close_redis()


@pytest.mark.asyncio
async def test_e2e_job_submission_and_completion_across_workers(setup_cluster):
    dispatcher = setup_cluster
    transport = ASGITransport(app=app)
    headers = {"X-API-Key": "test-key-1"}

    job_ids = []
    # Submit 6 jobs via API Gateway
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for i in range(6):
            res = await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "task_type": "cpu_hash",
                    "payload": {"iterations": 500, "input": f"e2e-task-{i}"},
                    "priority": 5,
                },
            )
            assert res.status_code == 202
            job_ids.append(res.json()["job_id"])

    # Wait for all jobs to reach COMPLETED status (max 10 seconds)
    completed_jobs = {}
    for _ in range(50):
        async with get_session_context() as session:
            for jid in job_ids:
                if jid not in completed_jobs:
                    job = await session.get(Job, uuid.UUID(jid))
                    if job and job.status == "COMPLETED":
                        completed_jobs[jid] = job

        if len(completed_jobs) == len(job_ids):
            break
        await asyncio.sleep(0.2)

    assert len(completed_jobs) == len(job_ids), (
        f"Only {len(completed_jobs)} of {len(job_ids)} jobs completed in time"
    )

    # Verify that workers assigned are part of the active worker pool
    assigned_workers = {job.assigned_worker for job in completed_jobs.values()}
    for w in assigned_workers:
        assert w in WORKER_IDS, f"Job was assigned to unknown worker: {w}"

    # Verify results are stored in PostgreSQL
    for jid, job in completed_jobs.items():
        assert job.result is not None
        assert "hash" in job.result
        assert job.result["iterations"] == 500
        assert job.completed_at is not None
