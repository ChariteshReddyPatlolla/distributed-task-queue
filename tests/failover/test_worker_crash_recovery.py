import asyncio
import time
import uuid
import grpc
import pytest
from services.common.db import close_db, get_session_context, init_db
from services.common.models import Job
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc
from services.common.redis_client import close_redis, get_redis_client
from services.dispatcher.dispatcher import JobDispatcher
from services.worker.heartbeat import HeartbeatEmitter
from services.worker.server import WorkerServiceServicer

PORT_A = 50081
PORT_B = 50082


@pytest.mark.asyncio
async def test_worker_crash_and_automatic_recovery():
    await init_db()
    redis = get_redis_client()
    await redis.delete("queue:ready")
    await redis.delete("workers:active")

    worker_a_id = f"crash-node-A-{uuid.uuid4().hex[:4]}"
    worker_b_id = f"crash-node-B-{uuid.uuid4().hex[:4]}"

    # 1. Start Worker A
    server_a = grpc.aio.server()
    servicer_a = WorkerServiceServicer(worker_id=worker_a_id)
    task_queue_pb2_grpc.add_WorkerServiceServicer_to_server(servicer_a, server_a)
    server_a.add_insecure_port(f"127.0.0.1:{PORT_A}")
    await server_a.start()

    emitter_a = HeartbeatEmitter(worker_id=worker_a_id, grpc_host="127.0.0.1", grpc_port=PORT_A)
    await emitter_a.start()

    # 2. Start Worker B
    server_b = grpc.aio.server()
    servicer_b = WorkerServiceServicer(worker_id=worker_b_id)
    task_queue_pb2_grpc.add_WorkerServiceServicer_to_server(servicer_b, server_b)
    server_b.add_insecure_port(f"127.0.0.1:{PORT_B}")
    await server_b.start()

    emitter_b = HeartbeatEmitter(worker_id=worker_b_id, grpc_host="127.0.0.1", grpc_port=PORT_B)
    await emitter_b.start()

    # 3. Start Dispatcher with fast watchdog interval (0.5s)
    dispatcher = JobDispatcher(poll_interval_ms=50)
    dispatcher.watchdog.interval = 0.5
    await dispatcher.sync_workers()
    assert len(dispatcher.ring) == 2

    dispatcher_task = asyncio.create_task(dispatcher.run())

    # 4. Create and enqueue a sleep task
    job_id = uuid.uuid4()
    job_id_str = str(job_id)

    async with get_session_context() as session:
        job = Job(
            id=job_id,
            client_id="test-failover",
            task_type="sleep",
            payload={"duration_sec": 3.0},
            priority=8,
            status="QUEUED",
            attempt=0,
            max_retries=3,
        )
        session.add(job)

    # Push to ready queue
    now_ms = int(time.time() * 1000)
    score = float((-8 * 1_000_000_000_000) + now_ms)
    await redis.zadd("queue:ready", {job_id_str: score})

    # Wait until the job starts running on one of the workers
    assigned_worker_initial = None
    for _ in range(40):
        async with get_session_context() as session:
            j = await session.get(Job, job_id)
            if j and j.status == "RUNNING" and j.assigned_worker:
                assigned_worker_initial = j.assigned_worker
                break
        await asyncio.sleep(0.1)

    assert assigned_worker_initial in [worker_a_id, worker_b_id], "Job failed to reach RUNNING state"

    # Determine which worker got the task and simulate its sudden crash!
    kill_timestamp = time.time()
    if assigned_worker_initial == worker_a_id:
        target_server = server_a
        target_emitter = emitter_a
        killed_worker = worker_a_id
        surviving_worker = worker_b_id
    else:
        target_server = server_b
        target_emitter = emitter_b
        killed_worker = worker_b_id
        surviving_worker = worker_a_id

    # Kill target worker abruptly without clean deregistration
    await target_server.stop(grace=0)
    await target_emitter.stop()
    # Ensure heartbeat key is removed to trigger immediate watchdog detection
    await redis.delete(f"worker:heartbeat:{killed_worker}")

    # 5. Measure failover recovery time until job completes on the surviving worker
    job_completed = False
    recovery_time = 0.0

    for _ in range(60):
        async with get_session_context() as session:
            j = await session.get(Job, job_id)
            if j and j.status == "COMPLETED":
                job_completed = True
                recovery_time = time.time() - kill_timestamp
                assert j.assigned_worker == surviving_worker, (
                    f"Job completed on {j.assigned_worker}, expected surviving {surviving_worker}"
                )
                break
        await asyncio.sleep(0.2)

    assert job_completed is True, "Job did not complete after worker crash!"
    assert recovery_time <= 15.0, f"Recovery took too long: {recovery_time:.2f}s"

    # Cleanup
    dispatcher_task.cancel()
    try:
        await dispatcher_task
    except asyncio.CancelledError:
        pass
    await dispatcher.stop()

    await emitter_a.deregister()
    await emitter_b.deregister()
    await server_a.stop(grace=0.1)
    await server_b.stop(grace=0.1)

    await close_db()
    await close_redis()
