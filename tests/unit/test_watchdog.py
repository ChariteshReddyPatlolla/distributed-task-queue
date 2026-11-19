import uuid
import pytest
from services.common.db import close_db, get_session_context, init_db
from services.common.hash_ring import ConsistentHashRing
from services.common.models import Job
from services.common.redis_client import close_redis, get_redis_client
from services.dispatcher.watchdog import HeartbeatWatchdog


@pytest.mark.asyncio
async def test_watchdog_detects_dead_worker_and_recovers_jobs():
    await init_db()
    redis = get_redis_client()
    ring = ConsistentHashRing()

    dead_worker_id = f"dead-worker-{uuid.uuid4().hex[:6]}"
    alive_worker_id = f"alive-worker-{uuid.uuid4().hex[:6]}"

    # Register both workers in ring and Redis
    ring.add_node(dead_worker_id)
    ring.add_node(alive_worker_id)

    await redis.hset("workers:active", dead_worker_id, "127.0.0.1:50091")
    await redis.hset("workers:active", alive_worker_id, "127.0.0.1:50092")

    # Alive worker has heartbeat key
    await redis.set(f"worker:heartbeat:{alive_worker_id}", "alive", ex=10)
    # Dead worker has NO heartbeat key (expired)

    # In-flight job on dead worker
    stranded_job_id = uuid.uuid4()
    stranded_job_id_str = str(stranded_job_id)
    async with get_session_context() as session:
        job = Job(
            id=stranded_job_id,
            client_id="test-watchdog",
            task_type="sleep",
            payload={"duration_sec": 1},
            priority=8,
            status="RUNNING",
            assigned_worker=dead_worker_id,
        )
        session.add(job)

    await redis.sadd(f"worker:inflight:{dead_worker_id}", stranded_job_id_str)

    watchdog = HeartbeatWatchdog(redis_client=redis, ring=ring)

    # Run check
    dead_detected = await watchdog.check_workers_step()
    assert dead_worker_id in dead_detected
    assert alive_worker_id not in dead_detected

    # Verify dead worker removed from ring and Redis
    assert dead_worker_id not in ring.get_nodes()
    assert await redis.hget("workers:active", dead_worker_id) is None

    # Verify stranded job was re-enqueued to queue:ready
    ready_score = await redis.zscore("queue:ready", stranded_job_id_str)
    assert ready_score is not None

    # Verify failover event was recorded
    failover_event = await redis.hget("failover:events", dead_worker_id)
    assert failover_event is not None

    # Cleanup
    await redis.zrem("queue:ready", stranded_job_id_str)
    await redis.delete(f"worker:heartbeat:{alive_worker_id}")
    await redis.hdel("workers:active", alive_worker_id)
    await redis.hdel("failover:events", dead_worker_id)
    await close_db()
    await close_redis()
