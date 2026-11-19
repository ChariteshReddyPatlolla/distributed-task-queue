import asyncio
from datetime import datetime, timezone
import time
import uuid
from typing import List, Optional, Set
import redis.asyncio as aioredis
import structlog
from services.common.config import settings
from services.common.db import get_session_context
from services.common.hash_ring import ConsistentHashRing
from services.common.models import Job
from services.common.redis_client import get_redis_client

logger = structlog.get_logger("watchdog")


class HeartbeatWatchdog:
    def __init__(
        self,
        redis_client: Optional[aioredis.Redis] = None,
        ring: Optional[ConsistentHashRing] = None,
        interval_sec: Optional[float] = None,
    ):
        self._redis = redis_client
        self.ring = ring
        self.interval = interval_sec or float(settings.WATCHDOG_INTERVAL_SEC)
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is not None:
            return self._redis
        return get_redis_client()

    async def check_workers_step(self) -> List[str]:
        # Get all registered worker IDs
        raw_workers = await self.redis.hgetall("workers:active")
        if not raw_workers:
            return []

        dead_workers: List[str] = []
        now_ts = datetime.now(timezone.utc).isoformat()
        now_ms = int(time.time() * 1000)

        for worker_id in raw_workers.keys():
            hb_key = f"worker:heartbeat:{worker_id}"
            is_alive = await self.redis.exists(hb_key)

            if not is_alive:
                dead_workers.append(worker_id)
                logger.error(
                    "worker_heartbeat_expired",
                    worker_id=worker_id,
                    timestamp=now_ts,
                )

                # Record failover event for benchmarking & audit
                await self.redis.hset("failover:events", worker_id, now_ts)
                # Remove from workers:active
                await self.redis.hdel("workers:active", worker_id)

                if self.ring:
                    self.ring.remove_node(worker_id)

                # Recover in-flight jobs
                inflight_key = f"worker:inflight:{worker_id}"
                in_flight_jobs: Set[str] = await self.redis.smembers(inflight_key)

                if in_flight_jobs:
                    logger.warning(
                        "recovering_in_flight_jobs",
                        worker_id=worker_id,
                        count=len(in_flight_jobs),
                        jobs=list(in_flight_jobs),
                    )

                    for job_id_str in in_flight_jobs:
                        try:
                            job_uuid = uuid.UUID(job_id_str)
                            priority = 5
                            async with get_session_context() as session:
                                job = await session.get(Job, job_uuid)
                                if job and job.status not in ["COMPLETED", "DEAD_LETTERED"]:
                                    priority = job.priority
                                    job.status = "QUEUED"
                                    job.assigned_worker = None

                            score = float((-priority * 1_000_000_000_000) + now_ms)
                            await self.redis.zadd("queue:ready", {job_id_str: score})
                            await self.redis.incr("metrics:jobs_recovered")
                            logger.info(
                                "failover_job_requeued",
                                job_id=job_id_str,
                                recovered_from=worker_id,
                            )
                        except Exception as e:
                            logger.error(
                                "failover_job_recovery_failed",
                                job_id=job_id_str,
                                error=str(e),
                            )

                    # Delete in-flight set
                    await self.redis.delete(inflight_key)

        return dead_workers

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.check_workers_step()
            except Exception as e:
                logger.error("watchdog_step_failed", error=str(e))

            await asyncio.sleep(self.interval)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("heartbeat_watchdog_started", interval_sec=self.interval)

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("heartbeat_watchdog_stopped")
