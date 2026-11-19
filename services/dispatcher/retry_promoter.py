import asyncio
import time
import uuid
from typing import List, Optional
import redis.asyncio as aioredis
from sqlalchemy import select
import structlog
from services.common.config import settings
from services.common.db import get_session_context
from services.common.models import Job
from services.common.redis_client import get_redis_client

logger = structlog.get_logger("retry_promoter")


class RetryPromoter:
    def __init__(
        self,
        redis_client: Optional[aioredis.Redis] = None,
        poll_interval_sec: float = 0.2,
    ):
        self._redis = redis_client
        self.poll_interval = poll_interval_sec
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is not None:
            return self._redis
        return get_redis_client()

    async def promote_step(self, batch_size: int = 50) -> int:
        now_ms = int(time.time() * 1000)
        # Fetch due jobs from queue:delayed with score <= now_ms
        due_job_ids: List[str] = await self.redis.zrangebyscore(
            "queue:delayed",
            "-inf",
            now_ms,
            start=0,
            num=batch_size,
        )

        if not due_job_ids:
            return 0

        promoted_count = 0
        for job_id_str in due_job_ids:
            # Atomically remove from delayed queue
            removed = await self.redis.zrem("queue:delayed", job_id_str)
            if not removed:
                continue

            priority = 5
            try:
                job_uuid = uuid.UUID(job_id_str)
                async with get_session_context() as session:
                    job = await session.get(Job, job_uuid)
                    if job:
                        priority = job.priority
                        job.status = "QUEUED"
            except Exception as e:
                logger.warning("promoter_pg_lookup_failed", job_id=job_id_str, error=str(e))

            # Push to queue:ready with correct priority score
            score = float((-priority * 1_000_000_000_000) + now_ms)
            await self.redis.zadd("queue:ready", {job_id_str: score})
            promoted_count += 1
            logger.info("retry_promoted_to_ready", job_id=job_id_str, priority=priority)

        return promoted_count

    async def _loop(self) -> None:
        while self._running:
            try:
                promoted = await self.promote_step()
                if promoted == 0:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error("promoter_loop_error", error=str(e))
                await asyncio.sleep(self.poll_interval)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("retry_promoter_started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("retry_promoter_stopped")
