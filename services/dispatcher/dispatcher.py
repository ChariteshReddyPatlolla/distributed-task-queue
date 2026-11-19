import asyncio
import json
import uuid
from typing import Dict, Optional, Set
import grpc
import redis.asyncio as aioredis
import structlog
from services.common.config import settings
from services.common.db import get_session_context
from services.common.hash_ring import ConsistentHashRing
from services.common.models import Job
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc
from services.common.redis_client import get_redis_client
from services.dispatcher.retry_promoter import RetryPromoter
from services.dispatcher.watchdog import HeartbeatWatchdog

logger = structlog.get_logger("dispatcher")


class JobDispatcher:
    def __init__(
        self,
        redis_client: Optional[aioredis.Redis] = None,
        poll_interval_ms: Optional[int] = None,
        timeout_sec: Optional[int] = None,
        ring: Optional[ConsistentHashRing] = None,
    ):
        self._redis = redis_client
        self.poll_interval = (poll_interval_ms or settings.DISPATCHER_POLL_INTERVAL_MS) / 1000.0
        self.timeout_sec = timeout_sec or settings.GRPC_TASK_TIMEOUT_SEC
        self.ring = ring or ConsistentHashRing()
        self.watchdog = HeartbeatWatchdog(redis_client=self._redis, ring=self.ring)
        self.promoter = RetryPromoter(redis_client=self._redis)
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._stubs: Dict[str, task_queue_pb2_grpc.WorkerServiceStub] = {}
        self._active_worker_addrs: Dict[str, str] = {}
        self._dispatch_tasks: Set[asyncio.Task] = set()
        self._running = False

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is not None:
            return self._redis
        return get_redis_client()

    async def sync_workers(self) -> None:
        try:
            # Query active workers registered in Redis: worker_id -> host:port
            raw_workers = await self.redis.hgetall("workers:active")
            active_workers: Dict[str, str] = {k: v for k, v in raw_workers.items()}

            current_ids = set(self._active_worker_addrs.keys())
            new_ids = set(active_workers.keys())

            # Remove departed workers
            for wid in current_ids - new_ids:
                if wid in self._channels:
                    await self._channels[wid].close()
                    del self._channels[wid]
                if wid in self._stubs:
                    del self._stubs[wid]
                self.ring.remove_node(wid)
                del self._active_worker_addrs[wid]
                logger.info("dispatcher_worker_removed", worker_id=wid)

            # Add newly discovered workers
            for wid in new_ids - current_ids:
                addr = active_workers[wid]
                channel = grpc.aio.insecure_channel(addr)
                stub = task_queue_pb2_grpc.WorkerServiceStub(channel)
                self._channels[wid] = channel
                self._stubs[wid] = stub
                self.ring.add_node(wid)
                self._active_worker_addrs[wid] = addr
                logger.info("dispatcher_worker_added", worker_id=wid, address=addr)

        except Exception as e:
            logger.warning("dispatcher_worker_sync_error", error=str(e))

    async def _execute_dispatch(self, job_id_str: str, score: float) -> None:
        try:
            job_uuid = uuid.UUID(job_id_str)
        except ValueError:
            logger.error("dispatcher_invalid_job_uuid", job_id=job_id_str)
            return

        # Fetch job metadata from PostgreSQL
        async with get_session_context() as session:
            job = await session.get(Job, job_uuid)
            if not job:
                logger.warning("dispatcher_job_not_found_in_pg", job_id=job_id_str)
                return
            if job.status not in ["QUEUED", "RETRYING", "RUNNING"]:
                logger.info("dispatcher_job_already_processed", job_id=job_id_str, status=job.status)
                return

            task_type = job.task_type
            payload_json = json.dumps(job.payload)
            attempt = job.attempt + 1
            max_retries = job.max_retries

        # Select worker via Consistent Hashing
        target_worker_id = self.ring.get_node(job_id_str)
        if not target_worker_id or target_worker_id not in self._stubs:
            # No worker available: re-enqueue to queue:ready so it is not lost
            await self.redis.zadd("queue:ready", {job_id_str: score})
            logger.warning("dispatcher_no_healthy_workers_available", job_id=job_id_str)
            return

        stub = self._stubs[target_worker_id]
        inflight_key = f"worker:inflight:{target_worker_id}"

        # Record in worker inflight set
        await self.redis.sadd(inflight_key, job_id_str)

        logger.info(
            "dispatcher_job_routed",
            job_id=job_id_str,
            worker_id=target_worker_id,
            attempt=attempt,
        )

        try:
            req = task_queue_pb2.TaskRequest(
                job_id=job_id_str,
                task_type=task_type,
                payload_json=payload_json,
                attempt=attempt,
                max_retries=max_retries,
            )
            resp: task_queue_pb2.TaskResponse = await stub.ExecuteTask(
                req,
                timeout=self.timeout_sec,
            )
            logger.info(
                "dispatcher_job_execution_response",
                job_id=job_id_str,
                worker_id=target_worker_id,
                success=resp.success,
                duration_ms=resp.duration_ms,
            )
        except grpc.RpcError as rpc_err:
            logger.error(
                "dispatcher_grpc_call_failed",
                job_id=job_id_str,
                worker_id=target_worker_id,
                error=str(rpc_err),
            )
            # Unreachable worker: immediately evict from ring, channels, and active list
            if target_worker_id in self._channels:
                try:
                    await self._channels[target_worker_id].close()
                except Exception:
                    pass
                del self._channels[target_worker_id]
            if target_worker_id in self._stubs:
                del self._stubs[target_worker_id]
            if target_worker_id in self._active_worker_addrs:
                del self._active_worker_addrs[target_worker_id]
            self.ring.remove_node(target_worker_id)
            try:
                await self.redis.hdel("workers:active", target_worker_id)
            except Exception:
                pass

            # Remove from target worker inflight
            await self.redis.srem(inflight_key, job_id_str)
            # Reset status in PostgreSQL so re-routed worker can accept it
            try:
                async with get_session_context() as session:
                    job = await session.get(Job, job_uuid)
                    if job and job.status not in ["COMPLETED", "DEAD_LETTERED"]:
                        job.status = "QUEUED"
                        job.assigned_worker = None
            except Exception:
                pass
            # Re-enqueue job to ready queue for immediate failover reassignment
            await self.redis.zadd("queue:ready", {job_id_str: score})
        except Exception as exc:
            logger.exception(
                "dispatcher_unexpected_dispatch_error",
                job_id=job_id_str,
                worker_id=target_worker_id,
                error=str(exc),
            )
            await self.redis.srem(inflight_key, job_id_str)
            await self.redis.zadd("queue:ready", {job_id_str: score})

    async def dispatch_step(self) -> bool:
        # Atomic pop highest priority job from ready queue
        popped = await self.redis.zpopmin("queue:ready", count=1)
        if not popped:
            return False

        job_id_str, score = popped[0]
        # Dispatch asynchronously so dispatcher does not block on worker completion
        task = asyncio.create_task(self._execute_dispatch(job_id_str, score))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        return True

    async def run(self) -> None:
        self._running = True
        logger.info("dispatcher_loop_started")
        await self.promoter.start()
        await self.watchdog.start()
        sync_counter = 0

        while self._running:
            # Sync active workers every ~1 second (20 * 50ms)
            sync_counter += 1
            if sync_counter >= 20:
                await self.sync_workers()
                sync_counter = 0

            dispatched = await self.dispatch_step()
            if not dispatched:
                await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        self._running = False
        logger.info("dispatcher_stopping", inflight_tasks=len(self._dispatch_tasks))
        await self.promoter.stop()
        await self.watchdog.stop()
        # Wait for currently executing dispatch calls with grace period
        if self._dispatch_tasks:
            await asyncio.gather(*self._dispatch_tasks, return_exceptions=True)
            self._dispatch_tasks.clear()

        # Close all gRPC channels
        for wid, ch in self._channels.items():
            try:
                await ch.close()
            except Exception:
                pass
        self._channels.clear()
        self._stubs.clear()
        logger.info("dispatcher_stopped_cleanly")
