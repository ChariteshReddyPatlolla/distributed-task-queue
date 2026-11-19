import asyncio
import json
import os
import random
import socket
import sys
import time
import uuid
from typing import Optional
import grpc
from sqlalchemy import select, update
from sqlalchemy.sql import func
import structlog
from services.common.config import settings
from services.common.db import close_db, get_session_context, init_db
from services.common.logging import setup_logging
from services.common.models import Job, JobAttempt
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc
from services.common.redis_client import close_redis, get_redis_client
from services.worker.handlers import execute_task_handler
from services.worker.heartbeat import HeartbeatEmitter

logger = setup_logging("worker")


class WorkerServiceServicer(task_queue_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_id: str, max_concurrency: Optional[int] = None):
        self.worker_id = worker_id
        concurrency = max_concurrency or settings.WORKER_MAX_CONCURRENCY
        self._concurrency_semaphore = asyncio.Semaphore(concurrency)
        self._inflight_count = 0

    @property
    def inflight_key(self) -> str:
        return f"worker:inflight:{self.worker_id}"

    async def ExecuteTask(
        self,
        request: task_queue_pb2.TaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> task_queue_pb2.TaskResponse:
        job_id_str = request.job_id
        task_type = request.task_type
        attempt = request.attempt
        max_retries = request.max_retries

        try:
            job_uuid = uuid.UUID(job_id_str)
        except ValueError:
            return task_queue_pb2.TaskResponse(
                job_id=job_id_str,
                success=False,
                error_message=f"Invalid UUID: {job_id_str}",
                duration_ms=0,
            )

        try:
            payload = json.loads(request.payload_json or "{}")
        except Exception as e:
            payload = {}

        redis = get_redis_client()
        start_time = time.time()

        async with self._concurrency_semaphore:
            self._inflight_count += 1
            # Track job in Redis in-flight set
            await redis.sadd(self.inflight_key, job_id_str)

            # Record attempt start in PostgreSQL
            attempt_record = None
            async with get_session_context() as session:
                job = await session.get(Job, job_uuid)
                if job:
                    job.status = "RUNNING"
                    job.attempt = attempt
                    job.assigned_worker = self.worker_id
                    job.updated_at = func.now()
                
                attempt_record = JobAttempt(
                    job_id=job_uuid,
                    attempt_number=attempt,
                    worker_id=self.worker_id,
                    status="STARTED",
                )
                session.add(attempt_record)

            logger.info(
                "task_execution_started",
                job_id=job_id_str,
                task_type=task_type,
                attempt=attempt,
                worker_id=self.worker_id,
            )

            # Execute the pluggable handler
            try:
                result = await execute_task_handler(task_type, payload)
                duration_ms = int((time.time() - start_time) * 1000)

                # Update PostgreSQL: Job COMPLETED & Attempt SUCCEEDED
                async with get_session_context() as session:
                    job = await session.get(Job, job_uuid)
                    if job:
                        job.status = "COMPLETED"
                        job.result = result
                        job.last_error = None
                        job.completed_at = func.now()
                        job.updated_at = func.now()

                    stmt = (
                        update(JobAttempt)
                        .where(
                            JobAttempt.job_id == job_uuid,
                            JobAttempt.attempt_number == attempt,
                            JobAttempt.worker_id == self.worker_id,
                        )
                        .values(
                            status="SUCCEEDED",
                            duration_ms=duration_ms,
                            finished_at=func.now(),
                        )
                    )
                    await session.execute(stmt)

                # Clear from Redis in-flight & increment completed metrics
                await redis.srem(self.inflight_key, job_id_str)
                await redis.incr("metrics:jobs_completed")

                logger.info(
                    "task_execution_succeeded",
                    job_id=job_id_str,
                    task_type=task_type,
                    duration_ms=duration_ms,
                    worker_id=self.worker_id,
                )

                self._inflight_count -= 1
                return task_queue_pb2.TaskResponse(
                    job_id=job_id_str,
                    success=True,
                    result_json=json.dumps(result),
                    error_message="",
                    duration_ms=duration_ms,
                )

            except Exception as exc:
                duration_ms = int((time.time() - start_time) * 1000)
                error_msg = str(exc)
                logger.error(
                    "task_execution_failed",
                    job_id=job_id_str,
                    task_type=task_type,
                    attempt=attempt,
                    error=error_msg,
                    duration_ms=duration_ms,
                    worker_id=self.worker_id,
                )

                # Determine retry vs DLQ
                async with get_session_context() as session:
                    job = await session.get(Job, job_uuid)
                    if job:
                        job.last_error = error_msg
                        job.updated_at = func.now()

                        if attempt < max_retries:
                            job.status = "RETRYING"
                            # Exponential backoff with jitter: min(max_delay, base * 2^attempt) + jitter
                            base_delay = settings.BASE_BACKOFF_SEC
                            max_delay = settings.MAX_BACKOFF_SEC
                            calc_delay = min(max_delay, base_delay * (2 ** attempt))
                            jitter = random.uniform(0.0, 1.0)
                            delay = calc_delay + jitter
                            next_retry_ms = int((time.time() + delay) * 1000)

                            await redis.zadd("queue:delayed", {job_id_str: next_retry_ms})
                            await redis.incr("metrics:jobs_retried")
                            logger.info(
                                "task_scheduled_for_retry",
                                job_id=job_id_str,
                                attempt=attempt,
                                next_retry_in_sec=round(delay, 2),
                            )
                        else:
                            job.status = "DEAD_LETTERED"
                            await redis.lpush("queue:dlq", job_id_str)
                            logger.warning(
                                "task_sent_to_dlq",
                                job_id=job_id_str,
                                total_attempts=attempt,
                                error=error_msg,
                            )

                    stmt = (
                        update(JobAttempt)
                        .where(
                            JobAttempt.job_id == job_uuid,
                            JobAttempt.attempt_number == attempt,
                            JobAttempt.worker_id == self.worker_id,
                        )
                        .values(
                            status="FAILED",
                            error_message=error_msg,
                            duration_ms=duration_ms,
                            finished_at=func.now(),
                        )
                    )
                    await session.execute(stmt)

                # Clear from Redis in-flight
                await redis.srem(self.inflight_key, job_id_str)
                self._inflight_count -= 1

                return task_queue_pb2.TaskResponse(
                    job_id=job_id_str,
                    success=False,
                    result_json="",
                    error_message=error_msg,
                    duration_ms=duration_ms,
                )

    async def HealthCheck(
        self,
        request: task_queue_pb2.HealthRequest,
        context: grpc.aio.ServicerContext,
    ) -> task_queue_pb2.HealthResponse:
        return task_queue_pb2.HealthResponse(
            worker_id=self.worker_id,
            healthy=True,
            inflight_count=self._inflight_count,
        )


async def serve():
    # Derive worker identity and network address
    worker_id = os.environ.get("WORKER_ID") or f"worker-{uuid.uuid4().hex[:8]}"
    port = int(os.environ.get("WORKER_GRPC_PORT", settings.WORKER_GRPC_PORT))
    
    # In Docker, HOSTNAME is the container hostname resolvable by other containers
    # For local/testing, fallback to 127.0.0.1
    host = os.environ.get("HOSTNAME") or "127.0.0.1"

    logger.info("worker_starting", worker_id=worker_id, host=host, port=port)

    await init_db()

    # Create gRPC async server
    server = grpc.aio.server()
    servicer = WorkerServiceServicer(worker_id=worker_id)
    task_queue_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)

    listen_addr = f"0.0.0.0:{port}"
    server.add_insecure_port(listen_addr)

    # Start Heartbeat Emitter
    heartbeat = HeartbeatEmitter(
        worker_id=worker_id,
        grpc_host=host,
        grpc_port=port,
    )
    await heartbeat.start()

    # Start gRPC Server
    await server.start()
    logger.info("worker_grpc_server_listening", address=listen_addr)

    # Handle graceful termination
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("worker_received_termination_signal")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

    try:
        if sys.platform == "win32":
            # On Windows loop.add_signal_handler is not supported; wait on server termination or event
            await server.wait_for_termination()
        else:
            await stop_event.wait()
    finally:
        logger.info("worker_shutting_down_cleanly", worker_id=worker_id)
        await heartbeat.stop()
        await server.stop(grace=5.0)
        await close_db()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(serve())
