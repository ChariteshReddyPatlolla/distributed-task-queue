import time
import uuid
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Header, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import structlog
from services.common.config import settings
from services.common.db import check_db_health, get_async_session
from services.common.models import Job
from services.common.redis_client import check_redis_health, get_redis_client
from services.gateway.schemas import (
    HealthResponse,
    JobDetailResponse,
    JobSubmitRequest,
    JobSubmitResponse,
    MetricsResponse,
)

logger = structlog.get_logger("gateway_routes")
router = APIRouter()


@router.post(
    "/v1/jobs",
    summary="Submit Asynchronous Job",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobSubmitResponse,
    responses={
        202: {"description": "Job queued for asynchronous execution", "model": JobSubmitResponse},
        200: {"description": "Duplicate idempotency key; returns existing job", "model": JobSubmitResponse},
        401: {"description": "Missing or invalid API key"},
        422: {"description": "Invalid payload, unknown task type, or priority out of range (1-10)"},
        429: {"description": "Rate limit exceeded (token bucket exhausted)"},
    },
)
async def submit_job(
    request: Request,
    response: Response,
    body: JobSubmitRequest,
    db: AsyncSession = Depends(get_async_session),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    client_id = x_api_key or (request.client.host if request.client else "unknown")
    req_id = getattr(request.state, "request_id", f"req_{uuid.uuid4().hex[:8]}")

    # Check for existing idempotency key
    if body.idempotency_key:
        stmt = select(Job).where(Job.idempotency_key == body.idempotency_key)
        result = await db.execute(stmt)
        existing_job = result.scalars().first()
        if existing_job:
            logger.info(
                "job_submission_idempotent_hit",
                job_id=str(existing_job.id),
                idempotency_key=body.idempotency_key,
                client_id=client_id,
            )
            response.status_code = status.HTTP_200_OK
            return JobSubmitResponse(
                job_id=str(existing_job.id),
                status=existing_job.status,
                priority=existing_job.priority,
                created_at=existing_job.created_at,
            )

    # Generate new job ID
    job_id = uuid.uuid4()
    new_job = Job(
        id=job_id,
        idempotency_key=body.idempotency_key,
        client_id=client_id,
        task_type=body.task_type,
        payload=body.payload,
        priority=body.priority,
        status="QUEUED",
        attempt=0,
        max_retries=body.max_retries,
    )
    db.add(new_job)
    await db.flush()

    # Redis priority queue score: (-priority * 1e12) + timestamp_ms
    timestamp_ms = int(time.time() * 1000)
    score = float((-body.priority * 1_000_000_000_000) + timestamp_ms)

    redis = get_redis_client()
    # Atomic push to ready queue
    await redis.zadd("queue:ready", {str(job_id): score})
    await redis.incr("metrics:jobs_submitted")

    logger.info(
        "job_queued",
        job_id=str(job_id),
        task_type=body.task_type,
        priority=body.priority,
        client_id=client_id,
    )

    response.status_code = status.HTTP_202_ACCEPTED
    return JobSubmitResponse(
        job_id=str(job_id),
        status="QUEUED",
        priority=body.priority,
        created_at=new_job.created_at,
    )


@router.get(
    "/v1/jobs/{job_id}",
    summary="Get Job Status & Result",
    response_model=JobDetailResponse,
    responses={
        200: {"description": "Full job lifecycle record", "model": JobDetailResponse},
        401: {"description": "Missing or invalid API key"},
        404: {"description": "Job not found"},
    },
)
async def get_job(
    job_id: str,
    request: Request,
    db: AsyncSession = Depends(get_async_session),
):
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found (invalid UUID format)",
        )

    job = await db.get(Job, job_uuid)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    return JobDetailResponse(
        id=str(job.id),
        idempotency_key=job.idempotency_key,
        client_id=job.client_id,
        task_type=job.task_type,
        payload=job.payload,
        priority=job.priority,
        status=job.status,
        attempt=job.attempt,
        max_retries=job.max_retries,
        assigned_worker=job.assigned_worker,
        result=job.result,
        last_error=job.last_error,
        created_at=job.created_at,
        updated_at=job.updated_at,
        completed_at=job.completed_at,
    )


@router.get(
    "/health",
    summary="Cluster Health Check",
    response_model=HealthResponse,
)
async def health_check():
    pg_healthy = await check_db_health()
    redis_healthy = await check_redis_health()
    
    active_workers = 0
    if redis_healthy:
        try:
            redis = get_redis_client()
            active_workers = await redis.hlen("workers:active")
        except Exception:
            active_workers = 0

    all_healthy = pg_healthy and redis_healthy
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        postgres=pg_healthy,
        redis=redis_healthy,
        active_workers=active_workers,
    )


@router.get(
    "/metrics",
    summary="Cluster Metrics",
    response_model=MetricsResponse,
)
async def metrics():
    redis = get_redis_client()
    try:
        pipe = redis.pipeline()
        pipe.get("metrics:jobs_submitted")
        pipe.get("metrics:jobs_completed")
        pipe.get("metrics:jobs_retried")
        pipe.llen("queue:dlq")
        pipe.hlen("workers:active")
        pipe.get("metrics:rate_limited_total")
        results = await pipe.execute()

        jobs_submitted = int(results[0] or 0)
        jobs_completed = int(results[1] or 0)
        jobs_retried = int(results[2] or 0)
        jobs_dlq = int(results[3] or 0)
        active_workers = int(results[4] or 0)
        rate_limited_total = int(results[5] or 0)
    except Exception as e:
        logger.error("metrics_retrieval_failed", error=str(e))
        jobs_submitted = 0
        jobs_completed = 0
        jobs_retried = 0
        jobs_dlq = 0
        active_workers = 0
        rate_limited_total = 0

    return MetricsResponse(
        jobs_submitted=jobs_submitted,
        jobs_completed=jobs_completed,
        jobs_retried=jobs_retried,
        jobs_dlq=jobs_dlq,
        active_workers=active_workers,
        rate_limited_total=rate_limited_total,
    )
