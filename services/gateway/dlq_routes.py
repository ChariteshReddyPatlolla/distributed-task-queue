import time
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
import structlog
from services.common.db import get_async_session
from services.common.models import Job
from services.common.redis_client import get_redis_client
from services.gateway.schemas import DLQResponse, JobDetailResponse, JobSubmitResponse

logger = structlog.get_logger("dlq_routes")
router = APIRouter()


@router.get(
    "/v1/dlq",
    summary="List Dead-Lettered Jobs",
    response_model=DLQResponse,
    responses={
        200: {"description": "List of dead-lettered jobs with total count", "model": DLQResponse},
        401: {"description": "Missing or invalid API key"},
    },
)
async def list_dlq(
    limit: int = Query(default=20, ge=1, le=100, description="Page limit (max 100)"),
    offset: int = Query(default=0, ge=0, description="Page offset"),
    db: AsyncSession = Depends(get_async_session),
):
    # Total count of dead-lettered jobs
    count_stmt = select(func.count()).select_from(Job).where(Job.status == "DEAD_LETTERED")
    count_res = await db.execute(count_stmt)
    total = count_res.scalar_one()

    # Paginated jobs
    stmt = (
        select(Job)
        .where(Job.status == "DEAD_LETTERED")
        .order_by(Job.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    result = await db.execute(stmt)
    jobs = result.scalars().all()

    job_responses = [
        JobDetailResponse(
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
        for job in jobs
    ]

    return DLQResponse(total=total, jobs=job_responses)


@router.post(
    "/v1/dlq/{job_id}/replay",
    summary="Replay Dead-Lettered Job",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobSubmitResponse,
    responses={
        202: {"description": "Job reset and re-queued for execution", "model": JobSubmitResponse},
        400: {"description": "Job is not in DEAD_LETTERED status"},
        401: {"description": "Missing or invalid API key"},
        404: {"description": "Job not found"},
    },
)
async def replay_dlq_job(
    job_id: str,
    response: Response,
    db: AsyncSession = Depends(get_async_session),
):
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found (invalid UUID)",
        )

    job = await db.get(Job, job_uuid)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job.status != "DEAD_LETTERED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job {job_id} is in status '{job.status}', cannot replay unless DEAD_LETTERED",
        )

    # Reset job attributes
    job.status = "QUEUED"
    job.attempt = 0
    job.last_error = None
    job.assigned_worker = None
    job.completed_at = None
    job.updated_at = func.now()
    await db.flush()

    # Remove from Redis DLQ list
    redis = get_redis_client()
    await redis.lrem("queue:dlq", 0, str(job.id))

    # Push back into ready queue with priority score
    timestamp_ms = int(time.time() * 1000)
    score = float((-job.priority * 1_000_000_000_000) + timestamp_ms)
    await redis.zadd("queue:ready", {str(job.id): score})

    logger.info("dlq_job_replayed", job_id=str(job.id), priority=job.priority)

    response.status_code = status.HTTP_202_ACCEPTED
    return JobSubmitResponse(
        job_id=str(job.id),
        status="QUEUED",
        priority=job.priority,
        created_at=job.created_at,
    )
