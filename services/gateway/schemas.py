import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator
from services.common.config import settings

TaskType = Literal["sleep", "cpu_hash", "webhook", "fail_sim"]


class JobSubmitRequest(BaseModel):
    task_type: TaskType = Field(
        ...,
        description="Type of task to execute: sleep | cpu_hash | webhook | fail_sim",
        examples=["cpu_hash"]
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary JSON payload for task execution (max 64 KB)",
        examples=[{"iterations": 50000, "input": "benchmark"}]
    )
    priority: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Job priority between 1 (lowest) and 10 (highest)",
        examples=[8]
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts on transient failure",
        examples=[3]
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Unique client-provided key to guarantee exactly-once job acceptance",
        examples=["req-2026-0001"]
    )

    @field_validator("payload")
    @classmethod
    def validate_payload_size(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        serialized = json.dumps(v)
        byte_len = len(serialized.encode("utf-8"))
        if byte_len > settings.MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"Payload size {byte_len} bytes exceeds maximum allowed limit of {settings.MAX_PAYLOAD_BYTES} bytes (64 KB)"
            )
        return v


class JobSubmitResponse(BaseModel):
    job_id: str
    status: str
    priority: int
    created_at: datetime


class JobDetailResponse(BaseModel):
    id: str
    idempotency_key: Optional[str]
    client_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: int
    status: str
    attempt: int
    max_retries: int
    assigned_worker: Optional[str]
    result: Optional[Dict[str, Any]]
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


class HealthResponse(BaseModel):
    status: str
    postgres: bool
    redis: bool
    active_workers: int


class MetricsResponse(BaseModel):
    jobs_submitted: int
    jobs_completed: int
    jobs_retried: int
    jobs_dlq: int
    active_workers: int
    rate_limited_total: int


class DLQResponse(BaseModel):
    total: int
    jobs: List[JobDetailResponse]


class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str


class ErrorResponse(BaseModel):
    error: ErrorDetail
