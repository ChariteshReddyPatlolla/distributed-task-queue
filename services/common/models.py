import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(128),
        unique=True,
        nullable=True
    )
    client_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True
    )
    task_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False
    )
    payload: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict
    )
    priority: Mapped[int] = mapped_column(
        SmallInteger,
        CheckConstraint("priority BETWEEN 1 AND 10", name="chk_jobs_priority"),
        nullable=False,
        default=5
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="QUEUED"
    )
    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0
    )
    max_retries: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3
    )
    assigned_worker: Mapped[Optional[str]] = mapped_column(
        String(64),
        nullable=True
    )
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    attempts: Mapped[List["JobAttempt"]] = relationship(
        "JobAttempt",
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="JobAttempt.attempt_number",
        lazy="selectin",
    )

    __table_args__ = (
        Index("idx_jobs_status_created", "status", created_at.desc()),
        Index("idx_jobs_client_id", "client_id"),
        Index("idx_jobs_dlq", created_at.desc(), postgresql_where=(status == "DEAD_LETTERED")),
    )


class JobAttempt(Base):
    __tablename__ = "job_attempts"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    attempt_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )
    worker_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True
    )
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    job: Mapped["Job"] = relationship(
        "Job",
        back_populates="attempts"
    )

    __table_args__ = (
        Index("idx_job_attempts_job_id", "job_id"),
    )
