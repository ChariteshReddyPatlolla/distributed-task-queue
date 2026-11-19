import pytest
from pydantic import ValidationError
from services.gateway.schemas import JobSubmitRequest


def test_valid_job_submit_request():
    req = JobSubmitRequest(
        task_type="cpu_hash",
        payload={"iterations": 5000},
        priority=8,
        max_retries=3,
        idempotency_key="req-test-1",
    )
    assert req.task_type == "cpu_hash"
    assert req.priority == 8
    assert req.max_retries == 3
    assert req.idempotency_key == "req-test-1"


def test_invalid_priority():
    with pytest.raises(ValidationError):
        JobSubmitRequest(task_type="sleep", priority=0)

    with pytest.raises(ValidationError):
        JobSubmitRequest(task_type="sleep", priority=11)


def test_invalid_task_type():
    with pytest.raises(ValidationError):
        JobSubmitRequest(task_type="unknown_task_type", priority=5)


def test_oversized_payload_rejected():
    # 65536 is 64 KB, 70,000 bytes will exceed it
    large_payload = {"data": "x" * 70000}
    with pytest.raises(ValidationError) as excinfo:
        JobSubmitRequest(task_type="sleep", payload=large_payload)
    assert "exceeds maximum allowed limit" in str(excinfo.value)
