import pytest
from services.common.proto import task_queue_pb2, task_queue_pb2_grpc


def test_task_request_serialization():
    req = task_queue_pb2.TaskRequest(
        job_id="test-job-123",
        task_type="cpu_hash",
        payload_json='{"iterations": 1000}',
        attempt=1,
        max_retries=3,
    )
    serialized = req.SerializeToString()
    deserialized = task_queue_pb2.TaskRequest()
    deserialized.ParseFromString(serialized)

    assert deserialized.job_id == "test-job-123"
    assert deserialized.task_type == "cpu_hash"
    assert deserialized.payload_json == '{"iterations": 1000}'
    assert deserialized.attempt == 1
    assert deserialized.max_retries == 3


def test_task_response_serialization():
    resp = task_queue_pb2.TaskResponse(
        job_id="test-job-123",
        success=True,
        result_json='{"hash": "abc"}',
        error_message="",
        duration_ms=42,
    )
    serialized = resp.SerializeToString()
    deserialized = task_queue_pb2.TaskResponse()
    deserialized.ParseFromString(serialized)

    assert deserialized.job_id == "test-job-123"
    assert deserialized.success is True
    assert deserialized.result_json == '{"hash": "abc"}'
    assert deserialized.duration_ms == 42


def test_health_check_serialization():
    req = task_queue_pb2.HealthRequest()
    serialized_req = req.SerializeToString()
    assert len(serialized_req) == 0

    resp = task_queue_pb2.HealthResponse(
        worker_id="worker-node-1",
        healthy=True,
        inflight_count=2,
    )
    serialized_resp = resp.SerializeToString()
    deserialized_resp = task_queue_pb2.HealthResponse()
    deserialized_resp.ParseFromString(serialized_resp)

    assert deserialized_resp.worker_id == "worker-node-1"
    assert deserialized_resp.healthy is True
    assert deserialized_resp.inflight_count == 2
