import pytest
from services.worker.handlers import execute_task_handler, handle_cpu_hash, handle_fail_sim, handle_sleep


@pytest.mark.asyncio
async def test_handle_sleep():
    res = await handle_sleep({"duration_sec": 0.05})
    assert res["status"] == "completed"
    assert res["slept_sec"] == 0.05


@pytest.mark.asyncio
async def test_handle_cpu_hash():
    res = await handle_cpu_hash({"iterations": 100, "input": "test-input"})
    assert "hash" in res
    assert res["iterations"] == 100
    assert len(res["hash"]) == 64  # SHA256 hex length


@pytest.mark.asyncio
async def test_handle_fail_sim():
    # When should_fail is True (default) -> raises RuntimeError
    with pytest.raises(RuntimeError) as excinfo:
        await handle_fail_sim({"error_message": "custom failure"})
    assert "custom failure" in str(excinfo.value)

    # When should_fail is False -> returns success
    res = await handle_fail_sim({"should_fail": False})
    assert res["status"] == "simulated_success"


@pytest.mark.asyncio
async def test_unknown_task_type():
    with pytest.raises(ValueError) as excinfo:
        await execute_task_handler("unknown_computation", {})
    assert "Unknown task type" in str(excinfo.value)
