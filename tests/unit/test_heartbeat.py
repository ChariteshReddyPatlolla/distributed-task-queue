import uuid
import pytest
from services.common.redis_client import close_redis, get_redis_client
from services.worker.heartbeat import HeartbeatEmitter


@pytest.mark.asyncio
async def test_heartbeat_emitter_lifecycle():
    redis = get_redis_client()
    worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
    emitter = HeartbeatEmitter(
        worker_id=worker_id,
        grpc_host="127.0.0.1",
        grpc_port=50051,
        redis_client=redis,
        interval_sec=1,
        ttl_sec=5,
    )

    # 1. Register
    await emitter.register()

    # Verify registered in workers:active hash
    addr = await redis.hget("workers:active", worker_id)
    assert addr == "127.0.0.1:50051"

    # Verify heartbeat key exists and has TTL
    hb_val = await redis.get(emitter.heartbeat_key)
    assert hb_val == "alive"
    ttl = await redis.ttl(emitter.heartbeat_key)
    assert 0 < ttl <= 5

    # 2. Emit once
    res = await emitter.emit_once()
    assert res is True

    # 3. Deregister
    await emitter.deregister()
    addr_after = await redis.hget("workers:active", worker_id)
    assert addr_after is None
    hb_after = await redis.get(emitter.heartbeat_key)
    assert hb_after is None

    await close_redis()
