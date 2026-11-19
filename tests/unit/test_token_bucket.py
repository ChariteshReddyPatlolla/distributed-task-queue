import time
import uuid
import pytest
from services.common.config import settings
from services.common.redis_client import close_redis, get_redis_client
from services.gateway.rate_limiter import TokenBucketRateLimiter


@pytest.mark.asyncio
async def test_token_bucket_lua_burst_and_rejection():
    redis = get_redis_client()
    limiter = TokenBucketRateLimiter(redis)
    test_client = f"client-test-{uuid.uuid4().hex[:8]}"

    # Clean initial state
    await redis.delete(f"ratelimit:{test_client}")

    # Initial check should allow burst up to capacity
    res = await limiter.check(test_client, cost=1)
    assert res.allowed is True
    assert res.limit == settings.BUCKET_CAPACITY
    assert res.remaining == settings.BUCKET_CAPACITY - 1

    # Consume all remaining tokens
    res_burst = await limiter.check(test_client, cost=res.remaining)
    assert res_burst.allowed is True
    assert res_burst.remaining == 0

    # Next attempt should be denied with 429 indicator
    res_denied = await limiter.check(test_client, cost=1)
    assert res_denied.allowed is False
    assert res_denied.remaining == 0
    assert res_denied.retry_after >= 1

    # Cleanup
    await redis.delete(f"ratelimit:{test_client}")
    await close_redis()


@pytest.mark.asyncio
async def test_token_bucket_refill():
    redis = get_redis_client()
    limiter = TokenBucketRateLimiter(redis)
    test_client = f"client-refill-{uuid.uuid4().hex[:8]}"
    key = f"ratelimit:{test_client}"

    await redis.delete(key)

    # Empty the bucket
    await limiter.check(test_client, cost=settings.BUCKET_CAPACITY)

    # Simulate elapsed time by directly modifying the last_refill timestamp in Redis
    # Shift last_refill 1.0 second into the past
    now = time.time()
    await redis.hset(key, mapping={"tokens": 0, "last_refill": now - 1.0})

    # Checking should now find refill_rate tokens (e.g. 20 tokens)
    res = await limiter.check(test_client, cost=1)
    assert res.allowed is True
    # At least 18 tokens should remain after consuming 1 token
    assert res.remaining >= 18

    # Cleanup
    await redis.delete(key)
    await close_redis()
