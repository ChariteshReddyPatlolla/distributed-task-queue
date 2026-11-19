import time
import uuid
import pytest
from services.common.redis_client import close_redis, get_redis_client
from services.dispatcher.retry_promoter import RetryPromoter


@pytest.mark.asyncio
async def test_retry_promoter_promotes_due_jobs():
    redis = get_redis_client()
    promoter = RetryPromoter(redis_client=redis)

    job_due_id = str(uuid.uuid4())
    job_future_id = str(uuid.uuid4())

    now_ms = int(time.time() * 1000)

    # 1. Add one job that is already due (scheduled 5 seconds in the past)
    await redis.zadd("queue:delayed", {job_due_id: now_ms - 5000})

    # 2. Add one job scheduled in the future (scheduled 60 seconds in future)
    await redis.zadd("queue:delayed", {job_future_id: now_ms + 60000})

    # Run promotion step
    promoted_count = await promoter.promote_step()
    assert promoted_count >= 1

    # Verify due job moved to queue:ready
    ready_score = await redis.zscore("queue:ready", job_due_id)
    assert ready_score is not None

    # Verify due job no longer in queue:delayed
    delayed_score_due = await redis.zscore("queue:delayed", job_due_id)
    assert delayed_score_due is None

    # Verify future job is still in queue:delayed
    delayed_score_future = await redis.zscore("queue:delayed", job_future_id)
    assert delayed_score_future is not None

    # Cleanup
    await redis.zrem("queue:ready", job_due_id)
    await redis.zrem("queue:delayed", job_future_id)
    await close_redis()
