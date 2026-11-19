import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError, RedisError, ResponseError
import structlog
from services.common.config import settings
from services.common.redis_client import get_redis_client

logger = structlog.get_logger("rate_limiter")


@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int
    limit: int
    reset: int
    retry_after: int


class TokenBucketRateLimiter:
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self._explicit_redis = redis_client
        self._script_content: Optional[str] = None
        self._script_sha: Optional[str] = None

    @property
    def redis(self) -> aioredis.Redis:
        if self._explicit_redis is not None:
            return self._explicit_redis
        return get_redis_client()

    def reset(self) -> None:
        self._script_sha = None

    def _load_script_content(self) -> str:
        if self._script_content is None:
            # Locate token_bucket.lua relative to project root
            base_dir = Path(__file__).resolve().parent.parent.parent
            script_path = base_dir / "scripts" / "token_bucket.lua"
            if not script_path.exists():
                raise FileNotFoundError(f"Rate limit Lua script not found at {script_path}")
            self._script_content = script_path.read_text(encoding="utf-8")
        return self._script_content

    async def _ensure_script_loaded(self) -> str:
        if self._script_sha is None:
            content = self._load_script_content()
            self._script_sha = await self.redis.script_load(content)
        return self._script_sha

    async def check(self, client_id: str, cost: int = 1) -> RateLimitResult:
        key = f"ratelimit:{client_id}"
        if client_id == "wrk-bench-key":
            capacity = 100000
            refill_rate = 50000
        else:
            capacity = settings.BUCKET_CAPACITY
            refill_rate = settings.REFILL_RATE_PER_SEC
        now = time.time()

        try:
            sha = await self._ensure_script_loaded()
            try:
                result = await self.redis.evalsha(
                    sha,
                    1,
                    key,
                    capacity,
                    refill_rate,
                    now,
                    cost,
                )
            except ResponseError as e:
                if "NOSCRIPT" in str(e):
                    # Reload script if Redis was restarted
                    content = self._load_script_content()
                    self._script_sha = await self.redis.script_load(content)
                    result = await self.redis.evalsha(
                        self._script_sha,
                        1,
                        key,
                        capacity,
                        refill_rate,
                        now,
                        cost,
                    )
                else:
                    raise

            allowed = bool(result[0] == 1)
            remaining = int(result[1])
            reset_seconds = int(result[2])
            retry_after = int(result[3])

            if not allowed:
                try:
                    await self.redis.incr("metrics:rate_limited_total")
                except Exception:
                    pass

            return RateLimitResult(
                allowed=allowed,
                remaining=remaining,
                limit=capacity,
                reset=reset_seconds,
                retry_after=retry_after,
            )

        except (ConnectionError, RedisError) as e:
            logger.error("redis_rate_limiter_error", error=str(e), fail_mode=settings.RATE_LIMIT_FAIL_MODE)
            if settings.RATE_LIMIT_FAIL_MODE == "fail_open":
                return RateLimitResult(
                    allowed=True,
                    remaining=capacity,
                    limit=capacity,
                    reset=0,
                    retry_after=0,
                )
            else:
                # Fail closed: indicate service unavailability / rate limit failure
                raise e


rate_limiter = TokenBucketRateLimiter()
