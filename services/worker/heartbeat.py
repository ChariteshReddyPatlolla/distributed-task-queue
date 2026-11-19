import asyncio
from typing import Optional
import redis.asyncio as aioredis
import structlog
from services.common.config import settings
from services.common.redis_client import get_redis_client

logger = structlog.get_logger("worker_heartbeat")


class HeartbeatEmitter:
    def __init__(
        self,
        worker_id: str,
        grpc_host: str,
        grpc_port: int,
        redis_client: Optional[aioredis.Redis] = None,
        interval_sec: Optional[int] = None,
        ttl_sec: Optional[int] = None,
    ):
        self.worker_id = worker_id
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self._redis = redis_client
        self.interval_sec = interval_sec or settings.HEARTBEAT_INTERVAL_SEC
        self.ttl_sec = ttl_sec or settings.HEARTBEAT_TTL_SEC
        self._task: Optional[asyncio.Task] = None
        self._running = False

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is not None:
            return self._redis
        return get_redis_client()

    @property
    def heartbeat_key(self) -> str:
        return f"worker:heartbeat:{self.worker_id}"

    @property
    def grpc_address(self) -> str:
        return f"{self.grpc_host}:{self.grpc_port}"

    async def register(self) -> None:
        try:
            # Register address in active workers hash
            await self.redis.hset("workers:active", self.worker_id, self.grpc_address)
            # Set initial heartbeat key with TTL
            await self.redis.set(self.heartbeat_key, "alive", ex=self.ttl_sec)
            logger.info(
                "worker_registered",
                worker_id=self.worker_id,
                address=self.grpc_address,
                ttl_sec=self.ttl_sec,
            )
        except Exception as e:
            logger.error("worker_registration_failed", worker_id=self.worker_id, error=str(e))
            raise

    async def emit_once(self) -> bool:
        try:
            await self.redis.set(self.heartbeat_key, "alive", ex=self.ttl_sec)
            return True
        except Exception as e:
            logger.warning("heartbeat_emission_failed", worker_id=self.worker_id, error=str(e))
            return False

    async def deregister(self) -> None:
        try:
            await self.redis.hdel("workers:active", self.worker_id)
            await self.redis.delete(self.heartbeat_key)
            logger.info("worker_deregistered", worker_id=self.worker_id)
        except Exception as e:
            logger.warning("worker_deregistration_error", worker_id=self.worker_id, error=str(e))

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.interval_sec)
            if not self._running:
                break
            await self.emit_once()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.register()
        self._task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.deregister()
