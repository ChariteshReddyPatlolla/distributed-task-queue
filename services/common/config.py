from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # PostgreSQL Configuration
    POSTGRES_USER: str = "taskqueue"
    POSTGRES_PASSWORD: str = "changeme_local_dev_only"
    POSTGRES_DB: str = "taskqueue_db"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://taskqueue:changeme_local_dev_only@postgres:5432/taskqueue_db"
    )

    # Redis Configuration
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_URL: str = "redis://redis:6379/0"

    # API Gateway Configuration
    GATEWAY_HOST: str = "0.0.0.0"
    GATEWAY_PORT: int = 8000
    MAX_PAYLOAD_BYTES: int = 65536
    ENABLE_API_KEY_AUTH: bool = True
    VALID_API_KEYS: str = "dev-key-1,bench-key-1,test-key-1,wrk-bench-key"

    # Token-Bucket Rate Limiter
    BUCKET_CAPACITY: int = 100
    REFILL_RATE_PER_SEC: int = 20
    RATE_LIMIT_FAIL_MODE: str = "fail_closed"

    # Dispatcher & Consistent Hash Ring
    HASH_RING_VIRTUAL_NODES: int = 100
    GRPC_TASK_TIMEOUT_SEC: int = 30
    DISPATCHER_POLL_INTERVAL_MS: int = 50
    WATCHDOG_INTERVAL_SEC: int = 2

    # Worker & Heartbeat Configuration
    WORKER_GRPC_PORT: int = 50051
    WORKER_MAX_CONCURRENCY: int = 16
    HEARTBEAT_INTERVAL_SEC: int = 3
    HEARTBEAT_TTL_SEC: int = 10

    # Retries & Exponential Backoff
    DEFAULT_MAX_RETRIES: int = 3
    BASE_BACKOFF_SEC: int = 2
    MAX_BACKOFF_SEC: int = 60

    # Observability
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"

    def get_valid_api_keys(self) -> List[str]:
        if not self.VALID_API_KEYS:
            return []
        return [k.strip() for k in self.VALID_API_KEYS.split(",") if k.strip()]


settings = Settings()
