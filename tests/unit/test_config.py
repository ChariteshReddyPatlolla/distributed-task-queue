import pytest
from services.common.config import Settings


def test_default_settings():
    settings = Settings()
    assert settings.POSTGRES_PORT == 5432
    assert settings.REDIS_PORT == 6379
    assert settings.GATEWAY_PORT == 8000
    assert settings.MAX_PAYLOAD_BYTES == 65536
    assert settings.BUCKET_CAPACITY == 100
    assert settings.REFILL_RATE_PER_SEC == 20
    assert settings.RATE_LIMIT_FAIL_MODE in ["fail_closed", "fail_open"]
    assert settings.DEFAULT_MAX_RETRIES == 3
    assert settings.WORKER_GRPC_PORT == 50051
    assert settings.WORKER_MAX_CONCURRENCY == 16
    assert settings.HEARTBEAT_INTERVAL_SEC == 3
    assert settings.HEARTBEAT_TTL_SEC == 10


def test_api_key_parsing():
    settings = Settings(VALID_API_KEYS="key-a, key-b ,key-c")
    keys = settings.get_valid_api_keys()
    assert keys == ["key-a", "key-b", "key-c"]

    empty_settings = Settings(VALID_API_KEYS="")
    assert empty_settings.get_valid_api_keys() == []
