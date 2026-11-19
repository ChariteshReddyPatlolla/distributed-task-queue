import time
import uuid
import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport
from services.common.db import close_db, get_async_session, init_db
from services.common.models import Job
from services.common.redis_client import close_redis, get_redis_client
from services.gateway.main import app
from services.gateway.rate_limiter import rate_limiter

VALID_API_KEY = "test-key-1"


@pytest_asyncio.fixture(autouse=True)
async def setup_teardown():
    await init_db()
    yield
    await close_db()
    await close_redis()
    rate_limiter.reset()


@pytest.mark.asyncio
async def test_health_and_metrics_endpoints():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # /health
        res = await client.get("/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] in ["healthy", "degraded"]
        assert data["postgres"] is True
        assert data["redis"] is True

        # /metrics
        res_m = await client.get("/metrics")
        assert res_m.status_code == 200
        metrics_data = res_m.json()
        assert "jobs_submitted" in metrics_data
        assert "rate_limited_total" in metrics_data


@pytest.mark.asyncio
async def test_api_key_authentication():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Missing header -> 401
        res = await client.post(
            "/v1/jobs",
            json={"task_type": "sleep", "payload": {}},
        )
        assert res.status_code == 401
        body = res.json()
        assert body["error"]["code"] == "UNAUTHORIZED"
        assert "Missing" in body["error"]["message"]

        # Invalid key -> 401
        res_invalid = await client.post(
            "/v1/jobs",
            headers={"X-API-Key": "bogus-key"},
            json={"task_type": "sleep", "payload": {}},
        )
        assert res_invalid.status_code == 401
        assert res_invalid.json()["error"]["code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_payload_validation_and_errors():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"X-API-Key": VALID_API_KEY}

        # Invalid priority (15) -> 422
        res1 = await client.post(
            "/v1/jobs",
            headers=headers,
            json={"task_type": "sleep", "priority": 15},
        )
        assert res1.status_code == 422
        assert res1.json()["error"]["code"] == "UNPROCESSABLE_ENTITY"

        # Unknown task type -> 422
        res2 = await client.post(
            "/v1/jobs",
            headers=headers,
            json={"task_type": "invalid_type", "priority": 5},
        )
        assert res2.status_code == 422

        # Oversized payload (> 64 KB) -> 422
        res3 = await client.post(
            "/v1/jobs",
            headers=headers,
            json={"task_type": "sleep", "payload": {"big": "a" * 70000}},
        )
        assert res3.status_code == 422


@pytest.mark.asyncio
async def test_job_submission_and_idempotency():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"X-API-Key": VALID_API_KEY}
        idempotency_key = f"test-idem-{uuid.uuid4().hex[:12]}"

        # 1. Initial submission -> 202 Accepted
        res = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "task_type": "cpu_hash",
                "payload": {"iterations": 1000},
                "priority": 9,
                "max_retries": 3,
                "idempotency_key": idempotency_key,
            },
        )
        assert res.status_code == 202
        body = res.json()
        job_id = body["job_id"]
        assert body["status"] == "QUEUED"
        assert body["priority"] == 9
        assert "X-RateLimit-Limit" in res.headers
        assert "X-RateLimit-Remaining" in res.headers

        # Verify job is in Redis ready queue
        redis = get_redis_client()
        score = await redis.zscore("queue:ready", job_id)
        assert score is not None
        # Score formula: (-priority * 1e12) + timestamp_ms
        # In year 2026, (-9e12) + ~1.79e12 = ~ -7.2e12
        expected_score_approx = float((-9 * 1_000_000_000_000) + int(time.time() * 1000))
        assert abs(score - expected_score_approx) < 10000

        # 2. Duplicate submission with same idempotency key -> 200 OK with same job_id
        res_dup = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "task_type": "cpu_hash",
                "payload": {"iterations": 1000},
                "priority": 9,
                "idempotency_key": idempotency_key,
            },
        )
        assert res_dup.status_code == 200
        assert res_dup.json()["job_id"] == job_id

        # 3. Lookup job status -> 200 OK
        res_lookup = await client.get(f"/v1/jobs/{job_id}", headers=headers)
        assert res_lookup.status_code == 200
        lookup_data = res_lookup.json()
        assert lookup_data["id"] == job_id
        assert lookup_data["idempotency_key"] == idempotency_key
        assert lookup_data["status"] == "QUEUED"
        assert lookup_data["task_type"] == "cpu_hash"

        # 4. Unknown job lookup -> 404 Not Found
        random_uuid = str(uuid.uuid4())
        res_404 = await client.get(f"/v1/jobs/{random_uuid}", headers=headers)
        assert res_404.status_code == 404
        assert res_404.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_rate_limiting_enforcement_and_429():
    transport = ASGITransport(app=app)
    client_key = "bench-key-1"
    headers = {"X-API-Key": client_key}
    redis = get_redis_client()

    # Pre-drain the token bucket for this client directly in Redis
    key = f"ratelimit:{client_key}"
    now = time.time()
    await redis.hset(key, mapping={"tokens": 0, "last_refill": now})

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.post(
            "/v1/jobs",
            headers=headers,
            json={"task_type": "sleep", "payload": {}},
        )
        assert res.status_code == 429
        body = res.json()
        assert body["error"]["code"] == "RATE_LIMIT_EXCEEDED"
        assert "Retry-After" in res.headers
        assert res.headers["X-RateLimit-Remaining"] == "0"

    # Clean up test ratelimit key
    await redis.delete(key)
