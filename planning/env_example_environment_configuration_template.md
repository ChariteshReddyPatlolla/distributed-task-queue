# ==========================================
# PostgreSQL Configuration
# ==========================================
POSTGRES_USER=taskqueue
POSTGRES_PASSWORD=changeme_local_dev_only
POSTGRES_DB=taskqueue_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
DATABASE_URL=postgresql+asyncpg://taskqueue:changeme_local_dev_only@postgres:5432/taskqueue_db

# ==========================================
# Redis Configuration
# ==========================================
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_URL=redis://redis:6379/0

# ==========================================
# API Gateway Configuration
# ==========================================
GATEWAY_HOST=0.0.0.0
GATEWAY_PORT=8000
MAX_PAYLOAD_BYTES=65536
ENABLE_API_KEY_AUTH=true
VALID_API_KEYS=dev-key-1,bench-key-1,test-key-1

# ==========================================
# Token-Bucket Rate Limiter
# ==========================================
BUCKET_CAPACITY=100
REFILL_RATE_PER_SEC=20
RATE_LIMIT_FAIL_MODE=fail_closed

# ==========================================
# Dispatcher & Consistent Hash Ring
# ==========================================
HASH_RING_VIRTUAL_NODES=100
GRPC_TASK_TIMEOUT_SEC=30
DISPATCHER_POLL_INTERVAL_MS=50
WATCHDOG_INTERVAL_SEC=2

# ==========================================
# Worker & Heartbeat Configuration
# ==========================================
WORKER_GRPC_PORT=50051
WORKER_MAX_CONCURRENCY=16
HEARTBEAT_INTERVAL_SEC=3
HEARTBEAT_TTL_SEC=10

# ==========================================
# Retries & Exponential Backoff
# ==========================================
DEFAULT_MAX_RETRIES=3
BASE_BACKOFF_SEC=2
MAX_BACKOFF_SEC=60

# ==========================================
# Observability
# ==========================================
LOG_LEVEL=INFO
LOG_FORMAT=json