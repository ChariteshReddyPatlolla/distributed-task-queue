import time
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import structlog
from services.common.config import settings
from services.common.db import close_db, init_db
from services.common.logging import setup_logging
from services.common.redis_client import close_redis
from services.gateway.dlq_routes import router as dlq_router
from services.gateway.rate_limiter import rate_limiter
from services.gateway.routes import router as gateway_router

logger = setup_logging("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("gateway_starting_up", port=settings.GATEWAY_PORT)
    try:
        await init_db()
        await rate_limiter._ensure_script_loaded()
        logger.info("gateway_infrastructure_connected")
    except Exception as e:
        logger.error("gateway_startup_warning", error=str(e))
    yield
    logger.info("gateway_shutting_down")
    await close_db()
    await close_redis()


app = FastAPI(
    title="High-Concurrency Distributed Task Queue & Rate-Limited API Gateway",
    description="Production-grade asynchronous task distribution platform with Redis token-bucket rate limiting, consistent hashing gRPC worker pool, and PostgreSQL persistence.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


@app.middleware("http")
async def gateway_middleware(request: Request, call_next):
    start_time = time.time()
    request_id = request.headers.get("X-Request-ID", f"req_{uuid.uuid4().hex[:8]}")
    request.state.request_id = request_id

    # Handle /v1/* routes: Authentication & Rate Limiting
    if request.url.path.startswith("/v1"):
        api_key = request.headers.get("X-API-Key")
        
        # 1. API Key Authentication
        if settings.ENABLE_API_KEY_AUTH:
            if not api_key:
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={
                        "error": {
                            "code": "UNAUTHORIZED",
                            "message": "Missing required X-API-Key header",
                            "request_id": request_id,
                        }
                    },
                    headers={"X-Request-ID": request_id},
                )
            valid_keys = settings.get_valid_api_keys()
            if valid_keys and api_key not in valid_keys:
                return JSONResponse(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    content={
                        "error": {
                            "code": "UNAUTHORIZED",
                            "message": "Invalid API key provided",
                            "request_id": request_id,
                        }
                    },
                    headers={"X-Request-ID": request_id},
                )

        # 2. Redis Token-Bucket Rate Limiter
        client_id = api_key or (request.client.host if request.client else "unknown")
        try:
            rl_res = await rate_limiter.check(client_id)
        except Exception as e:
            logger.error("rate_limit_backend_failed", client_id=client_id, error=str(e))
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "error": {
                        "code": "SERVICE_UNAVAILABLE",
                        "message": "Rate limiter storage service unavailable",
                        "request_id": request_id,
                    }
                },
                headers={"X-Request-ID": request_id},
            )

        if not rl_res.allowed:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": f"Token bucket exhausted. Retry after {rl_res.retry_after} seconds.",
                        "request_id": request_id,
                    }
                },
                headers={
                    "X-RateLimit-Limit": str(rl_res.limit),
                    "X-RateLimit-Remaining": str(rl_res.remaining),
                    "X-RateLimit-Reset": str(rl_res.reset),
                    "Retry-After": str(rl_res.retry_after),
                    "X-Request-ID": request_id,
                },
            )

        # Request allowed: execute next handler and attach rate limit headers
        response: Response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(rl_res.limit)
        response.headers["X-RateLimit-Remaining"] = str(rl_res.remaining)
        response.headers["X-RateLimit-Reset"] = str(rl_res.reset)
        response.headers["X-Request-ID"] = request_id
    else:
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

    duration_ms = int((time.time() - start_time) * 1000)
    logger.info(
        "http_request_finished",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        latency_ms=duration_ms,
        request_id=request_id,
    )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", f"req_{uuid.uuid4().hex[:8]}")
    code = "NOT_FOUND" if exc.status_code == 404 else "HTTP_ERROR"
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": exc.detail if isinstance(exc.detail, str) else str(exc.detail),
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    request_id = getattr(request.state, "request_id", f"req_{uuid.uuid4().hex[:8]}")
    errors = exc.errors()
    msg = "; ".join([f"{'.'.join(str(l) for l in err['loc'])}: {err['msg']}" for err in errors])
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={
            "error": {
                "code": "UNPROCESSABLE_ENTITY",
                "message": msg,
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", f"req_{uuid.uuid4().hex[:8]}")
    logger.exception("unhandled_internal_error", error=str(exc), request_id=request_id)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An internal server error occurred",
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )


app.include_router(gateway_router)
app.include_router(dlq_router)
