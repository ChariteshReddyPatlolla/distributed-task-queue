import asyncio
import hashlib
from typing import Any, Callable, Coroutine, Dict
import httpx
import structlog

logger = structlog.get_logger("worker_handlers")

TaskHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, Dict[str, Any]]]
TASK_HANDLERS: Dict[str, TaskHandler] = {}


def register_handler(name: str):
    def decorator(fn: TaskHandler):
        TASK_HANDLERS[name] = fn
        return fn
    return decorator


@register_handler("sleep")
async def handle_sleep(payload: Dict[str, Any]) -> Dict[str, Any]:
    duration = float(payload.get("duration_sec", 1.0))
    # Cap sleep duration at 60s for safety
    duration = max(0.0, min(duration, 60.0))
    await asyncio.sleep(duration)
    return {"slept_sec": duration, "status": "completed"}


def _compute_hash(data: bytes, iterations: int) -> str:
    current = data
    for _ in range(iterations):
        current = hashlib.sha256(current).digest()
    return current.hex()


@register_handler("cpu_hash")
async def handle_cpu_hash(payload: Dict[str, Any]) -> Dict[str, Any]:
    iterations = int(payload.get("iterations", 10000))
    # Cap iterations at 1,000,000 for safety
    iterations = max(1, min(iterations, 1_000_000))
    data = str(payload.get("input", "benchmark")).encode("utf-8")
    
    # Run CPU-bound hashing in a thread pool to avoid blocking the asyncio event loop
    final_hash = await asyncio.to_thread(_compute_hash, data, iterations)
    return {"hash": final_hash, "iterations": iterations}


@register_handler("webhook")
async def handle_webhook(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = str(payload.get("url", ""))
    if not url:
        raise ValueError("Missing 'url' in webhook task payload")
    
    timeout_sec = float(payload.get("timeout_sec", 5.0))
    data = payload.get("data", {})

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        response = await client.post(url, json=data)
        return {
            "status_code": response.status_code,
            "response": response.text[:1024],
        }


@register_handler("fail_sim")
async def handle_fail_sim(payload: Dict[str, Any]) -> Dict[str, Any]:
    should_fail = payload.get("should_fail", True)
    if should_fail:
        error_msg = str(payload.get("error_message", "Simulated failure for retry/DLQ validation"))
        raise RuntimeError(error_msg)
    return {"status": "simulated_success"}


async def execute_task_handler(task_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    handler = TASK_HANDLERS.get(task_type)
    if not handler:
        raise ValueError(f"Unknown task type '{task_type}'. Supported: {list(TASK_HANDLERS.keys())}")
    return await handler(payload)
