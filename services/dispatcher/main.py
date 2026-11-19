import asyncio
import sys
import structlog
from services.common.db import close_db, init_db
from services.common.logging import setup_logging
from services.common.redis_client import close_redis
from services.dispatcher.dispatcher import JobDispatcher

logger = setup_logging("dispatcher")


async def main():
    logger.info("dispatcher_service_initializing")
    await init_db()

    dispatcher = JobDispatcher()
    await dispatcher.sync_workers()

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("dispatcher_received_stop_signal")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)

    dispatcher_task = asyncio.create_task(dispatcher.run())

    try:
        if sys.platform == "win32":
            await dispatcher_task
        else:
            await stop_event.wait()
    finally:
        logger.info("dispatcher_service_shutting_down")
        await dispatcher.stop()
        dispatcher_task.cancel()
        try:
            await dispatcher_task
        except asyncio.CancelledError:
            pass
        await close_db()
        await close_redis()
        logger.info("dispatcher_service_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
