# run_workers.py
import asyncio
import logging
from job_queue import JobQueue, SOCKET_TIMEOUT, BRPOP_TIMEOUT
from worker import start_workers
import redis.asyncio as aioredis
from db import Database

logger = logging.getLogger(__name__)

async def main():
    logger.info("Worker startup: BRPOP_TIMEOUT=%s, SOCKET_TIMEOUT=%s", BRPOP_TIMEOUT, SOCKET_TIMEOUT)
    rdb = await aioredis.from_url(
        "redis://localhost",
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_TIMEOUT,
    )
    db = await Database.connect()
    await db.init()
    q = JobQueue(rdb, "queue:jobs", db)
    try:
        await start_workers(q, n=3)
    finally:
        await db.close()
        await rdb.aclose()

asyncio.run(main())