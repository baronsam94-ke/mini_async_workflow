# run_workers.py
import asyncio
from job_queue import JobQueue
from worker import start_workers
import redis.asyncio as aioredis

async def main():
    rdb = await aioredis.from_url("redis://localhost")
    q = JobQueue(rdb, "queue:jobs")
    await start_workers(q, n=3)

asyncio.run(main())