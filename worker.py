# worker.py — async worker pool
import asyncio, json, uuid, logging
from job_queue import JobQueue, BRPOP_TIMEOUT
from handlers import HANDLERS   # dict[str, coroutine fn]

logger = logging.getLogger("worker")
MAX_RETRIES = 3

async def run_worker(worker_id: int, q: JobQueue):
    logger.info(f"worker-{worker_id} started (BRPOP_TIMEOUT={BRPOP_TIMEOUT}s)")
    while True:
        raw = await q.pop()   # uses BRPOP_TIMEOUT from config
        if not raw:
            continue
        job = json.loads(raw)
        handler = HANDLERS.get(job["type"])
        if not handler:
            logger.warning(f"unknown type: {job['type']}")
            continue
        try:
            result = await handler(job["payload"])
            await q.ack(job["id"], result)
        except Exception as e:
            retries = job.get("retries", 0)
            if retries < MAX_RETRIES:
                delay = 2 ** retries          # exponential back-off
                await q.retry(job, delay)
            else:
                await q.dlq(job, str(e))    # dead-letter

# Launch N workers concurrently — they share one event loop
async def start_workers(q: JobQueue, n: int = 3):
    await asyncio.gather(*[run_worker(i, q) for i in range(n)])