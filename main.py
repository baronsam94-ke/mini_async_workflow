# main.py — FastAPI async service
from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, Depends, HTTPException, Request  # type: ignore[reportMissingImports]
import redis.asyncio as aioredis  # type: ignore[reportMissingImports]
from job_queue import JobQueue, SOCKET_TIMEOUT, BRPOP_TIMEOUT
from db import Database

logger = logging.getLogger(__name__)

# Lifespan: open/close shared resources once per process
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db  = await Database.connect()
    app.state.rdb = await aioredis.from_url(
        "redis://localhost",
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_TIMEOUT,
    )
    logger.info("Redis client initialized with socket_timeout=%s", SOCKET_TIMEOUT)
    # ensure DB schema exists and wire DB into the queue
    await app.state.db.init()
    app.state.q   = JobQueue(app.state.rdb, "queue:jobs", app.state.db)
    yield                          # ← app runs here
    await app.state.db.close()
    await app.state.rdb.aclose()

app = FastAPI(lifespan=lifespan)

# Dependency injection for request handlers
async def get_queue(request: Request) -> JobQueue:
    return request.app.state.q

@app.post("/score")
async def enqueue_score(payload: dict, q: JobQueue = Depends(get_queue)):
    depth = await q.depth()
    if depth > 500:
        raise HTTPException(429, "queue full")
    job_id = await q.push("score_request", payload)
    return {"job_id": job_id, "queued": True}

@app.get("/job/{job_id}")
async def get_job_status(job_id: str, q: JobQueue = Depends(get_queue)):
    """
    Poll job status by ID. Returns:
    - completed: result is available in "result" field
    - pending: job is in queue waiting
    - retrying: job failed but is scheduled to retry
    - failed: job exhausted retries (in dead-letter queue)
    - unknown: job_id not found
    """
    status = await q.get_job_status(job_id)
    if status["status"] == "unknown":
        raise HTTPException(404, f"Job {job_id} not found")
    return status