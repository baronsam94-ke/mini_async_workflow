# main.py — FastAPI async service
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException  # type: ignore[reportMissingImports]
import redis.asyncio as aioredis  # type: ignore[reportMissingImports]
from job_queue import JobQueue
from db import Database

# Lifespan: open/close shared resources once per process
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db  = await Database.connect()
    app.state.rdb = await aioredis.from_url("redis://localhost")
    app.state.q   = JobQueue(app.state.rdb, "queue:jobs")
    yield                          # ← app runs here
    await app.state.db.close()
    await app.state.rdb.aclose()

app = FastAPI(lifespan=lifespan)

# Dependency injection for request handlers
async def get_queue(request) -> JobQueue:
    return request.app.state.q

@app.post("/score")
async def enqueue_score(payload: dict, q: JobQueue = Depends(get_queue)):
    depth = await q.depth()
    if depth > 500:
        raise HTTPException(429, "queue full")
    job_id = await q.push("score_request", payload)
    return {"job_id": job_id, "queued": True}