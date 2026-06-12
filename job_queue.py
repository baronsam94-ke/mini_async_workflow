# queue.py — Redis-backed job queue abstraction
import json, time, uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis  # type: ignore[import]
else:
    Redis = Any

class JobQueue:
    def __init__(self, rdb: Redis, name: str):
        self.rdb, self.name = rdb, name
        self.dlq_key   = name + ":dlq"
        self.retry_key = name + ":retry"

    async def push(self, job_type: str, payload: dict) -> str:
        job_id = str(uuid.uuid4())
        raw = json.dumps({"id": job_id, "type": job_type,
                          "payload": payload, "retries": 0})
        await self.rdb.lpush(self.name, raw)
        return job_id

    async def pop(self, timeout: int = 5) -> str | None:
        # BRPOP blocks up to `timeout` secs — yields the event loop
        result = await self.rdb.brpop(self.name, timeout=timeout)
        return result[1] if result else None

    async def depth(self) -> int:
        return await self.rdb.llen(self.name)

    async def retry(self, job: dict, delay: float):
        job["retries"] += 1
        score = time.time() + delay   # ZSET score = next_run epoch
        await self.rdb.zadd(self.retry_key, {json.dumps(job): score})

    async def dlq(self, job: dict, error: str):
        job["error"] = error
        await self.rdb.lpush(self.dlq_key, json.dumps(job))

    async def ack(self, job_id: str, result):
        # In Redis Streams you'd XACK here; for list-based, just log
        await self.rdb.hset("job:results", job_id, json.dumps(result))