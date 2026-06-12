# queue.py — Redis-backed job queue abstraction
import json, time, uuid, os, logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis  # type: ignore[import]
else:
    Redis = Any

logger = logging.getLogger(__name__)

# BRPOP timeout (seconds) used for blocking operations
BRPOP_TIMEOUT = int(os.getenv("BRPOP_TIMEOUT", "5"))

# SOCKET_TIMEOUT must be strictly greater than BRPOP_TIMEOUT
# to prevent socket timeouts during blocking operations
_env_socket = os.getenv("SOCKET_TIMEOUT")
if _env_socket is not None:
    SOCKET_TIMEOUT = int(_env_socket)
    if SOCKET_TIMEOUT <= BRPOP_TIMEOUT:
        logger.warning(
            "SOCKET_TIMEOUT (%s) <= BRPOP_TIMEOUT (%s). Overriding to BRPOP_TIMEOUT + 1.",
            SOCKET_TIMEOUT,
            BRPOP_TIMEOUT,
        )
        SOCKET_TIMEOUT = BRPOP_TIMEOUT + 1
else:
    SOCKET_TIMEOUT = BRPOP_TIMEOUT + 1

logger.info("Redis config: BRPOP_TIMEOUT=%s, SOCKET_TIMEOUT=%s", BRPOP_TIMEOUT, SOCKET_TIMEOUT)

if SOCKET_TIMEOUT <= BRPOP_TIMEOUT:
    raise RuntimeError(
        f"Invariant violated: SOCKET_TIMEOUT ({SOCKET_TIMEOUT}) must be > BRPOP_TIMEOUT ({BRPOP_TIMEOUT})"
    )

class JobQueue:
    def __init__(self, rdb: Redis, name: str, db: Any = None):
        self.rdb, self.name = rdb, name
        # optional Database instance for durable result persistence
        self.db = db
        self.dlq_key   = name + ":dlq"
        self.retry_key = name + ":retry"

    async def push(self, job_type: str, payload: dict) -> str:
        job_id = str(uuid.uuid4())
        raw = json.dumps({"id": job_id, "type": job_type,
                          "payload": payload, "retries": 0})
        await self.rdb.lpush(self.name, raw)
        return job_id

    async def pop(self, timeout: int | None = None) -> str | None:
        # BRPOP blocks up to `timeout` secs — yields the event loop
        if timeout is None:
            timeout = BRPOP_TIMEOUT
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
        if self.db:
            # Best-effort write to Postgres; let DB layer handle upsert
            try:
                await self.db.write_result(job_id, result)
            except Exception:
                # don't let DB failures crash the worker; results still in Redis
                pass

    async def get_job_status(self, job_id: str) -> dict:
        """
        Get the status of a job. Returns dict with:
        {
            "job_id": str,
            "status": "completed|pending|retrying|failed",
            "result": Any | None,
            "error": str | None,
            "retries": int,
        }
        """
        # Check if completed in DB first (most durable)
        if self.db:
            row = await self.db.fetch_one(
                "SELECT payload FROM job_results WHERE id = $1",
                job_id
            )
            if row:
                try:
                    result = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
                except (json.JSONDecodeError, TypeError):
                    result = row["payload"]
                return {
                    "job_id": job_id,
                    "status": "completed",
                    "result": result,
                    "error": None,
                    "retries": 0,
                }

        # Check in-memory results hash
        result_raw = await self.rdb.hget("job:results", job_id)
        if result_raw:
            try:
                result = json.loads(result_raw)
            except (json.JSONDecodeError, TypeError):
                result = result_raw
            return {
                "job_id": job_id,
                "status": "completed",
                "result": result,
                "error": None,
                "retries": 0,
            }

        # Check retry queue (sorted set by next_run_time)
        retry_entries = await self.rdb.zrange(self.retry_key, 0, -1)
        for entry in retry_entries:
            try:
                job = json.loads(entry)
                if job.get("id") == job_id:
                    return {
                        "job_id": job_id,
                        "status": "retrying",
                        "result": None,
                        "error": job.get("error"),
                        "retries": job.get("retries", 0),
                    }
            except (json.JSONDecodeError, TypeError):
                continue

        # Check DLQ
        dlq_entries = await self.rdb.lrange(self.dlq_key, 0, -1)
        for entry in dlq_entries:
            try:
                job = json.loads(entry)
                if job.get("id") == job_id:
                    return {
                        "job_id": job_id,
                        "status": "failed",
                        "result": None,
                        "error": job.get("error"),
                        "retries": job.get("retries", 0),
                    }
            except (json.JSONDecodeError, TypeError):
                continue

        # Check main queue (list)
        queue_entries = await self.rdb.lrange(self.name, 0, -1)
        for entry in queue_entries:
            try:
                job = json.loads(entry)
                if job.get("id") == job_id:
                    return {
                        "job_id": job_id,
                        "status": "pending",
                        "result": None,
                        "error": None,
                        "retries": job.get("retries", 0),
                    }
            except (json.JSONDecodeError, TypeError):
                continue

        # Not found anywhere
        return {
            "job_id": job_id,
            "status": "unknown",
            "result": None,
            "error": "Job not found",
            "retries": 0,
        }