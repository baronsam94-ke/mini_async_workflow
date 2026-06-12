# db.py — async PostgreSQL with asyncpg connection pool
import importlib
from typing import Any

class Database:
    def __init__(self, pool: Any):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str = "postgresql://localhost/lyra"):
        asyncpg = importlib.import_module("asyncpg")
        pool = await asyncpg.create_pool(
            dsn,
            min_size=2, max_size=10,
            command_timeout=30
        )
        return cls(pool)

    async def close(self):
        await self.pool.close()

    async def fetch_one(self, sql: str, *args) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            return dict(row) if row else None

    async def execute(self, sql: str, *args) -> str:
        async with self.pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def write_result(self, job_id: str, result: Any):
        await self.execute(
            "INSERT INTO job_results(id,payload) VALUES($1,$2)",
            job_id, str(result)
        )