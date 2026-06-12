# db.py — async PostgreSQL with asyncpg connection pool
import importlib
import json
from typing import Any

class Database:
    def __init__(self, pool: Any):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str = "postgresql://postgres:devin@localhost/async-workflow"):
        asyncpg = importlib.import_module("asyncpg")
        pool = await asyncpg.create_pool(
            dsn,
            min_size=2, max_size=10,
            command_timeout=30
        )
        return cls(pool)

    async def close(self):
        await self.pool.close()

    async def init(self) -> None:
        """Create required tables if they don't exist."""
        await self.execute(
            """
            CREATE TABLE IF NOT EXISTS job_results (
                id TEXT PRIMARY KEY,
                payload JSONB,
                created_at TIMESTAMPTZ DEFAULT now()
            )
            """
        )

    async def fetch_one(self, sql: str, *args) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
            return dict(row) if row else None

    async def execute(self, sql: str, *args) -> str:
        async with self.pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def write_result(self, job_id: str, result: Any):
        # store the result as JSONB; use upsert to avoid duplicate PK errors
        payload = json.dumps(result)
        await self.execute(
            "INSERT INTO job_results(id,payload) VALUES($1,$2::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET payload = $2::jsonb, created_at = now()",
            job_id, payload,
        )