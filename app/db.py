from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg

from app.config import settings


class Database:
    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool:
            return

        self._pool = await asyncpg.create_pool(
            dsn=settings.db_dsn,
            min_size=1,
            max_size=10,
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        if not self._pool:
            raise RuntimeError("Database is not connected")

        conn = await self._pool.acquire()
        try:
            yield conn
        finally:
            await self._pool.release(conn)


db = Database()