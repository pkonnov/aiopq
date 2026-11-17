import asyncio
import os
import uuid

import aiohttp
import asyncpg
import pytest
import pytest_asyncio

from aiopq import AioPQ

API_URL = os.getenv("API_URL", "http://127.0.0.1:8080")
DATABASE_URL = os.getenv("DATABASE_URL")


def _require_database_url() -> str:
    if not DATABASE_URL:
        pytest.fail("DATABASE_URL is required to run integration tests")
    return DATABASE_URL


@pytest_asyncio.fixture
async def api():
    """HTTP client for the app service, waiting until it is healthy."""

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        base_url=API_URL, timeout=timeout
    ) as session:
        deadline = asyncio.get_running_loop().time() + 30
        last_error = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                async with session.get("/health") as response:
                    if response.status == 200:
                        yield session
                        return
                    last_error = f"HTTP {response.status}"
            except aiohttp.ClientError as exc:
                last_error = str(exc)
            await asyncio.sleep(0.1)
        pytest.fail(f"app at {API_URL} is not healthy: {last_error}")


@pytest.fixture
def db_dsn() -> str:
    return _require_database_url()


@pytest_asyncio.fixture
async def queue_env():
    """AioPQ instance on an isolated PostgreSQL schema."""

    dsn = _require_database_url()
    schema = f"aiopq_test_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    pool = None
    pq = None
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            server_settings={"search_path": f'"{schema}"'},
        )
        pq = AioPQ(
            pool,
            poll_interval=0.05,
            lease_seconds=0.2,
            max_attempts=3,
        )
        await pq.create()
        yield pq
    finally:
        if pq is not None:
            await pq.close()
        elif pool is not None:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


@pytest_asyncio.fixture
async def queue(queue_env):
    return queue_env["jobs"]
