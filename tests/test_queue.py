import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from aiopq import AioPQ
from job import Job


async def test_put_get_ack_round_trip(queue):
    payload = {"user": "O'Brien", "text": 'say "hi"'}
    job_id = await queue.put("jobs", payload)

    job = await queue.get(timeout=1)
    assert job is not None
    assert job.id == job_id
    assert job.data == payload
    assert job.attempts == 1

    assert await queue.ack(job) is True
    assert await queue.get(timeout=0.1) is None


async def test_async_for_yields_job(queue):
    await queue.put("jobs", {"iter": True})

    async for job in queue:
        assert isinstance(job, Job)
        assert await queue.ack(job) is True
        break


async def test_get_timeout_does_not_hold_connection(queue):
    started = time.monotonic()
    assert await queue.get(timeout=0.15) is None
    elapsed = time.monotonic() - started

    assert 0.10 <= elapsed < 1.0
    assert queue.pool.get_idle_size() == queue.pool.get_size()


async def test_null_schedule_at_is_pulled(queue):
    job_id = await queue.put("jobs", {"null_schedule": True}, None)

    job = await queue.get(timeout=1)
    assert job is not None
    assert job.id == job_id
    assert job.schedule_at is None
    assert await queue.ack(job) is True


async def test_future_schedule_at_is_not_pulled(queue):
    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=60)
    await queue.put("jobs", {"future": True}, scheduled_at)

    assert await queue.get(timeout=0.1) is None


async def test_transaction_propagates_errors(queue):
    with pytest.raises(asyncpg.UndefinedTableError):
        async with queue.transaction() as conn:
            await conn.fetch("select * from definitely_missing_table")

    # The pool must still be usable after the failed transaction.
    await queue.put("jobs", {"after_error": True})
    assert await queue.get(timeout=1) is not None


async def test_ready_index_is_used(queue):
    for index in range(5):
        await queue.put("jobs", {"index": index})

    async with queue.pool.acquire() as conn:
        await conn.execute("SET enable_seqscan = off")
        try:
            plan = await conn.fetch(
                f"""
                EXPLAIN SELECT id
                FROM {queue.qualified_table}
                WHERE q_name = $1
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND attempts < 3
                  AND (locked_until IS NULL OR locked_until <= now())
                  AND (schedule_at IS NULL OR schedule_at <= now())
                ORDER BY schedule_at NULLS FIRST, id
                LIMIT 1
                """,
                queue.name,
            )
        finally:
            await conn.execute("RESET enable_seqscan")

    plan_text = "\n".join(row[0] for row in plan)
    assert "queue_ready_idx" in plan_text


async def test_lease_redelivery_and_stale_ack(queue):
    job_id = await queue.put("jobs", {"lease": True})

    first = await queue.get(timeout=1)
    assert first is not None
    assert first.id == job_id
    assert first.attempts == 1

    await asyncio.sleep(0.25)  # queue_env uses lease_seconds=0.2

    second = await queue.get(timeout=1)
    assert second is not None
    assert second.id == job_id
    assert second.attempts == 2

    assert await queue.ack(first) is False
    assert await queue.ack(second) is True


async def test_nack_retries_then_dead_letters_and_requeue(queue):
    job_id = await queue.put("jobs", {"retry": True})

    first = await queue.get(timeout=1)
    assert first is not None and first.attempts == 1
    assert await queue.nack(first, error="first failure") is True

    second = await queue.get(timeout=1)
    assert second is not None and second.attempts == 2
    assert await queue.nack(second, error="second failure") is True

    third = await queue.get(timeout=1)
    assert third is not None and third.attempts == 3
    assert await queue.nack(third, error="third failure") is False

    assert await queue.get(timeout=0.1) is None

    failed = await queue.get_failed()
    assert any(job.id == job_id for job in failed)

    assert await queue.requeue(job_id) is True
    restored = await queue.get(timeout=1)
    assert restored is not None
    assert restored.id == job_id
    assert restored.attempts == 1
    assert await queue.ack(restored) is True


async def test_create_is_idempotent(queue_env):
    await queue_env.create()
    await queue_env.create()


async def test_legacy_dequeued_at_is_migrated(db_dsn):
    schema = f"aiopq_legacy_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(db_dsn)
    pool = None
    pq = None
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        await admin.execute(
            f"""
            CREATE TABLE "{schema}".queue (
                id bigserial primary key,
                q_name text not null check (length(q_name) > 0),
                enqueued_at timestamptz not null default current_timestamp,
                dequeued_at timestamptz,
                schedule_at timestamptz,
                data jsonb not null
            )
            """
        )
        await admin.execute(
            f"""
            INSERT INTO "{schema}".queue (q_name, dequeued_at, data)
            VALUES ('jobs', now(), '{{"legacy": true}}'::jsonb)
            """
        )

        pool = await asyncpg.create_pool(
            db_dsn,
            min_size=1,
            max_size=1,
            server_settings={"search_path": f'"{schema}"'},
        )
        pq = AioPQ(pool)
        await pq.create()

        row = await pool.fetchrow(
            'SELECT finished_at, attempts FROM "queue" WHERE id = 1'
        )
        assert row["finished_at"] is not None
        assert row["attempts"] == 0
    finally:
        if pq is not None:
            await pq.close()
        elif pool is not None:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()
