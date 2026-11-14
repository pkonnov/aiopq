import asyncio
import logging
import os
import re
import socket
import uuid
import warnings
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from weakref import WeakValueDictionary

import asyncpg
import orjson

from job import Job

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(value: str) -> str:
    """Return a safely quoted PostgreSQL identifier.

    Table/schema names cannot be passed as query parameters, so they are
    validated and quoted instead of being interpolated as-is.
    """

    if not isinstance(value, str) or _IDENT_RE.fullmatch(value) is None:
        raise ValueError(f"invalid SQL identifier: {value!r}")
    if len(value.encode("utf-8")) > 63:
        raise ValueError(f"SQL identifier is too long: {value!r}")
    return f'"{value}"'


def _positive_float(value: Any, name: str) -> float:
    result = float(value)
    if result <= 0:
        raise ValueError(f"{name} must be > 0")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = int(value)
    if result <= 0:
        raise ValueError(f"{name} must be > 0")
    return result


class AioPQ:
    table_name = "queue"
    template_schema = os.path.dirname(__file__)
    queue_class = None

    def __init__(
        self,
        pool: asyncpg.Pool,
        queue_class=None,
        timeout: float | None = None,
        *,
        poll_interval: float = 0.5,
        lease_seconds: float = 30.0,
        max_attempts: int = 3,
    ) -> None:
        if pool is None:
            raise ValueError("pool is required")

        if timeout is not None:
            warnings.warn(
                "AioPQ(timeout=...) is deprecated; use poll_interval=...",
                DeprecationWarning,
                stacklevel=2,
            )
            poll_interval = timeout

        self.pool = pool
        self.poll_interval = _positive_float(poll_interval, "poll_interval")
        self.lease_seconds = _positive_float(lease_seconds, "lease_seconds")
        self.max_attempts = _positive_int(max_attempts, "max_attempts")
        if queue_class is not None:
            self.queue_class = queue_class
        self.queues = WeakValueDictionary()
        logger.debug("Init AioPQ")

    def __getitem__(self, name: str) -> "Queue":
        queue = self.queues.get(name)
        if queue is not None:
            return queue

        factory = self.queue_class or Queue
        queue = factory(
            name,
            pool=self.pool,
            poll_interval=self.poll_interval,
            lease_seconds=self.lease_seconds,
            max_attempts=self.max_attempts,
        )
        self.queues[name] = queue
        return queue

    async def create(self) -> None:
        queue = self[""]

        with open(
            os.path.join(self.template_schema, "create.sql"), encoding="utf-8"
        ) as f:
            sql = f.read()

        async with queue.transaction() as init_conn:
            await init_conn.execute(
                sql.format(
                    table=queue.qualified_table,
                    index=_quote_ident(queue.index_name),
                )
            )

    async def close(self) -> None:
        """Close the pool passed to ``AioPQ``.

        Call this once during shutdown if ``AioPQ`` owns the pool.  If the
        pool is shared with other components, close it at the application
        level and do not call this method.
        """

        await self.pool.close()


class AQueueIterator:
    def __init__(self, queue: "Queue"):
        self.queue = queue

    def __aiter__(self) -> "AQueueIterator":
        return self

    async def __anext__(self) -> Job:
        job = await self.queue.get()
        if job is None:
            raise StopAsyncIteration
        return job


class Queue:
    """A logical queue backed by a PostgreSQL table."""

    dumps = loads = staticmethod(lambda data: data)

    encode = staticmethod(orjson.dumps)
    decode = staticmethod(orjson.loads)

    def __init__(
        self,
        name: str,
        *,
        pool: asyncpg.Pool | None = None,
        table_name: str = "queue",
        schema: str | None = None,
        timeout: float | None = None,
        poll_interval: float = 0.5,
        lease_seconds: float = 30.0,
        max_attempts: int = 3,
        worker_id: str | None = None,
    ):
        if pool is None:
            raise ValueError("pool is required")

        if timeout is not None:
            warnings.warn(
                "Queue(timeout=...) is deprecated; use poll_interval=...",
                DeprecationWarning,
                stacklevel=2,
            )
            poll_interval = timeout

        self.pool = pool
        self.name = name
        self.table_name = table_name
        self.schema = schema
        self.poll_interval = _positive_float(poll_interval, "poll_interval")
        self.lease_seconds = _positive_float(lease_seconds, "lease_seconds")
        self.max_attempts = _positive_int(max_attempts, "max_attempts")

        # Stable base for lock tokens; each pull adds a unique UUID suffix.
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{name}"

    def __aiter__(self) -> AQueueIterator:
        return AQueueIterator(self)

    @property
    def qualified_table(self) -> str:
        table = _quote_ident(self.table_name)
        if self.schema:
            return f"{_quote_ident(self.schema)}.{table}"
        return table

    @property
    def index_name(self) -> str:
        name = f"{self.table_name}_ready_idx"
        if len(name.encode("utf-8")) > 63:
            raise ValueError("table_name is too long for the derived index name")
        return name

    async def get(self, timeout: float | None = None) -> Job | None:
        """Return the next available job or ``None`` after ``timeout``.

        The pool connection is only held for the duration of the pull query.
        Waiting between polls never holds a connection or a transaction.
        """

        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(0.0, float(timeout))

        while True:
            async with self.pool.acquire() as conn:
                row = await self._pull_item(conn)

            if row is not None:
                return self._job_from_row(row)

            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return None
            else:
                remaining = None

            delay = self.poll_interval
            if remaining is not None:
                delay = min(delay, remaining)
            if delay > 0:
                await asyncio.sleep(delay)

    async def put(
        self,
        name: str,
        data: dict,
        schedule_at: datetime | None = None,
    ) -> int:
        """Enqueue ``data`` into the logical queue ``name``.

        A naive ``schedule_at`` is treated as UTC.  ``None`` means "ready
        immediately".
        """

        if not name:
            raise ValueError("queue name must be non-empty")

        if schedule_at is not None:
            if not isinstance(schedule_at, datetime):
                raise TypeError("schedule_at must be a datetime or None")
            if schedule_at.tzinfo is None:
                schedule_at = schedule_at.replace(tzinfo=timezone.utc)

        async with self.pool.acquire() as conn:
            return await self._put_item(
                conn, name=name, data=data, schedule_at=schedule_at
            )

    async def ack(self, job: Job) -> bool:
        """Mark a successfully processed job as finished.

        Returns ``True`` when this worker still owned the job lease.  The
        lease token is unique per pull, so a worker that lost the lease cannot
        acknowledge a job that was handed to another consumer.
        """

        if job.locked_by is None:
            return False

        async with self.pool.acquire() as conn:
            finished_id = await conn.fetchval(
                f"""
                UPDATE {self.qualified_table}
                SET finished_at = now(),
                    locked_until = NULL,
                    locked_by = NULL
                WHERE id = $1
                  AND q_name = $2
                  AND locked_by = $3
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                RETURNING id
                """,
                job.id,
                self.name,
                job.locked_by,
            )
        return finished_id is not None

    async def nack(
        self,
        job: Job,
        error: BaseException | str | None = None,
        *,
        delay: float = 0.0,
    ) -> bool | None:
        """Return a failed job to the queue.

        Returns ``True`` when the job was requeued, ``False`` when it reached
        ``max_attempts`` and was moved to the dead-letter state, and ``None``
        when the worker no longer owns the job.
        """

        if job.locked_by is None:
            return None

        error_text = None if error is None else str(error)[:10000]

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE {self.qualified_table}
                SET locked_until = NULL,
                    locked_by = NULL,
                    last_error = $4,
                    schedule_at = now() + make_interval(secs => $5::double precision),
                    failed_at = CASE WHEN attempts >= $6 THEN now() ELSE failed_at END
                WHERE id = $1
                  AND q_name = $2
                  AND locked_by = $3
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                RETURNING failed_at IS NOT NULL AS failed
                """,
                job.id,
                self.name,
                job.locked_by,
                error_text,
                float(delay),
                self.max_attempts,
            )

        if row is None:
            return None
        return not row["failed"]

    async def get_failed(self, limit: int = 100) -> list[Job]:
        """Return jobs that exhausted ``max_attempts`` (logical DLQ)."""

        limit = _positive_int(limit, "limit")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id,
                       data::text AS data,
                       length(data::text) AS size,
                       enqueued_at,
                       schedule_at,
                       attempts,
                       locked_until,
                       locked_by
                FROM {self.qualified_table}
                WHERE q_name = $1
                  AND failed_at IS NOT NULL
                ORDER BY failed_at DESC, id
                LIMIT $2
                """,
                self.name,
                limit,
            )
        return [self._job_from_row(row) for row in rows]

    async def requeue(self, job: Job | int) -> bool:
        """Move a finished or dead-lettered job back to the queue."""

        job_id = self._job_id(job)
        async with self.pool.acquire() as conn:
            requeued_id = await conn.fetchval(
                f"""
                UPDATE {self.qualified_table}
                SET failed_at = NULL,
                    finished_at = NULL,
                    attempts = 0,
                    locked_until = NULL,
                    locked_by = NULL,
                    last_error = NULL,
                    schedule_at = now()
                WHERE id = $1
                  AND q_name = $2
                  AND (failed_at IS NOT NULL OR finished_at IS NOT NULL)
                RETURNING id
                """,
                job_id,
                self.name,
            )
        return requeued_id is not None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        """Acquire a connection and run a transaction.

        Exceptions are propagated to the caller; the transaction is rolled
        back and the connection is returned to the pool.
        """

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def _pull_item(self, conn: asyncpg.Connection) -> asyncpg.Record | None:
        """Atomically lease the next available job.

        Ordering and the partial index are aligned:
        ``(q_name, schedule_at NULLS FIRST, id) WHERE finished_at IS NULL
        AND failed_at IS NULL``.  The ``expired`` CTE moves jobs that used up
        all attempts but were never acked/nacked into the dead-letter state.
        """

        # A fresh token per pull: ack/nack can only be performed by the
        # consumer that received this exact lease.
        lock_token = f"{self.worker_id}:{uuid.uuid4().hex}"

        return await conn.fetchrow(
            f"""
            WITH selected AS (
                SELECT id
                FROM {self.qualified_table}
                WHERE q_name = $1
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND attempts < $2
                  AND (locked_until IS NULL OR locked_until <= now())
                  AND (schedule_at IS NULL OR schedule_at <= now())
                ORDER BY schedule_at NULLS FIRST, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            ),
            expired AS (
                UPDATE {self.qualified_table}
                SET failed_at = now(),
                    locked_until = NULL,
                    locked_by = NULL,
                    last_error = COALESCE(last_error, 'lease expired')
                WHERE q_name = $1
                  AND finished_at IS NULL
                  AND failed_at IS NULL
                  AND attempts >= $2
                  AND (locked_until IS NULL OR locked_until <= now())
                RETURNING id
            )
            UPDATE {self.qualified_table} AS q
            SET locked_until = now() + make_interval(secs => $3::double precision),
                locked_by = $4,
                attempts = q.attempts + 1
            FROM selected
            WHERE q.id = selected.id
            RETURNING
                q.id,
                q.data::text AS data,
                length(q.data::text) AS size,
                q.enqueued_at,
                q.schedule_at,
                q.attempts,
                q.locked_until,
                q.locked_by
            """,
            self.name,
            self.max_attempts,
            self.lease_seconds,
            lock_token,
        )

    async def _put_item(
        self,
        conn: asyncpg.Connection,
        *,
        name: str,
        data: dict,
        schedule_at: datetime | None,
    ) -> int:
        payload = self.encode(data)
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        return await conn.fetchval(
            f"""
            INSERT INTO {self.qualified_table} (q_name, data, schedule_at)
            VALUES ($1, $2::jsonb, $3)
            RETURNING id
            """,
            name,
            payload,
            schedule_at,
        )

    def _job_from_row(self, row: asyncpg.Record) -> Job:
        return Job(
            row["id"],
            self.decode(row["data"]),
            row["size"],
            row["enqueued_at"],
            row["schedule_at"],
            attempts=row["attempts"],
            locked_until=row["locked_until"],
            locked_by=row["locked_by"],
        )

    @staticmethod
    def _job_id(job: Job | int) -> int:
        if isinstance(job, Job):
            return job.id
        return int(job)
