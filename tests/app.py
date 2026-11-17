"""A small HTTP service used by the integration tests.

The process is intentionally separate from PostgreSQL and from the test
runner: docker-compose starts three services (db, app, tests), and the tests
only talk to the app over HTTP.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
import aiohttp
from aiohttp import web

from aiopq import AioPQ

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "2"))
LEASE_SECONDS = float(os.getenv("LEASE_SECONDS", "5"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.05"))
PORT = int(os.getenv("PORT", "8080"))


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


async def _process(job) -> None:
    payload = job.data
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    if payload.get("fail"):
        raise RuntimeError(str(payload.get("error", "requested failure")))

    delay = float(payload.get("delay", 0))
    if delay > 0:
        await asyncio.sleep(delay)


async def _worker(queue) -> None:
    async for job in queue:
        try:
            await _process(job)
        except Exception as exc:
            outcome = await queue.nack(job, error=exc)
            logger.warning(
                "job %s failed (requeued=%r): %s", job.id, outcome, exc
            )
        else:
            acked = await queue.ack(job)
            if not acked:
                logger.warning("job %s lease was lost before ack", job.id)


async def health(request: web.Request) -> web.Response:
    app = request.app
    async with app["pool"].acquire() as conn:
        await conn.fetchval("SELECT 1")
    return web.json_response(
        {
            "status": "ok",
            "queue": app["queue"].name,
            "workers": len(app["workers"]),
        }
    )


async def create_task(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except (ValueError, aiohttp.ContentTypeError):
        raise web.HTTPBadRequest(text="invalid JSON body")

    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text="body must be a JSON object")

    payload = body.get("payload")
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="payload must be a JSON object")

    schedule_at = None
    raw_schedule = body.get("schedule_at")
    if raw_schedule is not None:
        if not isinstance(raw_schedule, str):
            raise web.HTTPBadRequest(text="schedule_at must be an ISO string")
        try:
            schedule_at = datetime.fromisoformat(
                raw_schedule.replace("Z", "+00:00")
            )
        except ValueError:
            raise web.HTTPBadRequest(text="schedule_at must be a valid ISO date")

    queue = request.app["queue"]
    task_id = await queue.put(queue.name, payload, schedule_at)
    return web.json_response({"task_id": task_id}, status=201)


async def get_task(request: web.Request) -> web.Response:
    try:
        task_id = int(request.match_info["task_id"])
    except (KeyError, ValueError):
        raise web.HTTPNotFound()

    app = request.app
    queue = app["queue"]
    row = await app["pool"].fetchrow(
        f"""
        SELECT id,
               data::text AS data,
               attempts,
               last_error,
               finished_at,
               failed_at,
               locked_until
        FROM {queue.qualified_table}
        WHERE id = $1
          AND q_name = $2
        """,
        task_id,
        queue.name,
    )
    if row is None:
        raise web.HTTPNotFound()

    now = datetime.now(timezone.utc)
    if row["failed_at"] is not None:
        status = "failed"
    elif row["finished_at"] is not None:
        status = "finished"
    elif row["locked_until"] is not None and row["locked_until"] > now:
        status = "locked"
    else:
        status = "pending"

    return web.json_response(
        {
            "id": row["id"],
            "status": status,
            "attempts": row["attempts"],
            "payload": json.loads(row["data"]),
            "last_error": row["last_error"],
            "finished_at": _iso(row["finished_at"]),
            "failed_at": _iso(row["failed_at"]),
        }
    )


async def stats(request: web.Request) -> web.Response:
    app = request.app
    queue = app["queue"]
    row = await app["pool"].fetchrow(
        f"""
        SELECT
            count(*) FILTER (
                WHERE finished_at IS NULL AND failed_at IS NULL
            ) AS pending,
            count(*) FILTER (
                WHERE finished_at IS NOT NULL
            ) AS finished,
            count(*) FILTER (
                WHERE failed_at IS NOT NULL
            ) AS failed
        FROM {queue.qualified_table}
        WHERE q_name = $1
        """,
        queue.name,
    )
    return web.json_response(
        {
            "pending": row["pending"],
            "finished": row["finished"],
            "failed": row["failed"],
        }
    )


async def _startup(app: web.Application) -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    aiopq = AioPQ(
        pool,
        poll_interval=POLL_INTERVAL,
        lease_seconds=LEASE_SECONDS,
        max_attempts=MAX_ATTEMPTS,
    )
    await aiopq.create()
    queue = aiopq[QUEUE_NAME]

    app["pool"] = pool
    app["aiopq"] = aiopq
    app["queue"] = queue
    app["workers"] = [
        asyncio.create_task(_worker(queue), name=f"worker-{i}")
        for i in range(WORKER_COUNT)
    ]
    logger.info(
        "started queue=%s workers=%d max_attempts=%d",
        QUEUE_NAME,
        WORKER_COUNT,
        MAX_ATTEMPTS,
    )


async def _cleanup(app: web.Application) -> None:
    workers = app.get("workers", [])
    for task in workers:
        task.cancel()
    if workers:
        await asyncio.gather(*workers, return_exceptions=True)

    aiopq: AioPQ | None = app.get("aiopq")
    if aiopq is not None:
        await aiopq.close()


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_post("/tasks", create_task)
    app.router.add_get(r"/tasks/{task_id:\d+}", get_task)
    app.router.add_get("/stats", stats)
    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)
    return app


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
