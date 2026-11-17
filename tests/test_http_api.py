import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone

MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "2"))


async def post_task(api, payload, schedule_at=None):
    body = {"payload": payload}
    if schedule_at is not None:
        body["schedule_at"] = schedule_at.isoformat()

    async with api.post("/tasks", json=body) as response:
        assert response.status == 201, await response.text()
        data = await response.json()
        assert isinstance(data["task_id"], int)
        return data["task_id"]


async def get_task(api, task_id):
    async with api.get(f"/tasks/{task_id}") as response:
        assert response.status == 200, await response.text()
        return await response.json()


async def wait_for_status(api, task_id, expected, timeout=15):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await get_task(api, task_id)
        if last["status"] in expected:
            return last
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"task {task_id} did not reach {expected} within {timeout}s; last={last}"
    )


async def test_health(api):
    async with api.get("/health") as response:
        assert response.status == 200
        body = await response.json()
    assert body["status"] == "ok"
    assert body["queue"] == "tasks"
    assert body["workers"] >= 1


async def test_task_is_processed(api):
    payload = {"user": "O'Brien", "text": 'say "hi"'}
    task_id = await post_task(api, payload)

    task = await wait_for_status(api, task_id, {"finished"})
    assert task["attempts"] == 1
    assert task["payload"] == payload
    assert task["last_error"] is None


async def test_concurrent_tasks_are_processed_exactly_once(api):
    task_ids = await asyncio.gather(
        *(post_task(api, {"delay": 0.01, "n": index}) for index in range(25))
    )

    tasks = await asyncio.gather(
        *(wait_for_status(api, task_id, {"finished"}) for task_id in task_ids)
    )

    assert len({task["id"] for task in tasks}) == len(task_ids)
    assert all(task["attempts"] == 1 for task in tasks)


async def test_failed_task_retries_then_reaches_dead_letter(api):
    task_id = await post_task(api, {"fail": True, "error": "boom"})

    task = await wait_for_status(api, task_id, {"failed"})
    assert task["attempts"] == MAX_ATTEMPTS
    assert "boom" in (task["last_error"] or "")


async def test_scheduled_task_waits_for_schedule(api):
    scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    task_id = await post_task(api, {"scheduled": True}, scheduled_at)

    initial = await get_task(api, task_id)
    assert initial["status"] in {"pending", "locked"}

    task = await wait_for_status(api, task_id, {"finished"})
    assert task["attempts"] == 1


async def test_invalid_payload_is_rejected(api):
    async with api.post("/tasks", json={"payload": "not-an-object"}) as response:
        assert response.status == 400

    async with api.post(
        "/tasks",
        json={"payload": {}, "schedule_at": "not-a-date"},
    ) as response:
        assert response.status == 400

    async with api.post(
        "/tasks",
        data="not-json",
        headers={"Content-Type": "application/json"},
    ) as response:
        assert response.status == 400


async def test_unknown_task_returns_404(api):
    async with api.get("/tasks/999999999") as response:
        assert response.status == 404


async def test_stats_are_available(api):
    task_id = await post_task(api, {"stats": True})
    await wait_for_status(api, task_id, {"finished"})

    async with api.get("/stats") as response:
        assert response.status == 200
        stats = await response.json()

    assert stats["finished"] >= 1
    assert set(stats) == {"pending", "finished", "failed"}
    json.dumps(stats)
