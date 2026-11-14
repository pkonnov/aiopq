from datetime import datetime, timezone
from typing import Any

import orjson


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


class Job:
    """An item in the queue."""

    __slots__ = (
        "_data",
        "_size",
        "_id",
        "enqueued_at",
        "schedule_at",
        "attempts",
        "locked_until",
        "locked_by",
    )

    def __init__(
        self,
        job_id: int,
        data: Any,
        size: int,
        enqueued_at: datetime,
        schedule_at: datetime | None,
        attempts: int = 0,
        locked_until: datetime | None = None,
        locked_by: str | None = None,
    ):
        self._data = data
        self._size = size
        self._id = job_id
        self.enqueued_at = enqueued_at
        self.schedule_at = schedule_at
        self.attempts = attempts
        self.locked_until = locked_until
        self.locked_by = locked_by

    @property
    def id(self) -> int:
        return self._id

    @property
    def data(self) -> Any:
        return self._data

    @property
    def size(self) -> int:
        return self._size

    def __repr__(self):
        cls = type(self)
        return (
            "<%s.%s id=%d size=%d attempts=%d data=%s enqueued_at=%s "
            "schedule_at=%s>"
            % (
                cls.__module__,
                cls.__name__,
                self._id,
                self._size,
                self.attempts,
                orjson.dumps(self._data).decode("utf-8"),
                _format_dt(self.enqueued_at),
                _format_dt(self.schedule_at),
            )
        )
