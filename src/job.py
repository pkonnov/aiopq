from src.utils import utc_format


class Job(object):
    """An item in the queue."""

    __slots__ = (
        "_data", "_size", "_id", "name", "enqueued_at", "schedule_at",
        "dequeued_at",
    )

    def __init__(
            self,
            job_id,
            data,
            size,
            enqueued_at,
            schedule_at,
    ):
        self._data = data
        self._size = size
        self._id = job_id
        self.enqueued_at = enqueued_at
        self.schedule_at = schedule_at

    def __repr__(self):
        cls = type(self)
        return (
                '<%s.%s id=%d size=%d enqueued_at=%r '
                'schedule_at=%r>' % (
                    cls.__module__,
                    cls.__name__,
                    self._id,
                    self._size,
                    utc_format(self.enqueued_at),
                    utc_format(self.schedule_at) if self.schedule_at else None,
                )
        ).replace("'", '"')
