import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from weakref import WeakValueDictionary

from .utils import Literal, utc_format


logger = logging.getLogger(__name__)


class AioPQ:
    table_name = 'queue'
    template_schema = os.path.dirname(__file__)
    queue_class = None

    def __init__(self, *args, **kwargs):
        queue_class = kwargs.pop('queue_class', None)
        if queue_class is not None:
            self.queue_class = queue_class
        self.params = args, kwargs
        self.queues = WeakValueDictionary()

    def __getitem__(self, name):
        try:
            return self.queues[name]
        except KeyError:
            factory = self.queue_class
            if factory is None:
                factory = Queue
            return self.queues.setdefault(
                name, factory(name, *self.params[0], **self.params[1])
            )

    def close(self):
        self[''].close()

    async def create(self):
        queue = self['']

        with open(os.path.join(self.template_schema, 'create.sql')) as f:
            sql = f.read()

        async with queue.transaction() as init_conn:
            await init_conn.execute(sql.format(name=queue.table_name))


class AQueueIterator(object):

    def __init__(self, queue):
        self.queue = queue

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get(timeout=self.timeout)


class Queue:
    """
    conn : asyncpg.connect
    """

    dumps = loads = staticmethod(lambda data: data)

    encode = staticmethod(json.dumps)
    decode = staticmethod(json.loads)

    def __init__(self, name, conn=None, pool=None, table_name='queue', schema=None, **kwargs):
        self.conn = conn
        self.pool = pool
        self.name = name
        self.table_name = Literal((schema + "." if schema else "") + table_name)

    def __aiter__(self):
        return AQueueIterator(self)

    async def get(self, block: bool = True, timeout: float = None):
        while True:
            if timeout:
                await asyncio.sleep(timeout)
            async with self.transaction() as init_conn:
                job = await self._pull_item(init_conn, block)
            if job:
                (
                    job_id,
                    data,
                    size,
                    enqueued_at,
                    schedule_at,
                ) = job
                decoded = self.decode(data)

                return Job(
                    job_id, self.loads(decoded), size, enqueued_at, schedule_at
                )

    async def listen(self, conn):
        try:
            await conn.execute('LISTEN %s' % self.name)
        except Exception as ex:
            logger.error(ex)

    async def _pull_item(self, conn, block=True):
        # This method uses the following query:
        """
        WITH
          selected AS (
            SELECT * FROM {table_name}
            WHERE
              q_name = '{name}' AND
              dequeued_at IS NULL
            ORDER BY schedule_at nulls first, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
          ),
          updated AS (
            UPDATE {table_name} AS t SET dequeued_at = current_timestamp
            FROM selected
            WHERE
              t.id = selected.id AND
              (t.schedule_at <= now() OR t.schedule_at is NULL)
            RETURNING t.data, length(t.data::text) AS length
          )
        SELECT
          id,
          (SELECT data::text FROM updated),
          (SELECT length FROM updated),
          enqueued_at AT TIME ZONE 'utc' AS enqueued_at,
          schedule_at AT TIME ZONE 'utc' AS schedule_at
        FROM selected
        """
        job = await conn.fetch(self._pull_item.__doc__.format(table_name=self.table_name, name=self.name))
        if job:
            return job[0]
        return

    async def _put_item(self, conn, **kwargs):
        """
        INSERT INTO {table_name} (q_name, data, schedule_at, expected_at)
        VALUES ({name}, {data}, {schedule_at}, {expected_at}) RETURNING id
        """
        return await conn.execute(self._put_item.__doc__.format(table_name=self.table_name, **kwargs))

    @asynccontextmanager
    async def transaction(self):
        tr = self.conn.transaction()
        await tr.start()
        try:
            yield self.conn
        except Exception as ex:
            await tr.rollback()
            raise ex
        else:
            await tr.commit()


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
