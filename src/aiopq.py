import asyncio
from datetime import datetime
from typing import Union

import orjson
import logging
import os
from contextlib import asynccontextmanager
from weakref import WeakValueDictionary

from asyncpg.protocol.protocol import Record

from .job import Job
from .utils import Literal


logger = logging.getLogger(__name__)


class AioPQ:
    table_name = 'queue'
    template_schema = os.path.dirname(__file__)
    queue_class = None

    def __init__(self, pool, queue_class=None):
        self.pool = pool
        if queue_class is not None:
            self.queue_class = queue_class
        self.queues = WeakValueDictionary()
        logger.debug('Init AioPQ')

    def __getitem__(self, name):
        try:
            return self.queues[name]
        except KeyError:
            factory = self.queue_class
            if factory is None:
                factory = Queue
            return self.queues.setdefault(
               name, factory(name, self.pool)
            )

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
        return await self.queue.get()


class Queue:

    dumps = loads = staticmethod(lambda data: data)

    encode = staticmethod(orjson.dumps)
    decode = staticmethod(orjson.loads)

    def __init__(self, name, pool=None, table_name='queue', schema=None):
        self.pool = pool
        self.name = name
        self.table_name = Literal((schema + "." if schema else "") + table_name)

    def __aiter__(self):
        return AQueueIterator(self)

    async def get(self, timeout: float = None) -> Union[Job, None]:
        while True:
            if timeout:
                await asyncio.sleep(timeout)
            async with self.transaction() as init_conn:
                job = await self._pull_item(init_conn)
            if job:
                (
                    job_id,
                    data,
                    size,
                    enqueued_at,
                    schedule_at,
                ) = job
                decoded = self.decode(data.encode())
                _job = Job(
                    job_id, self.loads(decoded), size, enqueued_at, schedule_at
                )
                logger.debug('Delete %s from queue', _job)
                return _job

    async def put(self, name: str, data: dict, schedule_at: datetime) -> int:
        async with self.transaction() as init_conn:
            job_id = await self._put_item(init_conn,
                                          name=name,
                                          data=data,
                                          schedule_at=schedule_at)
            return job_id[0].get('id')

    async def _pull_item(self, conn):
        # This method uses the following query:
        """
        WITH
          selected AS (
            SELECT * FROM {table_name}
            WHERE
              q_name = '{name}' AND
              dequeued_at IS NULL AND
              schedule_at <= now()
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
          )
        SELECT
          id,
          data::text,
          length(data::text),
          enqueued_at AT TIME ZONE 'utc' AS enqueued_at,
          schedule_at AT TIME ZONE 'utc' AS schedule_at
        FROM selected
        """
        q = self._pull_item.__doc__.format(table_name=self.table_name, name=self.name)
        job = await conn.fetch(q)
        if job:
            return job[0]
        return

    async def _put_item(self, conn, *, name: str, data: dict, schedule_at: datetime) -> Record:
        """
        INSERT INTO {table_name} (q_name, data, schedule_at)
        VALUES ('{name}', '{data}', '{schedule_at}') RETURNING id
        """
        data = self.encode(data)
        q = self._put_item.__doc__.format(table_name=self.table_name,
                                          name=name,
                                          data=data.decode('utf-8'),
                                          schedule_at=str(schedule_at))
        return await conn.fetch(q)

    @asynccontextmanager
    async def transaction(self):
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction() as _:
                    yield conn
        except Exception as ex:
            logger.exception(ex)
            await self.close()

    async def close(self):
        self.pool.close()
        logger.debug('Connection with id %s', self.pool.id)
