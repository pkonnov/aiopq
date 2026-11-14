# P0 remediation — aiopq

- Дата: 2026-11-14
- Статус: **P0 закрыты**, интеграционные проверки пройдены
- Основание: код-ревью от 2026-11-14 (P0-1 … P0-6)
- Область: только блокеры P0; P1/P2 не входили в эту итерацию

## Итог

Все шесть P0-проблем исправлены. Очередь теперь:

- не склеивает SQL-строки из пользовательских данных;
- реально доставляет `Job` через `async for`;
- не держит соединение и транзакцию во время ожидания;
- не маскирует ошибки БД и корректно закрывает пул;
- имеет индекс, соответствующий запросу, и умеет выбирать задачи с `schedule_at IS NULL`;
- использует lease + `ack`/`nack`, счётчик попыток и dead-letter state, поэтому падение воркера не теряет задачу.

## Что исправлено

### P0-1. SQL-инъекция и поломка SQL на кавычках

Файлы: [src/aiopq.py](../../src/aiopq.py), [src/create.sql](../../src/create.sql)

- `_put_item` теперь использует `$1`, `$2::jsonb`, `$3` и `fetchval`.
- `_pull_item` фильтрует по `$1` (`q_name`), а не подставляет имя очереди в SQL.
- Имена таблицы/схемы проходят через `_quote_ident()`: regexp-валидация,
  проверка длины и кавычки PostgreSQL.
- `Literal` больше не используется для построения SQL (сам класс оставлен,
  его удаление — P2).
- `q_name` не может быть пустым при `put()`.

Проверено: данные `{"user": "O'Brien"}` и имя очереди
`x'; DROP TABLE queue; --` сохраняются и читаются как данные; таблица
`queue` остаётся на месте.

### P0-2. `async for` отдавал coroutine

Файл: [src/aiopq.py](../../src/aiopq.py)

- `AQueueIterator.__anext__` теперь делает `return await self.queue.get()`.
- Добавлена защитная обработка `None` → `StopAsyncIteration`.
- `Job` стал полноценным объектом: публичные `id`, `data`, `size`,
  `attempts`, `locked_until`, `locked_by`; `repr` больше не печатает
  `b"..."` и не делает `.replace("'", '"')`.

Проверено: `async for job in queue` возвращает `Job`, а не coroutine;
`ack(job)` из цикла работает.

### P0-3. Busy-loop и `sleep` внутри транзакции

Файл: [src/aiopq.py](../../src/aiopq.py)

- Ожидание вынесено из транзакции: соединение берётся только на время
  `_pull_item`, после чего возвращается в пул.
- `Queue.get(timeout=...)` теперь **максимальное время ожидания**: при
  отсутствии задачи возвращает `None`; `timeout=None` — ждать бесконечно.
- `poll_interval` вынесен в отдельный параметр (по умолчанию `0.5`).
- `AioPQ(timeout=...)` и `Queue(timeout=...)` оставлены как deprecated-alias
  для `poll_interval`, чтобы не ломать старый вызов.
- Убрана пауза перед возвратом уже найденной задачи.
- `AioPQ.__getitem__` больше не создаёт лишний `Queue` при попадании в кэш.

Проверено: `get(timeout=0.2)` возвращает `None` примерно через 0.2 с;
`pool.get_idle_size() == pool.get_size()` после операции.

### P0-4. Маскировка ошибок и жизненный цикл пула

Файл: [src/aiopq.py](../../src/aiopq.py)

- `Queue.transaction()` больше не ловит `Exception`, не вызывает `close()`
  и не подменяет исходную ошибку. Контекстный менеджер просто делает
  `pool.acquire()` + `conn.transaction()` и пробрасывает исключение.
- `Queue.close()` удалён: очередь не владеет общим пулом.
- Добавлен `AioPQ.close()`, который делает `await self.pool.close()`.
- Убраны обращения к несуществующему `asyncpg.Pool.id`.
- `create()` использует тот же исправленный `transaction()`.

Проверено: `UndefinedTableError` из транзакции доходит до вызывающего кода;
после ошибки пул остаётся рабочим, новые задачи добавляются и забираются.

### P0-5. Индекс, порядок и `NULL schedule_at`

Файлы: [src/create.sql](../../src/create.sql), [src/aiopq.py](../../src/aiopq.py)

Индекс теперь соответствует запросу:

```sql
CREATE INDEX IF NOT EXISTS queue_ready_idx ON queue
    (q_name, schedule_at nulls first, id)
 WHERE finished_at IS NULL
   AND failed_at IS NULL;
```

Запрос выбирает задачи с учётом `NULL`:

```sql
AND (schedule_at IS NULL OR schedule_at <= now())
ORDER BY schedule_at NULLS FIRST, id
```

- Убраны мёртвые условия `schedule_at <= now()` без `NULL`-ветки.
- Имя индекса выводится из имени таблицы и квотируется; при использовании
  `schema` больше не получается `priority_idx_schema.queue`.
- `put(..., schedule_at=None)` означает «готова сразу»; naive datetime
  трактуется как UTC.

Проверено: задача с `schedule_at IS NULL` забирается; задача с будущим
`schedule_at` не забирается; `EXPLAIN` при `enable_seqscan=off` использует
`queue_ready_idx` для реального имени очереди `jobs`.

### P0-6. At-most-once → lease + ack/nack + DLQ

Файлы: [src/create.sql](../../src/create.sql), [src/aiopq.py](../../src/aiopq.py), [src/job.py](../../src/job.py)

Новая схема (добавляется через `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`):

- `locked_until timestamptz` — срок аренды;
- `locked_by text` — уникальный lease-token текущего получения;
- `attempts integer not null default 0` — число получений;
- `last_error text` — последняя ошибка;
- `finished_at timestamptz` — успешный `ack`;
- `failed_at timestamptz` — dead-letter state после `max_attempts`;
- `dequeued_at` оставлен только для миграции старых строк.

Логика:

- `_pull_item` атомарно выбирает задачу `FOR UPDATE SKIP LOCKED`, ставит
  `locked_until = now() + lease_seconds`, уникальный `locked_by` и
  увеличивает `attempts`.
- `ack(job)` завершает задачу только если lease-token совпадает.
- `nack(job, error=...)/nack(job, error=..., delay=...)` возвращает задачу
  в очередь; при `attempts >= max_attempts` переводит её в DLQ.
- `expired` CTE в `_pull_item` переводит в DLQ задачи, у которых
  закончилась аренда и исчерпаны попытки (сценарий «воркер упал и не вызвал
  `nack`»).
- `get_failed()` возвращает DLQ-задачи; `requeue(job_id)` возвращает их в
  очередь с обнулением `attempts`.
- Старые строки с `dequeued_at` при `create()` мигрируют в
  `finished_at`, чтобы не быть доставленными повторно.

Проверено:

- после истечения `lease_seconds` задача приходит повторно с `attempts=2`;
- старый lease-token не может сделать `ack` после переизбрания задачи;
- после третьей неудачи `nack()` переводит задачу в DLQ;
- задача, у которой воркер «упал», после трёх попыток и истечения lease
  попадает в DLQ;
- `requeue()` возвращает задачу с `attempts=1`;
- legacy-строка с `dequeued_at` получает `finished_at` и не переотправляется.

## Изменения API

- `AioPQ(pool, poll_interval=0.5, lease_seconds=30, max_attempts=3)`.
- `AioPQ(timeout=...)` / `Queue(timeout=...)` — deprecated alias для
  `poll_interval`.
- `Queue.get(timeout=None)` — максимальное ожидание, а не интервал polling.
- `Queue.put(name, data, schedule_at=None)` — `schedule_at` теперь
  необязателен.
- `Queue.ack(job)` / `Queue.nack(job, error=..., delay=...)` — обязательный
  шаг после обработки.
- `Queue.get_failed()` / `Queue.requeue(job_or_id)` — работа с DLQ.
- `AioPQ.close()` — корректное закрытие пула.
- `Job` получил публичные `id`, `data`, `size`, `attempts`, `locked_until`,
  `locked_by`.

Пример в [README.md](../../README.md) и [tmp/aiopq_debug.py](../../tmp/aiopq_debug.py)
обновлён: `async for` + `ack`/`nack`, исправлено имя очереди при `put`,
`web.json_response`, закрытие пула.

## Проверка

Проверка выполнялась на локальном PostgreSQL 14 в отдельной временной схеме;
после завершения схема удалялась (`DROP SCHEMA ... CASCADE`), постоянных
изменений в БД не осталось. Использовались `asyncpg 0.27.0` и `orjson`.

Временный скрипт проверки (не коммитился) прошёл все пункты:

```text
ok - create() is idempotent
ok - put() returns an integer id
ok - quoted queue name is stored as data
ok - table survived a quote in q_name
ok - get() returns the job
ok - data round-trips
ok - attempts is incremented on pull
ok - ack() finishes the job
ok - async for yields a Job, not a coroutine
ok - ack() works from async for
ok - get(timeout) returns None when empty
ok - get(timeout) waited ~0.2s (got 0.203s)
ok - get() returns the connection to the pool
ok - NULL schedule_at is pulled
ok - future schedule_at is not pulled
ok - leased job
ok - first attempt
ok - expired lease is redelivered
ok - second attempt
ok - nack() requeues
ok - third attempt
ok - nack() dead-letters at max_attempts
ok - dead-lettered job is not pulled
ok - get_failed() sees the job
ok - requeue() restores the job
ok - requeued job is pullable again
ok - expired lease is redelivered to a new pull
ok - stale lease cannot ack
ok - current lease can ack
ok - crash attempt 1
ok - crash attempt 2
ok - crash attempt 3
ok - exhausted job is not pulled after lease expiry
ok - crashed job reached the dead-letter state
ok - transaction() propagates database errors
ok - pool remains usable after a failed transaction
ok - job can be pulled after error
ok - ready index is used for q_name='jobs'
ok - legacy dequeued_at migrated to finished_at
ok - legacy row got attempts default

ALL P0 CHECKS PASSED
```

## Что осталось за рамками (P1/P2)

Эти пункты из ревью **не** исправлялись и остаются техдолгом:

- упаковка/импорты (`src` layout, `__init__.py`, `from job` vs `from src.utils`,
  `packages` в `pyproject.toml`, `create.sql` в wheel);
- автоматические тесты и CI; `tests/` всё ещё в `.gitignore`;
- `poetry.lock` (asyncpg 0.13.0) не синхронизирован с `pyproject.toml`
  (asyncpg 0.27.0); `greenlet` не используется; метаданные проекта —
  плейсхолдеры;
- retention/очистка `finished_at`-строк, batch-операции, jitter/backoff,
  `LISTEN/NOTIFY`;
- строгая типизация оставшихся мест (`Union`, `Any`, старые identity
  `dumps`/`loads`, `Literal`, `utc_format`);
- конфигурация схемы на уровне `AioPQ`, миграционный фреймворк (сейчас
  миграция встроена в `create.sql`);
- документация по установке/запуску и graceful shutdown вне примера.

## Риски и эксплуатационные заметки

- Семантика доставки теперь **at-least-once**: без `ack` задача вернётся
  после `lease_seconds`.
- `lease_seconds` должен быть больше максимального времени обработки;
  для долгих задач нужно либо увеличивать lease, либо добавить продление
  аренды (пока не реализовано).
- `max_attempts` считает получения, включая упавшие воркеры. После
  исчерпания задача остаётся в таблице как DLQ, но не выбирается.
- `create()` идемпотентен и добавляет новые колонки к старой таблице; старые
  строки с `dequeued_at` помечаются завершёнными.
- Перед выкаткой на существующую БД стоит сделать backup: `create()`
  выполняет `ALTER TABLE` и `UPDATE` в одной транзакции.
