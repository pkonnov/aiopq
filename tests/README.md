# Integration tests

Тесты запускаются в Docker Compose из трёх сервисов:

| Сервис | Назначение |
|---|---|
| `db` | PostgreSQL 14, отдельная тестовая БД |
| `app` | HTTP-сервер очереди на aiohttp (`tests/app.py`), 3 воркера |
| `tests` | отдельный контейнер с pytest, тесты ходят в `app` по HTTP и в `db` напрямую для проверок схемы |

## Запуск

```bash
docker compose -f docker-compose.test.yml up --build \
  --abort-on-container-exit --exit-code-from tests
```

`--exit-code-from tests` возвращает код pytest; `--abort-on-container-exit`
останавливает `app` и `db` после завершения тестов.

Очистка:

```bash
docker compose -f docker-compose.test.yml down -v
```

## Что проверяется

HTTP-тесты (`tests/test_http_api.py`):

- health-check приложения и БД;
- обработка задачи с кавычками/апострофами в payload;
- 25 параллельных задач не обрабатываются дважды (`attempts == 1`);
- падающая задача проходит retry и доходит до dead-letter;
- отложенная задача не забирается раньше `schedule_at`;
- невалидный JSON/payload/schedule_at возвращает 400;
- неизвестная задача — 404;
- `/stats` отдаёт агрегаты.

Тесты библиотеки (`tests/test_queue.py`) работают с отдельной временной
схемой PostgreSQL и проверяют:

- `get`/`ack`/`nack`, `NULL schedule_at` и отложенные задачи;
- `async for` отдаёт `Job`, а не coroutine;
- `get(timeout)` не удерживает соединение;
- ошибки транзакции пробрасываются, пул остаётся рабочим;
- используется индекс `queue_ready_idx`;
- lease-переизбрание и невозможность устаревшего `ack`;
- retry → dead-letter → `requeue`;
- миграция старой колонки `dequeued_at` в `finished_at`.

## Локальный запуск без Docker

Нужны PostgreSQL и Python 3.10:

```bash
python3 -m venv .test_venv
. .test_venv/bin/activate
pip install -r tests/requirements-test.txt

export DATABASE_URL=postgresql://user:123456@127.0.0.1:5432/aiopq
export API_URL=http://127.0.0.1:8080
export MAX_ATTEMPTS=2
export LEASE_SECONDS=5
export POLL_INTERVAL=0.05
export PORT=8080

PYTHONPATH=src python tests/app.py &
PYTHONPATH=src pytest -q tests
```

Тесты библиотеки создают и удаляют собственные схемы (`aiopq_test_*`,
`aiopq_legacy_*`) и не мешают данным приложения.
