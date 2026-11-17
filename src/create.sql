CREATE TABLE IF NOT EXISTS {table} (
    id           bigserial   primary key,
    q_name       text        not null check (length(q_name) > 0),
    enqueued_at  timestamptz not null default current_timestamp,
    schedule_at  timestamptz,
    locked_until timestamptz,
    locked_by    text,
    attempts     integer     not null default 0,
    last_error   text,
    finished_at  timestamptz,
    failed_at    timestamptz,
    data         jsonb       not null
);

-- Upgrade a table created by the previous schema.  The old code used
-- dequeued_at as a terminal marker; preserve those jobs as finished.
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS locked_until timestamptz;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS locked_by text;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS attempts integer not null default 0;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS last_error text;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS finished_at timestamptz;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS failed_at timestamptz;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS dequeued_at timestamptz;

UPDATE {table}
   SET finished_at = dequeued_at
 WHERE finished_at IS NULL
   AND dequeued_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS {index} ON {table}
       (q_name, schedule_at nulls first, id)
    WHERE finished_at IS NULL
      AND failed_at IS NULL;
