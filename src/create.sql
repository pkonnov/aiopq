CREATE TABLE IF NOT EXISTS {name} (
    id          bigserial   primary key,
    q_name      text not null check (length(q_name) > 0),
    enqueued_at timestamptz not null default current_timestamp,
    dequeued_at timestamptz,
    schedule_at timestamptz,
    data        jsonb not null
);

CREATE index IF NOT EXISTS priority_idx_{name} ON {name}
      (schedule_at nulls first, q_name)
    WHERE dequeued_at IS NULL
          AND q_name = '{name}';
