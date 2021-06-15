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

CREATE OR replace function pq_notify() returns trigger AS $$ BEGIN
  perform pg_notify(
    CASE length(new.q_name) > 63
      WHEN true THEN 'pq_' || md5(new.q_name)
      ELSE new.q_name
    END,
    ''
  );
  return NULL;
END $$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (select trigger_name from information_schema.triggers) THEN
        CREATE trigger pq_insert
        after INSERT ON {name}
        FOR each row
        EXECUTE PROCEDURE pq_notify();
    END IF;
END$$

