CREATE TABLE IF NOT EXISTS jobs (
    id              uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    request_hash    bytea NOT NULL,
    payload         jsonb NOT NULL,
    status          text NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    lease_token     uuid,
    lease_expires_at timestamptz,
    worker_id       text,
    attempt_count   integer NOT NULL DEFAULT 0,
    result          jsonb,
    error           text,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at      timestamptz,
    finished_at     timestamptz,
    CHECK ((status = 'running') = (lease_token IS NOT NULL)),
    CHECK ((status = 'running') = (lease_expires_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS jobs_claim_idx
    ON jobs (created_at)
    WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS jobs_list_idx ON jobs (created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS job_effects (
    job_id       uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    lease_token  uuid NOT NULL,
    worker_id    text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
