-- 001: ops schema. Mutable operational state lives here; immutable captures live in the lake.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.jobs (
    job_id      text PRIMARY KEY,
    spec        jsonb       NOT NULL,
    budget_cap  integer     NOT NULL CHECK (budget_cap >= 0),
    state       text        NOT NULL DEFAULT 'created'
                CHECK (state IN ('created', 'running', 'succeeded', 'stopped', 'failed')),
    stop_reason text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per (job, host) plus one row with host '*' for the job total.
-- A request is reserved here, and committed, before it is sent.
CREATE TABLE ops.budget_ledger (
    job_id     text        NOT NULL REFERENCES ops.jobs (job_id) ON DELETE CASCADE,
    host       text        NOT NULL,
    cap        integer     NOT NULL CHECK (cap >= 0),
    spent      integer     NOT NULL DEFAULT 0 CHECK (spent >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, host),
    CHECK (spent <= cap)
);

CREATE TABLE ops.policies (
    host          text PRIMARY KEY,
    max_rps       numeric     NOT NULL CHECK (max_rps > 0),
    burst         integer     NOT NULL DEFAULT 1 CHECK (burst >= 1),
    max_in_flight integer     NOT NULL DEFAULT 1 CHECK (max_in_flight >= 1),
    crawl_delay_s numeric     NOT NULL DEFAULT 0 CHECK (crawl_delay_s >= 0),
    user_agent    text,
    notes         text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.fetch_attempts (
    attempt_id     bigserial PRIMARY KEY,
    job_id         text        NOT NULL REFERENCES ops.jobs (job_id) ON DELETE CASCADE,
    host           text        NOT NULL,
    url            text        NOT NULL,
    attempt        integer     NOT NULL CHECK (attempt >= 1),
    started_at     timestamptz NOT NULL,
    elapsed_ms     integer,
    http_status    integer,
    outcome        text        NOT NULL
                   CHECK (outcome IN ('ok', 'definitive_empty', 'retryable_error', 'rate_limited', 'blocked')),
    retry_after_ms integer,
    bytes          integer,
    error          text
);

CREATE INDEX fetch_attempts_job_idx ON ops.fetch_attempts (job_id);
