-- 006: the fetch pool's work queue (ADR-009). One row per page a pooled job must fetch.
--
--   pending --lease--> leased --result ok--> fetched --row in the lake--> stored
--      ^                 |  \--404/410-----> empty
--      |                 |   \--attempts spent--> failed
--      +--expired, lost, retryable, paused--+
--
-- A lease is fenced: lease_id changes on every grant, and a result is applied only while the row
-- still holds that lease_id in state 'leased'. A worker that was presumed dead and reports late
-- changes nothing. 'fetched' rows carry their bronze row (bytes already sealed by the worker), so
-- a scheduler that dies before writing them to the lake writes them when the job resumes.
CREATE TABLE ops.work_items (
    job_id           text        NOT NULL REFERENCES ops.jobs (job_id) ON DELETE CASCADE,
    capture_id       text        NOT NULL,
    seq              integer     NOT NULL,  -- the job's fetch order (md5 of the URL)
    url              text        NOT NULL,
    host             text        NOT NULL,  -- the host the request goes to
    ref              jsonb       NOT NULL,  -- the CaptureRef
    state            text        NOT NULL DEFAULT 'pending'
                     CHECK (state IN ('pending', 'leased', 'fetched', 'stored', 'empty', 'failed')),
    attempts         integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_id         text,
    worker_id        text,
    lease_expires_at timestamptz,
    not_before       timestamptz,  -- a retry waits for its backoff
    capture          jsonb,        -- the bronze row, from 'fetched' on
    error            text,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, capture_id),
    CHECK ((state = 'leased') = (lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK ((state IN ('fetched', 'stored')) = (capture IS NOT NULL))
);
CREATE INDEX work_items_pending ON ops.work_items (job_id, seq) WHERE state = 'pending';
