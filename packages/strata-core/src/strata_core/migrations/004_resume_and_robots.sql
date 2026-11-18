-- 004: week 2. Crash-resume (S12) and robots.txt verdicts (S13, ADR-006).

-- A running job writes a heartbeat; one whose heartbeat went stale died mid-run and can resume.
ALTER TABLE ops.jobs ADD COLUMN heartbeat_at timestamptz;
ALTER TABLE ops.jobs DROP CONSTRAINT IF EXISTS jobs_state_check;
ALTER TABLE ops.jobs ADD CONSTRAINT jobs_state_check
    CHECK (state IN ('created', 'running', 'succeeded', 'stopped', 'failed', 'interrupted'));

-- Footer and manifest reads are logged but not charged to a job (ADR-004). Resume compares the
-- charged attempts with the ledger to find reservations that were never sent, and refunds them.
ALTER TABLE ops.fetch_attempts ADD COLUMN charged boolean NOT NULL DEFAULT true;

-- What each host's robots.txt said for a job: the basis of every verdict below it.
CREATE TABLE ops.robots_hosts (
    job_id            text        NOT NULL REFERENCES ops.jobs (job_id) ON DELETE CASCADE,
    host              text        NOT NULL,
    basis             text        NOT NULL
                      CHECK (basis IN ('parsed', 'unavailable', 'unreachable', 'missing', 'unresolved')),
    robots_capture_id text,
    robots_url        text,
    archived_status   integer,
    crawl_delay_s     numeric,
    content_usage     text[]      NOT NULL DEFAULT '{}',
    error             text,
    decided_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, host)
);

-- One verdict per candidate page, decided before any page request. `selected` marks the pages
-- the job keeps (each target's first allowed pages in md5 order): the job's rows are exactly
-- these captures, whichever job stored them first, so a rerun reuses stored pages and the gate
-- still proves every row the job asked for.
CREATE TABLE ops.policy_decisions (
    job_id            text        NOT NULL REFERENCES ops.jobs (job_id) ON DELETE CASCADE,
    url               text        NOT NULL,
    host              text        NOT NULL,
    verdict           text        NOT NULL
                      CHECK (verdict IN ('allowed', 'disallowed', 'needs_review')),
    robots_capture_id text,
    capture_id        text        NOT NULL,
    selected          boolean     NOT NULL DEFAULT false,
    decided_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, url)
);
CREATE INDEX policy_decisions_selected ON ops.policy_decisions (job_id) WHERE selected;
