-- Where a job's data lives: the local staging folder of its write-ahead log, the raw store and
-- the lake (no credentials). A job is resumed only against the same storage, and a targets file
-- run against new storage is a new job. NULL for jobs created before this migration: their
-- storage is unknown, so they are never resumed or matched.
ALTER TABLE ops.jobs ADD COLUMN storage jsonb;
