-- 003: who sent a request. DuckDB reads the Parquet URL index with its own HTTP client; its
-- requests are read from DuckDB's log after each query and stored here beside ours, so a slow
-- lookup no longer shows as "0 requests" (week 2, S10; amends ADR-004).
ALTER TABLE ops.fetch_attempts
    ADD COLUMN client text NOT NULL DEFAULT 'strata'
    CHECK (client IN ('strata', 'duckdb'));
