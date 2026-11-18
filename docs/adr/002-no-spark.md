# ADR-002: No Spark: one DuckDB process covers the largest unit of work

- Status: accepted
- Supersedes: the Spark step in ADR-001's original plan

## Context

The first plan had Spark processing whole crawls into silver and gold tables. Strata then became a
toolkit for scraping projects: it answers questions about one site at a time (profile, diff,
change rate) and pulls a chosen set of pages into the user's database. So the question is
whether any unit of that work is too large for one process.

The largest unit is a site catalog: every capture of a site in one crawl, read from the URL
index. Measured on `CC-MAIN-2026-43` with `strata count` and `strata gate1 run`:

| Site | URL-index rows scanned | Captures kept | DuckDB requests | Bytes read | Query time |
| --- | --- | --- | --- | --- | --- |
| peps.python.org | 1,650,100 (one row group) | 236 | 47 | 90.1 MB | 53.4 s |
| www.rfc-editor.org | 1,673,554 (one row group) | 6,852 | 55 | 95.1 MB | 80.1 s |

Both queries ran with one DuckDB thread and at most one request in flight (ADR-003), and the time
is network time: DuckDB spends it waiting for 2 MiB range reads that the politeness rules
deliberately keep sequential. The CPU work (filtering 1.7 million rows) takes well under a
second once the bytes are there. Every later question about the site (patterns, counts, shapes,
page choice, profiles) runs on the cached catalog in milliseconds.

## Decision

No Spark, and no cluster engine of any kind. Site-level work runs in DuckDB inside the Strata
process. Larger jobs are many site-level units, which the fetch worker pool (S6) spreads across
processes, each still bounded by the same per-host politeness state.

## Consequences

- Nothing to provision beyond Postgres and an S3 API; the compose file stays two services.
- Throughput is bounded by politeness (requests per second to `data.commoncrawl.org`), which
  Spark would not raise. Adding executors would only queue more requests behind the same gate.
- Revisit if a feature has to scan a whole crawl rather than one site's SURT range; nothing
  planned does.
