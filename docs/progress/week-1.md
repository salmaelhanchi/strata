# Week 1 progress: the walking skeleton

Goal: one command turns a domain into up to 50 verified Common Crawl captures, stored as untouched
WARC bytes with a bronze row that traces back to them.

## Status by step

| Step | State | Evidence |
| --- | --- | --- |
| S0 Sourcing and table-format ADRs | Done | `docs/adr/001-table-format.md`, `docs/adr/003-sourcing-policy.md`; ADR-004 (index sources) added after the first live runs |
| S1 Repo skeleton, CI, Compose | Done; Compose run live; CI not yet run on GitHub | `pyproject.toml` (uv workspace), `.github/workflows/ci.yml`, `deploy/docker-compose.yml`, `Makefile` |
| S2 Core models and settings | Done | `strata_core/models.py`, `ids.py`, `settings.py`; property tests |
| S3 Ops database and budget ledger | Done | migrations 001–002, `ledger.py`; 50 concurrent reservations vs cap 10 give exactly 10 |
| S4 Common Crawl discovery | Done; both indexes run live (CDX down all morning; URL index answered) | `CdxIndex`, `ColumnarIndex` with a shard map, `AutoIndex` fallback (ADR-004), contract test |
| S5 Polite WARC fetcher | Done; run live: 20/20 records at 2.1 requests/s | `fetcher.py`, `ratelimit.py`, `warc.py`; every outcome-table row tested |
| S6 Bronze table | Done; live on DuckLake with the Postgres catalog (20 rows) | `lake.py`; idempotent rerun test |
| S7 CLI, report, lineage | Done; `strata demo` run live end to end | `strata demo`, `run cc`, `report`, `lineage`; CLI end-to-end test |
| S8 Verify and hand-off | In progress | see "Still to do" |

Checks after the timestamp fix (ADR-004, item 10): 109 tests passed, 1 skipped (DuckLake: extensions
can't be downloaded in the build sandbox); `ruff check`, `ruff format --check` and `mypy --strict`
clean.

## Still to do this week

- [ ] Push to GitHub and get both CI jobs green (the integration job also runs the DuckLake test)
- [x] `docker compose up`, then `strata demo` against real Common Crawl (2026-11-08, see below)
- [x] Live run of 20 records; real output in the README ("A real run")
- [x] First live URL-index lookup: shard map built for CC-MAIN-2026-43 (see item 10)
- [x] Rerun `strata discover --domain python.org --limit 20 --index columnar` after the
      timestamp fix: 0 footers read, 1 of 300 files queried, 55.3 s inside DuckDB
- [x] Record measured numbers below
- [ ] Tag `v0.1.0`

## Measured numbers (live runs, 2026-11-08, python.org, CC-MAIN-2026-43)

| Measure | Value |
| --- | --- |
| Index requests per domain (first run / rerun) | URL index: 0 of ours after the shard map, plus DuckDB's own requests (55.3 s, not yet counted); rerun from cache: 0 |
| CDX requests ending in a 504 after about 10 s | 14 of 17 on 2026-11-08; the other 3 were 500s from our malformed filter; none succeeded |
| Shard-map build: footers, retries, time | 300 footers (`subset=warc`), 0 retries, 0 without statistics; about 2.5 min at 2 requests/s (wall time not recorded) |
| URL-index files covering one domain | python.org: 1 of 300 |
| URL-index file numbers in key order | 150 of 299 neighbouring pairs (so no binary search) |
| Index rows scanned per distinct page | 100 rows gave 91 distinct pages |
| Index retries and backoff time | `auto`: 2 CDX attempts and a 2 s backoff, about 23 s, before falling back |
| Achieved fetch rate (req/s) | 2.105 (policy: 2/s, burst 2) |
| 429 / 503 responses | 0 of 20 from `data.commoncrawl.org` |
| Mean bytes per record | 8,861 B compressed (177,213 B for 20); `/about/`: 8,358 B, 43,226 B decompressed |
| Run time for 50 records | 20 records: 9.5 s, so about 25 s for 50 at 2 requests/s |
| Recompute digest mismatches | 0 of 20; quarantined 0; budget 20 of 40 |

## Changes from the plan, and why

1. **No testcontainers.** Postgres tests use `STRATA_TEST_PG_DSN` (a CI service container or the
   compose database) or a throwaway cluster from local binaries. GitHub's service containers are
   simpler than Docker-in-tests, and this also works on Windows.
2. **Discovery cache is JSON Lines, not Parquet.** Human-readable and dependency-free at this size.
   The first line records the query (cache format version, rows asked for, rows returned), so a
   larger `--limit` refetches only when the cache can't answer it, and an older cache format is
   ignored.
3. **`ops.policies` gained `max_in_flight`, `ops.jobs` gained `stop_reason`.** The fetcher needs the
   in-flight cap per host; reports need to say why a job stopped.
4. **Bronze timestamps are naive UTC `TIMESTAMP`s**, so reading them back needs no timezone library.
5. **A `strata demo` command** does what `make demo` does, because Windows usually has no `make`.
6. **Windows event loop.** psycopg's async driver needs a selector loop; the CLI and the test
   fixtures select it on Windows.
7. **Bug found and fixed:** the rate gate could stall forever when a float refill landed a hair
   under one token. It now uses a tolerance and a minimum sleep, and a property test covers it.
8. **Discovery fixed after the first live run (python.org, limit 20).** It took 4 index requests
   and about 86 s, and all 20 results were the homepage: repeat captures plus `?ref=` and
   `?featured_on=` variants. Causes: the index is sorted by SURT key, so the homepage and its
   query-string variants come first; nothing removed duplicates; and the retries were invisible,
   so about 42 s of deliberate backoff (2 + 10 + 30 s, per ADR-003) looked like slowness. Fix:
   over-fetch rows in one request, keep the newest capture per URL, skip query-string URLs by
   default (server-side `filter=!~url:?`; wrong, see 9), give index requests a 90 s timeout
   (`STRATA_INDEX_TIMEOUT_S`; useless, see 9), and record every index attempt (CLI output and
   `ops.fetch_attempts`).
9. **Second live run: our filter was malformed, and the CDX server is unreliable (ADR-004).** The
   rerun failed after 5 requests and about 2.5 minutes: three gateway `504`s after 10–11 s each,
   and two `500 Internal Error: nothing to repeat at position 0`. The 500s were our bug. In pywb's
   filter syntax `~` means "regex", not "contains", so `!~url:?` asked the server to compile `?`.
   The tests had passed because the fake server encoded the same wrong reading. What changed:
   - the filter is now `!url:?`, filters are checked locally before sending, and the fake applies
     pywb's documented semantics, including the 500 for a bad regex;
   - 500s are read, not counted: throttling (`Slow Down`) backs off, regex errors are never
     retried, and any other 500 is retried once;
   - error text shows the server's message or page title instead of raw HTML, and timings exclude
     our own rate-gate wait;
   - the 90 s index timeout went back to 30 s, since the gateway gives up at about 10 s;
   - a new `--index auto|cdx|columnar` option. `auto` tries the CDX server twice, then the Parquet
     URL index through a cached, paced, per-crawl shard map that opens only the 1–2 files (of
     300) covering the domain.

   Lesson: a test double must follow the upstream's documentation, not our reading of it.
10. **First live URL-index lookup (python.org, CC-MAIN-2026-43, `--index auto`).**
    - **What worked:** the CDX server failed twice with `504`, and discovery switched to the URL
      index as designed. `data.commoncrawl.org` honoured suffix ranges, and all 300 footers were
      read on the first try. Every file had statistics, and python.org falls in exactly 1 of 300.
    - **What failed:** the DuckDB query, with "Required module 'pytz' failed to import". Common
      Crawl's `fetch_time` is timezone-aware (`TIMESTAMPTZ` in DuckDB), and DuckDB needs `pytz` to
      turn that type into Python objects, which it returns in the machine's local time zone.
      Installing `pytz` would have hidden a second bug: our code labelled the result as UTC,
      shifting it by the local offset.
    - **Fix:** select `epoch_ms(fetch_time)` and convert to UTC in Python. That works for both
      timestamp types and needs no dependency.
    - **Why tests missed it:** the fixtures used a naive timestamp. They now use the real type plus
      a naive variant, and a test blocks `pytz`; it fails on the old code with the live error.
    - **Also corrected:** the docs said "about 900 files, 7–8 minutes". The manifest's 900 files
      are 300 each for page captures, crawl diagnostics and robots.txt. Strata reads the 300 page
      files, about 2.5 minutes once per crawl.

    Lesson, again: fixtures should copy the upstream's real schema, not a convenient one.

## Parking lot

- Count DuckDB's own HTTP requests and bytes for URL-index queries (they show as "0 requests"
  today), for example from `EXPLAIN ANALYZE` statistics.
- `strata lineage` prints whole header values; a long `Content-Security-Policy` floods the
  output. Shorten values to about 120 characters.

- Measure how often the recomputed payload digest differs from the WARC header (week 2).
- Robots.txt verdicts from Common Crawl's robots.txt captures (week 2).
