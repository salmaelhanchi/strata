# Week 2 progress: Gate 1 at full scale

Goal: by the end of the week, one command acquires 1,000 verified pages from two public
documentation sites (Python Enhancement Proposals and RFC info pages), survives a deliberate kill
mid-run, records a robots.txt verdict for every page, keeps raw WARC and lake files on a local
S3-compatible store, and one lineage query proves every row traces back to intact bytes.

The crawl is pinned to `CC-MAIN-2026-43`, whose shard map already exists.

## Status by step

| Step | State | Evidence |
| --- | --- | --- |
| S9 Decisions and two spikes | Done: RustFS chosen; PEP count 192, RFC count 3,393 (live) | `strata count`; `deploy/spike/`; ADR-005, ADR-006 |
| S10 Count DuckDB's own requests | Done; checked live (python.org: 48 requests, 1 in flight) | migration 003; `DuckDbUsage`; ADR-004 amendment |
| S11 Choose pages, not just sites | Done | targets (site, pattern, pages); site catalog; md5 selection with spares; pre-flight cost estimate; `tests/unit/test_query_budget.py` |
| S12 Crash-resume | Done | write-ahead parts and manifests; `strata resume`; heartbeat; `STRATA_CRASH_AT`; `tests/integration/test_resume_and_gate1.py` |
| S13 robots.txt verdicts | Done | `strata_fetch.robots`; `ops.robots_hosts`, `ops.policy_decisions`; `tests/unit/test_robots.py` |
| S14 Raw WARC and lake files on S3 | Done locally (moto in tests); RustFS run pending (the live run used local folders) | `S3BlobStore`; DuckLake `DATA_PATH s3://`; RustFS in `deploy/docker-compose.yml` |
| S15 The 1,000-page run | Done live on local folders: 1,000/1,000, killed at record 400, finished | `gate1.toml`; rehearsal test (1,000 pages, kill at 40 %, run again) |
| S16 Normalizers | Done; agreement against the live indexes pending (step 2 below) | `strata_core.normalize`; `prove` in `strata_fetch.gate` |
| S17 Digests and compaction | Done | digest classes in the gate; `strata lake-checkpoint --older-than` |
| S18 Gate 1 demo and hand-off | One command, `strata gate1 run`; proof, commit and `v0.2.0` tag pending | `strata gate1 run` |

## Day 1 (2026-11-08)

### Done

- **`strata count SITE --pattern REGEX`.** One DuckDB scan over the shard-map files answers: captures
  (status 200, `text/html`, the host and its subdomains), distinct URLs, and distinct URLs that match
  the pattern in full, plus a sample ordered by the URL's md5 (stable across runs and DuckDB
  versions; the first N in index order are neighbours). The pattern is compiled by DuckDB's own
  regex engine (RE2) before anything is fetched, so a look-ahead that Python accepts is refused for
  free.
- **Host filter fix.** The URL-index query filtered on `url_host_registered_domain`, which is
  `python.org` for `peps.python.org`: both Gate 1 sites would have returned nothing. It now filters
  on `url_host_name` (host or subdomain); the SURT range still prunes. Fixtures gained the column.
- **DuckDB's requests counted (S10).** Each URL-index query reads DuckDB's `HTTP` and `FileSystem`
  logs afterwards: method, URL, status, duration, bytes. They are reported as `client = duckdb`
  attempts, stored in `ops.fetch_attempts`, shown by `discover`, `count`, `run` and `report`, and
  charged to the job after the query. A job they overspend stops before fetching anything.
- **DuckDB's concurrency bounded.** Measured with a threaded local server: `threads = 1` still had
  6 requests in flight (Parquet prefetching). `http_client_pool_capacity = threads` holds it to the
  thread count with the same number of requests; `disable_parquet_prefetching` would have cost 12×
  the requests. Tests assert both the server's view and DuckDB's log.
- **Object-store spike kit.** `deploy/spike/compose.yaml` (RustFS, SeaweedFS) and
  `deploy/spike/check_store.py` (5 required checks, 3 informational). Run against `moto`'s S3 server
  to check the script itself: 8/8 passed.
- **Subject switch in code and docs.** `Source.EPO_OPS` and `Source.WIKIDATA` became
  `Source.OFFICIAL_API`; ADR-003's example now names `peps.json`.
- Tests: 128 passed, 0 skipped (the DuckLake-on-Postgres test now runs locally too); `ruff` and
  `mypy --strict` clean. (139 after the fixes below.)

### To run on a machine with Docker and internet access

1. Site counts (S9), one per site:

   ```
   uv run strata count peps.python.org --crawl CC-MAIN-2026-43 --pattern '^https://peps\.python\.org/pep-\d{4}/$'
   uv run strata count www.rfc-editor.org --crawl CC-MAIN-2026-43 --pattern '^https://www\.rfc-editor\.org/info/rfc\d+$'
   ```

   Targets: at least 400 and 600 matching URLs. Otherwise rebalance, or switch to a fallback site
   (docs.python.org, www.w3.org/TR).
2. Object-store spike: see ADR-005, "Method".
3. A forced refresh of the week 1 lookup, to see DuckDB's real cost:
   `uv run strata discover --domain python.org --crawl CC-MAIN-2026-43 --limit 20 --index columnar`
   after deleting `data/cache/refs/CC-MAIN-2026-43/python.org.jsonl`.

## Measured numbers (live, 2026-11-08, CC-MAIN-2026-43)

| Measure | Value |
| --- | --- |
| peps.python.org: captures / distinct URLs / matching `pep-\d{4}/` | 233 / 213 / 192; DuckDB: 21 requests, 35.6 MB, 26.6 s |
| www.rfc-editor.org | first count 0: our `www` key bug (fixed, see below); rerun pending |
| python.org lookup, fresh (week 1 showed it as "0 requests") | 48 DuckDB requests, 90.1 MB, 66.0 s, at most 1 in flight |
| Same with `--threads 2` | not run yet |
| Object store | RustFS, `sha256:ed9210d6…93dcc2`, 8/8 checks (ADR-005) |

## Issues found on day 1

1. **`www` hosts got an empty key range. Fixed.** Common Crawl builds `url_surtkey` with the `surt`
   library, which drops a leading `www.` (`https://www.rfc-editor.org/info/rfc9309` becomes
   `org,rfc-editor)/info/rfc9309`). Ours kept it, so `www.rfc-editor.org` was looked up in
   `org,rfc-editor,www)`, where nothing can be: 0 pages for 23 requests and 40 MB. The tests agreed
   with the code because the fake built its keys with the same function, the week 1 lesson
   again. Now: the library's rule (ASCII `www[0-9]*.`), CDX answers filtered to the host too, the
   key test pinned to the library's real output, and a cross-check on 4,663 random hosts with no
   difference (ADR-004, decision 15). Hypothesis also caught that Python's `\d` matches non-ASCII
   digits while the library's bytes regex doesn't.
2. **192 PEP pages, not 400. Open.** The crawl has 213 distinct URLs on the host. Options:
   rebalance toward RFC pages, union several crawls, or switch to the fallback site. Decided once
   the RFC count is known.
3. **The demo's default budget (60) couldn't cover a fresh lookup** (48 DuckDB requests plus 20
   fetches). Fixed: `run cc` and `demo` default to 150.

Tests after the fixes: 139 passed; `ruff` and `mypy --strict` clean.

## Days 2 to 5 (built 2026-11-14, tested locally)

### What was built

- **Targets instead of one domain (S11).** A job takes `[[target]]` entries (site, RE2 pattern,
  pages) from a TOML file (`gate1.toml`). Each site is read once per crawl into a **site catalog**
  (one DuckDB query, cached), so counts, URL shapes and page choices after it are free. Pages are
  chosen in md5 order of the URL (stable, spread across the site), with spares for pages
  robots.txt rules out.
- **Pre-flight cost estimate.** The shard map now keeps each row group's key range, row count and
  the layout of the columns a site query reads. Before a query runs, its cost is estimated from
  those statistics, **reserved** from the job's budget, and settled to the real count afterwards
  (refund or charge). A query the budget can't cover is not run at all. The estimate counts what
  DuckDB was measured to send: per file a HEAD, its last 16 KiB and the rest of the footer; per
  row group one read per run of column chunks less than 16 KiB apart.
- **Crash-resume (S12).** Each record's bytes and its manifest line are fsynced before anything
  else happens; at each flush the part is sealed into the store (with its SHA-256) before its rows
  reach the lake; inserts are idempotent. `strata resume JOB` replays the manifests, refunds
  reservations whose request never left, and fetches only what is missing. A heartbeat tells a
  dead job from a running one. `STRATA_CRASH_AT=after_manifest:7` and friends kill the process
  at an exact point, for tests.
- **robots.txt verdicts (S13).** For every host, the newest robots.txt capture in the same crawl
  (URL index, `robotstxt` subset), redirects followed, parsed with Protego for the `strata`
  token. Every candidate gets a verdict in `ops.policy_decisions` before any page request, and
  every bronze row points at the robots.txt capture its verdict rests on.
- **S3 (S14).** Raw parts are sealed into a bucket (`STRATA_RAW_STORE=s3`) and DuckLake writes to
  `s3://` through a DuckDB secret. Setup creates the buckets.
- **Normalizers (S16).** Pure functions (dates with their precision, statuses per publisher,
  names without addresses, titles for comparison), tested with Hypothesis properties. Two
  throwaway extractors read a PEP page's and an RFC info page's header; normalizing writes
  `silver.documents` and compares title, status and date with `peps.json` and `rfc-index.txt`
  (downloaded once, with their provenance).
- **Compaction (S17).** `strata lake-checkpoint --older-than '7 days'` merges small files and,
  with a retention, deletes the merged ones. The gate sorts every row's recomputed payload
  digest into `match`, `truncated` or `unexplained`.
- **Gate 1 (S18).** `strata gate1 run gate1.toml`: one lineage query over every row the job
  selected, joined to its robots.txt capture, then each row's bytes read back and checked; plus
  budget, duplicates, pages per target and normalizer agreement.

### Measured in the local rehearsal

Two sites (210 PEP pages and 900 RFC info pages on the fake Common Crawl, plus pages the
patterns must skip), robots.txt disallowing two RFC pages, raw parts and lake files on an S3
server (moto), killed at 40 %, then resumed:

| Check | Result |
| --- | --- |
| Pages per target | 175/175 and 825/825; 2 disallowed pages replaced by spares |
| Duplicates | 1,000 rows, 1,000 capture ids, 1,000 URLs |
| Robots verdict on every row | allowed = 1,000; every row points at one of 2 stored robots.txt captures |
| Stored bytes, index digest, recomputed digest | 1,000/1,000 each |
| Budget | 1,017 of 1,200 spent, equal to the charged attempts; 1 reservation refunded on resume |
| Recovered on resume | 24 rows rebuilt from manifests, not fetched again |
| Normalizer agreement | 99.9 % on title, status and date (two planted disagreements) |

Five crash points (after a record's bytes, after its manifest line, after a seal, after an
insert, twice) and an outside `kill` all resume to the same pages an uninterrupted run picks.
Tests: 232 passed, 0 skipped; `ruff` and `mypy --strict` clean.

### Issues found while building, all fixed

1. **A robots.txt redirect to the `www` host looked like a loop.** Keys drop `www` (and the
   scheme), so `example.org/robots.txt` and `www.example.org/robots.txt` share a key; the loop
   check used the key alone. Now a loop is the same scheme, host and key twice, and the capture
   whose scheme matches is preferred. An http capture redirecting to an https one that was never
   captured is `unresolved` (needs review), not a loop.
2. **Our SURT keys kept trailing slashes.** The `surt` library drops them (`/pep-0008/` becomes
   `/pep-0008`) and collapses repeated slashes. Only exact-key robots.txt lookups use the full
   key, but the fake used it for every page. Pinned to 20 more of the library's outputs.
3. **The first estimate was 6 times too high.** It counted one request per column chunk; DuckDB
   reads a row group's neighbouring chunks in one request. Measured on a local server, then
   modelled as merged runs: estimate and actual now agree on the test file.
4. **DuckLake inlines small inserts by default** (up to 10 rows in DuckDB 2.0), so
   `STRATA_LAKE_INLINING_ROWS=0` did not mean "off". The limit is now always passed.
5. **Sealing trusted the hash it was given.** S3 refuses a body whose SHA-256 doesn't match, but
   the hash came from the caller; a part changed on disk after its rows were written would have
   been stored under the old hash by a store that doesn't verify (moto doesn't). Both stores now
   hash the file before sealing. The spike gained a `bad-digest` check for the store's half.
6. **A second job couldn't pass the gate.** Bronze rows are keyed by capture, so pages a first job
   stored belonged to it; a later job found them "not new" and its own row count stayed short.
   Now each job records which captures it selected; pages already in the lake are reused, not
   fetched again (a rerun costs zero requests), and the gate proves the job's selection.
7. **Windows expanded `.*` into file names** (Click's default). The `strata` command now turns
   that off.

## Live run (2026-11-15, CC-MAIN-2026-43, local folders)

`strata count www.rfc-editor.org --pattern '^https://www\.rfc-editor\.org/info/rfc\d+/$'`:
3,393 matching URLs of 4,629 distinct; 49 DuckDB requests, 95.1 MB, 67.6 s.

`strata gate1 run gate1.toml` with `STRATA_CRASH_AT=after_manifest:400`: the process died after
its 400th stored record (2 robots.txt records and 398 pages). Resumed:

| | Result |
| --- | --- |
| Pages | 175/175 PEP, 825/825 RFC; 398 already stored, 602 fetched |
| Recovered on resume | 23 rows rebuilt from the manifests; 1 reservation refunded (never sent) |
| Budget | 1,065 of 1,200 |
| Stored | 38.6 MB; 0 quarantined; 0 digest mismatches |
| robots.txt | peps.python.org: no robots.txt in the crawl (allow); www.rfc-editor.org: 93 of 3,393 candidates disallowed, replaced by spares |

### What the live run showed, and what changed

1. **The cost estimate was 8 times too low on Common Crawl's files** (6 requests estimated, 49
   sent; bytes right: 94.8 MB vs 95.1 MB). The model came from a small test file. Measured on a
   1.6-million-row row group: DuckDB splits any read over its remote block size (2 MiB,
   `external_file_cache_remote_max_block_size`) into blocks. The estimate now counts blocks per
   run, reads the block size from DuckDB, and DuckDB's merge gap is pinned to the model's 16 KiB;
   on the test file it sends 54 requests and the estimate says 55. The shard map now caches each
   row group's byte ranges (format 3), so the next run re-reads the footers once (uncharged).
2. **The reported rate was wrong** (4.514 req/s): it divided the whole job's requests, an
   earlier run's and 300 footer reads included, by the resumed run's time. Pages were in fact
   fetched at about 2 per second. A run now reports its own page requests over its own fetching
   time, and `strata report` shows the busiest second per host over the whole job.
3. **RustFS wasn't used:** the S3 settings were in a file named `env`, not `.env`, and lacked
   `STRATA_RAW_STORE=s3` and the lake's data path. `.env.example` now holds one S3 block to
   uncomment, with its own catalog database: DuckLake refuses to reopen a lake with a different
   data path (measured), so an S3 lake can't take over the local one.
4. **Too many commands** (migrate, lake-init, oracle fetch, run, jobs, resume with a copied id,
   normalize, check). Now one command does it all and continues after a crash; see below.

### One command, and jobs bound to their storage

- `strata gate1 run gate1.toml` sets up what's missing (ops schema, the lake's catalog database,
  buckets, lake tables), downloads the publishers' indexes if absent, then finds this targets
  file's job: none, start it; interrupted, stopped or failed, continue it; heartbeat still
  fresh, refuse; succeeded, leave it. Then it normalizes, proves every row and exits 0 or 1.
- Each job records where its data lives (staging folder, raw store, lake; no credentials;
  migration 005). The same targets file on the same storage is the same job; `resume` refuses a
  job whose storage differs from the current settings.
- Removed as redundant: `lake-init`, `oracle fetch`, `normalize`, `gate1 check`. `migrate` does
  the whole setup; the library functions stay for the Django workers.

## To run on your machine (PowerShell)

1. Rename `env` to `.env` and replace its contents with the S3 block of `.env.example`
   (uncommented).
2. `$env:STRATA_CRASH_AT = "after_manifest:400"; uv run strata gate1 run gate1.toml`, then
   `Remove-Item Env:STRATA_CRASH_AT`, wait a minute (the dead run's heartbeat must go stale) and
   run `uv run strata gate1 run gate1.toml` again. It finishes the job and prints the proof.

Without the S3 block, step 2 without the crash proves today's local pages instead: the job
from this morning predates storage records, so a new job selects the same 1,000 pages, finds
them all stored and makes no requests.

## Parking lot

- DuckDB's requests are bounded in flight but not paced to 2 requests/s. Routing them through
  Strata's own client is a week 3 option (ADR-004, amendment).
- 90 MB for one domain's 100 rows: DuckDB downloads whole column chunks of the row groups holding
  the domain. A narrower first pass (keys and URLs only) could cut it; week 3.
- The verdict doesn't name the robots.txt rule that matched: Protego doesn't expose it. The
  stored robots.txt bytes let anyone re-derive it (ADR-006, amendment).
- A request in flight when the process dies is refunded on resume although it may have reached
  Common Crawl: at most one per in-flight slot (2).
