# Strata

[![ci](https://github.com/salmaelhanchi/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/salmaelhanchi/strata/actions/workflows/ci.yml)

**A Common Crawl toolkit. Before you scrape a site, ask Strata.** It tells you what a site
contains and what its robots.txt allows, and pulls the pages you need from Common Crawl
instead of from the site, with proof of where every row came from. It's a helper for scraping
projects, not a scraper.

> Status: **site profiles, crawl-to-crawl diffs and change rates work** (from the index only,
> no database needed), and **Gate 1 passed**: one targets file turns two public documentation
> sites into 1,000 verified Common Crawl captures, with a robots.txt verdict for every page, raw
> WARC parts and lake files on an S3-compatible store, a run that survives being killed, and one
> query that proves every row. Export into your own database, a Python SDK, a web console and an
> MCP server come next.

## Before you scrape: profile, diff, changes

Three questions a scraping project asks first, answered from Common Crawl's URL index and the
crawl's own robots.txt captures. No page of the site is fetched and no database is needed.

```powershell
uv run strata profile rfc-editor.org --crawls 2 --html docs/examples/rfc-editor.org.html
uv run strata diff rfc-editor.org            # the two newest crawls
uv run strata changes rfc-editor.org --crawls 3
```

**What does the site hold, and what does robots.txt allow?** Real reports:
[rfc-editor.org](docs/examples/rfc-editor.org.html) and
[peps.python.org](docs/examples/peps.python.org.html) (open the HTML files locally; they load
nothing from anywhere).

```text
rfc-editor.org: 4,686 page(s) in CC-MAIN-2026-43 (6,975 captures)
  CC-MAIN-2026-39: 3,560 page(s) (5,180 captures)
  hosts: 6; languages: eng 4,681, - 5
  URL shapes (pages, allowed by robots.txt):
    /info/rfcN/…                               3,393, 3,393 allowed
    /rfc/rfcN.html                               319, 319 allowed
    /info/bcpN/…                                  73, 73 allowed
    /search/?…                                    53, 53 allowed
  robots.txt www.rfc-editor.org: parsed; allowed 4,629
  robots.txt mailman3.rfc-editor.org: unavailable; allowed 22
  robots.txt queue.rfc-editor.org: parsed; allowed 14
  cost: 99 request(s), 186.7 MB read by DuckDB; catalogs from cache: 0/2
```

Two site catalogs (one per crawl) and three robots.txt lookups, read in 3 minutes. The
peps.python.org profile cost 73 requests (131.7 MB) and found its robots.txt archived as a 404,
which RFC 9309 reads as "everything allowed". The first lookup in a crawl also reads that crawl's
300 Parquet footers once (about 2.5 minutes at 2 requests/s); they are shared by every later
lookup and never charged to a budget.

**What changed between two crawls?** From the payload digests the index stores for every
capture, both catalogs already cached by the profile:

```text
rfc-editor.org: CC-MAIN-2026-39 -> CC-MAIN-2026-43: added 4,058, removed 2,932, changed 619, same 9
  index requests: 0; page fetches: 0
  by URL shape (added / removed / changed / same):
    /info/rfcN/…                              2,829  2,407    564      0
    /rfc/rfcN.html                              314    322      0      5
    /info/bcpN/…                                 66     42      7      0
  removed = not captured in the later crawl (Common Crawl doesn't revisit every page)
```

**What is worth re-scraping?** Per URL shape, how often a page captured in two consecutive crawls
had changed. A third crawl's catalog cost 49 requests here (plus that crawl's 300 footer reads):

```text
rfc-editor.org over 3 crawls (CC-MAIN-2026-34 .. CC-MAIN-2026-43): 939 of 953 re-captures changed (99%)
  changing (changed / re-captured, pages seen):
    /info/rfcN/…                              100%     831/831      7,584
    /search/?…                                100%      28/28          98
  stable (changed / re-captured, pages seen):
    /rfc/rfcN.html                              0%       0/9          919
```

Read it as a scraper would: an RFC's text (`/rfc/rfcN.html`) never changed between crawls, while
every re-captured `/info/rfcN/` page did. That page embeds something that differs on every
capture, so its digest says nothing about the RFC. Track changes on the RFC text, not the info
page. The same command on peps.python.org found 25 of 40 re-captured PEPs changed: PEPs are
living documents.

## Gate 1 and the job engine

How the packages and the two paths (index only, jobs) fit together:
[docs/architecture.md](docs/architecture.md).

```
targets file ──► site catalog per crawl (cached) ──► pages chosen in md5 order
             ──► robots.txt verdict per page (Common Crawl's own robots.txt captures)
             ──► budget ledger (estimate reserved first) ──► polite byte-range fetch
             ──► WARC verification ──► staged part + manifest (fsync'd write-ahead log)
             ──► part sealed into S3 (SHA-256) ──► bronze row in DuckLake ──► lineage, gate check
```

- **Discovery** through Common Crawl's CDX server or the Parquet URL index. A cached per-crawl
  shard map means a lookup opens one or two of the 300 Parquet files, and a site is read once per
  crawl into a catalog, so choosing pages, counting and reruns cost nothing
  ([ADR-004](docs/adr/004-index-sources.md)).
- **Budgets** reserved and committed in Postgres *before* each request, retries included.
  DuckDB's own requests are estimated from Parquet statistics before a query, reserved, and
  settled to the real count afterwards; a query the budget can't cover is never run.
- **Politeness** per host: a token bucket, an in-flight cap and a host-wide pause on 429/503,
  with backoff and `Retry-After` handling ([ADR-003](docs/adr/003-sourcing-policy.md)).
- **robots.txt verdicts** from the robots.txt Common Crawl captured in the same crawl (RFC 9309,
  Protego), decided before any page request; every row points at the robots.txt bytes its
  verdict rests on ([ADR-006](docs/adr/006-robots-verdicts.md)).
- **Crash-resume.** A record's bytes and its manifest line are on disk before anything else
  happens, and a part is sealed before its rows reach the lake. `strata resume` replays what a
  dead run made durable, refunds reservations that never left, and fetches only what is missing.
- **Verification** of every record: one WARC response, the right URL, and the WARC payload digest
  equal to the index's. Bad bytes are retried once, then quarantined.
- **Provenance**: each bronze row keeps two pointers (Common Crawl's original and our copy),
  digests and the policy verdict. `strata lineage` walks a row back to its bytes and re-checks
  them; `strata gate1 run` does it for every row of a job.
- **Normalizers** checked against the publishers' own indexes (`peps.json`, `rfc-index.txt`).
- **Idempotent reruns**: capture ids are derived from the upstream location, so a rerun reuses
  stored pages and makes no requests.

## Quickstart

You need [uv](https://docs.astral.sh/uv/) and Docker (for Postgres).

**Windows (PowerShell)**

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"   # once
uv sync
uv run strata profile peps.python.org --html peps.html      # no database needed
docker compose -f deploy/docker-compose.yml up -d --wait    # Postgres and RustFS, for jobs
uv run strata demo --domain python.org --limit 20
```

**macOS, Linux, WSL**

```bash
make dev                                    # Postgres and RustFS, dependencies, schema
uv run strata profile peps.python.org --html peps.html
make demo DOMAIN=python.org LIMIT=20
```

Strata identifies itself to Common Crawl as `strata/0.1 (+https://github.com/salmaelhanchi/strata)`
(ADR-003). If you fork it, set `STRATA_USER_AGENT` to your fork's URL.

`demo` migrates the ops schema, prepares the lake, fetches the captures, prints the run report and
traces the first row. The default budget is 150 requests: a fresh URL-index lookup for python.org
costs about 48 of DuckDB's own requests, which are charged too, before the 20 page fetches. No Docker? Set `STRATA_LAKE_LOCAL_FILE=data/lake.duckdb` to use a plain DuckDB
file for the lake; the ops database still needs Postgres.

### Gate 1: two sites, 1,000 pages

`gate1.toml` asks for 175 PEP pages and 825 RFC info pages from `CC-MAIN-2026-43`. With the S3
block of [.env.example](.env.example) uncommented in `.env` (raw parts and lake files on RustFS):

```powershell
uv run strata gate1 run gate1.toml
```

It sets up what's missing (schema, catalog database, buckets, lake tables), downloads the
publishers' indexes once, runs the job, normalizes the pages and proves every row: one lineage
query, each row's bytes re-checked, robots verdicts, budget, agreement with the indexes. If the
run dies, the same command continues the same job.

To see that, set `$env:STRATA_CRASH_AT = "after_manifest:400"` first: the process exits on the
spot after its 400th stored record. Unset it, wait a minute for the dead run's heartbeat to go
stale, and run the command again. The same run is rehearsed in the test suite against a fake
Common Crawl and an S3 server ([progress log](docs/progress/week-2.md)).

### The real Gate 1 run

Raw parts and lake files on RustFS, starting from an empty cache, while Common Crawl was
throttling clients with `503 SlowDown`. What happened, in order:

| Run | What happened |
| --- | --- |
| 1 | The URL-index shard map read 247 of 300 Parquet footers, then the job failed cleanly on repeated 503s: 0 pages, 0 budget spent. The 247 footers stay cached. |
| 2 | Discovery found 192 PEP and 3,393 RFC candidates. Both robots.txt lookups hit transient errors, and the job wrongly reported success with 0 pages. **Fixed:** a robots.txt lookup that fails for a technical reason now fails the job, and a job short of its pages ends as stopped. |
| 3 | `STRATA_CRASH_AT=after_manifest:400`: the process died after its 400th durable record (2 robots.txt files, 398 pages). |
| 4 | Same command, crash point unset. **23 rows rebuilt from the write-ahead manifests, 1 reserved request refunded (never sent), 398 pages reused, none fetched twice.** It stopped at 930/1,000 when the 1,200-request budget ran out: 135 page retries under throttling, all charged. |
| 5 | Budget raised to 1,500 in `gate1.toml`, which makes it a new job. Discovery and robots.txt came from cache (0 requests); the job fetched the last 70 pages in 73 requests, then proved every row. |

The proof printed by `strata gate1 run gate1.toml`:

```text
silver.documents: 1000 row(s); 0 page(s) without an extractor
  pep.date         175/175   100.0%
  pep.status       175/175   100.0%
  pep.title        166/175   94.9%
  rfc.date         824/825   99.9%
  rfc.status       825/825   100.0%
  rfc.title        818/825   99.2%
Gate 1 for job_20261118T165613Z_12e79d85: 1000 row(s)
  PASS  job finished: state succeeded
  PASS  every selected page is in the lake: 1000/1000 (70 stored by this job, 930 by earlier jobs)
  PASS  pages per target: peps.python.org 175/175, www.rfc-editor.org 825/825
  PASS  no duplicates: 1000 rows, 1000 capture ids, 1000 URLs
  PASS  robots verdict on every row: allowed=1000; rows without a stored robots.txt capture: 0
  PASS  stored bytes intact (SHA-256): 1000/1000
  PASS  index digest = WARC header digest: 1000/1000
  PASS  robots.txt bytes intact: 1000/1000 rows, 2 robots.txt capture(s)
  info  recomputed payload digest: match=1000
  PASS  budget: spent 73/1500; charged attempts 73
  PASS  normalizers agree with the official indexes: 99.4% on title, status and date (target 95%); 0 page(s) not in the index
PASSED
```

The first proof failed at 70.6% agreement, and that was useful: rfc-editor.org's redesigned info
pages no longer show a date (the extractor now reads their `citation_*` tags), and
`rfc-index.txt` stopped zero-padding numbers below 1000 (82 RFCs looked missing). The
disagreements left are the publishers' own formatting: the index hyphenates words at line wraps
and peps.json writes code in backticks. 38.6 MB of WARC records sit on RustFS, and every row
traces back to them:

```text
capture 586a16297c817c428c98536a77654a7d5db2c9bd886022748477d133937ff51a
  url:            https://www.rfc-editor.org/info/rfc5885/
  our copy:       s3://strata-raw/job_20261118T165613Z_12e79d85/part-00000.warc.gz @ 0 (+46562 B)
  bytes intact:   yes (sha256 f16102de2fde9ee1...)
  digests:        index sha1:TY6FR46YVXRR5OJ3YMRIN2MTREYTEPWB
                  warc  sha1:TY6FR46YVXRR5OJ3YMRIN2MTREYTEPWB  -> index matches header: yes
                  ours  sha1:TY6FR46YVXRR5OJ3YMRIN2MTREYTEPWB  -> recompute matches: yes
  policy verdict: allowed (robots.txt capture 1fca4631fb03b4c1...)
```

### A real one-site run

Common Crawl's CDX server was timing out at the time, so this run used the Parquet URL index
(`--index columnar`). Discovery had been cached by an earlier `strata discover`, so the run spent
only the 20 page fetches (`--budget 40`). Without the cache, that lookup costs 48 more requests,
90 MB and about a minute, measured later the same day.

```text
job job_20261108T110540Z_87977a2e: succeeded
  python.org in CC-MAIN-2026-43: 20 distinct page(s) (from cache, 0 requests, via columnar)
  outcomes: ok=20; rows written: 20
  budget: 20/40; rate-limited attempts: 0
  stored 177213 B in 9.5 s (2.105 req/s)
  quarantined: 0; recompute digest mismatches: 0
4/4 lineage of the first capture
  url:            https://www.python.org/about/
  upstream:       https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-43/segments/1791170054603.53/warc/CC-MAIN-20261011165953-20261011195953-00568.warc.gz [bytes=828656605-828664962]
  upstream time:  2026-10-11 17:42:07   fetched by strata: 2026-11-08 11:05:40 UTC
  our copy:       data/raw/job_20261108T110540Z_87977a2e/part-00000.warc.gz @ 0 (+8358 B)
  bytes intact:   yes
  digests:        index sha1:PIRPGW75GSYHCQJU36ZOHGXDEAANKL2X
                  warc  sha1:PIRPGW75GSYHCQJU36ZOHGXDEAANKL2X  -> index matches header: yes
                  ours  sha1:PIRPGW75GSYHCQJU36ZOHGXDEAANKL2X  -> recompute matches: yes
  archived reply: HTTP/1.1 200
```

Twenty range requests at the policy's 2 per second, every record checked against the index, and
the stored bytes are the same 8,358 bytes Common Crawl serves.

## Commands

| Command | What it does |
| --- | --- |
| `strata crawls` | Newest Common Crawl crawl ids (cached for a day) |
| `strata profile SITE [--crawls N] [--html FILE]` | Pages per crawl, URL shapes, statuses, MIME types, languages, hosts, path depth and robots.txt (rules, sitemaps, what it allows of each shape), from the index only; an HTML report on request; needs no database |
| `strata diff SITE [--from A] [--to B]` | Added, removed and changed pages between two crawls, from the index's payload digests; no page fetched; needs no database |
| `strata changes SITE [--crawls N]` | How often each URL shape changed between consecutive crawls: what is worth re-scraping; needs no database |
| `strata count SITE --pattern REGEX [--crawl C]` | A site's eligible pages in a crawl, how many URLs match the pattern in full, URL shapes, and the cost estimated before the query; needs no database |
| `strata discover --domain D [--crawl C] [--limit N] [--index auto\|cdx\|columnar]` | One newest capture per distinct page, with any failed index request listed; needs no database |
| `strata run cc --domain D [--pattern R] [--limit N] [--budget B] [--index ...]` | Fetch one site into the bronze table |
| `strata gate1 run TARGETS.toml [--oracle DIR]` | Run a targets file (several sites, a pattern and a page count each) to the end, or continue it after a crash; then normalize and prove every row |
| `strata jobs` / `strata resume JOB_ID [--force]` | Newest jobs and their state / continue a `run cc` job that died or stopped (on the storage it started with) |
| `strata report JOB_ID` | Attempts per outcome, budget used, the busiest second per host, robots verdicts, rows in the lake |
| `strata lineage CAPTURE_ID` | Upstream pointer, our copy, digests, archived response headers |
| `strata migrate` / `strata lake-checkpoint [--older-than I]` | Set everything up (schema, catalog database, buckets, lake tables; runs also do this) / merge small lake files and apply a retention |
| `strata demo --domain D` | Set up, fetch a few pages, report and trace one row |

Discovery asks the index for five times as many rows as pages wanted (at most 1,000) in a single
request, keeps the newest capture of each URL, and skips URLs with a query string (`?ref=`,
`?utm_source=` and similar tracking variants of the same page) unless `--include-query-strings`
is given. In a job, every index request is logged to `ops.fetch_attempts` next to the record
fetches, so `strata report` shows index retries and the time spent waiting on them.

`--index auto` (the default) tries Common Crawl's CDX server twice, then falls back to the Parquet
URL index. The first URL-index lookup in a crawl reads each of its 300 Parquet footers once, at
2 requests/s, which takes about 2.5 minutes. That map is cached, so later lookups open only the
one or two files that can hold the domain. While the CDX server is down, `--index columnar` skips
the CDX attempts. A 403 or a malformed query never triggers the fallback. DuckDB reads those files
with its own HTTP client; its requests are read back from DuckDB's log, stored with
`client = 'duckdb'`, charged to the job and capped at the thread count in flight. See
[ADR-004](docs/adr/004-index-sources.md) and the list of
[Common Crawl caveats](docs/common-crawl.md) behind it.

Add `--json` to `profile`, `diff`, `changes`, `discover`, `count`, `run cc`, `resume`, `gate1 run`, `report` and `lineage` for machine-readable output. Settings
come from `STRATA_*` environment variables or a `.env` file ([.env.example](.env.example)).
Logs go to stderr with a `run_id` on every line (and the `job_id` inside a job); set
`STRATA_LOG_LEVEL=info` to see job, index-query and sealing events, `STRATA_LOG_FORMAT=json` for
JSON lines.

No Python? The image runs the CLI as a non-root user:

```powershell
docker build -f deploy/Dockerfile -t strata .
docker run --rm -v strata-data:/data strata profile peps.python.org --html /data/peps.html
```

## Tests

```bash
uv run pytest                    # unit + integration
uv run ruff check . && uv run mypy
```

No test touches the internet (pytest-socket allows localhost only). A fake Common Crawl serves real
WARC records, URL-index Parquet files (page and robots.txt subsets) and the crawl manifest, with
Range support and scripted failures. An in-process S3 server (moto) stands in for RustFS. Crash
tests run the real `strata` command and kill it, at chosen points of the write path and from
outside, then resume it. Tests that read Parquet over HTTP need DuckDB's httpfs extension, which DuckDB installs on
first use; they are skipped where it can't. Postgres tests use `STRATA_TEST_PG_DSN` when
set (CI, or the compose database), otherwise a throwaway cluster from local PostgreSQL binaries, and
skip if neither exists. On Windows:

```powershell
$env:STRATA_TEST_PG_DSN = "postgresql://strata:strata@localhost:5432/strata_ops"
uv run pytest
```

## Layout

```
packages/
  strata-core/    models, deterministic ids, settings, migrations, ops store, budget ledger, lake,
                  normalizers
  strata-fetch/   adapters: Common Crawl index sources (cc/: CDX, URL index, site catalog, shard
                  map), rate gate, fetcher, WARC checks, blob stores (local, S3), robots.txt,
                  jobs and resume, documents, gate
  strata-services/ use cases every interface calls: profile, diff, changes; their wiring
  strata-cli/     Typer CLI: parses, calls a service or a job, prints
tests/            unit, integration (Postgres, DuckLake, S3, crash-resume), fake Common Crawl
deploy/           docker compose (Postgres 16, RustFS), the CLI's Dockerfile; spike/ checks
                  S3-compatible stores (ADR-005)
gate1.toml        the Gate 1 targets
docs/architecture.md  packages, the two paths (index only, jobs), what each request passes
docs/adr/         decisions
docs/examples/    site profiles of real sites (HTML)
docs/progress/    weekly progress logs
```

## Decisions

- [ADR-001: DuckLake with a Postgres catalog](docs/adr/001-table-format.md)
- [ADR-002: No Spark: one DuckDB process covers the largest unit of work](docs/adr/002-no-spark.md)
- [ADR-003: Sourcing policy](docs/adr/003-sourcing-policy.md)
- [ADR-004: Index sources and how discovery fails](docs/adr/004-index-sources.md) (amended twice)
- [ADR-005: Object storage after MinIO](docs/adr/005-object-storage.md) (RustFS)
- [ADR-006: robots.txt verdicts](docs/adr/006-robots-verdicts.md)
- [ADR-007: Code layout: a uv workspace, with the use cases in their own package](docs/adr/007-code-layout.md)
- [Common Crawl caveats and bottlenecks](docs/common-crawl.md)

## Patterns this code is built on

- **A status state machine per item, enforced in SQL.** A job's state and every attempt's outcome
  are `CHECK`-constrained columns; the ledger's `CHECK (spent <= cap)` makes overspending a
  database error, not a bug to notice later.
- **Idempotent reruns.** Capture ids are hashes of the upstream location (crawl, WARC file,
  offset), so a rerun or a resume reuses what is stored and inserts nothing twice.
- **Budget before request.** Every request, retries and DuckDB's own included, is reserved
  before it is sent; a query the budget can't cover is never run.
- **Durable before visible.** Bytes and their manifest line are fsync'd, the part is sealed into
  the store, and only then does a row reach the lake.
- **Provenance on every row.** Two pointers (Common Crawl's and ours), digests, the robots.txt
  capture behind the verdict, and the job.
- **Politeness as config.** Per-host token bucket, in-flight cap, pause on 429/503 with
  `Retry-After`, robots.txt from the same crawl, and an honest User-Agent, all from
  `ops.policies` and settings.

## What it refuses to do

- No CAPTCHA solving, proxy rotation or fingerprint evasion. A block stops the host and tells a human.
- No bulk redistribution of third-party content; pipelines and small samples with provenance only.
- No home-made policy standards: robots.txt, source terms and existing signals such as TDMRep.

## License

[Apache-2.0](LICENSE)
