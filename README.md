# Strata

[![ci](https://github.com/salmaelhanchi/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/salmaelhanchi/strata/actions/workflows/ci.yml)

**An MCP-native, provenance-first web data sourcing engine.** Agents plan and run polite
acquisitions over Common Crawl, the Wayback Machine and official APIs, and every row in the
lakehouse traces back to the capture, policy decision and parser version that produced it.

> Status: **week 2 of 6, Gate 1.** One targets file turns two public documentation sites into
> 1,000 verified Common Crawl captures: a robots.txt verdict for every page, raw WARC parts and
> lake files on an S3-compatible store, a run that survives being killed, and one query that
> proves every row. The MCP server, gRPC scheduler, Spark jobs and Terraform arrive in later
> weeks ([week 1](docs/progress/week-1.md), [week 2](docs/progress/week-2.md)).

## What works today

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
docker compose -f deploy/docker-compose.yml up -d --wait    # Postgres and RustFS
uv run strata demo --domain python.org --limit 20
```

**macOS, Linux, WSL**

```bash
uv sync
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
Common Crawl and an S3 server ([week 2 log](docs/progress/week-2.md)).

### A real run (8 November 2026)

Common Crawl's CDX server was timing out that morning, so this run used the Parquet URL index
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
| `strata count SITE --pattern REGEX [--crawl C]` | A site's eligible pages in a crawl, how many URLs match the pattern in full, URL shapes, and the cost estimated before the query; needs no database |
| `strata discover --domain D [--crawl C] [--limit N] [--index auto\|cdx\|columnar]` | One newest capture per distinct page, with any failed index request listed; needs no database |
| `strata run cc --domain D [--pattern R] [--limit N] [--budget B] [--index ...]` | Fetch one site into the bronze table |
| `strata gate1 run TARGETS.toml [--oracle DIR]` | Run a targets file (several sites, a pattern and a page count each) to the end, or continue it after a crash; then normalize and prove every row |
| `strata jobs` / `strata resume JOB_ID [--force]` | Newest jobs and their state / continue a `run cc` job that died or stopped (on the storage it started with) |
| `strata report JOB_ID` | Attempts per outcome, budget used, the busiest second per host, robots verdicts, rows in the lake |
| `strata lineage CAPTURE_ID` | Upstream pointer, our copy, digests, archived response headers |
| `strata migrate` / `strata lake-checkpoint [--older-than I]` | Set everything up (schema, catalog database, buckets, lake tables; runs also do this) / merge small lake files and apply a retention |
| `strata demo --domain D` | Migrate, fetch, report and trace one row |

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

Add `--json` to `discover`, `count`, `run cc`, `resume`, `gate1 run`, `report` and `lineage` for machine-readable output. Settings
come from `STRATA_*` environment variables or a `.env` file ([.env.example](.env.example)).

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
  strata-fetch/   Common Crawl index sources and shard maps, rate gate, fetcher, WARC checks,
                  blob stores (local, S3), robots.txt verdicts, jobs and resume, documents, gate
  strata-cli/     Typer CLI over the library functions
tests/            unit, integration (Postgres, DuckLake, S3, crash-resume), fake Common Crawl
deploy/           docker compose (Postgres 16, RustFS); spike/ checks S3-compatible stores (ADR-005)
gate1.toml        the Gate 1 targets
docs/adr/         decisions
docs/progress/    weekly progress logs
```

## Decisions

- [ADR-001: DuckLake with a Postgres catalog](docs/adr/001-table-format.md)
- [ADR-003: Sourcing policy](docs/adr/003-sourcing-policy.md)
- [ADR-004: Index sources and how discovery fails](docs/adr/004-index-sources.md) (amended twice in week 2)
- [ADR-005: Object storage after MinIO](docs/adr/005-object-storage.md) (RustFS)
- [ADR-006: robots.txt verdicts](docs/adr/006-robots-verdicts.md)
- [Common Crawl caveats and bottlenecks](docs/common-crawl.md)

## What it refuses to do

- No CAPTCHA solving, proxy rotation or fingerprint evasion. A block stops the host and tells a human.
- No bulk redistribution of third-party content; pipelines and small samples with provenance only.
- No home-made policy standards: robots.txt, source terms and existing signals such as TDMRep.

## License

[Apache-2.0](LICENSE)
