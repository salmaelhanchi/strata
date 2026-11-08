# Strata

**An MCP-native, provenance-first web data sourcing engine.** Agents plan and run polite
acquisitions over Common Crawl, the Wayback Machine and official APIs, and every row in the
lakehouse traces back to the capture, policy decision and parser version that produced it.

> Status: **week 1 of 6, the walking skeleton.** One command turns a domain into verified Common
> Crawl captures, stored as untouched WARC bytes plus bronze rows that trace back to them. The MCP
> server, gRPC scheduler, Spark jobs and Terraform arrive in later weeks
> ([progress](docs/progress/week-1.md)).

## What works today

```
domain ──► Common Crawl index (cached) ──► budget ledger ──► polite byte-range fetch
       ──► WARC verification ──► raw WARC copy (fsync'd) ──► bronze row in DuckLake ──► lineage
```

- **Discovery** through Common Crawl's CDX server, with a fallback to the Parquet URL index when
  that server is down. A cached per-crawl shard map means a lookup opens only one or two of the
  300 Parquet files of page captures. Answers are cached on disk, so reruns make zero requests
  ([ADR-004](docs/adr/004-index-sources.md)).
- **Politeness** per host: a token bucket, an in-flight cap and a host-wide pause on 429/503,
  with backoff and `Retry-After` handling ([ADR-003](docs/adr/003-sourcing-policy.md)).
- **Budgets** reserved and committed in Postgres *before* each request, retries included, so a crash
  can never lose a spent request and a job can never overspend.
- **Verification** of every record: one WARC response, the right URL, and the WARC payload digest
  equal to the index's. Bad bytes are retried once, then quarantined.
- **Provenance**: each bronze row keeps two pointers (Common Crawl's original and our copy), digests
  and the policy verdict. `strata lineage` walks a row back to its bytes and re-checks them.
- **Idempotent reruns**: capture ids are derived from the upstream location, so re-running a job
  adds nothing.

## Quickstart

You need [uv](https://docs.astral.sh/uv/) and Docker (for Postgres).

**Windows (PowerShell)**

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"   # once
uv sync
docker compose -f deploy/docker-compose.yml up -d --wait
$env:STRATA_USER_AGENT = "strata/0.1 (+https://github.com/<you>/strata)"
uv run strata demo --domain python.org --limit 20 --budget 40
```

**macOS, Linux, WSL**

```bash
uv sync
export STRATA_USER_AGENT="strata/0.1 (+https://github.com/<you>/strata)"
make demo DOMAIN=python.org LIMIT=20 BUDGET=40
```

`demo` migrates the ops schema, prepares the lake, fetches the captures, prints the run report and
traces the first row. No Docker? Set `STRATA_LAKE_LOCAL_FILE=data/lake.duckdb` to use a plain DuckDB
file for the lake; the ops database still needs Postgres.

### A real run (8 November 2026)

Common Crawl's CDX server was timing out that morning, so this run used the Parquet URL index
(`--index columnar`). Discovery had been cached by an earlier `strata discover`.

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
| `strata discover --domain D [--crawl C] [--limit N] [--include-query-strings] [--index auto\|cdx\|columnar]` | One newest capture per distinct page, with any failed index request listed; needs no database |
| `strata run cc --domain D [--crawl C] [--limit N] [--budget B] [--index ...]` | Fetch into the bronze table |
| `strata report JOB_ID` | Attempts per outcome, budget used, achieved rate, rows in the lake |
| `strata lineage CAPTURE_ID` | Upstream pointer, our copy, digests, archived response headers |
| `strata migrate` / `strata lake-init` | Create or upgrade the ops schema / lake tables |
| `strata demo --domain D` | All of the above, in order |

Discovery asks the index for five times as many rows as pages wanted (at most 1,000) in a single
request, keeps the newest capture of each URL, and skips URLs with a query string (`?ref=`,
`?utm_source=` and similar tracking variants of the same page) unless `--include-query-strings`
is given. In a job, every index request is logged to `ops.fetch_attempts` next to the record
fetches, so `strata report` shows index retries and the time spent waiting on them.

`--index auto` (the default) tries Common Crawl's CDX server twice, then falls back to the Parquet
URL index. The first URL-index lookup in a crawl reads each of its 300 Parquet footers once, at
2 requests/s, which takes about 2.5 minutes. That map is cached, so later lookups open only the
one or two files that can hold the domain. While the CDX server is down, `--index columnar` skips
the CDX attempts. A 403 or a malformed query never triggers the fallback. See
[ADR-004](docs/adr/004-index-sources.md) and the list of
[Common Crawl caveats](docs/common-crawl.md) behind it.

Add `--json` to `discover`, `run cc`, `report` and `lineage` for machine-readable output. Settings
come from `STRATA_*` environment variables or a `.env` file ([.env.example](.env.example)).

## Tests

```bash
uv run pytest                    # unit + integration
uv run ruff check . && uv run mypy
```

No test touches the internet (pytest-socket allows localhost only). A fake Common Crawl serves real
WARC records with Range support and scripted failures. Postgres tests use `STRATA_TEST_PG_DSN` when
set (CI, or the compose database), otherwise a throwaway cluster from local PostgreSQL binaries, and
skip if neither exists. On Windows:

```powershell
$env:STRATA_TEST_PG_DSN = "postgresql://strata:strata@localhost:5432/strata_ops"
uv run pytest
```

## Layout

```
packages/
  strata-core/    models, deterministic ids, settings, migrations, ops store, budget ledger, lake
  strata-fetch/   Common Crawl index sources, rate gate, fetcher, WARC checks, blob store, jobs, lineage
  strata-cli/     Typer CLI over the library functions
tests/            unit, integration (Postgres, DuckLake), fake Common Crawl
deploy/           docker compose (Postgres 16)
docs/adr/         decisions
docs/progress/    weekly progress logs
```

## Decisions

- [ADR-001: DuckLake with a Postgres catalog](docs/adr/001-table-format.md)
- [ADR-003: Sourcing policy](docs/adr/003-sourcing-policy.md)
- [ADR-004: Index sources and how discovery fails](docs/adr/004-index-sources.md)
- [Common Crawl caveats and bottlenecks](docs/common-crawl.md)

## What it refuses to do

- No CAPTCHA solving, proxy rotation or fingerprint evasion. A block stops the host and tells a human.
- No bulk redistribution of third-party content; pipelines and small samples with provenance only.
- No home-made policy standards: robots.txt, source terms and existing signals such as TDMRep.

## License

[Apache-2.0](LICENSE)
