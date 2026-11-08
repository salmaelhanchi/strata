# ADR-004: Index sources and how discovery fails

- Status: accepted
- Date: 2026-11-08
- Amends: ADR-003 (budget rule for the URL-index shard map)

## Context

Discovery turns a domain into a list of WARC pointers. Common Crawl offers two indexes for that:

- **The CDX server** (`index.commoncrawl.org`, pywb). One request answers one domain, which makes it
  the natural default. In practice it is a shared service that Common Crawl's own staff describe as
  overloaded, and our first live runs bore that out. In a logged run for `python.org`, three
  requests ended as gateway `504`s after 10–11 s each, and the whole lookup took 158 s. An earlier,
  unlogged run took 86 s for four requests, which fits the same pattern.
- **The URL index** (formerly the "columnar index"): about 900 Parquet files per crawl, of which
  300 hold page captures (`subset=warc`); the rest cover crawl diagnostics and robots.txt. Rows are
  roughly sorted by SURT key. It has no server to overload, but pointing DuckDB at all 300 files
  makes it read 300 footers on every lookup, without our rate limits and without a cache.

The second run also exposed a bug of ours that the CDX server reports confusingly. To drop
`?ref=` style URLs we sent `filter=!~url:?`, assuming that `~` means "contains". In pywb, `~` means
"regular expression", so the server tried to compile `?` and answered
`500 Internal Error: nothing to repeat`. Because a 500 looks retryable, we spent the whole backoff
schedule on an error that could never succeed. Our tests passed, because the fake server encoded
the same wrong assumption.

## Decision

1. **Three index modes**, chosen per job or command with `--index`:
   - `cdx`: the CDX server only.
   - `columnar`: the Parquet URL index only.
   - `auto` (default): the CDX server with one retry, then the URL index if the CDX server is
     *unavailable*.
2. **Only "unavailable" falls back.** That means timeouts and 5xx errors after retries. A 403 still
   stops the host and goes to a human (ADR-003), and a malformed query is our bug. Neither is
   routed around.
3. **Read 500s, don't just count them.** pywb wraps several distinct failures in a 500:
   - `Slow Down` (S3 throttling, relayed) is treated as rate limiting: the host pauses and retries.
   - A Python regex error is a query error and is never retried.
   - Any other 500 is retried once.
   - 502 and 504 keep the full backoff schedule.
4. **Check filters locally first.** Filters are built in one place and checked against pywb's
   grammar before sending: `[!][=|~]field:expression`. No modifier means "contains", `=` exact,
   `~` regex (`re.match`), `!` negates. Regex filters are compiled the way pywb compiles them.
   The query-string filter is `!url:?`.
5. **The fake server follows the documented grammar,** not our reading of it. Its CDX endpoint
   applies filters with pywb's semantics and answers a bad regex with the same 500 that pywb sends.
6. **A shard map for the URL index.** For each crawl we read every Parquet footer once and keep each
   file's `url_surtkey` minimum and maximum. Each footer costs one suffix-range request (`Range:
   bytes=-65536`), or two if the footer is larger. Reads go through our own client and the data
   host's gate, so they follow ADR-003's rates, backoff and User-Agent. The map is cached as JSON
   Lines (`cache/shardmaps/<crawl>.jsonl`), appended as it grows, so an interrupted build resumes.
   A lookup then opens only the files whose range overlaps the domain, usually one or two. A server
   that ignores `Range` would send a whole file, a couple of hundred MB, so a `200` is refused
   without reading its body.
7. **Budget exception (amends ADR-003 §6).** Footer reads are paced, logged to `ops.fetch_attempts`
   and counted in reports, but not charged to the job that triggers them. The map is built once per
   crawl and serves every later job. Charging it to the first job would fail any small job, such as
   a 30-request demo against a 300-request one-off cost.
8. **DuckDB's own requests.** The query over the selected files is made by DuckDB, not our client.
   It runs one query at a time, single-threaded, inside the data host's gate, and DuckDB's
   User-Agent carries ours (`custom_user_agent`). `fetch_time` is selected as `epoch_ms(...)` and
   converted to UTC in Python. Common Crawl stores it timezone-aware, and DuckDB turns
   `TIMESTAMPTZ` into Python objects only with `pytz` installed, in the machine's local time zone.
   Epoch milliseconds avoid both the dependency and the time-zone shift, and also read the naive
   `TIMESTAMP` of older files as UTC.
9. **One answer cache for every source.** Discovery answers are cached per crawl and domain whichever
   index produced them, so a rerun makes no requests either way. The CDX index timeout goes back
   to 30 s, because the gateway gives up at about 10 s anyway.
10. **Readable failures.** Error text keeps the useful part of the body: pywb's `message`, or an HTML
    page's `<title>`. Request timings exclude the wait for our own rate gate.

## Consequences

- When the CDX server is healthy, nothing changes: one request per domain, then the cache.
- When it is not, `auto` loses about 22 s (two attempts and a 2 s backoff) before switching. The
  first URL-index lookup for a crawl then reads 300 footers at 2 requests/s, about 2.5 minutes,
  once. Later lookups in that crawl cost the cache read plus one small DuckDB query, usually over a
  single file. While the CDX server is down, `--index columnar` skips the 22 s detour.
- Reports can show more attempts than budget spent: the difference is shard-map reads.
- The shard map relies on footer statistics. A file without statistics is never ruled out, so a
  missing statistic costs extra reads, never a missed page.

## Alternatives considered

- **Keep retrying the CDX server.** Its failures come in streaks, under load we don't control, so
  more retries mostly add load to a struggling shared service.
- **Query all 300 files with DuckDB on every lookup.** It works, but it makes the same 300 footer
  reads every time and bypasses our rate limiter.
- **Binary search over file numbers.** About 9 footer reads instead of 300, but only if file
  numbers follow key order. Measured on CC-MAIN-2026-43: only 150 of 299 neighbouring pairs do,
  so a binary search would miss files. Rejected.
- **Amazon Athena.** Common Crawl's status page advises against it for this index: its retry on
  `503 Slow Down` is limited, and re-running failed queries costs money. It also needs an AWS
  account, which a portfolio demo should not require.
- **Download the whole URL index.** About 0.2 TiB per crawl. That is right for crawl-wide jobs
  inside AWS (week 5), but far too much for a one-domain lookup.

## Verification

- Unit tests cover each 500 type, gateway timeouts and the attempt cap, local filter checks, the
  fake's pywb semantics (including the original bad filter returning 500), fallback and no-fallback
  cases, footer reads (local and over HTTP, small and large footers, refused full downloads,
  throttling, blocks), and resuming and versioning of the shard map.
- First live run, 2026-11-08 (`python.org`, CC-MAIN-2026-43, `--index auto`):
  - the CDX server failed twice with `504` and discovery switched to the URL index;
  - `data.commoncrawl.org` honoured suffix ranges, and all 300 footers were read on the first try,
    each with statistics;
  - python.org falls in exactly 1 of the 300 files.

  The run then failed in the DuckDB query: Common Crawl's `fetch_time` is timezone-aware, and our
  fixtures had used a naive timestamp, so the `pytz` requirement (decision 8) only surfaced live.
  The fixtures now use the real type plus a naive variant, and a test blocks `pytz` to prove it
  is never needed; it fails on the old code with the live error.
