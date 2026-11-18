# Working with Common Crawl: caveats and bottlenecks

What we learned building Strata's Common Crawl source, as of November 2026. Items marked
**measured** come from our own runs; everything else links to Common Crawl's documentation,
announcements and mailing list, or to the tools and studies named. How Strata responds to each
caveat is in the last section and in [ADR-004](adr/004-index-sources.md).

## 1. Access: three doors, different costs

- **S3 needs AWS credentials.** Since 2022, unsigned access to `s3://commoncrawl/` is disabled;
  the S3 API is for authenticated AWS users, and the data lives in `us-east-1`
  ([CloudFront announcement](https://commoncrawl.org/blog/introducing-cloudfront-access-to-common-crawl-data)).
- **Outside AWS, use HTTPS:** `https://data.commoncrawl.org/` (CloudFront), which supports byte
  ranges ([Get Started](https://commoncrawl.org/the-data/get-started/)).
- **A Hugging Face Storage Bucket** has carried crawls since April 2026 as an experiment; tools that
  use it note that only 2026 crawls are there ([Common Crawl blog](https://commoncrawl.org/blog),
  [Daft docs](https://docs.getdaft.io/en/stable/datasets/common-crawl/)).
- **Capacity is shared and finite.** Common Crawl says it can serve a few thousand small requests
  per second for everyone combined, and asks each client to stay under 10 per second
  ([performance post](https://blog.commoncrawl.org/blog/oct-nov-2023-performance-issues)). When S3
  is overwhelmed it sends `503 Slow Down` to everyone, including clients sending a single request.
- **Errors carry meaning.** On CloudFront, most 4xx responses are deliberate rate-limit rejections
  and 5xx responses are S3 backpressure passed through. Retrying works for whole-file downloads,
  but not well for partial reads such as index lookups and single records
  ([status page](https://thom.status.commoncrawl.org/)).

## 2. The CDX index server (`index.commoncrawl.org`)

- **It is often overloaded.** Common Crawl's own staff have said the CDX server "has been ill for
  quite a while", and they point users to the Parquet URL index instead
  ([mailing list, 2024](https://groups.google.com/g/common-crawl/c/cwtFIB5-7Hc)). Gateway `504`s
  under load go back years ([2023](https://groups.google.com/g/common-crawl/c/d7ftvUSuGYI)).
- **Measured: the gateway gives up after about 10 s.** On 8 November 2026, 14 of our 17 CDX
  requests (for `python.org` and `fcc.org`) ended as gateway `504`s after 10.2–11.0 s. The other
  3 were `500`s caused by our own malformed filter, so none of the 17 succeeded. A client timeout
  longer than about 10 s buys nothing.
- **Some 500s are throttling.** The server is pywb, and it has relayed S3 throttling as its own 500:
  `Internal Error: 503 Server Error: Slow Down` ([mailing list, 2022](https://groups.google.com/g/common-crawl/c/kEHzXZNu5To)).
  Treat that as a rate limit and back off; don't count it as a server bug.
- **Other 500s are your bug.** In pywb's filter syntax, no modifier means "contains", `=` means
  exact match, `~` means a regular expression matched from the start of the field, and `!` negates
  ([pywb CDX API](https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html)). We sent
  `!~url:?` meaning "URL does not contain ?", and pywb compiled `?` as a regex:
  `500 Internal Error: nothing to repeat at position 0`. **Measured:** a retry policy that treats
  every 500 as transient spent 5 requests and about 2.5 minutes, 108 s of it backing off, on an
  error that could never succeed. The correct filter is `!url:?`.
- **`limit` counts rows after filtering.** A filter that discards most rows makes the server scan
  more to fill the limit, which puts heavy load on it ([cdx_toolkit docs](https://pypi.org/project/cdx-toolkit/0.9.25)).
- **Measured: results come in URL (SURT) order, so the first rows are the least diverse.** For a
  domain, the homepage, its repeat captures and its tracking-parameter variants (`?ref=…`,
  `?featured_on=…`) sort first. Our first `python.org` lookup asked for 20 rows and got 20
  versions of the homepage. Over-fetch in a single request, then keep one capture per URL.

## 3. The URL index (Parquet; called the "columnar index" until June 2026)

- **About 900 files per crawl** (about 0.20 TiB), versus 302 files for the CDX index
  ([August 2026 crawl](https://commoncrawl.org/blog/august-2026-crawl-archive-now-available)).
  **Measured:** they are 300 files each for page captures (`subset=warc`), crawl diagnostics and
  robots.txt captures, so a page lookup only needs the 300 `warc` files (CC-MAIN-2026-43
  manifest). The index was renamed in June 2026 ([announcement](https://blog.commoncrawl.org/blog/the-columnar-index-is-now-the-url-index)).
- **The anonymous bucket can't be listed.** Get the file list from each crawl's
  `cc-index-table.paths.gz` manifest instead.
- **It is sorted, but only approximately.** Rows follow SURT order like the CDX index, but files
  aren't strictly sorted ([cc-index-table#12](https://github.com/commoncrawl/cc-index-table/issues/12)),
  so a file's minimum and maximum key are bounds, not a partition. Per-file ranges are narrow,
  as an analysis of the 2022 index also found ([Skeptric](https://skeptric.com/reading-parquet-metadata/index.html)).
  **Measured:** in CC-MAIN-2026-43 a whole domain, python.org, falls in 1 of the 300 files.
- **File numbers don't follow key order.** **Measured:** only 150 of 299 neighbouring files are
  in key order, so a binary search by file number would miss files.
- **The naive query is expensive.** `read_parquet(<all 300 files>)` makes DuckDB read 300 footers
  for every lookup, unpaced. Read each footer once, keep each file's key range, and cache that per
  crawl, since released crawls don't change. **Measured:** `data.commoncrawl.org` honours
  suffix-range requests (`Range: bytes=-65536`), so a footer needs no separate request for the
  file size. All 300 were read without a single retry at 2 requests/s, about 2.5 minutes, once per
  crawl.
- **`fetch_time` is timezone-aware.** **Measured:** DuckDB reads it as `TIMESTAMP WITH TIME ZONE`,
  and turning it into Python objects failed without `pytz`. DuckDB also converts that type to the
  session's local time zone, so code that assumes UTC can be silently wrong. Select
  `epoch_ms(fetch_time)` and convert explicitly.
- **Keys drop a leading `www`.** `url_surtkey` comes from the Internet Archive's `surt` library,
  which removes a leading `www.` (or `www2.` and so on) before reversing the host:
  `https://www.rfc-editor.org/info/rfc9309` is keyed `org,rfc-editor)/info/rfc9309`, the same as
  `rfc-editor.org`. A range built for `org,rfc-editor,www)` finds nothing, and silently.
  **Measured:** our first count for `www.rfc-editor.org` returned 0 pages this way, after 23
  requests and 40 MB. Look up a `www` host in its parent domain's range, then keep the host with
  `url_host_name`.
- **Keys drop trailing slashes and the scheme too.** The `surt` library also removes repeated and
  trailing slashes (`/pep-0008/` is keyed `/pep-0008`) and ignores the scheme, so
  `http://example.org/robots.txt` and `https://www.example.org/robots.txt` share one key. A lookup
  by exact key must then pick the right host and scheme among the rows it gets back. **Found by a
  test:** the usual robots.txt redirect, `example.org` to `www.example.org`, revisits the same key,
  and a loop check on the key alone called it a loop.
- **robots.txt captures have their own subset.** `subset=robotstxt` holds what servers returned
  for `/robots.txt`, separate from page captures (`warc`) and other non-200 responses
  (`crawldiagnostics`) ([2016 announcement](https://commoncrawl.org/blog/robotstxt-and-404-redirect-data-sets)).
  That is the evidence a verdict can rest on without sending the site a request. Gaps: since
  2021 a robots.txt is not archived when a redirect leads to a host whose own robots.txt forbids
  it, when URL filters exclude the whole site, or when the response isn't a plausible robots.txt
  type such as HTML or PDF ([July/August 2021 crawl](https://commoncrawl.org/blog/july-august-2021-crawl-archive-available)),
  so "no capture" doesn't mean "no robots.txt": Strata marks such hosts `needs_review`. A redirect
  target in the URL index may be relative ([errata](https://commoncrawl.org/errata)); resolve it
  against the capture's URL. Some older crawls carry wrong `fetch_time` values for robots.txt
  captures, which matters when choosing the newest one ([errata](https://commoncrawl.org/errata)).
- **A host is not a registered domain.** `url_host_registered_domain` for `peps.python.org` is
  `python.org`, so filtering a host's pages on that column finds nothing, silently. Filter on
  `url_host_name` (the host, or a suffix for its subdomains) and let the SURT range do the pruning;
  `url_host_name` has no useful row-group statistics of its own
  ([Skeptric](https://skeptric.com/reading-parquet-metadata/index.html) found them empty).
- **DuckDB's thread count doesn't bound its requests.** **Measured** against a local server: with
  `threads = 1`, DuckDB's Parquet reader still had 6 range requests in flight, prefetching column
  chunks in parallel. `disable_parquet_prefetching` brings that to one at a time but multiplies the
  requests by 12 (10 became 123). `http_client_pool_capacity` set to the thread count bounds the
  requests in flight and keeps the same 10 requests. DuckDB's `HTTP` log
  (`duckdb_logs_parsed('HTTP')`) lists every request with status, duration and size, so they can be
  counted and charged like any other.
- **DuckDB merges nearby reads, then splits big ones into 2 MiB blocks.** **Measured** against a
  local server: per file a HEAD, a tail read and the rest of the footer; then, per row group, the
  column chunks a query needs are merged into runs where they sit close together, and each run
  is fetched in blocks of `external_file_cache_remote_max_block_size` (2 MiB in DuckDB 2.0).
  Common Crawl's row groups are large (1.67 million rows in the one holding www.rfc-editor.org),
  so the block size, not the number of columns, sets the request count: **measured live**, 49
  requests for 95 MB. A model of one request per run predicted 6; counting blocks predicts within
  one request on a 1.6-million-row test file. The columns a site query skips (`url_path`,
  `url_query`, ...) sit between the ones it reads, which is what splits a row group into runs.
- **Compression changed.** Files are ZStandard-compressed from `CC-MAIN-2026-30`
  ([July 2026 crawl](https://blog.commoncrawl.org/blog/july-2026-crawl-archive-now-available)), so
  a reader without ZStandard support can't read new crawls.
- **Athena isn't the easy way out.** Common Crawl advises against Athena for this index: its
  retries on `503 Slow Down` are limited, and re-running failed queries costs money. It recommends
  downloading the Parquet files and querying them with DuckDB ([status page](https://thom.status.commoncrawl.org/)).

## 4. WARC records

- **Payloads are truncated.** The limit was 1 MiB until `CC-MAIN-2025-13` and is 5 MiB since; in
  that crawl, the truncated share of HTML fell from 2.2% to 0.04%
  ([March 2025 crawl](https://blog.commoncrawl.org/blog/march-2025-crawl-archive-now-available)).
- **Truncation is marked, mostly.** Since November 2019 a `WARC-Truncated` header marks it, the
  original length moves to `X-Crawler-Content-Length`, and `Content-Length` is rewritten. Timeouts
  and disconnects truncate too ([mailing list](https://groups.google.com/g/common-crawl/c/X4KaO55x2c8)).
  Older crawls didn't always flag truncation, and the index's truncation flag only starts in
  `CC-MAIN-2019-47` ([errata](https://commoncrawl.org/errata)).
- **Payloads are stored decoded, and misleading headers are renamed.** **Measured:** python.org
  served `/about/` gzip-compressed. The archived HTTP headers keep that as
  `X-Crawler-Content-Encoding: gzip`, while the payload in the record is plain HTML, so a parser
  must not try to decompress it again. It's the same pattern as `X-Crawler-Content-Length` for
  truncated pages. (8,358 compressed bytes held a 43,226-byte record.)
- **URLs may contain raw non-ASCII characters,** not percent-encoded or Punycode, in WARC headers
  and the indexes ([erratum](https://commoncrawl.org/blog/january-2017-crawl-archive-now-available)).
  Normalise before comparing.
- **Formats drift.** WARC record IDs are type-7 UUIDs from `CC-MAIN-2026-30`, and revisit records
  changed their WARC `Content-Type` in April 2026 ([July 2026](https://blog.commoncrawl.org/blog/july-2026-crawl-archive-now-available),
  [April 2026](https://blog.commoncrawl.org/blog/april-2026-crawl-archive-now-available)). Parse
  defensively, and read the errata page.

## 5. Coverage: a sample, not a mirror

- **A crawl is a sample.** The October 2026 crawl holds 2.16 billion pages
  ([blog](https://commoncrawl.org/blog)). A page missing from it may still exist.
- **Many sites block CCBot,** which is built to respect robots.txt. Estimates depend on the
  population measured:
  - about 6% of roughly 140 million sites ([Ahrefs](https://ahrefs.com/de/blog/ai-bot-block-rates/));
  - 16% of the top 10,000 sites ([Thunderbit](https://thunderbit.com/blog/ai-crawlers-website-allow));
  - 25% of top websites ([Known Agents](https://knownagents.com/agents/ccbot));
  - 75% of about 100 top US and UK news sites ([BuzzStream](https://www.buzzstream.com/blog/publishers-block-ai-study/)).

  Blocked sites are missing or stale, which skews any dataset toward sites that allow crawling.

## 6. What Strata does about it

| Caveat | Strata's response |
| --- | --- |
| Shared capacity, `503 Slow Down` | 2 requests/s to the data host, one request per 2 s to the index; backoff 2/10/30/60 s with jitter; a 429 or 503 pauses the whole host (ADR-003) |
| CDX server overloaded, 10 s gateway | `--index auto`: two CDX attempts, then the Parquet URL index; answers cached per crawl and domain |
| 500s that mean different things | Throttling is backed off, regex errors are never retried, other 500s get one retry |
| Filter syntax traps | Filters built in one place and checked locally against pywb's grammar; the fake server implements the documented semantics, so tests catch a wrong reading |
| SURT order: homepage variants first | CDX: over-fetch 5× in one request; URL index: a site read once per crawl into a cached catalog. One newest capture per URL, query-string URLs dropped unless asked for, pages chosen in md5 order |
| 300 footers per URL-index query | A shard map: each footer read once with a suffix-range request, paced and logged, cached per crawl, resumable; a lookup opens 1–2 files (python.org: 1) |
| Timezone-aware `fetch_time` | Read as epoch milliseconds and converted to UTC; tests use the real column type and block `pytz` |
| DuckDB's own requests | Read from DuckDB's HTTP log after every query, stored with `client = 'duckdb'`, charged to the job, and capped at 1–2 in flight with `http_client_pool_capacity` (ADR-004, amendments) |
| Host vs registered domain | Pages filtered by `url_host_name` and SURT range, so a host such as `peps.python.org` works |
| Keys without `www` | Keys built with the `surt` library's rule (a leading `www`/`wwwN` dropped), pinned in tests to the library's real output; a `www` host is searched in its parent's range and filtered by host |
| Truncated payloads | Bytes stored exactly as served; index digest checked against the WARC header; recomputed digest tracked separately |
| Format drift | Parse defensively; read the errata before adopting a new crawl (a practice, not code) |
| Keys without slashes or scheme | Exact keys built like the library's (pinned to 20 more of its outputs); robots.txt lookups keep only the exact host and prefer the scheme asked for; a loop is the same scheme, host and key twice |
| DuckDB's cost known only afterwards | Estimated before the query from the byte ranges the shard map keeps per row group (runs, 2 MiB blocks, as measured), reserved from the budget, settled to the real count; a query the budget can't cover is not run |
| robots.txt without asking the site | Verdicts from the crawl's own robots.txt captures (`robotstxt` subset), redirects followed up to 5, RFC 9309 status rules (ADR-006) |
| Coverage bias | Robots verdicts recorded per page; reports state the crawl and source of every row |
