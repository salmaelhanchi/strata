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
| SURT order: homepage variants first | Over-fetch 5× in one request, one newest capture per URL, query-string URLs dropped unless asked for |
| 300 footers per URL-index query | A shard map: each footer read once with a suffix-range request, paced and logged, cached per crawl, resumable; a lookup opens 1–2 files (python.org: 1) |
| Timezone-aware `fetch_time` | Read as epoch milliseconds and converted to UTC; tests use the real column type and block `pytz` |
| Truncated payloads | Bytes stored exactly as served; index digest checked against the WARC header; recomputed digest tracked separately |
| Format drift | Parse defensively; read the errata before adopting a new crawl (a practice, not code) |
| Coverage bias | Robots verdicts recorded per page (week 2); reports state the crawl and source of every row |
