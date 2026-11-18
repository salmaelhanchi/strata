"""Finding captures in Common Crawl.

Three sources behind one protocol, all returning identical ``CaptureRef``s (ADR-004):

* ``CdxIndex``: Common Crawl's CDX server (pywb). Answers one domain in one request, but it is
  shared, often overloaded, and its gateway gives up after about 10 s. Answers are cached on disk.
* ``ColumnarIndex``: DuckDB over the Parquet URL index (formerly the "columnar index"). With a
  shard map it opens only the files, of the 300 page-capture files per crawl, whose SURT range
  covers the domain (one, for python.org in CC-MAIN-2026-43). DuckDB sends those requests with
  its own HTTP client; they are read back from DuckDB's log after each query and reported as
  ``client="duckdb"`` attempts, so a slow lookup never shows as "0 requests".
* ``AutoIndex``: the CDX server first, the URL index when the CDX server is unavailable.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin, urlparse, urlsplit

import duckdb
import httpx
from pydantic import BaseModel, ConfigDict

from strata_core.ledger import BudgetExhaustedError
from strata_core.models import CaptureRef, Outcome, RequestClient
from strata_fetch.http import (
    RETRYABLE_EXCEPTIONS,
    backoff_delay,
    classify_status,
    retry_after_seconds,
)
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.surt import host_forms, normalise_domain, on_site, surt_range

Reserve = Callable[[str], Awaitable[bool]]


class IndexAttempt(BaseModel):
    """One request to an index, kept so a slow or failed lookup can be explained."""

    model_config = ConfigDict(frozen=True)

    attempt: int
    url: str
    started_at: datetime
    elapsed_ms: int
    http_status: int | None
    outcome: Outcome
    retry_after_ms: int | None = None
    waited_ms: int | None = None  # backoff slept (or host paused) after this attempt
    error: str | None = None
    client: RequestClient = RequestClient.STRATA
    charged: bool = True  # shard-map footer reads are shared by every job, so not charged
    method: str | None = None  # set for DuckDB's requests (HEAD or GET)
    bytes: int | None = None  # response body bytes, when known


class DuckDbUsage(BaseModel):
    """What one URL-index query cost, from DuckDB's own HTTP and file-system logs."""

    model_config = ConfigDict(frozen=True)

    requests: int = 0
    bytes: int = 0  # response bodies received (Content-Length of each GET)
    bytes_read: int = 0  # what DuckDB's file system read from remote files; should equal bytes
    files: int = 0  # distinct remote files touched
    failed: int = 0  # requests without a 2xx answer
    peak_in_flight: int = 0  # most requests open at once, from their start times and durations
    query_s: float = 0.0  # wall time of the query


class DiscoveryResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    refs: list[CaptureRef]
    from_cache: bool
    requests: int  # HTTP requests this lookup made: ours and, for the URL index, DuckDB's
    source: str
    rows_scanned: int = 0  # index rows read before removing duplicates and variants
    attempts: list[IndexAttempt] = []
    elapsed_s: float = 0.0
    fallback_reason: str | None = None  # set when AutoIndex had to leave the CDX server
    shards_total: int = 0  # URL index: Parquet files in the crawl
    shards_queried: int = 0  # URL index: files whose SURT range covers the domain
    footers_read: int = 0  # URL index: Parquet footers read to build or extend the shard map
    duckdb: DuckDbUsage | None = None  # URL index: DuckDB's own requests for this lookup
    candidates: int | None = None  # pages matching before the limit, when the source knows


# DuckDB merges the column-chunk reads of a row group whose gap is at most this many bytes.
# `run_logged_query` pins DuckDB's `parquet_prefetch_column_gap` to it and the shard map's cost
# estimate assumes it, so the two agree. Common Crawl's files hold columns a site query skips
# (url_path, url_query, ...) between the ones it reads, which splits a row group into runs.
MERGE_GAP = 16 * 1024


class CostEstimate(BaseModel):
    """What a site query should cost, from the cached row-group statistics (before any request).

    ``requests`` counts, per file, the HEAD and footer reads, and per row group the reads DuckDB
    needs: chunks less than 16 KiB apart merged (``shardmap.MERGE_GAP``), each run read in blocks
    of DuckDB's remote block size, plus one read per run. A job reserves it before the query and
    is charged or refunded the difference.
    """

    model_config = ConfigDict(frozen=True)

    files: int = 0
    row_groups: int = 0
    rows: int = 0
    requests: int = 0
    bytes: int = 0


class ShardSelection(BaseModel):
    """The URL-index files that may hold a domain, and what finding them cost."""

    model_config = ConfigDict(frozen=True)

    files: list[str]
    attempts: list[IndexAttempt] = []
    footers_read: int = 0
    estimate: CostEstimate | None = None  # None when the map has no row-group statistics


class IndexSource(Protocol):
    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult: ...


class ShardSelector(Protocol):
    async def select(self, crawl: str, files: list[str], lo: str, hi: str) -> ShardSelection: ...


class _IndexError(Exception):
    def __init__(self, message: str, attempts: Sequence[IndexAttempt] = ()) -> None:
        super().__init__(message)
        self.attempts = list(attempts)


class IndexUnavailableError(_IndexError):
    """The index kept failing after every retry the policy allows."""


class IndexQueryError(_IndexError):
    """The index refused the query, or the query is malformed: a retry gets the same answer."""


class IndexBlockedError(_IndexError):
    """The index answered 403: stop and tell a human (ADR-003)."""


OnAttempt = Callable[[IndexAttempt], Awaitable[None]]


def select_distinct(
    refs: Sequence[CaptureRef], limit: int, *, include_query_strings: bool = False
) -> list[CaptureRef]:
    """One capture per URL, newest first, in index (SURT) order; query-string variants dropped.

    The index is sorted by URL, so a popular page and its tracking variants (``?ref=...``) and
    repeated captures all come first; without this, "20 captures" can be 20 copies of one page.
    """
    best: dict[str, CaptureRef] = {}
    for ref in refs:
        if not include_query_strings and "?" in ref.url:
            continue
        current = best.get(ref.url)
        if current is None or _newer(ref, current):
            best[ref.url] = ref
    return list(best.values())[:limit]


def _newer(a: CaptureRef, b: CaptureRef) -> bool:
    if a.fetch_time is None:
        return False
    return b.fetch_time is None or a.fetch_time > b.fetch_time


def rows_to_request(limit: int, *, overfetch: int = 5, max_rows: int = 1000) -> int:
    """Ask for more rows than pages wanted, in the same single request, to survive deduplication."""
    return max(limit, min(limit * overfetch, max_rows))


# ------------------------------------------------------------------------- CDX query building
def cdx_filters(*, include_query_strings: bool = False) -> list[str]:
    """pywb filters: ``[!][=|~]field:expression``. No modifier means "contains", ``=`` exact,
    ``~`` a regular expression matched at the start of the field (``re.match``); ``!`` negates."""
    filters = ["=status:200", "=mime:text/html"]
    if not include_query_strings:
        filters.append("!url:?")  # url does not contain "?"
    return filters


def check_filter(spec: str) -> None:
    """Reject a malformed filter before it reaches the server, which would answer with a 500.

    ``~`` expressions are compiled the way pywb compiles them: ``!~url:?`` once cost five
    requests and two and a half minutes of backoff to learn that "?" is not a valid regex.
    """
    body = spec.removeprefix("!")
    mode = body[:1] if body[:1] in ("=", "~") else ""
    field, colon, expression = body[len(mode) :].partition(":")
    if not field or not colon:
        raise IndexQueryError(f"malformed CDX filter {spec!r}: expected [!][=|~]field:expression")
    if mode == "~":
        try:
            re.compile(expression)
        except re.error as exc:
            raise IndexQueryError(f"CDX filter {spec!r} is not a valid regex: {exc}") from exc


_TITLE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def server_message(body: str) -> str:
    """The useful part of an error body, on one line: pywb's JSON ``message``, an HTML page's
    title (a gateway's "504 Gateway Time-out"), or the start of the text."""
    text = body.strip()
    if text.startswith("{"):
        try:
            message = json.loads(text).get("message")
        except (ValueError, AttributeError):
            message = None
        if isinstance(message, str):
            return " ".join(message.split())
    if found := _TITLE.search(text):
        return " ".join(found.group(1).split())
    return " ".join(text.split())[:200]


# Python ``re`` errors that pywb reports as "Internal Error: ..." with status 500.
_QUERY_ERRORS = (
    "nothing to repeat",
    "multiple repeat",
    "unterminated",
    "unbalanced parenthesis",
    "missing ), unterminated subpattern",
    "bad escape",
    "bad character range",
    "look-behind requires fixed-width pattern",
)


def classify_index_reply(status: int, body: str) -> Outcome:
    """Like ``classify_status``, but reads pywb's 500s: some are throttling, some are our bug."""
    if status == 200 or (status == 404 and "no captures found" in body.lower()):
        return Outcome.OK  # a 404 "no captures found" is a definitive (empty) answer
    if status == 500:
        message = server_message(body).lower()
        if "slow down" in message:
            return Outcome.RATE_LIMITED  # pywb relays S3's "503 Slow Down" as its own 500
        if any(marker in message for marker in _QUERY_ERRORS):
            return Outcome.DEFINITIVE_EMPTY  # our query is malformed: never retry it
    return classify_status(status)


def summarise_attempts(attempts: Sequence[IndexAttempt]) -> str:
    """'2 attempts: HTTP 504, HTTP 504' for a one-line explanation."""
    parts = [
        f"HTTP {a.http_status}" if a.http_status is not None else "no response" for a in attempts
    ]
    return f"{len(attempts)} attempt(s): {', '.join(parts)}" if parts else "no attempt"


# ------------------------------------------------------------------------- answer cache
_CACHE_VERSION = 2


class RefCache:
    """Discovery answers on disk, one JSON Lines file per (crawl, domain), shared by every source.

    The first line records the query (rows asked for, rows returned, options, source), so a later
    and larger request refetches only when the cache can't answer it. Released crawls never change.
    """

    def __init__(self, cache_dir: Path | None, *, include_query_strings: bool = False) -> None:
        self._dir = cache_dir
        self._include_query = include_query_strings

    def path(self, crawl: str, domain: str) -> Path | None:
        return None if self._dir is None else self._dir / "refs" / crawl / f"{domain}.jsonl"

    def read(self, crawl: str, domain: str) -> tuple[list[CaptureRef], str, bool] | None:
        """Every cached page, the source that found them, and whether the index had no more."""
        path = self.path(crawl, domain)
        if path is None or not path.exists():
            return None
        lines = path.read_text(encoding="utf-8").splitlines()
        meta = json.loads(lines[0])
        if (
            meta.get("v") != _CACHE_VERSION
            or meta.get("include_query_strings") != self._include_query
        ):
            return None  # written by an older version or with other options: ask again
        refs = [CaptureRef.model_validate_json(line) for line in lines[1:] if line.strip()]
        exhausted = int(meta["rows_returned"]) < int(meta["rows_requested"])  # index had no more
        return refs, str(meta.get("source", "cdx")), exhausted

    def write(
        self,
        crawl: str,
        domain: str,
        *,
        source: str,
        rows_requested: int,
        rows_returned: int,
        refs: Sequence[CaptureRef],
    ) -> None:
        path = self.path(crawl, domain)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        meta = {
            "v": _CACHE_VERSION,
            "crawl": crawl,
            "domain": domain,
            "source": source,
            "include_query_strings": self._include_query,
            "rows_requested": rows_requested,
            "rows_returned": rows_returned,
            "cached_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        with tmp.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps(meta) + "\n")
            for ref in refs:
                fh.write(ref.model_dump_json() + "\n")
        tmp.replace(path)  # atomic: a crash never leaves a half-written cache


# ---------------------------------------------------------------------------------- CDX server
def parse_cdx_line(line: str, crawl: str) -> CaptureRef:
    rec: dict[str, Any] = json.loads(line)
    ts = rec.get("timestamp")
    return CaptureRef(
        crawl=crawl,
        url=rec["url"],
        url_surtkey=rec.get("urlkey"),
        fetch_time=datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=UTC) if ts else None,
        warc_filename=rec["filename"],
        warc_record_offset=int(rec["offset"]),
        warc_record_length=int(rec["length"]),
        content_digest=rec.get("digest"),
        mime=rec.get("mime"),
        status=int(rec["status"]) if rec.get("status") not in (None, "-") else None,
    )


# A plain 500 (not throttling, not a query error) is retried once: it may be a hiccup, but a
# second one in a row is more likely a bug on either side than bad luck.
_MAX_PLAIN_500S = 2


class CdxIndex:
    source = "cdx"

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        *,
        base_url: str = "https://index.commoncrawl.org/",
        cache_dir: Path | None = None,
        backoff: Sequence[float] = (2.0, 10.0, 30.0, 60.0),
        sleep: Sleep = asyncio.sleep,
        reserve: Reserve | None = None,
        include_query_strings: bool = False,
        overfetch: int = 5,
        max_rows: int = 1000,
        timeout_s: float | None = None,
        on_attempt: OnAttempt | None = None,
        max_attempts: int | None = None,
    ) -> None:
        self._client = client
        self._gate = gate
        self._base = base_url if base_url.endswith("/") else base_url + "/"
        self._cache = RefCache(cache_dir, include_query_strings=include_query_strings)
        self._backoff = tuple(backoff)
        self._sleep = sleep
        self._reserve = reserve
        self._include_query = include_query_strings
        self._overfetch = overfetch
        self._max_rows = max_rows
        self._timeout = timeout_s
        self._on_attempt = on_attempt
        most = len(self._backoff) + 1
        self._max_attempts = most if max_attempts is None else max(1, min(max_attempts, most))
        self.host = urlparse(self._base).hostname or ""

    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult:
        domain = normalise_domain(domain)
        if pattern is not None:
            check_pattern(pattern)
        cached = self._cache.read(crawl, domain)
        if cached is not None:
            refs, source, exhausted = cached
            chosen = choose(refs, pattern=pattern, include_query_strings=self._include_query)
            if exhausted or len(chosen) >= limit:
                return DiscoveryResult(
                    refs=chosen[:limit],
                    from_cache=True,
                    requests=0,
                    source=source,
                    candidates=len(chosen) if exhausted else None,
                )
        filters = cdx_filters(include_query_strings=self._include_query)
        for spec in filters:
            check_filter(spec)  # a malformed filter fails here, without a request
        rows = rows_to_request(limit, overfetch=self._overfetch, max_rows=self._max_rows)
        params: tuple[tuple[str, str], ...] = (
            ("url", f"*.{domain}"),
            ("output", "json"),
            *(("filter", spec) for spec in filters),
            ("limit", str(rows)),
        )
        t0 = time.monotonic()
        body, attempts = await self._get(urljoin(self._base, f"{crawl}-index"), params)
        scanned = [parse_cdx_line(line, crawl) for line in body.splitlines() if line.strip()]
        # pywb matches by SURT key, which drops a leading www: asked for www.example.org, it
        # answers for example.org and every subdomain. Keep the host asked for, as the URL index
        # does. (Its row limit applies before this, so a www host can get fewer pages.)
        on_host = [ref for ref in scanned if on_site(ref.url, domain)]
        # Cache every distinct page found, not just `limit`: a later, larger request may be
        # answerable without asking the index again.
        distinct = select_distinct(on_host, len(on_host), include_query_strings=self._include_query)
        self._cache.write(
            crawl,
            domain,
            source=self.source,
            rows_requested=rows,
            rows_returned=len(scanned),
            refs=distinct,
        )
        # The same choice as the URL index's: pattern, then md5 order (not the index's order).
        chosen = choose(distinct, pattern=pattern, include_query_strings=self._include_query)
        return DiscoveryResult(
            refs=chosen[:limit],
            from_cache=False,
            requests=len(attempts),
            source=self.source,
            rows_scanned=len(scanned),
            attempts=attempts,
            elapsed_s=round(time.monotonic() - t0, 3),
            candidates=len(chosen) if len(scanned) < rows else None,
        )

    async def _get(
        self, url: str, params: tuple[tuple[str, str], ...]
    ) -> tuple[str, list[IndexAttempt]]:
        log: list[IndexAttempt] = []
        full_url = str(httpx.URL(url, params=params))
        plain_500s = 0
        for attempt in range(1, self._max_attempts + 1):
            if self._reserve is not None and not await self._reserve(self.host):
                raise BudgetExhaustedError(None, self.host)
            started_at, t0 = datetime.now(UTC), time.monotonic()
            status: int | None = None
            retry_after: float | None = None
            error: str | None = None
            body = ""
            try:
                async with self._gate.slot():
                    # time the request itself, not the wait for our own rate gate
                    started_at, t0 = datetime.now(UTC), time.monotonic()
                    if self._timeout is None:
                        resp = await self._client.get(url, params=params)
                    else:
                        resp = await self._client.get(url, params=params, timeout=self._timeout)
            except RETRYABLE_EXCEPTIONS as exc:
                outcome, error = Outcome.RETRYABLE_ERROR, f"{type(exc).__name__}: {exc}"
            else:
                status, body = resp.status_code, resp.text
                retry_after = retry_after_seconds(resp.headers.get("Retry-After"))
                outcome = classify_index_reply(status, body)
                if outcome is not Outcome.OK:
                    error = f"HTTP {status}: {server_message(body)}"

            if status == 500 and outcome is Outcome.RETRYABLE_ERROR:
                plain_500s += 1
            final = (
                outcome in (Outcome.OK, Outcome.BLOCKED, Outcome.DEFINITIVE_EMPTY)
                or plain_500s >= _MAX_PLAIN_500S
            )
            delay = (
                None
                if final or attempt == self._max_attempts
                else backoff_delay(attempt, self._backoff, retry_after)
            )
            record = IndexAttempt(
                attempt=attempt,
                url=full_url,
                started_at=started_at,
                elapsed_ms=int((time.monotonic() - t0) * 1000),
                http_status=status,
                outcome=outcome,
                retry_after_ms=int(retry_after * 1000) if retry_after is not None else None,
                waited_ms=int(delay * 1000) if delay is not None else None,
                error=error,
            )
            log.append(record)
            if self._on_attempt is not None:
                await self._on_attempt(record)

            if outcome is Outcome.OK:
                return (body if status == 200 else ""), log
            if outcome is Outcome.BLOCKED:
                raise IndexBlockedError(error or "blocked", log)
            if outcome is Outcome.DEFINITIVE_EMPTY:
                raise IndexQueryError(error or "refused", log)
            if delay is None:
                break
            if outcome is Outcome.RATE_LIMITED:
                self._gate.pause(delay)  # the host as a whole waits
            else:
                await self._sleep(delay)
        raise IndexUnavailableError(f"{full_url}: {log[-1].error if log else 'no attempt'}", log)


# ------------------------------------------------------------------- DuckDB's own requests
# DuckDB 2.0 logs every HTTP request its httpfs extension makes. start_time is a TIMESTAMPTZ, so it
# is read as epoch milliseconds: turning it into a Python object would need pytz (ADR-004).
_HTTP_LOG_SQL = """
SELECT request.type, request.url, epoch_ms(request.start_time), request.duration_ms,
       response.status, response.headers['Content-Length']
FROM duckdb_logs_parsed('HTTP')
ORDER BY 3
"""

_FS_READ_SQL = """
SELECT coalesce(sum(bytes), 0) FROM duckdb_logs_parsed('FileSystem')
WHERE op = 'READ' AND fs <> 'LocalFileSystem'
"""


def parse_http_status(value: str | None) -> int | None:
    """DuckDB logs statuses as names with the code last: ``PartialContent_206`` -> 206."""
    if not value:
        return None
    code = value.rsplit("_", 1)[-1]
    return int(code) if code.isdigit() else None


def peak_in_flight(intervals: Sequence[tuple[int, int]]) -> int:
    """Most intervals open at once; each is [start_ms, start_ms + duration_ms), at least 1 ms."""
    events: list[tuple[int, int]] = []
    for start, duration in intervals:
        events.append((start, 1))
        events.append((start + max(duration, 1), -1))
    events.sort()  # at equal times an end (-1) sorts before a start (+1): touching isn't overlap
    peak = current = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak


def read_duckdb_requests(
    con: duckdb.DuckDBPyConnection, query_s: float = 0.0
) -> tuple[list[IndexAttempt], DuckDbUsage]:
    """DuckDB's requests since logging was enabled on ``con``, as attempts plus a summary."""
    attempts: list[IndexAttempt] = []
    intervals: list[tuple[int, int]] = []
    for method, url, start_ms, duration_ms, status_name, length in con.execute(
        _HTTP_LOG_SQL
    ).fetchall():
        status = parse_http_status(status_name)
        if status is None:
            outcome = Outcome.RETRYABLE_ERROR
        elif 200 <= status < 300:
            outcome = Outcome.OK
        else:
            outcome = classify_status(status)
        received = int(length) if method == "GET" and length and str(length).isdigit() else 0
        attempts.append(
            IndexAttempt(
                attempt=1,
                url=url,
                started_at=datetime.fromtimestamp(start_ms / 1000, UTC),
                elapsed_ms=int(duration_ms or 0),
                http_status=status,
                outcome=outcome,
                error=None if outcome is Outcome.OK else (status_name or "no response"),
                client=RequestClient.DUCKDB,
                method=method,
                bytes=received,
            )
        )
        intervals.append((int(start_ms), int(duration_ms or 0)))
    fs_row = con.execute(_FS_READ_SQL).fetchone()
    usage = DuckDbUsage(
        requests=len(attempts),
        bytes=sum(a.bytes or 0 for a in attempts),
        bytes_read=int(fs_row[0]) if fs_row else 0,
        files=len({a.url for a in attempts}),
        failed=sum(1 for a in attempts if a.outcome is not Outcome.OK),
        peak_in_flight=peak_in_flight(intervals),
        query_s=round(query_s, 3),
    )
    return attempts, usage


class _DuckDbQueryError(Exception):
    def __init__(self, error: duckdb.Error, attempts: list[IndexAttempt]) -> None:
        super().__init__(str(error))
        self.error = error
        self.attempts = attempts


def check_pattern(pattern: str) -> None:
    """Compile a URL pattern with DuckDB's own regex engine (RE2) before any remote query.

    RE2 is not Python's ``re``: look-arounds and backreferences are refused. A bad pattern found
    here costs nothing; found by the remote query, it costs the requests made before it failed.
    """
    con = duckdb.connect()
    try:
        con.execute("SELECT regexp_full_match('', ?)", [pattern]).fetchall()
    except duckdb.Error as exc:
        raise IndexQueryError(f"URL pattern {pattern!r} is not a valid RE2 regex: {exc}") from exc
    finally:
        con.close()


# ------------------------------------------------------------------------------ columnar index
class ShardResolver(Protocol):
    async def shards(self, crawl: str) -> list[str]: ...


class StaticShards:
    """Explicit shard paths or URLs: local fixtures, or a list prepared elsewhere."""

    def __init__(self, paths: Sequence[str]) -> None:
        self._paths = list(paths)

    async def shards(self, crawl: str) -> list[str]:
        return [p for p in self._paths if f"crawl={crawl}/" in p.replace("\\", "/")]


class ManifestShards:
    """Resolve shard URLs from a crawl's ``cc-index-table.paths.gz`` manifest, because the
    Common Crawl bucket can't be listed anonymously. The manifest is cached on disk."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        *,
        data_base: str = "https://data.commoncrawl.org/",
        cache_dir: Path | None = None,
        subset: str = "warc",
    ) -> None:
        self._client = client
        self._gate = gate
        self._base = data_base if data_base.endswith("/") else data_base + "/"
        self._cache_dir = cache_dir
        self._subset = subset

    async def shards(self, crawl: str) -> list[str]:
        cache = self._cache_dir / "manifests" / f"{crawl}.paths" if self._cache_dir else None
        if cache is not None and cache.exists():
            text = cache.read_text(encoding="utf-8")
        else:
            url = urljoin(self._base, f"crawl-data/{crawl}/cc-index-table.paths.gz")
            async with self._gate.slot():
                resp = await self._client.get(url)
            if resp.status_code != 200:
                raise IndexUnavailableError(
                    f"{url}: HTTP {resp.status_code}: {server_message(resp.text)}"
                )
            text = gzip.decompress(resp.content).decode("utf-8")
            if cache is not None:
                cache.parent.mkdir(parents=True, exist_ok=True)
                cache.write_text(text, encoding="utf-8")
        return [urljoin(self._base, p) for p in text.split() if f"subset={self._subset}/" in p]


# ------------------------------------------------------------------------------ site catalog
# One query reads every capture of a site (the host and its subdomains) from the URL index; the
# answer is cached per crawl and site, so choosing pages, trying another pattern or counting
# again costs no request at all. fetch_time comes back as epoch milliseconds: Common Crawl stores
# it timezone-aware, and DuckDB turns TIMESTAMPTZ into Python objects only with pytz installed,
# in the session's local time zone (ADR-004).
#
# The SURT range narrows the scan (and lets row-group statistics skip the rest); the host test
# keeps the answer exact. Filtering on url_host_registered_domain instead would find nothing for
# peps.python.org, whose registered domain is python.org. url_host_name may hold an international
# host in Unicode or punycode, so both forms are accepted.
_CATALOG_COLUMNS = """
SELECT url, url_surtkey, epoch_ms(fetch_time), fetch_status, content_digest, content_mime_type,
       warc_filename, warc_record_offset, warc_record_length, crawl
FROM read_parquet(?, hive_partitioning = true)
WHERE crawl = ?
  AND url_surtkey >= ? AND url_surtkey < ?
"""

CATALOG_VERSION = 1


def _site_sql(forms: Sequence[str]) -> tuple[str, list[str]]:
    clauses, params = [], []
    for form in forms:
        clauses.append("url_host_name = ? OR ends_with(url_host_name, ?)")
        params += [form, "." + form]
    return _CATALOG_COLUMNS + "  AND (" + " OR ".join(clauses) + ")\n", params


def url_shape(url: str) -> str:
    """A URL's path with digit runs as N, up to two segments: ``/info/rfc4767/`` -> ``/info/rfcN/``.

    Pages of one kind share a shape, so a site's shapes show which patterns are worth writing.
    """
    parts = urlsplit(url)
    segments = [re.sub(r"\d+", "N", seg) for seg in parts.path.split("/")[1:]]
    shape = "/" + "/".join(segments[:2]) + ("/…" if len(segments) > 2 else "")
    return shape + ("?…" if parts.query else "")


class SiteSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    rows: int = 0  # every capture of the site in the crawl, whatever its status or type
    eligible: int = 0  # captures with status 200 and text/html
    urls: int = 0  # distinct eligible URLs
    by_status: dict[str, int] = {}  # all captures, by HTTP status
    by_mime: dict[str, int] = {}  # all captures, by MIME type (top 8)
    shapes: dict[str, int] = {}  # distinct eligible URLs by shape (top 12)


class SiteCatalog(BaseModel):
    """A site's newest eligible capture per URL in one crawl, and what the crawl holds overall."""

    model_config = ConfigDict(frozen=True)

    crawl: str
    site: str
    built_at: datetime
    summary: SiteSummary
    refs: list[CaptureRef]
    shards_total: int = 0
    shards_queried: int = 0
    estimate: CostEstimate | None = None
    duckdb: DuckDbUsage | None = None  # what building it cost (zero when read from the cache)


def summarise_rows(rows: Sequence[tuple[Any, ...]]) -> tuple[SiteSummary, list[CaptureRef]]:
    """From catalog query rows to the summary and the newest eligible capture of each URL."""
    status: Counter[str] = Counter()
    mime: Counter[str] = Counter()
    newest: dict[str, CaptureRef] = {}
    eligible = 0
    for row in rows:
        code = None if row[3] is None else int(row[3])
        status[str(code) if code is not None else "-"] += 1
        mime[str(row[5] or "-")] += 1
        if code != 200 or row[5] != "text/html":
            continue
        eligible += 1
        ref = CaptureRef(
            crawl=row[9],
            url=row[0],
            url_surtkey=row[1],
            fetch_time=_from_epoch_ms(row[2]),
            warc_filename=row[6],
            warc_record_offset=int(row[7]),
            warc_record_length=int(row[8]),
            content_digest=row[4],
            mime=row[5],
            status=code,
        )
        seen = newest.get(ref.url)
        if seen is None or _newer(ref, seen):
            newest[ref.url] = ref
    refs = sorted(newest.values(), key=lambda r: (r.url_surtkey or "", r.url))
    shapes = Counter(url_shape(r.url) for r in refs)
    summary = SiteSummary(
        rows=len(rows),
        eligible=eligible,
        urls=len(refs),
        by_status=dict(status.most_common()),
        by_mime=dict(mime.most_common(8)),
        shapes=dict(shapes.most_common(12)),
    )
    return summary, refs


class CatalogStore:
    """Site catalogs on disk: ``catalogs/{crawl}/{site}.jsonl.gz``, a header line then one
    CaptureRef per line. Written atomically; a released crawl never changes."""

    def __init__(self, cache_dir: Path | None) -> None:
        self._dir = cache_dir

    def path(self, crawl: str, site: str) -> Path | None:
        return None if self._dir is None else self._dir / "catalogs" / crawl / f"{site}.jsonl.gz"

    def read(self, crawl: str, site: str) -> SiteCatalog | None:
        path = self.path(crawl, site)
        if path is None or not path.exists():
            return None
        try:
            lines = gzip.decompress(path.read_bytes()).decode("utf-8").splitlines()
            head = json.loads(lines[0])
            if head.get("v") != CATALOG_VERSION:
                return None
            refs = [CaptureRef.model_validate_json(line) for line in lines[1:] if line.strip()]
            body = {k: v for k, v in head.items() if k != "v"}
            return SiteCatalog.model_validate({**body, "refs": refs, "duckdb": None})
        except (OSError, ValueError, IndexError):
            return None  # damaged or partial: rebuild

    def write(self, catalog: SiteCatalog) -> None:
        path = self.path(catalog.crawl, catalog.site)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        head = {"v": CATALOG_VERSION, **catalog.model_dump(mode="json", exclude={"refs"})}
        lines = [json.dumps(head)] + [ref.model_dump_json() for ref in catalog.refs]
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
        tmp.replace(path)


def match_urls(urls: Sequence[str], pattern: str) -> set[str]:
    """The URLs ``pattern`` matches in full, with DuckDB's RE2: the engine every query uses."""
    if not urls:
        return set()
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT u FROM (SELECT unnest(?::VARCHAR[]) AS u) WHERE regexp_full_match(u, ?)",
            [list(urls), pattern],
        ).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows}


def choose(
    refs: Sequence[CaptureRef],
    *,
    pattern: str | None = None,
    include_query_strings: bool = False,
    limit: int | None = None,
) -> list[CaptureRef]:
    """Pages to take from a site: query strings dropped unless asked for, the pattern applied,
    then ordered by the md5 of the URL. That order spreads a sample across the site and is the
    same on every run, so a resumed job chooses exactly the same pages."""
    kept = [r for r in refs if include_query_strings or "?" not in r.url]
    if pattern is not None:
        hits = match_urls([r.url for r in kept], pattern)
        kept = [r for r in kept if r.url in hits]
    kept.sort(key=lambda r: _md5(r.url))
    return kept if limit is None else kept[:limit]


def _md5(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()  # noqa: S324 - an order, not security


def spread_sample(urls: Sequence[str], n: int) -> list[str]:
    """``n`` URLs in md5 order: spread across the site, and the same on every run and machine.

    The index is sorted by SURT key, so "the first n" are neighbours (week 1: n homepage
    variants). md5 is stable across DuckDB versions, unlike DuckDB's internal hash().
    """
    return sorted(urls, key=_md5)[:n]


class PageCount(BaseModel):
    """How many eligible pages a site has in a crawl's URL index (week 2, S9 and S11)."""

    model_config = ConfigDict(frozen=True)

    crawl: str
    site: str
    pattern: str
    captures: int  # rows with status 200 and text/html, on the site or its subdomains
    urls: int  # distinct URLs among them
    matching: int  # distinct URLs matching the pattern in full (query strings included)
    sample: list[str]  # a few matching URLs, spread across the site (ordered by md5 of the URL)
    shards_total: int
    shards_queried: int
    footers_read: int
    summary: SiteSummary = SiteSummary()
    from_cache: bool = False
    estimate: CostEstimate | None = None
    attempts: list[IndexAttempt] = []
    duckdb: DuckDbUsage = DuckDbUsage()
    elapsed_s: float = 0.0


class QueryBudget(Protocol):
    """How a job pays for DuckDB's requests, which DuckDB can't ask about one by one."""

    async def reserve(self, n: int) -> bool:
        """Reserve ``n`` requests before the query; False if the budget can't cover them."""
        ...

    async def settle(self, reserved: int, actual: int) -> int:
        """After the query: charge what went beyond the reservation or refund what was left.
        Returns how many requests the budget could not cover (above zero means stop)."""
        ...


class IndexBudgetError(_IndexError):
    """The job's budget can't pay for a URL-index query (before it runs, or after)."""


class UrlIndexQueries:
    """DuckDB queries over URL-index files, paid for like any other request: the estimate is
    reserved first, every request DuckDB sends is logged and reported, and the job is charged
    or refunded the difference afterwards."""

    def __init__(
        self,
        shards: ShardResolver,
        *,
        shard_map: ShardSelector | None = None,
        gate: HostGate | None = None,
        threads: int = 1,
        user_agent: str | None = None,
        on_attempt: OnAttempt | None = None,
        budget: QueryBudget | None = None,
    ) -> None:
        # threads (with the HTTP client pool sized to match) caps how many range requests DuckDB
        # keeps in flight; the gate, when given, keeps a second query from starting meanwhile.
        self._shards = shards
        self._shard_map = shard_map
        self._gate = gate
        self._threads = threads
        self._user_agent = user_agent
        self._on_attempt = on_attempt
        self._budget = budget

    async def _files(
        self, crawl: str, lo: str, hi: str
    ) -> tuple[list[str], int, list[IndexAttempt], int, CostEstimate | None]:
        """Files that may hold keys in [lo, hi): (files, files in the crawl, footer requests,
        footers read, cost estimate)."""
        files = await self._shards.shards(crawl)
        total = len(files)
        if not files or self._shard_map is None:
            return files, total, [], 0, None
        selection = await self._shard_map.select(crawl, files, lo, hi)
        return (
            selection.files,
            total,
            list(selection.attempts),
            selection.footers_read,
            selection.estimate,
        )

    async def _paid_query(
        self,
        sql: str,
        params: list[Any],
        estimate: CostEstimate | None,
        attempts: list[IndexAttempt],
    ) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
        """Reserve, run, settle. Returns rows, every attempt so far (earlier ones first) and the
        DuckDB usage of this query."""
        reserved = await self._reserve(estimate, attempts)
        try:
            rows, duck, usage = await self._run(sql, params, attempts)
        except IndexUnavailableError as exc:
            await self._settle(reserved, exc.attempts, exc.attempts)
            raise
        attempts = [*attempts, *duck]
        await self._settle(reserved, duck, attempts)
        return rows, attempts, usage

    async def _reserve(self, estimate: CostEstimate | None, attempts: list[IndexAttempt]) -> int:
        if self._budget is None:
            return 0
        want = estimate.requests if estimate is not None else 0
        if want and not await self._budget.reserve(want):
            raise IndexBudgetError(
                f"the URL-index query should take about {want} request(s)"
                f" ({estimate.bytes / 1e6 if estimate else 0:.1f} MB); the budget can't cover"
                " them, so it was not run",
                attempts,
            )
        return want

    async def _settle(
        self, reserved: int, duck: Sequence[IndexAttempt], attempts: list[IndexAttempt]
    ) -> None:
        if self._budget is None:
            return
        sent = sum(1 for a in duck if a.client is RequestClient.DUCKDB)
        over = await self._budget.settle(reserved, sent)
        if over > 0:
            raise IndexBudgetError(
                f"the URL-index query made {sent} request(s), {over} more than the budget had"
                " left; nothing was fetched",
                attempts,
            )

    async def _run(
        self, sql: str, params: list[Any], earlier: list[IndexAttempt]
    ) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
        """Run one query in a worker thread, then report DuckDB's requests even if it failed."""
        try:
            if self._gate is None:
                rows, duck, usage = await asyncio.to_thread(self._query, sql, params)
            else:
                async with self._gate.slot():
                    rows, duck, usage = await asyncio.to_thread(self._query, sql, params)
        except _DuckDbQueryError as exc:
            await self._report(exc.attempts)
            raise IndexUnavailableError(
                f"URL index query failed: {exc.error}", [*earlier, *exc.attempts]
            ) from exc.error
        await self._report(duck)
        return rows, duck, usage

    async def _report(self, attempts: list[IndexAttempt]) -> None:
        if self._on_attempt is not None:
            for attempt in attempts:
                await self._on_attempt(attempt)

    def _query(
        self, sql: str, params: list[Any]
    ) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
        return run_logged_query(sql, params, threads=self._threads, user_agent=self._user_agent)


class ColumnarIndex(UrlIndexQueries):
    source = "columnar"

    def __init__(
        self,
        shards: ShardResolver,
        *,
        shard_map: ShardSelector | None = None,
        cache_dir: Path | None = None,
        gate: HostGate | None = None,
        threads: int = 1,
        include_query_strings: bool = False,
        user_agent: str | None = None,
        on_attempt: OnAttempt | None = None,
        budget: QueryBudget | None = None,
    ) -> None:
        super().__init__(
            shards,
            shard_map=shard_map,
            gate=gate,
            threads=threads,
            user_agent=user_agent,
            on_attempt=on_attempt,
            budget=budget,
        )
        self._catalogs = CatalogStore(cache_dir)
        self._include_query = include_query_strings

    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult:
        site = normalise_domain(domain)
        if pattern is not None:
            check_pattern(pattern)
        t0 = time.monotonic()
        catalog, attempts, footers, cached = await self.catalog(site, crawl)
        matching = choose(catalog.refs, pattern=pattern, include_query_strings=self._include_query)
        return DiscoveryResult(
            refs=matching[:limit],
            from_cache=cached,
            requests=len(attempts),
            source=self.source,
            rows_scanned=catalog.summary.rows,
            attempts=attempts,
            elapsed_s=round(time.monotonic() - t0, 3),
            shards_total=catalog.shards_total,
            shards_queried=catalog.shards_queried,
            footers_read=footers,
            duckdb=None if cached else catalog.duckdb,
            candidates=len(matching),
        )

    async def count(self, site: str, crawl: str, pattern: str, *, sample: int = 5) -> PageCount:
        """Count a site's eligible pages, and those whose URL matches ``pattern`` in full."""
        site = normalise_domain(site)
        check_pattern(pattern)
        t0 = time.monotonic()
        catalog, attempts, footers, cached = await self.catalog(site, crawl)
        matching = choose(catalog.refs, pattern=pattern, include_query_strings=True)
        return PageCount(
            crawl=crawl,
            site=site,
            pattern=pattern,
            captures=catalog.summary.eligible,
            urls=catalog.summary.urls,
            matching=len(matching),
            sample=[r.url for r in matching[:sample]],
            shards_total=catalog.shards_total,
            shards_queried=catalog.shards_queried,
            footers_read=footers,
            summary=catalog.summary,
            from_cache=cached,
            estimate=catalog.estimate,
            attempts=attempts,
            duckdb=DuckDbUsage() if cached or catalog.duckdb is None else catalog.duckdb,
            elapsed_s=round(time.monotonic() - t0, 3),
        )

    async def catalog(
        self, site: str, crawl: str
    ) -> tuple[SiteCatalog, list[IndexAttempt], int, bool]:
        """The site's catalog (cached or built) and the requests made: (catalog, attempts,
        footers read, from cache)."""
        cached = self._catalogs.read(crawl, site)
        if cached is not None:
            return cached, [], 0, True
        lo, hi = surt_range(site)
        files, total, attempts, footers, estimate = await self._files(crawl, lo, hi)
        rows: list[tuple[Any, ...]] = []
        usage: DuckDbUsage | None = None
        if files:
            sql, host_params = _site_sql(host_forms(site))
            rows, attempts, usage = await self._paid_query(
                sql, [files, crawl, lo, hi, *host_params], estimate, attempts
            )
        summary, refs = summarise_rows(rows)
        catalog = SiteCatalog(
            crawl=crawl,
            site=site,
            built_at=datetime.now(UTC),
            summary=summary,
            refs=refs,
            shards_total=total,
            shards_queried=len(files),
            estimate=estimate,
            duckdb=usage,
        )
        if total:
            self._catalogs.write(catalog)
        return catalog, attempts, footers, False


def run_logged_query(
    sql: str, params: list[Any], *, threads: int = 1, user_agent: str | None = None
) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
    """One DuckDB query with its HTTP requests logged, bounded and reported (S10)."""
    # Our honest User-Agent rides along on DuckDB's own HTTP requests (ADR-003).
    config: dict[str, str | bool | int | float | list[str]] = {}
    if user_agent:
        config["custom_user_agent"] = user_agent
    try:
        con = duckdb.connect(config=config)
    except duckdb.Error as exc:
        raise _DuckDbQueryError(exc, []) from exc
    t0 = time.monotonic()
    try:
        # `threads` alone doesn't bound DuckDB's requests: its Parquet reader prefetches column
        # chunks in parallel (6 in flight with threads=1, measured 2026-11-08). Capping the
        # HTTP client pool does, without the 12x more requests of disabling prefetching.
        con.execute(f"SET threads = {int(threads)}")
        con.execute(f"SET http_client_pool_capacity = {int(threads)}")
        # Pinned to the gap the cost estimate assumes, instead of DuckDB's adaptive default.
        con.execute(f"SET parquet_prefetch_column_gap = {MERGE_GAP}")
        con.execute("CALL enable_logging(['HTTP', 'FileSystem'])")
        rows = con.execute(sql, params).fetchall()
    except duckdb.Error as exc:
        try:  # the requests made before the failure still happened, and still count
            attempts, _ = read_duckdb_requests(con, time.monotonic() - t0)
        except duckdb.Error:
            attempts = []
        con.close()
        raise _DuckDbQueryError(exc, attempts) from exc
    try:
        attempts, usage = read_duckdb_requests(con, time.monotonic() - t0)
    finally:
        con.close()
    return rows, attempts, usage


def _from_epoch_ms(value: int | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value / 1000, UTC)


class AutoIndex:
    """The CDX server first; the URL index when the CDX server is unavailable (ADR-004).

    Only "unavailable" (timeouts, 5xx after retries) falls back. A 403 means stop and ask a human
    (ADR-003), and a malformed query is our bug: both are raised, not routed around.
    """

    source = "auto"

    def __init__(self, primary: IndexSource, fallback: IndexSource) -> None:
        self._primary = primary
        self._fallback = fallback

    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult:
        try:
            return await self._primary.find(domain, crawl, limit, pattern)
        except IndexUnavailableError as exc:
            reason = f"CDX server unavailable ({summarise_attempts(exc.attempts)})"
            try:
                result = await self._fallback.find(domain, crawl, limit, pattern)
            except _IndexError as fallback_exc:
                raise type(fallback_exc)(
                    f"{fallback_exc} (tried after: {reason})",
                    [*exc.attempts, *fallback_exc.attempts],
                ) from fallback_exc
            return result.model_copy(
                update={
                    "fallback_reason": reason,
                    "attempts": [*exc.attempts, *result.attempts],
                    "requests": len(exc.attempts) + result.requests,
                }
            )
