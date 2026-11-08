"""Finding captures in Common Crawl.

Three sources behind one protocol, all returning identical ``CaptureRef``s (ADR-004):

* ``CdxIndex``: Common Crawl's CDX server (pywb). Answers one domain in one request, but it is
  shared, often overloaded, and its gateway gives up after about 10 s. Answers are cached on disk.
* ``ColumnarIndex``: DuckDB over the Parquet URL index (formerly the "columnar index"). With a
  shard map it opens only the files, of the 300 page-capture files per crawl, whose SURT range
  covers the domain (one, for python.org in CC-MAIN-2026-43).
* ``AutoIndex``: the CDX server first, the URL index when the CDX server is unavailable.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin, urlparse

import duckdb
import httpx
from pydantic import BaseModel, ConfigDict

from strata_core.ledger import BudgetExhaustedError
from strata_core.models import CaptureRef, Outcome
from strata_fetch.http import (
    RETRYABLE_EXCEPTIONS,
    backoff_delay,
    classify_status,
    retry_after_seconds,
)
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.surt import normalise_domain, surt_range

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


class DiscoveryResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    refs: list[CaptureRef]
    from_cache: bool
    requests: int  # HTTP requests this lookup made through our client
    source: str
    rows_scanned: int = 0  # index rows read before removing duplicates and variants
    attempts: list[IndexAttempt] = []
    elapsed_s: float = 0.0
    fallback_reason: str | None = None  # set when AutoIndex had to leave the CDX server
    shards_total: int = 0  # URL index: Parquet files in the crawl
    shards_queried: int = 0  # URL index: files whose SURT range covers the domain
    footers_read: int = 0  # URL index: Parquet footers read to build or extend the shard map


class ShardSelection(BaseModel):
    """The URL-index files that may hold a domain, and what finding them cost."""

    model_config = ConfigDict(frozen=True)

    files: list[str]
    attempts: list[IndexAttempt] = []
    footers_read: int = 0


class IndexSource(Protocol):
    async def find(self, domain: str, crawl: str, limit: int) -> DiscoveryResult: ...


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
    field, sep, expression = body[len(mode) :].partition(":")
    if not field or not sep:
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

    def read(self, crawl: str, domain: str, limit: int) -> tuple[list[CaptureRef], str] | None:
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
        if exhausted or len(refs) >= limit:
            return refs[:limit], str(meta.get("source", "cdx"))
        return None

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


def _from_cache(cached: tuple[list[CaptureRef], str]) -> DiscoveryResult:
    refs, source = cached
    return DiscoveryResult(refs=refs, from_cache=True, requests=0, source=source)


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

    async def find(self, domain: str, crawl: str, limit: int) -> DiscoveryResult:
        domain = normalise_domain(domain)
        cached = self._cache.read(crawl, domain, limit)
        if cached is not None:
            return _from_cache(cached)
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
        # Cache every distinct page found, not just `limit`: a later, larger request may be
        # answerable without asking the index again.
        distinct = select_distinct(scanned, len(scanned), include_query_strings=self._include_query)
        self._cache.write(
            crawl,
            domain,
            source=self.source,
            rows_requested=rows,
            rows_returned=len(scanned),
            refs=distinct,
        )
        return DiscoveryResult(
            refs=distinct[:limit],
            from_cache=False,
            requests=len(attempts),
            source=self.source,
            rows_scanned=len(scanned),
            attempts=attempts,
            elapsed_s=round(time.monotonic() - t0, 3),
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
    ) -> None:
        self._client = client
        self._gate = gate
        self._base = data_base if data_base.endswith("/") else data_base + "/"
        self._cache_dir = cache_dir

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
        return [urljoin(self._base, p) for p in text.split() if "subset=warc/" in p]


# fetch_time comes back as epoch milliseconds: Common Crawl stores it timezone-aware, and DuckDB
# turns TIMESTAMPTZ into Python objects only with pytz installed, in the session's local time
# zone. epoch_ms() reads both that and the naive TIMESTAMP of older files as UTC (ADR-004).
_COLUMNAR_SQL = """
SELECT url, url_surtkey, epoch_ms(fetch_time), fetch_status, content_digest, content_mime_type,
       warc_filename, warc_record_offset, warc_record_length, crawl
FROM read_parquet(?, hive_partitioning = true)
WHERE crawl = ?
  AND url_surtkey >= ? AND url_surtkey < ?
  AND url_host_registered_domain = ?
  AND fetch_status = 200
  AND content_mime_type = 'text/html'
  AND (? OR strpos(url, '?') = 0)
ORDER BY url_surtkey, warc_filename, warc_record_offset
LIMIT ?
"""


class ColumnarIndex:
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
        overfetch: int = 5,
        max_rows: int = 1000,
        user_agent: str | None = None,
    ) -> None:
        # threads caps how many range requests DuckDB keeps in flight against a remote index;
        # the gate, when given, keeps a second query from starting while one is running.
        self._shards = shards
        self._shard_map = shard_map
        self._cache = RefCache(cache_dir, include_query_strings=include_query_strings)
        self._gate = gate
        self._threads = threads
        self._include_query = include_query_strings
        self._overfetch = overfetch
        self._max_rows = max_rows
        self._user_agent = user_agent

    async def find(self, domain: str, crawl: str, limit: int) -> DiscoveryResult:
        domain = normalise_domain(domain)
        cached = self._cache.read(crawl, domain, limit)
        if cached is not None:
            return _from_cache(cached)
        t0 = time.monotonic()
        files = await self._shards.shards(crawl)
        total = len(files)
        if not files:
            return DiscoveryResult(refs=[], from_cache=False, requests=0, source=self.source)
        lo, hi = surt_range(domain)
        attempts: list[IndexAttempt] = []
        footers = 0
        if self._shard_map is not None:
            selection = await self._shard_map.select(crawl, files, lo, hi)
            files, attempts, footers = selection.files, selection.attempts, selection.footers_read
        wanted = rows_to_request(limit, overfetch=self._overfetch, max_rows=self._max_rows)
        rows: list[tuple[Any, ...]] = []
        if files:
            try:
                if self._gate is None:
                    rows = await asyncio.to_thread(
                        self._query, files, domain, crawl, lo, hi, wanted
                    )
                else:
                    async with self._gate.slot():
                        rows = await asyncio.to_thread(
                            self._query, files, domain, crawl, lo, hi, wanted
                        )
            except duckdb.Error as exc:
                raise IndexUnavailableError(f"URL index query failed: {exc}", attempts) from exc
        scanned = [
            CaptureRef(
                crawl=row[9],
                url=row[0],
                url_surtkey=row[1],
                fetch_time=_from_epoch_ms(row[2]),
                warc_filename=row[6],
                warc_record_offset=int(row[7]),
                warc_record_length=int(row[8]),
                content_digest=row[4],
                mime=row[5],
                status=int(row[3]) if row[3] is not None else None,
            )
            for row in rows
        ]
        distinct = select_distinct(scanned, len(scanned), include_query_strings=self._include_query)
        self._cache.write(
            crawl,
            domain,
            source=self.source,
            rows_requested=wanted,
            rows_returned=len(scanned),
            refs=distinct,
        )
        return DiscoveryResult(
            refs=distinct[:limit],
            from_cache=False,
            requests=len(attempts),
            source=self.source,
            rows_scanned=len(scanned),
            attempts=attempts,
            elapsed_s=round(time.monotonic() - t0, 3),
            shards_total=total,
            shards_queried=len(files),
            footers_read=footers,
        )

    def _query(
        self, files: list[str], domain: str, crawl: str, lo: str, hi: str, limit: int
    ) -> list[tuple[Any, ...]]:
        # Our honest User-Agent rides along on DuckDB's own HTTP requests (ADR-003).
        config: dict[str, str | bool | int | float | list[str]] = {}
        if self._user_agent:
            config["custom_user_agent"] = self._user_agent
        con = duckdb.connect(config=config)
        try:
            con.execute(f"SET threads = {int(self._threads)}")
            params = [files, crawl, lo, hi, domain, self._include_query, limit]
            return con.execute(_COLUMNAR_SQL, params).fetchall()
        finally:
            con.close()


def _from_epoch_ms(value: int | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value / 1000, UTC)


# --------------------------------------------------------------------------------- auto
class AutoIndex:
    """The CDX server first; the URL index when the CDX server is unavailable (ADR-004).

    Only "unavailable" (timeouts, 5xx after retries) falls back. A 403 means stop and ask a human
    (ADR-003), and a malformed query is our bug: both are raised, not routed around.
    """

    source = "auto"

    def __init__(self, primary: IndexSource, fallback: IndexSource) -> None:
        self._primary = primary
        self._fallback = fallback

    async def find(self, domain: str, crawl: str, limit: int) -> DiscoveryResult:
        try:
            return await self._primary.find(domain, crawl, limit)
        except IndexUnavailableError as exc:
            reason = f"CDX server unavailable ({summarise_attempts(exc.attempts)})"
            try:
                result = await self._fallback.find(domain, crawl, limit)
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
