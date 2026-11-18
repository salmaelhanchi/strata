"""Common Crawl's CDX server (pywb), with answers cached on disk.

It answers one domain in one request, but it is shared, often overloaded, and its gateway gives
up after about 10 s. ``AutoIndex`` falls back to the URL index when it is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

from strata_core.ledger import BudgetExhaustedError
from strata_core.models import CaptureRef, Outcome
from strata_fetch.cc.attempts import (
    IndexAttempt,
    IndexBlockedError,
    IndexQueryError,
    IndexUnavailableError,
    OnAttempt,
    classify_index_reply,
    server_message,
)
from strata_fetch.cc.catalog import choose, select_distinct
from strata_fetch.cc.duckdb_query import check_pattern
from strata_fetch.cc.index import DiscoveryResult
from strata_fetch.http import (
    RETRYABLE_EXCEPTIONS,
    backoff_delay,
    retry_after_seconds,
)
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.surt import normalise_domain, on_site


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


Reserve = Callable[[str], Awaitable[bool]]


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
