"""robots.txt verdicts from Common Crawl's own robots.txt captures (ADR-006, week 2 S13).

For each host a job takes pages from: find the newest capture of ``/robots.txt`` in the same
crawl through the URL index's ``robotstxt`` subset, follow up to five redirects, fetch that
record like any capture (paced, verified, stored raw), and decide every page before it is
fetched:

==========================  ====================================================
Newest robots.txt capture   Verdict for the host's pages
==========================  ====================================================
2xx                         parse (Protego, RFC 9309); product token ``strata``
4xx ("unavailable")         allowed
5xx ("unreachable")         disallowed
redirects past five, or     needs_review
a redirect target missing
none in the crawl           needs_review
==========================  ====================================================

Crawl-delay is recorded, not applied: Strata sends the site no traffic. ``Content-Usage`` lines
(the IETF AIPREF drafts) are kept verbatim for later evaluation.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

from protego import Protego
from pydantic import BaseModel, ConfigDict

from strata_core.models import CaptureRef, PolicyVerdict
from strata_fetch.cc.index import (
    IndexAttempt,
    UrlIndexQueries,
    _from_epoch_ms,
)
from strata_fetch.surt import normalise_domain, url_key
from strata_fetch.warc import read_records

PRODUCT_TOKEN = "strata"  # noqa: S105 - the robots.txt product token, not a secret
MAX_REDIRECTS = 5  # RFC 9309 §2.3.1.2

_ROBOTS_SQL = """
SELECT url, url_surtkey, epoch_ms(fetch_time), fetch_status, fetch_redirect, content_digest,
       content_mime_type, warc_filename, warc_record_offset, warc_record_length, crawl
FROM read_parquet(?, hive_partitioning = true)
WHERE crawl = ? AND url_surtkey = ?
"""


class RobotsCapture(BaseModel):
    """One robots.txt capture found in the index, with what the crawler got back."""

    model_config = ConfigDict(frozen=True)

    ref: CaptureRef
    redirect: str | None = None


class RobotsLookup(BaseModel):
    """The redirect chain for a host's robots.txt, newest capture first at each step."""

    model_config = ConfigDict(frozen=True)

    host: str
    crawl: str
    chain: list[RobotsCapture]
    basis: str  # parsed, unavailable, unreachable, missing, unresolved
    error: str | None = None

    @property
    def final(self) -> RobotsCapture | None:
        return self.chain[-1] if self.chain else None


def _same_host(url: str, host: str) -> bool:
    found = (urlsplit(url).hostname or "").rstrip(".")
    try:
        found = found.encode("idna").decode("ascii")
    except UnicodeError:
        return False
    return found == host


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _newest(rows: Sequence[tuple[Any, ...]], url: str) -> RobotsCapture | None:
    """The newest capture of ``url``'s host, preferring ``url``'s scheme (robots.txt rules are
    per scheme and host). Keys drop the scheme and ``www``, so ``http://``, ``https://``,
    ``www.example.org`` and ``example.org`` share a robots.txt key: other hosts are ignored."""
    scheme = urlsplit(url).scheme
    host = (urlsplit(url).hostname or "").rstrip(".")
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return None
    found = [
        RobotsCapture(
            ref=CaptureRef(
                crawl=row[10],
                url=row[0],
                url_surtkey=row[1],
                fetch_time=_from_epoch_ms(row[2]),
                warc_filename=row[7],
                warc_record_offset=int(row[8]),
                warc_record_length=int(row[9]),
                content_digest=row[5],
                mime=row[6],
                status=None if row[3] is None else int(row[3]),
            ),
            redirect=row[4] or None,
        )
        for row in rows
        if _same_host(row[0], host)
    ]
    same_scheme = [c for c in found if urlsplit(c.ref.url).scheme == scheme]
    return max(same_scheme or found, key=lambda c: c.ref.fetch_time or _EPOCH, default=None)


def basis_for(status: int | None) -> str:
    if status is None:
        return "missing"
    if 200 <= status < 300:
        return "parsed"
    if 400 <= status < 500:
        return "unavailable"
    if status >= 500:
        return "unreachable"
    return "unresolved"


class RobotsLocator(UrlIndexQueries):
    """Finds robots.txt captures through the URL index's robotstxt subset (cached per host)."""

    def __init__(self, *args: Any, cache_dir: Path | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._dir = cache_dir

    def _cache(self, crawl: str, host: str) -> Path | None:
        return None if self._dir is None else self._dir / "robots" / crawl / f"{host}.json"

    async def lookup(self, host: str, crawl: str) -> tuple[RobotsLookup, list[IndexAttempt]]:
        host = normalise_domain(host)
        cache = self._cache(crawl, host)
        if cache is not None and cache.exists():
            try:
                return RobotsLookup.model_validate_json(cache.read_text("utf-8")), []
            except ValueError:
                pass
        attempts: list[IndexAttempt] = []
        chain: list[RobotsCapture] = []
        url = f"https://{host}/robots.txt"
        seen: set[str] = set()
        basis, error = "missing", None
        for _ in range(MAX_REDIRECTS + 1):
            key = url_key(url)
            # A key drops the scheme and "www", so http -> https and example.org ->
            # www.example.org (the commonest robots.txt redirects) keep the key: a loop is the
            # same scheme, host *and* key seen twice.
            parts = urlsplit(url)
            visit = f"{parts.scheme}://{(parts.hostname or '').rstrip('.')} {key}"
            if visit in seen:
                basis, error = "unresolved", f"redirect loop at {url}"
                break
            seen.add(visit)
            found, attempts = await self._find(url, key, crawl, attempts)
            if found is None:
                basis = "missing" if not chain else "unresolved"
                error = None if not chain else f"redirect target not captured: {url}"
                break
            chain.append(found)
            status = found.ref.status
            if status is not None and 300 <= status < 400:
                if not found.redirect:
                    basis, error = "unresolved", f"HTTP {status} without a redirect target"
                    break
                url = urljoin(found.ref.url, found.redirect)
                continue
            basis = basis_for(status)
            break
        else:
            basis, error = "unresolved", f"more than {MAX_REDIRECTS} redirects"
        result = RobotsLookup(host=host, crawl=crawl, chain=chain, basis=basis, error=error)
        if cache is not None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            tmp = cache.with_suffix(".tmp")
            tmp.write_text(result.model_dump_json(), encoding="utf-8")
            tmp.replace(cache)
        return result, attempts

    async def _find(
        self, url: str, key: str, crawl: str, attempts: list[IndexAttempt]
    ) -> tuple[RobotsCapture | None, list[IndexAttempt]]:
        files, _, more, _, estimate = await self._files(crawl, key, key + "\x00")
        attempts = [*attempts, *more]
        if not files:
            return None, attempts
        rows, attempts, _ = await self._paid_query(
            _ROBOTS_SQL, [files, crawl, key], estimate, attempts
        )
        return _newest(rows, url), attempts


# ---------------------------------------------------------------------------- verdicts
class HostRobots(BaseModel):
    """What a host's robots.txt says, ready to decide any of its URLs."""

    model_config = ConfigDict(frozen=True)

    host: str
    basis: str
    robots_capture_id: str | None = None
    robots_url: str | None = None
    archived_status: int | None = None
    crawl_delay_s: float | None = None
    content_usage: list[str] = []
    error: str | None = None
    text: str | None = None  # the robots.txt body, when it was parsed

    def verdict(self, url: str) -> PolicyVerdict:
        if self.basis == "parsed":
            return (
                PolicyVerdict.ALLOWED
                if _parser(self.text or "").can_fetch(url, PRODUCT_TOKEN)
                else PolicyVerdict.DISALLOWED
            )
        if self.basis == "unavailable":
            return PolicyVerdict.ALLOWED
        if self.basis == "unreachable":
            return PolicyVerdict.DISALLOWED
        return PolicyVerdict.NEEDS_REVIEW


_PARSERS: dict[str, Protego] = {}


def _parser(text: str) -> Protego:
    parser = _PARSERS.get(text)
    if parser is None:
        parser = _PARSERS[text] = Protego.parse(text)
    return parser


def payload_text(record: bytes) -> str:
    """The archived response body of one WARC record (Common Crawl stores it decoded)."""
    records = read_records(record)
    body = records[0][1] if records else b""
    return body.decode("utf-8", errors="replace")


def content_usage(text: str) -> list[str]:
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip().lower().startswith("content-usage:")
    ]


def host_robots(
    lookup: RobotsLookup, *, capture_id: str | None, record: bytes | None
) -> HostRobots:
    final = lookup.final
    common: dict[str, Any] = {
        "host": lookup.host,
        "basis": lookup.basis,
        "robots_capture_id": capture_id,
        "robots_url": final.ref.url if final else None,
        "archived_status": final.ref.status if final else None,
        "error": lookup.error,
    }
    if lookup.basis != "parsed":
        return HostRobots(**common)
    if record is None:
        return HostRobots(
            **{**common, "basis": "unresolved", "error": "robots.txt capture not fetched"}
        )
    text = payload_text(record)
    return HostRobots(
        **common,
        crawl_delay_s=_parser(text).crawl_delay(PRODUCT_TOKEN),
        content_usage=content_usage(text),
        text=text,
    )
