"""A site's captures in one crawl, read once from the URL index and cached on disk.

Choosing pages, trying another pattern, counting or profiling a site afterwards costs no request.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import duckdb
from pydantic import BaseModel, ConfigDict

from strata_core.models import CaptureRef
from strata_fetch.cc.attempts import IndexAttempt
from strata_fetch.cc.duckdb_query import CostEstimate, DuckDbUsage

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
       warc_filename, warc_record_offset, warc_record_length, crawl, content_languages
FROM read_parquet(?, hive_partitioning = true)
WHERE crawl = ?
  AND url_surtkey >= ? AND url_surtkey < ?
"""

CATALOG_VERSION = 2  # 2: languages


def site_sql(forms: Sequence[str]) -> tuple[str, list[str]]:
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
    # distinct eligible URLs by the first language Common Crawl detected (ISO 639-3, top 10)
    by_language: dict[str, int] = {}


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
    language: dict[str, str] = {}
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
            fetch_time=from_epoch_ms(row[2]),
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
            language[ref.url] = primary_language(row[10] if len(row) > 10 else None)
    refs = sorted(newest.values(), key=lambda r: (r.url_surtkey or "", r.url))
    shapes = Counter(url_shape(r.url) for r in refs)
    summary = SiteSummary(
        rows=len(rows),
        eligible=eligible,
        urls=len(refs),
        by_status=dict(status.most_common()),
        by_mime=dict(mime.most_common(8)),
        shapes=dict(shapes.most_common(12)),
        by_language=dict(Counter(language.values()).most_common(10)),
    )
    return summary, refs


def primary_language(value: str | None) -> str:
    """``"eng,fra"`` -> ``"eng"``: the language Common Crawl's detector ranked first."""
    first = (value or "").split(",", 1)[0].strip()
    return first or "-"


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


def from_epoch_ms(value: int | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value / 1000, UTC)
