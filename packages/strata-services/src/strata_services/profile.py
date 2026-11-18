"""Site profile: what a site holds in Common Crawl, before you scrape it.

From the URL index only (one site catalog per crawl) plus each main host's robots.txt capture:
pages per crawl, URL shapes, statuses, MIME types, languages, hosts, path depth, and what
robots.txt allows of each shape. No page of the site is fetched.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict

from strata_core import __version__
from strata_core.models import CaptureRef, PolicyVerdict
from strata_fetch.cc.attempts import IndexAttempt
from strata_fetch.cc.catalog import SiteCatalog, url_shape
from strata_fetch.robots import PRODUCT_TOKEN, HostRobots
from strata_fetch.robots_reader import RobotsRead
from strata_fetch.robots_rules import RobotsGroup, parse_rules
from strata_fetch.surt import normalise_domain


class CatalogSource(Protocol):
    async def catalog(
        self, site: str, crawl: str
    ) -> tuple[SiteCatalog, list[IndexAttempt], int, bool]: ...


class RobotsSource(Protocol):
    async def read(self, host: str, crawl: str) -> RobotsRead: ...


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True)


class ShapeProfile(_Frozen):
    shape: str  # /info/rfcN/: digit runs as N, two path segments at most
    urls: int
    example: str
    allowed: int | None = None  # URLs robots.txt allows, when their host's robots.txt is known


class CrawlProfile(_Frozen):
    crawl: str
    captures: int  # every capture of the site, whatever its status or type
    eligible: int  # captures with status 200 and text/html
    urls: int  # distinct eligible URLs
    by_status: dict[str, int]
    by_mime: dict[str, int]
    by_language: dict[str, int]
    hosts: dict[str, int]  # distinct eligible URLs per host (top 10)
    depth: dict[int, int]  # distinct eligible URLs per number of path segments
    shapes: list[ShapeProfile]
    query_string_urls: int
    first_capture: datetime | None
    last_capture: datetime | None
    from_cache: bool
    requests: int  # this run, footer reads excluded; 0 when the catalog came from the cache
    bytes: int  # what DuckDB downloaded for it this run
    query_s: float


class RobotsProfile(_Frozen):
    host: str
    basis: str  # parsed, unavailable, unreachable, missing, unresolved (ADR-006)
    robots_url: str | None
    capture_id: str | None
    archived_status: int | None
    crawl_delay_s: float | None
    sitemaps: list[str]
    group: RobotsGroup | None  # the group that applies to Strata's product token
    groups: int  # groups in the file
    content_usage: list[str]
    verdicts: dict[str, int]  # the latest crawl's eligible URLs of this host, by verdict
    error: str | None = None
    requests: int = 0


class SiteProfile(_Frozen):
    site: str
    built_at: datetime
    crawls: list[CrawlProfile]  # newest first
    robots: list[RobotsProfile]
    requests: int  # requests this profile spent: index queries and robots.txt reads
    # Parquet footer reads that built a crawl's shard map: once per crawl, shared by every later
    # lookup in that crawl, so never charged to a budget (ADR-004)
    footer_reads: int = 0
    strata_version: str = __version__

    @property
    def latest(self) -> CrawlProfile:
        return self.crawls[0]


TOP_SHAPES = 25
TOP_HOSTS = 10


class SiteProfiler:
    """Profiles a site in one or more crawls. ``robots_hosts`` caps how many hosts (the largest
    in the newest crawl) get their robots.txt read: each costs an index lookup."""

    def __init__(
        self,
        catalogs: CatalogSource,
        robots: RobotsSource,
        *,
        robots_hosts: int = 3,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._catalogs = catalogs
        self._robots = robots
        self._robots_hosts = robots_hosts
        self._now = now

    async def profile(self, site: str, crawls: Sequence[str]) -> SiteProfile:
        if not crawls:
            raise ValueError("a profile needs at least one crawl")
        site = normalise_domain(site)
        built: list[tuple[SiteCatalog, list[IndexAttempt], bool]] = []
        for crawl in crawls:
            catalog, attempts, _, cached = await self._catalogs.catalog(site, crawl)
            built.append((catalog, attempts, cached))
        newest = built[0][0]
        hosts = Counter(_host(r.url) for r in newest.refs)
        reads = {
            host: await self._robots.read(host, newest.crawl)
            for host, _ in hosts.most_common(self._robots_hosts)
        }
        rules = {h: r.robots for h, r in reads.items()}
        profiles = [
            crawl_profile(c, a, cached, rules if i == 0 else {})
            for i, (c, a, cached) in enumerate(built)
        ]
        robots = [robots_profile(read, newest.refs) for read in reads.values()]
        attempts = [x for _, a, _ in built for x in a]
        attempts += [x for r in reads.values() for x in r.index_attempts]
        records = sum(r.record_requests for r in reads.values())
        return SiteProfile(
            site=site,
            built_at=self._now(),
            crawls=profiles,
            robots=robots,
            requests=sum(1 for a in attempts if a.charged) + records,
            footer_reads=sum(1 for a in attempts if not a.charged),
        )


def _host(url: str) -> str:
    return (urlsplit(url).hostname or "").rstrip(".")


def _depth(url: str) -> int:
    return len([s for s in urlsplit(url).path.split("/") if s])


def crawl_profile(
    catalog: SiteCatalog,
    attempts: Sequence[IndexAttempt],
    cached: bool,
    robots: dict[str, HostRobots],
) -> CrawlProfile:
    refs = catalog.refs
    by_shape: dict[str, list[CaptureRef]] = {}
    for ref in refs:
        by_shape.setdefault(url_shape(ref.url), []).append(ref)
    ranked = sorted(by_shape.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:TOP_SHAPES]
    shapes = [
        ShapeProfile(
            shape=shape,
            urls=len(members),
            example=members[0].url,
            allowed=_allowed(members, robots),
        )
        for shape, members in ranked
    ]
    times = [r.fetch_time for r in refs if r.fetch_time is not None]
    usage = None if cached else catalog.duckdb
    return CrawlProfile(
        crawl=catalog.crawl,
        captures=catalog.summary.rows,
        eligible=catalog.summary.eligible,
        urls=catalog.summary.urls,
        by_status=catalog.summary.by_status,
        by_mime=catalog.summary.by_mime,
        by_language=catalog.summary.by_language,
        hosts=dict(Counter(_host(r.url) for r in refs).most_common(TOP_HOSTS)),
        depth=dict(sorted(Counter(_depth(r.url) for r in refs).items())),
        shapes=shapes,
        query_string_urls=sum(1 for r in refs if "?" in r.url),
        first_capture=min(times, default=None),
        last_capture=max(times, default=None),
        from_cache=cached,
        requests=sum(1 for a in attempts if a.charged),
        bytes=usage.bytes if usage else 0,
        query_s=usage.query_s if usage else 0.0,
    )


def _allowed(refs: Sequence[CaptureRef], robots: dict[str, HostRobots]) -> int | None:
    """How many of ``refs`` robots.txt allows; None when any of their hosts went unprofiled."""
    count = 0
    for ref in refs:
        rules = robots.get(_host(ref.url))
        if rules is None:
            return None
        count += rules.verdict(ref.url) is PolicyVerdict.ALLOWED
    return count


def robots_profile(read: RobotsRead, refs: Sequence[CaptureRef]) -> RobotsProfile:
    robots = read.robots
    parsed = parse_rules(robots.text or "")
    mine = [r for r in refs if _host(r.url) == robots.host]
    verdicts = Counter(robots.verdict(r.url).value for r in mine)
    return RobotsProfile(
        host=robots.host,
        basis=robots.basis,
        robots_url=robots.robots_url,
        capture_id=robots.robots_capture_id,
        archived_status=robots.archived_status,
        crawl_delay_s=robots.crawl_delay_s,
        sitemaps=parsed.sitemaps,
        group=parsed.applying_to(PRODUCT_TOKEN),
        groups=len(parsed.groups),
        content_usage=robots.content_usage,
        verdicts=dict(verdicts.most_common()),
        error=robots.error,
        requests=read.requests,
    )
