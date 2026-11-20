"""What the tools return: compact digests of the services' models, sized for a model's context.

A profile of a large site lists hundreds of URL shapes and thousands of changed URLs; an agent
needs the top of each list, the totals, and how much the answer cost. Every digest says so.
"""

from __future__ import annotations

from pydantic import BaseModel

from strata_fetch.lineage import LineageReport
from strata_services.changes import ChangeRates, ShapeRate, SiteDiff
from strata_services.profile import SiteProfile
from strata_services.pull import PullReport


class CrawlCount(BaseModel):
    crawl: str
    pages: int  # distinct URLs answered 200 with text/html
    captures: int  # every row the index holds for the site


class Shape(BaseModel):
    shape: str  # the URL path with digit runs as N, two segments at most
    pages: int
    share: float
    allowed: int | None  # pages robots.txt allows, when the host's robots.txt was read
    example: str


class Robots(BaseModel):
    host: str
    basis: str  # parsed, unavailable (4xx: all allowed), unreachable, missing, unresolved
    verdicts: dict[str, int]
    rules: list[str]  # the group that applies to Strata, as written
    crawl_delay_s: float | None
    sitemaps: list[str]


class ProfileDigest(BaseModel):
    site: str
    crawls: list[CrawlCount]  # newest first
    shapes: list[Shape]  # the largest, in the newest crawl
    statuses: dict[str, int]
    languages: dict[str, int]
    hosts: dict[str, int]
    robots: list[Robots]
    requests: int
    page_fetches: int = 0


def profile_digest(p: SiteProfile, top: int = 12) -> ProfileDigest:
    c = p.latest
    return ProfileDigest(
        site=p.site,
        crawls=[CrawlCount(crawl=x.crawl, pages=x.urls, captures=x.captures) for x in p.crawls],
        shapes=[
            Shape(
                shape=s.shape,
                pages=s.urls,
                share=round(s.urls / c.urls, 4) if c.urls else 0.0,
                allowed=s.allowed,
                example=s.example,
            )
            for s in c.shapes[:top]
        ],
        statuses=c.by_status,
        languages=c.by_language,
        hosts=dict(list(c.hosts.items())[:5]),
        robots=[
            Robots(
                host=r.host,
                basis=r.basis,
                verdicts=r.verdicts,
                rules=(
                    [f"User-agent: {', '.join(r.group.agents)}"]
                    + [f"{k.capitalize()}: {v}" for k, v in r.group.rules[:20]]
                    if r.group
                    else []
                ),
                crawl_delay_s=r.crawl_delay_s,
                sitemaps=r.sitemaps[:5],
            )
            for r in p.robots
        ],
        requests=p.requests,
    )


class DiffDigest(BaseModel):
    site: str
    from_crawl: str
    to_crawl: str
    counts: dict[str, int]
    shapes: list[dict[str, int | str]]  # the largest shapes and their counts per kind
    examples: dict[str, list[str]]  # a few URLs of each kind
    requests: int
    page_fetches: int = 0
    note: str = (
        "removed = not captured in the later crawl (Common Crawl doesn't revisit every page);"
        " changed = a different payload digest, which a page with a timestamp shows every time"
    )


def diff_digest(d: SiteDiff, examples: int = 8, top: int = 10) -> DiffDigest:
    kinds: dict[str, list[str]] = {}
    for c in d.changes:
        found = kinds.setdefault(c.change.value, [])
        if len(found) < examples:
            found.append(c.url)
    return DiffDigest(
        site=d.site,
        from_crawl=d.from_crawl,
        to_crawl=d.to_crawl,
        counts={k.value: v for k, v in d.counts.items()},
        shapes=[
            {"shape": s.shape, **{k.value: v for k, v in s.counts.items()}} for s in d.shapes[:top]
        ],
        examples=kinds,
        requests=d.requests,
    )


class Rate(BaseModel):
    shape: str
    changed: int
    recaptured: int  # (URL, consecutive crawl pair) cases seen in both crawls
    pages: int
    example: str


class RatesDigest(BaseModel):
    site: str
    crawls: list[str]  # oldest first
    changing: list[Rate]  # most often changing shapes: re-scrape these
    stable: list[Rate]  # shapes that never changed when re-captured: scrape once
    requests: int


def rates_digest(r: ChangeRates, top: int = 8) -> RatesDigest:
    def rate(s: ShapeRate) -> Rate:
        return Rate(
            shape=s.shape, changed=s.changed, recaptured=s.compared, pages=s.urls, example=s.example
        )

    seen = [s for s in r.shapes if s.compared]
    stable = sorted((s for s in seen if not s.changed), key=lambda s: -s.compared)
    return RatesDigest(
        site=r.site,
        crawls=r.crawls,
        changing=[rate(s) for s in seen if s.changed][:top],
        stable=[rate(s) for s in stable][:top],
        requests=r.requests,
    )


class PulledPage(BaseModel):
    url: str
    title: str | None
    chars: int
    capture_id: str  # pass it to trace_row
    robots_verdict: str
    excerpt: str


class PullDigest(BaseModel):
    job_id: str
    state: str
    stop_reason: str | None
    written: int
    selected: int
    requests: int  # budget spent by this run (0 when the job had already finished)
    stored_in: str
    pages: list[PulledPage]


class TraceDigest(BaseModel):
    capture_id: str
    url: str
    checks: dict[str, bool | None]
    common_crawl: dict[str, str | None]
    strata_copy: dict[str, str | int | None]


def trace_digest(l: LineageReport) -> TraceDigest:  # noqa: E741 - l for lineage, as in the CLI
    c = l.capture
    return TraceDigest(
        capture_id=c.capture_id,
        url=c.url,
        checks={
            "stored bytes still hash to their recorded sha256": l.raw_bytes_intact,
            "index digest matches the WARC header": l.index_matches_warc_header,
            "payload re-hashed matches the WARC header": l.recompute_matches_header,
            "robots.txt allowed it": c.policy_verdict.value == "allowed",
        },
        common_crawl={
            "crawl": c.crawl,
            "captured_at": c.upstream_fetch_time.isoformat() if c.upstream_fetch_time else None,
            "warc_url": l.upstream_url,
            "range": l.upstream_range,
            "digest": l.index_digest,
            "archived_reply": l.http_status_line,
        },
        strata_copy={
            "uri": c.raw_uri,
            "offset": c.raw_offset,
            "length": c.raw_length,
            "sha256": c.raw_sha256,
            "job_id": c.job_id,
            "robots_capture_id": c.robots_capture_id,
        },
    )


def pull_digest(report: PullReport, pages: list[PulledPage]) -> PullDigest:
    return PullDigest(
        job_id=report.job_id,
        state=report.state.value,
        stop_reason=report.run.stop_reason if report.run else None,
        written=report.written,
        selected=report.selected,
        requests=report.run.budget_spent if report.run else 0,
        stored_in=report.sink,
        pages=pages,
    )
