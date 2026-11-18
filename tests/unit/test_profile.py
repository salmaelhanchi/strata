"""Site profiles: the request budget, robots.txt rules for people, the profile and its report."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from strata_core.models import CaptureRef
from strata_fetch.cc.attempts import IndexAttempt
from strata_fetch.cc.catalog import SiteCatalog, SiteSummary
from strata_fetch.robots import HostRobots
from strata_fetch.robots_reader import RobotsRead
from strata_fetch.robots_rules import parse_rules
from strata_services.budget import RequestBudget
from strata_services.profile import SiteProfiler
from strata_services.profile_html import render

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------- request budget
async def test_a_request_budget_is_spent_before_the_request() -> None:
    budget = RequestBudget(5)
    assert await budget.reserve(3)
    assert not await budget.reserve(3)  # 6 > 5: refused, nothing spent
    assert budget.spent == 3
    assert await budget.settle(3, 2) == 0  # one reserved request was never sent: refunded
    assert budget.spent == 2
    assert budget.take() and budget.take() and budget.take()
    assert not budget.take()
    assert budget.left == 0
    assert await budget.settle(0, 2) == 2  # DuckDB sent two more than the cap allowed


def test_a_request_budget_needs_a_cap() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        RequestBudget(0)


# ---------------------------------------------------------------------------- robots.txt rules
ROBOTS = """\
# a comment
User-agent: Googlebot
User-agent: Bingbot
Disallow: /private/   # trailing comment

User-agent: *
Crawl-delay: 2
Disallow: /search
Allow: /search/help

User-agent: strata
Disallow: /drafts/
Sitemap: https://example.org/sitemap.xml
"""


def test_robots_rules_are_listed_group_by_group() -> None:
    rules = parse_rules(ROBOTS)
    assert [g.agents for g in rules.groups] == [["Googlebot", "Bingbot"], ["*"], ["strata"]]
    assert rules.groups[0].rules == [("disallow", "/private/")]
    assert rules.groups[1].crawl_delay == "2"
    assert rules.sitemaps == ["https://example.org/sitemap.xml"]


def test_the_group_naming_strata_wins_over_the_star_group() -> None:
    rules = parse_rules(ROBOTS)
    assert rules.applying_to("strata") == rules.groups[2]
    assert rules.applying_to("STRATA") == rules.groups[2]
    assert rules.applying_to("otherbot") == rules.groups[1]
    assert parse_rules("Sitemap: https://x.org/s.xml\n").applying_to("strata") is None


# ---------------------------------------------------------------------------- the profile
def ref(url: str, crawl: str = "CC-MAIN-2026-43", day: int = 1) -> CaptureRef:
    return CaptureRef(
        crawl=crawl,
        url=url,
        fetch_time=datetime(2026, 10, day, tzinfo=UTC),
        warc_filename="crawl-data/x.warc.gz",
        warc_record_offset=0,
        warc_record_length=100,
        content_digest="A" * 32,
        mime="text/html",
        status=200,
    )


def catalog(crawl: str, urls: list[str]) -> SiteCatalog:
    refs = [ref(u, crawl, day=1 + i % 28) for i, u in enumerate(urls)]
    return SiteCatalog(
        crawl=crawl,
        site="example.org",
        built_at=NOW,
        summary=SiteSummary(
            rows=len(urls) + 3,
            eligible=len(urls),
            urls=len(urls),
            by_status={"200": len(urls), "301": 3},
            by_mime={"text/html": len(urls) + 3},
            by_language={"eng": len(urls)},
        ),
        refs=refs,
    )


def attempt() -> IndexAttempt:
    return IndexAttempt(
        attempt=1, url="u", started_at=NOW, elapsed_ms=1, http_status=206, outcome="ok"
    )  # type: ignore[arg-type]


class FakeCatalogs:
    def __init__(self, catalogs: dict[str, SiteCatalog], cached: set[str]) -> None:
        self.catalogs = catalogs
        self.cached = cached
        self.asked: list[tuple[str, str]] = []

    async def catalog(
        self, site: str, crawl: str
    ) -> tuple[SiteCatalog, list[IndexAttempt], int, bool]:
        self.asked.append((site, crawl))
        hit = crawl in self.cached
        return self.catalogs[crawl], [] if hit else [attempt(), attempt()], 0, hit


class FakeRobots:
    def __init__(self, texts: dict[str, str]) -> None:
        self.texts = texts
        self.asked: list[str] = []

    async def read(self, host: str, crawl: str) -> RobotsRead:
        self.asked.append(host)
        text = self.texts.get(host)
        robots = HostRobots(
            host=host,
            basis="parsed" if text is not None else "missing",
            robots_capture_id="c" * 64 if text is not None else None,
            robots_url=f"https://{host}/robots.txt",
            archived_status=200,
            text=text,
        )
        return RobotsRead(robots=robots, index_attempts=[attempt()], record_requests=1)


URLS = [
    *(f"https://example.org/pep-{n:04d}/" for n in range(1, 9)),
    "https://example.org/search?q=x",
    "https://example.org/search/help",
    "https://docs.example.org/guide/intro",
]


async def test_a_profile_reads_the_index_and_robots_txt_but_no_page() -> None:
    catalogs = FakeCatalogs(
        {
            "CC-MAIN-2026-43": catalog("CC-MAIN-2026-43", URLS),
            "CC-MAIN-2026-38": catalog("CC-MAIN-2026-38", URLS[:5]),
        },
        cached={"CC-MAIN-2026-38"},
    )
    robots = FakeRobots({"example.org": "User-agent: *\nDisallow: /search\nAllow: /search/help\n"})
    profiler = SiteProfiler(catalogs, robots, robots_hosts=1, now=lambda: NOW)

    p = await profiler.profile("Example.org", ["CC-MAIN-2026-43", "CC-MAIN-2026-38"])

    assert p.site == "example.org"
    assert [c.crawl for c in p.crawls] == ["CC-MAIN-2026-43", "CC-MAIN-2026-38"]
    assert [c.urls for c in p.crawls] == [11, 5]
    assert p.latest.hosts == {"example.org": 10, "docs.example.org": 1}
    assert robots.asked == ["example.org"]  # robots_hosts=1: the largest host only
    shapes = {s.shape: s for s in p.latest.shapes}
    assert shapes["/pep-N/"].urls == 8
    assert shapes["/pep-N/"].allowed == 8
    assert shapes["/search?…"].allowed == 0  # /search?q=x is disallowed
    assert shapes["/search/help"].allowed == 1  # the longer Allow wins (RFC 9309)
    assert shapes["/guide/intro"].allowed is None  # docs.example.org's robots.txt wasn't read
    assert p.latest.query_string_urls == 1
    assert p.latest.depth == {1: 9, 2: 2}
    assert p.robots[0].verdicts == {"allowed": 9, "disallowed": 1}
    assert p.robots[0].group is not None and p.robots[0].group.agents == ["*"]
    assert p.crawls[1].from_cache and p.crawls[1].requests == 0
    assert p.requests == 2 + 2  # two index requests for the new catalog, two for robots.txt


async def test_a_profile_needs_a_crawl() -> None:
    profiler = SiteProfiler(FakeCatalogs({}, set()), FakeRobots({}))
    with pytest.raises(ValueError, match="at least one crawl"):
        await profiler.profile("example.org", [])


async def test_the_html_report_is_self_contained_and_escapes_what_the_index_says() -> None:
    hostile = "https://example.org/<script>alert(1)</script>"
    catalogs = FakeCatalogs({"CC-MAIN-2026-43": catalog("CC-MAIN-2026-43", [hostile])}, set())
    robots = FakeRobots({"example.org": "User-agent: *\nDisallow: /<b>\nSitemap: <i>x</i>\n"})
    p = await SiteProfiler(catalogs, robots, now=lambda: NOW).profile(
        "example.org", ["CC-MAIN-2026-43"]
    )

    page = render(p)

    assert "<script>" not in page
    assert "&lt;script&gt;" in page
    assert "Disallow: /&lt;b&gt;" in page
    assert "<i>x</i>" not in page
    for heading in ("Pages per crawl", "URL shapes in CC-MAIN-2026-43", "robots.txt", "Language"):
        assert heading in page
    assert "http://" not in page and "src=" not in page  # nothing loaded from elsewhere
