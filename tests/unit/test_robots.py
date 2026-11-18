"""robots.txt verdicts from Common Crawl's own captures (ADR-006).

The lookups run against the fake Common Crawl: robots.txt records in a WARC file and rows in
the URL index's ``robotstxt`` subset, read by DuckDB over HTTP like the real thing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from pytest_httpserver import HTTPServer

from strata_core.models import CaptureRef, PolicyVerdict
from strata_core.settings import Settings
from strata_fetch.cc.sources import make_robots
from strata_fetch.ratelimit import HostGate
from strata_fetch.robots import (
    HostRobots,
    RobotsLookup,
    content_usage,
    host_robots,
    payload_text,
)
from tests.fakecc import CRAWL, FakeCommonCrawl

pytestmark = pytest.mark.usefixtures("httpfs")


async def no_sleep(_: float) -> None:
    return None


@pytest.fixture
def cc(httpserver: HTTPServer) -> FakeCommonCrawl:
    fake = FakeCommonCrawl(httpserver)
    fake.install()
    return fake


def record_of(cc: FakeCommonCrawl, ref: CaptureRef) -> bytes:
    data = cc.files["/" + ref.warc_filename]
    return data[ref.warc_record_offset : ref.warc_record_offset + ref.warc_record_length]


async def lookup(cc: FakeCommonCrawl, host: str, tmp_path: Path) -> tuple[RobotsLookup, int]:
    settings = Settings(cc_data_base=cc.base_url, cache_dir=tmp_path / "cache")
    async with httpx.AsyncClient() as client:
        locator = make_robots(
            client=client,
            settings=settings,
            data_gate=HostGate(rate=1000, burst=10),
            sleep=no_sleep,
        )
        found, attempts = await locator.lookup(host, CRAWL)
    return found, len(attempts)


async def verdicts(
    cc: FakeCommonCrawl, host: str, tmp_path: Path, *paths: str
) -> tuple[HostRobots, list[PolicyVerdict]]:
    found, _ = await lookup(cc, host, tmp_path)
    final = found.final
    hr = host_robots(
        found,
        capture_id="robots-1" if final else None,
        record=record_of(cc, final.ref) if final else None,
    )
    return hr, [hr.verdict(f"https://{host}{p}") for p in paths]


async def test_strata_follows_its_own_group_not_the_wildcard(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    cc.add_robots(
        "example.org",
        b"User-agent: *\nDisallow: /\n\n"
        b"User-agent: strata\nDisallow: /private/\nAllow: /\nCrawl-delay: 7\n"
        b"Content-Usage: train-ai=n\n",
    )
    hr, got = await verdicts(cc, "example.org", tmp_path, "/docs/a", "/private/b")
    assert hr.basis == "parsed"
    assert got == [PolicyVerdict.ALLOWED, PolicyVerdict.DISALLOWED]
    assert hr.crawl_delay_s == 7  # recorded, not applied: Strata sends the site nothing
    assert hr.content_usage == ["Content-Usage: train-ai=n"]
    assert hr.archived_status == 200
    assert hr.robots_url == "https://example.org/robots.txt"


async def test_the_wildcard_group_applies_when_strata_has_none(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    cc.add_robots("example.org", b"User-agent: otherbot\nAllow: /\n\nUser-agent: *\nDisallow: /\n")
    _, got = await verdicts(cc, "example.org", tmp_path, "/a")
    assert got == [PolicyVerdict.DISALLOWED]


@pytest.mark.parametrize(
    ("status", "basis", "verdict"),
    [
        (404, "unavailable", PolicyVerdict.ALLOWED),  # RFC 9309 2.3.1.3
        (410, "unavailable", PolicyVerdict.ALLOWED),
        (503, "unreachable", PolicyVerdict.DISALLOWED),  # RFC 9309 2.3.1.4
        (500, "unreachable", PolicyVerdict.DISALLOWED),
    ],
)
async def test_status_codes_decide_without_a_body(
    cc: FakeCommonCrawl, tmp_path: Path, status: int, basis: str, verdict: PolicyVerdict
) -> None:
    cc.add_robots("example.org", b"User-agent: *\nDisallow: /\n", status=status)
    hr, got = await verdicts(cc, "example.org", tmp_path, "/a")
    assert (hr.basis, got) == (basis, [verdict])
    assert hr.robots_capture_id == "robots-1"  # the verdict still points at its evidence


async def test_no_capture_in_the_crawl_needs_review(cc: FakeCommonCrawl, tmp_path: Path) -> None:
    cc.add_robots("other.org")  # the robotstxt subset exists, but not for this host
    hr, got = await verdicts(cc, "example.org", tmp_path, "/a")
    assert (hr.basis, hr.robots_capture_id, got) == ("missing", None, [PolicyVerdict.NEEDS_REVIEW])


async def test_a_redirect_is_followed_to_the_www_host_sharing_its_key(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    """example.org and www.example.org have the same SURT key: the lookup must still tell the
    redirect (example.org) from its target (www.example.org) by host."""
    cc.add_robots("example.org", b"", status=301, redirect="https://www.example.org/robots.txt")
    cc.add_robots("www.example.org", b"User-agent: *\nDisallow: /search\n")
    found, _ = await lookup(cc, "example.org", tmp_path)
    assert [c.ref.url for c in found.chain] == [
        "https://example.org/robots.txt",
        "https://www.example.org/robots.txt",
    ]
    hr, got = await verdicts(cc, "example.org", tmp_path, "/a", "/search?q=x")
    assert hr.basis == "parsed"
    assert got == [PolicyVerdict.ALLOWED, PolicyVerdict.DISALLOWED]
    www, _ = await lookup(cc, "www.example.org", tmp_path)
    assert [c.ref.url for c in www.chain] == ["https://www.example.org/robots.txt"]


async def test_http_redirects_to_https_and_the_https_capture_is_preferred(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    cc.add_robots(
        "example.org", b"", status=301, redirect="https://example.org/robots.txt", scheme="http"
    )
    cc.add_robots("example.org", b"User-agent: *\nDisallow: /x\n")
    found, _ = await lookup(cc, "example.org", tmp_path)
    assert [c.ref.url for c in found.chain] == ["https://example.org/robots.txt"]
    assert found.basis == "parsed"


async def test_only_an_http_redirect_to_https_is_unresolved(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    """The https file the redirect points at was never captured: no rules to apply."""
    cc.add_robots(
        "example.org", b"", status=301, redirect="https://example.org/robots.txt", scheme="http"
    )
    hr, got = await verdicts(cc, "example.org", tmp_path, "/a")
    assert hr.basis == "unresolved"
    assert got == [PolicyVerdict.NEEDS_REVIEW]


async def test_a_redirect_to_an_uncaptured_target_is_unresolved(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    cc.add_robots("example.org", b"", status=302, redirect="https://cdn.example.net/robots.txt")
    hr, got = await verdicts(cc, "example.org", tmp_path, "/a")
    assert hr.basis == "unresolved"
    assert hr.error is not None and "not captured" in hr.error
    assert got == [PolicyVerdict.NEEDS_REVIEW]


async def test_redirect_loops_and_long_chains_are_unresolved(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    cc.add_robots("a.example.org", b"", status=301, redirect="https://b.example.org/robots.txt")
    cc.add_robots("b.example.org", b"", status=301, redirect="https://a.example.org/robots.txt")
    for i in range(7):  # c0 -> c1 -> ... -> c6: more than five redirects
        cc.add_robots(
            f"c{i}.example.org",
            b"",
            status=301,
            redirect=f"https://c{i + 1}.example.org/robots.txt",
        )
    loop, _ = await lookup(cc, "a.example.org", tmp_path)
    long, _ = await lookup(cc, "c0.example.org", tmp_path)
    assert (loop.basis, long.basis) == ("unresolved", "unresolved")
    assert loop.error is not None and "loop" in loop.error
    assert long.error is not None and "more than 5 redirects" in long.error
    assert len(long.chain) == 6  # the original request and five redirects


async def test_the_newest_capture_wins(cc: FakeCommonCrawl, tmp_path: Path) -> None:
    old, new = datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 20, tzinfo=UTC)
    cc.add_robots("example.org", b"User-agent: *\nDisallow: /\n", fetch_time=new)
    cc.add_robots("example.org", b"User-agent: *\nAllow: /\n", fetch_time=old)
    _, got = await verdicts(cc, "example.org", tmp_path, "/a")
    assert got == [PolicyVerdict.DISALLOWED]


async def test_a_lookup_is_cached_per_crawl_and_host(cc: FakeCommonCrawl, tmp_path: Path) -> None:
    cc.add_robots("example.org")
    first, attempts = await lookup(cc, "example.org", tmp_path)
    before = len(cc.requests)
    again, cached_attempts = await lookup(cc, "example.org", tmp_path)
    assert attempts > 0 and cached_attempts == 0
    assert len(cc.requests) == before  # nothing sent
    assert again == first


def test_a_parsed_basis_without_its_record_is_unresolved() -> None:
    found = RobotsLookup(host="example.org", crawl=CRAWL, chain=[], basis="parsed")
    hr = host_robots(found, capture_id=None, record=None)
    assert hr.basis == "unresolved"
    assert hr.verdict("https://example.org/a") is PolicyVerdict.NEEDS_REVIEW


def test_content_usage_lines_are_kept_verbatim() -> None:
    text = "User-agent: *\n  Content-Usage: train-ai=n, search=y  \ncontent-usage: /x train-ai=y\n"
    assert content_usage(text) == [
        "Content-Usage: train-ai=n, search=y",
        "content-usage: /x train-ai=y",
    ]
    assert payload_text(b"") == ""


async def test_a_relative_redirect_target_is_resolved_against_its_url(
    cc: FakeCommonCrawl, tmp_path: Path
) -> None:
    """Common Crawl's erratum: the redirect target in the URL index may be relative."""
    cc.add_robots("example.org", b"", status=301, redirect="//www.example.org/robots.txt")
    cc.add_robots("www.example.org", b"User-agent: *\nDisallow: /x\n")
    found, _ = await lookup(cc, "example.org", tmp_path)
    assert found.basis == "parsed"
    assert found.chain[-1].ref.url == "https://www.example.org/robots.txt"
