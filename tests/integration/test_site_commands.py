"""``strata profile``, ``diff`` and ``changes`` end to end against a fake Common Crawl: index
and robots.txt only."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner

from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc

pytestmark = pytest.mark.usefixtures("httpfs")

PAGES = [
    *((f"https://example.org/pep-{i:04d}/", b"<html>pep</html>" * 20) for i in range(6)),
    ("https://example.org/drafts/one", b"<html>draft</html>" * 20),
    ("https://docs.example.org/guide/intro", b"<html>guide</html>" * 20),
]
ROBOTS = b"User-agent: *\nDisallow: /drafts/\nSitemap: https://example.org/sitemap.xml\n"


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    cc.add_warc(warc)
    cc.add_url_index(warc.refs)
    cc.add_robots("example.org", ROBOTS)
    cc.install()
    return cc


def env_for(fake: FakeCommonCrawl, tmp: Path) -> dict[str, str]:
    return {
        "STRATA_CACHE_DIR": str(tmp / "cache"),
        "STRATA_CC_DATA_BASE": fake.base_url,
        "STRATA_CC_INDEX_BASE": fake.base_url,
        "STRATA_BACKOFF_S": "[0.01, 0.01]",
        "STRATA_USER_AGENT": "strata-tests/0.1",
    }


def test_profile_writes_a_report_and_fetches_no_page(fake: FakeCommonCrawl, tmp_path: Path) -> None:
    from strata_cli.main import app

    env, report = env_for(fake, tmp_path), tmp_path / "out" / "example.html"
    runner = CliRunner()
    args = ["profile", "example.org", "--crawl", CRAWL, "--robots-hosts", "1"]

    first = runner.invoke(app, [*args, "--html", str(report)], env=env)

    assert first.exit_code == 0, first.output
    assert f"example.org: 8 page(s) in {CRAWL}" in first.output
    assert "robots.txt example.org: parsed; allowed 6, disallowed 1" in first.output
    assert "/pep-N/" in first.output
    page_warc = "/" + build_warc(PAGES).filename
    assert fake.count(page_warc) == 0  # no page of the site was read
    html = report.read_text(encoding="utf-8")
    assert "URL shapes in" in html and "https://example.org/sitemap.xml" in html
    robots_reads = fake.count("/crawl-data/" + CRAWL + "/segments/1/robotstxt/")
    assert robots_reads == 1

    again = runner.invoke(app, args, env=env)

    assert again.exit_code == 0, again.output
    assert "cost: 0 request(s); catalogs from cache: 1/1" in again.output
    assert fake.count("/crawl-data/" + CRAWL + "/segments/1/robotstxt/") == robots_reads


def test_profile_stops_before_a_query_its_budget_cannot_pay(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    from strata_cli.main import app

    result = CliRunner().invoke(
        app,
        ["profile", "example.org", "--crawl", CRAWL, "--budget", "1"],
        env=env_for(fake, tmp_path),
    )

    assert result.exit_code == 1
    assert "profile failed" in result.output
    assert "so it was not run" in result.output


def test_profile_waits_out_a_throttled_robots_txt_read(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    from strata_cli.main import app

    robots_file = f"/crawl-data/{CRAWL}/segments/1/robotstxt/{CRAWL}-robots-00000.warc.gz"
    fake.fail_next[robots_file].append((503, {"Retry-After": "0"}))

    result = CliRunner().invoke(
        app, ["profile", "example.org", "--crawl", CRAWL], env=env_for(fake, tmp_path)
    )

    assert result.exit_code == 0, result.output
    assert "robots.txt example.org: parsed" in result.output
    assert fake.count(robots_file) == 2  # the 503, then the read that worked


OLD = "CC-MAIN-2026-13"


def test_diff_and_changes_read_digests_from_the_index_only(
    httpserver: HTTPServer, tmp_path: Path
) -> None:
    from strata_cli.main import app

    cc = FakeCommonCrawl(httpserver)
    before = build_warc([(f"https://example.org/p{i}", b"v1" * 40) for i in range(5)], crawl=OLD)
    after = build_warc(
        [(f"https://example.org/p{i}", b"v2" * 40 if i == 2 else b"v1" * 40) for i in range(1, 6)]
    )
    for warc, crawl in ((before, OLD), (after, CRAWL)):
        cc.add_warc(warc)
        cc.add_url_index(warc.refs, crawl)
    cc.files["/collinfo.json"] = json.dumps([{"id": CRAWL}, {"id": OLD}]).encode()
    cc.install()
    env, runner = env_for(cc, tmp_path), CliRunner()

    diffed = runner.invoke(app, ["diff", "example.org", "--from", OLD, "--to", CRAWL], env=env)

    assert diffed.exit_code == 0, diffed.output
    assert f"{OLD} -> {CRAWL}: added 1, removed 1, changed 1, same 3" in diffed.output
    assert "page fetches: 0" in diffed.output
    assert "changed  https://example.org/p2" in diffed.output
    assert cc.count("/" + before.filename) == 0
    assert cc.count("/" + after.filename) == 0

    rates = runner.invoke(app, ["changes", "example.org", "--crawls", "2"], env=env)

    assert rates.exit_code == 0, rates.output
    assert "1 of 4 re-captures changed (25%)" in rates.output
    assert "index requests: 0" in rates.output  # both catalogs were cached by the diff
