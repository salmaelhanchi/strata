"""``strata pull`` end to end: fake Common Crawl -> job -> lake -> your database, and back."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import psycopg
import pytest
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner

from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc

pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("httpfs")]

TEXT = " ".join(f"Sentence {i} of a page that has real content to extract." for i in range(15))
PAGES = [
    (
        f"https://example.org/guide/{i}",
        f'<html lang="en"><head><title>Guide {i}</title></head><body><nav><a href="/">Home</a>'
        f"</nav><article><p>{TEXT}</p><p>{TEXT}</p></article>"
        f'<a href="/guide/{i + 1}">next</a></body></html>'.encode(),
    )
    for i in range(4)
]


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    cc.add_warc(warc)
    cc.add_url_index(warc.refs)
    cc.add_robots("example.org")
    cc.install()
    return cc


def env_for(fake: FakeCommonCrawl, pg_dsn: str, tmp: Path) -> dict[str, str]:
    return {
        "STRATA_OPS_DSN": pg_dsn,
        "STRATA_LAKE_LOCAL_FILE": str(tmp / "lake.duckdb"),
        "STRATA_RAW_ROOT": str(tmp / "raw"),
        "STRATA_CACHE_DIR": str(tmp / "cache"),
        "STRATA_CC_DATA_BASE": fake.base_url,
        "STRATA_CC_INDEX_BASE": fake.base_url,
        "STRATA_BACKOFF_S": "[0.01, 0.01]",
        "STRATA_USER_AGENT": "strata-tests/0.1",
    }


def test_pull_into_sqlite_then_postgres_with_lineage(
    fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path
) -> None:
    from strata_cli.main import app

    env, runner = env_for(fake, pg_dsn, tmp_path), CliRunner()
    db = tmp_path / "pages.db"
    args = ["pull", "example.org", "--crawl", CRAWL, "--limit", "3", "--budget", "40"]

    first = runner.invoke(app, [*args, "--into", f"sqlite:///{db.as_posix()}"], env=env)

    assert first.exit_code == 0, first.output
    assert "pulled 3 of 3 selected page(s)" in first.output
    with sqlite3.connect(db) as con:
        rows = con.execute(
            "SELECT url, title, lang, main_text, robots_verdict, capture_id FROM strata_pages"
        ).fetchall()
    assert len(rows) == 3
    assert all(r[1].startswith("Guide ") and r[2] == "en" for r in rows)
    assert all("real content to extract" in r[3] and "Home" not in r[3] for r in rows)
    assert {r[4] for r in rows} == {"allowed"}

    traced = runner.invoke(app, ["lineage", rows[0][5]], env=env)
    assert traced.exit_code == 0, traced.output
    assert "bytes intact:   yes" in traced.output

    warc = "/" + build_warc(PAGES).filename
    reads = fake.count(warc)
    again = runner.invoke(app, [*args, "--into", pg_dsn], env=env)

    assert again.exit_code == 0, again.output
    assert "already succeeded; nothing fetched" in again.output
    assert fake.count(warc) == reads  # the same job: no page is read twice
    with psycopg.connect(pg_dsn) as conn:
        count, links = conn.execute(
            "SELECT count(*), max(jsonb_array_length(out_links)) FROM strata_pages"
        ).fetchone()  # type: ignore[misc]
    assert (count, links) == (3, 2)


def test_pull_refuses_an_unknown_sink(fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path) -> None:
    from strata_cli.main import app

    result = CliRunner().invoke(
        app, ["pull", "example.org", "--into", "mysql://x/y"], env=env_for(fake, pg_dsn, tmp_path)
    )
    assert result.exit_code != 0
    assert "use one of" in result.output
