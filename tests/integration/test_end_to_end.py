"""The week-1 walking skeleton end to end: fake Common Crawl -> Postgres ops -> raw WARC -> lake."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner

from strata_core.db import migrate
from strata_core.lake import Lake
from strata_core.ledger import PostgresLedger
from strata_core.models import (
    CrawlSpec,
    IndexChoice,
    JobState,
    Outcome,
    PolicyVerdict,
    RequestClient,
)
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_fetch.blobstore import LocalBlobStore
from strata_fetch.jobs import Services, host_of, run_cc_job
from strata_fetch.lineage import lineage
from tests.fakecc import CRAWL, INDEX_PATH, FakeCommonCrawl, build_warc

# Every job looks up robots.txt in the URL index's robotstxt subset, which DuckDB reads over HTTP.
pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("httpfs")]

PAGES = [(f"https://example.org/p{i}", f"<html>page {i}</html>".encode() * 30) for i in range(5)]


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    cc.add_warc(warc)
    cc.cdx_refs[CRAWL] = warc.refs
    cc.add_robots("example.org")
    cc.install()
    return cc


def make_settings(
    pg_dsn: str, fake: FakeCommonCrawl, tmp: Path, cache_dir: Path | None = None
) -> Settings:
    return Settings(
        ops_dsn=pg_dsn,
        lake_local_file=tmp / "lake.duckdb",
        raw_root=tmp / "raw",
        cache_dir=cache_dir or tmp / "cache",
        cc_data_base=fake.base_url,
        cc_index_base=fake.base_url,
        backoff_s=(0.01, 0.01, 0.01),
        bronze_batch_size=2,
        user_agent="strata-tests/0.1",
    )


async def allow_fast_localhost(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO ops.policies (host, max_rps, burst, max_in_flight)"
            " VALUES ('localhost', 200, 5, 2)"
        )


async def run(settings: Settings, pool: AsyncConnectionPool, spec: CrawlSpec):  # type: ignore[no-untyped-def]
    with Lake.open(settings) as lake:
        lake.bootstrap()
        async with httpx.AsyncClient() as client:
            svc = Services(
                settings=settings,
                ops=OpsStore(pool),
                ledger=PostgresLedger(pool),
                lake=lake,
                client=client,
                store=LocalBlobStore(settings.raw_root),
            )
            return await run_cc_job(spec, svc)


async def test_walking_skeleton(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    await allow_fast_localhost(ops_pool)
    settings = make_settings(pg_dsn, fake, tmp_path)
    fake.fail_next["/" + fake.cdx_refs[CRAWL][0].warc_filename].append((503, {"Retry-After": "0"}))
    spec = CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=20)

    report = await run(settings, ops_pool, spec)

    duck = duckdb_requests_seen(fake)  # the robots.txt lookup in the URL index
    assert report.state is JobState.SUCCEEDED, report.stop_reason
    assert report.outcomes == {Outcome.OK: 5}
    assert report.rows_written == 5
    assert report.targets[0].robots == "parsed: 1 host(s)"
    # 1 CDX lookup + DuckDB's robots lookup + the robots.txt record + 5 records + the 503 retry
    assert duck > 0
    assert report.budget_spent == 1 + duck + 1 + 5 + 1
    assert report.rate_limited == 1
    assert (report.index_attempts, report.index_rows_scanned) == (1, 5)
    assert report.digest_recompute_mismatches == 0

    # Every bronze row traces back to intact bytes whose digests agree.
    with Lake.open(settings, read_only=True) as lake:
        ids = lake.capture_ids(report.job_id)
        assert len(ids) == 5
        for cid in ids:
            rep = lineage(cid, lake, LocalBlobStore(settings.raw_root), data_base=fake.base_url)
            assert rep is not None
            assert rep.raw_bytes_intact
            assert rep.index_matches_warc_header is True
            assert rep.recompute_matches_header is True
            assert rep.http_status_line == "HTTP/1.1 200 OK"

    summary = await OpsStore(ops_pool).job_summary(report.job_id)
    assert summary is not None
    # Everything charged, plus the robotstxt shard map's footer read: logged, never charged.
    assert summary.attempts == report.budget_spent + 1
    assert summary.attempt_outcomes == {Outcome.OK: summary.attempts - 1, Outcome.RATE_LIMITED: 1}
    assert summary.budget_spent == report.budget_spent
    assert summary.verdicts == {PolicyVerdict.ALLOWED: 5}
    assert summary.peak_requests_per_second is not None and summary.peak_requests_per_second >= 1
    # the fake serves index and data from one host, so the breakdown has a single entry
    assert set(summary.attempts_by_host) == {host_of(fake.base_url)}


async def test_rerun_is_idempotent_and_uses_the_discovery_cache(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    await allow_fast_localhost(ops_pool)
    settings = make_settings(pg_dsn, fake, tmp_path)
    spec = CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=20)

    first = await run(settings, ops_pool, spec)
    requests_before = len(fake.requests)
    second = await run(settings, ops_pool, spec)

    assert first.rows_written == 5
    assert second.state is JobState.SUCCEEDED
    assert second.refs_from_cache is True
    # Zero requests of any kind: discovery and the robots.txt lookup are cached, the robots.txt
    # capture is read back from the store, and every selected page is already in the lake.
    assert len(fake.requests) == requests_before
    assert second.budget_spent == 0
    assert second.already_stored == 5
    assert second.rows_written == 0  # same captures, same ids: nothing duplicated
    assert second.targets[0].rows == 5  # the second job's rows are the stored captures
    with Lake.open(settings, read_only=True) as lake:
        assert lake.count() == 5
    ops = OpsStore(ops_pool)
    assert await ops.selected_capture_ids(second.job_id) == await ops.selected_capture_ids(
        first.job_id
    )


async def test_budget_stops_the_job_cleanly(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    """With discovery and the robots.txt lookup already cached by an earlier job, a budget of 3
    pays for the robots.txt record and two pages; the job then stops cleanly."""
    await allow_fast_localhost(ops_pool)
    warm = make_settings(pg_dsn, fake, tmp_path / "warm")
    await run(warm, ops_pool, CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=40))
    pages = "/" + fake.cdx_refs[CRAWL][0].warc_filename
    before = fake.count(pages)

    settings = make_settings(pg_dsn, fake, tmp_path / "cold", cache_dir=warm.cache_dir)
    spec = CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=3)
    report = await run(settings, ops_pool, spec)

    assert report.state is JobState.STOPPED
    assert report.stop_reason is not None and "budget" in report.stop_reason
    assert report.refs_from_cache is True
    assert report.budget_spent == 3
    assert report.rows_written == 2  # robots.txt + 2 records = the whole budget
    assert fake.count(pages) - before == 2


def duckdb_requests_seen(fake: FakeCommonCrawl, subset: str | None = None) -> int:
    """Requests the fake saw from DuckDB (its User-Agent starts with "duckdb/"), optionally only
    those for one subset of the URL index ("warc" for pages, "robotstxt" for robots.txt)."""
    return sum(
        1
        for ua, (_, path, _) in zip(fake.user_agents, fake.requests, strict=True)
        if ua and ua.startswith("duckdb/") and (subset is None or f"/subset={subset}/" in path)
    )


async def test_url_index_requests_are_logged_and_charged(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    """Week 2, S10: DuckDB's own requests show up in the report, the attempt log and the budget."""
    await allow_fast_localhost(ops_pool)
    fake.add_url_index(fake.cdx_refs[CRAWL])
    settings = make_settings(pg_dsn, fake, tmp_path)
    spec = CrawlSpec(
        domain="example.org", crawl=CRAWL, limit=5, budget=40, index=IndexChoice.COLUMNAR
    )

    report = await run(settings, ops_pool, spec)

    pages, robots = duckdb_requests_seen(fake, "warc"), duckdb_requests_seen(fake, "robotstxt")
    assert pages > 0 and robots > 0
    assert pages + robots == duckdb_requests_seen(fake)
    assert report.state is JobState.SUCCEEDED, report.stop_reason
    assert report.rows_written == 5
    assert report.index_source == "columnar"
    assert report.index_duckdb_requests == pages
    # Charged: DuckDB's requests for both lookups, the robots.txt record and the 5 pages. Not
    # charged: the crawl's manifest and the two shard maps' footer reads, which every later job
    # shares (ADR-004). The estimate reserved before each query was settled to the actual count.
    assert report.budget_spent == pages + robots + 1 + 5
    summary = await OpsStore(ops_pool).job_summary(report.job_id)
    assert summary is not None
    assert summary.attempts_by_client == {
        RequestClient.DUCKDB: pages + robots,
        RequestClient.STRATA: 2 + 1 + 5,
    }
    async with ops_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*), sum(bytes) FROM ops.fetch_attempts"
            " WHERE job_id = %s AND client = 'duckdb' AND url LIKE %s",
            (report.job_id, "%" + INDEX_PATH.format(crawl=CRAWL, subset="warc")),
        )
        row = await cur.fetchone()
    assert row is not None and row[0] == pages and row[1] == report.index_duckdb_bytes > 0


async def test_a_url_index_query_that_overspends_stops_before_fetching(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    await allow_fast_localhost(ops_pool)
    fake.add_url_index(fake.cdx_refs[CRAWL])
    first = await run(
        make_settings(pg_dsn, fake, tmp_path / "a"),
        ops_pool,
        CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=40, index="columnar"),
    )
    assert first.state is JobState.SUCCEEDED, first.stop_reason
    actual = first.index_duckdb_requests
    pages = "/" + fake.cdx_refs[CRAWL][0].warc_filename
    warc_reads, duck_seen = fake.count(pages), duckdb_requests_seen(fake)

    # A fresh cache, so the query must run again. The pre-flight estimate is an upper bound (one
    # request per column chunk, plus the file's HEAD and footer), so a budget that would just
    # have covered the real query is still refused before DuckDB sends anything.
    report = await run(
        make_settings(pg_dsn, fake, tmp_path / "b"),
        ops_pool,
        CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=actual, index="columnar"),
    )

    assert report.state is JobState.STOPPED
    assert report.stop_reason is not None
    assert "during discovery" in report.stop_reason
    assert "so it was not run" in report.stop_reason
    assert report.budget_spent == 0  # nothing reserved stays charged
    assert report.rows_written == 0
    assert duckdb_requests_seen(fake) == duck_seen  # DuckDB never ran
    assert fake.count(pages) == warc_reads  # and no page was read


def test_cli_demo_and_lineage(fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path) -> None:
    from strata_cli.main import app

    env = {
        "STRATA_OPS_DSN": pg_dsn,
        "STRATA_LAKE_LOCAL_FILE": str(tmp_path / "lake.duckdb"),
        "STRATA_RAW_ROOT": str(tmp_path / "raw"),
        "STRATA_CACHE_DIR": str(tmp_path / "cache"),
        "STRATA_CC_DATA_BASE": fake.base_url,
        "STRATA_CC_INDEX_BASE": fake.base_url,
        "STRATA_BACKOFF_S": "[0.01, 0.01]",
        "STRATA_USER_AGENT": "strata-tests/0.1",
    }
    runner = CliRunner()

    discovered = runner.invoke(
        app, ["discover", "--domain", "example.org", "--crawl", CRAWL, "--limit", "3"], env=env
    )
    assert discovered.exit_code == 0, discovered.output
    assert "3 distinct page(s) of 5 matching from 5 index row(s); 1 request(s)" in discovered.output

    demo = runner.invoke(
        app,
        ["demo", "--domain", "example.org", "--crawl", CRAWL, "--limit", "3", "--budget", "40"],
        env=env,
    )
    assert demo.exit_code == 0, demo.output
    assert "succeeded" in demo.output
    assert "index matches header: yes" in demo.output
    assert "bytes intact:   yes" in demo.output

    with psycopg.connect(pg_dsn) as conn:
        job_id = conn.execute("SELECT job_id FROM ops.jobs").fetchone()[0]  # type: ignore[index]
    reported = runner.invoke(app, ["report", job_id], env=env)
    assert reported.exit_code == 0, reported.output
    assert "rows in lake: 3" in reported.output

    missing = runner.invoke(app, ["lineage", "nope"], env=env)
    assert missing.exit_code == 1


def test_cli_count_and_discover_show_duckdbs_requests(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    from strata_cli.main import app

    fake.add_url_index(fake.cdx_refs[CRAWL])
    env = {
        "STRATA_CACHE_DIR": str(tmp_path / "cache"),
        "STRATA_CC_DATA_BASE": fake.base_url,
        "STRATA_CC_INDEX_BASE": fake.base_url,
        "STRATA_USER_AGENT": "strata-tests/0.1",
    }
    runner = CliRunner()
    counted = runner.invoke(
        app,
        ["count", "example.org", "--pattern", r"^https://example\.org/p[0-3]$", "--crawl", CRAWL],
        env=env,
    )
    assert counted.exit_code == 0, counted.output
    assert "URLs:     5 distinct, 4 matching the pattern" in counted.output
    assert "DuckDB: 1 file(s)," in counted.output
    assert "(at most 1 in flight)" in counted.output

    found = runner.invoke(
        app,
        ["discover", "--domain", "example.org", "--crawl", CRAWL, "--index", "columnar"],
        env=env,
    )
    # `count` built the site catalog; discovery reads it instead of querying again.
    assert found.exit_code == 0, found.output
    assert "5 page(s) of 5 matching from cache (0 requests; found via columnar)" in found.output

    bad = runner.invoke(
        app, ["count", "example.org", "--pattern", "(?=x)", "--crawl", CRAWL], env=env
    )
    assert bad.exit_code == 1
    assert "not a valid RE2 regex" in bad.output


async def test_migrate_then_policy_lookup(pg_dsn: str) -> None:
    await migrate(pg_dsn)
    async with AsyncConnectionPool(pg_dsn, open=False) as pool:
        policy = await OpsStore(pool).policy("data.commoncrawl.org")
    assert policy is not None and policy.max_in_flight == 2


class _WindowsOs:
    name = "nt"

    def __getattr__(self, attr: str) -> Any:
        return getattr(os, attr)


def test_windows_does_not_expand_a_pattern_into_file_names(
    fake: FakeCommonCrawl,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """On Windows, Click expands wildcards in arguments: ``--pattern '.*'`` became the dot-files
    in the current folder. ``main()`` turns that off; ``app()`` alone shows the bug."""
    from strata_cli.main import app, main

    fake.add_url_index(fake.cdx_refs[CRAWL])
    for name in (".env", ".gitignore"):
        (tmp_path / name).write_text("x")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STRATA_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("STRATA_CC_DATA_BASE", fake.base_url)
    monkeypatch.setenv("STRATA_CC_INDEX_BASE", fake.base_url)
    # Pretend to be Windows only where Click and Typer decide whether to expand arguments
    # (setting os.name itself would make pathlib build WindowsPaths).
    for module in ("click.core", "typer.core"):
        monkeypatch.setattr(f"{module}.os", _WindowsOs())
    argv = ["strata", "count", "example.org", "--pattern", ".*", "--crawl", CRAWL, "--sample", "0"]
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit) as expanded:
        app()  # Click's default on Windows: '.*' -> '.env .gitignore'
    assert expanded.value.code == 2
    assert "Got unexpected extra argument" in capsys.readouterr().err

    with pytest.raises(SystemExit) as kept:
        main()
    out = capsys.readouterr().out
    assert kept.value.code == 0, out
    assert "pattern:  .*" in out
    assert "5 distinct, 5 matching the pattern" in out
