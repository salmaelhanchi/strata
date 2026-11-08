"""The week-1 walking skeleton end to end: fake Common Crawl -> Postgres ops -> raw WARC -> lake."""

from __future__ import annotations

from pathlib import Path

import httpx
import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner

from strata_core.db import migrate
from strata_core.lake import Lake
from strata_core.ledger import PostgresLedger
from strata_core.models import CrawlSpec, JobState, Outcome
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_fetch.blobstore import LocalBlobStore
from strata_fetch.jobs import Services, host_of, run_cc_job
from strata_fetch.lineage import lineage
from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc

pytestmark = pytest.mark.postgres

PAGES = [(f"https://example.org/p{i}", f"<html>page {i}</html>".encode() * 30) for i in range(5)]


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    cc.add_warc(warc)
    cc.cdx_refs[CRAWL] = warc.refs
    cc.install()
    return cc


def make_settings(pg_dsn: str, fake: FakeCommonCrawl, tmp: Path) -> Settings:
    return Settings(
        ops_dsn=pg_dsn,
        lake_local_file=tmp / "lake.duckdb",
        raw_root=tmp / "raw",
        cache_dir=tmp / "cache",
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

    assert report.state is JobState.SUCCEEDED, report.stop_reason
    assert report.outcomes == {Outcome.OK: 5}
    assert report.rows_written == 5
    assert report.budget_spent == 7  # 1 index lookup + 5 records + 1 retry after the 503
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
    # 1 index lookup + 5 record fetches + the 503 before a retry; index attempts are logged too
    assert summary.attempts == 7
    assert summary.attempt_outcomes == {Outcome.OK: 6, Outcome.RATE_LIMITED: 1}
    assert summary.budget_spent == 7
    # the fake serves index and data from one host, so the breakdown has a single entry
    assert summary.attempts_by_host == {
        host_of(fake.base_url): {Outcome.OK: 6, Outcome.RATE_LIMITED: 1}
    }


async def test_rerun_is_idempotent_and_uses_the_discovery_cache(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    await allow_fast_localhost(ops_pool)
    settings = make_settings(pg_dsn, fake, tmp_path)
    spec = CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=20)

    first = await run(settings, ops_pool, spec)
    index_requests = fake.count("/" + CRAWL + "-index")
    second = await run(settings, ops_pool, spec)

    assert first.rows_written == 5
    assert second.refs_from_cache is True
    assert fake.count("/" + CRAWL + "-index") == index_requests  # zero new index requests
    assert second.rows_written == 0  # same captures, same ids: nothing duplicated
    with Lake.open(settings, read_only=True) as lake:
        assert lake.count() == 5


async def test_budget_stops_the_job_cleanly(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> None:
    await allow_fast_localhost(ops_pool)
    settings = make_settings(pg_dsn, fake, tmp_path)
    spec = CrawlSpec(domain="example.org", crawl=CRAWL, limit=5, budget=3)

    report = await run(settings, ops_pool, spec)

    assert report.state is JobState.STOPPED
    assert report.stop_reason is not None and "budget" in report.stop_reason
    assert report.budget_spent == 3
    assert report.rows_written == 2  # 1 index lookup + 2 records = the whole budget
    data_requests = fake.count("/crawl-data/")
    assert data_requests == 2


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
    assert "3 distinct page(s) from 5 index row(s); 1 request(s)" in discovered.output

    demo = runner.invoke(
        app,
        ["demo", "--domain", "example.org", "--crawl", CRAWL, "--limit", "3", "--budget", "10"],
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


async def test_migrate_then_policy_lookup(pg_dsn: str) -> None:
    await migrate(pg_dsn)
    async with AsyncConnectionPool(pg_dsn, open=False) as pool:
        policy = await OpsStore(pool).policy("data.commoncrawl.org")
    assert policy is not None and policy.max_in_flight == 2
