"""The budget ledger and ops store against real Postgres."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from strata_core.db import migrate
from strata_core.ledger import JOB_TOTAL, PostgresLedger
from strata_core.models import Attempt, CrawlSpec, JobState, Outcome
from strata_core.ops import OpsStore

pytestmark = pytest.mark.postgres

HOST = "data.commoncrawl.org"


def _spec(budget: int = 10) -> CrawlSpec:
    return CrawlSpec(domain="example.org", crawl="CC-MAIN-2026-17", limit=50, budget=budget)


async def test_migrate_is_idempotent(pg_dsn: str) -> None:
    first = await migrate(pg_dsn)
    second = await migrate(pg_dsn)
    assert first == ["001_init", "002_seed_policies"]
    assert second == []


async def test_seeded_policies_follow_adr_003(ops_pool: AsyncConnectionPool) -> None:
    ops = OpsStore(ops_pool)
    data = await ops.policy("data.commoncrawl.org")
    index = await ops.policy("index.commoncrawl.org")
    assert data is not None and index is not None
    assert data.max_rps < 10  # Common Crawl's guidance for small requests
    assert index.max_in_flight == 1
    assert await ops.policy("unknown.example") is None


async def test_concurrent_reservations_never_exceed_the_cap(ops_pool: AsyncConnectionPool) -> None:
    ops, ledger = OpsStore(ops_pool), PostgresLedger(ops_pool)
    await ops.create_job("job_a", _spec(10))
    await ledger.open("job_a", total_cap=10, host_caps={HOST: 50})

    results = await asyncio.gather(*(ledger.reserve("job_a", HOST) for _ in range(50)))

    assert sum(results) == 10
    usage = await ledger.usage("job_a")
    assert usage[JOB_TOTAL] == (10, 10)
    assert usage[HOST] == (10, 50)


async def test_host_cap_binds_before_the_job_total(ops_pool: AsyncConnectionPool) -> None:
    ops, ledger = OpsStore(ops_pool), PostgresLedger(ops_pool)
    await ops.create_job("job_b", _spec(10))
    await ledger.open("job_b", total_cap=10, host_caps={HOST: 3, "index.commoncrawl.org": 10})

    assert [await ledger.reserve("job_b", HOST) for _ in range(5)] == [True] * 3 + [False] * 2
    assert await ledger.reserve("job_b", "index.commoncrawl.org") is True
    assert (await ledger.usage("job_b"))[JOB_TOTAL] == (4, 10)


async def test_unknown_host_or_job_is_refused(ops_pool: AsyncConnectionPool) -> None:
    ops, ledger = OpsStore(ops_pool), PostgresLedger(ops_pool)
    await ops.create_job("job_c", _spec())
    await ledger.open("job_c", total_cap=5, host_caps={HOST: 5})
    assert await ledger.reserve("job_c", "elsewhere.example") is False
    assert await ledger.reserve("no_such_job", HOST) is False


async def test_a_crash_after_reserving_counts_the_request_as_spent(
    pg_dsn: str, ops_pool: AsyncConnectionPool
) -> None:
    """Conservative by design: the spend commits before the request, so a crash can't refund it."""
    ops, ledger = OpsStore(ops_pool), PostgresLedger(ops_pool)
    await ops.create_job("job_d", _spec())
    await ledger.open("job_d", total_cap=5, host_caps={HOST: 5})

    async def fetch_that_crashes() -> None:
        assert await ledger.reserve("job_d", HOST)
        raise RuntimeError("process died mid-request")

    with pytest.raises(RuntimeError):
        await fetch_that_crashes()

    # A brand-new connection (as after a restart) sees the spend.
    async with await psycopg.AsyncConnection.connect(pg_dsn) as conn:
        cur = await conn.execute(
            "SELECT spent FROM ops.budget_ledger WHERE job_id = 'job_d' AND host = %s", (HOST,)
        )
        assert await cur.fetchone() == (1,)


async def test_database_refuses_overspend_even_without_the_ledger(
    ops_pool: AsyncConnectionPool,
) -> None:
    ops, ledger = OpsStore(ops_pool), PostgresLedger(ops_pool)
    await ops.create_job("job_e", _spec())
    await ledger.open("job_e", total_cap=1, host_caps={HOST: 1})
    async with ops_pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute("UPDATE ops.budget_ledger SET spent = 2 WHERE job_id = 'job_e'")


async def test_job_state_and_attempt_log(ops_pool: AsyncConnectionPool) -> None:
    ops = OpsStore(ops_pool)
    await ops.create_job("job_f", _spec())
    await ops.set_state("job_f", JobState.STOPPED, "budget exhausted")
    assert await ops.job_state("job_f") == (JobState.STOPPED, "budget exhausted")

    now = datetime.now(UTC)
    for n, outcome in enumerate([Outcome.RATE_LIMITED, Outcome.OK], start=1):
        await ops.record(
            Attempt(job_id="job_f", host=HOST, url="u", attempt=n, started_at=now, outcome=outcome)
        )
    counts = await ops.attempt_outcomes("job_f")
    assert counts == {Outcome.RATE_LIMITED: 1, Outcome.OK: 1}
