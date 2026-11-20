"""The scheduler's rules without gRPC or Postgres: politeness and budget across workers, retries,
and rows a crashed run had already fetched."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from strata_core.ledger import BudgetExhaustedError
from strata_core.models import BlobRef, CaptureRef, Outcome, Policy
from strata_fetch.fetcher import FetchResult, capture_row
from strata_fetch.jobs import ref_capture_id
from strata_pool.dispatch import Dispatcher
from strata_pool.leases import Grant, Report, StoredRecord
from strata_pool.queue import WorkItem
from strata_pool.wire import grant_from_pb, grant_to_pb, report_from_pb, report_to_pb
from tests.fakes import MemoryAttemptLog, MemoryLedger, MemoryWorkQueue

JOB = "job_test"
HOST = "data.example"
REFS = [
    CaptureRef(
        crawl="CC-MAIN-2026-17",
        url=f"https://example.org/p{i}",
        warc_filename="crawl-data/a.warc.gz",
        warc_record_offset=i * 100,
        warc_record_length=50,
    )
    for i in range(4)
]
POLICY = Policy(host=HOST, max_rps=1000, burst=10, max_in_flight=2)


class VirtualClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


async def setup(budget: int = 20) -> tuple[Dispatcher, MemoryWorkQueue, MemoryLedger, VirtualClock]:
    queue, ledger, clock = MemoryWorkQueue(), MemoryLedger(), VirtualClock()
    await ledger.open(JOB, budget, {HOST: budget})
    dispatcher = Dispatcher(
        queue,
        ledger,
        MemoryAttemptLog(),
        data_base=f"https://{HOST}/",
        lease_ttl_s=30,
        backoff=(1.0, 2.0),
        version="test",
        clock=clock,
        sleep=clock.sleep,
    )
    return dispatcher, queue, ledger, clock


async def collect(results: AsyncIterator[FetchResult]) -> list[FetchResult]:
    return [r async for r in results]


def ok(grant: Grant) -> Report:
    stored = StoredRecord("file:///parts/part-00000.warc.gz", 0, 50, "ab" * 32, 200)
    return Report(grant.lease_id, Outcome.OK, datetime.now(UTC), 3, 206, None, 50, stored=stored)


async def lease(dispatcher: Dispatcher, worker: str = "w") -> Grant:
    grant = await asyncio.wait_for(dispatcher.lease(worker), 5)
    assert grant is not None
    return grant


async def test_workers_together_never_exceed_the_hosts_in_flight_cap() -> None:
    dispatcher, queue, _, _ = await setup()
    job = asyncio.create_task(collect(dispatcher.fetch(JOB, REFS, POLICY)))
    a, b = await lease(dispatcher, "w1"), await lease(dispatcher, "w2")
    third = asyncio.create_task(dispatcher.lease("w3"))
    await asyncio.sleep(0.2)
    assert not third.done()  # two leases out, and the policy allows two in flight

    assert await dispatcher.report("w1", ok(a))
    c = await asyncio.wait_for(third, 5)
    assert c is not None
    for grant in (b, c):
        assert await dispatcher.report("w", ok(grant))
    assert await dispatcher.report("w", ok(await lease(dispatcher)))
    assert await asyncio.wait_for(dispatcher.lease("w"), 5) is None  # nothing left

    results = await asyncio.wait_for(job, 5)
    assert sorted(r.ref.url for r in results) == sorted(r.url for r in REFS)
    assert all(r.capture is not None for r in results)
    assert (await queue.counts(JOB)) == {"fetched": 4}  # stored() comes from the job


async def test_no_lease_without_budget_and_the_job_stops_after_settling() -> None:
    dispatcher, queue, ledger, _ = await setup(budget=2)
    job = asyncio.create_task(collect(dispatcher.fetch(JOB, REFS, POLICY)))
    a, b = await lease(dispatcher), await lease(dispatcher)
    refused = asyncio.create_task(dispatcher.lease("w"))
    await asyncio.sleep(0.2)
    assert not refused.done()  # waits for the two outstanding leases to settle
    assert not job.done()
    for grant in (a, b):
        assert await dispatcher.report("w", ok(grant))
    assert await asyncio.wait_for(refused, 5) is None
    with pytest.raises(BudgetExhaustedError):
        await asyncio.wait_for(job, 5)
    assert (await ledger.usage(JOB))["*"] == (2, 2)
    assert (await queue.counts(JOB)) == {"fetched": 2, "pending": 2}  # left for a resume


async def test_a_rate_limited_answer_pauses_the_host_for_every_worker() -> None:
    dispatcher, _queue, _, clock = await setup()
    job = asyncio.create_task(collect(dispatcher.fetch(JOB, REFS[:1], POLICY)))
    first = await lease(dispatcher, "w1")
    limited = Report(first.lease_id, Outcome.RATE_LIMITED, datetime.now(UTC), 3, 503, 30.0)
    assert await dispatcher.report("w1", limited)
    again = await lease(dispatcher, "w2")  # another worker, same host: it waits too
    assert again.ref == first.ref and again.attempt == 2
    assert sum(clock.slept) >= 30.0  # Retry-After won over the 1 s backoff step
    assert await dispatcher.report("w2", ok(again))
    assert len(await asyncio.wait_for(job, 5)) == 1


async def test_a_page_that_keeps_failing_ends_failed_after_its_attempts() -> None:
    dispatcher, queue, _, _ = await setup()
    job = asyncio.create_task(collect(dispatcher.fetch(JOB, REFS[:1], POLICY)))
    for attempt in (1, 2, 3):  # backoff has two steps: three attempts in all
        grant = await lease(dispatcher)
        assert grant.attempt == attempt
        error = Report(grant.lease_id, Outcome.RETRYABLE_ERROR, datetime.now(UTC), 3, 500)
        assert await dispatcher.report("w", error)
    (result,) = await asyncio.wait_for(job, 5)
    assert (result.outcome, result.attempts) == (Outcome.RETRYABLE_ERROR, 3)
    assert (await queue.counts(JOB)) == {"failed": 1}


async def test_rows_fetched_before_a_crash_come_first_and_are_not_leased_again() -> None:
    dispatcher, queue, _, _ = await setup()
    done = REFS[0]
    await queue.enqueue(JOB, [WorkItem(0, ref_capture_id(done), HOST, done)])
    claim = await queue.claim(JOB, "earlier", 30)
    assert claim is not None
    row = capture_row(
        done, JOB, BlobRef(uri="file:///x", offset=0, length=50), "ab" * 32, 200, datetime.now(UTC)
    )
    assert await queue.fetched(claim.lease_id, row)

    results = dispatcher.fetch(JOB, REFS[:2], POLICY)
    first = await anext(results)
    assert first.capture == row and first.attempts == 0
    grant = await lease(dispatcher)
    assert grant.ref == REFS[1]  # the fetched page is not leased again
    assert await dispatcher.report("w", ok(grant))
    rest = [r async for r in results]
    assert [r.ref for r in rest] == [REFS[1]]


async def test_a_result_for_an_unknown_lease_changes_nothing() -> None:
    dispatcher, queue, _, _ = await setup()
    job = asyncio.create_task(collect(dispatcher.fetch(JOB, REFS[:1], POLICY)))
    grant = await lease(dispatcher)
    stale = Grant("not-a-lease", JOB, grant.ref, 1, 30, "https://data.example/")
    assert not await dispatcher.report("w", ok(stale))
    assert (await queue.counts(JOB)) == {"leased": 1}
    assert await dispatcher.report("w", ok(grant))
    await asyncio.wait_for(job, 5)


@pytest.mark.parametrize("full", [True, False])
def test_messages_survive_the_wire(full: bool) -> None:
    ref = REFS[1].model_copy(
        update=(
            {
                "url_surtkey": "org,example)/p1",
                "fetch_time": datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
                "content_digest": "ABC",
                "mime": "text/html",
                "status": 200,
            }
            if full
            else {}
        )
    )
    grant = Grant("l1", JOB, ref, 2, 30.0, "https://data.example/")
    assert grant_from_pb(grant_to_pb(grant).lease) == grant
    started = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    report = (
        ok(grant)
        if full
        else Report("l1", Outcome.DEFINITIVE_EMPTY, started, 5, 404, None, 0, "HTTP 404")
    )
    report = Report(**{**report.__dict__, "started_at": started})
    assert report_from_pb(report_to_pb(report).result) == report
