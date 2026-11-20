"""The fetch pool end to end: a real gRPC scheduler, workers, Postgres and a fake Common Crawl.

S6's acceptance test is here: a worker killed mid-job has its page leased again, a late result
from it is refused, and the job ends with every page stored exactly once.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import grpc
import httpx
import pytest
from psycopg_pool import AsyncConnectionPool
from pytest_httpserver import HTTPServer

from strata_core.lake import Lake
from strata_core.ledger import JOB_TOTAL, PostgresLedger
from strata_core.models import CrawlSpec, JobState, RunReport, Target
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_fetch.blobstore import LocalBlobStore
from strata_fetch.jobs import Services, run_until_done
from strata_fetch.lineage import lineage
from strata_pool.dispatch import Dispatcher
from strata_pool.proto import pool_pb2 as pb
from strata_pool.queue import PostgresWorkQueue
from strata_pool.service import add_to_server
from strata_pool.wire import WORK_METHOD
from strata_pool.worker import Worker
from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc
from tests.integration.test_end_to_end import allow_fast_localhost, make_settings

pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("httpfs")]

PAGES = [(f"https://example.org/p{i}", f"<html>page {i}</html>".encode() * 30) for i in range(5)]
SPEC = CrawlSpec(targets=[Target(site="example.org", pages=5)], crawl=CRAWL, budget=40)


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    cc.add_warc(warc)
    cc.cdx_refs[CRAWL] = warc.refs
    cc.add_robots("example.org")
    cc.install()
    return cc


@dataclass
class Running:
    address: str
    job: asyncio.Task[tuple[str, RunReport | None]]
    settings: Settings
    client: httpx.AsyncClient


@pytest.fixture
async def scheduler(
    fake: FakeCommonCrawl, ops_pool: AsyncConnectionPool, pg_dsn: str, tmp_path: Path
) -> AsyncIterator[Callable[[float], Awaitable[Running]]]:
    """Start the job behind a gRPC scheduler on a free port; the test drives the workers."""
    await allow_fast_localhost(ops_pool)
    servers: list[grpc.aio.Server] = []

    async def start(lease_ttl_s: float = 30.0) -> Running:
        settings = make_settings(pg_dsn, fake, tmp_path).model_copy(
            update={"lease_ttl_s": lease_ttl_s}
        )
        lake = Lake.open(settings)
        lake.bootstrap()
        client = httpx.AsyncClient()
        svc = Services(
            settings=settings,
            ops=OpsStore(ops_pool),
            ledger=PostgresLedger(ops_pool),
            lake=lake,
            client=client,
            store=LocalBlobStore(settings.raw_root),
        )
        dispatcher = Dispatcher(
            PostgresWorkQueue(ops_pool),
            svc.ledger,
            svc.ops,
            data_base=settings.cc_data_base,
            lease_ttl_s=lease_ttl_s,
            backoff=settings.backoff_s,
            version=settings.version,
        )
        server = grpc.aio.server()
        add_to_server(server, dispatcher)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)

        async def job() -> tuple[str, RunReport | None]:
            try:
                return await run_until_done(SPEC, replace(svc, pool=dispatcher))
            finally:
                dispatcher.close()
                lake.close()

        return Running(f"127.0.0.1:{port}", asyncio.create_task(job()), settings, client)

    yield start
    for server in servers:
        await server.stop(None)


def open_stream(
    channel: grpc.aio.Channel,
) -> grpc.aio.StreamStreamCall[pb.WorkerMessage, pb.SchedulerMessage]:
    return channel.stream_stream(
        WORK_METHOD,
        request_serializer=pb.WorkerMessage.SerializeToString,
        response_deserializer=pb.SchedulerMessage.FromString,
    )(wait_for_ready=True)


async def next_lease(
    call: grpc.aio.StreamStreamCall[pb.WorkerMessage, pb.SchedulerMessage],
) -> pb.Lease:
    msg = await asyncio.wait_for(call.read(), 30)
    assert isinstance(msg, pb.SchedulerMessage) and msg.HasField("lease"), msg
    return msg.lease


async def until_pending(pool: AsyncConnectionPool, url: str) -> None:
    async with asyncio.timeout(30):
        while True:
            async with pool.connection() as conn:
                cur = await conn.execute("SELECT state FROM ops.work_items WHERE url = %s", (url,))
                row = await cur.fetchone()
            if row is not None and row[0] == "pending":
                return
            await asyncio.sleep(0.05)


def worker(run: Running, name: str) -> Worker:
    return Worker(
        name,
        slots=2,
        client=run.client,
        staging_root=run.settings.raw_root,
        store=LocalBlobStore(run.settings.raw_root),
        version=run.settings.version,
    )


async def test_a_killed_worker_is_re_leased_and_its_late_result_is_refused(
    scheduler: Callable[[float], Awaitable[Running]], ops_pool: AsyncConnectionPool
) -> None:
    run = await scheduler(30.0)
    async with grpc.aio.insecure_channel(run.address) as channel:
        # A worker takes the first page, then dies before answering (its stream is cut).
        doomed = open_stream(channel)
        await doomed.write(pb.WorkerMessage(hello=pb.Hello(worker_id="doomed", slots=1)))
        first = await next_lease(doomed)
        assert first.attempt == 1
        doomed.cancel()
        await until_pending(ops_pool, first.page.url)  # the server notices the cut stream

        # The page goes back to the queue at once: the next worker is leased it again, under a
        # new lease id. A result for the dead worker's lease, arriving late, is refused.
        late = open_stream(channel)
        await late.write(pb.WorkerMessage(hello=pb.Hello(worker_id="late", slots=1)))
        again = await next_lease(late)
        assert (again.page.url, again.attempt) == (first.page.url, 2)
        assert again.lease_id != first.lease_id
        stale = pb.Result(
            lease_id=first.lease_id,
            outcome=pb.OUTCOME_OK,
            started_at=datetime.now(UTC).isoformat(),
            stored=pb.Stored(
                raw_uri="file:///nowhere", raw_offset=0, raw_length=1, raw_sha256="0" * 64
            ),
        )
        await late.write(pb.WorkerMessage(result=stale))
        ack = await asyncio.wait_for(late.read(), 30)
        assert isinstance(ack, pb.SchedulerMessage)
        assert ack.ack.lease_id == first.lease_id and ack.ack.accepted is False
        late.cancel()  # and this one dies holding its lease too

        # A healthy worker does the whole job, the twice-orphaned page included.
        stats = await asyncio.wait_for(worker(run, "healthy").run(channel), 60)

    job_id, report = await asyncio.wait_for(run.job, 60)
    assert report is not None
    assert report.state is JobState.SUCCEEDED, report.stop_reason
    assert report.rows_written == 5
    assert stats.leases == 5 and stats.rejected == 0

    # No duplicates: five rows, five distinct captures, each tracing back to intact bytes.
    with Lake.open(run.settings, read_only=True) as lake:
        ids = lake.capture_ids(job_id)
        assert len(ids) == len(set(ids)) == 5
        assert lake.count() == 5
        for cid in ids:
            rep = lineage(
                cid,
                lake,
                LocalBlobStore(run.settings.raw_root),
                data_base=run.settings.cc_data_base,
            )
            assert rep is not None and rep.raw_bytes_intact and rep.recompute_matches_header

    async with ops_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT state, count(*) FROM ops.work_items WHERE job_id = %s GROUP BY state", (job_id,)
        )
        assert dict(await cur.fetchall()) == {"stored": 5}
        cur = await conn.execute(
            "SELECT attempt, outcome, error FROM ops.fetch_attempts"
            " WHERE job_id = %s AND url = %s ORDER BY attempt",
            (job_id, first.page.url),
        )
        tries = await cur.fetchall()
    assert [(a, o) for a, o, _ in tries] == [
        (1, "retryable_error"),
        (2, "retryable_error"),
        (3, "ok"),
    ]
    assert "doomed disconnected" in tries[0][2] and "late disconnected" in tries[1][2]

    # The two lost requests stay charged: the ledger matches the attempt log exactly.
    usage = await PostgresLedger(ops_pool).usage(job_id)
    charged = await OpsStore(ops_pool).charged_attempts(job_id)
    assert usage[JOB_TOTAL][0] == sum(charged.values())


async def test_a_lease_nobody_answers_expires_and_goes_to_another_worker(
    scheduler: Callable[[float], Awaitable[Running]], ops_pool: AsyncConnectionPool
) -> None:
    run = await scheduler(0.5)
    async with grpc.aio.insecure_channel(run.address) as channel:
        hung = open_stream(channel)  # connected, holding a lease, never answering
        await hung.write(pb.WorkerMessage(hello=pb.Hello(worker_id="hung", slots=1)))
        held = await next_lease(hung)
        stats = await asyncio.wait_for(worker(run, "healthy").run(channel), 60)
        hung.cancel()

    job_id, report = await asyncio.wait_for(run.job, 60)
    assert report is not None and report.state is JobState.SUCCEEDED, report
    assert report.rows_written == 5 and stats.leases == 5
    async with ops_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT error FROM ops.fetch_attempts WHERE job_id = %s AND url = %s AND attempt = 1",
            (job_id, held.page.url),
        )
        row = await cur.fetchone()
    assert row is not None and "lease expired: worker hung did not answer" in row[0]


async def test_three_workers_share_one_job(
    scheduler: Callable[[float], Awaitable[Running]],
) -> None:
    run = await scheduler(30.0)
    async with grpc.aio.insecure_channel(run.address) as channel:
        results = await asyncio.wait_for(
            asyncio.gather(*(worker(run, f"w{i}").run(channel) for i in range(3))), 60
        )
    _, report = await asyncio.wait_for(run.job, 60)
    assert report is not None and report.state is JobState.SUCCEEDED
    assert report.rows_written == 5
    assert sum(s.leases for s in results) == 5  # each page leased once: nobody died
