"""Running a job with its pages fetched by the pool, and running a worker: what ``strata
scheduler`` and ``strata worker`` call."""

from __future__ import annotations

import os
import socket
from collections.abc import Callable
from dataclasses import replace

import grpc
import httpx
from prometheus_client import start_http_server

from strata_core import db
from strata_core.models import CrawlSpec, RunReport
from strata_core.settings import Settings
from strata_fetch.blobstore import open_store
from strata_fetch.jobs import Services, run_until_done
from strata_pool.dispatch import Dispatcher
from strata_pool.queue import PostgresWorkQueue
from strata_pool.service import add_to_server
from strata_pool.worker import Worker, WorkerStats


async def run_pooled(
    spec: CrawlSpec,
    svc: Services,
    *,
    listen: str,
    on_listening: Callable[[str], None] = lambda _: None,
) -> tuple[str, RunReport | None]:
    """The targets file's job (started, resumed or left alone, as ``run_until_done``) with its
    page requests leased to workers through a gRPC server on ``listen``. Returns once the job
    ends; connected workers are told there is nothing left and stop."""
    settings = svc.settings
    async with db.make_pool(settings.ops_dsn, max_size=4) as pool:
        dispatcher = Dispatcher(
            PostgresWorkQueue(pool),
            svc.ledger,
            svc.ops,
            data_base=settings.cc_data_base,
            lease_ttl_s=settings.lease_ttl_s,
            backoff=settings.backoff_s,
            version=settings.version,
            sleep=svc.sleep,
        )
        server = grpc.aio.server()
        add_to_server(server, dispatcher)
        port = server.add_insecure_port(listen)
        if port == 0:
            raise OSError(f"the scheduler cannot listen on {listen}")
        await server.start()
        on_listening(f"{listen.rsplit(':', 1)[0]}:{port}")
        try:
            return await run_until_done(spec, replace(svc, pool=dispatcher))
        finally:
            dispatcher.close()
            await server.stop(grace=5)


def serve_metrics(port: int, host: str = "127.0.0.1") -> None:
    """Prometheus metrics on http://host:port/metrics, from a background thread."""
    start_http_server(port, addr=host)


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


async def run_worker(
    settings: Settings, *, scheduler: str, worker_id: str, slots: int
) -> WorkerStats:
    """Fetch pages for the scheduler at ``scheduler`` until its job ends. The raw store and
    staging folder come from ``settings``, as for a local job."""
    async with (
        httpx.AsyncClient(
            headers={"User-Agent": settings.user_agent},
            timeout=settings.http_timeout_s,
            follow_redirects=False,
        ) as client,
        grpc.aio.insecure_channel(scheduler) as channel,
    ):
        worker = Worker(
            worker_id,
            slots=slots,
            client=client,
            staging_root=settings.raw_root,
            store=open_store(settings),
            version=settings.version,
        )
        return await worker.run(channel)
