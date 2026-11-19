"""What every interface needs to run a service: an event loop, an HTTP client, host gates, and the
engine (ops database, ledger, lake, raw store) opened and closed around a body of work."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

import httpx

from strata_core import db
from strata_core.lake import Lake
from strata_core.ledger import PostgresLedger
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_fetch.blobstore import open_store
from strata_fetch.cc.crawls import list_crawls, resolve_crawl
from strata_fetch.jobs import Services
from strata_fetch.ratelimit import HostGate
from strata_fetch.setup import prepare


def run_sync[T](coro: Coroutine[Any, Any, T]) -> T:
    # psycopg's async driver needs a selector event loop; Windows defaults to the proactor loop.
    if sys.platform == "win32":
        return asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(coro)


def http_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.http_timeout_s,
        follow_redirects=False,
    )


def index_gate() -> HostGate:
    # Matches the seeded policy for index.commoncrawl.org (ADR-003), for commands that run without
    # the ops database.
    return HostGate(rate=0.5, burst=1, max_in_flight=1)


def data_gate() -> HostGate:
    # Matches the seeded policy for data.commoncrawl.org (ADR-003).
    return HostGate(rate=2.0, burst=2, max_in_flight=2)


async def resolve(settings: Settings, client: httpx.AsyncClient, crawl: str) -> str:
    """``latest`` -> the newest crawl id; anything else unchanged."""
    return await resolve_crawl(
        crawl, client, index_gate(), index_base=settings.cc_index_base, cache_dir=settings.cache_dir
    )


@asynccontextmanager
async def engine(
    settings: Settings, on_setup: Callable[[str], None] = lambda _: None
) -> AsyncIterator[Services]:
    """Set up what's missing (schema, catalog database, buckets, lake tables), then open the
    ops pool, the HTTP client, the lake and the raw store for the body's jobs."""
    for line in await prepare(settings):
        on_setup(line)
    async with db.make_pool(settings.ops_dsn) as pool, http_client(settings) as client:
        with Lake.open(settings) as lake:
            yield Services(
                settings=settings,
                ops=OpsStore(pool),
                ledger=PostgresLedger(pool),
                lake=lake,
                client=client,
                store=open_store(settings),
            )


async def newest_crawls(settings: Settings, client: httpx.AsyncClient, n: int) -> list[str]:
    """The ``n`` newest crawl ids, newest first (the crawl list is cached for a day)."""
    listed = await list_crawls(
        client, index_gate(), index_base=settings.cc_index_base, cache_dir=settings.cache_dir
    )
    return [str(c["id"]) for c in listed[:n]]
