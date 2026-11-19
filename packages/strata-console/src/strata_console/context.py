"""What the console process shares across requests, opened once at startup.

One HTTP client and one gate per Common Crawl host for the whole process: two browser tabs
profiling at once still send at most the policy's rate (ADR-003).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import duckdb
import httpx
from psycopg_pool import AsyncConnectionPool

from strata_console.metrics import Metrics

if TYPE_CHECKING:
    from strata_console.pulls import PullRuns
from strata_core import db
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_core.sinks import DuckDbSink
from strata_fetch.ratelimit import HostGate
from strata_services.runtime import data_gate, http_client, index_gate


@dataclass
class Console:
    settings: Settings
    client: httpx.AsyncClient
    data_gate: HostGate
    index_gate: HostGate
    pages: duckdb.DuckDBPyConnection  # strata_pages of every pull started here
    metrics: Metrics
    pool: AsyncConnectionPool | None = None  # the ops database, set once the console is open
    pulls: PullRuns = field(init=False)

    @property
    def ops(self) -> OpsStore | None:
        return None if self.pool is None else OpsStore(self.pool)


@asynccontextmanager
async def open_console(settings: Settings, metrics: Metrics) -> AsyncIterator[Console]:
    settings.console_db.parent.mkdir(parents=True, exist_ok=True)
    pages = duckdb.connect(str(settings.console_db))
    DuckDbSink(pages)  # creates strata_pages and strata_site_changes if missing
    async with http_client(settings) as client:
        console = Console(
            settings=settings,
            client=client,
            data_gate=data_gate(),
            index_gate=index_gate(),
            pages=pages,
            metrics=metrics,
        )
        from strata_console.pulls import PullRuns

        console.pulls = PullRuns(console)
        try:
            async with db.make_pool(settings.ops_dsn, max_size=4) as pool:
                console.pool = pool
                yield console
        finally:
            pages.close()
