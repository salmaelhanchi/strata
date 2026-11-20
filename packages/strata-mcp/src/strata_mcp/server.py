"""Strata's MCP server: the same services as the CLI and the console, as tools for AI agents.

Read-only tools answer from Common Crawl's index and fetch no page of the site. ``pull_pages``
runs a job (budget, robots.txt, verified records, resumable) and keeps the pages in the same
store as the console. Every answer says what it cost in requests.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import duckdb
import httpx
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from strata_core.lake import Lake
from strata_core.models import CrawlSpec, IndexChoice, Target
from strata_core.settings import Settings
from strata_core.sinks import DuckDbSink, describe, open_sink
from strata_fetch.blobstore import open_store
from strata_fetch.cc.attempts import IndexBlockedError, IndexQueryError, IndexUnavailableError
from strata_fetch.cc.columnar import IndexBudgetError
from strata_fetch.jobs import ResumeRefusedError
from strata_fetch.lineage import lineage
from strata_fetch.ratelimit import HostGate
from strata_fetch.surt import normalise_domain
from strata_mcp.digests import (
    DiffDigest,
    ProfileDigest,
    PullDigest,
    PulledPage,
    RatesDigest,
    TraceDigest,
    diff_digest,
    profile_digest,
    pull_digest,
    rates_digest,
    trace_digest,
)
from strata_services.budget import RequestBudget
from strata_services.runtime import data_gate, engine, http_client, newest_crawls
from strata_services.wiring import change_service, pull_service, site_profiler

INSTRUCTIONS = """\
Strata answers questions about a website from Common Crawl, a public web archive, before anyone
scrapes it. Start with profile_site (what the site holds, its URL shapes, what robots.txt
allows), then diff_site or change_rates (what changes between crawls, so what is worth
re-scraping). These three read only Common Crawl's index and fetch no page. pull_pages fetches
chosen pages from the archive (never from the site itself) within a request budget, and
trace_row proves where any pulled row came from. A site is a host or domain such as
peps.python.org; it includes its subdomains. URL patterns are RE2 regular expressions that must
match the whole URL."""

READ_ONLY = ToolAnnotations(read_only_hint=True, idempotent_hint=True, open_world_hint=True)
_UPSTREAM = (IndexBlockedError, IndexQueryError, IndexUnavailableError, IndexBudgetError)


def build_server(settings: Settings | None = None) -> MCPServer:
    settings = settings or Settings()
    gate: HostGate | None = None  # one gate for the process, made inside its event loop

    def shared_gate() -> HostGate:
        nonlocal gate
        if gate is None:
            gate = data_gate()
        return gate

    @asynccontextmanager
    async def client() -> AsyncIterator[httpx.AsyncClient]:
        async with http_client(settings) as c:
            yield c

    async def guarded[T](body: Callable[[], Awaitable[T]]) -> T:
        try:
            return await body()
        except IndexBudgetError as exc:
            raise ToolError(f"{exc}. Call again with a larger budget.") from exc
        except _UPSTREAM as exc:
            raise ToolError(str(exc)) from exc

    server = MCPServer(
        name="strata",
        title="Strata: a Common Crawl toolkit",
        instructions=INSTRUCTIONS,
        version=settings.version,
        log_level="WARNING",  # the SDK logs every HTTP request at INFO
    )

    @server.tool(annotations=READ_ONLY)
    async def profile_site(site: str, crawls: int = 1, budget: int = 400) -> ProfileDigest:
        """What a site holds in Common Crawl: pages per crawl (the newest `crawls` crawls), its
        largest URL shapes and what robots.txt allows of each, HTTP statuses, languages, hosts,
        and the robots.txt rules that apply. Reads the index only; no page is fetched. The first
        look at a crawl takes a minute or two; later ones come from a cache."""

        async def run() -> ProfileDigest:
            async with client() as c:
                ids = await newest_crawls(settings, c, max(1, min(crawls, 6)))
                spend = RequestBudget(budget)
                p = await site_profiler(settings, c, shared_gate(), spend).profile(site, ids)
                return profile_digest(p)

        return await guarded(run)

    @server.tool(annotations=READ_ONLY)
    async def diff_site(
        site: str, from_crawl: str | None = None, to_crawl: str | None = None, budget: int = 400
    ) -> DiffDigest:
        """Pages added, removed and changed between two crawls (by default the two newest), from
        the payload digests Common Crawl's index records; with counts per URL shape and example
        URLs of each kind. No page is fetched."""

        async def run() -> DiffDigest:
            async with client() as c:
                newest = await newest_crawls(settings, c, 2)
                b = to_crawl or newest[0]
                a = from_crawl or next(x for x in newest if x != b)
                service = change_service(settings, c, shared_gate(), RequestBudget(budget))
                return diff_digest(await service.diff(site, a, b))

        return await guarded(run)

    @server.tool(annotations=READ_ONLY)
    async def change_rates(site: str, crawls: int = 3, budget: int = 400) -> RatesDigest:
        """How often each URL shape changed between consecutive crawls over the newest `crawls`
        crawls: the shapes that change are worth re-scraping, the stable ones once."""

        async def run() -> RatesDigest:
            async with client() as c:
                ids = list(reversed(await newest_crawls(settings, c, max(2, min(crawls, 6)))))
                service = change_service(settings, c, shared_gate(), RequestBudget(budget))
                return rates_digest(await service.rates(site, ids))

        return await guarded(run)

    @server.tool(
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=True, open_world_hint=True
        )
    )
    async def pull_pages(
        site: str,
        ctx: Context,
        pattern: str | None = None,
        limit: int = 10,
        crawl: str = "latest",
        budget: int = 150,
        into: str | None = None,
    ) -> PullDigest:
        """Fetch up to `limit` pages of a site from Common Crawl's archive (never from the site)
        as a job: the budget is spent before each request, robots.txt from the same crawl decides
        first, every record is verified. Returns each page's title, an excerpt and a capture_id
        for trace_row. Rows go to `into` (postgresql://, sqlite:///, duckdb:///, parquet:///)
        or, by default, to Strata's own page store. Calling again with the same arguments
        continues an unfinished pull and fetches nothing twice."""
        spec = CrawlSpec(
            crawl=crawl,
            budget=max(1, min(budget, 5000)),
            targets=(
                Target(site=normalise_domain(site), pattern=pattern, pages=max(1, min(limit, 200))),
            ),
            index=IndexChoice.COLUMNAR,
        )
        wanted = spec.targets[0].pages
        await ctx.report_progress(
            0, wanted, f"pulling up to {wanted} page(s) of {spec.targets[0].site}"
        )

        async def run() -> PullDigest:
            settings.console_db.parent.mkdir(parents=True, exist_ok=True)
            sink = open_sink(into) if into else DuckDbSink.open(settings.console_db)
            try:
                async with engine(settings) as svc:
                    report = await pull_service(svc).pull(
                        spec, sink, sink_name=describe(into) if into else "strata page store"
                    )
                pages = await asyncio.to_thread(_pulled, sink, report.job_id)
            finally:
                sink.close()
            await ctx.report_progress(report.written, wanted, f"{report.written} page(s) stored")
            return pull_digest(report, pages)

        try:
            return await guarded(run)
        except ResumeRefusedError as exc:
            raise ToolError(str(exc)) from exc
        except OSError as exc:  # psycopg's connection errors are OSError subclasses too
            raise ToolError(
                f"the pull needs Strata's ops database (STRATA_OPS_DSN): {exc}"
            ) from exc

    @server.tool(annotations=READ_ONLY)
    async def trace_row(capture_id: str) -> TraceDigest:
        """Where a pulled row came from: Common Crawl's WARC file and byte range, Strata's stored
        copy, and the checks that tie them (bytes re-hashed, both digests compared, the
        robots.txt verdict). `capture_id` comes from pull_pages or a strata_pages table."""

        def run() -> TraceDigest:
            try:
                lake = Lake.open(settings, read_only=True)
            except duckdb.Error as exc:
                raise ToolError(
                    f"no capture {capture_id}: the lake can't be opened (nothing pulled yet?)"
                ) from exc
            with lake:
                found = lineage(
                    capture_id, lake, open_store(settings), data_base=settings.cc_data_base
                )
            if found is None:
                raise ToolError(f"no capture {capture_id}")
            return trace_digest(found)

        return await asyncio.to_thread(run)

    return server


def _pulled(sink: Any, job_id: str) -> list[PulledPage]:
    """The pulled rows, read back when they went to a DuckDB store (the default); for the
    user's own database the digest gives counts, and the rows are in its strata_pages table."""
    con = getattr(sink, "con", None)
    if con is None:
        return []
    rows = con.execute(
        "SELECT url, title, coalesce(length(main_text), 0), capture_id, robots_verdict,"
        " coalesce(left(main_text, 280), '') FROM strata_pages WHERE job_id = ? ORDER BY url",
        [job_id],
    ).fetchall()
    return [
        PulledPage(
            url=r[0], title=r[1], chars=r[2], capture_id=r[3], robots_verdict=r[4], excerpt=r[5]
        )
        for r in rows
    ]
