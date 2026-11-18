"""The Parquet URL index (formerly the "columnar index"), queried with DuckDB (ADR-004).

With a shard map it opens only the files, of the 300 page-capture files per crawl, whose SURT
range covers the domain (one, for python.org in CC-MAIN-2026-43).
"""

from __future__ import annotations

import asyncio
import gzip
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin

import httpx
from pydantic import BaseModel, ConfigDict

from strata_core import logs
from strata_core.models import RequestClient
from strata_fetch.cc.attempts import (
    IndexAttempt,
    IndexLookupError,
    IndexUnavailableError,
    OnAttempt,
    server_message,
)
from strata_fetch.cc.catalog import (
    CatalogStore,
    PageCount,
    SiteCatalog,
    choose,
    site_sql,
    summarise_rows,
)
from strata_fetch.cc.duckdb_query import (
    CostEstimate,
    DuckDbQueryError,
    DuckDbUsage,
    check_pattern,
    run_logged_query,
)
from strata_fetch.cc.index import DiscoveryResult
from strata_fetch.ratelimit import HostGate
from strata_fetch.surt import host_forms, normalise_domain, surt_range

log = logs.get_logger(__name__)


class ShardSelection(BaseModel):
    """The URL-index files that may hold a domain, and what finding them cost."""

    model_config = ConfigDict(frozen=True)

    files: list[str]
    attempts: list[IndexAttempt] = []
    footers_read: int = 0
    estimate: CostEstimate | None = None  # None when the map has no row-group statistics


class ShardSelector(Protocol):
    async def select(self, crawl: str, files: list[str], lo: str, hi: str) -> ShardSelection: ...


# ------------------------------------------------------------------------------ columnar index
class ShardResolver(Protocol):
    async def shards(self, crawl: str) -> list[str]: ...


class StaticShards:
    """Explicit shard paths or URLs: local fixtures, or a list prepared elsewhere."""

    def __init__(self, paths: Sequence[str]) -> None:
        self._paths = list(paths)

    async def shards(self, crawl: str) -> list[str]:
        return [p for p in self._paths if f"crawl={crawl}/" in p.replace("\\", "/")]


class ManifestShards:
    """Resolve shard URLs from a crawl's ``cc-index-table.paths.gz`` manifest, because the
    Common Crawl bucket can't be listed anonymously. The manifest is cached on disk."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        *,
        data_base: str = "https://data.commoncrawl.org/",
        cache_dir: Path | None = None,
        subset: str = "warc",
    ) -> None:
        self._client = client
        self._gate = gate
        self._base = data_base if data_base.endswith("/") else data_base + "/"
        self._cache_dir = cache_dir
        self._subset = subset

    async def shards(self, crawl: str) -> list[str]:
        cache = self._cache_dir / "manifests" / f"{crawl}.paths" if self._cache_dir else None
        if cache is not None and cache.exists():
            text = cache.read_text(encoding="utf-8")
        else:
            url = urljoin(self._base, f"crawl-data/{crawl}/cc-index-table.paths.gz")
            async with self._gate.slot():
                resp = await self._client.get(url)
            if resp.status_code != 200:
                raise IndexUnavailableError(
                    f"{url}: HTTP {resp.status_code}: {server_message(resp.text)}"
                )
            text = gzip.decompress(resp.content).decode("utf-8")
            if cache is not None:
                cache.parent.mkdir(parents=True, exist_ok=True)
                cache.write_text(text, encoding="utf-8")
        return [urljoin(self._base, p) for p in text.split() if f"subset={self._subset}/" in p]


class QueryBudget(Protocol):
    """How a job pays for DuckDB's requests, which DuckDB can't ask about one by one."""

    async def reserve(self, n: int) -> bool:
        """Reserve ``n`` requests before the query; False if the budget can't cover them."""
        ...

    async def settle(self, reserved: int, actual: int) -> int:
        """After the query: charge what went beyond the reservation or refund what was left.
        Returns how many requests the budget could not cover (above zero means stop)."""
        ...


class IndexBudgetError(IndexLookupError):
    """The job's budget can't pay for a URL-index query (before it runs, or after)."""


class UrlIndexQueries:
    """DuckDB queries over URL-index files, paid for like any other request: the estimate is
    reserved first, every request DuckDB sends is logged and reported, and the job is charged
    or refunded the difference afterwards."""

    def __init__(
        self,
        shards: ShardResolver,
        *,
        shard_map: ShardSelector | None = None,
        gate: HostGate | None = None,
        threads: int = 1,
        user_agent: str | None = None,
        on_attempt: OnAttempt | None = None,
        budget: QueryBudget | None = None,
    ) -> None:
        # threads (with the HTTP client pool sized to match) caps how many range requests DuckDB
        # keeps in flight; the gate, when given, keeps a second query from starting meanwhile.
        self._shards = shards
        self._shard_map = shard_map
        self._gate = gate
        self._threads = threads
        self._user_agent = user_agent
        self._on_attempt = on_attempt
        self._budget = budget

    async def _files(
        self, crawl: str, lo: str, hi: str
    ) -> tuple[list[str], int, list[IndexAttempt], int, CostEstimate | None]:
        """Files that may hold keys in [lo, hi): (files, files in the crawl, footer requests,
        footers read, cost estimate)."""
        files = await self._shards.shards(crawl)
        total = len(files)
        if not files or self._shard_map is None:
            return files, total, [], 0, None
        selection = await self._shard_map.select(crawl, files, lo, hi)
        return (
            selection.files,
            total,
            list(selection.attempts),
            selection.footers_read,
            selection.estimate,
        )

    async def _paid_query(
        self,
        sql: str,
        params: list[Any],
        estimate: CostEstimate | None,
        attempts: list[IndexAttempt],
    ) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
        """Reserve, run, settle. Returns rows, every attempt so far (earlier ones first) and the
        DuckDB usage of this query."""
        reserved = await self._reserve(estimate, attempts)
        try:
            rows, duck, usage = await self._run(sql, params, attempts)
        except IndexUnavailableError as exc:
            await self._settle(reserved, exc.attempts, exc.attempts)
            raise
        attempts = [*attempts, *duck]
        await self._settle(reserved, duck, attempts)
        return rows, attempts, usage

    async def _reserve(self, estimate: CostEstimate | None, attempts: list[IndexAttempt]) -> int:
        if self._budget is None:
            return 0
        want = estimate.requests if estimate is not None else 0
        if want and not await self._budget.reserve(want):
            raise IndexBudgetError(
                f"the URL-index query should take about {want} request(s)"
                f" ({estimate.bytes / 1e6 if estimate else 0:.1f} MB); the budget can't cover"
                " them, so it was not run",
                attempts,
            )
        return want

    async def _settle(
        self, reserved: int, duck: Sequence[IndexAttempt], attempts: list[IndexAttempt]
    ) -> None:
        if self._budget is None:
            return
        sent = sum(1 for a in duck if a.client is RequestClient.DUCKDB)
        over = await self._budget.settle(reserved, sent)
        if over > 0:
            raise IndexBudgetError(
                f"the URL-index query made {sent} request(s), {over} more than the budget had"
                " left; nothing was fetched",
                attempts,
            )

    async def _run(
        self, sql: str, params: list[Any], earlier: list[IndexAttempt]
    ) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
        """Run one query in a worker thread, then report DuckDB's requests even if it failed."""
        try:
            if self._gate is None:
                rows, duck, usage = await asyncio.to_thread(self._query, sql, params)
            else:
                async with self._gate.slot():
                    rows, duck, usage = await asyncio.to_thread(self._query, sql, params)
        except DuckDbQueryError as exc:
            await self._report(exc.attempts)
            raise IndexUnavailableError(
                f"URL index query failed: {exc.error}", [*earlier, *exc.attempts]
            ) from exc.error
        await self._report(duck)
        log.info(
            "index.query",
            requests=usage.requests,
            bytes=usage.bytes,
            files=usage.files,
            failed=usage.failed,
            query_s=usage.query_s,
            rows=len(rows),
        )
        return rows, duck, usage

    async def _report(self, attempts: list[IndexAttempt]) -> None:
        if self._on_attempt is not None:
            for attempt in attempts:
                await self._on_attempt(attempt)

    def _query(
        self, sql: str, params: list[Any]
    ) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
        return run_logged_query(sql, params, threads=self._threads, user_agent=self._user_agent)


class ColumnarIndex(UrlIndexQueries):
    source = "columnar"

    def __init__(
        self,
        shards: ShardResolver,
        *,
        shard_map: ShardSelector | None = None,
        cache_dir: Path | None = None,
        gate: HostGate | None = None,
        threads: int = 1,
        include_query_strings: bool = False,
        user_agent: str | None = None,
        on_attempt: OnAttempt | None = None,
        budget: QueryBudget | None = None,
    ) -> None:
        super().__init__(
            shards,
            shard_map=shard_map,
            gate=gate,
            threads=threads,
            user_agent=user_agent,
            on_attempt=on_attempt,
            budget=budget,
        )
        self._catalogs = CatalogStore(cache_dir)
        self._include_query = include_query_strings

    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult:
        site = normalise_domain(domain)
        if pattern is not None:
            check_pattern(pattern)
        t0 = time.monotonic()
        catalog, attempts, footers, cached = await self.catalog(site, crawl)
        matching = choose(catalog.refs, pattern=pattern, include_query_strings=self._include_query)
        return DiscoveryResult(
            refs=matching[:limit],
            from_cache=cached,
            requests=len(attempts),
            source=self.source,
            rows_scanned=catalog.summary.rows,
            attempts=attempts,
            elapsed_s=round(time.monotonic() - t0, 3),
            shards_total=catalog.shards_total,
            shards_queried=catalog.shards_queried,
            footers_read=footers,
            duckdb=None if cached else catalog.duckdb,
            candidates=len(matching),
        )

    async def count(self, site: str, crawl: str, pattern: str, *, sample: int = 5) -> PageCount:
        """Count a site's eligible pages, and those whose URL matches ``pattern`` in full."""
        site = normalise_domain(site)
        check_pattern(pattern)
        t0 = time.monotonic()
        catalog, attempts, footers, cached = await self.catalog(site, crawl)
        matching = choose(catalog.refs, pattern=pattern, include_query_strings=True)
        return PageCount(
            crawl=crawl,
            site=site,
            pattern=pattern,
            captures=catalog.summary.eligible,
            urls=catalog.summary.urls,
            matching=len(matching),
            sample=[r.url for r in matching[:sample]],
            shards_total=catalog.shards_total,
            shards_queried=catalog.shards_queried,
            footers_read=footers,
            summary=catalog.summary,
            from_cache=cached,
            estimate=catalog.estimate,
            attempts=attempts,
            duckdb=DuckDbUsage() if cached or catalog.duckdb is None else catalog.duckdb,
            elapsed_s=round(time.monotonic() - t0, 3),
        )

    async def catalog(
        self, site: str, crawl: str
    ) -> tuple[SiteCatalog, list[IndexAttempt], int, bool]:
        """The site's catalog (cached or built) and the requests made: (catalog, attempts,
        footers read, from cache)."""
        cached = self._catalogs.read(crawl, site)
        if cached is not None:
            return cached, [], 0, True
        lo, hi = surt_range(site)
        files, total, attempts, footers, estimate = await self._files(crawl, lo, hi)
        rows: list[tuple[Any, ...]] = []
        usage: DuckDbUsage | None = None
        if files:
            sql, host_params = site_sql(host_forms(site))
            rows, attempts, usage = await self._paid_query(
                sql, [files, crawl, lo, hi, *host_params], estimate, attempts
            )
        summary, refs = summarise_rows(rows)
        catalog = SiteCatalog(
            crawl=crawl,
            site=site,
            built_at=datetime.now(UTC),
            summary=summary,
            refs=refs,
            shards_total=total,
            shards_queried=len(files),
            estimate=estimate,
            duckdb=usage,
        )
        if total:
            self._catalogs.write(catalog)
        return catalog, attempts, footers, False
