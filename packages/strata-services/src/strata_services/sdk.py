"""The Python SDK: the same services as the CLI, as plain synchronous calls.

    from strata_services import Strata

    strata = Strata()
    profile = strata.profile("peps.python.org")
    pages = strata.pages("peps.python.org", pattern=r"^https://peps\\.python\\.org/pep-0\\d{3}/$",
                         limit=20)
    for page in pages:
        print(page.url, page.title)
    con = pages.to_duckdb()          # a strata_pages table in an in-memory DuckDB
    frame = pages.to_pandas()        # needs pandas installed

``profile`` and ``diff`` need no database. ``pages`` runs a pull job, so it needs Strata's ops
Postgres (``STRATA_OPS_DSN``) like the CLI; every page it returns was verified against Common
Crawl's index and carries its lineage.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any, overload

import duckdb

from strata_core.models import CrawlSpec, IndexChoice, Target
from strata_core.settings import Settings
from strata_core.sinks import ChangeRow, DuckDbSink, PageRow
from strata_fetch.surt import normalise_domain
from strata_services.budget import RequestBudget
from strata_services.changes import SiteDiff
from strata_services.profile import SiteProfile
from strata_services.pull import PullReport
from strata_services.runtime import data_gate, engine, http_client, newest_crawls, run_sync
from strata_services.wiring import change_service, pull_service, site_profiler


class _MemorySink:
    def __init__(self) -> None:
        self.pages: dict[tuple[str, str], PageRow] = {}

    def write_pages(self, pages: Sequence[PageRow]) -> int:
        self.pages.update({(p.url, p.crawl): p for p in pages})
        return len(pages)

    def write_changes(self, changes: Sequence[ChangeRow]) -> int:
        return 0

    def close(self) -> None:
        return None


class Pages(Sequence[PageRow]):
    """Pulled pages, in URL order, with the report of the pull that produced them."""

    def __init__(self, rows: Sequence[PageRow], report: PullReport) -> None:
        self._rows = sorted(rows, key=lambda r: r.url)
        self.report = report

    @overload
    def __getitem__(self, i: int) -> PageRow: ...
    @overload
    def __getitem__(self, i: slice) -> Sequence[PageRow]: ...
    def __getitem__(self, i: int | slice) -> PageRow | Sequence[PageRow]:
        return self._rows[i]

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[PageRow]:
        return iter(self._rows)

    def to_duckdb(self, con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
        """The pages in a ``strata_pages`` table, in ``con`` or a new in-memory database."""
        sink = DuckDbSink(con or duckdb.connect())
        sink.write_pages(self._rows)
        return sink.con

    def to_pandas(self) -> Any:
        """A pandas DataFrame, one row per page (pandas is not a Strata dependency)."""
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("to_pandas() needs pandas: pip install pandas") from exc
        return pd.DataFrame([r.model_dump() for r in self._rows])


class Strata:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()

    def profile(self, site: str, *, crawls: int = 1, budget: int = 400) -> SiteProfile:
        async def go() -> SiteProfile:
            async with http_client(self.settings) as client:
                ids = await newest_crawls(self.settings, client, crawls)
                profiler = site_profiler(self.settings, client, data_gate(), RequestBudget(budget))
                return await profiler.profile(site, ids)

        return run_sync(go())

    def diff(
        self,
        site: str,
        from_crawl: str | None = None,
        to_crawl: str | None = None,
        *,
        budget: int = 400,
    ) -> SiteDiff:
        """Changes between two crawls (by default the two newest), from the index alone."""

        async def go() -> SiteDiff:
            async with http_client(self.settings) as client:
                newest = await newest_crawls(self.settings, client, 2)
                b = to_crawl or newest[0]
                a = from_crawl or next(c for c in newest if c != b)
                service = change_service(self.settings, client, data_gate(), RequestBudget(budget))
                return await service.diff(site, a, b)

        return run_sync(go())

    def pages(
        self,
        site: str,
        *,
        pattern: str | None = None,
        limit: int = 50,
        crawl: str = "latest",
        budget: int = 150,
    ) -> Pages:
        """Pull up to ``limit`` pages (a job: budget, robots.txt, verified records) and return
        them normalized. Running it again continues an unfinished pull."""
        spec = CrawlSpec(
            crawl=crawl,
            budget=budget,
            targets=(Target(site=normalise_domain(site), pattern=pattern, pages=limit),),
            index=IndexChoice.COLUMNAR,
        )
        sink = _MemorySink()

        async def go() -> PullReport:
            async with engine(self.settings) as svc:
                return await pull_service(svc).pull(spec, sink, sink_name="memory")

        report = run_sync(go())
        return Pages(list(sink.pages.values()), report)


__all__ = ["Pages", "Strata"]
