"""Commands that look at a site before anyone scrapes it; none of them needs a database."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from strata_cli.common import (
    JsonOpt,
    ThreadsOpt,
    app,
    data_gate,
    http_client,
    index_gate,
    load_settings,
    run_sync,
)
from strata_cli.output import (
    print_attempts,
    print_diff,
    print_json,
    print_profile,
    print_rates,
    shard_progress,
)
from strata_core.settings import Settings
from strata_fetch.cc.attempts import IndexBlockedError, IndexQueryError, IndexUnavailableError
from strata_fetch.cc.columnar import IndexBudgetError
from strata_fetch.cc.crawls import list_crawls
from strata_services.budget import RequestBudget
from strata_services.changes import ChangeRates, ChangeService, SiteDiff
from strata_services.profile import SiteProfile
from strata_services.profile_html import render
from strata_services.wiring import change_service, site_profiler

SiteArg = Annotated[str, typer.Argument(help="Host or domain, with its subdomains")]
# One site catalog per crawl: about 50 requests when built (python.org-sized sites), 0 cached.
PROFILE_BUDGET = 400


@app.command()
def profile(
    site: SiteArg,
    crawls: Annotated[
        int, typer.Option(min=1, max=12, help="How many of the newest crawls to compare")
    ] = 1,
    crawl: Annotated[
        list[str] | None,
        typer.Option("--crawl", help="A crawl id to include (repeatable; replaces --crawls)"),
    ] = None,
    html: Annotated[
        Path | None, typer.Option("--html", help="Write the report as one HTML file here")
    ] = None,
    budget: Annotated[
        int, typer.Option(min=1, help="Maximum requests, DuckDB's URL-index reads included")
    ] = PROFILE_BUDGET,
    robots_hosts: Annotated[
        int, typer.Option(min=0, max=10, help="Read robots.txt for this many of the largest hosts")
    ] = 3,
    threads: ThreadsOpt = None,
    as_json: JsonOpt = False,
) -> None:
    """Profile a site from the URL index: pages per crawl, URL shapes, statuses, types,
    languages, hosts and robots.txt. No page of the site is fetched."""
    settings = load_settings(threads)
    spend = RequestBudget(budget)

    async def go() -> SiteProfile:
        async with http_client(settings) as client:
            ids = list(crawl or [])
            if not ids:
                listed = await list_crawls(
                    client,
                    index_gate(),
                    index_base=settings.cc_index_base,
                    cache_dir=settings.cache_dir,
                )
                ids = [str(c["id"]) for c in listed[:crawls]]
            profiler = site_profiler(
                settings,
                client,
                data_gate(),
                spend,
                robots_hosts=robots_hosts,
                on_shard_progress=shard_progress,
            )
            return await profiler.profile(site, ids)

    try:
        result = run_sync(go())
    except (IndexBlockedError, IndexQueryError, IndexUnavailableError, IndexBudgetError) as exc:
        typer.secho(f"profile failed: {exc}", fg=typer.colors.RED, err=True)
        print_attempts(exc.attempts, err=True)
        typer.echo(f"requests spent: {spend.spent}/{spend.cap}", err=True)
        raise typer.Exit(1) from exc
    if html is not None:
        html.parent.mkdir(parents=True, exist_ok=True)
        html.write_text(render(result), encoding="utf-8")
    if as_json:
        print_json(result)
        return
    print_profile(result)
    if html is not None:
        typer.echo(f"report: {html}")


Newest = Callable[[int], Awaitable[list[str]]]  # the n newest crawl ids
IndexErrors = (IndexBlockedError, IndexQueryError, IndexUnavailableError, IndexBudgetError)


async def _newest_crawls(client: httpx.AsyncClient, settings: Settings, n: int) -> list[str]:
    listed = await list_crawls(
        client, index_gate(), index_base=settings.cc_index_base, cache_dir=settings.cache_dir
    )
    return [str(c["id"]) for c in listed[:n]]


def _run_or_exit[T](what: str, coro: Coroutine[Any, Any, T], spend: RequestBudget) -> T:
    try:
        return run_sync(coro)
    except IndexErrors as exc:
        typer.secho(f"{what} failed: {exc}", fg=typer.colors.RED, err=True)
        print_attempts(exc.attempts, err=True)
        typer.echo(f"requests spent: {spend.spent}/{spend.cap}", err=True)
        raise typer.Exit(1) from exc


def _with_changes[T](
    threads: int | None, budget: int, body: Callable[[ChangeService, Newest], Awaitable[T]]
) -> tuple[Callable[[], Coroutine[Any, Any, T]], RequestBudget]:
    settings = load_settings(threads)
    spend = RequestBudget(budget)

    async def go() -> T:
        async with http_client(settings) as client:
            service = change_service(
                settings, client, data_gate(), spend, on_shard_progress=shard_progress
            )
            return await body(service, lambda n: _newest_crawls(client, settings, n))

    return go, spend


@app.command()
def diff(
    site: SiteArg,
    from_crawl: Annotated[
        str | None, typer.Option("--from", help="Earlier crawl id (default: the second newest)")
    ] = None,
    to_crawl: Annotated[
        str | None, typer.Option("--to", help="Later crawl id (default: the newest)")
    ] = None,
    show: Annotated[int, typer.Option(min=0, help="Changed URLs to list")] = 10,
    budget: Annotated[int, typer.Option(min=1, help="Maximum requests")] = PROFILE_BUDGET,
    threads: ThreadsOpt = None,
    as_json: JsonOpt = False,
) -> None:
    """Added, removed and changed pages between two crawls, from the index's payload digests.
    No page is fetched."""

    async def body(service: ChangeService, newest_crawls: Newest) -> SiteDiff:
        a, b = from_crawl, to_crawl
        if a is None or b is None:
            newest = await newest_crawls(2)
            b = b or newest[0]
            a = a or next(c for c in newest if c != b)
        return await service.diff(site, a, b)

    go, spend = _with_changes(threads, budget, body)
    result = _run_or_exit("diff", go(), spend)
    print_json(result) if as_json else print_diff(result, show=show)


@app.command()
def changes(
    site: SiteArg,
    crawls: Annotated[
        int, typer.Option(min=2, max=12, help="How many of the newest crawls to compare")
    ] = 3,
    budget: Annotated[int, typer.Option(min=1, help="Maximum requests")] = PROFILE_BUDGET,
    threads: ThreadsOpt = None,
    as_json: JsonOpt = False,
) -> None:
    """How often each URL shape changed over the last crawls: what is worth re-scraping.
    No page is fetched."""

    async def body(service: ChangeService, newest_crawls: Newest) -> ChangeRates:
        ids = list(reversed(await newest_crawls(crawls)))
        return await service.rates(site, ids)

    go, spend = _with_changes(threads, budget, body)
    result = _run_or_exit("changes", go(), spend)
    print_json(result) if as_json else print_rates(result)
