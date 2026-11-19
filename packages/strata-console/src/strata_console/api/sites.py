"""Sites: what a site holds and how it changes, from Common Crawl's index. No page is fetched."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel

from strata_console.context import Console
from strata_console.pagination import Page, decode, page_of
from strata_fetch.cc.crawls import list_crawls
from strata_services.budget import RequestBudget
from strata_services.changes import Change, ChangeRates, ShapeDiff, SiteDiff, UrlChange
from strata_services.profile import SiteProfile
from strata_services.wiring import change_service, site_profiler

router = APIRouter(prefix="/v1", tags=["sites"])


def console(request: Request) -> Console:
    found: Console = request.app.state.console
    return found


Ctx = Annotated[Console, Depends(console)]
Budget = Annotated[int, Query(ge=1, le=5000, description="Maximum requests for this answer")]


class Crawl(BaseModel):
    id: str
    name: str


class DiffSummary(BaseModel):
    """A diff without its URL list; the list pages through ``.../diff/changes``."""

    site: str
    from_crawl: str
    to_crawl: str
    counts: dict[Change, int]
    shapes: list[ShapeDiff]
    requests: int
    footer_reads: int
    page_fetches: int


@router.get("/crawls")
async def crawls(ctx: Ctx, limit: Annotated[int, Query(ge=1, le=50)] = 6) -> list[Crawl]:
    listed = await list_crawls(
        ctx.client,
        ctx.index_gate,
        index_base=ctx.settings.cc_index_base,
        cache_dir=ctx.settings.cache_dir,
    )
    return [Crawl(id=str(c["id"]), name=str(c.get("name", ""))) for c in listed[:limit]]


async def _newest(ctx: Console, n: int) -> list[str]:
    return [c.id for c in await crawls(ctx, limit=n)]


def _spent(ctx: Console, kind: str, budget: RequestBudget) -> None:
    ctx.metrics.upstream_requests.labels(kind).inc(budget.spent)


@router.get("/sites/{site}/profile")
async def profile(
    site: str,
    ctx: Ctx,
    crawls_: Annotated[int, Query(alias="crawls", ge=1, le=12)] = 2,
    budget: Budget = 400,
) -> SiteProfile:
    spend = RequestBudget(budget)
    try:
        profiler = site_profiler(ctx.settings, ctx.client, ctx.data_gate, spend)
        return await profiler.profile(site, await _newest(ctx, crawls_))
    finally:
        _spent(ctx, "profile", spend)


async def _diff(ctx: Console, site: str, a: str | None, b: str | None, budget: int) -> SiteDiff:
    if a is None or b is None:
        newest = await _newest(ctx, 2)
        b = b or newest[0]
        a = a or next(c for c in newest if c != b)
    spend = RequestBudget(budget)
    try:
        return await change_service(ctx.settings, ctx.client, ctx.data_gate, spend).diff(site, a, b)
    finally:
        _spent(ctx, "diff", spend)


FromCrawl = Annotated[str | None, Query(alias="from", description="Earlier crawl id")]
ToCrawl = Annotated[str | None, Query(alias="to", description="Later crawl id")]


@router.get("/sites/{site}/diff")
async def diff(
    site: str, ctx: Ctx, from_: FromCrawl = None, to: ToCrawl = None, budget: Budget = 400
) -> DiffSummary:
    d = await _diff(ctx, site, from_, to, budget)
    return DiffSummary.model_validate(d.model_dump(exclude={"changes"}))


@router.get("/sites/{site}/diff/changes")
async def diff_changes(
    site: str,
    ctx: Ctx,
    from_: FromCrawl = None,
    to: ToCrawl = None,
    change: Annotated[Change | None, Query(description="Only this kind")] = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    budget: Budget = 400,
) -> Page[UrlChange]:
    """The changed URLs of a diff, in URL order, a page at a time."""
    d = await _diff(ctx, site, from_, to, budget)
    after = decode(cursor)
    rows = [
        c
        for c in d.changes
        if (change is None or c.change is change) and (after is None or c.url > after)
    ][: limit + 1]
    return page_of(rows, limit, rows[limit - 1].url if len(rows) > limit else None)


@router.get("/sites/{site}/changes")
async def changes(
    site: str,
    ctx: Ctx,
    crawls_: Annotated[int, Query(alias="crawls", ge=2, le=12)] = 3,
    budget: Budget = 400,
) -> ChangeRates:
    spend = RequestBudget(budget)
    try:
        ids = list(reversed(await _newest(ctx, crawls_)))
        service = change_service(ctx.settings, ctx.client, ctx.data_gate, spend)
        return await service.rates(site, ids)
    finally:
        _spent(ctx, "changes", spend)
