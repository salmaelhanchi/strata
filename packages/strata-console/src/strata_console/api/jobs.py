"""Jobs, pulls, pulled pages and lineage: the console's view of the job engine."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from strata_console.api.sites import Ctx
from strata_console.context import Console
from strata_console.pagination import Page, decode, page_of
from strata_console.pulls import PullRequest, PullRun
from strata_core.lake import Lake
from strata_core.models import JobListItem, JobSummary
from strata_core.ops import OpsStore
from strata_fetch.blobstore import open_store
from strata_fetch.lineage import LineageReport, lineage

router = APIRouter(prefix="/v1", tags=["jobs"])


def _ops(ctx: Console) -> OpsStore:
    if ctx.ops is None:
        raise HTTPException(503, "the ops database is not reachable")
    return ctx.ops


@router.get("/jobs")
async def jobs(
    ctx: Ctx, cursor: str | None = None, limit: Annotated[int, Query(ge=1, le=100)] = 20
) -> Page[JobListItem]:
    ops = _ops(ctx)
    await ops.mark_interrupted(ctx.settings.stale_after_s)
    rows = await ops.list_jobs(limit + 1, before=decode(cursor))
    return page_of(rows, limit, rows[limit - 1].job_id if len(rows) > limit else None)


@router.get("/jobs/{job_id}")
async def job(job_id: str, ctx: Ctx) -> JobSummary:
    found = await _ops(ctx).job_summary(job_id)
    if found is None:
        raise HTTPException(404, f"no job {job_id}")
    return found


class PageItem(BaseModel):
    """A pulled page in a list: enough to choose one; its lineage has the rest."""

    url: str
    crawl: str
    title: str | None
    lang: str | None
    chars: int | None
    links: int
    robots_verdict: str
    captured_at: datetime | None
    capture_id: str


_PAGE_SQL = """
SELECT url, crawl, title, lang, length(main_text), coalesce(len(out_links), 0), robots_verdict,
       captured_at,
       capture_id
FROM strata_pages WHERE job_id = ? AND (?::VARCHAR IS NULL OR url > ?) ORDER BY url LIMIT ?
"""


@router.get("/jobs/{job_id}/pages")
async def job_pages(
    job_id: str,
    ctx: Ctx,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> Page[PageItem]:
    """Pages the console pulled for this job (pulls from the CLI go to their own sinks)."""
    after = decode(cursor)

    def read() -> list[tuple[object, ...]]:
        return ctx.pages.cursor().execute(_PAGE_SQL, [job_id, after, after, limit + 1]).fetchall()

    columns = list(PageItem.model_fields)
    rows = [
        PageItem.model_validate(dict(zip(columns, r, strict=True)))
        for r in await asyncio.to_thread(read)
    ]
    return page_of(rows, limit, rows[limit - 1].url if len(rows) > limit else None)


class PageText(BaseModel):
    title: str | None
    main_text: str | None
    out_links: list[str]
    normalizer_version: str


class Lineage(BaseModel):
    lineage: LineageReport
    page: PageText | None  # when the console pulled this capture


@router.get("/captures/{capture_id}/lineage")
async def capture_lineage(capture_id: str, ctx: Ctx) -> Lineage:
    """The row back to its bytes: Common Crawl's WARC location, our copy, both digests
    re-checked, the archived HTTP headers, and the robots.txt capture behind the verdict."""

    def trace() -> LineageReport | None:
        with Lake.open(ctx.settings, read_only=True) as lake:
            return lineage(
                capture_id, lake, open_store(ctx.settings), data_base=ctx.settings.cc_data_base
            )

    def text() -> PageText | None:
        row = (
            ctx.pages.cursor()
            .execute(
                "SELECT title, main_text, out_links, normalizer_version FROM strata_pages"
                " WHERE capture_id = ? LIMIT 1",
                [capture_id],
            )
            .fetchone()
        )
        if row is None:
            return None
        return PageText(
            title=row[0], main_text=row[1], out_links=row[2] or [], normalizer_version=row[3]
        )

    found = await asyncio.to_thread(trace)
    if found is None:
        raise HTTPException(404, f"no capture {capture_id}")
    return Lineage(lineage=found, page=await asyncio.to_thread(text))


# ---------------------------------------------------------------------------------- pulls
@router.post("/pulls", status_code=202)
async def start_pull(body: PullRequest, ctx: Ctx) -> PullRun:
    """Start a pull in the background; follow it at ``/v1/pulls/{run_id}/events``."""
    _ops(ctx)
    return ctx.pulls.start(body)


@router.get("/pulls/{run_id}")
async def pull(run_id: str, ctx: Ctx) -> PullRun:
    found = ctx.pulls.get(run_id)
    if found is None:
        raise HTTPException(404, f"no pull {run_id}")
    return found


@router.get("/pulls/{run_id}/events")
async def pull_events(run_id: str, ctx: Ctx) -> StreamingResponse:
    """Server-Sent Events: started, progress (once a second), then done or error."""
    if ctx.pulls.get(run_id) is None:
        raise HTTPException(404, f"no pull {run_id}")
    return StreamingResponse(
        ctx.pulls.stream(run_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
