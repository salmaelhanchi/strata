"""The console's FastAPI app: the REST API under /v1, /metrics, and the built web app.

``create_app`` takes its settings and the folder of the built web app, so tests run it against
a fake Common Crawl and a throwaway database, and serve a stand-in page.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from strata_console import problems
from strata_console.api import jobs, sites
from strata_console.context import open_console
from strata_console.metrics import Metrics
from strata_core import __version__
from strata_core.settings import Settings
from strata_fetch.setup import prepare

WEB_DIST = Path(__file__).parent / "static"

_NOT_BUILT = """<!doctype html><meta charset="utf-8"><title>Strata console</title>
<body style="font:16px system-ui;max-width:40rem;margin:4rem auto;padding:0 1rem">
<h1>The API is up; the web app isn't built</h1>
<p>Build it once with <code>make web</code> (or <code>npm ci && npm run build</code> in
<code>web/</code>), then reload. The API itself is documented at <a href="/docs">/docs</a>.</p>
"""


def create_app(settings: Settings | None = None, web_dist: Path = WEB_DIST) -> FastAPI:
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await prepare(settings)  # schema, catalog database, buckets, lake tables, if missing
        async with open_console(settings, metrics) as console:
            app.state.console = console
            yield

    app = FastAPI(
        title="Strata console",
        version=__version__,
        description="Profile a site, diff its crawls, pull its pages, trace any row: Common"
        " Crawl through Strata's polite, budgeted, verified engine.",
        lifespan=lifespan,
    )
    problems.install(app)
    metrics.install(app)
    app.include_router(sites.router)
    app.include_router(jobs.router)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    index = web_dist / "index.html"
    if (web_dist / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=web_dist / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False, response_model=None)
    def web(path: str) -> FileResponse | HTMLResponse:  # sync: FastAPI runs it in a thread
        """The web app for every other path: it routes in the browser."""
        if path.startswith(("v1/", "metrics", "docs", "openapi.json")):
            raise HTTPException(404, f"no route /{path}")
        if not index.exists():
            return HTMLResponse(_NOT_BUILT)
        candidate = (web_dist / path).resolve()
        if path and candidate.is_file() and candidate.is_relative_to(web_dist.resolve()):
            return FileResponse(candidate)
        return FileResponse(index)

    return app
