"""``strata pull``: a site's pages from Common Crawl into your own database."""

from __future__ import annotations

from typing import Annotated

import typer

from strata_cli.common import (
    DEFAULT_BUDGET,
    CrawlOpt,
    IndexOpt,
    JsonOpt,
    PatternOpt,
    QueryOpt,
    app,
    load_settings,
    run_sync,
)
from strata_cli.output import print_json, print_pull
from strata_core.models import CrawlSpec, IndexChoice, JobState, Target
from strata_core.sinks import SCHEMES, describe, open_sink
from strata_fetch.jobs import ResumeRefusedError
from strata_fetch.surt import normalise_domain
from strata_services.pull import PullReport
from strata_services.runtime import engine
from strata_services.wiring import pull_service

IntoOpt = Annotated[
    str,
    typer.Option(
        "--into",
        help="Where the rows go: postgresql://..., sqlite:///pages.db, duckdb:///pages.duckdb"
        " or parquet:///folder",
    ),
]


@app.command()
def pull(
    site: Annotated[str, typer.Argument(help="Host or domain, with its subdomains")],
    into: IntoOpt,
    pattern: PatternOpt = None,
    limit: Annotated[int, typer.Option(min=1, max=5000, help="Pages to pull")] = 50,
    crawl: CrawlOpt = "latest",
    budget: Annotated[
        int, typer.Option(min=1, help="Maximum requests, retries and index reads included")
    ] = DEFAULT_BUDGET,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.COLUMNAR,
    as_json: JsonOpt = False,
) -> None:
    """Fetch pages of SITE as a job (budget, robots.txt, verified records, crash-resume), then
    write title, main text, language and links, with lineage, into a table named strata_pages.
    Running it again continues an unfinished pull and replaces rows instead of duplicating them."""
    if not into.startswith(SCHEMES) and "://" in into:
        raise typer.BadParameter(f"use one of {', '.join(SCHEMES)}", param_hint="--into")
    settings = load_settings()
    spec = CrawlSpec(
        crawl=crawl,
        budget=budget,
        targets=(Target(site=normalise_domain(site), pattern=pattern, pages=limit),),
        include_query_strings=include_query_strings,
        index=index,
    )

    async def go() -> PullReport:
        sink = open_sink(into)
        try:
            async with engine(settings, on_setup=lambda s: typer.echo(s, err=True)) as svc:
                return await pull_service(svc).pull(spec, sink, sink_name=describe(into))
        finally:
            sink.close()

    try:
        report = run_sync(go())
    except ResumeRefusedError as exc:
        typer.secho(f"pull refused: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    print_json(report) if as_json else print_pull(report)
    if report.state is not JobState.SUCCEEDED:
        raise typer.Exit(1)
