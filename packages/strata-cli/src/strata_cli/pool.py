"""``strata scheduler`` and ``strata worker``: a job's page requests spread over worker processes,
with the budget and each host's politeness kept in the scheduler (ADR-009)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from strata_cli.common import JsonOpt, app, load_settings, run_sync
from strata_cli.output import print_json, print_report
from strata_core.models import JobState, RunReport
from strata_fetch.gate import load_targets
from strata_fetch.jobs import ResumeRefusedError, Services
from strata_pool.scheduler import default_worker_id, run_pooled, run_worker, serve_metrics
from strata_services.runtime import engine, resolve


@app.command()
def scheduler(
    targets: Annotated[Path, typer.Argument(help="TOML file: crawl, budget and [[target]]s")],
    listen: Annotated[
        str | None, typer.Option(help="host:port for workers (default STRATA_POOL_LISTEN)")
    ] = None,
    as_json: JsonOpt = False,
) -> None:
    """Run a targets file's job with its pages fetched by `strata worker` processes. Discovery,
    robots.txt verdicts and the bronze rows stay here; so do the budget and the data host's rate
    limit, which every worker's lease waits for. Run it again to continue an interrupted job."""
    settings = load_settings()
    spec = load_targets(targets)
    address = listen or settings.pool_listen
    if settings.pool_metrics_port:
        serve_metrics(settings.pool_metrics_port)

    async def body(svc: Services) -> tuple[str, RunReport | None]:
        crawl_id = await resolve(settings, svc.client, spec.crawl)
        return await run_pooled(
            spec.model_copy(update={"crawl": crawl_id}),
            svc,
            listen=address,
            on_listening=lambda where: typer.echo(
                f"scheduler on {where}; start workers with `strata worker`", err=True
            ),
        )

    async def go() -> tuple[str, RunReport | None]:
        async with engine(settings, on_setup=lambda line: typer.echo(line, err=True)) as svc:
            return await body(svc)

    try:
        job_id, report = run_sync(go())
    except ResumeRefusedError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    if report is None:
        typer.echo(f"job {job_id} already succeeded; nothing to fetch")
        return
    print_json(report) if as_json else print_report(report)
    if report.state is JobState.FAILED:
        raise typer.Exit(1)


@app.command()
def worker(
    scheduler: Annotated[
        str | None, typer.Option(help="The scheduler's host:port (default STRATA_POOL_SCHEDULER)")
    ] = None,
    worker_id: Annotated[
        str | None, typer.Option("--id", help="A name for this worker (default host-pid)")
    ] = None,
    slots: Annotated[
        int | None, typer.Option(min=1, max=16, help="Leases held at once (STRATA_WORKER_SLOTS)")
    ] = None,
) -> None:
    """Fetch pages for a scheduler until its job ends. Stores records in this machine's raw store
    settings, exactly as a local job would; waits for the scheduler if it isn't up yet."""
    settings = load_settings()
    name = worker_id or default_worker_id()
    stats = run_sync(
        run_worker(
            settings,
            scheduler=scheduler or settings.pool_scheduler,
            worker_id=name,
            slots=slots or settings.worker_slots,
        )
    )
    outcomes = ", ".join(f"{o.value} {n}" for o, n in sorted(stats.outcomes.items())) or "none"
    typer.echo(f"worker {name}: {stats.leases} lease(s); {outcomes}; rejected {stats.rejected}")
