"""``strata``: a thin Typer layer.

Every command calls a library function and prints the model it returns.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from strata_cli import pool as pool_commands  # noqa: F401 - registers its commands on app
from strata_cli import pull as pull_commands  # noqa: F401 - registers its commands on app
from strata_cli import serve as serve_commands  # noqa: F401 - registers its commands on app
from strata_cli import site as site_commands  # noqa: F401 - registers its commands on app
from strata_cli.common import (
    DEFAULT_BUDGET,
    BudgetOpt,
    CrawlOpt,
    DomainOpt,
    IndexOpt,
    JsonOpt,
    LimitOpt,
    OracleDirOpt,
    PatternOpt,
    QueryOpt,
    ThreadsOpt,
    app,
    data_gate,
    gate_app,
    http_client,
    index_gate,
    load_settings,
    run_app,
    run_sync,
)
from strata_cli.output import (
    duckdb_line,
    megabytes,
    print_attempts,
    print_gate1,
    print_json,
    print_lineage,
    print_report,
    print_summary,
    shard_progress,
)
from strata_core import db
from strata_core.lake import Lake
from strata_core.models import (
    CrawlSpec,
    IndexChoice,
    JobState,
    JobSummary,
    Outcome,
    RunReport,
    Target,
)
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_fetch.blobstore import open_store
from strata_fetch.cc.attempts import (
    IndexBlockedError,
    IndexQueryError,
    IndexUnavailableError,
)
from strata_fetch.cc.catalog import PageCount
from strata_fetch.cc.crawls import list_crawls, resolve_crawl
from strata_fetch.cc.index import DiscoveryResult
from strata_fetch.cc.sources import make_columnar, make_index
from strata_fetch.documents import ensure_oracles
from strata_fetch.gate import Gate1Run, load_targets, prove
from strata_fetch.jobs import (
    ResumeRefusedError,
    Services,
    resume_cc_job,
    run_cc_job,
    run_until_done,
)
from strata_fetch.lineage import lineage
from strata_fetch.setup import prepare
from strata_fetch.surt import normalise_domain
from strata_services.runtime import engine, resolve


# ------------------------------------------------------------------------------------ commands
@app.command()
def migrate() -> None:
    """Set everything up, idempotently: ops schema, the lake's catalog database, the S3 buckets
    (when used) and the lake's tables. Commands that write do this themselves first."""
    changed = run_sync(prepare(load_settings()))
    typer.echo("\n".join(changed) if changed else "everything already set up")


@app.command("lake-checkpoint")
def lake_checkpoint(
    older_than: Annotated[
        str | None,
        typer.Option(
            "--older-than",
            help="Retention, e.g. '7 days': expire older snapshots and delete files only they"
            " used (kept in the catalog for later checkpoints). Time travel reaches back that far.",
        ),
    ] = None,
) -> None:
    """DuckLake maintenance (CHECKPOINT): flush inlined rows, merge small files, clean up."""
    with Lake.open(load_settings()) as lake:
        before, after, seconds = lake.checkpoint(older_than)
    typer.echo(f"CHECKPOINT in {seconds:.2f} s; active data files {before} -> {after}")


@app.command()
def crawls(limit: Annotated[int, typer.Option(min=1)] = 5) -> None:
    """List the newest Common Crawl crawls (cached for a day)."""
    settings = load_settings()

    async def go() -> list[dict[str, Any]]:
        async with http_client(settings) as client:
            return await list_crawls(
                client,
                index_gate(),
                index_base=settings.cc_index_base,
                cache_dir=settings.cache_dir,
            )

    for crawl in run_sync(go())[:limit]:
        typer.echo(f"{crawl.get('id')}\t{crawl.get('name', '')}")


@app.command()
def discover(
    domain: DomainOpt,
    crawl: CrawlOpt = "latest",
    limit: LimitOpt = 50,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.AUTO,
    threads: ThreadsOpt = None,
    pattern: PatternOpt = None,
    as_json: JsonOpt = False,
) -> None:
    """Find distinct pages for one domain (cached; needs no database)."""
    settings = load_settings(threads)

    async def go() -> DiscoveryResult:
        async with http_client(settings) as client:
            gate = index_gate()
            crawl_id = await resolve_crawl(
                crawl,
                client,
                gate,
                index_base=settings.cc_index_base,
                cache_dir=settings.cache_dir,
            )
            source = make_index(
                index,
                client=client,
                settings=settings,
                index_gate=gate,
                data_gate=data_gate(),
                include_query_strings=include_query_strings,
                on_shard_progress=shard_progress,
            )
            return await source.find(normalise_domain(domain), crawl_id, limit, pattern)

    try:
        result = run_sync(go())
    except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
        typer.secho(f"discovery failed: {exc}", fg=typer.colors.RED, err=True)
        print_attempts(exc.attempts, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        print_json(result)
        return
    of = f" of {result.candidates:,} matching" if result.candidates is not None else ""
    if result.from_cache:
        typer.echo(
            f"{len(result.refs)} page(s){of} from cache (0 requests; found via {result.source})"
        )
    else:
        typer.echo(
            f"{len(result.refs)} distinct page(s){of} from {result.rows_scanned} index row(s);"
            f" {result.requests} request(s) in {result.elapsed_s:.1f} s"
        )
        if result.fallback_reason:
            typer.echo(f"  {result.fallback_reason}; used the Parquet URL index instead")
        if result.source == "columnar":
            typer.echo(
                f"  URL index: queried {result.shards_queried} of {result.shards_total} Parquet"
                f" file(s); {result.footers_read} footer(s) read this run"
            )
            if result.duckdb is not None:
                typer.echo(f"  {duckdb_line(result.shards_queried, result.duckdb)}")
        if any(a.outcome is not Outcome.OK for a in result.attempts):
            print_attempts(result.attempts)
    for ref in result.refs:
        when = ref.fetch_time.strftime("%Y-%m-%d") if ref.fetch_time else "?"
        typer.echo(f"  {ref.url}\t{when}\t{ref.warc_record_length} B")


@app.command()
def count(
    site: Annotated[str, typer.Argument(help="Host or domain, with its subdomains")],
    pattern: Annotated[
        str, typer.Option(help="RE2 regex the whole URL must match, e.g. '^https://x\\.org/a/$'")
    ],
    crawl: CrawlOpt = "latest",
    threads: ThreadsOpt = None,
    sample: Annotated[int, typer.Option(min=0, max=50, help="Matching URLs to show")] = 5,
    as_json: JsonOpt = False,
) -> None:
    """Count a site's eligible pages in a crawl's URL index (needs no database)."""
    settings = load_settings(threads)

    async def go() -> PageCount:
        async with http_client(settings) as client:
            crawl_id = await resolve_crawl(
                crawl,
                client,
                index_gate(),
                index_base=settings.cc_index_base,
                cache_dir=settings.cache_dir,
            )
            columnar = make_columnar(
                client=client,
                settings=settings,
                data_gate=data_gate(),
                on_shard_progress=shard_progress,
            )
            return await columnar.count(site, crawl_id, pattern, sample=sample)

    try:
        result = run_sync(go())
    except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
        typer.secho(f"count failed: {exc}", fg=typer.colors.RED, err=True)
        print_attempts(exc.attempts, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        print_json(result)
        return
    summary = result.summary
    statuses = ", ".join(f"{k}={v:,}" for k, v in list(summary.by_status.items())[:6])
    mimes = ", ".join(f"{k}={v:,}" for k, v in list(summary.by_mime.items())[:5])
    source = (
        "from the site catalog cache: 0 requests"
        if result.from_cache
        else duckdb_line(result.shards_queried, result.duckdb)
    )
    typer.echo(
        f"{result.site} in {result.crawl}\n"
        f"  pattern:  {result.pattern}\n"
        f"  captures: {result.captures:,} eligible (status 200, text/html) of {summary.rows:,}"
        f" for the site and its subdomains\n"
        f"  URLs:     {result.urls:,} distinct, {result.matching:,} matching the pattern\n"
        f"  all captures by status: {statuses or 'none'}\n"
        f"  by MIME type: {mimes or 'none'}\n"
        f"  URL index: queried {result.shards_queried} of {result.shards_total} Parquet file(s);"
        f" {result.footers_read} footer(s) read this run\n"
        f"  {source}"
    )
    if result.estimate is not None and not result.from_cache:
        e = result.estimate
        typer.echo(
            f"  estimated beforehand: {e.requests} request(s), {megabytes(e.bytes)}"
            f" ({e.row_groups} row group(s), {e.rows:,} rows)"
        )
    if any(a.outcome is not Outcome.OK for a in result.attempts):
        print_attempts(result.attempts)
    if summary.shapes:
        typer.echo("  URL shapes (distinct eligible URLs; digits shown as N):")
        for shape, n in summary.shapes.items():
            typer.echo(f"    {n:>7,}  {shape}")
    if result.sample:
        typer.echo("  sample of matching URLs:")
    for url in result.sample:
        typer.echo(f"    {url}")


async def _resolve(settings: Settings, client: httpx.AsyncClient, crawl: str) -> str:
    return await resolve(settings, client, crawl)


async def _with_services[T](settings: Settings, body: Callable[[Services], Awaitable[T]]) -> T:
    """Set up what's missing, open the engine, then run ``body``."""
    async with engine(settings, on_setup=lambda line: typer.echo(line, err=True)) as services:
        return await body(services)


async def _run_spec(settings: Settings, spec: CrawlSpec) -> RunReport:
    async def body(svc: Services) -> RunReport:
        crawl_id = await _resolve(settings, svc.client, spec.crawl)
        return await run_cc_job(spec.model_copy(update={"crawl": crawl_id}), svc)

    return await _with_services(settings, body)


async def _run_cc(
    settings: Settings,
    domain: str,
    crawl: str,
    limit: int,
    budget: int,
    include_query_strings: bool = False,
    index: IndexChoice = IndexChoice.AUTO,
    pattern: str | None = None,
) -> RunReport:
    spec = CrawlSpec(
        crawl=crawl,
        budget=budget,
        targets=(Target(site=normalise_domain(domain), pattern=pattern, pages=limit),),
        include_query_strings=include_query_strings,
        index=index,
    )
    return await _run_spec(settings, spec)


@run_app.command("cc")
def run_cc(
    domain: DomainOpt,
    crawl: CrawlOpt = "latest",
    limit: LimitOpt = 50,
    budget: BudgetOpt = DEFAULT_BUDGET,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.AUTO,
    pattern: PatternOpt = None,
    as_json: JsonOpt = False,
) -> None:
    """Fetch up to LIMIT distinct pages of DOMAIN from Common Crawl into the bronze table."""
    report = run_sync(
        _run_cc(
            load_settings(), domain, crawl, limit, budget, include_query_strings, index, pattern
        )
    )
    print_json(report) if as_json else print_report(report)
    if report.state is JobState.FAILED:
        raise typer.Exit(1)


@app.command()
def resume(
    job_id: str,
    force: Annotated[
        bool, typer.Option(help="Resume a job marked running even if its heartbeat is recent")
    ] = False,
    as_json: JsonOpt = False,
) -> None:
    """Continue a job that died or stopped: replay its write-ahead manifests, refund requests that
    were reserved but never sent, and fetch only the pages it still lacks."""
    settings = load_settings()

    async def body(svc: Services) -> RunReport:
        return await resume_cc_job(job_id, svc, force=force)

    try:
        report = run_sync(_with_services(settings, body))
    except ResumeRefusedError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    print_json(report) if as_json else print_report(report)
    if report.state is JobState.FAILED:
        raise typer.Exit(1)


@gate_app.command("run")
def gate_run(
    targets: Annotated[Path, typer.Argument(help="TOML file: crawl, budget and [[target]]s")],
    oracle: OracleDirOpt = Path("data/oracle"),
    as_json: JsonOpt = False,
) -> None:
    """Gate 1 in one command: set up, fetch the publishers' indexes if missing, run this targets
    file's job (or continue it after a crash), then normalize and prove every row. Run it again
    to continue an interrupted job; a finished one is only re-proven."""
    settings = load_settings()
    spec = load_targets(targets)

    async def body(svc: Services) -> Gate1Run:
        for f in await ensure_oracles(svc.client, oracle):
            typer.echo(f"downloaded {f.url}: {f.bytes:,} B, sha256 {f.sha256[:16]}...", err=True)
        crawl_id = await _resolve(settings, svc.client, spec.crawl)
        job_id, run = await run_until_done(spec.model_copy(update={"crawl": crawl_id}), svc)
        record = await svc.ops.job(job_id)
        if record is None or record.state is not JobState.SUCCEEDED:
            return Gate1Run(job_id=job_id, run=run, proof=None)
        assert isinstance(svc.ops, OpsStore)  # the CLI always runs on Postgres
        proof = await prove(job_id, svc.ops, svc.lake, svc.store, oracle)
        return Gate1Run(job_id=job_id, run=run, proof=proof)

    try:
        result = run_sync(_with_services(settings, body))
    except ResumeRefusedError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        print_json(result)
    else:
        print_gate1(result, oracle)
    if result.proof is None or not result.proof.gate.passed:
        raise typer.Exit(1)


@app.command()
def jobs(limit: Annotated[int, typer.Option(min=1, max=100)] = 10) -> None:
    """The newest jobs, with their state: find the id of a job to resume, report or check."""
    settings = load_settings()

    async def go() -> list[tuple[str, JobState, str | None, datetime, datetime | None]]:
        async with db.make_pool(settings.ops_dsn, max_size=2) as pool:
            ops = OpsStore(pool)
            await ops.mark_interrupted(settings.stale_after_s)
            return await ops.recent_jobs(limit)

    for job_id, state, reason, created, beat in run_sync(go()):
        seen = f"; last heartbeat {beat:%Y-%m-%d %H:%M:%S}" if beat else ""
        typer.echo(
            f"{job_id}  {state.value:<11} created {created:%Y-%m-%d %H:%M:%S}{seen}"
            + (f"  ({reason})" if reason else "")
        )


@app.command()
def report(job_id: str, as_json: JsonOpt = False) -> None:
    """Summarise a job from the ops database and the lake."""
    settings = load_settings()

    async def go() -> JobSummary | None:
        async with db.make_pool(settings.ops_dsn, max_size=2) as pool:
            ops = OpsStore(pool)
            await ops.mark_interrupted(settings.stale_after_s)
            return await ops.job_summary(job_id)

    summary = run_sync(go())
    if summary is None:
        typer.secho(f"no job {job_id}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)
    with Lake.open(settings, read_only=True) as lake:
        summary = summary.model_copy(update={"rows_in_lake": len(lake.capture_ids(job_id))})
    print_json(summary) if as_json else print_summary(summary)


@app.command("lineage")
def lineage_cmd(capture_id: str, as_json: JsonOpt = False) -> None:
    """Trace a bronze row back to its bytes and check them."""
    settings = load_settings()
    with Lake.open(settings, read_only=True) as lake:
        found = lineage(capture_id, lake, open_store(settings), data_base=settings.cc_data_base)
    if found is None:
        typer.secho(f"no capture {capture_id}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)
    print_json(found) if as_json else print_lineage(found)


@app.command()
def demo(
    domain: DomainOpt,
    crawl: CrawlOpt = "latest",
    limit: LimitOpt = 50,
    budget: BudgetOpt = DEFAULT_BUDGET,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.AUTO,
) -> None:
    """Set up, fetch a few pages of one site, report, then trace one row to its WARC bytes."""
    settings = load_settings()
    changed = run_sync(prepare(settings))
    typer.echo(f"1/3 set up: {', '.join(changed) if changed else 'nothing to do'}")
    typer.echo(f"2/3 fetching up to {limit} distinct page(s) of {domain} (budget {budget})")
    run_report = run_sync(
        _run_cc(settings, domain, crawl, limit, budget, include_query_strings, index)
    )
    print_report(run_report)
    if run_report.first_capture_id is None:
        typer.echo("3/3 no capture stored, so there is nothing to trace")
        raise typer.Exit(1 if run_report.state is JobState.FAILED else 0)
    typer.echo("3/3 lineage of the first capture")
    with Lake.open(settings, read_only=True) as lake:
        found = lineage(
            run_report.first_capture_id,
            lake,
            open_store(settings),
            data_base=settings.cc_data_base,
        )
    if found is not None:
        print_lineage(found)


def main() -> None:
    """The ``strata`` command. On Windows, Click expands wildcards in arguments by default,
    so ``--pattern '.*'`` became a list of dot-files; Strata's arguments are never file globs."""
    app(windows_expand_args=False)


if __name__ == "__main__":
    main()
