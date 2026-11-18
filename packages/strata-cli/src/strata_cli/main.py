"""``strata``: a thin Typer layer.

Every command calls a library function and prints the model it returns.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable, Coroutine
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer
from pydantic import BaseModel

from strata_core import db
from strata_core.lake import Lake
from strata_core.ledger import PostgresLedger
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
from strata_fetch.cc.crawls import list_crawls, resolve_crawl
from strata_fetch.cc.index import (
    DiscoveryResult,
    DuckDbUsage,
    IndexAttempt,
    IndexBlockedError,
    IndexQueryError,
    IndexUnavailableError,
    PageCount,
)
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
from strata_fetch.lineage import LineageReport, lineage
from strata_fetch.ratelimit import HostGate
from strata_fetch.setup import prepare
from strata_fetch.surt import normalise_domain

app = typer.Typer(no_args_is_help=True, help="Strata: polite, provenance-first web data sourcing.")
run_app = typer.Typer(no_args_is_help=True, help="Run an acquisition job.")
app.add_typer(run_app, name="run")
gate_app = typer.Typer(no_args_is_help=True, help="Gate 1: run a targets file, then prove it.")
app.add_typer(gate_app, name="gate1")


def main() -> None:
    """The ``strata`` command. On Windows, Click expands wildcards in arguments by default,
    so ``--pattern '.*'`` became a list of dot-files; Strata's arguments are never file globs."""
    app(windows_expand_args=False)


DomainOpt = Annotated[
    str, typer.Option(help="Site: a domain or host, with its subdomains, e.g. peps.python.org")
]
CrawlOpt = Annotated[str, typer.Option(help="Crawl id such as CC-MAIN-2026-17, or 'latest'")]
LimitOpt = Annotated[int, typer.Option(min=1, max=5000, help="Maximum distinct pages")]
BudgetOpt = Annotated[
    int,
    typer.Option(min=1, help="Maximum requests: retries and DuckDB's URL-index requests included"),
]
# A fresh URL-index lookup for python.org cost 48 DuckDB requests (2026-11-08), and those are
# charged (ADR-004, amendment): 20 pages need about 70, so 60 would stop a demo halfway.
DEFAULT_BUDGET = 150
JsonOpt = Annotated[bool, typer.Option("--json", help="Print JSON instead of text")]
PatternOpt = Annotated[
    str | None,
    typer.Option(help="RE2 regex the whole URL must match, e.g. '^https://x\\.org/a/$'"),
]
OracleDirOpt = Annotated[
    Path, typer.Option("--oracle", help="Folder with peps.json and rfc-index.txt")
]
QueryOpt = Annotated[
    bool,
    typer.Option(
        "--include-query-strings",
        help="Keep URLs with a query string (by default /?ref=... style variants are dropped)",
    ),
]
ThreadsOpt = Annotated[
    int | None,
    typer.Option(
        "--threads",
        min=1,
        max=2,
        help="DuckDB threads for URL-index queries (default STRATA_INDEX_THREADS or 1; ADR-003"
        " allows two requests in flight)",
    ),
]
IndexOpt = Annotated[
    IndexChoice,
    typer.Option(
        "--index",
        help="auto: CDX server, then the Parquet URL index if it is unavailable; "
        "cdx or columnar: only that one (ADR-004)",
    ),
]


def _run[T](coro: Coroutine[Any, Any, T]) -> T:
    # psycopg's async driver needs a selector event loop; Windows defaults to the proactor loop.
    if sys.platform == "win32":
        return asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(coro)


def _settings(threads: int | None = None) -> Settings:
    settings = Settings()
    if threads is not None:
        settings = settings.model_copy(update={"index_threads": threads})
    return settings


def _client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.http_timeout_s,
        follow_redirects=False,
    )


def _index_gate() -> HostGate:
    # Matches the seeded policy for index.commoncrawl.org (ADR-003), for commands that run without
    # the ops database.
    return HostGate(rate=0.5, burst=1, max_in_flight=1)


def _data_gate() -> HostGate:
    # Matches the seeded policy for data.commoncrawl.org (ADR-003).
    return HostGate(rate=2.0, burst=2, max_in_flight=2)


def _shard_progress(done: int, total: int) -> None:
    # stderr, so `--json` output stays parseable
    if done == 0:
        minutes = total / 2.0 / 60  # one footer request each, at 2 requests/s (ADR-003)
        typer.echo(
            f"URL index: reading {total} Parquet footer(s) once for this crawl, about "
            f"{minutes:.0f} min at 2 requests/s; cached afterwards",
            err=True,
        )
    else:
        typer.echo(f"  footers read: {done}/{total}", err=True)


def _print_json(model: BaseModel) -> None:
    typer.echo(model.model_dump_json(indent=2))


# ------------------------------------------------------------------------------------ commands
@app.command()
def migrate() -> None:
    """Set everything up, idempotently: ops schema, the lake's catalog database, the S3 buckets
    (when used) and the lake's tables. Commands that write do this themselves first."""
    changed = _run(prepare(_settings()))
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
    with Lake.open(_settings()) as lake:
        before, after, seconds = lake.checkpoint(older_than)
    typer.echo(f"CHECKPOINT in {seconds:.2f} s; active data files {before} -> {after}")


@app.command()
def crawls(limit: Annotated[int, typer.Option(min=1)] = 5) -> None:
    """List the newest Common Crawl crawls (cached for a day)."""
    settings = _settings()

    async def go() -> list[dict[str, Any]]:
        async with _client(settings) as client:
            return await list_crawls(
                client,
                _index_gate(),
                index_base=settings.cc_index_base,
                cache_dir=settings.cache_dir,
            )

    for crawl in _run(go())[:limit]:
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
    settings = _settings(threads)

    async def go() -> DiscoveryResult:
        async with _client(settings) as client:
            index_gate = _index_gate()
            crawl_id = await resolve_crawl(
                crawl,
                client,
                index_gate,
                index_base=settings.cc_index_base,
                cache_dir=settings.cache_dir,
            )
            source = make_index(
                index,
                client=client,
                settings=settings,
                index_gate=index_gate,
                data_gate=_data_gate(),
                include_query_strings=include_query_strings,
                on_shard_progress=_shard_progress,
            )
            return await source.find(normalise_domain(domain), crawl_id, limit, pattern)

    try:
        result = _run(go())
    except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
        typer.secho(f"discovery failed: {exc}", fg=typer.colors.RED, err=True)
        _print_attempts(exc.attempts, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        _print_json(result)
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
                typer.echo(f"  {_duckdb_line(result.shards_queried, result.duckdb)}")
        if any(a.outcome is not Outcome.OK for a in result.attempts):
            _print_attempts(result.attempts)
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
    """Count a site's eligible pages in a crawl's URL index (week 2, S9; needs no database)."""
    settings = _settings(threads)

    async def go() -> PageCount:
        async with _client(settings) as client:
            crawl_id = await resolve_crawl(
                crawl,
                client,
                _index_gate(),
                index_base=settings.cc_index_base,
                cache_dir=settings.cache_dir,
            )
            columnar = make_columnar(
                client=client,
                settings=settings,
                data_gate=_data_gate(),
                on_shard_progress=_shard_progress,
            )
            return await columnar.count(site, crawl_id, pattern, sample=sample)

    try:
        result = _run(go())
    except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
        typer.secho(f"count failed: {exc}", fg=typer.colors.RED, err=True)
        _print_attempts(exc.attempts, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        _print_json(result)
        return
    summary = result.summary
    statuses = ", ".join(f"{k}={v:,}" for k, v in list(summary.by_status.items())[:6])
    mimes = ", ".join(f"{k}={v:,}" for k, v in list(summary.by_mime.items())[:5])
    source = (
        "from the site catalog cache: 0 requests"
        if result.from_cache
        else _duckdb_line(result.shards_queried, result.duckdb)
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
            f"  estimated beforehand: {e.requests} request(s), {_megabytes(e.bytes)}"
            f" ({e.row_groups} row group(s), {e.rows:,} rows)"
        )
    if any(a.outcome is not Outcome.OK for a in result.attempts):
        _print_attempts(result.attempts)
    if summary.shapes:
        typer.echo("  URL shapes (distinct eligible URLs; digits shown as N):")
        for shape, n in summary.shapes.items():
            typer.echo(f"    {n:>7,}  {shape}")
    if result.sample:
        typer.echo("  sample of matching URLs:")
    for url in result.sample:
        typer.echo(f"    {url}")


async def _resolve(settings: Settings, client: httpx.AsyncClient, crawl: str) -> str:
    return await resolve_crawl(
        crawl,
        client,
        _index_gate(),
        index_base=settings.cc_index_base,
        cache_dir=settings.cache_dir,
    )


async def _with_services[T](settings: Settings, body: Callable[[Services], Awaitable[T]]) -> T:
    """Set up what's missing, open the ops pool, the HTTP client, the lake and the raw store,
    then run ``body``."""
    for line in await prepare(settings):
        typer.echo(line, err=True)
    async with db.make_pool(settings.ops_dsn) as pool, _client(settings) as client:
        with Lake.open(settings) as lake:
            services = Services(
                settings=settings,
                ops=OpsStore(pool),
                ledger=PostgresLedger(pool),
                lake=lake,
                client=client,
                store=open_store(settings),
            )
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
    report = _run(
        _run_cc(_settings(), domain, crawl, limit, budget, include_query_strings, index, pattern)
    )
    _print_json(report) if as_json else _print_report(report)
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
    settings = _settings()

    async def body(svc: Services) -> RunReport:
        return await resume_cc_job(job_id, svc, force=force)

    try:
        report = _run(_with_services(settings, body))
    except ResumeRefusedError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    _print_json(report) if as_json else _print_report(report)
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
    settings = _settings()
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
        result = _run(_with_services(settings, body))
    except ResumeRefusedError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        _print_json(result)
    else:
        _print_gate1(result, oracle)
    if result.proof is None or not result.proof.gate.passed:
        raise typer.Exit(1)


def _print_gate1(result: Gate1Run, oracle: Path) -> None:
    if result.run is None:
        typer.echo(f"job {result.job_id} had already finished: proving it again")
    else:
        _print_report(result.run)
    if result.proof is None:
        typer.echo(
            "the job didn't finish, so there is nothing to prove; run the same command again"
        )
        return
    p = result.proof
    typer.echo(
        f"silver.documents: {p.documents} row(s); {p.without_extractor} page(s) without an"
        " extractor"
    )
    if p.agreement is None:
        typer.echo(f"  no publisher indexes in {oracle}: agreement not checked")
    else:
        for name, f in p.agreement.fields.items():
            rate = "n/a" if f.rate is None else f"{f.rate:.1%}"
            typer.echo(f"  {name:<14} {f.agreed:>5}/{f.compared:<5} {rate}")
        for line in p.agreement.disagreements:
            typer.echo(f"    {line}")
    g = p.gate
    typer.echo(f"Gate 1 for {g.job_id}: {g.rows} row(s)")
    for c in g.checks:
        mark = {True: "PASS", False: "FAIL", None: "info"}[c.passed]
        typer.echo(f"  {mark}  {c.name}: {c.detail}")
    typer.echo("PASSED" if g.passed else "FAILED")


@app.command()
def jobs(limit: Annotated[int, typer.Option(min=1, max=100)] = 10) -> None:
    """The newest jobs, with their state: find the id of a job to resume, report or check."""
    settings = _settings()

    async def go() -> list[tuple[str, JobState, str | None, datetime, datetime | None]]:
        async with db.make_pool(settings.ops_dsn, max_size=2) as pool:
            ops = OpsStore(pool)
            await ops.mark_interrupted(settings.stale_after_s)
            return await ops.recent_jobs(limit)

    for job_id, state, reason, created, beat in _run(go()):
        seen = f"; last heartbeat {beat:%Y-%m-%d %H:%M:%S}" if beat else ""
        typer.echo(
            f"{job_id}  {state.value:<11} created {created:%Y-%m-%d %H:%M:%S}{seen}"
            + (f"  ({reason})" if reason else "")
        )


@app.command()
def report(job_id: str, as_json: JsonOpt = False) -> None:
    """Summarise a job from the ops database and the lake."""
    settings = _settings()

    async def go() -> JobSummary | None:
        async with db.make_pool(settings.ops_dsn, max_size=2) as pool:
            ops = OpsStore(pool)
            await ops.mark_interrupted(settings.stale_after_s)
            return await ops.job_summary(job_id)

    summary = _run(go())
    if summary is None:
        typer.secho(f"no job {job_id}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)
    with Lake.open(settings, read_only=True) as lake:
        summary = summary.model_copy(update={"rows_in_lake": len(lake.capture_ids(job_id))})
    _print_json(summary) if as_json else _print_summary(summary)


@app.command("lineage")
def lineage_cmd(capture_id: str, as_json: JsonOpt = False) -> None:
    """Trace a bronze row back to its bytes and check them."""
    settings = _settings()
    with Lake.open(settings, read_only=True) as lake:
        found = lineage(capture_id, lake, open_store(settings), data_base=settings.cc_data_base)
    if found is None:
        typer.secho(f"no capture {capture_id}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)
    _print_json(found) if as_json else _print_lineage(found)


@app.command()
def demo(
    domain: DomainOpt,
    crawl: CrawlOpt = "latest",
    limit: LimitOpt = 50,
    budget: BudgetOpt = DEFAULT_BUDGET,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.AUTO,
) -> None:
    """Week-1 demo: migrate, prepare the lake, fetch, report, then trace one row."""
    settings = _settings()
    changed = _run(prepare(settings))
    typer.echo(f"1/4 set up: {', '.join(changed) if changed else 'nothing to do'}")
    typer.echo("2/4 lake ready")
    typer.echo(f"3/4 fetching up to {limit} distinct page(s) of {domain} (budget {budget})")
    run_report = _run(_run_cc(settings, domain, crawl, limit, budget, include_query_strings, index))
    _print_report(run_report)
    if run_report.first_capture_id is None:
        typer.echo("4/4 no capture stored, so there is nothing to trace")
        raise typer.Exit(1 if run_report.state is JobState.FAILED else 0)
    typer.echo("4/4 lineage of the first capture")
    with Lake.open(settings, read_only=True) as lake:
        found = lineage(
            run_report.first_capture_id,
            lake,
            open_store(settings),
            data_base=settings.cc_data_base,
        )
    if found is not None:
        _print_lineage(found)


# ------------------------------------------------------------------------------------ printing
def _ok(flag: bool | None) -> str:
    return {True: "yes", False: "NO", None: "n/a"}[flag]


def _print_attempts(attempts: list[IndexAttempt], *, err: bool = False) -> None:
    # Show every attempt of a short retry sequence; of a long one (a shard-map build makes
    # hundreds of requests), only the failures.
    if len(attempts) > 12:
        attempts = [a for a in attempts if a.outcome is not Outcome.OK]
    for a in attempts:
        status = f"HTTP {a.http_status}" if a.http_status is not None else "no response"
        line = (
            f"  attempt {a.attempt}: {status} {a.outcome.value} after {a.elapsed_ms / 1000:.1f} s"
        )
        if a.waited_ms is not None:
            line += f"; waited {a.waited_ms / 1000:.1f} s before retrying"
        if a.error and a.outcome is not Outcome.OK:
            line += f"\n    {a.error[:160]}"
        typer.echo(line, err=err)


def _megabytes(n: int) -> str:
    return f"{n / 1_000_000:.1f} MB"


def _duckdb_line(files: int, usage: DuckDbUsage) -> str:
    """e.g. 'DuckDB: 1 file, 23 requests, 61.0 MB in 31.2 s (at most 1 in flight)'."""
    line = (
        f"DuckDB: {files} file(s), {usage.requests} request(s), {_megabytes(usage.bytes)}"
        f" in {usage.query_s:.1f} s (at most {usage.peak_in_flight} in flight)"
    )
    if usage.failed:
        line += f"; {usage.failed} request(s) failed"
    return line


def _counts(outcomes: dict[Outcome, int]) -> str:
    return ", ".join(f"{k.value}={v}" for k, v in sorted(outcomes.items())) or "none"


def _print_report(r: RunReport) -> None:
    index = (
        "from cache, 0 requests"
        if r.refs_from_cache
        else f"{r.index_attempts} index request(s), {r.index_rows_scanned} row(s) read"
    )
    if r.index_source:
        index += f", via {r.index_source}"
    if r.index_duckdb_requests:
        index += (
            f"; DuckDB made {r.index_duckdb_requests} of them"
            f" ({_megabytes(r.index_duckdb_bytes)}, charged to the budget)"
        )
    if r.index_fallback:
        index += f"; {r.index_fallback}"
    typer.echo(
        f"job {r.job_id}: {r.state.value}"
        + (f" ({r.stop_reason})" if r.stop_reason else "")
        + f"\n  {r.domain} in {r.crawl}: {r.refs_found} distinct page(s) ({index})"
        + f"\n  outcomes: {_counts(r.outcomes)}; rows written: {r.rows_written}"
        + f"\n  budget: {r.budget_spent}/{r.budget_cap}; rate-limited attempts: {r.rate_limited}"
        + f"\n  stored {r.bytes_stored} B; run took {r.duration_s:.1f} s,"
        f" pages fetched at {r.achieved_rps} req/s"
        + f"\n  quarantined: {r.quarantined}; recompute digest mismatches: "
        + f"{r.digest_recompute_mismatches}"
        + (f"\n  first capture: {r.first_capture_id}" if r.first_capture_id else "")
    )
    if r.already_stored:
        typer.echo(f"  already in the lake: {r.already_stored} selected page(s), not fetched again")
    if r.resumed:
        typer.echo(
            f"  resumed: {r.recovered_rows} row(s) rebuilt from the write-ahead manifests;"
            f" {r.refunded} reserved request(s) refunded (never sent)"
        )
    if len(r.targets) > 1 or (r.targets and r.targets[0].pattern):
        for t in r.targets:
            typer.echo(
                f"  target {t.site}: {t.rows}/{t.pages_wanted} row(s); {t.candidates} candidate(s),"
                f" {t.allowed} allowed by robots.txt ({t.robots or 'no hosts'})"
            )


def _print_summary(s: JobSummary) -> None:
    per_host = "".join(
        f"\n    {host}: {_counts(outcomes)}" for host, outcomes in s.attempts_by_host.items()
    )
    typer.echo(
        f"job {s.job_id}: {s.state.value}"
        + (f" ({s.stop_reason})" if s.stop_reason else "")
        + f"\n  {s.domain} in {s.crawl}"
        + f"\n  attempts: {s.attempts} ({_counts(s.attempt_outcomes)})"
        + per_host
        + (
            "\n  by client: "
            + ", ".join(f"{c.value}={n}" for c, n in sorted(s.attempts_by_client.items()))
            if s.attempts_by_client
            else ""
        )
        + (
            "\n  robots.txt verdicts: "
            + ", ".join(f"{v.value}={n}" for v, n in sorted(s.verdicts.items()))
            if s.verdicts
            else ""
        )
        + f"\n  budget: {s.budget_spent}/{s.budget_cap};"
        f" bytes downloaded (index reads included): {s.bytes_stored:,}"
        + f"\n  busiest second: {s.peak_requests_per_second} request(s) to one host;"
        f" rows in lake: {s.rows_in_lake}"
    )


def _print_lineage(rep: LineageReport) -> None:
    c = rep.capture
    lines = [
        f"capture {c.capture_id}",
        f"  url:            {c.url}",
        f"  upstream:       {rep.upstream_url} [{rep.upstream_range}]",
        f"  upstream time:  {c.upstream_fetch_time}   fetched by strata: {c.fetched_at} UTC",
        f"  our copy:       {c.raw_uri} @ {c.raw_offset} (+{c.raw_length} B)",
        f"  bytes intact:   {_ok(rep.raw_bytes_intact)} (sha256 {c.raw_sha256[:16]}...)",
        f"  digests:        index {rep.index_digest}",
        f"                  warc  {rep.warc_header_digest}"
        f"  -> index matches header: {_ok(rep.index_matches_warc_header)}",
        f"                  ours  {rep.recomputed_digest}"
        f"  -> recompute matches: {_ok(rep.recompute_matches_header)}",
        f"  policy verdict: {c.policy_verdict.value} (robots.txt capture"
        f" {c.robots_capture_id or 'none'}); job {c.job_id}; strata {c.strata_version}",
        f"  archived reply: {rep.http_status_line}",
    ]
    lines += [f"    {k}: {v}" for k, v in rep.http_headers[:6]]
    typer.echo("\n".join(lines))


if __name__ == "__main__":
    main()
