"""``strata``: a thin Typer layer.

Every command calls a library function and prints the model it returns.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Coroutine
from typing import Annotated, Any

import httpx
import typer
from pydantic import BaseModel

from strata_core import db
from strata_core.lake import Lake
from strata_core.ledger import PostgresLedger
from strata_core.models import CrawlSpec, IndexChoice, JobState, JobSummary, Outcome, RunReport
from strata_core.ops import OpsStore
from strata_core.settings import Settings
from strata_fetch.blobstore import LocalBlobStore
from strata_fetch.cc.crawls import list_crawls, resolve_crawl
from strata_fetch.cc.index import (
    DiscoveryResult,
    IndexAttempt,
    IndexBlockedError,
    IndexQueryError,
    IndexUnavailableError,
)
from strata_fetch.cc.sources import make_index
from strata_fetch.jobs import Services, run_cc_job
from strata_fetch.lineage import LineageReport, lineage
from strata_fetch.ratelimit import HostGate
from strata_fetch.surt import normalise_domain

app = typer.Typer(no_args_is_help=True, help="Strata: polite, provenance-first web data sourcing.")
run_app = typer.Typer(no_args_is_help=True, help="Run an acquisition job.")
app.add_typer(run_app, name="run")

DomainOpt = Annotated[str, typer.Option(help="Registered domain, e.g. example.org")]
CrawlOpt = Annotated[str, typer.Option(help="Crawl id such as CC-MAIN-2026-17, or 'latest'")]
LimitOpt = Annotated[int, typer.Option(min=1, max=5000, help="Maximum distinct pages")]
BudgetOpt = Annotated[int, typer.Option(min=1, help="Maximum requests, retries included")]
JsonOpt = Annotated[bool, typer.Option("--json", help="Print JSON instead of text")]
QueryOpt = Annotated[
    bool,
    typer.Option(
        "--include-query-strings",
        help="Keep URLs with a query string (by default /?ref=... style variants are dropped)",
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


def _settings() -> Settings:
    settings = Settings()
    if "YOUR_GITHUB_USER" in settings.user_agent:
        typer.secho(
            "note: set STRATA_USER_AGENT to name your repo URL (ADR-003: an honest User-Agent).",
            fg=typer.colors.YELLOW,
            err=True,
        )
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
    """Create or upgrade the ops schema in Postgres."""
    applied = _run(db.migrate(_settings().ops_dsn))
    typer.echo(f"applied: {', '.join(applied)}" if applied else "ops schema is up to date")


@app.command("lake-init")
def lake_init() -> None:
    """Create the lake's schemas and tables (idempotent)."""
    with Lake.open(_settings()) as lake:
        lake.bootstrap()
        typer.echo(f"lake ready: {lake.count()} bronze capture(s)")


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
    as_json: JsonOpt = False,
) -> None:
    """Find distinct pages for one domain (cached; needs no database)."""
    settings = _settings()

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
            return await source.find(normalise_domain(domain), crawl_id, limit)

    try:
        result = _run(go())
    except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
        typer.secho(f"discovery failed: {exc}", fg=typer.colors.RED, err=True)
        _print_attempts(exc.attempts, err=True)
        raise typer.Exit(1) from exc
    if as_json:
        _print_json(result)
        return
    if result.from_cache:
        typer.echo(f"{len(result.refs)} page(s) from cache (0 requests; found via {result.source})")
    else:
        typer.echo(
            f"{len(result.refs)} distinct page(s) from {result.rows_scanned} index row(s);"
            f" {result.requests} request(s) in {result.elapsed_s:.1f} s"
        )
        if result.fallback_reason:
            typer.echo(f"  {result.fallback_reason}; used the Parquet URL index instead")
        if result.source == "columnar":
            typer.echo(
                f"  URL index: queried {result.shards_queried} of {result.shards_total} Parquet"
                f" file(s); {result.footers_read} footer(s) read this run"
            )
        if any(a.outcome is not Outcome.OK for a in result.attempts):
            _print_attempts(result.attempts)
    for ref in result.refs:
        when = ref.fetch_time.strftime("%Y-%m-%d") if ref.fetch_time else "?"
        typer.echo(f"  {ref.url}\t{when}\t{ref.warc_record_length} B")


async def _run_cc(
    settings: Settings,
    domain: str,
    crawl: str,
    limit: int,
    budget: int,
    include_query_strings: bool = False,
    index: IndexChoice = IndexChoice.AUTO,
) -> RunReport:
    async with db.make_pool(settings.ops_dsn) as pool, _client(settings) as client:
        crawl_id = await resolve_crawl(
            crawl,
            client,
            _index_gate(),
            index_base=settings.cc_index_base,
            cache_dir=settings.cache_dir,
        )
        spec = CrawlSpec(
            domain=normalise_domain(domain),
            crawl=crawl_id,
            limit=limit,
            budget=budget,
            include_query_strings=include_query_strings,
            index=index,
        )
        with Lake.open(settings) as lake:
            lake.bootstrap()
            services = Services(
                settings=settings,
                ops=OpsStore(pool),
                ledger=PostgresLedger(pool),
                lake=lake,
                client=client,
                store=LocalBlobStore(settings.raw_root),
            )
            return await run_cc_job(spec, services)


@run_app.command("cc")
def run_cc(
    domain: DomainOpt,
    crawl: CrawlOpt = "latest",
    limit: LimitOpt = 50,
    budget: BudgetOpt = 60,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.AUTO,
    as_json: JsonOpt = False,
) -> None:
    """Fetch up to LIMIT distinct pages of DOMAIN from Common Crawl into the bronze table."""
    report = _run(_run_cc(_settings(), domain, crawl, limit, budget, include_query_strings, index))
    _print_json(report) if as_json else _print_report(report)
    if report.state is JobState.FAILED:
        raise typer.Exit(1)


@app.command()
def report(job_id: str, as_json: JsonOpt = False) -> None:
    """Summarise a job from the ops database and the lake."""
    settings = _settings()

    async def go() -> JobSummary | None:
        async with db.make_pool(settings.ops_dsn, max_size=2) as pool:
            return await OpsStore(pool).job_summary(job_id)

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
        found = lineage(
            capture_id, lake, LocalBlobStore(settings.raw_root), data_base=settings.cc_data_base
        )
    if found is None:
        typer.secho(f"no capture {capture_id}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1)
    _print_json(found) if as_json else _print_lineage(found)


@app.command()
def demo(
    domain: DomainOpt,
    crawl: CrawlOpt = "latest",
    limit: LimitOpt = 50,
    budget: BudgetOpt = 60,
    include_query_strings: QueryOpt = False,
    index: IndexOpt = IndexChoice.AUTO,
) -> None:
    """Week-1 demo: migrate, prepare the lake, fetch, report, then trace one row."""
    settings = _settings()
    applied = _run(db.migrate(settings.ops_dsn))
    typer.echo(f"1/4 ops schema: {'applied ' + ', '.join(applied) if applied else 'up to date'}")
    with Lake.open(settings) as lake:
        lake.bootstrap()
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
            LocalBlobStore(settings.raw_root),
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
    if r.index_fallback:
        index += f"; {r.index_fallback}"
    typer.echo(
        f"job {r.job_id}: {r.state.value}"
        + (f" ({r.stop_reason})" if r.stop_reason else "")
        + f"\n  {r.domain} in {r.crawl}: {r.refs_found} distinct page(s) ({index})"
        + f"\n  outcomes: {_counts(r.outcomes)}; rows written: {r.rows_written}"
        + f"\n  budget: {r.budget_spent}/{r.budget_cap}; rate-limited attempts: {r.rate_limited}"
        + f"\n  stored {r.bytes_stored} B in {r.duration_s:.1f} s ({r.achieved_rps} req/s)"
        + f"\n  quarantined: {r.quarantined}; recompute digest mismatches: "
        + f"{r.digest_recompute_mismatches}"
        + (f"\n  first capture: {r.first_capture_id}" if r.first_capture_id else "")
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
        + f"\n  budget: {s.budget_spent}/{s.budget_cap}; bytes stored: {s.bytes_stored}"
        + f"\n  achieved rate: {s.achieved_rps} req/s; rows in lake: {s.rows_in_lake}"
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
        f"  policy verdict: {c.policy_verdict.value}; job {c.job_id}; strata {c.strata_version}",
        f"  archived reply: {rep.http_status_line}",
    ]
    lines += [f"    {k}: {v}" for k, v in rep.http_headers[:6]]
    typer.echo("\n".join(lines))


if __name__ == "__main__":
    app()
