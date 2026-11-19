"""How the CLI prints reports: plain text for people, JSON with ``--json``."""

from __future__ import annotations

from pathlib import Path

import typer
from pydantic import BaseModel

from strata_core.models import (
    JobSummary,
    Outcome,
    RunReport,
)
from strata_fetch.cc.attempts import (
    IndexAttempt,
)
from strata_fetch.cc.duckdb_query import DuckDbUsage
from strata_fetch.gate import Gate1Run
from strata_fetch.lineage import LineageReport
from strata_services.changes import Change, ChangeRates, SiteDiff
from strata_services.profile import SiteProfile
from strata_services.pull import PullReport


def shard_progress(done: int, total: int) -> None:
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


def print_json(model: BaseModel) -> None:
    typer.echo(model.model_dump_json(indent=2))


def print_gate1(result: Gate1Run, oracle: Path) -> None:
    if result.run is None:
        typer.echo(f"job {result.job_id} had already finished: proving it again")
    else:
        print_report(result.run)
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


# ------------------------------------------------------------------------------------ printing
def ok(flag: bool | None) -> str:
    return {True: "yes", False: "NO", None: "n/a"}[flag]


def print_attempts(attempts: list[IndexAttempt], *, err: bool = False) -> None:
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


def megabytes(n: int) -> str:
    return f"{n / 1_000_000:.1f} MB"


def duckdb_line(files: int, usage: DuckDbUsage) -> str:
    """e.g. 'DuckDB: 1 file, 23 requests, 61.0 MB in 31.2 s (at most 1 in flight)'."""
    line = (
        f"DuckDB: {files} file(s), {usage.requests} request(s), {megabytes(usage.bytes)}"
        f" in {usage.query_s:.1f} s (at most {usage.peak_in_flight} in flight)"
    )
    if usage.failed:
        line += f"; {usage.failed} request(s) failed"
    return line


def counts(outcomes: dict[Outcome, int]) -> str:
    return ", ".join(f"{k.value}={v}" for k, v in sorted(outcomes.items())) or "none"


def print_report(r: RunReport) -> None:
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
            f" ({megabytes(r.index_duckdb_bytes)}, charged to the budget)"
        )
    if r.index_fallback:
        index += f"; {r.index_fallback}"
    typer.echo(
        f"job {r.job_id}: {r.state.value}"
        + (f" ({r.stop_reason})" if r.stop_reason else "")
        + f"\n  {r.domain} in {r.crawl}: {r.refs_found} distinct page(s) ({index})"
        + f"\n  outcomes: {counts(r.outcomes)}; rows written: {r.rows_written}"
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


def print_summary(s: JobSummary) -> None:
    per_host = "".join(
        f"\n    {host}: {counts(outcomes)}" for host, outcomes in s.attempts_by_host.items()
    )
    typer.echo(
        f"job {s.job_id}: {s.state.value}"
        + (f" ({s.stop_reason})" if s.stop_reason else "")
        + f"\n  {s.domain} in {s.crawl}"
        + f"\n  attempts: {s.attempts} ({counts(s.attempt_outcomes)})"
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


def print_lineage(rep: LineageReport) -> None:
    c = rep.capture
    lines = [
        f"capture {c.capture_id}",
        f"  url:            {c.url}",
        f"  upstream:       {rep.upstream_url} [{rep.upstream_range}]",
        f"  upstream time:  {c.upstream_fetch_time}   fetched by strata: {c.fetched_at} UTC",
        f"  our copy:       {c.raw_uri} @ {c.raw_offset} (+{c.raw_length} B)",
        f"  bytes intact:   {ok(rep.raw_bytes_intact)} (sha256 {c.raw_sha256[:16]}...)",
        f"  digests:        index {rep.index_digest}",
        f"                  warc  {rep.warc_header_digest}"
        f"  -> index matches header: {ok(rep.index_matches_warc_header)}",
        f"                  ours  {rep.recomputed_digest}"
        f"  -> recompute matches: {ok(rep.recompute_matches_header)}",
        f"  policy verdict: {c.policy_verdict.value} (robots.txt capture"
        f" {c.robots_capture_id or 'none'}); job {c.job_id}; strata {c.strata_version}",
        f"  archived reply: {rep.http_status_line}",
    ]
    lines += [f"    {k}: {v}" for k, v in rep.http_headers[:6]]
    typer.echo("\n".join(lines))


def print_profile(p: SiteProfile) -> None:
    c = p.latest
    typer.echo(f"{p.site}: {c.urls:,} page(s) in {c.crawl} ({c.captures:,} captures)")
    for other in p.crawls[1:]:
        typer.echo(f"  {other.crawl}: {other.urls:,} page(s) ({other.captures:,} captures)")
    langs = ", ".join(f"{k} {v:,}" for k, v in list(c.by_language.items())[:4])
    typer.echo(f"  hosts: {len(c.hosts)}; languages: {langs or '-'}")
    typer.echo("  URL shapes (pages, allowed by robots.txt):")
    for s in c.shapes[:10]:
        allowed = "" if s.allowed is None else f", {s.allowed:,} allowed"
        typer.echo(f"    {s.shape:<40} {s.urls:>7,}{allowed}")
    for r in p.robots:
        verdicts = ", ".join(f"{k} {v:,}" for k, v in r.verdicts.items()) or "no pages"
        typer.echo(f"  robots.txt {r.host}: {r.basis}; {verdicts}")
        if r.error:
            typer.echo(f"    {r.error}")
    built = [x for x in p.crawls if not x.from_cache]
    cost = f"{p.requests} request(s)"
    if built:
        cost += f", {megabytes(sum(x.bytes for x in built))} read by DuckDB"
    if p.footer_reads:
        cost += f" (and {p.footer_reads} one-time footer reads for the crawls' shard maps)"
    typer.echo(f"  cost: {cost}; catalogs from cache: {len(p.crawls) - len(built)}/{len(p.crawls)}")


def print_diff(d: SiteDiff, *, show: int = 10) -> None:
    counts = ", ".join(f"{k.value} {v:,}" for k, v in d.counts.items())
    typer.echo(f"{d.site}: {d.from_crawl} -> {d.to_crawl}: {counts}")
    typer.echo(f"  index requests: {d.requests}{_footers(d.footer_reads)}; page fetches: 0")
    typer.echo("  by URL shape (added / removed / changed / same):")
    for s in d.shapes[:12]:
        c = s.counts
        typer.echo(
            f"    {s.shape:<40} {c[Change.ADDED]:>6,} {c[Change.REMOVED]:>6,}"
            f" {c[Change.CHANGED]:>6,} {c[Change.SAME]:>6,}"
        )
    for change in d.changes[:show]:
        typer.echo(f"  {change.change.value:<8} {change.url}")
    if len(d.changes) > show:
        typer.echo(f"  ... {len(d.changes) - show:,} more (--json lists them all)")
    typer.echo(
        "  removed = not captured in the later crawl (Common Crawl doesn't revisit every page)"
    )


def print_rates(r: ChangeRates, *, top: int = 8) -> None:
    rate = r.changed / r.compared if r.compared else 0.0
    typer.echo(
        f"{r.site} over {len(r.crawls)} crawls ({r.crawls[0]} .. {r.crawls[-1]}):"
        f" {r.changed:,} of {r.compared:,} re-captures changed ({rate:.0%})"
    )
    seen = [s for s in r.shapes if s.compared]
    changing = [s for s in seen if s.changed][:top]
    stable = sorted((s for s in seen if not s.changed), key=lambda s: -s.compared)[:top]
    for title, group in (("changing", changing), ("stable", stable)):
        typer.echo(f"  {title} (changed / re-captured, pages seen):")
        for s in group:
            typer.echo(
                f"    {s.shape:<40} {s.rate:>5.0%}  {s.changed:>6,}/{s.compared:<6,} {s.urls:>7,}"
            )
        if not group:
            typer.echo("    none")
    never = sum(1 for s in r.shapes if not s.compared)
    typer.echo(f"  {never} more shape(s) never re-captured in consecutive crawls")
    typer.echo(f"  index requests: {r.requests}{_footers(r.footer_reads)}; page fetches: 0")


def _footers(n: int) -> str:
    return f" (+{n} one-time footer reads for a crawl's shard map)" if n else ""


def print_pull(p: PullReport) -> None:
    if p.run is not None:
        print_report(p.run)
    else:
        typer.echo(f"job {p.job_id}: already succeeded; nothing fetched")
    langs = ", ".join(f"{k} {v:,}" for k, v in list(p.languages.items())[:4]) or "-"
    typer.echo(
        f"pulled {p.written:,} of {p.selected:,} selected page(s) into {p.sink} (table"
        f" strata_pages); main text found for {p.with_text:,}; languages: {langs}"
    )
    typer.echo(f"  normalizer {p.normalizer_version}; `strata lineage CAPTURE_ID` traces any row")
    if p.state.value != "succeeded":
        typer.echo("  the job did not finish: run the same command again to continue it")
