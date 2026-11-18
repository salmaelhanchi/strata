"""Running a Common Crawl job end to end, and resuming one that died.

    targets --> discovery (site catalogs, ADR-004) --> robots.txt verdicts (ADR-006)
            --> budgeted, polite fetch --> raw part + manifest (write-ahead log)
            --> seal the part into the store --> bronze rows

These are library functions returning models. The CLI only prints them; the console and the MCP
server wrap the same calls.

Crash safety: a record's bytes and its manifest line are fsynced before anything else
happens, a part is sealed into the store before its rows reach the lake, and rows are inserted
idempotently. ``resume_cc_job`` replays what a dead run made durable, refunds reservations that
were never sent, chooses exactly the same pages again (the catalog and robots lookups are cached;
the order is the md5 of the URL) and fetches only what is missing.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from urllib.parse import urlparse

import httpx

from strata_core import logs
from strata_core.ids import capture_id, new_job_id
from strata_core.lake import Lake
from strata_core.ledger import JOB_TOTAL, BudgetExhaustedError, Ledger
from strata_core.models import (
    Attempt,
    BlobRef,
    Capture,
    CaptureRef,
    CrawlSpec,
    JobState,
    Outcome,
    Policy,
    PolicyVerdict,
    RunReport,
    Source,
    Target,
    TargetReport,
)
from strata_core.ops import JobStore
from strata_core.settings import Settings
from strata_fetch.blobstore import BlobStore, PartWriter, recover, seal_part
from strata_fetch.cc.attempts import (
    IndexAttempt,
    IndexBlockedError,
    IndexQueryError,
    IndexUnavailableError,
    OnAttempt,
)
from strata_fetch.cc.columnar import IndexBudgetError
from strata_fetch.cc.index import DiscoveryResult, IndexSource
from strata_fetch.cc.sources import make_index, make_robots
from strata_fetch.crash import arm, crashpoint
from strata_fetch.fetcher import FetchContext, FetchResult, HostBlockedError, fetch_capture
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.robots import HostRobots, RobotsLocator, host_robots

log = logs.get_logger(__name__)


@dataclass
class Services:
    settings: Settings
    ops: JobStore
    ledger: Ledger
    lake: Lake
    client: httpx.AsyncClient
    store: BlobStore
    sleep: Sleep = asyncio.sleep
    index: IndexSource | None = None  # override the index built from spec.index (tests)
    robots: RobotsLocator | None = None  # override the robots.txt locator (tests)


class ResumeRefusedError(Exception):
    """The job can't be resumed: unknown, finished, or still running."""


def host_of(url: str) -> str:
    return urlparse(url).hostname or ""


def ref_capture_id(ref: CaptureRef) -> str:
    """The capture id a fetched ref's bronze row gets: derived from the upstream location."""
    return capture_id(Source.COMMON_CRAWL, ref.crawl, ref.warc_filename, ref.warc_record_offset)


async def policy_for(ops: JobStore, host: str) -> Policy:
    found = await ops.policy(host)
    # A host with no row in ops.policies gets a conservative default: 1 request/s, one at a time.
    return found if found is not None else Policy(host=host, max_rps=1.0, burst=1, max_in_flight=1)


@dataclass
class LedgerBudget:
    """Pays for DuckDB's URL-index requests from the job's budget for the data host: the
    estimate is reserved before the query, the difference charged or refunded after it."""

    ledger: Ledger
    job_id: str
    host: str

    async def reserve(self, n: int) -> bool:
        return await self.ledger.reserve_n(self.job_id, self.host, n)

    async def settle(self, reserved: int, actual: int) -> int:
        if actual > reserved:
            return await self.ledger.charge(self.job_id, self.host, actual - reserved)
        await self.ledger.refund(self.job_id, self.host, reserved - actual)
        return 0


# ------------------------------------------------------------------------------ entry points
async def run_cc_job(spec: CrawlSpec, svc: Services) -> RunReport:
    job_id = new_job_id()
    data_host, index_host = _hosts(svc.settings)
    await svc.ops.create_job(job_id, spec, svc.settings.storage())
    await svc.ledger.open(
        job_id, total_cap=spec.budget, host_caps={data_host: spec.budget, index_host: spec.budget}
    )
    return await _execute(job_id, spec, svc, resumed=False)


async def run_until_done(spec: CrawlSpec, svc: Services) -> tuple[str, RunReport | None]:
    """The job for ``spec`` on this storage, brought to an end: started if there is none,
    resumed if it was interrupted, stopped or failed, left alone if it already succeeded (the
    report is then None). A job whose heartbeat is still fresh is refused: it may be running.

    This is what makes a targets file a single command: run it again after a crash and the same
    job continues. Jobs are matched on their stored spec and storage, nothing else."""
    found = await svc.ops.find_job(spec, svc.settings.storage())
    if found is None:
        report = await run_cc_job(spec, svc)
        return report.job_id, report
    if found.state is JobState.SUCCEEDED:
        return found.job_id, None
    return found.job_id, await resume_cc_job(found.job_id, svc)


def _describe(storage: dict[str, str]) -> str:
    return "; ".join(f"{k} {v}" for k, v in sorted(storage.items()))


async def resume_cc_job(job_id: str, svc: Services, *, force: bool = False) -> RunReport:
    """Continue a job that died or was stopped: replay its manifests, refund reservations that
    were never sent, then fetch only the pages it still lacks."""
    found = await svc.ops.job(job_id)
    if found is None:
        raise ResumeRefusedError(f"no job {job_id}")
    spec, state = found.spec, found.state
    if state is JobState.SUCCEEDED:
        raise ResumeRefusedError(f"job {job_id} already succeeded")
    here = svc.settings.storage()
    if found.storage != here:
        # Its write-ahead log, parts and rows are elsewhere: continuing here would split the job.
        was = _describe(found.storage) if found.storage else "storage not recorded (older job)"
        raise ResumeRefusedError(
            f"job {job_id} keeps its data in {was}; these settings use {_describe(here)}"
        )
    if state is JobState.RUNNING and not force:
        stale = await svc.ops.mark_interrupted(svc.settings.stale_after_s)
        if job_id not in stale:
            raise ResumeRefusedError(
                f"job {job_id} is still running (its heartbeat is recent); use --force only if"
                " you are sure its process is gone"
            )

    rows, dropped = await asyncio.to_thread(recover, svc.settings.raw_root, svc.store, job_id)
    pages = [r.capture for r in rows if r.kind == "page"]
    robots = [r.capture for r in rows if r.kind == "robots"]
    recovered = await asyncio.to_thread(svc.lake.insert_captures, pages)
    await asyncio.to_thread(svc.lake.insert_captures, robots, "robots")

    # A reservation is committed before its request is sent. One whose request never left
    # (the process died in between) shows as spend without a charged attempt: give it back.
    refunded = 0
    charged = await svc.ops.charged_attempts(job_id)
    for host, (spent, _cap) in (await svc.ledger.usage(job_id)).items():
        if host != JOB_TOTAL and spent > charged.get(host, 0):
            refunded += await svc.ledger.refund(job_id, host, spent - charged.get(host, 0))
    return await _execute(
        job_id,
        spec,
        svc,
        resumed=True,
        recovered=recovered,
        refunded=refunded,
        dropped=dropped,
    )


# ------------------------------------------------------------------------------ one run
def _hosts(settings: Settings) -> tuple[str, str]:
    return host_of(settings.cc_data_base), host_of(settings.cc_index_base)


@dataclass
class _Plan:
    target: Target
    discovery: DiscoveryResult | None = None
    selected: list[CaptureRef] = field(default_factory=list)
    allowed: int = 0
    robots_note: str | None = None


async def _execute(
    job_id: str,
    spec: CrawlSpec,
    svc: Services,
    *,
    resumed: bool,
    recovered: int = 0,
    refunded: int = 0,
    dropped: int = 0,
) -> RunReport:
    settings = svc.settings
    arm(settings.crash_at)
    logs.bind(job_id=job_id)
    log.info(
        "job.started",
        crawl=spec.crawl,
        targets=[t.site for t in spec.targets],
        budget=spec.budget,
        resumed=resumed,
        recovered=recovered,
        refunded=refunded,
    )
    data_host, index_host = _hosts(settings)
    started = time.monotonic()
    await svc.ops.set_state(job_id, JobState.RUNNING)
    beat = asyncio.create_task(_heartbeat(svc.ops, job_id, settings.heartbeat_s))

    async def record_index_attempt(a: IndexAttempt) -> None:
        # Index lookups (CDX queries, footer reads, DuckDB's own range requests) land in the same
        # attempt log as WARC reads, so `strata report` explains a slow discovery too.
        await svc.ops.record(
            Attempt(
                job_id=job_id,
                host=host_of(a.url),
                url=a.url,
                attempt=a.attempt,
                started_at=a.started_at,
                elapsed_ms=a.elapsed_ms,
                http_status=a.http_status,
                outcome=a.outcome,
                retry_after_ms=a.retry_after_ms,
                bytes=a.bytes,
                error=a.error,
                client=a.client,
                charged=a.charged,
            )
        )

    data_policy = await policy_for(svc.ops, data_host)
    data_gate = HostGate.from_policy(data_policy, sleep=svc.sleep)
    budget = LedgerBudget(svc.ledger, job_id, data_host)

    async def reserve(host: str) -> bool:
        return await svc.ledger.reserve(job_id, host)

    index = svc.index or make_index(
        spec.index,
        client=svc.client,
        settings=settings,
        index_gate=HostGate.from_policy(await policy_for(svc.ops, index_host), sleep=svc.sleep),
        data_gate=data_gate,
        sleep=svc.sleep,
        reserve=reserve,
        include_query_strings=spec.include_query_strings,
        on_attempt=record_index_attempt,
        budget=budget,
    )
    robots_locator = svc.robots or make_robots(
        client=svc.client,
        settings=settings,
        data_gate=data_gate,
        sleep=svc.sleep,
        on_attempt=record_index_attempt,
        budget=budget,
    )
    writer = PartWriter(
        settings.raw_root, svc.store, job_id, rotate_bytes=settings.raw_rotate_bytes
    )
    quarantine = PartWriter(settings.raw_root, svc.store, f"quarantine/{job_id}")
    ctx = FetchContext(
        client=svc.client,
        gate=data_gate,
        ledger=svc.ledger,
        attempts=svc.ops,
        raw=writer,
        quarantine=quarantine,
        job_id=job_id,
        data_base=settings.cc_data_base,
        backoff=settings.backoff_s,
        sleep=svc.sleep,
        version=settings.version,
    )
    run = _Run(svc.lake, writer, settings.bronze_batch_size)
    plans = [_Plan(t) for t in spec.targets]
    try:
        try:
            await _discover(plans, index, spec.crawl, run)
            if run.stop_reason is None:
                robots = await _robots(plans, robots_locator, ctx, svc, job_id, spec, run)
                if run.stop_reason is None:
                    await _select(plans, robots, spec, svc.ops, job_id)
                    # A page already in the lake (stored by this job before a crash, or by an
                    # earlier job) is not fetched again: its bytes were verified when stored.
                    wanted = [ref_capture_id(r) for p in plans for r in p.selected]
                    stored = svc.lake.existing_ids(wanted)
                    run.already_stored = len(stored)
                    skip = await svc.ops.definitive_urls(job_id) if resumed else set()
                    todo = [
                        r
                        for p in plans
                        for r in p.selected
                        if ref_capture_id(r) not in stored and r.url not in skip
                    ]
                    verdicts = {
                        r.url: (_verdict_of(r, robots), _robots_id(r, robots))
                        for p in plans
                        for r in p.selected
                    }
                    fetch_started = time.monotonic()
                    await run.fetch_all(
                        todo,
                        ctx,
                        workers=data_policy.max_in_flight,
                        decorate=lambda c: c.model_copy(
                            update={
                                "policy_verdict": verdicts[c.url][0],
                                "robots_capture_id": verdicts[c.url][1],
                            }
                        ),
                    )
                    run.fetch_seconds = time.monotonic() - fetch_started
                    short = [
                        f"{p.target.site} {len(p.selected)}/{p.target.pages}"
                        for p in plans
                        if len(p.selected) < p.target.pages
                    ]
                    if short:
                        # Succeeded means every target got the pages it asked for.
                        run.stop(f"fewer pages allowed than asked for: {', '.join(short)}")
        except (BudgetExhaustedError, IndexBudgetError) as exc:
            run.stop(f"budget exhausted during discovery: {exc}")
        except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
            run.stop(f"discovery failed: {type(exc).__name__}: {exc}", state=JobState.FAILED)
        await run.flush(force=True)
        quarantine.rotate(force=True)
        for part in quarantine.take_closed():
            await asyncio.to_thread(seal_part, svc.store, part)
    except BaseException as exc:
        interrupted = isinstance(exc, KeyboardInterrupt | asyncio.CancelledError)
        state = JobState.INTERRUPTED if interrupted else JobState.FAILED
        with contextlib.suppress(Exception):
            await svc.ops.set_state(job_id, state, f"{type(exc).__name__}: {exc}"[:500])
        raise
    finally:
        beat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await beat

    await svc.ops.set_state(job_id, run.state, run.stop_reason)
    report = await _report(job_id, spec, svc, run, plans, started, resumed, recovered, refunded)
    log.info(
        "job.finished",
        state=report.state.value,
        stop_reason=report.stop_reason,
        rows=report.rows_written,
        budget_spent=report.budget_spent,
    )
    return report


async def _heartbeat(ops: JobStore, job_id: str, every: float) -> None:
    while True:
        with contextlib.suppress(Exception):  # a missed beat must not kill the run
            await ops.heartbeat(job_id)
        await asyncio.sleep(every)


async def _discover(plans: Sequence[_Plan], index: IndexSource, crawl: str, run: _Run) -> None:
    for plan in plans:
        t = plan.target
        # Ask for spares: pages robots.txt rules out are replaced by the next in md5 order.
        plan.discovery = await index.find(t.site, crawl, max(4 * t.pages, t.pages + 50), t.pattern)


async def _robots(
    plans: Sequence[_Plan],
    locator: RobotsLocator,
    ctx: FetchContext,
    svc: Services,
    job_id: str,
    spec: CrawlSpec,
    run: _Run,
) -> dict[str, HostRobots]:
    """A verdict basis for every host the candidates live on, decided before any page request.

    A lookup or robots.txt read that fails for a technical reason (throttling, a cut transfer)
    is not a verdict: it stops the job as failed, before any page request, so running the same
    command again asks once more. Only the crawl's own answer becomes a basis.
    """
    hosts = sorted({host_of(r.url) for p in plans if p.discovery for r in p.discovery.refs})
    out: dict[str, HostRobots] = {}
    for host in hosts:
        try:
            lookup, _ = await locator.lookup(host, spec.crawl)
        except (IndexUnavailableError, IndexQueryError) as exc:
            run.stop(
                f"robots.txt lookup failed for {host}: {exc}"[:400] + "; run the command again",
                state=JobState.FAILED,
            )
            return out
        else:
            cid, record = None, None
            final = lookup.final
            if final is not None:
                cid = capture_id(
                    Source.COMMON_CRAWL,
                    final.ref.crawl,
                    final.ref.warc_filename,
                    final.ref.warc_record_offset,
                )
                # A robots.txt capture is shared evidence, like a footer: hosts that redirect
                # to the same file share it within a run, and a capture any job already stored
                # (this one too, before a crash) is read back from the store, not fetched again.
                record = run.robots_bodies.get(cid)
                cap = None if record is not None else svc.lake.get_capture(cid, "robots")
                if cap is not None:
                    record = await asyncio.to_thread(
                        svc.store.read,
                        BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length),
                    )
                elif record is None:
                    result = await fetch_capture(final.ref, ctx)
                    if result.outcome in (Outcome.RETRYABLE_ERROR, Outcome.RATE_LIMITED):
                        run.stop(
                            f"robots.txt for {host} could not be read ({result.outcome.value}:"
                            f" {result.error}); run the command again",
                            state=JobState.FAILED,
                        )
                        return out
                    if result.capture is None or result.body is None:
                        cid = None
                    else:
                        run.add_robots(result.capture, result.body)
                        record = result.body
            out[host] = host_robots(lookup, capture_id=cid, record=record)
        hr = out[host]
        await svc.ops.save_robots(
            job_id,
            (
                host,
                hr.basis,
                hr.robots_capture_id,
                hr.robots_url,
                hr.archived_status,
                hr.crawl_delay_s,
                hr.content_usage,
                hr.error,
            ),
        )
    return out


def _verdict_of(ref: CaptureRef, robots: dict[str, HostRobots]) -> PolicyVerdict:
    hr = robots.get(host_of(ref.url))
    return PolicyVerdict.NEEDS_REVIEW if hr is None else hr.verdict(ref.url)


def _robots_id(ref: CaptureRef, robots: dict[str, HostRobots]) -> str | None:
    hr = robots.get(host_of(ref.url))
    return None if hr is None else hr.robots_capture_id


async def _select(
    plans: Sequence[_Plan],
    robots: dict[str, HostRobots],
    spec: CrawlSpec,
    ops: JobStore,
    job_id: str,
) -> None:
    """Keep each target's first ``pages`` candidates (md5 order) that robots.txt allows."""
    ok = {PolicyVerdict.ALLOWED}
    if spec.allow_unknown_robots:
        ok.add(PolicyVerdict.NEEDS_REVIEW)
    for plan in plans:
        refs = plan.discovery.refs if plan.discovery else []
        decisions = []
        for ref in refs:
            verdict = _verdict_of(ref, robots)
            chosen = verdict in ok and len(plan.selected) < plan.target.pages
            if verdict in ok:
                plan.allowed += 1
            if chosen:
                plan.selected.append(ref)
            decisions.append(
                (
                    ref.url,
                    host_of(ref.url),
                    verdict,
                    _robots_id(ref, robots),
                    ref_capture_id(ref),
                    chosen,
                )
            )
        await ops.save_decisions(job_id, decisions)
        bases = Counter(robots[h].basis for h in {host_of(r.url) for r in refs} if h in robots)
        plan.robots_note = ", ".join(f"{b}: {n} host(s)" for b, n in sorted(bases.items()))


async def _report(
    job_id: str,
    spec: CrawlSpec,
    svc: Services,
    run: _Run,
    plans: Sequence[_Plan],
    started: float,
    resumed: bool,
    recovered: int,
    refunded: int,
) -> RunReport:
    attempts = await svc.ops.attempt_outcomes(job_id)
    usage = await svc.ledger.usage(job_id)
    duration = time.monotonic() - started
    # This run's page requests over this run's fetching time: the pace the data host saw. Not
    # the job's attempts (an earlier run's, index and footer reads) over this run's duration.
    page_requests = sum(r.attempts for r in run.results)
    found = [p.discovery for p in plans if p.discovery is not None]
    in_lake = svc.lake.existing_ids([ref_capture_id(r) for p in plans for r in p.selected])
    first = run.first_capture_id or next(iter(sorted(in_lake)), None)
    duck = [d.duckdb for d in found if d.duckdb is not None]
    return RunReport(
        job_id=job_id,
        state=run.state,
        domain=spec.domain,
        crawl=spec.crawl,
        refs_found=sum(len(p.selected) for p in plans),
        refs_from_cache=bool(found) and all(d.from_cache for d in found),
        outcomes=dict(Counter(r.outcome for r in run.results)),
        rows_written=run.rows_written,
        budget_cap=spec.budget,
        budget_spent=usage.get(JOB_TOTAL, (0, 0))[0],
        rate_limited=attempts.get(Outcome.RATE_LIMITED, 0),
        bytes_stored=sum(r.bytes_stored for r in run.results),
        duration_s=round(duration, 3),
        achieved_rps=(
            round(page_requests / run.fetch_seconds, 3) if run.fetch_seconds > 0 else 0.0
        ),
        quarantined=sum(1 for r in run.results if r.quarantined is not None),
        digest_recompute_mismatches=sum(
            1
            for r in run.results
            if r.verified is not None and r.verified.recompute_matches is False
        ),
        index_attempts=sum(len(d.attempts) for d in found),
        index_rows_scanned=sum(d.rows_scanned for d in found),
        index_source=found[0].source if found else None,
        index_fallback=next((d.fallback_reason for d in found if d.fallback_reason), None),
        index_duckdb_requests=sum(u.requests for u in duck),
        index_duckdb_bytes=sum(u.bytes for u in duck),
        stop_reason=run.stop_reason,
        first_capture_id=first,
        targets=[
            TargetReport(
                site=p.target.site,
                pattern=p.target.pattern,
                pages_wanted=p.target.pages,
                candidates=(p.discovery.candidates or len(p.discovery.refs)) if p.discovery else 0,
                allowed=p.allowed,
                selected=len(p.selected),
                rows=sum(1 for r in p.selected if ref_capture_id(r) in in_lake),
                robots=p.robots_note,
            )
            for p in plans
        ],
        resumed=resumed,
        recovered_rows=recovered,
        refunded=refunded,
        already_stored=run.already_stored,
    )


class _Run:
    """Mutable state of one run: results, the bronze buffers and the stop signal."""

    def __init__(self, lake: Lake, writer: PartWriter, batch_size: int) -> None:
        self.lake = lake
        self.writer = writer
        self.batch_size = batch_size
        self.results: list[FetchResult] = []
        self.buffer: list[Capture] = []
        self.robots_buffer: list[Capture] = []
        self.robots_bodies: dict[str, bytes] = {}
        self.rows_written = 0
        self.already_stored = 0
        self.fetch_seconds = 0.0  # time spent fetching pages in this run (not discovery)
        self.first_capture_id: str | None = None
        self.state = JobState.SUCCEEDED
        self.stop_reason: str | None = None
        self.stopped = asyncio.Event()
        self._lake_lock = asyncio.Lock()

    def stop(self, reason: str, *, state: JobState = JobState.STOPPED) -> None:
        if self.stop_reason is None:
            self.stop_reason, self.state = reason, state
        self.stopped.set()

    def add_robots(self, capture: Capture, body: bytes) -> None:
        self.writer.log("robots", capture)
        self.robots_buffer.append(capture)
        self.robots_bodies[capture.capture_id] = body

    async def fetch_all(
        self,
        refs: Sequence[CaptureRef],
        ctx: FetchContext,
        *,
        workers: int,
        decorate: Callable[[Capture], Capture] = lambda c: c,
    ) -> None:
        queue: asyncio.Queue[CaptureRef] = asyncio.Queue()
        for ref in refs:
            queue.put_nowait(ref)

        async def worker() -> None:
            while not self.stopped.is_set():
                try:
                    ref = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    result = await fetch_capture(ref, ctx)
                except (BudgetExhaustedError, HostBlockedError) as exc:
                    self.stop(str(exc))
                    return
                # No await from the append inside fetch_capture to here: the record, its
                # manifest line and its place in the buffer can't be split by a flush.
                if result.capture is not None:
                    capture = decorate(result.capture)
                    self.writer.log("page", capture)
                    self.buffer.append(capture)
                    if self.first_capture_id is None:
                        self.first_capture_id = capture.capture_id
                self.results.append(replace(result, body=None))
                await self.flush()

        async with asyncio.TaskGroup() as tg:
            for _ in range(max(1, min(workers, len(refs)))):
                tg.create_task(worker())

    async def flush(self, *, force: bool = False) -> None:
        """Seal the parts holding the buffered rows, then write the rows: never the reverse."""
        async with self._lake_lock:
            if not force and len(self.buffer) < self.batch_size:
                return
            # Synchronous snapshot: every buffered row's bytes are in a part closed here.
            self.writer.rotate()
            parts = self.writer.take_closed()
            pages, self.buffer = self.buffer, []
            robots, self.robots_buffer = self.robots_buffer, []
            for part in parts:
                await asyncio.to_thread(seal_part, self.writer.store, part)
            if robots:
                await asyncio.to_thread(self.lake.insert_captures, robots, "robots")
            if pages:
                self.rows_written += await asyncio.to_thread(self.lake.insert_captures, pages)
            if pages or robots:
                crashpoint("after_insert")


# Re-exported for callers that build their own index (the CLI's discover and count).
__all__ = [
    "LedgerBudget",
    "OnAttempt",
    "ResumeRefusedError",
    "Services",
    "ref_capture_id",
    "resume_cc_job",
    "run_cc_job",
    "run_until_done",
]
