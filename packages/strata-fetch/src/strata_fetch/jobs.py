"""Running a Common Crawl job end to end: discover -> budgeted, polite fetch -> raw WARC -> bronze.

These are library functions returning models. The CLI only prints them; in weeks 3-4 the REST and
MCP layers wrap the same calls (``discover_common_crawl``, ``job_start``, ``job_status``).
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from strata_core.ids import new_job_id
from strata_core.lake import Lake
from strata_core.ledger import JOB_TOTAL, BudgetExhaustedError, Ledger
from strata_core.models import Attempt, Capture, CrawlSpec, JobState, Outcome, Policy, RunReport
from strata_core.ops import JobStore
from strata_core.settings import Settings
from strata_fetch.blobstore import BlobStore, RawWarcWriter
from strata_fetch.cc.index import (
    DiscoveryResult,
    IndexAttempt,
    IndexBlockedError,
    IndexQueryError,
    IndexSource,
    IndexUnavailableError,
)
from strata_fetch.cc.sources import make_index
from strata_fetch.fetcher import FetchContext, FetchResult, HostBlockedError, fetch_capture
from strata_fetch.ratelimit import HostGate, Sleep


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


def host_of(url: str) -> str:
    return urlparse(url).hostname or ""


async def policy_for(ops: JobStore, host: str) -> Policy:
    found = await ops.policy(host)
    # A host with no row in ops.policies gets a conservative default: 1 request/s, one at a time.
    return found if found is not None else Policy(host=host, max_rps=1.0, burst=1, max_in_flight=1)


async def run_cc_job(spec: CrawlSpec, svc: Services) -> RunReport:
    settings = svc.settings
    job_id = new_job_id()
    data_host, index_host = host_of(settings.cc_data_base), host_of(settings.cc_index_base)
    started = time.monotonic()

    await svc.ops.create_job(job_id, spec)
    await svc.ledger.open(
        job_id, total_cap=spec.budget, host_caps={data_host: spec.budget, index_host: spec.budget}
    )
    await svc.ops.set_state(job_id, JobState.RUNNING)

    async def reserve(host: str) -> bool:
        return await svc.ledger.reserve(job_id, host)

    async def record_index_attempt(a: IndexAttempt) -> None:
        # Index lookups (CDX queries, URL-index footer reads) land in the same attempt log as WARC
        # reads, so `strata report` explains a slow discovery as well as a slow fetch.
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
                error=a.error,
            )
        )

    data_policy = await policy_for(svc.ops, data_host)
    data_gate = HostGate.from_policy(data_policy, sleep=svc.sleep)
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
    )
    ctx = FetchContext(
        client=svc.client,
        gate=data_gate,
        ledger=svc.ledger,
        attempts=svc.ops,
        raw=RawWarcWriter(svc.store, job_id, rotate_bytes=settings.raw_rotate_bytes),
        quarantine=RawWarcWriter(svc.store, f"quarantine/{job_id}"),
        job_id=job_id,
        data_base=settings.cc_data_base,
        backoff=settings.backoff_s,
        sleep=svc.sleep,
        version=settings.version,
    )

    run = _Run(svc.lake, settings.bronze_batch_size)
    discovery: DiscoveryResult | None = None
    try:
        try:
            discovery = await index.find(spec.domain, spec.crawl, spec.limit)
        except BudgetExhaustedError as exc:
            run.stop(f"budget exhausted during discovery: {exc}")
        except (IndexBlockedError, IndexQueryError, IndexUnavailableError) as exc:
            run.stop(f"discovery failed: {type(exc).__name__}: {exc}", state=JobState.FAILED)
        if discovery is not None and discovery.refs:
            await run.fetch_all(discovery, ctx, workers=data_policy.max_in_flight)
        await run.flush(force=True)
    except BaseException as exc:
        await svc.ops.set_state(job_id, JobState.FAILED, f"{type(exc).__name__}: {exc}"[:500])
        raise

    await svc.ops.set_state(job_id, run.state, run.stop_reason)
    attempts = await svc.ops.attempt_outcomes(job_id)
    usage = await svc.ledger.usage(job_id)
    duration = time.monotonic() - started
    total_attempts = sum(attempts.values())
    return RunReport(
        job_id=job_id,
        state=run.state,
        domain=spec.domain,
        crawl=spec.crawl,
        refs_found=len(discovery.refs) if discovery else 0,
        refs_from_cache=bool(discovery and discovery.from_cache),
        outcomes=dict(Counter(r.outcome for r in run.results)),
        rows_written=run.rows_written,
        budget_cap=spec.budget,
        budget_spent=usage.get(JOB_TOTAL, (0, 0))[0],
        rate_limited=attempts.get(Outcome.RATE_LIMITED, 0),
        bytes_stored=sum(r.bytes_stored for r in run.results),
        duration_s=round(duration, 3),
        achieved_rps=round(total_attempts / duration, 3) if duration > 0 else 0.0,
        quarantined=sum(1 for r in run.results if r.quarantined is not None),
        digest_recompute_mismatches=sum(
            1
            for r in run.results
            if r.verified is not None and r.verified.recompute_matches is False
        ),
        index_attempts=len(discovery.attempts) if discovery else 0,
        index_rows_scanned=discovery.rows_scanned if discovery else 0,
        index_source=discovery.source if discovery else None,
        index_fallback=discovery.fallback_reason if discovery else None,
        stop_reason=run.stop_reason,
        first_capture_id=run.first_capture_id,
    )


class _Run:
    """Mutable state of one job run: results, the bronze buffer and the stop signal."""

    def __init__(self, lake: Lake, batch_size: int) -> None:
        self.lake = lake
        self.batch_size = batch_size
        self.results: list[FetchResult] = []
        self.buffer: list[Capture] = []
        self.rows_written = 0
        self.first_capture_id: str | None = None
        self.state = JobState.SUCCEEDED
        self.stop_reason: str | None = None
        self.stopped = asyncio.Event()
        self._lake_lock = asyncio.Lock()

    def stop(self, reason: str, *, state: JobState = JobState.STOPPED) -> None:
        if self.stop_reason is None:
            self.stop_reason, self.state = reason, state
        self.stopped.set()

    async def fetch_all(
        self, discovery: DiscoveryResult, ctx: FetchContext, *, workers: int
    ) -> None:
        queue: asyncio.Queue[int] = asyncio.Queue()
        for i in range(len(discovery.refs)):
            queue.put_nowait(i)

        async def worker() -> None:
            while not self.stopped.is_set():
                try:
                    i = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    result = await fetch_capture(discovery.refs[i], ctx)
                except BudgetExhaustedError as exc:
                    self.stop(str(exc))
                    return
                except HostBlockedError as exc:
                    self.stop(str(exc))
                    return
                self.results.append(result)
                if result.capture is not None:
                    self.buffer.append(result.capture)
                    if self.first_capture_id is None:
                        self.first_capture_id = result.capture.capture_id
                    await self.flush()

        async with asyncio.TaskGroup() as tg:
            for _ in range(max(1, min(workers, len(discovery.refs)))):
                tg.create_task(worker())

    async def flush(self, *, force: bool = False) -> None:
        async with self._lake_lock:
            if not self.buffer or (len(self.buffer) < self.batch_size and not force):
                return
            batch, self.buffer = self.buffer, []
            self.rows_written += await asyncio.to_thread(self.lake.insert_captures, batch)
