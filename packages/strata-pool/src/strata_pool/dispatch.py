"""The scheduler's core, without gRPC: which page goes to which worker, and when.

A lease is granted only after, in this order:

1. a free in-flight slot for the data host (the policy's ``max_in_flight``, across all workers),
2. the next pending page, claimed in Postgres under a new lease id,
3. a token from the host's bucket (its rate, and any pause a 429 or 503 started),
4. one request reserved in the budget ledger.

So the politeness rules and the budget hold however many workers connect: adding workers adds
throughput only up to what the host's policy allows. Each result is applied only if its lease is
still current. A lease whose worker disconnects, or that is not answered within its TTL, goes back
to the queue as a failed attempt (its request may have been sent, so its budget stays spent) and
the page is leased again. Rows are keyed by the capture's upstream location, so a page fetched
twice still becomes one row.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import urljoin

from strata_core import logs
from strata_core.ledger import BudgetExhaustedError, Ledger
from strata_core.models import Attempt, BlobRef, CaptureRef, Outcome, Policy
from strata_core.ops import AttemptLog
from strata_fetch.fetcher import FetchResult, HostBlockedError, capture_row
from strata_fetch.http import backoff_delay
from strata_fetch.jobs import host_of, ref_capture_id
from strata_fetch.ratelimit import Clock, HostGate, Sleep
from strata_pool import metrics
from strata_pool.leases import Grant, Report
from strata_pool.queue import Claim, WorkItem, WorkQueue

log = logs.get_logger(__name__)

_MAX_WAIT_S = 1.0  # waiters re-check the queue at least this often


@dataclass
class _Job:
    job_id: str
    host: str
    gate: HostGate
    in_flight: asyncio.Semaphore
    results: asyncio.Queue[FetchResult | None] = field(default_factory=asyncio.Queue)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    stop: Exception | None = None


@dataclass
class _Held:
    job: _Job
    worker_id: str
    claim: Claim
    deadline: float


class Dispatcher:
    """Leases one job's pages to workers. Implements ``strata_fetch.pooled.PagePool``."""

    def __init__(
        self,
        queue: WorkQueue,
        ledger: Ledger,
        attempts: AttemptLog,
        *,
        data_base: str,
        lease_ttl_s: float,
        backoff: Sequence[float],
        version: str,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._queue = queue
        self._ledger = ledger
        self._attempts = attempts
        self._data_base = data_base
        self._ttl = lease_ttl_s
        self._backoff = tuple(backoff)
        self._max_attempts = len(self._backoff) + 1
        self._version = version
        self._clock = clock
        self._sleep = sleep
        self._job: _Job | None = None
        self._leases: dict[str, _Held] = {}
        self._changed = asyncio.Event()
        self._closed = False

    # ------------------------------------------------------------------ the job side (PagePool)
    async def fetch(
        self, job_id: str, refs: Sequence[CaptureRef], policy: Policy
    ) -> AsyncIterator[FetchResult]:
        if self._job is not None:
            raise RuntimeError("the dispatcher runs one job at a time")
        host = host_of(urljoin(self._data_base, "x"))
        by_id = {ref_capture_id(r): r for r in refs}
        reopened = await self._queue.reopen(job_id)
        added = await self._queue.enqueue(
            job_id, [WorkItem(i, cid, host, r) for i, (cid, r) in enumerate(by_id.items())]
        )
        recovered = await self._queue.fetched_rows(job_id)
        # Fetched by an earlier run and already in the lake (the job left them out of ``refs``).
        await self._queue.stored(
            job_id, [c.capture_id for c in recovered if c.capture_id not in by_id]
        )
        log.info("pool.job", job_id=job_id, pages=len(by_id), new=added, reopened=reopened)

        job = _Job(
            job_id,
            host,
            HostGate.from_policy(policy, clock=self._clock, sleep=self._sleep),
            asyncio.Semaphore(policy.max_in_flight),
        )
        for capture in recovered:
            if capture.capture_id in by_id:
                ref = by_id[capture.capture_id]
                job.results.put_nowait(FetchResult(ref, Outcome.OK, 0, capture=capture))
        self._job = job
        self._wake()
        reaper = asyncio.create_task(self._reap(job))
        try:
            await self._settle_if_done(job)
            while (result := await job.results.get()) is not None:
                yield result
        finally:
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper
            self._job = None
            self._wake()
        if job.stop is not None:
            raise job.stop

    async def stored(self, job_id: str, capture_ids: Sequence[str]) -> None:
        await self._queue.stored(job_id, capture_ids)

    def close(self) -> None:
        """No more jobs: waiting workers are told there is nothing left."""
        self._closed = True
        self._wake()

    # ------------------------------------------------------------------ the worker side
    async def lease(self, worker_id: str) -> Grant | None:
        """The next request for this worker, or None once the job has nothing left to lease."""
        seen: _Job | None = None
        while True:
            job = self._job
            if job is None:
                if self._closed or seen is not None:  # closed, or the job we waited on ended
                    return None
                await self._wait()
                continue
            seen = job
            if job.done.is_set():
                return None
            if job.stop is not None:
                # Done once the outstanding leases settle (at once if there are none).
                if not await self._settle_if_done(job):
                    await self._wait()
                continue
            grant = await self._try_grant(job, worker_id)
            if grant is not None:
                return grant

    async def _try_grant(self, job: _Job, worker_id: str) -> Grant | None:
        await job.in_flight.acquire()
        claim: Claim | None = None
        try:
            claim = await self._queue.claim(job.job_id, worker_id, self._ttl)
            if claim is None:
                job.in_flight.release()
                if not await self._settle_if_done(job):
                    due = await self._queue.next_due_s(job.job_id)
                    await self._wait(due)
                return None
            await job.gate.acquire()
            if job.stop is None and not await self._ledger.reserve(job.job_id, claim.host):
                self._stop(job, BudgetExhaustedError(job.job_id, claim.host))
            if job.stop is not None:
                await self._queue.unclaim(claim.lease_id)
                job.in_flight.release()
                return None
        except BaseException:
            if claim is not None:
                with contextlib.suppress(Exception):
                    await self._queue.unclaim(claim.lease_id)
            job.in_flight.release()
            raise
        self._leases[claim.lease_id] = _Held(job, worker_id, claim, self._clock() + self._ttl)
        metrics.LEASES_GRANTED.inc()
        metrics.LEASES_IN_FLIGHT.inc()
        return Grant(
            claim.lease_id, job.job_id, claim.ref, claim.attempt, self._ttl, self._data_base
        )

    async def report(self, worker_id: str, report: Report) -> bool:
        """Apply a worker's result. False if its lease is no longer current: nothing changes."""
        held = self._leases.pop(report.lease_id, None)
        if held is None:
            metrics.RESULTS_REJECTED.inc()
            log.warning("pool.result_rejected", worker=worker_id, lease_id=report.lease_id)
            return False
        job, claim = held.job, held.claim
        self._release(job)
        metrics.RESULTS.labels(report.outcome.value).inc()
        await self._attempts.record(
            Attempt(
                job_id=job.job_id,
                host=job.host,
                url=claim.ref.url,
                attempt=claim.attempt,
                started_at=report.started_at,
                elapsed_ms=report.elapsed_ms,
                http_status=report.http_status,
                outcome=report.outcome,
                retry_after_ms=(
                    None if report.retry_after_s is None else int(report.retry_after_s * 1000)
                ),
                bytes=report.bytes or None,
                error=report.error,
            )
        )
        outcome, stored = report.outcome, report.stored
        if outcome is Outcome.OK and stored is not None:
            capture = capture_row(
                claim.ref,
                job.job_id,
                BlobRef(uri=stored.raw_uri, offset=stored.raw_offset, length=stored.raw_length),
                stored.raw_sha256,
                stored.http_status,
                report.started_at,
                version=self._version,
            )
            if await self._queue.fetched(claim.lease_id, capture):
                await job.results.put(
                    FetchResult(
                        claim.ref,
                        Outcome.OK,
                        claim.attempt,
                        capture=capture,
                        bytes_stored=stored.raw_length,
                    )
                )
        elif outcome is Outcome.DEFINITIVE_EMPTY:
            await self._queue.finish(claim.lease_id, "empty", report.error)
            await job.results.put(
                FetchResult(claim.ref, outcome, claim.attempt, error=report.error)
            )
        elif outcome is Outcome.BLOCKED:
            # Left pending: a human decides, and a resumed job asks again.
            await self._queue.retry(claim.lease_id, 0.0, report.error)
            self._stop(job, HostBlockedError(job.host, report.http_status or 0))
        else:
            delay = backoff_delay(claim.attempt, self._backoff, report.retry_after_s)
            if outcome is Outcome.RATE_LIMITED:
                job.gate.pause(delay)  # the whole host waits, whichever worker asks next
                delay = 0.0
            error = report.error or ("no stored record" if outcome is Outcome.OK else None)
            await self._retry_or_fail(job, claim, outcome, error, delay)
        self._wake()
        await self._settle_if_done(job)
        return True

    async def release(self, lease_ids: Sequence[str], reason: str) -> None:
        """Take back leases whose worker is gone (its stream closed)."""
        for lease_id in lease_ids:
            held = self._leases.pop(lease_id, None)
            if held is not None:
                await self._take_back(held, "disconnected", f"worker {held.worker_id} {reason}")

    # ------------------------------------------------------------------ internals
    async def _take_back(self, held: _Held, kind: str, error: str) -> None:
        job, claim = held.job, held.claim
        self._release(job)
        metrics.LEASES_TAKEN_BACK.labels(kind).inc()
        log.warning("pool.lease_taken_back", lease_id=claim.lease_id, url=claim.ref.url, why=error)
        # Its request may have been sent: the attempt is logged and its budget stays spent.
        await self._attempts.record(
            Attempt(
                job_id=job.job_id,
                host=job.host,
                url=claim.ref.url,
                attempt=claim.attempt,
                started_at=datetime.now(UTC),
                outcome=Outcome.RETRYABLE_ERROR,
                error=error,
            )
        )
        await self._retry_or_fail(job, claim, Outcome.RETRYABLE_ERROR, error, 0.0)
        self._wake()
        await self._settle_if_done(job)

    async def _retry_or_fail(
        self, job: _Job, claim: Claim, outcome: Outcome, error: str | None, delay: float
    ) -> None:
        if claim.attempt >= self._max_attempts:
            await self._queue.finish(claim.lease_id, "failed", error)
            await job.results.put(FetchResult(claim.ref, outcome, claim.attempt, error=error))
        else:
            await self._queue.retry(claim.lease_id, delay, error)

    async def _reap(self, job: _Job) -> None:
        """Take back leases nobody answered within their TTL (a hung or partitioned worker)."""
        while True:
            # Real time between checks; deadlines are on the injected clock.
            await asyncio.sleep(min(max(self._ttl / 4, 0.05), _MAX_WAIT_S))
            now = self._clock()
            for lease_id, held in list(self._leases.items()):
                if held.job is job and held.deadline <= now:
                    del self._leases[lease_id]
                    await self._take_back(
                        held, "expired", f"lease expired: worker {held.worker_id} did not answer"
                    )

    def _release(self, job: _Job) -> None:
        job.in_flight.release()
        metrics.LEASES_IN_FLIGHT.dec()

    def _stop(self, job: _Job, exc: Exception) -> None:
        if job.stop is None:
            job.stop = exc
            log.warning("pool.stopping", job_id=job.job_id, reason=str(exc))
        self._wake()

    async def _settle_if_done(self, job: _Job) -> bool:
        """Finish the job when nothing is pending or leased (or it is stopping and nothing is
        leased): the job side's iterator then ends."""
        if job.done.is_set():
            return True
        outstanding = any(h.job is job for h in self._leases.values())
        if outstanding:
            return False
        if job.stop is None:
            counts = await self._queue.counts(job.job_id)
            if counts.get("pending", 0) or counts.get("leased", 0):
                return False
        job.done.set()
        job.results.put_nowait(None)
        self._wake()
        return True

    def _wake(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()

    async def _wait(self, due_s: float | None = None) -> None:
        """Until something changes, a retry is due (``due_s``), or ``_MAX_WAIT_S`` passes."""
        limit = _MAX_WAIT_S if due_s is None else min(max(due_s, 0.01), _MAX_WAIT_S)
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(limit):
                await self._changed.wait()
