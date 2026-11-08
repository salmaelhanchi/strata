"""The polite WARC record fetcher (S5).

For each capture: reserve budget, wait for the host's rate slot, read exactly one record by byte
range, verify it, append the untouched bytes to our own WARC file, and only then build the bronze
row. Every attempt is logged. Outcomes follow ADR-003:

=================  ==================  ===================================================
Response           Outcome             Action
=================  ==================  ===================================================
206, verified      OK                  store bytes, then the bronze row
503, 429           RATE_LIMITED        pause the whole host; back off (Retry-After wins)
timeout, 5xx       RETRYABLE_ERROR     back off; every retry spends budget
403                BLOCKED             stop the host for this job; a human decides
404, 416, ...      DEFINITIVE_EMPTY    record it; never retry
206, bad bytes     RETRYABLE_ERROR     retry once, then quarantine; no bronze row
=================  ==================  ===================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse

import httpx

from strata_core import __version__
from strata_core.ids import capture_id
from strata_core.ledger import BudgetExhaustedError, Ledger
from strata_core.models import (
    Attempt,
    BlobRef,
    Capture,
    CaptureRef,
    Outcome,
    PolicyVerdict,
    Source,
    Verification,
)
from strata_core.ops import AttemptLog
from strata_fetch.blobstore import RawWarcWriter
from strata_fetch.http import (
    RETRYABLE_EXCEPTIONS,
    backoff_delay,
    classify_status,
    retry_after_seconds,
)
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.warc import VerifiedRecord, verify_record

MAX_VERIFICATION_FAILURES = 2


class HostBlockedError(Exception):
    def __init__(self, host: str, status: int) -> None:
        super().__init__(f"{host} answered HTTP {status}: stopping this host for the job")
        self.host = host
        self.status = status


@dataclass
class FetchContext:
    client: httpx.AsyncClient
    gate: HostGate
    ledger: Ledger
    attempts: AttemptLog
    raw: RawWarcWriter
    quarantine: RawWarcWriter
    job_id: str
    data_base: str = "https://data.commoncrawl.org/"
    backoff: Sequence[float] = (2.0, 10.0, 30.0, 60.0)
    sleep: Sleep = asyncio.sleep
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    version: str = __version__


@dataclass(frozen=True)
class FetchResult:
    ref: CaptureRef
    outcome: Outcome
    attempts: int
    capture: Capture | None = None
    verified: VerifiedRecord | None = None
    quarantined: BlobRef | None = None
    bytes_stored: int = 0
    error: str | None = None


async def _read_capped(resp: httpx.Response, cap: int) -> bytes:
    """Read at most ``cap`` bytes: a server that ignores Range must not stream us a 1 GB file."""
    buf = bytearray()
    async for chunk in resp.aiter_bytes():
        buf.extend(chunk)
        if len(buf) >= cap:
            break
    return bytes(buf[:cap])


async def fetch_capture(ref: CaptureRef, ctx: FetchContext) -> FetchResult:
    url = urljoin(ctx.data_base, ref.warc_filename)
    host = urlparse(url).hostname or ""
    max_attempts = len(ctx.backoff) + 1
    verification_failures = 0
    outcome = Outcome.RETRYABLE_ERROR
    error: str | None = "no attempt made"
    verified: VerifiedRecord | None = None

    for attempt in range(1, max_attempts + 1):
        if not await ctx.ledger.reserve(ctx.job_id, host):
            raise BudgetExhaustedError(ctx.job_id, host)

        started_at, t0 = ctx.now(), time.monotonic()
        status: int | None = None
        body: bytes | None = None
        retry_after: float | None = None
        error = None
        verified = None
        try:
            async with (
                ctx.gate.slot(),
                ctx.client.stream("GET", url, headers={"Range": ref.range_header()}) as resp,
            ):
                status = resp.status_code
                retry_after = retry_after_seconds(resp.headers.get("Retry-After"))
                if status == 206:
                    body = await _read_capped(resp, ref.warc_record_length + 1)
        except RETRYABLE_EXCEPTIONS as exc:
            outcome, error = Outcome.RETRYABLE_ERROR, f"{type(exc).__name__}: {exc}"
        else:
            outcome = classify_status(status) if status is not None else Outcome.RETRYABLE_ERROR
            if outcome is Outcome.OK and body is None:
                outcome, error = Outcome.RETRYABLE_ERROR, f"HTTP {status}: range not honoured"
            elif outcome is not Outcome.OK:
                error = f"HTTP {status}"

        if outcome is Outcome.OK and body is not None:
            verified = verify_record(body, ref)
            if verified.verification is not Verification.OK:
                verification_failures += 1
                outcome = Outcome.RETRYABLE_ERROR
                error = f"verification failed: {verified.verification.value}"
                if verified.detail:
                    error += f" ({verified.detail})"

        await ctx.attempts.record(
            Attempt(
                job_id=ctx.job_id,
                host=host,
                url=ref.url,
                attempt=attempt,
                started_at=started_at,
                elapsed_ms=int((time.monotonic() - t0) * 1000),
                http_status=status,
                outcome=outcome,
                retry_after_ms=int(retry_after * 1000) if retry_after is not None else None,
                bytes=len(body) if body is not None else None,
                error=error,
            )
        )

        if outcome is Outcome.OK and body is not None and verified is not None:
            blob = ctx.raw.append(body)  # durable (fsync) before the row exists
            return FetchResult(
                ref=ref,
                outcome=Outcome.OK,
                attempts=attempt,
                capture=_capture(ref, ctx, blob, body, verified, started_at),
                verified=verified,
                bytes_stored=len(body),
            )
        if verification_failures >= MAX_VERIFICATION_FAILURES and body is not None:
            return FetchResult(
                ref=ref,
                outcome=Outcome.RETRYABLE_ERROR,
                attempts=attempt,
                verified=verified,
                quarantined=ctx.quarantine.append(body),
                error=error,
            )
        if outcome is Outcome.DEFINITIVE_EMPTY:
            return FetchResult(ref=ref, outcome=outcome, attempts=attempt, error=error)
        if outcome is Outcome.BLOCKED:
            raise HostBlockedError(host, status or 0)
        if attempt == max_attempts:
            break
        delay = backoff_delay(attempt, ctx.backoff, retry_after)
        if outcome is Outcome.RATE_LIMITED:
            ctx.gate.pause(delay)  # every task for this host waits, not just this one
        else:
            await ctx.sleep(delay)

    return FetchResult(
        ref=ref, outcome=outcome, attempts=max_attempts, verified=verified, error=error
    )


def _capture(
    ref: CaptureRef,
    ctx: FetchContext,
    blob: BlobRef,
    body: bytes,
    verified: VerifiedRecord,
    fetched_at: datetime,
) -> Capture:
    return Capture(
        capture_id=capture_id(
            Source.COMMON_CRAWL, ref.crawl, ref.warc_filename, ref.warc_record_offset
        ),
        job_id=ctx.job_id,
        source=Source.COMMON_CRAWL,
        crawl=ref.crawl,
        url=ref.url,
        url_surtkey=ref.url_surtkey,
        upstream_fetch_time=ref.fetch_time,
        fetched_at=fetched_at,
        upstream_warc_filename=ref.warc_filename,
        upstream_offset=ref.warc_record_offset,
        upstream_length=ref.warc_record_length,
        content_digest_sha1=ref.content_digest,
        raw_uri=blob.uri,
        raw_offset=blob.offset,
        raw_length=blob.length,
        raw_sha256=hashlib.sha256(body).hexdigest(),
        http_status=verified.http_status,
        mime=ref.mime,
        outcome=Outcome.OK,
        verification=Verification.OK,
        policy_verdict=PolicyVerdict.NOT_EVALUATED,
        strata_version=ctx.version,
    )
