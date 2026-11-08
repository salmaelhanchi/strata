"""HTTP outcome rules shared by every connector (ADR-003)."""

from __future__ import annotations

import random
from collections.abc import Sequence
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from strata_core.models import Outcome

RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
)


def classify_status(status: int) -> Outcome:
    """Map an HTTP status to an outcome. 206 is the only success for a byte-range read."""
    if status in (200, 206):
        return Outcome.OK
    if status in (429, 503):
        return Outcome.RATE_LIMITED
    if status == 403:
        return Outcome.BLOCKED
    if status >= 500 or status == 408:
        return Outcome.RETRYABLE_ERROR
    return Outcome.DEFINITIVE_EMPTY  # 404, 410, 416 and other client errors: don't retry


def retry_after_seconds(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After as delta-seconds or an HTTP date; None when absent or unparsable."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - (now or datetime.now(UTC))).total_seconds())


def backoff_delay(
    attempt: int,
    schedule: Sequence[float],
    retry_after: float | None = None,
    *,
    rng: random.Random | None = None,
) -> float:
    """Delay before retry number ``attempt`` (1-based): the schedule step plus up to 25% jitter,
    never shorter than Retry-After. Jitter only lengthens, so the floor of the schedule holds."""
    base = schedule[min(attempt, len(schedule)) - 1] if schedule else 0.0
    jitter = (rng or random).uniform(0.0, 0.25) * base
    return max(base + jitter, retry_after or 0.0)
