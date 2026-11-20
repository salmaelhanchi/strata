"""What the scheduler and a worker say to each other, as plain values (``wire.py`` maps them to
and from the protobuf messages)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from strata_core.models import CaptureRef, Outcome


@dataclass(frozen=True)
class Grant:
    """One request a worker may send now: budget reserved, rate limiter passed."""

    lease_id: str
    job_id: str
    ref: CaptureRef
    attempt: int
    ttl_s: float
    data_base: str


@dataclass(frozen=True)
class StoredRecord:
    """Where a worker sealed a verified record."""

    raw_uri: str
    raw_offset: int
    raw_length: int
    raw_sha256: str
    http_status: int | None  # the archived response's status


@dataclass(frozen=True)
class Report:
    """A worker's answer to one lease."""

    lease_id: str
    outcome: Outcome
    started_at: datetime
    elapsed_ms: int
    http_status: int | None = None
    retry_after_s: float | None = None
    bytes: int = 0
    error: str | None = None
    stored: StoredRecord | None = None  # set exactly when outcome is OK
