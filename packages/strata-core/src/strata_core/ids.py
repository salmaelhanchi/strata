"""Deterministic identifiers.

The same upstream capture always gets the same id, so reruns are idempotent.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime

from strata_core.models import Source

_SEP = "\x1f"  # unit separator: cannot appear in URLs or WARC file names


def capture_id(source: Source, crawl: str | None, warc_filename: str, offset: int) -> str:
    """SHA-256 over the upstream location of a capture, hex-encoded."""
    if offset < 0:
        raise ValueError("offset must be >= 0")
    key = _SEP.join([source.value, crawl or "", warc_filename, str(offset)])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def new_job_id(now: datetime | None = None) -> str:
    """Time-sortable, collision-resistant job id, e.g. job_20261107T171500Z_3f9a1c2b."""
    ts = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"job_{ts}_{secrets.token_hex(4)}"
