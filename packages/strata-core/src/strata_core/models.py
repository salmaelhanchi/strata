"""Core domain models.

Every later contract (protobuf, MCP tool schemas, lake tables) derives from these.

Enum *names* match the week-3 protobuf enums (``Outcome.OK`` <-> ``OUTCOME_OK``), so the
mapping between Python and the wire is mechanical. Enum *values* are the lowercase strings
stored in databases.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Source(StrEnum):
    COMMON_CRAWL = "common_crawl"
    WAYBACK = "wayback"
    EPO_OPS = "epo_ops"
    WIKIDATA = "wikidata"
    LIVE = "live"


class Outcome(StrEnum):
    """Result of one fetch. Definitive answers are data; technical failures are retried."""

    OK = "ok"
    DEFINITIVE_EMPTY = "definitive_empty"  # 404/416, "no captures": a final answer, never retried
    RETRYABLE_ERROR = "retryable_error"  # timeouts, 5xx, short reads, failed verification
    RATE_LIMITED = "rate_limited"  # 429 / 503 Slow Down: the whole host pauses
    BLOCKED = "blocked"  # 403 or a challenge: stop the host, tell a human


class PolicyVerdict(StrEnum):
    NOT_EVALUATED = "not_evaluated"
    ALLOWED = "allowed"
    DISALLOWED = "disallowed"
    NEEDS_REVIEW = "needs_review"


class Verification(StrEnum):
    OK = "ok"
    NOT_ONE_RESPONSE_RECORD = "not_one_response_record"
    TARGET_URI_MISMATCH = "target_uri_mismatch"
    DIGEST_MISMATCH = "digest_mismatch"
    LENGTH_MISMATCH = "length_mismatch"
    UNREADABLE = "unreadable"


class JobState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    STOPPED = "stopped"  # budget exhausted or host blocked: a clean, explained stop
    FAILED = "failed"


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CaptureRef(_Frozen):
    """Where an archived capture lives upstream. Both index sources return exactly this."""

    crawl: str
    url: str
    url_surtkey: str | None = None
    fetch_time: datetime | None = None
    warc_filename: str
    warc_record_offset: int = Field(ge=0)
    warc_record_length: int = Field(gt=0)
    content_digest: str | None = None  # SHA-1, base32, as in WARC-Payload-Digest without "sha1:"
    mime: str | None = None
    status: int | None = None

    def range_header(self) -> str:
        last = self.warc_record_offset + self.warc_record_length - 1
        return f"bytes={self.warc_record_offset}-{last}"


class BlobRef(_Frozen):
    """Where we stored a copy of the bytes."""

    uri: str
    offset: int = Field(ge=0)
    length: int = Field(gt=0)


class Capture(_Frozen):
    """One bronze row: the upstream pointer, our raw-copy pointer and the checks that tie them."""

    capture_id: str
    job_id: str
    source: Source
    crawl: str | None
    url: str
    url_surtkey: str | None
    upstream_fetch_time: datetime | None
    fetched_at: datetime
    upstream_warc_filename: str | None
    upstream_offset: int | None
    upstream_length: int | None
    content_digest_sha1: str | None
    raw_uri: str
    raw_offset: int
    raw_length: int
    raw_sha256: str
    http_status: int | None
    mime: str | None
    outcome: Outcome
    verification: Verification
    policy_verdict: PolicyVerdict = PolicyVerdict.NOT_EVALUATED
    strata_version: str


class Policy(_Frozen):
    host: str
    max_rps: float = Field(gt=0)
    burst: int = Field(ge=1)
    max_in_flight: int = Field(ge=1)
    crawl_delay_s: float = Field(ge=0, default=0)
    user_agent: str | None = None


class Attempt(_Frozen):
    """One HTTP attempt, kept in ops.fetch_attempts for reports and block-rate metrics."""

    job_id: str
    host: str
    url: str
    attempt: int = Field(ge=1)
    started_at: datetime
    elapsed_ms: int | None = None
    http_status: int | None = None
    outcome: Outcome
    retry_after_ms: int | None = None
    bytes: int | None = None
    error: str | None = None


class IndexChoice(StrEnum):
    """Where discovery looks (ADR-004)."""

    AUTO = "auto"  # the CDX server, then the URL index if the CDX server is unavailable
    CDX = "cdx"  # Common Crawl's CDX server only
    COLUMNAR = "columnar"  # the Parquet URL index only


class CrawlSpec(_Frozen):
    """The subset of the MCP CrawlSpec that week 1 needs."""

    source: Source = Source.COMMON_CRAWL
    domain: str
    crawl: str
    limit: int = Field(gt=0, le=5_000)
    budget: int = Field(gt=0, le=20_000)
    # Pages like /?ref=... are usually tracking variants of one page; keep them only on request.
    include_query_strings: bool = False
    index: IndexChoice = IndexChoice.AUTO


class RunReport(_Frozen):
    job_id: str
    state: JobState
    domain: str
    crawl: str
    refs_found: int
    refs_from_cache: bool
    outcomes: dict[Outcome, int]
    rows_written: int
    budget_cap: int
    budget_spent: int
    rate_limited: int
    bytes_stored: int
    duration_s: float
    achieved_rps: float
    quarantined: int = 0
    digest_recompute_mismatches: int = 0
    index_attempts: int = 0
    index_rows_scanned: int = 0
    index_source: str | None = None  # "cdx" or "columnar": which index answered
    index_fallback: str | None = None  # why discovery left the CDX server, if it did
    stop_reason: str | None = None
    first_capture_id: str | None = None


class JobSummary(_Frozen):
    """What ``strata report`` shows for a finished or running job, rebuilt from the databases."""

    job_id: str
    state: JobState
    stop_reason: str | None
    domain: str | None
    crawl: str | None
    budget_cap: int
    budget_spent: int
    attempts: int
    attempt_outcomes: dict[Outcome, int]
    attempts_by_host: dict[str, dict[Outcome, int]] = Field(default_factory=dict)
    bytes_stored: int
    first_attempt_at: datetime | None
    last_attempt_at: datetime | None
    achieved_rps: float | None
    rows_in_lake: int | None = None
