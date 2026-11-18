"""Core domain models.

Every other contract (lake tables, API and tool schemas) derives from these.

Enum *names* are stable (``Outcome.OK`` <-> a wire format's ``OUTCOME_OK``), so mapping
between Python and any wire format is mechanical. Enum *values* are the lowercase strings
stored in databases.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Source(StrEnum):
    COMMON_CRAWL = "common_crawl"
    OFFICIAL_API = "official_api"  # a publisher's own index or API (for example peps.json)


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


class RequestClient(StrEnum):
    """Who sent a request: Strata's own HTTP client, or DuckDB reading the Parquet URL index."""

    STRATA = "strata"
    DUCKDB = "duckdb"


class JobState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    STOPPED = "stopped"  # budget exhausted or host blocked: a clean, explained stop
    FAILED = "failed"
    INTERRUPTED = "interrupted"  # the process died mid-run (stale heartbeat); `--resume` continues


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
    robots_capture_id: str | None = None  # the robots.txt capture the verdict was based on


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
    client: RequestClient = RequestClient.STRATA
    charged: bool = True  # False for shared reads exempt from budgets (ADR-004: footers)


class IndexChoice(StrEnum):
    """Where discovery looks (ADR-004)."""

    AUTO = "auto"  # the CDX server, then the URL index if the CDX server is unavailable
    CDX = "cdx"  # Common Crawl's CDX server only
    COLUMNAR = "columnar"  # the Parquet URL index only


class Target(_Frozen):
    """One site to take pages from: a host or domain (with subdomains), an optional URL pattern
    (an RE2 regex the whole URL must match) and how many pages to keep."""

    site: str
    pattern: str | None = None
    pages: int = Field(gt=0, le=5_000)


class CrawlSpec(_Frozen):
    """What a job acquires: pages from one or more targets in one crawl, under one budget."""

    source: Source = Source.COMMON_CRAWL
    crawl: str
    budget: int = Field(gt=0, le=20_000)
    targets: tuple[Target, ...] = Field(min_length=1, max_length=20)
    # Pages like /?ref=... are usually tracking variants of one page; keep them only on request.
    include_query_strings: bool = False
    index: IndexChoice = IndexChoice.AUTO
    # ADR-006: pages whose host has no robots.txt capture in the crawl are left out unless set.
    allow_unknown_robots: bool = False

    @property
    def domain(self) -> str:
        return ", ".join(t.site for t in self.targets)

    @property
    def limit(self) -> int:
        return sum(t.pages for t in self.targets)


class TargetReport(_Frozen):
    """What one target of a job produced."""

    site: str
    pattern: str | None
    pages_wanted: int
    candidates: int  # matching pages found in the index
    allowed: int = 0  # candidates whose robots.txt verdict allows fetching
    selected: int = 0  # pages chosen to fetch (at most pages_wanted)
    rows: int = 0  # bronze rows written for this target
    robots: str | None = None  # how robots.txt was decided for the site's host(s)


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
    achieved_rps: float  # this run's page requests per second while fetching pages
    quarantined: int = 0
    digest_recompute_mismatches: int = 0
    index_attempts: int = 0
    index_rows_scanned: int = 0
    index_source: str | None = None  # "cdx" or "columnar": which index answered
    index_fallback: str | None = None  # why discovery left the CDX server, if it did
    index_duckdb_requests: int = 0  # requests DuckDB made reading the URL index (charged)
    index_duckdb_bytes: int = 0
    stop_reason: str | None = None
    first_capture_id: str | None = None
    targets: list[TargetReport] = Field(default_factory=list)
    resumed: bool = False
    recovered_rows: int = 0  # rows rebuilt from the write-ahead manifest on resume
    refunded: int = 0  # reservations returned on resume: reserved, never sent
    # selected pages already in the lake when fetching began (an earlier job, or this job before a
    # crash): not fetched again
    already_stored: int = 0


class JobRecord(BaseModel):
    """A job as ops.jobs holds it."""

    model_config = ConfigDict(frozen=True)

    job_id: str
    spec: CrawlSpec
    state: JobState
    heartbeat_at: datetime | None
    storage: dict[str, str] | None  # None for jobs from before migration 005


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
    attempts_by_client: dict[RequestClient, int] = Field(default_factory=dict)
    verdicts: dict[PolicyVerdict, int] = Field(default_factory=dict)
    heartbeat_at: datetime | None = None
    bytes_stored: int
    first_attempt_at: datetime | None
    last_attempt_at: datetime | None
    # The most requests one host received within any second of the job, all clients and runs:
    # what politeness is judged by. An average over the job would span a crash and its resume.
    peak_requests_per_second: int | None
    rows_in_lake: int | None = None


class Document(_Frozen):
    """One silver row: the fields a page states about the document it describes."""

    capture_id: str
    job_id: str
    url: str
    kind: str  # "pep" or "rfc": which extractor read it
    number: int | None
    title: str | None
    status: str | None
    doc_type: str | None = None
    date: str | None = None  # ISO prefix at the precision the page gives
    date_precision: str | None = None
    authors: list[str] = Field(default_factory=list)
    extractor: str
    extracted_at: datetime
