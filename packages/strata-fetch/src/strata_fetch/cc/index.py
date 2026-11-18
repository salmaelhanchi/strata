"""Finding captures in Common Crawl: the protocol every index source answers (ADR-004).

* ``cdx.CdxIndex``: Common Crawl's CDX server. One domain in one request, but often overloaded.
* ``columnar.ColumnarIndex``: DuckDB over the Parquet URL index, through a cached site catalog.
* ``AutoIndex``: the CDX server first, the URL index when the CDX server is unavailable.

All three return identical ``CaptureRef``s.
"""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict

from strata_core.models import CaptureRef
from strata_fetch.cc.attempts import (
    IndexAttempt,
    IndexLookupError,
    IndexUnavailableError,
    summarise_attempts,
)
from strata_fetch.cc.duckdb_query import DuckDbUsage


class DiscoveryResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    refs: list[CaptureRef]
    from_cache: bool
    requests: int  # HTTP requests this lookup made: ours and, for the URL index, DuckDB's
    source: str
    rows_scanned: int = 0  # index rows read before removing duplicates and variants
    attempts: list[IndexAttempt] = []
    elapsed_s: float = 0.0
    fallback_reason: str | None = None  # set when AutoIndex had to leave the CDX server
    shards_total: int = 0  # URL index: Parquet files in the crawl
    shards_queried: int = 0  # URL index: files whose SURT range covers the domain
    footers_read: int = 0  # URL index: Parquet footers read to build or extend the shard map
    duckdb: DuckDbUsage | None = None  # URL index: DuckDB's own requests for this lookup
    candidates: int | None = None  # pages matching before the limit, when the source knows


class IndexSource(Protocol):
    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult: ...


class AutoIndex:
    """The CDX server first; the URL index when the CDX server is unavailable (ADR-004).

    Only "unavailable" (timeouts, 5xx after retries) falls back. A 403 means stop and ask a human
    (ADR-003), and a malformed query is our bug: both are raised, not routed around.
    """

    source = "auto"

    def __init__(self, primary: IndexSource, fallback: IndexSource) -> None:
        self._primary = primary
        self._fallback = fallback

    async def find(
        self, domain: str, crawl: str, limit: int, pattern: str | None = None
    ) -> DiscoveryResult:
        try:
            return await self._primary.find(domain, crawl, limit, pattern)
        except IndexUnavailableError as exc:
            reason = f"CDX server unavailable ({summarise_attempts(exc.attempts)})"
            try:
                result = await self._fallback.find(domain, crawl, limit, pattern)
            except IndexLookupError as fallback_exc:
                raise type(fallback_exc)(
                    f"{fallback_exc} (tried after: {reason})",
                    [*exc.attempts, *fallback_exc.attempts],
                ) from fallback_exc
            return result.model_copy(
                update={
                    "fallback_reason": reason,
                    "attempts": [*exc.attempts, *result.attempts],
                    "requests": len(exc.attempts) + result.requests,
                }
            )
