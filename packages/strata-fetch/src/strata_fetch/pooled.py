"""Pages read by a worker pool instead of this process (ADR-009).

A job still discovers, decides robots.txt verdicts, selects its pages and writes its bronze rows
itself; only the page requests move to the pool. The scheduler behind a ``PagePool`` reserves the
budget and waits for the host's rate limiter before it leases a page, so the rules of ADR-003 hold
however many workers there are. A result that carries a capture has its bytes sealed in the raw
store already (durable before visible); the pool keeps the row until ``stored`` says it is in the
lake, so a job that dies in between writes it when it resumes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from strata_core.models import CaptureRef, Policy
from strata_fetch.fetcher import FetchResult


class PagePool(Protocol):
    def fetch(
        self, job_id: str, refs: Sequence[CaptureRef], policy: Policy
    ) -> AsyncIterator[FetchResult]:
        """Fetch ``refs`` under ``policy`` (the data host's), yielding results as workers report
        them, rows a resumed job had already fetched first. When the job must stop (budget spent,
        host blocked) no further lease is granted, the outstanding ones are settled, and
        ``BudgetExhaustedError`` or ``HostBlockedError`` is raised."""
        ...

    async def stored(self, job_id: str, capture_ids: Sequence[str]) -> None:
        """These rows are in the lake: their pages are done."""
        ...
