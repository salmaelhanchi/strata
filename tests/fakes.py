"""In-process fakes with the same semantics as the Postgres stores, for unit tests."""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from strata_core.ledger import JOB_TOTAL
from strata_core.models import Capture
from strata_core.ops import Attempt
from strata_pool.queue import Claim, Final, WorkItem


class MemoryAttemptLog:
    """In-process attempt log."""

    def __init__(self) -> None:
        self.attempts: list[Attempt] = []

    async def record(self, attempt: Attempt) -> None:
        self.attempts.append(attempt)


class MemoryLedger:
    """In-process budget ledger."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], list[int]] = {}

    async def open(self, job_id: str, total_cap: int, host_caps: dict[str, int]) -> None:
        self._rows.setdefault((job_id, JOB_TOTAL), [0, total_cap])
        for host, cap in host_caps.items():
            self._rows.setdefault((job_id, host), [0, cap])

    async def reserve(self, job_id: str, host: str) -> bool:
        return await self.reserve_n(job_id, host, 1)

    async def reserve_n(self, job_id: str, host: str, n: int) -> bool:
        keys = [(job_id, JOB_TOTAL), (job_id, host)]
        if n <= 0:
            return True
        if any(k not in self._rows for k in keys):
            return False
        if any(self._rows[k][0] + n > self._rows[k][1] for k in keys):
            return False
        for k in keys:
            self._rows[k][0] += n
        return True

    async def refund(self, job_id: str, host: str, n: int) -> int:
        keys = [(job_id, JOB_TOTAL), (job_id, host)]
        if n <= 0 or any(k not in self._rows for k in keys):
            return 0
        back = min(n, *(self._rows[k][0] for k in keys))
        for k in keys:
            self._rows[k][0] -= back
        return back

    async def charge(self, job_id: str, host: str, n: int) -> int:
        keys = [(job_id, JOB_TOTAL), (job_id, host)]
        if n <= 0:
            return 0
        if any(k not in self._rows for k in keys):
            return n
        take = max(0, min(n, *(self._rows[k][1] - self._rows[k][0] for k in keys)))
        for k in keys:
            self._rows[k][0] += take
        return n - take

    async def usage(self, job_id: str) -> dict[str, tuple[int, int]]:
        return {h: (s, c) for (j, h), (s, c) in self._rows.items() if j == job_id}


@dataclass
class _Row:
    item: WorkItem
    state: str = "pending"
    attempts: int = 0
    lease_id: str | None = None
    capture: Capture | None = None
    due: float = 0.0  # virtual seconds; retries in these tests are due at once


class MemoryWorkQueue:
    """In-process work queue with ops.work_items' states and lease fencing (one job)."""

    def __init__(self) -> None:
        self.rows: dict[str, _Row] = {}

    async def enqueue(self, job_id: str, items: Sequence[WorkItem]) -> int:
        new = [i for i in items if i.capture_id not in self.rows]
        for i in new:
            self.rows[i.capture_id] = _Row(i)
        return len(new)

    async def reopen(self, job_id: str) -> int:
        n = 0
        for r in self.rows.values():
            if r.state in ("leased", "failed"):
                r.attempts = 0 if r.state == "failed" else r.attempts
                r.state, r.lease_id, n = "pending", None, n + 1
        return n

    async def claim(self, job_id: str, worker_id: str, ttl_s: float) -> Claim | None:
        waiting = sorted(
            (r for r in self.rows.values() if r.state == "pending"), key=lambda r: r.item.seq
        )
        if not waiting:
            return None
        r = waiting[0]
        r.state, r.attempts, r.lease_id = "leased", r.attempts + 1, uuid.uuid4().hex
        return Claim(r.lease_id, r.item.capture_id, r.item.host, r.item.ref, r.attempts)

    def _leased(self, lease_id: str) -> _Row | None:
        return next(
            (r for r in self.rows.values() if r.lease_id == lease_id and r.state == "leased"),
            None,
        )

    async def unclaim(self, lease_id: str) -> bool:
        r = self._leased(lease_id)
        if r is not None:
            r.state, r.lease_id, r.attempts = "pending", None, r.attempts - 1
        return r is not None

    async def fetched(self, lease_id: str, capture: Capture) -> bool:
        r = self._leased(lease_id)
        if r is not None:
            r.state, r.lease_id, r.capture = "fetched", None, capture
        return r is not None

    async def retry(self, lease_id: str, delay_s: float, error: str | None) -> bool:
        r = self._leased(lease_id)
        if r is not None:
            r.state, r.lease_id = "pending", None
        return r is not None

    async def finish(self, lease_id: str, state: Final, error: str | None) -> bool:
        r = self._leased(lease_id)
        if r is not None:
            r.state, r.lease_id = state, None
        return r is not None

    async def stored(self, job_id: str, capture_ids: Sequence[str]) -> None:
        for cid in capture_ids:
            if cid in self.rows and self.rows[cid].state == "fetched":
                self.rows[cid].state = "stored"

    async def fetched_rows(self, job_id: str) -> list[Capture]:
        rows = sorted(self.rows.values(), key=lambda r: r.item.seq)
        return [r.capture for r in rows if r.state == "fetched" and r.capture is not None]

    async def counts(self, job_id: str) -> dict[str, int]:
        return dict(Counter(r.state for r in self.rows.values()))

    async def next_due_s(self, job_id: str) -> float | None:
        return 0.0 if any(r.state == "pending" for r in self.rows.values()) else None
