"""In-process fakes with the same semantics as the Postgres stores, for unit tests."""

from __future__ import annotations

from strata_core.ledger import JOB_TOTAL
from strata_core.ops import Attempt


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
