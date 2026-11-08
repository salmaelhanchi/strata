"""The budget ledger: a request is spent, and committed, before it is sent.

This is the harness's ``take_request`` made multi-process. In week 3 the scheduler calls the same
``reserve`` inside its lease transaction, so nothing here is rewritten.
"""

from __future__ import annotations

from typing import Protocol

from psycopg_pool import AsyncConnectionPool

JOB_TOTAL = "*"


class BudgetExhaustedError(Exception):
    """Raised by callers when ``reserve`` refuses; the job stops cleanly."""

    def __init__(self, job_id: str | None, host: str) -> None:
        super().__init__(f"budget exhausted for job {job_id or '?'} (host {host})")
        self.job_id = job_id
        self.host = host


class Ledger(Protocol):
    async def open(self, job_id: str, total_cap: int, host_caps: dict[str, int]) -> None: ...

    async def reserve(self, job_id: str, host: str) -> bool:
        """Spend one request for (job, host) and for the job total, or refuse without spending."""
        ...

    async def usage(self, job_id: str) -> dict[str, tuple[int, int]]:
        """host -> (spent, cap), including the job total under ``"*"``."""
        ...


class PostgresLedger:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def open(self, job_id: str, total_cap: int, host_caps: dict[str, int]) -> None:
        rows = [(job_id, JOB_TOTAL, total_cap)] + [(job_id, h, c) for h, c in host_caps.items()]
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO ops.budget_ledger (job_id, host, cap) VALUES (%s, %s, %s)"
                " ON CONFLICT (job_id, host) DO NOTHING",
                rows,
            )

    async def reserve(self, job_id: str, host: str) -> bool:
        async with self._pool.connection() as conn, conn.transaction():
            # Lock both rows in a fixed order ('*' sorts first) so two reservations can't deadlock.
            cur = await conn.execute(
                "SELECT host, spent, cap FROM ops.budget_ledger"
                " WHERE job_id = %s AND host = ANY(%s) ORDER BY host FOR UPDATE",
                (job_id, [host, JOB_TOTAL]),
            )
            rows = await cur.fetchall()
            if len(rows) != 2 or any(spent >= cap for _, spent, cap in rows):
                return False
            await conn.execute(
                "UPDATE ops.budget_ledger SET spent = spent + 1, updated_at = now()"
                " WHERE job_id = %s AND host = ANY(%s)",
                (job_id, [host, JOB_TOTAL]),
            )
        return True  # committed: the spend survives a crash of the request that follows

    async def usage(self, job_id: str) -> dict[str, tuple[int, int]]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT host, spent, cap FROM ops.budget_ledger WHERE job_id = %s", (job_id,)
            )
            return {host: (spent, cap) for host, spent, cap in await cur.fetchall()}


class MemoryLedger:
    """In-process ledger with the same semantics, for unit tests and dry runs."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], list[int]] = {}

    async def open(self, job_id: str, total_cap: int, host_caps: dict[str, int]) -> None:
        self._rows.setdefault((job_id, JOB_TOTAL), [0, total_cap])
        for host, cap in host_caps.items():
            self._rows.setdefault((job_id, host), [0, cap])

    async def reserve(self, job_id: str, host: str) -> bool:
        keys = [(job_id, JOB_TOTAL), (job_id, host)]
        if any(k not in self._rows for k in keys):
            return False
        if any(self._rows[k][0] >= self._rows[k][1] for k in keys):
            return False
        for k in keys:
            self._rows[k][0] += 1
        return True

    async def usage(self, job_id: str) -> dict[str, tuple[int, int]]:
        return {h: (s, c) for (j, h), (s, c) in self._rows.items() if j == job_id}
