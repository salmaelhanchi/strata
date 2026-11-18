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

    async def reserve_n(self, job_id: str, host: str, n: int) -> bool:
        """Spend ``n`` requests at once, or none: for a query whose cost is estimated up front."""
        ...

    async def refund(self, job_id: str, host: str, n: int) -> int:
        """Give back up to ``n`` reserved requests that were never sent; returns how many."""
        ...

    async def charge(self, job_id: str, host: str, n: int) -> int:
        """Spend ``n`` requests that were already sent, as far as the caps allow.

        For requests nobody could ask about first: DuckDB reads the URL index with its own HTTP
        client, so its requests are counted after the query (ADR-004 as amended in week 2).
        Returns how many did not fit; anything above zero means the job must stop.
        """
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
        return await self.reserve_n(job_id, host, 1)

    async def reserve_n(self, job_id: str, host: str, n: int) -> bool:
        if n <= 0:
            return True
        async with self._pool.connection() as conn, conn.transaction():
            # Lock both rows in a fixed order ('*' sorts first) so two reservations can't deadlock.
            cur = await conn.execute(
                "SELECT host, spent, cap FROM ops.budget_ledger"
                " WHERE job_id = %s AND host = ANY(%s) ORDER BY host FOR UPDATE",
                (job_id, [host, JOB_TOTAL]),
            )
            rows = await cur.fetchall()
            if len(rows) != 2 or any(spent + n > cap for _, spent, cap in rows):
                return False
            await conn.execute(
                "UPDATE ops.budget_ledger SET spent = spent + %s, updated_at = now()"
                " WHERE job_id = %s AND host = ANY(%s)",
                (n, job_id, [host, JOB_TOTAL]),
            )
        return True  # committed: the spend survives a crash of the request that follows

    async def refund(self, job_id: str, host: str, n: int) -> int:
        if n <= 0:
            return 0
        async with self._pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                "SELECT host, spent FROM ops.budget_ledger"
                " WHERE job_id = %s AND host = ANY(%s) ORDER BY host FOR UPDATE",
                (job_id, [host, JOB_TOTAL]),
            )
            rows = await cur.fetchall()
            if len(rows) != 2:
                return 0
            back = min(n, *(int(spent) for _, spent in rows))
            if back > 0:
                await conn.execute(
                    "UPDATE ops.budget_ledger SET spent = spent - %s, updated_at = now()"
                    " WHERE job_id = %s AND host = ANY(%s)",
                    (back, job_id, [host, JOB_TOTAL]),
                )
        return max(back, 0)

    async def charge(self, job_id: str, host: str, n: int) -> int:
        if n <= 0:
            return 0
        async with self._pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                "SELECT host, spent, cap FROM ops.budget_ledger"
                " WHERE job_id = %s AND host = ANY(%s) ORDER BY host FOR UPDATE",
                (job_id, [host, JOB_TOTAL]),
            )
            rows = await cur.fetchall()
            if len(rows) != 2:
                return n
            room: int = min(int(cap) - int(spent) for _, spent, cap in rows)
            take = max(0, min(n, room))
            if take > 0:
                await conn.execute(
                    "UPDATE ops.budget_ledger SET spent = spent + %s, updated_at = now()"
                    " WHERE job_id = %s AND host = ANY(%s)",
                    (take, job_id, [host, JOB_TOTAL]),
                )
        return n - take

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
