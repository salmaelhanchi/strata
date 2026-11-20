"""The work queue in Postgres (ops.work_items, migration 006): one row per page, a state machine
checked by SQL, and leases fenced by their id.

Every change to a leased row names the lease it applies to (``WHERE lease_id = ... AND state =
'leased'``). A lease that expired, or was taken back when its worker disconnected, has already been
cleared, so a late result for it matches no row and changes nothing.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from strata_core.models import Capture, CaptureRef


@dataclass(frozen=True)
class WorkItem:
    """A page to enqueue: its place in the job's order, its id and where it is upstream."""

    seq: int
    capture_id: str
    host: str
    ref: CaptureRef


@dataclass(frozen=True)
class Claim:
    lease_id: str
    capture_id: str
    host: str
    ref: CaptureRef
    attempt: int  # 1 for the first lease of a page


Final = Literal["empty", "failed"]


class WorkQueue(Protocol):
    async def enqueue(self, job_id: str, items: Sequence[WorkItem]) -> int:
        """Add pages not already queued for the job; returns how many were new."""
        ...

    async def reopen(self, job_id: str) -> int:
        """Return every lease of an earlier scheduler, and every failed page, to pending."""
        ...

    async def claim(self, job_id: str, worker_id: str, ttl_s: float) -> Claim | None:
        """Lease the next pending page whose backoff is over, in the job's order."""
        ...

    async def unclaim(self, lease_id: str) -> bool:
        """Undo a claim whose request was never sent (the budget refused it)."""
        ...

    async def fetched(self, lease_id: str, capture: Capture) -> bool:
        """The page's bytes are sealed in the raw store: keep its row until it is in the lake."""
        ...

    async def retry(self, lease_id: str, delay_s: float, error: str | None) -> bool:
        """Back to pending, not before ``delay_s`` from now."""
        ...

    async def finish(self, lease_id: str, state: Final, error: str | None) -> bool: ...

    async def stored(self, job_id: str, capture_ids: Sequence[str]) -> None: ...

    async def fetched_rows(self, job_id: str) -> list[Capture]:
        """Rows fetched but not yet written to the lake (a scheduler died in between)."""
        ...

    async def counts(self, job_id: str) -> dict[str, int]: ...

    async def next_due_s(self, job_id: str) -> float | None:
        """Seconds until the earliest waiting retry is due (0 if one is due now), or None."""
        ...


_CLAIMED = "RETURNING lease_id, capture_id, host, ref, attempts"


class PostgresWorkQueue:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def enqueue(self, job_id: str, items: Sequence[WorkItem]) -> int:
        if not items:
            return 0
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO ops.work_items (job_id, capture_id, seq, url, host, ref)"
                " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (job_id, capture_id) DO NOTHING",
                [
                    (
                        job_id,
                        i.capture_id,
                        i.seq,
                        i.ref.url,
                        i.host,
                        Jsonb(i.ref.model_dump(mode="json")),
                    )
                    for i in items
                ],
            )
            return max(cur.rowcount, 0)

    async def reopen(self, job_id: str) -> int:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "UPDATE ops.work_items SET state = 'pending', lease_id = NULL, worker_id = NULL,"
                " lease_expires_at = NULL, not_before = NULL, updated_at = now(),"
                " attempts = CASE WHEN state = 'failed' THEN 0 ELSE attempts END"
                " WHERE job_id = %s AND state IN ('leased', 'failed')",
                (job_id,),
            )
            return max(cur.rowcount, 0)

    async def claim(self, job_id: str, worker_id: str, ttl_s: float) -> Claim | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "UPDATE ops.work_items w SET state = 'leased', lease_id = %s, worker_id = %s,"
                " lease_expires_at = now() + make_interval(secs => %s), not_before = NULL,"
                " attempts = w.attempts + 1, updated_at = now()"
                " WHERE (w.job_id, w.capture_id) = (SELECT job_id, capture_id FROM ops.work_items"
                "   WHERE job_id = %s AND state = 'pending'"
                "   AND (not_before IS NULL OR not_before <= now())"
                "   ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED) " + _CLAIMED,
                (uuid.uuid4().hex, worker_id, ttl_s, job_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        lease_id, capture_id, host, ref, attempts = row
        return Claim(lease_id, capture_id, host, CaptureRef.model_validate(ref), int(attempts))

    async def unclaim(self, lease_id: str) -> bool:
        return await self._leased("state = 'pending', attempts = attempts - 1", lease_id, ())

    async def fetched(self, lease_id: str, capture: Capture) -> bool:
        return await self._leased(
            "state = 'fetched', capture = %s, error = NULL",
            lease_id,
            (Jsonb(capture.model_dump(mode="json")),),
        )

    async def retry(self, lease_id: str, delay_s: float, error: str | None) -> bool:
        return await self._leased(
            "state = 'pending', not_before = now() + make_interval(secs => %s), error = %s",
            lease_id,
            (delay_s, error),
        )

    async def finish(self, lease_id: str, state: Final, error: str | None) -> bool:
        return await self._leased("state = %s, error = %s", lease_id, (state, error))

    async def _leased(self, assignments: str, lease_id: str, params: tuple[object, ...]) -> bool:
        """Apply ``assignments`` to the row only while it still holds this lease."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"UPDATE ops.work_items SET {assignments}, lease_id = NULL, worker_id = NULL,"
                " lease_expires_at = NULL, updated_at = now()"
                " WHERE lease_id = %s AND state = 'leased'",
                (*params, lease_id),
            )
            return cur.rowcount == 1

    async def stored(self, job_id: str, capture_ids: Sequence[str]) -> None:
        if not capture_ids:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE ops.work_items SET state = 'stored', updated_at = now()"
                " WHERE job_id = %s AND capture_id = ANY(%s) AND state = 'fetched'",
                (job_id, list(capture_ids)),
            )

    async def fetched_rows(self, job_id: str) -> list[Capture]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT capture FROM ops.work_items WHERE job_id = %s AND state = 'fetched'"
                " ORDER BY seq",
                (job_id,),
            )
            return [Capture.model_validate(r[0]) for r in await cur.fetchall()]

    async def counts(self, job_id: str) -> dict[str, int]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT state, count(*) FROM ops.work_items WHERE job_id = %s GROUP BY state",
                (job_id,),
            )
            return {str(s): int(n) for s, n in await cur.fetchall()}

    async def next_due_s(self, job_id: str) -> float | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT greatest(0, extract(epoch FROM min(coalesce(not_before, now())) - now()))"
                " FROM ops.work_items WHERE job_id = %s AND state = 'pending'",
                (job_id,),
            )
            row = await cur.fetchone()
        return None if row is None or row[0] is None else float(row[0])
