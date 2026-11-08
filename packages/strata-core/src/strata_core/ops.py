"""Operational state in Postgres: jobs, policies and the per-attempt log."""

from __future__ import annotations

from collections import Counter
from typing import Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from strata_core.models import Attempt, CrawlSpec, JobState, JobSummary, Outcome, Policy


class AttemptLog(Protocol):
    async def record(self, attempt: Attempt) -> None: ...


class PolicySource(Protocol):
    async def policy(self, host: str) -> Policy | None: ...


class JobStore(AttemptLog, PolicySource, Protocol):
    """Everything a job run needs from operational storage."""

    async def create_job(self, job_id: str, spec: CrawlSpec) -> None: ...

    async def set_state(
        self, job_id: str, state: JobState, stop_reason: str | None = None
    ) -> None: ...

    async def attempt_outcomes(self, job_id: str) -> Counter[Outcome]: ...


class OpsStore:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def create_job(self, job_id: str, spec: CrawlSpec) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "INSERT INTO ops.jobs (job_id, spec, budget_cap) VALUES (%s, %s, %s)",
                (job_id, Jsonb(spec.model_dump(mode="json")), spec.budget),
            )

    async def set_state(self, job_id: str, state: JobState, stop_reason: str | None = None) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE ops.jobs SET state = %s, stop_reason = %s, updated_at = now()"
                " WHERE job_id = %s",
                (state.value, stop_reason, job_id),
            )

    async def job_state(self, job_id: str) -> tuple[JobState, str | None] | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT state, stop_reason FROM ops.jobs WHERE job_id = %s", (job_id,)
            )
            row = await cur.fetchone()
        return None if row is None else (JobState(row[0]), row[1])

    async def policy(self, host: str) -> Policy | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT host, max_rps, burst, max_in_flight, crawl_delay_s, user_agent"
                " FROM ops.policies WHERE host = %s",
                (host,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return Policy(
            host=row[0],
            max_rps=float(row[1]),
            burst=row[2],
            max_in_flight=row[3],
            crawl_delay_s=float(row[4]),
            user_agent=row[5],
        )

    async def record(self, attempt: Attempt) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "INSERT INTO ops.fetch_attempts (job_id, host, url, attempt, started_at,"
                " elapsed_ms, http_status, outcome, retry_after_ms, bytes, error)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    attempt.job_id,
                    attempt.host,
                    attempt.url,
                    attempt.attempt,
                    attempt.started_at,
                    attempt.elapsed_ms,
                    attempt.http_status,
                    attempt.outcome.value,
                    attempt.retry_after_ms,
                    attempt.bytes,
                    attempt.error,
                ),
            )

    async def attempt_outcomes(self, job_id: str) -> Counter[Outcome]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT outcome, count(*) FROM ops.fetch_attempts"
                " WHERE job_id = %s GROUP BY outcome",
                (job_id,),
            )
            return Counter({Outcome(o): n for o, n in await cur.fetchall()})

    async def attempt_outcomes_by_host(self, job_id: str) -> dict[str, dict[Outcome, int]]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT host, outcome, count(*) FROM ops.fetch_attempts"
                " WHERE job_id = %s GROUP BY host, outcome ORDER BY host, outcome",
                (job_id,),
            )
            out: dict[str, dict[Outcome, int]] = {}
            for host, outcome, n in await cur.fetchall():
                out.setdefault(host, {})[Outcome(outcome)] = n
            return out

    async def job_summary(self, job_id: str) -> JobSummary | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT j.state, j.stop_reason, j.spec->>'domain', j.spec->>'crawl', j.budget_cap,"
                " coalesce((SELECT spent FROM ops.budget_ledger b"
                "           WHERE b.job_id = j.job_id AND b.host = '*'), 0),"
                " count(a.attempt_id),"
                " coalesce(sum(a.bytes) FILTER (WHERE a.outcome = 'ok'), 0),"
                " min(a.started_at),"
                " max(a.started_at + make_interval(secs => coalesce(a.elapsed_ms, 0) / 1000.0))"
                " FROM ops.jobs j LEFT JOIN ops.fetch_attempts a ON a.job_id = j.job_id"
                " WHERE j.job_id = %s GROUP BY j.job_id",
                (job_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        first, last = row[8], row[9]
        span = (last - first).total_seconds() if first and last else 0.0
        return JobSummary(
            job_id=job_id,
            state=JobState(row[0]),
            stop_reason=row[1],
            domain=row[2],
            crawl=row[3],
            budget_cap=row[4],
            budget_spent=row[5],
            attempts=row[6],
            attempt_outcomes=dict(await self.attempt_outcomes(job_id)),
            attempts_by_host=await self.attempt_outcomes_by_host(job_id),
            bytes_stored=int(row[7]),
            first_attempt_at=first,
            last_attempt_at=last,
            achieved_rps=round(row[6] / span, 3) if span > 0 else None,
        )


class MemoryAttemptLog:
    """In-process attempt log for unit tests."""

    def __init__(self) -> None:
        self.attempts: list[Attempt] = []

    async def record(self, attempt: Attempt) -> None:
        self.attempts.append(attempt)


class MemoryJobStore(MemoryAttemptLog):
    """In-process JobStore with the same semantics, for unit tests and dry runs."""

    def __init__(self, policies: dict[str, Policy] | None = None) -> None:
        super().__init__()
        self.jobs: dict[str, tuple[CrawlSpec, JobState, str | None]] = {}
        self.policies = dict(policies or {})

    async def create_job(self, job_id: str, spec: CrawlSpec) -> None:
        self.jobs[job_id] = (spec, JobState.CREATED, None)

    async def set_state(self, job_id: str, state: JobState, stop_reason: str | None = None) -> None:
        spec, _, _ = self.jobs[job_id]
        self.jobs[job_id] = (spec, state, stop_reason)

    async def policy(self, host: str) -> Policy | None:
        return self.policies.get(host)

    async def attempt_outcomes(self, job_id: str) -> Counter[Outcome]:
        return Counter(a.outcome for a in self.attempts if a.job_id == job_id)
