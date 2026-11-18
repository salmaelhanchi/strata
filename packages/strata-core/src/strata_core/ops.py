"""Operational state in Postgres: jobs, policies and the per-attempt log."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from strata_core.models import (
    Attempt,
    CrawlSpec,
    JobRecord,
    JobState,
    JobSummary,
    Outcome,
    Policy,
    PolicyVerdict,
    RequestClient,
)


class AttemptLog(Protocol):
    async def record(self, attempt: Attempt) -> None: ...


class PolicySource(Protocol):
    async def policy(self, host: str) -> Policy | None: ...


class JobStore(AttemptLog, PolicySource, Protocol):
    """Everything a job run needs from operational storage."""

    async def create_job(self, job_id: str, spec: CrawlSpec, storage: dict[str, str]) -> None: ...

    async def set_state(
        self, job_id: str, state: JobState, stop_reason: str | None = None
    ) -> None: ...

    async def attempt_outcomes(self, job_id: str) -> Counter[Outcome]: ...

    async def heartbeat(self, job_id: str) -> None: ...

    async def save_robots(self, job_id: str, robots: RobotsRow) -> None: ...

    async def save_decisions(self, job_id: str, rows: Sequence[DecisionRow]) -> None: ...

    async def charged_attempts(self, job_id: str) -> dict[str, int]:
        """Charged attempts per host: what the ledger's spend should add up to."""
        ...

    async def selected_capture_ids(self, job_id: str) -> list[str]:
        """The captures a job selected: its rows, whichever job stored them first."""
        ...

    async def definitive_urls(self, job_id: str) -> set[str]:
        """URLs whose fetch got a final "nothing here" answer: never retried on resume."""
        ...

    async def job(self, job_id: str) -> JobRecord | None: ...

    async def find_job(self, spec: CrawlSpec, storage: dict[str, str]) -> JobRecord | None:
        """The newest job with exactly this spec and storage: a targets file's job."""
        ...

    async def mark_interrupted(self, stale_after_s: float) -> list[str]:
        """Mark running jobs with a stale heartbeat as interrupted; returns their ids."""
        ...


# (job's robots row) host, basis, robots_capture_id, robots_url, archived_status,
# crawl_delay_s, content_usage, error
RobotsRow = tuple[
    str, str, str | None, str | None, int | None, float | None, Sequence[str], str | None
]
# url, host, verdict, robots capture id, the page's capture id, selected for the job
DecisionRow = tuple[str, str, PolicyVerdict, str | None, str, bool]


class OpsStore:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def create_job(self, job_id: str, spec: CrawlSpec, storage: dict[str, str]) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "INSERT INTO ops.jobs (job_id, spec, budget_cap, storage) VALUES (%s, %s, %s, %s)",
                (job_id, Jsonb(spec.model_dump(mode="json")), spec.budget, Jsonb(storage)),
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
                " elapsed_ms, http_status, outcome, retry_after_ms, bytes, error, client, charged)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
                    attempt.client.value,
                    attempt.charged,
                ),
            )

    async def heartbeat(self, job_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE ops.jobs SET heartbeat_at = now() WHERE job_id = %s", (job_id,)
            )

    async def job(self, job_id: str) -> JobRecord | None:
        return await self._one("WHERE job_id = %s", (job_id,))

    async def find_job(self, spec: CrawlSpec, storage: dict[str, str]) -> JobRecord | None:
        # jsonb equality ignores key order and whitespace: the same request, the same storage.
        return await self._one(
            "WHERE spec = %s AND storage = %s ORDER BY created_at DESC, job_id DESC LIMIT 1",
            (Jsonb(spec.model_dump(mode="json")), Jsonb(storage)),
        )

    async def _one(self, where: str, params: tuple[object, ...]) -> JobRecord | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"SELECT job_id, spec, state, heartbeat_at, storage FROM ops.jobs {where}", params
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return JobRecord(
            job_id=row[0],
            spec=CrawlSpec.model_validate(row[1]),
            state=JobState(row[2]),
            heartbeat_at=row[3],
            storage=row[4],
        )

    async def mark_interrupted(self, stale_after_s: float) -> list[str]:
        """Running jobs whose heartbeat went stale died mid-run: mark them interrupted."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "UPDATE ops.jobs SET state = 'interrupted', updated_at = now(),"
                " stop_reason = coalesce(stop_reason, 'no heartbeat since '"
                " || to_char(heartbeat_at, 'YYYY-MM-DD HH24:MI:SS TZ'))"
                " WHERE state = 'running'"
                " AND coalesce(heartbeat_at, updated_at) < now() - make_interval(secs => %s)"
                " RETURNING job_id",
                (stale_after_s,),
            )
            return [r[0] for r in await cur.fetchall()]

    async def charged_attempts(self, job_id: str) -> dict[str, int]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT host, count(*) FROM ops.fetch_attempts"
                " WHERE job_id = %s AND charged GROUP BY host",
                (job_id,),
            )
            return {str(h): int(n) for h, n in await cur.fetchall()}

    async def definitive_urls(self, job_id: str) -> set[str]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT DISTINCT url FROM ops.fetch_attempts"
                " WHERE job_id = %s AND outcome = 'definitive_empty' AND client = 'strata'",
                (job_id,),
            )
            return {r[0] for r in await cur.fetchall()}

    async def save_robots(self, job_id: str, robots: RobotsRow) -> None:
        host, basis, capture_id, url, status, delay, usage, error = robots
        async with self._pool.connection() as conn:
            await conn.execute(
                "INSERT INTO ops.robots_hosts (job_id, host, basis, robots_capture_id,"
                " robots_url, archived_status, crawl_delay_s, content_usage, error)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                " ON CONFLICT (job_id, host) DO UPDATE SET basis = EXCLUDED.basis,"
                " robots_capture_id = EXCLUDED.robots_capture_id,"
                " robots_url = EXCLUDED.robots_url, archived_status = EXCLUDED.archived_status,"
                " crawl_delay_s = EXCLUDED.crawl_delay_s,"
                " content_usage = EXCLUDED.content_usage, error = EXCLUDED.error,"
                " decided_at = now()",
                (job_id, host, basis, capture_id, url, status, delay, list(usage), error),
            )

    async def save_decisions(self, job_id: str, rows: Sequence[DecisionRow]) -> None:
        if not rows:
            return
        async with self._pool.connection() as conn, conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO ops.policy_decisions (job_id, url, host, verdict, robots_capture_id,"
                " capture_id, selected) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                # A rerun records its own decision and the evidence it rests on.
                " ON CONFLICT (job_id, url) DO UPDATE SET verdict = EXCLUDED.verdict,"
                " robots_capture_id = EXCLUDED.robots_capture_id,"
                " capture_id = EXCLUDED.capture_id, selected = EXCLUDED.selected,"
                " decided_at = now()",
                [
                    (job_id, url, host, v.value, rid, cid, selected)
                    for url, host, v, rid, cid, selected in rows
                ],
            )

    async def recent_jobs(
        self, limit: int = 10
    ) -> list[tuple[str, JobState, str | None, datetime, datetime | None]]:
        """The newest jobs: id, state, stop reason, created, last heartbeat."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT job_id, state, stop_reason, created_at, heartbeat_at FROM ops.jobs"
                " ORDER BY created_at DESC, job_id DESC LIMIT %s",
                (limit,),
            )
            return [(str(j), JobState(s), r, c, h) for j, s, r, c, h in await cur.fetchall()]

    async def selected_capture_ids(self, job_id: str) -> list[str]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT capture_id FROM ops.policy_decisions WHERE job_id = %s AND selected"
                " ORDER BY capture_id",
                (job_id,),
            )
            return [str(r[0]) for r in await cur.fetchall()]

    async def verdicts(self, job_id: str) -> dict[PolicyVerdict, int]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT verdict, count(*) FROM ops.policy_decisions WHERE job_id = %s"
                " GROUP BY verdict",
                (job_id,),
            )
            return {PolicyVerdict(v): n for v, n in await cur.fetchall()}

    async def robots_hosts(self, job_id: str) -> list[dict[str, object]]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT host, basis, robots_capture_id, robots_url, archived_status,"
                " crawl_delay_s, content_usage, error FROM ops.robots_hosts"
                " WHERE job_id = %s ORDER BY host",
                (job_id,),
            )
            names = [d.name for d in cur.description or []]
            return [dict(zip(names, row, strict=True)) for row in await cur.fetchall()]

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

    async def attempts_by_client(self, job_id: str) -> dict[RequestClient, int]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT client, count(*) FROM ops.fetch_attempts"
                " WHERE job_id = %s GROUP BY client ORDER BY client",
                (job_id,),
            )
            return {RequestClient(client): n for client, n in await cur.fetchall()}

    async def job_summary(self, job_id: str) -> JobSummary | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT j.state, j.stop_reason,"
                " coalesce(j.spec->>'domain', (SELECT string_agg(t->>'site', ', ')"
                "   FROM jsonb_array_elements(j.spec->'targets') t)),"
                " j.spec->>'crawl', j.budget_cap,"
                " coalesce((SELECT spent FROM ops.budget_ledger b"
                "           WHERE b.job_id = j.job_id AND b.host = '*'), 0),"
                " count(a.attempt_id),"
                " coalesce(sum(a.bytes) FILTER (WHERE a.outcome = 'ok'), 0),"
                " min(a.started_at),"
                " max(a.started_at + make_interval(secs => coalesce(a.elapsed_ms, 0) / 1000.0)),"
                " j.heartbeat_at"
                " FROM ops.jobs j LEFT JOIN ops.fetch_attempts a ON a.job_id = j.job_id"
                " WHERE j.job_id = %s GROUP BY j.job_id",
                (job_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        first, last = row[8], row[9]
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT max(n) FROM (SELECT count(*) OVER (PARTITION BY host ORDER BY started_at"
                " RANGE BETWEEN INTERVAL '999 milliseconds' PRECEDING AND CURRENT ROW) AS n"
                " FROM ops.fetch_attempts WHERE job_id = %s) w",
                (job_id,),
            )
            peak = await cur.fetchone()
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
            attempts_by_client=await self.attempts_by_client(job_id),
            verdicts=await self.verdicts(job_id),
            heartbeat_at=row[10],
            bytes_stored=int(row[7]),
            first_attempt_at=first,
            last_attempt_at=last,
            peak_requests_per_second=None if peak is None or peak[0] is None else int(peak[0]),
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
        self.storages: dict[str, dict[str, str]] = {}
        self.policies = dict(policies or {})
        self.heartbeats: dict[str, int] = {}
        self.robots: dict[tuple[str, str], RobotsRow] = {}
        self.decisions: dict[tuple[str, str], DecisionRow] = {}

    async def create_job(self, job_id: str, spec: CrawlSpec, storage: dict[str, str]) -> None:
        self.jobs[job_id] = (spec, JobState.CREATED, None)
        self.storages[job_id] = storage

    async def set_state(self, job_id: str, state: JobState, stop_reason: str | None = None) -> None:
        spec, _, _ = self.jobs[job_id]
        self.jobs[job_id] = (spec, state, stop_reason)

    async def policy(self, host: str) -> Policy | None:
        return self.policies.get(host)

    async def attempt_outcomes(self, job_id: str) -> Counter[Outcome]:
        return Counter(a.outcome for a in self.attempts if a.job_id == job_id)

    async def heartbeat(self, job_id: str) -> None:
        self.heartbeats[job_id] = self.heartbeats.get(job_id, 0) + 1

    async def job(self, job_id: str) -> JobRecord | None:
        found = self.jobs.get(job_id)
        if found is None:
            return None
        return JobRecord(
            job_id=job_id,
            spec=found[0],
            state=found[1],
            heartbeat_at=None,
            storage=self.storages.get(job_id),
        )

    async def find_job(self, spec: CrawlSpec, storage: dict[str, str]) -> JobRecord | None:
        ids = [j for j, (s, _, _) in self.jobs.items() if s == spec and self.storages[j] == storage]
        return await self.job(max(ids)) if ids else None  # ids sort by creation time

    async def mark_interrupted(self, stale_after_s: float) -> list[str]:
        return []  # one process: a job that is running here is alive

    async def save_robots(self, job_id: str, robots: RobotsRow) -> None:
        self.robots[(job_id, robots[0])] = robots

    async def save_decisions(self, job_id: str, rows: Sequence[DecisionRow]) -> None:
        for row in rows:
            self.decisions.setdefault((job_id, row[0]), row)

    async def charged_attempts(self, job_id: str) -> dict[str, int]:
        return dict(Counter(a.host for a in self.attempts if a.job_id == job_id and a.charged))

    async def selected_capture_ids(self, job_id: str) -> list[str]:
        return sorted(r[4] for (j, _), r in self.decisions.items() if j == job_id and r[5])

    async def definitive_urls(self, job_id: str) -> set[str]:
        return {
            a.url
            for a in self.attempts
            if a.job_id == job_id
            and a.outcome is Outcome.DEFINITIVE_EMPTY
            and a.client is RequestClient.STRATA
        }
