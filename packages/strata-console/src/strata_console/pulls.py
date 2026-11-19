"""Pulls started from the console: run in the background, watched over Server-Sent Events.

A pull is a normal job (strata_services.pull), so it is budgeted, robots-checked, verified and
resumable. While it runs, its progress is read back from the ops database once a second, the
same numbers ``strata report`` shows, and published as events. A client that connects late gets
every event so far, then the live ones.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncIterator
from typing import Any, Literal

from pydantic import BaseModel, Field

from strata_console.context import Console
from strata_core.models import CrawlSpec, IndexChoice, Target
from strata_core.sinks import DuckDbSink
from strata_fetch.surt import normalise_domain
from strata_services.runtime import engine, resolve
from strata_services.wiring import pull_service

RunState = Literal["running", "succeeded", "stopped", "failed", "error"]


class PullRequest(BaseModel):
    site: str
    pattern: str | None = None
    limit: int = Field(default=20, ge=1, le=500)
    crawl: str = "latest"
    budget: int = Field(default=150, ge=1, le=5000)


class PullRun(BaseModel):
    run_id: str
    request: PullRequest
    state: RunState = "running"
    job_id: str | None = None
    events: list[dict[str, Any]] = []
    model_config = {"arbitrary_types_allowed": True}


class PullRuns:
    def __init__(self, console: Console) -> None:
        self._console = console
        self._runs: dict[str, PullRun] = {}
        self._changed: dict[str, asyncio.Condition] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    def get(self, run_id: str) -> PullRun | None:
        return self._runs.get(run_id)

    def start(self, request: PullRequest) -> PullRun:
        run = PullRun(run_id="pull_" + secrets.token_hex(6), request=request)
        self._runs[run.run_id] = run
        self._changed[run.run_id] = asyncio.Condition()
        task = asyncio.create_task(self._run(run))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return run

    async def _publish(self, run: PullRun, kind: str, data: dict[str, Any]) -> None:
        run.events.append({"event": kind, "data": data})
        async with self._changed[run.run_id]:
            self._changed[run.run_id].notify_all()

    async def _run(self, run: PullRun) -> None:
        console, req = self._console, run.request
        console.metrics.pulls_running.inc()
        try:
            crawl = await resolve(console.settings, console.client, req.crawl)
            spec = CrawlSpec(
                crawl=crawl,
                budget=req.budget,
                targets=(
                    Target(site=normalise_domain(req.site), pattern=req.pattern, pages=req.limit),
                ),
                index=IndexChoice.COLUMNAR,
            )
            await self._publish(run, "started", {"site": spec.targets[0].site, "crawl": crawl})
            watch = asyncio.create_task(self._watch(run, spec))
            try:
                async with engine(console.settings) as svc:
                    sink = DuckDbSink(console.pages.cursor())
                    report = await pull_service(svc).pull(spec, sink, sink_name="console")
            finally:
                watch.cancel()
            run.job_id = report.job_id
            if report.run is not None:
                console.metrics.upstream_requests.labels("pull").inc(report.run.budget_spent)
            # The last event goes out before the state changes: a stream ends on the state.
            await self._publish(run, "done", json.loads(report.model_dump_json()))
            run.state = report.state.value  # type: ignore[assignment]
        except Exception as exc:  # noqa: BLE001 - the browser must hear about any failure
            await self._publish(run, "error", {"detail": f"{type(exc).__name__}: {exc}"})
            run.state = "error"
        finally:
            console.metrics.pulls_running.dec()
            console.metrics.pulls.labels(run.state).inc()
            async with self._changed[run.run_id]:
                self._changed[run.run_id].notify_all()

    async def _watch(self, run: PullRun, spec: CrawlSpec) -> None:
        """Progress from the ops database, once a second, while the job runs."""
        ops = self._console.ops
        if ops is None:
            return
        storage = self._console.settings.storage()
        while True:
            await asyncio.sleep(1.0)
            found = await ops.find_job(spec, storage)
            if found is None:
                continue
            run.job_id = found.job_id
            summary = await ops.job_summary(found.job_id)
            if summary is None:
                continue
            await self._publish(
                run,
                "progress",
                {
                    "job_id": found.job_id,
                    "state": summary.state.value,
                    "attempts": summary.attempts,
                    "budget_spent": summary.budget_spent,
                    "budget_cap": summary.budget_cap,
                    "outcomes": {k.value: v for k, v in summary.attempt_outcomes.items()},
                    "bytes": summary.bytes_stored,
                },
            )

    async def stream(self, run_id: str) -> AsyncIterator[str]:
        """Every event so far, then each new one, as SSE frames; ends after the last one."""
        run = self._runs[run_id]
        sent = 0
        while True:
            while sent < len(run.events):
                event = run.events[sent]
                sent += 1
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
            if run.state != "running":
                return
            woken = True
            async with self._changed[run_id]:
                # Checked under the lock: an event published after this check wakes the wait.
                if sent == len(run.events) and run.state == "running":
                    try:
                        await asyncio.wait_for(self._changed[run_id].wait(), timeout=15)
                    except TimeoutError:
                        woken = False
            if not woken:
                yield ": keep-alive\n\n"  # proxies drop a stream that stays silent too long
