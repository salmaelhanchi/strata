"""Pull: a site's pages from Common Crawl into the user's own database, with lineage per row.

A pull is a normal job (budget, robots.txt verdicts, verified records, crash-resume: running the
same pull again continues it), then each selected page is normalized and upserted into the sink.
Pages another job already stored are reused, not fetched again.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from strata_core.models import Capture, CrawlSpec, JobState, RunReport
from strata_core.sinks import PageRow, Sink
from strata_services.pages import NORMALIZER_VERSION, normalize

BATCH = 50


class JobRunner(Protocol):
    async def run(self, spec: CrawlSpec) -> tuple[str, RunReport | None]:
        """Run the job for ``spec`` to its end, or continue it; None if it had succeeded."""
        ...

    async def state(self, job_id: str) -> JobState: ...

    async def selected(self, job_id: str) -> list[str]:
        """The capture ids the job chose (stored by it or reused from earlier jobs)."""
        ...


class CaptureStore(Protocol):
    def captures(self, capture_ids: Sequence[str]) -> list[Capture]: ...

    def record(self, capture: Capture) -> bytes: ...


class PullReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    state: JobState
    run: RunReport | None  # None when the job had already succeeded: nothing was fetched
    selected: int  # pages the job chose
    written: int  # rows upserted into the sink
    with_text: int  # rows where a main text was found
    languages: dict[str, int]
    sink: str  # where the rows went, without credentials
    normalizer_version: str = NORMALIZER_VERSION


class PullService:
    def __init__(self, runner: JobRunner, store: CaptureStore) -> None:
        self._runner = runner
        self._store = store

    async def pull(self, spec: CrawlSpec, sink: Sink, *, sink_name: str) -> PullReport:
        job_id, run = await self._runner.run(spec)
        state = run.state if run is not None else await self._runner.state(job_id)
        selected = await self._runner.selected(job_id)
        rows = await asyncio.to_thread(self._write, selected, sink, job_id)
        return PullReport(
            job_id=job_id,
            state=state,
            run=run,
            selected=len(selected),
            written=len(rows),
            with_text=sum(1 for r in rows if r.main_text),
            languages=dict(Counter(r.lang or "-" for r in rows).most_common()),
            sink=sink_name,
        )

    def _write(self, selected: Sequence[str], sink: Sink, job_id: str) -> list[PageRow]:
        """Normalize and upsert in batches; a page whose bytes the job hasn't stored yet (it
        stopped before reaching it) is simply not there: running the pull again adds it."""
        out: list[PageRow] = []
        for start in range(0, len(selected), BATCH):
            captures = self._store.captures(selected[start : start + BATCH])
            rows = [normalize(c, self._store.record(c), job_id=job_id) for c in captures]
            sink.write_pages(rows)
            out += rows
        return out
