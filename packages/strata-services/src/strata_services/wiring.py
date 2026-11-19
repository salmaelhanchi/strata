"""Building services from settings: the one place adapters are chosen, for every interface."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import httpx

from strata_core.lake import Lake
from strata_core.models import BlobRef, Capture, CrawlSpec, JobState, RunReport
from strata_core.settings import Settings
from strata_fetch.blobstore import BlobStore
from strata_fetch.cc.shardmap import Progress
from strata_fetch.cc.sources import make_columnar, make_robots
from strata_fetch.jobs import Services, run_until_done
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.robots_reader import RobotsReader
from strata_services.budget import RequestBudget
from strata_services.changes import ChangeService
from strata_services.profile import SiteProfiler
from strata_services.pull import PullService
from strata_services.runtime import resolve


def site_profiler(
    settings: Settings,
    client: httpx.AsyncClient,
    data_gate: HostGate,
    budget: RequestBudget,
    *,
    robots_hosts: int = 3,
    on_shard_progress: Progress | None = None,
    sleep: Sleep = asyncio.sleep,
) -> SiteProfiler:
    catalogs = make_columnar(
        client=client,
        settings=settings,
        data_gate=data_gate,
        sleep=sleep,
        on_shard_progress=on_shard_progress,
        budget=budget,
    )
    locator = make_robots(
        client=client,
        settings=settings,
        data_gate=data_gate,
        sleep=sleep,
        on_shard_progress=on_shard_progress,
        budget=budget,
    )
    robots = RobotsReader(
        locator,
        client,
        data_gate,
        data_base=settings.cc_data_base,
        backoff=settings.backoff_s,
        cache_dir=settings.cache_dir,
        take=budget.take,
        sleep=sleep,
    )
    return SiteProfiler(catalogs, robots, robots_hosts=robots_hosts)


def change_service(
    settings: Settings,
    client: httpx.AsyncClient,
    data_gate: HostGate,
    budget: RequestBudget,
    *,
    on_shard_progress: Progress | None = None,
    sleep: Sleep = asyncio.sleep,
) -> ChangeService:
    catalogs = make_columnar(
        client=client,
        settings=settings,
        data_gate=data_gate,
        sleep=sleep,
        on_shard_progress=on_shard_progress,
        budget=budget,
    )
    return ChangeService(catalogs)


class EngineRunner:
    """The job engine behind ``PullService``: ``run_until_done`` on an open engine."""

    def __init__(self, svc: Services) -> None:
        self._svc = svc

    async def run(self, spec: CrawlSpec) -> tuple[str, RunReport | None]:
        crawl = await resolve(self._svc.settings, self._svc.client, spec.crawl)
        return await run_until_done(spec.model_copy(update={"crawl": crawl}), self._svc)

    async def state(self, job_id: str) -> JobState:
        found = await self._svc.ops.job(job_id)
        if found is None:
            raise ValueError(f"no job {job_id}")
        return found.state

    async def selected(self, job_id: str) -> list[str]:
        return await self._svc.ops.selected_capture_ids(job_id)


class LakeCaptures:
    """Bronze rows from the lake, their bytes from the raw store."""

    def __init__(self, lake: Lake, store: BlobStore) -> None:
        self._lake = lake
        self._store = store

    def captures(self, capture_ids: Sequence[str]) -> list[Capture]:
        return self._lake.captures_by_ids(capture_ids)

    def record(self, capture: Capture) -> bytes:
        return self._store.read(
            BlobRef(uri=capture.raw_uri, offset=capture.raw_offset, length=capture.raw_length)
        )


def pull_service(svc: Services) -> PullService:
    return PullService(EngineRunner(svc), LakeCaptures(svc.lake, svc.store))
