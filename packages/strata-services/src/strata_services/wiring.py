"""Building services from settings: the one place adapters are chosen, for every interface."""

from __future__ import annotations

import asyncio

import httpx

from strata_core.settings import Settings
from strata_fetch.cc.shardmap import Progress
from strata_fetch.cc.sources import make_columnar, make_robots
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.robots_reader import RobotsReader
from strata_services.budget import RequestBudget
from strata_services.changes import ChangeService
from strata_services.profile import SiteProfiler


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
