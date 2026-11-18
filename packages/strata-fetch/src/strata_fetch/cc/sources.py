"""Build the index source a job or command asked for (ADR-004)."""

from __future__ import annotations

import asyncio

import httpx

from strata_core.models import IndexChoice
from strata_core.settings import Settings
from strata_fetch.cc.index import (
    AutoIndex,
    CdxIndex,
    ColumnarIndex,
    IndexSource,
    ManifestShards,
    OnAttempt,
    QueryBudget,
    Reserve,
)
from strata_fetch.cc.shardmap import HttpTail, Progress, ShardMap
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.robots import RobotsLocator

# In "auto" mode the CDX server gets one retry before discovery moves to the URL index: its
# failures come in streaks (gateway timeouts under load), so a third try rarely helps.
AUTO_CDX_ATTEMPTS = 2


def make_columnar(
    *,
    client: httpx.AsyncClient,
    settings: Settings,
    data_gate: HostGate,
    sleep: Sleep = asyncio.sleep,
    include_query_strings: bool = False,
    on_attempt: OnAttempt | None = None,
    on_shard_progress: Progress | None = None,
    budget: QueryBudget | None = None,
) -> ColumnarIndex:
    """The Parquet URL index behind a cached shard map.

    Footer reads are paced by the data host's gate but not charged to a job's budget: the shard
    map is built once per crawl and shared by every later job (ADR-004). DuckDB's own requests
    are reported through ``on_attempt`` after each query, and a job charges them (week 2, S10).
    """
    tails = HttpTail(
        client,
        data_gate,
        backoff=settings.backoff_s,
        sleep=sleep,
        timeout_s=settings.http_timeout_s,
        on_attempt=on_attempt,
    )
    return ColumnarIndex(
        ManifestShards(
            client, data_gate, data_base=settings.cc_data_base, cache_dir=settings.cache_dir
        ),
        shard_map=ShardMap(tails, cache_dir=settings.cache_dir, on_progress=on_shard_progress),
        cache_dir=settings.cache_dir,
        gate=data_gate,
        threads=settings.index_threads,
        include_query_strings=include_query_strings,
        user_agent=settings.user_agent,
        on_attempt=on_attempt,
        budget=budget,
    )


def make_index(
    choice: IndexChoice,
    *,
    client: httpx.AsyncClient,
    settings: Settings,
    index_gate: HostGate,
    data_gate: HostGate,
    sleep: Sleep = asyncio.sleep,
    reserve: Reserve | None = None,
    include_query_strings: bool = False,
    on_attempt: OnAttempt | None = None,
    on_shard_progress: Progress | None = None,
    budget: QueryBudget | None = None,
) -> IndexSource:
    def cdx(max_attempts: int | None = None) -> CdxIndex:
        return CdxIndex(
            client,
            index_gate,
            base_url=settings.cc_index_base,
            cache_dir=settings.cache_dir,
            backoff=settings.backoff_s,
            sleep=sleep,
            reserve=reserve,
            include_query_strings=include_query_strings,
            timeout_s=settings.index_timeout_s,
            on_attempt=on_attempt,
            max_attempts=max_attempts,
        )

    def columnar() -> ColumnarIndex:
        return make_columnar(
            client=client,
            settings=settings,
            data_gate=data_gate,
            sleep=sleep,
            include_query_strings=include_query_strings,
            on_attempt=on_attempt,
            on_shard_progress=on_shard_progress,
            budget=budget,
        )

    if choice is IndexChoice.CDX:
        return cdx()
    if choice is IndexChoice.COLUMNAR:
        return columnar()
    return AutoIndex(cdx(max_attempts=AUTO_CDX_ATTEMPTS), columnar())


def make_robots(
    *,
    client: httpx.AsyncClient,
    settings: Settings,
    data_gate: HostGate,
    sleep: Sleep = asyncio.sleep,
    on_attempt: OnAttempt | None = None,
    on_shard_progress: Progress | None = None,
    budget: QueryBudget | None = None,
) -> RobotsLocator:
    """robots.txt captures through the URL index's robotstxt subset: its own 300 files, with its
    own shard map (read once per crawl, like the page-capture map, and exempt from budgets)."""
    tails = HttpTail(
        client,
        data_gate,
        backoff=settings.backoff_s,
        sleep=sleep,
        timeout_s=settings.http_timeout_s,
        on_attempt=on_attempt,
    )
    return RobotsLocator(
        ManifestShards(
            client,
            data_gate,
            data_base=settings.cc_data_base,
            cache_dir=settings.cache_dir,
            subset="robotstxt",
        ),
        shard_map=ShardMap(
            tails, cache_dir=settings.cache_dir, subset="robotstxt", on_progress=on_shard_progress
        ),
        cache_dir=settings.cache_dir,
        gate=data_gate,
        threads=settings.index_threads,
        user_agent=settings.user_agent,
        on_attempt=on_attempt,
        budget=budget,
    )
