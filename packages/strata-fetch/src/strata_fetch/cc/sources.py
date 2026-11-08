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
    Reserve,
)
from strata_fetch.cc.shardmap import HttpTail, Progress, ShardMap
from strata_fetch.ratelimit import HostGate, Sleep

# In "auto" mode the CDX server gets one retry before discovery moves to the URL index: its
# failures come in streaks (gateway timeouts under load), so a third try rarely helps.
AUTO_CDX_ATTEMPTS = 2


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
        # Footer reads are paced by the data host's gate but not charged to a job's budget: the
        # shard map is built once per crawl and shared by every later job (ADR-004).
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
            include_query_strings=include_query_strings,
            user_agent=settings.user_agent,
        )

    if choice is IndexChoice.CDX:
        return cdx()
    if choice is IndexChoice.COLUMNAR:
        return columnar()
    return AutoIndex(cdx(max_attempts=AUTO_CDX_ATTEMPTS), columnar())
