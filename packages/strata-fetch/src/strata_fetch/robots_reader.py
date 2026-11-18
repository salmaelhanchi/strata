"""A host's robots.txt for one crawl, read outside a job (``strata profile``).

The same evidence a job uses (ADR-006): the newest robots.txt capture in the crawl's own
robotstxt subset, redirects followed, the WARC record read by byte range and verified. A job
stores that record in the lake; here it is cached on disk next to the lookup, because a released
crawl never changes. Nothing is written to Postgres, so a profile needs no database.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict

from strata_core.ids import capture_id
from strata_core.models import Outcome, Source, Verification
from strata_fetch.cc.attempts import IndexAttempt
from strata_fetch.fetcher import read_record
from strata_fetch.ratelimit import HostGate, Sleep
from strata_fetch.robots import HostRobots, RobotsLocator, host_robots
from strata_fetch.warc import verify_record


class RobotsRead(BaseModel):
    model_config = ConfigDict(frozen=True)

    robots: HostRobots
    index_attempts: list[IndexAttempt] = []
    record_requests: int = 0  # ranged reads of the robots.txt record (0 when cached)

    @property
    def requests(self) -> int:
        return len(self.index_attempts) + self.record_requests


class RobotsReader:
    def __init__(
        self,
        locator: RobotsLocator,
        client: httpx.AsyncClient,
        gate: HostGate,
        *,
        data_base: str,
        backoff: Sequence[float],
        cache_dir: Path | None = None,
        take: Callable[[], bool] = lambda: True,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._locator = locator
        self._client = client
        self._gate = gate
        self._data_base = data_base
        self._backoff = backoff
        self._dir = cache_dir
        self._take = take
        self._sleep = sleep

    async def read(self, host: str, crawl: str) -> RobotsRead:
        lookup, attempts = await self._locator.lookup(host, crawl)
        final = lookup.final
        if lookup.basis != "parsed" or final is None:
            return RobotsRead(
                robots=host_robots(lookup, capture_id=None, record=None), index_attempts=attempts
            )
        ref = final.ref
        cid = capture_id(Source.COMMON_CRAWL, ref.crawl, ref.warc_filename, ref.warc_record_offset)
        cache = None if self._dir is None else self._dir / "robots" / crawl / f"{cid}.warc"
        if cache is not None and cache.exists():
            record = cache.read_bytes()
            if verify_record(record, ref).verification is Verification.OK:
                return RobotsRead(
                    robots=host_robots(lookup, capture_id=cid, record=record),
                    index_attempts=attempts,
                )
        read, tries = await read_record(
            ref,
            self._client,
            self._gate,
            data_base=self._data_base,
            backoff=self._backoff,
            take=self._take,
            sleep=self._sleep,
        )
        if read.outcome is not Outcome.OK or read.body is None:
            robots = host_robots(lookup, capture_id=cid, record=None).model_copy(
                update={"error": f"robots.txt record not read: {read.error}"}
            )
            return RobotsRead(robots=robots, index_attempts=attempts, record_requests=tries)
        if cache is not None:
            cache.parent.mkdir(parents=True, exist_ok=True)
            tmp = cache.with_suffix(".tmp")
            tmp.write_bytes(read.body)
            tmp.replace(cache)
        return RobotsRead(
            robots=host_robots(lookup, capture_id=cid, record=read.body),
            index_attempts=attempts,
            record_requests=tries,
        )
