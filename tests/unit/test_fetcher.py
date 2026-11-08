"""The polite fetcher against a fake Common Crawl: every row of the ADR-003 outcome table."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from pytest_httpserver import HTTPServer

from strata_core.ids import capture_id
from strata_core.ledger import JOB_TOTAL, BudgetExhaustedError, MemoryLedger
from strata_core.models import BlobRef, Outcome, Source, Verification
from strata_core.ops import MemoryAttemptLog
from strata_fetch.blobstore import LocalBlobStore, RawWarcWriter
from strata_fetch.fetcher import FetchContext, HostBlockedError, fetch_capture
from strata_fetch.ratelimit import HostGate
from strata_fetch.warc import verify_record
from tests.fakecc import FakeCommonCrawl, FakeWarc, build_warc

PAGES = [(f"https://example.org/p{i}", f"<html>page {i}</html>".encode() * 20) for i in range(4)]
JOB = "job_test"


class SpyGate(HostGate):
    def __init__(self) -> None:
        super().__init__(rate=1000, burst=10, max_in_flight=2)
        self.pauses: list[float] = []

    def pause(self, seconds: float) -> None:
        self.pauses.append(seconds)  # record instead of waiting, to keep tests fast


@dataclass
class Rig:
    fake: FakeCommonCrawl
    warc: FakeWarc
    ctx: FetchContext
    ledger: MemoryLedger
    log: MemoryAttemptLog
    gate: SpyGate
    store: LocalBlobStore
    host: str


async def no_sleep(_: float) -> None:
    return None


@pytest.fixture
async def rig(httpserver: HTTPServer, tmp_path: Path) -> AsyncIterator[Rig]:
    fake = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    fake.add_warc(warc)
    fake.install()
    host = httpserver.host
    ledger, log, gate = MemoryLedger(), MemoryAttemptLog(), SpyGate()
    await ledger.open(JOB, total_cap=100, host_caps={host: 100})
    store = LocalBlobStore(tmp_path / "raw")
    async with httpx.AsyncClient() as client:
        ctx = FetchContext(
            client=client,
            gate=gate,
            ledger=ledger,
            attempts=log,
            raw=RawWarcWriter(store, JOB),
            quarantine=RawWarcWriter(store, f"quarantine/{JOB}"),
            job_id=JOB,
            data_base=fake.base_url,
            backoff=(0.01, 0.01, 0.01, 0.01),
            sleep=no_sleep,
        )
        yield Rig(fake, warc, ctx, ledger, log, gate, store, host)


def path_of(rig: Rig) -> str:
    return "/" + rig.warc.filename


async def test_happy_path_stores_untouched_bytes_and_builds_the_row(rig: Rig) -> None:
    ref = rig.warc.refs[2]
    result = await fetch_capture(ref, rig.ctx)

    assert result.outcome is Outcome.OK and result.attempts == 1
    cap = result.capture
    assert cap is not None
    assert cap.capture_id == capture_id(
        Source.COMMON_CRAWL, ref.crawl, ref.warc_filename, ref.warc_record_offset
    )
    assert (cap.upstream_offset, cap.upstream_length) == (
        ref.warc_record_offset,
        ref.warc_record_length,
    )
    copy = rig.store.read(BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length))
    upstream = rig.warc.data[
        ref.warc_record_offset : ref.warc_record_offset + ref.warc_record_length
    ]
    assert copy == upstream  # byte-exact copy of Common Crawl's record
    assert verify_record(copy, ref).verification is Verification.OK
    assert rig.fake.requests[-1][2] == ref.range_header()
    assert [a.outcome for a in rig.log.attempts] == [Outcome.OK]


async def test_rate_limited_pauses_the_whole_host_and_retries(rig: Rig) -> None:
    rig.fake.fail_next[path_of(rig)].append((503, {"Retry-After": "7"}))
    result = await fetch_capture(rig.warc.refs[0], rig.ctx)
    assert result.outcome is Outcome.OK and result.attempts == 2
    assert rig.gate.pauses and rig.gate.pauses[0] >= 7  # Retry-After wins over the schedule
    assert [a.outcome for a in rig.log.attempts] == [Outcome.RATE_LIMITED, Outcome.OK]
    assert rig.log.attempts[0].retry_after_ms == 7000


async def test_every_retry_spends_budget(rig: Rig) -> None:
    rig.fake.fail_next[path_of(rig)].extend([(500, {}), (502, {})])
    result = await fetch_capture(rig.warc.refs[0], rig.ctx)
    assert result.outcome is Outcome.OK and result.attempts == 3
    assert (await rig.ledger.usage(JOB))[JOB_TOTAL] == (3, 100)


async def test_the_job_stops_exactly_at_its_cap(rig: Rig) -> None:
    ledger = MemoryLedger()
    await ledger.open("capped", total_cap=2, host_caps={rig.host: 50})
    rig.ctx.ledger, rig.ctx.job_id = ledger, "capped"
    for ref in rig.warc.refs[:2]:
        assert (await fetch_capture(ref, rig.ctx)).outcome is Outcome.OK
    with pytest.raises(BudgetExhaustedError):
        await fetch_capture(rig.warc.refs[2], rig.ctx)
    assert rig.fake.count(path_of(rig)) == 2  # the refused request was never sent


async def test_bad_bytes_are_retried_once_then_quarantined(rig: Rig) -> None:
    ref = rig.warc.refs[1].model_copy(update={"content_digest": "A" * 32})
    result = await fetch_capture(ref, rig.ctx)
    assert result.outcome is Outcome.RETRYABLE_ERROR
    assert result.capture is None and result.quarantined is not None
    assert "quarantine" in result.quarantined.uri
    assert result.attempts == 2
    assert result.verified is not None
    assert result.verified.verification is Verification.DIGEST_MISMATCH


async def test_403_stops_the_host(rig: Rig) -> None:
    rig.fake.fail_next[path_of(rig)].append((403, {}))
    with pytest.raises(HostBlockedError):
        await fetch_capture(rig.warc.refs[0], rig.ctx)
    assert [a.outcome for a in rig.log.attempts] == [Outcome.BLOCKED]


async def test_404_is_definitive_and_never_retried(rig: Rig) -> None:
    rig.fake.fail_next[path_of(rig)].append((404, {}))
    result = await fetch_capture(rig.warc.refs[0], rig.ctx)
    assert result.outcome is Outcome.DEFINITIVE_EMPTY and result.attempts == 1
    assert rig.fake.count(path_of(rig)) == 1


async def test_a_server_ignoring_range_is_not_read_in_full(rig: Rig) -> None:
    rig.fake.ignore_range = True
    result = await fetch_capture(rig.warc.refs[0], rig.ctx)
    assert result.outcome is Outcome.RETRYABLE_ERROR
    assert result.error is not None and "range not honoured" in result.error
    assert all(a.bytes is None for a in rig.log.attempts)


async def test_retries_give_up_after_the_schedule(rig: Rig) -> None:
    rig.fake.fail_next[path_of(rig)].extend([(500, {})] * 10)
    result = await fetch_capture(rig.warc.refs[0], rig.ctx)
    assert result.outcome is Outcome.RETRYABLE_ERROR
    assert result.attempts == len(rig.ctx.backoff) + 1
