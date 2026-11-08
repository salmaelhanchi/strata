from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from strata_core.models import Outcome
from strata_fetch.http import backoff_delay, classify_status, retry_after_seconds
from strata_fetch.ratelimit import HostGate


class VirtualTime:
    """A clock that only moves when someone sleeps, so rate tests are exact and instant."""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += max(0.0, seconds)


def make_gate(rate: float, burst: int = 1) -> tuple[HostGate, VirtualTime]:
    vt = VirtualTime()
    return HostGate(rate, burst, max_in_flight=4, clock=vt.clock, sleep=vt.sleep), vt


async def test_burst_then_steady_rate() -> None:
    gate, vt = make_gate(rate=2, burst=2)
    times = []
    for _ in range(6):
        await gate.acquire()
        times.append(vt.now)
    assert times == pytest.approx([0.0, 0.0, 0.5, 1.0, 1.5, 2.0])


@settings(max_examples=60, deadline=None)
@given(
    rate=st.floats(min_value=0.2, max_value=20),
    burst=st.integers(min_value=1, max_value=5),
    gaps=st.lists(st.floats(min_value=0, max_value=2), min_size=1, max_size=40),
)
async def test_no_window_ever_exceeds_burst_plus_rate(
    rate: float, burst: int, gaps: list[float]
) -> None:
    gate, vt = make_gate(rate, burst)
    times: list[float] = []
    for gap in gaps:
        await vt.sleep(gap)
        await gate.acquire()
        times.append(vt.now)
    for i, start in enumerate(times):
        for end in times[i:]:
            in_window = sum(1 for t in times if start <= t <= end)
            assert in_window <= burst + rate * (end - start) + 1e-6


async def test_pause_holds_every_caller() -> None:
    gate, vt = make_gate(rate=100, burst=5)
    await gate.acquire()
    gate.pause(30)
    await gate.acquire()
    assert vt.now >= 30
    gate.pause(5)
    gate.pause(1)  # a shorter pause never shortens an existing one
    await gate.acquire()
    assert vt.now >= 35


def test_gate_rejects_nonsense() -> None:
    with pytest.raises(ValueError):
        HostGate(rate=0)


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (206, Outcome.OK),
        (429, Outcome.RATE_LIMITED),
        (503, Outcome.RATE_LIMITED),
        (403, Outcome.BLOCKED),
        (500, Outcome.RETRYABLE_ERROR),
        (502, Outcome.RETRYABLE_ERROR),
        (404, Outcome.DEFINITIVE_EMPTY),
        (416, Outcome.DEFINITIVE_EMPTY),
    ],
)
def test_status_classification(status: int, outcome: Outcome) -> None:
    assert classify_status(status) is outcome


def test_retry_after_parsing() -> None:
    now = datetime(2026, 11, 7, 12, 0, 0, tzinfo=UTC)
    assert retry_after_seconds("120") == 120.0
    assert retry_after_seconds("Sat, 07 Nov 2026 12:00:30 GMT", now=now) == 30.0
    assert retry_after_seconds("Sat, 07 Nov 2026 11:00:00 GMT", now=now) == 0.0
    assert retry_after_seconds("soon") is None
    assert retry_after_seconds(None) is None


@given(attempt=st.integers(min_value=1, max_value=10), seed=st.integers())
def test_backoff_never_drops_below_the_schedule_or_retry_after(attempt: int, seed: int) -> None:
    schedule = (2.0, 10.0, 30.0, 60.0)
    rng = random.Random(seed)
    floor = schedule[min(attempt, len(schedule)) - 1]
    delay = backoff_delay(attempt, schedule, rng=rng)
    assert floor <= delay <= floor * 1.25
    assert backoff_delay(attempt, schedule, retry_after=500, rng=rng) == 500
