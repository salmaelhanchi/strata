"""Per-host politeness: a token bucket, an in-flight cap, and a host-wide pause.

This is the harness's global ``Pacer``, scoped per host: every task fetching from one host
shares one ``HostGate``, and a 429/503 pauses all of them, not just the task that saw it.
Clock and sleep are injectable so tests run in virtual time.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from strata_core.models import Policy

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

_EPSILON = 1e-9
_MIN_SLEEP_S = 1e-3


class HostGate:
    def __init__(
        self,
        rate: float,
        burst: int = 1,
        max_in_flight: int = 1,
        *,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if rate <= 0 or burst < 1 or max_in_flight < 1:
            raise ValueError("rate must be > 0, burst and max_in_flight >= 1")
        self.rate = rate
        self.burst = burst
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(burst)
        self._last = clock()
        self._paused_until = 0.0
        self._lock = asyncio.Lock()
        self._in_flight = asyncio.Semaphore(max_in_flight)

    @classmethod
    def from_policy(
        cls, policy: Policy, *, clock: Clock = time.monotonic, sleep: Sleep = asyncio.sleep
    ) -> HostGate:
        return cls(policy.max_rps, policy.burst, policy.max_in_flight, clock=clock, sleep=sleep)

    def pause(self, seconds: float) -> None:
        """Hold every request to this host for at least ``seconds`` from now."""
        self._paused_until = max(self._paused_until, self._clock() + seconds)

    @property
    def paused_until(self) -> float:
        return self._paused_until

    async def acquire(self) -> None:
        """Wait for the pause to end and for a token; waiters are served in order."""
        async with self._lock:
            while True:
                now = self._clock()
                if now < self._paused_until:
                    await self._sleep(self._paused_until - now)
                    continue
                self._refill(now)
                # Tolerance: float refills can land a hair under 1 and stall on ever-tinier sleeps.
                if self._tokens >= 1 - _EPSILON:
                    self._tokens = max(0.0, self._tokens - 1)
                    return
                await self._sleep(max((1 - self._tokens) / self.rate, _MIN_SLEEP_S))

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """At most ``max_in_flight`` requests at once, each released at the bucket's rate."""
        async with self._in_flight:
            await self.acquire()
            yield

    def _refill(self, now: float) -> None:
        self._tokens = min(self.burst, self._tokens + (now - self._last) * self.rate)
        self._last = now
