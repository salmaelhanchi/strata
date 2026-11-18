"""A request budget for commands that run without a job: spent before each request is sent.

A job's budget lives in Postgres (``strata_core.ledger``); a profile or a diff needs no database,
so its budget is held here, with the same rule: reserve first, then send, and settle what DuckDB
actually sent after a query (``QueryBudget``).
"""

from __future__ import annotations


class RequestBudget:
    def __init__(self, cap: int) -> None:
        if cap < 1:
            raise ValueError("a request budget needs a cap of at least 1")
        self.cap = cap
        self.spent = 0

    @property
    def left(self) -> int:
        return max(0, self.cap - self.spent)

    def take(self) -> bool:
        """Spend one request; False (nothing spent) when the cap is reached."""
        if self.spent + 1 > self.cap:
            return False
        self.spent += 1
        return True

    async def reserve(self, n: int) -> bool:
        if n <= 0:
            return True
        if self.spent + n > self.cap:
            return False
        self.spent += n
        return True

    async def settle(self, reserved: int, actual: int) -> int:
        """Charge what went beyond the reservation or refund what was left; returns how many
        requests the cap could not cover."""
        self.spent += actual - reserved
        return max(0, self.spent - self.cap)
