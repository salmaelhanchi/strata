"""Prometheus metrics for the scheduler, served on /metrics by ``strata scheduler``."""

from __future__ import annotations

from prometheus_client import Counter, Gauge

LEASES_GRANTED = Counter("strata_pool_leases_granted", "Leases granted to workers")
LEASES_TAKEN_BACK = Counter(
    "strata_pool_leases_taken_back",
    "Leases returned to the queue without a result",
    ["reason"],  # expired | disconnected
)
RESULTS = Counter("strata_pool_results", "Results accepted from workers", ["outcome"])
RESULTS_REJECTED = Counter(
    "strata_pool_results_rejected", "Results for a lease that was no longer current"
)
LEASES_IN_FLIGHT = Gauge("strata_pool_leases_in_flight", "Leases granted and not yet answered")
WORKERS = Gauge("strata_pool_workers", "Workers connected")
