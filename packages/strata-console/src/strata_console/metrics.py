"""Prometheus metrics for the console: HTTP requests, pulls, and the requests Strata spends."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.http_requests = Counter(
            "strata_http_requests_total",
            "HTTP requests served, by route and status",
            ["method", "route", "status"],
            registry=self.registry,
        )
        self.http_seconds = Histogram(
            "strata_http_request_seconds",
            "Time to serve a request, by route",
            ["route"],
            registry=self.registry,
        )
        self.pulls = Counter(
            "strata_pulls_total", "Pulls finished, by end state", ["state"], registry=self.registry
        )
        self.pulls_running = Gauge(
            "strata_pulls_running", "Pulls running now", registry=self.registry
        )
        self.upstream_requests = Counter(
            "strata_upstream_requests_total",
            "Requests sent to Common Crawl on the console's behalf, by kind",
            ["kind"],
            registry=self.registry,
        )

    def install(self, app: FastAPI) -> None:
        @app.middleware("http")
        async def measure(
            request: Request, call_next: Callable[[Request], Awaitable[Response]]
        ) -> Response:
            t0 = time.perf_counter()
            response = await call_next(request)
            route = request.scope.get("route")
            name = getattr(route, "path", "unmatched")
            self.http_seconds.labels(name).observe(time.perf_counter() - t0)
            self.http_requests.labels(request.method, name, str(response.status_code)).inc()
            return response

        @app.get("/metrics", include_in_schema=False)
        async def metrics() -> Response:
            return Response(generate_latest(self.registry), media_type=CONTENT_TYPE_LATEST)
