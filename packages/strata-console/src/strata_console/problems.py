"""Errors as ``application/problem+json`` (RFC 9457): a type, a title, a status, the detail.

Upstream failures keep their meaning: an index that is down is a 503 the client may retry, a
query Common Crawl refuses or a pattern RE2 rejects is a 422, a budget that can't pay is a 402
(the request was fine; the money ran out), and a 403 from Common Crawl stops everything (ADR-003).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from strata_fetch.cc.attempts import IndexBlockedError, IndexQueryError, IndexUnavailableError
from strata_fetch.cc.columnar import IndexBudgetError
from strata_fetch.jobs import ResumeRefusedError

MEDIA_TYPE = "application/problem+json"
BASE = "https://github.com/salmaelhanchi/strata/blob/main/docs/api.md#"


def problem(status: int, title: str, detail: str | None = None, **extra: Any) -> JSONResponse:
    body: dict[str, Any] = {
        "type": BASE + title.lower().replace(" ", "-"),
        "title": title,
        "status": status,
    }
    if detail:
        body["detail"] = detail
    body.update(extra)
    return JSONResponse(body, status_code=status, media_type=MEDIA_TYPE)


_UPSTREAM: list[tuple[type[Exception], int, str]] = [
    (IndexBudgetError, 402, "Budget exhausted"),
    (IndexQueryError, 422, "Query refused"),
    (IndexBlockedError, 502, "Blocked by Common Crawl"),
    (IndexUnavailableError, 503, "Index unavailable"),
    (ResumeRefusedError, 409, "Job conflict"),
]


def install(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_error(_: Request, exc: HTTPException) -> JSONResponse:
        titles = {404: "Not found", 400: "Bad request", 409: "Conflict"}
        return problem(exc.status_code, titles.get(exc.status_code, "Error"), str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"loc": [str(p) for p in e.get("loc", ())], "msg": e.get("msg", "")}
            for e in exc.errors()
        ]
        return problem(422, "Invalid request", "the request did not validate", errors=errors)

    for kind, status, title in _UPSTREAM:

        async def upstream(
            _: Request, exc: Exception, status: int = status, title: str = title
        ) -> JSONResponse:
            attempts = len(getattr(exc, "attempts", []))
            response = problem(status, title, str(exc), attempts=attempts)
            if status == 503:
                response.headers["Retry-After"] = "60"
            return response

        app.add_exception_handler(kind, upstream)
