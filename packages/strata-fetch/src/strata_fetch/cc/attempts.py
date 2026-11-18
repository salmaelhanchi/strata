"""Index requests as records, the errors that carry them, and how index replies are read."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from strata_core.models import Outcome, RequestClient
from strata_fetch.http import (
    classify_status,
)


class IndexAttempt(BaseModel):
    """One request to an index, kept so a slow or failed lookup can be explained."""

    model_config = ConfigDict(frozen=True)

    attempt: int
    url: str
    started_at: datetime
    elapsed_ms: int
    http_status: int | None
    outcome: Outcome
    retry_after_ms: int | None = None
    waited_ms: int | None = None  # backoff slept (or host paused) after this attempt
    error: str | None = None
    client: RequestClient = RequestClient.STRATA
    charged: bool = True  # shard-map footer reads are shared by every job, so not charged
    method: str | None = None  # set for DuckDB's requests (HEAD or GET)
    bytes: int | None = None  # response body bytes, when known


class IndexLookupError(Exception):
    def __init__(self, message: str, attempts: Sequence[IndexAttempt] = ()) -> None:
        super().__init__(message)
        self.attempts = list(attempts)


class IndexUnavailableError(IndexLookupError):
    """The index kept failing after every retry the policy allows."""


class IndexQueryError(IndexLookupError):
    """The index refused the query, or the query is malformed: a retry gets the same answer."""


class IndexBlockedError(IndexLookupError):
    """The index answered 403: stop and tell a human (ADR-003)."""


OnAttempt = Callable[[IndexAttempt], Awaitable[None]]


_TITLE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def server_message(body: str) -> str:
    """The useful part of an error body, on one line: pywb's JSON ``message``, an HTML page's
    title (a gateway's "504 Gateway Time-out"), or the start of the text."""
    text = body.strip()
    if text.startswith("{"):
        try:
            message = json.loads(text).get("message")
        except (ValueError, AttributeError):
            message = None
        if isinstance(message, str):
            return " ".join(message.split())
    if found := _TITLE.search(text):
        return " ".join(found.group(1).split())
    return " ".join(text.split())[:200]


# Python ``re`` errors that pywb reports as "Internal Error: ..." with status 500.
_QUERY_ERRORS = (
    "nothing to repeat",
    "multiple repeat",
    "unterminated",
    "unbalanced parenthesis",
    "missing ), unterminated subpattern",
    "bad escape",
    "bad character range",
    "look-behind requires fixed-width pattern",
)


def classify_index_reply(status: int, body: str) -> Outcome:
    """Like ``classify_status``, but reads pywb's 500s: some are throttling, some are our bug."""
    if status == 200 or (status == 404 and "no captures found" in body.lower()):
        return Outcome.OK  # a 404 "no captures found" is a definitive (empty) answer
    if status == 500:
        message = server_message(body).lower()
        if "slow down" in message:
            return Outcome.RATE_LIMITED  # pywb relays S3's "503 Slow Down" as its own 500
        if any(marker in message for marker in _QUERY_ERRORS):
            return Outcome.DEFINITIVE_EMPTY  # our query is malformed: never retry it
    return classify_status(status)


def summarise_attempts(attempts: Sequence[IndexAttempt]) -> str:
    """'2 attempts: HTTP 504, HTTP 504' for a one-line explanation."""
    parts = [
        f"HTTP {a.http_status}" if a.http_status is not None else "no response" for a in attempts
    ]
    return f"{len(attempts)} attempt(s): {', '.join(parts)}" if parts else "no attempt"
