"""Structured logs on stderr, every line carrying the ``run_id`` of the process that wrote it.

A run is one invocation of a command. Jobs add their ``job_id``, so the lines of a job that died
and was resumed share the job id but show two run ids. Reports a command prints go to stdout and
are not logs; the default level (warning) keeps the terminal quiet unless something goes wrong.
"""

from __future__ import annotations

import logging
import secrets
import sys
from typing import Any

import structlog


class _Stderr:
    """Whatever ``sys.stderr`` is when a line is written: test runners and CLIs swap it."""

    def write(self, text: str) -> int:
        return sys.stderr.write(text)

    def flush(self) -> None:
        sys.stderr.flush()


def new_run_id() -> str:
    return secrets.token_hex(6)


def configure(level: str = "warning", fmt: str = "console") -> None:
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
        logger_factory=structlog.PrintLoggerFactory(file=_Stderr()),  # type: ignore[arg-type]
        cache_logger_on_first_use=False,
    )


def bind(**values: Any) -> None:
    """Add ``values`` (``run_id``, ``job_id``) to every later line of this task."""
    structlog.contextvars.bind_contextvars(**values)


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)


if not structlog.is_configured():
    configure()  # used as a library: warnings and errors only, on stderr
