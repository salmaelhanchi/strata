"""Structured logs: JSON lines on stderr, each carrying the run id (and the job id in a job)."""

from __future__ import annotations

import json

import pytest
import structlog

from strata_core import logs


def test_every_line_carries_the_run_id(capsys: pytest.CaptureFixture[str]) -> None:
    logs.configure("info", "json")
    try:
        logs.bind(run_id="abc123", job_id="job_1")
        logs.get_logger("test").info("job.started", crawl="CC-MAIN-2026-43")
        logs.get_logger("test").debug("too.detailed")
        lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    finally:
        structlog.contextvars.clear_contextvars()
        logs.configure()

    assert len(lines) == 1  # debug is below the level
    assert lines[0]["event"] == "job.started"
    assert (lines[0]["run_id"], lines[0]["job_id"]) == ("abc123", "job_1")
    assert lines[0]["level"] == "info"


def test_run_ids_differ() -> None:
    assert logs.new_run_id() != logs.new_run_id()
