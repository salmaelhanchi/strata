"""DuckDB queries over remote Parquet with every HTTP request logged, bounded and costed.

DuckDB sends URL-index requests with its own HTTP client. They are read back from DuckDB's log
after each query and reported as ``client="duckdb"`` attempts, so a slow lookup never shows as
"0 requests" and a job pays for every one.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import duckdb
from pydantic import BaseModel, ConfigDict

from strata_core.models import Outcome, RequestClient
from strata_fetch.cc.attempts import IndexAttempt, IndexQueryError
from strata_fetch.http import (
    classify_status,
)


class DuckDbUsage(BaseModel):
    """What one URL-index query cost, from DuckDB's own HTTP and file-system logs."""

    model_config = ConfigDict(frozen=True)

    requests: int = 0
    bytes: int = 0  # response bodies received (Content-Length of each GET)
    bytes_read: int = 0  # what DuckDB's file system read from remote files; should equal bytes
    files: int = 0  # distinct remote files touched
    failed: int = 0  # requests without a 2xx answer
    peak_in_flight: int = 0  # most requests open at once, from their start times and durations
    query_s: float = 0.0  # wall time of the query


# DuckDB merges the column-chunk reads of a row group whose gap is at most this many bytes.
# `run_logged_query` pins DuckDB's `parquet_prefetch_column_gap` to it and the shard map's cost
# estimate assumes it, so the two agree. Common Crawl's files hold columns a site query skips
# (url_path, url_query, ...) between the ones it reads, which splits a row group into runs.
MERGE_GAP = 16 * 1024


class CostEstimate(BaseModel):
    """What a site query should cost, from the cached row-group statistics (before any request).

    ``requests`` counts, per file, the HEAD and footer reads, and per row group the reads DuckDB
    needs: chunks less than 16 KiB apart merged (``shardmap.MERGE_GAP``), each run read in blocks
    of DuckDB's remote block size, plus one read per run. A job reserves it before the query and
    is charged or refunded the difference.
    """

    model_config = ConfigDict(frozen=True)

    files: int = 0
    row_groups: int = 0
    rows: int = 0
    requests: int = 0
    bytes: int = 0


# ------------------------------------------------------------------- DuckDB's own requests
# DuckDB 2.0 logs every HTTP request its httpfs extension makes. start_time is a TIMESTAMPTZ, so it
# is read as epoch milliseconds: turning it into a Python object would need pytz (ADR-004).
_HTTP_LOG_SQL = """
SELECT request.type, request.url, epoch_ms(request.start_time), request.duration_ms,
       response.status, response.headers['Content-Length']
FROM duckdb_logs_parsed('HTTP')
ORDER BY 3
"""

_FS_READ_SQL = """
SELECT coalesce(sum(bytes), 0) FROM duckdb_logs_parsed('FileSystem')
WHERE op = 'READ' AND fs <> 'LocalFileSystem'
"""


def parse_http_status(value: str | None) -> int | None:
    """DuckDB logs statuses as names with the code last: ``PartialContent_206`` -> 206."""
    if not value:
        return None
    code = value.rsplit("_", 1)[-1]
    return int(code) if code.isdigit() else None


def peak_in_flight(intervals: Sequence[tuple[int, int]]) -> int:
    """Most intervals open at once; each is [start_ms, start_ms + duration_ms), at least 1 ms."""
    events: list[tuple[int, int]] = []
    for start, duration in intervals:
        events.append((start, 1))
        events.append((start + max(duration, 1), -1))
    events.sort()  # at equal times an end (-1) sorts before a start (+1): touching isn't overlap
    peak = current = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak


def read_duckdb_requests(
    con: duckdb.DuckDBPyConnection, query_s: float = 0.0
) -> tuple[list[IndexAttempt], DuckDbUsage]:
    """DuckDB's requests since logging was enabled on ``con``, as attempts plus a summary."""
    attempts: list[IndexAttempt] = []
    intervals: list[tuple[int, int]] = []
    for method, url, start_ms, duration_ms, status_name, length in con.execute(
        _HTTP_LOG_SQL
    ).fetchall():
        status = parse_http_status(status_name)
        if status is None:
            outcome = Outcome.RETRYABLE_ERROR
        elif 200 <= status < 300:
            outcome = Outcome.OK
        else:
            outcome = classify_status(status)
        received = int(length) if method == "GET" and length and str(length).isdigit() else 0
        attempts.append(
            IndexAttempt(
                attempt=1,
                url=url,
                started_at=datetime.fromtimestamp(start_ms / 1000, UTC),
                elapsed_ms=int(duration_ms or 0),
                http_status=status,
                outcome=outcome,
                error=None if outcome is Outcome.OK else (status_name or "no response"),
                client=RequestClient.DUCKDB,
                method=method,
                bytes=received,
            )
        )
        intervals.append((int(start_ms), int(duration_ms or 0)))
    fs_row = con.execute(_FS_READ_SQL).fetchone()
    usage = DuckDbUsage(
        requests=len(attempts),
        bytes=sum(a.bytes or 0 for a in attempts),
        bytes_read=int(fs_row[0]) if fs_row else 0,
        files=len({a.url for a in attempts}),
        failed=sum(1 for a in attempts if a.outcome is not Outcome.OK),
        peak_in_flight=peak_in_flight(intervals),
        query_s=round(query_s, 3),
    )
    return attempts, usage


class DuckDbQueryError(Exception):
    def __init__(self, error: duckdb.Error, attempts: list[IndexAttempt]) -> None:
        super().__init__(str(error))
        self.error = error
        self.attempts = attempts


def check_pattern(pattern: str) -> None:
    """Compile a URL pattern with DuckDB's own regex engine (RE2) before any remote query.

    RE2 is not Python's ``re``: look-arounds and backreferences are refused. A bad pattern found
    here costs nothing; found by the remote query, it costs the requests made before it failed.
    """
    con = duckdb.connect()
    try:
        con.execute("SELECT regexp_full_match('', ?)", [pattern]).fetchall()
    except duckdb.Error as exc:
        raise IndexQueryError(f"URL pattern {pattern!r} is not a valid RE2 regex: {exc}") from exc
    finally:
        con.close()


def run_logged_query(
    sql: str, params: list[Any], *, threads: int = 1, user_agent: str | None = None
) -> tuple[list[tuple[Any, ...]], list[IndexAttempt], DuckDbUsage]:
    """One DuckDB query with its HTTP requests logged, bounded and reported (S10)."""
    # Our honest User-Agent rides along on DuckDB's own HTTP requests (ADR-003).
    config: dict[str, str | bool | int | float | list[str]] = {}
    if user_agent:
        config["custom_user_agent"] = user_agent
    try:
        con = duckdb.connect(config=config)
    except duckdb.Error as exc:
        raise DuckDbQueryError(exc, []) from exc
    t0 = time.monotonic()
    try:
        # `threads` alone doesn't bound DuckDB's requests: its Parquet reader prefetches column
        # chunks in parallel (6 in flight with threads=1, measured 2026-11-08). Capping the
        # HTTP client pool does, without the 12x more requests of disabling prefetching.
        con.execute(f"SET threads = {int(threads)}")
        con.execute(f"SET http_client_pool_capacity = {int(threads)}")
        # Pinned to the gap the cost estimate assumes, instead of DuckDB's adaptive default.
        con.execute(f"SET parquet_prefetch_column_gap = {MERGE_GAP}")
        con.execute("CALL enable_logging(['HTTP', 'FileSystem'])")
        rows = con.execute(sql, params).fetchall()
    except duckdb.Error as exc:
        try:  # the requests made before the failure still happened, and still count
            attempts, _ = read_duckdb_requests(con, time.monotonic() - t0)
        except duckdb.Error:
            attempts = []
        con.close()
        raise DuckDbQueryError(exc, attempts) from exc
    try:
        attempts, usage = read_duckdb_requests(con, time.monotonic() - t0)
    finally:
        con.close()
    return rows, attempts, usage
