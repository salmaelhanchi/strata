"""Which URL-index Parquet files can hold a domain: a per-crawl "shard map" (ADR-004).

Common Crawl's URL index has 300 Parquet files of page captures per crawl (``subset=warc``; 900
with the crawl-diagnostics and robots.txt subsets). Rows are approximately sorted by SURT key
(cc-index-table#12), but file numbers don't follow key order: in CC-MAIN-2026-43 only 150 of 299
neighbouring pairs do, so a search by file number would miss files. Pointing DuckDB at all 300
makes it read 300 footers on every lookup, unpaced and from scratch each time. Instead we read
each footer once, through our own rate-limited client, keep only the ``url_surtkey`` minimum and
maximum per file, and cache that per crawl: a released crawl never changes. Later lookups open
only the files whose range overlaps the domain's SURT range (one of 300 for python.org).

Reading a footer costs one ranged request for the last ``TAIL_BYTES`` of the file (a second one
if the footer is larger). DuckDB then reads the statistics from a file holding only the footer.
"""

from __future__ import annotations

import asyncio
import json
import struct
import tempfile
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import duckdb
import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from strata_core.models import Outcome
from strata_fetch.cc.index import (
    IndexAttempt,
    IndexBlockedError,
    IndexUnavailableError,
    OnAttempt,
    ShardSelection,
    server_message,
)
from strata_fetch.http import (
    RETRYABLE_EXCEPTIONS,
    backoff_delay,
    classify_status,
    retry_after_seconds,
)
from strata_fetch.ratelimit import HostGate, Sleep

MAGIC = b"PAR1"
TAIL_BYTES = 64 * 1024  # most footers fit; a larger one costs one more request
_MAP_VERSION = 1
Progress = Callable[[int, int], None]  # (footers read so far, footers to read)


class ShardMapError(IndexUnavailableError):
    """A Parquet footer could not be read, so the shard map can't be completed."""


class ShardRange(BaseModel):
    model_config = ConfigDict(frozen=True)

    path: str
    min_key: str | None
    max_key: str | None

    def covers(self, lo: str, hi: str) -> bool:
        """Whether the file may hold keys in ``[lo, hi)``. Missing statistics mean "maybe"."""
        if self.min_key is None or self.max_key is None:
            return True
        return self.min_key < hi and self.max_key >= lo


# ---------------------------------------------------------------------------- reading tails
class TailReader(Protocol):
    async def read_tail(self, path: str, n: int) -> tuple[bytes, int, list[IndexAttempt]]:
        """The last ``n`` bytes (or the whole file if smaller), the file size, the requests made."""
        ...

    async def read_range(
        self, path: str, first: int, last: int
    ) -> tuple[bytes, list[IndexAttempt]]:
        """Bytes ``first..last`` inclusive and the requests made."""
        ...


class LocalTail:
    """Footers of local files: downloaded shards and test fixtures. Makes no requests."""

    async def read_tail(self, path: str, n: int) -> tuple[bytes, int, list[IndexAttempt]]:
        def read() -> tuple[bytes, int]:
            file = Path(path)
            size = file.stat().st_size
            with file.open("rb") as fh:
                fh.seek(max(0, size - n))
                return fh.read(), size

        data, size = await asyncio.to_thread(read)
        return data, size, []

    async def read_range(
        self, path: str, first: int, last: int
    ) -> tuple[bytes, list[IndexAttempt]]:
        def read() -> bytes:
            with Path(path).open("rb") as fh:
                fh.seek(first)
                return fh.read(last - first + 1)

        return await asyncio.to_thread(read), []


class HttpTail:
    """Byte ranges of remote files through our client: paced by the host gate, retried with the
    ADR-003 backoff, and every request reported to ``on_attempt``.

    A server that ignored ``Range`` would send the whole file (a couple of hundred MB), so a 200
    is refused without reading its body.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        gate: HostGate,
        *,
        backoff: Sequence[float] = (2.0, 10.0, 30.0, 60.0),
        sleep: Sleep = asyncio.sleep,
        timeout_s: float | None = None,
        on_attempt: OnAttempt | None = None,
    ) -> None:
        self._client = client
        self._gate = gate
        self._backoff = tuple(backoff)
        self._sleep = sleep
        self._timeout = timeout_s
        self._on_attempt = on_attempt

    async def read_tail(self, path: str, n: int) -> tuple[bytes, int, list[IndexAttempt]]:
        return await self._get(path, f"bytes=-{n}")  # a suffix range: no HEAD request needed

    async def read_range(
        self, path: str, first: int, last: int
    ) -> tuple[bytes, list[IndexAttempt]]:
        data, _, log = await self._get(path, f"bytes={first}-{last}")
        return data, log

    async def _get(self, url: str, range_value: str) -> tuple[bytes, int, list[IndexAttempt]]:
        log: list[IndexAttempt] = []
        max_attempts = len(self._backoff) + 1
        for attempt in range(1, max_attempts + 1):
            started_at, t0 = datetime.now(UTC), time.monotonic()
            status: int | None = None
            retry_after: float | None = None
            error: str | None = None
            data, size, error_body = b"", 0, ""
            timeout = self._timeout if self._timeout is not None else httpx.USE_CLIENT_DEFAULT
            try:
                async with self._gate.slot():
                    started_at, t0 = datetime.now(UTC), time.monotonic()  # gate wait excluded
                    async with self._client.stream(
                        "GET", url, headers={"Range": range_value}, timeout=timeout
                    ) as resp:
                        status = resp.status_code
                        retry_after = retry_after_seconds(resp.headers.get("Retry-After"))
                        if status == 206:
                            data = await resp.aread()
                            size = _total_size(resp.headers.get("Content-Range"))
                        elif status != 200:  # a 200 is the whole file: leave its body unread
                            error_body = (await resp.aread())[:4096].decode("utf-8", "replace")
            except RETRYABLE_EXCEPTIONS as exc:
                outcome, error = Outcome.RETRYABLE_ERROR, f"{type(exc).__name__}: {exc}"
            else:
                if status == 206 and size > 0:
                    outcome = Outcome.OK
                elif status == 206:
                    outcome, error = Outcome.DEFINITIVE_EMPTY, "206 without a file size"
                elif status == 200:
                    outcome = Outcome.DEFINITIVE_EMPTY
                    error = "the server ignored the Range header; refusing to download the file"
                else:
                    outcome = classify_status(status or 0)
                    error = f"HTTP {status}: {server_message(error_body)}"

            final = outcome in (Outcome.OK, Outcome.BLOCKED, Outcome.DEFINITIVE_EMPTY)
            delay = (
                None
                if final or attempt == max_attempts
                else backoff_delay(attempt, self._backoff, retry_after)
            )
            record = IndexAttempt(
                attempt=attempt,
                url=url,
                started_at=started_at,
                elapsed_ms=int((time.monotonic() - t0) * 1000),
                http_status=status,
                outcome=outcome,
                retry_after_ms=int(retry_after * 1000) if retry_after is not None else None,
                waited_ms=int(delay * 1000) if delay is not None else None,
                error=error,
            )
            log.append(record)
            if self._on_attempt is not None:
                await self._on_attempt(record)

            if outcome is Outcome.OK:
                return data, size, log
            if outcome is Outcome.BLOCKED:
                raise IndexBlockedError(f"{url}: {error}", log)
            if outcome is Outcome.DEFINITIVE_EMPTY:
                raise ShardMapError(f"{url}: {error}", log)
            if delay is None:
                break
            if outcome is Outcome.RATE_LIMITED:
                self._gate.pause(delay)
            else:
                await self._sleep(delay)
        raise ShardMapError(f"{url}: {log[-1].error if log else 'no attempt'}", log)


def _total_size(content_range: str | None) -> int:
    """``bytes 100-199/12345`` -> 12345; 0 when absent or unknown (``/*``)."""
    if not content_range or "/" not in content_range:
        return 0
    total = content_range.rsplit("/", 1)[1].strip()
    return int(total) if total.isdigit() else 0


# ---------------------------------------------------------------------------- footers
async def read_footer(
    reader: TailReader, path: str, tail_bytes: int = TAIL_BYTES
) -> tuple[bytes, list[IndexAttempt]]:
    """A minimal Parquet file holding only ``path``'s footer: enough for its statistics."""
    data, size, log = await reader.read_tail(path, tail_bytes)
    if size < 12 or len(data) < 8 or data[-4:] != MAGIC:
        raise ShardMapError(f"{path}: not a Parquet file", log)
    need = struct.unpack("<I", data[-8:-4])[0] + 8  # footer + its length + magic
    if need + 4 > size:
        raise ShardMapError(f"{path}: footer length exceeds the file", log)
    if need > len(data):
        head, more = await reader.read_range(path, size - need, size - len(data) - 1)
        log = [*log, *more]
        data = head + data
    return MAGIC + data[-need:], log


def footer_stats(footer: bytes, column: str = "url_surtkey") -> tuple[str | None, str | None]:
    """Minimum and maximum of ``column`` over all row groups; ``(None, None)`` when any row group
    lacks statistics, because then the file can't be ruled out."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "footer.parquet"
        path.write_bytes(footer)
        con = duckdb.connect()
        try:
            row = con.execute(
                """
                SELECT count(*),
                       count(coalesce(stats_min_value, stats_min)),
                       count(coalesce(stats_max_value, stats_max)),
                       min(coalesce(stats_min_value, stats_min)),
                       max(coalesce(stats_max_value, stats_max))
                FROM parquet_metadata(?)
                WHERE path_in_schema = ?
                """,
                [path.as_posix(), column],
            ).fetchone()
        finally:
            con.close()  # before the directory goes: Windows won't delete an open file
    if row is None or row[0] == 0 or row[1] < row[0] or row[2] < row[0]:
        return None, None
    return str(row[3]), str(row[4])


# ---------------------------------------------------------------------------- the map
class ShardMap:
    """Per-crawl SURT ranges of URL-index files, read once and cached as JSON Lines.

    Entries are appended as footers are read, so an interrupted build resumes where it stopped.
    """

    def __init__(
        self,
        reader: TailReader,
        *,
        cache_dir: Path | None = None,
        tail_bytes: int = TAIL_BYTES,
        on_progress: Progress | None = None,
        progress_every: int = 50,
    ) -> None:
        self._reader = reader
        self._dir = cache_dir
        self._tail = tail_bytes
        self._on_progress = on_progress
        self._every = max(1, progress_every)
        self._memory: dict[str, dict[str, ShardRange]] = {}

    async def select(self, crawl: str, files: list[str], lo: str, hi: str) -> ShardSelection:
        ranges, attempts, read = await self.ranges(crawl, files)
        return ShardSelection(
            files=[r.path for r in ranges if r.covers(lo, hi)],
            attempts=attempts,
            footers_read=read,
        )

    async def ranges(
        self, crawl: str, files: Sequence[str]
    ) -> tuple[list[ShardRange], list[IndexAttempt], int]:
        known = self._load(crawl)
        missing = [f for f in files if f not in known]
        attempts: list[IndexAttempt] = []
        if missing and self._on_progress is not None:
            self._on_progress(0, len(missing))
        for done, path in enumerate(missing, start=1):
            try:
                footer, log = await read_footer(self._reader, path, self._tail)
            except (ShardMapError, IndexBlockedError) as exc:
                # keep what this build already paid for in the error, for the attempt log
                raise type(exc)(str(exc), [*attempts, *exc.attempts]) from exc
            attempts += log
            try:
                lo_key, hi_key = await asyncio.to_thread(footer_stats, footer)
            except duckdb.Error as exc:
                raise ShardMapError(f"{path}: unreadable Parquet footer: {exc}", attempts) from exc
            shard = ShardRange(path=path, min_key=lo_key, max_key=hi_key)
            self._append(crawl, shard)
            known[path] = shard
            if self._on_progress is not None and (done % self._every == 0 or done == len(missing)):
                self._on_progress(done, len(missing))
        return [known[f] for f in files], attempts, len(missing)

    # -- cache ---------------------------------------------------------------------------------
    def _path(self, crawl: str) -> Path | None:
        return None if self._dir is None else self._dir / "shardmaps" / f"{crawl}.jsonl"

    def _load(self, crawl: str) -> dict[str, ShardRange]:
        if crawl in self._memory:
            return self._memory[crawl]
        known: dict[str, ShardRange] = {}
        path = self._path(crawl)
        if path is not None and path.exists():
            lines = path.read_text(encoding="utf-8").splitlines()
            header = _json_or_none(lines[0]) if lines else None
            if header is not None and header.get("v") == _MAP_VERSION:
                for line in lines[1:]:
                    try:
                        shard = ShardRange.model_validate_json(line)
                    except ValidationError:
                        continue  # a line cut short by an interrupted build
                    known[shard.path] = shard
            else:
                path.unlink()  # unreadable or from another version: rebuild
        self._memory[crawl] = known
        return known

    def _append(self, crawl: str, shard: ShardRange) -> None:
        path = self._path(crawl)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        new = not path.exists()
        with path.open("a", encoding="utf-8") as fh:
            if new:
                fh.write(json.dumps({"v": _MAP_VERSION, "crawl": crawl}) + "\n")
            fh.write(shard.model_dump_json() + "\n")


def _json_or_none(line: str) -> dict[str, object] | None:
    try:
        value = json.loads(line)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None
