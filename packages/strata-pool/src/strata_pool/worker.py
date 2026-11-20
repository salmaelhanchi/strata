"""A fetch worker: reads the records it is leased, stores them, and reports.

It sends each request at once: the scheduler already reserved the budget for it and waited for
the host's rate limiter. A verified record is appended to the worker's own part, with its manifest
line, and the part is sealed into the raw store before the result goes out, so the scheduler never
writes a row whose bytes are not durable. Any other result goes out at once.

Results for verified records wait only while another of the worker's requests is still running
(the scheduler leases a slot again only after its result arrives), so a seal covers up to ``slots``
records and nothing waits for long.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin

import grpc
import httpx

from strata_core import logs
from strata_core.models import Outcome
from strata_fetch.blobstore import BlobStore, PartWriter, seal_part
from strata_fetch.fetcher import capture_row, read_once
from strata_fetch.ratelimit import HostGate
from strata_pool.leases import Grant, Report, StoredRecord
from strata_pool.proto import pool_pb2 as pb
from strata_pool.wire import WORK_METHOD, grant_from_pb, report_to_pb

log = logs.get_logger(__name__)

Send = Callable[[pb.WorkerMessage], Awaitable[None]]


@dataclass
class WorkerStats:
    leases: int = 0
    outcomes: Counter[Outcome] = field(default_factory=Counter)
    rejected: int = 0  # results the scheduler no longer wanted (their lease had moved on)


def safe_worker_id(name: str) -> str:
    """A worker id usable as a folder name: its parts are staged under it."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip(".-")
    return cleaned or "worker"


class Worker:
    def __init__(
        self,
        worker_id: str,
        *,
        slots: int,
        client: httpx.AsyncClient,
        staging_root: Path,
        store: BlobStore,
        version: str,
    ) -> None:
        self.worker_id = safe_worker_id(worker_id)
        self.slots = max(1, slots)
        self.stats = WorkerStats()
        self._client = client
        self._staging = staging_root
        self._store = store
        self._version = version
        # The scheduler paces requests; this gate only satisfies read_once and bounds the
        # worker's own concurrency to its slots.
        self._gate = HostGate(rate=1_000_000.0, burst=self.slots, max_in_flight=self.slots)
        self._writers: dict[str, PartWriter] = {}
        self._unsent: list[Report] = []  # verified records waiting for their part's seal
        self._busy = 0
        self._seal_lock = asyncio.Lock()

    async def run(self, channel: grpc.aio.Channel) -> WorkerStats:
        """Work until the scheduler ends the stream (the job has nothing left to lease)."""
        call = channel.stream_stream(
            WORK_METHOD,
            request_serializer=pb.WorkerMessage.SerializeToString,
            response_deserializer=pb.SchedulerMessage.FromString,
        )(wait_for_ready=True)
        lock = asyncio.Lock()

        async def send(msg: pb.WorkerMessage) -> None:
            async with lock:  # one write at a time on the stream
                await call.write(msg)

        await send(pb.WorkerMessage(hello=pb.Hello(worker_id=self.worker_id, slots=self.slots)))
        log.info("worker.connected", worker=self.worker_id, slots=self.slots)
        tasks: set[asyncio.Task[None]] = set()
        try:
            while (msg := await call.read()) is not grpc.aio.EOF:
                assert isinstance(msg, pb.SchedulerMessage)
                if msg.HasField("lease"):
                    self.stats.leases += 1
                    grant = grant_from_pb(msg.lease)
                    log.info("worker.lease", url=grant.ref.url, attempt=grant.attempt)
                    task = asyncio.create_task(self._serve(grant, send))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                elif msg.HasField("ack") and not msg.ack.accepted:
                    self.stats.rejected += 1
            if tasks:
                await asyncio.gather(*tasks)
            await call.done_writing()
        finally:
            for task in tasks:
                task.cancel()
        log.info("worker.finished", worker=self.worker_id, leases=self.stats.leases)
        return self.stats

    async def _serve(self, grant: Grant, send: Send) -> None:
        self._busy += 1
        try:
            report = await self.fetch(grant)
        finally:
            self._busy -= 1
        self.stats.outcomes[report.outcome] += 1
        if report.stored is None:
            await send(report_to_pb(report))
        else:
            self._unsent.append(report)
        # Whichever request finishes last seals what the others left waiting.
        if self._unsent and (self._busy == 0 or len(self._unsent) >= self.slots):
            await self._seal_and_send(send)

    async def fetch(self, grant: Grant) -> Report:
        """One request for one lease: read the record's byte range, verify it, stage it."""
        ref = grant.ref
        url = urljoin(grant.data_base, ref.warc_filename)
        started, t0 = datetime.now(UTC), time.monotonic()
        read = await read_once(ref, url, self._client, self._gate)
        elapsed = int((time.monotonic() - t0) * 1000)
        body, verified = read.body, read.verified
        if read.outcome is Outcome.OK and body is not None and verified is not None:
            writer = self._writer(grant.job_id)
            blob = writer.append(body)  # fsynced
            sha256 = hashlib.sha256(body).hexdigest()
            row = capture_row(
                ref,
                grant.job_id,
                blob,
                sha256,
                verified.http_status,
                started,
                version=self._version,
            )
            writer.log("page", row)  # the worker's own write-ahead log, as in a local job
            stored = StoredRecord(blob.uri, blob.offset, blob.length, sha256, verified.http_status)
            return Report(
                grant.lease_id,
                Outcome.OK,
                started,
                elapsed,
                read.status,
                read.retry_after,
                len(body),
                stored=stored,
            )
        return Report(
            grant.lease_id,
            read.outcome,
            started,
            elapsed,
            read.status,
            read.retry_after,
            len(body) if body is not None else 0,
            read.error,
        )

    async def _seal_and_send(self, send: Send) -> None:
        async with self._seal_lock:
            batch, self._unsent = self._unsent, []
            if not batch:
                return
            # Every record in the batch was appended before this point, so it is in a part
            # closed here (a record appended meanwhile is sealed early, which is harmless).
            for writer in self._writers.values():
                writer.rotate()
                for part in writer.take_closed():
                    await asyncio.to_thread(seal_part, self._store, part)
            for report in batch:
                await send(report_to_pb(report))

    def _writer(self, job_id: str) -> PartWriter:
        if job_id not in self._writers:
            self._writers[job_id] = PartWriter(
                self._staging, self._store, f"{job_id}/workers/{self.worker_id}"
            )
        return self._writers[job_id]
