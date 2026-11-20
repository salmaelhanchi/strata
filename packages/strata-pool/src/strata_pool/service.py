"""The scheduler's gRPC service: one bidirectional stream per worker (pool.proto).

The service only moves messages. A worker's stream holds at most ``slots`` leases; each result
frees a slot. When the stream closes, for whatever reason, its unanswered leases are taken back at
once, without waiting for their TTL.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import grpc

from strata_core import logs
from strata_pool import metrics
from strata_pool.dispatch import Dispatcher
from strata_pool.proto import pool_pb2 as pb
from strata_pool.wire import SERVICE, grant_to_pb, report_from_pb

log = logs.get_logger(__name__)


class SchedulerService:
    def __init__(self, dispatcher: Dispatcher) -> None:
        self._dispatcher = dispatcher

    async def work(
        self,
        requests: AsyncIterator[pb.WorkerMessage],
        context: grpc.aio.ServicerContext[pb.WorkerMessage, pb.SchedulerMessage],
    ) -> AsyncIterator[pb.SchedulerMessage]:
        first = await anext(aiter(requests), None)
        if first is None or not first.HasField("hello"):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the first message must be hello")
            return
        worker = first.hello.worker_id or context.peer()
        slots = asyncio.Semaphore(max(1, first.hello.slots))
        mine: set[str] = set()  # this stream's unanswered leases
        out: asyncio.Queue[pb.SchedulerMessage | None] = asyncio.Queue()
        dispatcher = self._dispatcher

        async def results() -> None:
            async for msg in requests:
                if not msg.HasField("result"):
                    continue
                report = report_from_pb(msg.result)
                accepted = await dispatcher.report(worker, report)
                if report.lease_id in mine:
                    mine.discard(report.lease_id)
                    slots.release()
                await out.put(
                    pb.SchedulerMessage(ack=pb.Ack(lease_id=report.lease_id, accepted=accepted))
                )
            await out.put(None)  # the worker closed its side

        async def leases() -> None:
            while True:
                await slots.acquire()
                grant = await dispatcher.lease(worker)
                if grant is None:
                    break
                mine.add(grant.lease_id)
                await out.put(grant_to_pb(grant))
            await out.put(None)  # nothing left: end the stream

        metrics.WORKERS.inc()
        log.info("pool.worker_connected", worker=worker, slots=first.hello.slots)
        tasks = [asyncio.create_task(results()), asyncio.create_task(leases())]
        try:
            while (msg := await out.get()) is not None:
                yield msg
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            metrics.WORKERS.dec()
            if mine:
                await dispatcher.release(sorted(mine), "disconnected")
            log.info("pool.worker_left", worker=worker, unanswered=len(mine))


def add_to_server(server: grpc.aio.Server, dispatcher: Dispatcher) -> None:
    service = SchedulerService(dispatcher)
    handler = grpc.method_handlers_generic_handler(
        SERVICE,
        {
            "Work": grpc.stream_stream_rpc_method_handler(
                service.work,
                request_deserializer=pb.WorkerMessage.FromString,
                response_serializer=pb.SchedulerMessage.SerializeToString,
            )
        },
    )
    server.add_generic_rpc_handlers((handler,))
