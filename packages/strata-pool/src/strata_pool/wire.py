"""Between the protobuf messages of pool.proto and the plain values in ``leases.py``.

Enum names match (``Outcome.OK`` <-> ``OUTCOME_OK``), so the outcome mapping is by name.
"""

from __future__ import annotations

from datetime import datetime

from strata_core.models import CaptureRef, Outcome
from strata_pool.leases import Grant, Report, StoredRecord
from strata_pool.proto import pool_pb2 as pb

SERVICE = "strata.pool.v1.Scheduler"
WORK_METHOD = f"/{SERVICE}/Work"


def outcome_to_pb(outcome: Outcome) -> str:
    """The enum value's name, which protobuf accepts wherever the enum is expected."""
    return f"OUTCOME_{outcome.name}"


def outcome_from_pb(value: int) -> Outcome:
    name = pb.Outcome.Name(value).removeprefix("OUTCOME_")
    if name not in Outcome.__members__:
        raise ValueError(f"result without an outcome: {pb.Outcome.Name(value)}")
    return Outcome[name]


def ref_to_pb(ref: CaptureRef) -> pb.PageRef:
    page = pb.PageRef(
        crawl=ref.crawl,
        url=ref.url,
        warc_filename=ref.warc_filename,
        warc_record_offset=ref.warc_record_offset,
        warc_record_length=ref.warc_record_length,
    )
    if ref.url_surtkey is not None:
        page.url_surtkey = ref.url_surtkey
    if ref.fetch_time is not None:
        page.fetch_time = ref.fetch_time.isoformat()
    if ref.content_digest is not None:
        page.content_digest = ref.content_digest
    if ref.mime is not None:
        page.mime = ref.mime
    if ref.status is not None:
        page.status = ref.status
    return page


def ref_from_pb(page: pb.PageRef) -> CaptureRef:
    return CaptureRef(
        crawl=page.crawl,
        url=page.url,
        url_surtkey=page.url_surtkey if page.HasField("url_surtkey") else None,
        fetch_time=(
            datetime.fromisoformat(page.fetch_time) if page.HasField("fetch_time") else None
        ),
        warc_filename=page.warc_filename,
        warc_record_offset=page.warc_record_offset,
        warc_record_length=page.warc_record_length,
        content_digest=page.content_digest if page.HasField("content_digest") else None,
        mime=page.mime if page.HasField("mime") else None,
        status=page.status if page.HasField("status") else None,
    )


def grant_to_pb(grant: Grant) -> pb.SchedulerMessage:
    return pb.SchedulerMessage(
        lease=pb.Lease(
            lease_id=grant.lease_id,
            job_id=grant.job_id,
            page=ref_to_pb(grant.ref),
            attempt=grant.attempt,
            ttl_s=grant.ttl_s,
            data_base=grant.data_base,
        )
    )


def grant_from_pb(lease: pb.Lease) -> Grant:
    return Grant(
        lease_id=lease.lease_id,
        job_id=lease.job_id,
        ref=ref_from_pb(lease.page),
        attempt=lease.attempt,
        ttl_s=lease.ttl_s,
        data_base=lease.data_base,
    )


def report_to_pb(report: Report) -> pb.WorkerMessage:
    result = pb.Result(
        lease_id=report.lease_id,
        outcome=outcome_to_pb(report.outcome),
        bytes=report.bytes,
        started_at=report.started_at.isoformat(),
        elapsed_ms=report.elapsed_ms,
    )
    if report.http_status is not None:
        result.http_status = report.http_status
    if report.retry_after_s is not None:
        result.retry_after_s = report.retry_after_s
    if report.error is not None:
        result.error = report.error
    if report.stored is not None:
        s = report.stored
        result.stored.CopyFrom(
            pb.Stored(
                raw_uri=s.raw_uri,
                raw_offset=s.raw_offset,
                raw_length=s.raw_length,
                raw_sha256=s.raw_sha256,
                http_status=s.http_status or 0,
            )
        )
    return pb.WorkerMessage(result=result)


def report_from_pb(result: pb.Result) -> Report:
    stored = None
    if result.HasField("stored"):
        s = result.stored
        stored = StoredRecord(
            raw_uri=s.raw_uri,
            raw_offset=s.raw_offset,
            raw_length=s.raw_length,
            raw_sha256=s.raw_sha256,
            http_status=s.http_status or None,
        )
    return Report(
        lease_id=result.lease_id,
        outcome=outcome_from_pb(result.outcome),
        started_at=datetime.fromisoformat(result.started_at),
        elapsed_ms=result.elapsed_ms,
        http_status=result.http_status if result.HasField("http_status") else None,
        retry_after_s=result.retry_after_s if result.HasField("retry_after_s") else None,
        bytes=result.bytes,
        error=result.error if result.HasField("error") else None,
        stored=stored,
    )
