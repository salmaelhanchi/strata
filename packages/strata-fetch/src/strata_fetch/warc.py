"""WARC record checks: "two independent counts must agree", applied to bytes.

Must pass: exactly one ``response`` record, ``WARC-Target-URI`` equal to the index's URL, and
``WARC-Payload-Digest`` equal to the index's ``content_digest``. Also recorded, but not failed on:
a SHA-1 recomputed over the payload, because truncation or transfer encoding can explain a
difference (to be measured in week 2).
"""

from __future__ import annotations

import base64
import hashlib
import io
from dataclasses import dataclass, field

from warcio.archiveiterator import ArchiveIterator

from strata_core.models import CaptureRef, Verification


def sha1_b32(data: bytes) -> str:
    return base64.b32encode(hashlib.sha1(data).digest()).decode("ascii")  # noqa: S324 - WARC digest


@dataclass(frozen=True)
class VerifiedRecord:
    verification: Verification
    target_uri: str | None = None
    http_status: int | None = None
    warc_payload_digest: str | None = None  # "sha1:BASE32" as in the header
    recomputed_digest: str | None = None  # "sha1:BASE32" over the payload we read
    truncated: str | None = None
    detail: str | None = None

    @property
    def recompute_matches(self) -> bool | None:
        if self.warc_payload_digest is None or self.recomputed_digest is None:
            return None
        return self.warc_payload_digest == self.recomputed_digest


@dataclass(frozen=True)
class RecordView:
    """What lineage shows: the archived response's headers, read back from our copy."""

    warc_headers: list[tuple[str, str]] = field(default_factory=list)
    http_status_line: str | None = None
    http_headers: list[tuple[str, str]] = field(default_factory=list)
    payload_digest: str | None = None


def read_records(raw: bytes) -> list[tuple[object, bytes]]:
    out: list[tuple[object, bytes]] = []
    for rec in ArchiveIterator(io.BytesIO(raw)):
        out.append((rec, rec.raw_stream.read()))
    return out


def verify_record(raw: bytes, ref: CaptureRef) -> VerifiedRecord:
    if len(raw) != ref.warc_record_length:
        return VerifiedRecord(
            Verification.LENGTH_MISMATCH,
            detail=f"got {len(raw)} bytes, index says {ref.warc_record_length}",
        )
    try:
        records = read_records(raw)
    except Exception as exc:  # noqa: BLE001 - any parse failure means unreadable bytes
        return VerifiedRecord(Verification.UNREADABLE, detail=f"{type(exc).__name__}: {exc}")
    responses = [(r, p) for r, p in records if getattr(r, "rec_type", None) == "response"]
    if len(records) != 1 or len(responses) != 1:
        return VerifiedRecord(
            Verification.NOT_ONE_RESPONSE_RECORD, detail=f"{len(records)} record(s) in range"
        )
    rec, payload = responses[0]
    headers = rec.rec_headers  # type: ignore[attr-defined]
    target = headers.get_header("WARC-Target-URI")
    header_digest = headers.get_header("WARC-Payload-Digest")
    http = rec.http_headers  # type: ignore[attr-defined]
    status = int(http.get_statuscode()) if http is not None and http.get_statuscode() else None
    common = {
        "target_uri": target,
        "http_status": status,
        "warc_payload_digest": header_digest,
        "recomputed_digest": "sha1:" + sha1_b32(payload),
        "truncated": headers.get_header("WARC-Truncated"),
    }
    if target != ref.url:
        return VerifiedRecord(Verification.TARGET_URI_MISMATCH, **common)
    if ref.content_digest is not None and header_digest != f"sha1:{ref.content_digest}":
        return VerifiedRecord(Verification.DIGEST_MISMATCH, **common)
    return VerifiedRecord(Verification.OK, **common)


def view_record(raw: bytes, *, max_headers: int = 12) -> RecordView:
    records = read_records(raw)
    if not records:
        return RecordView()
    rec, _payload = records[0]
    headers = rec.rec_headers  # type: ignore[attr-defined]
    http = rec.http_headers  # type: ignore[attr-defined]
    return RecordView(
        warc_headers=[(k, v) for k, v in headers.headers][:max_headers],
        http_status_line=http.protocol + " " + http.statusline if http is not None else None,
        http_headers=[(k, v) for k, v in http.headers][:max_headers] if http is not None else [],
        payload_digest=headers.get_header("WARC-Payload-Digest"),
    )
