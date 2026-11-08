"""Lineage: from a bronze row back to the exact bytes, and proof that they are the right bytes."""

from __future__ import annotations

import hashlib
from urllib.parse import urljoin

from pydantic import BaseModel, ConfigDict

from strata_core.lake import Lake
from strata_core.models import BlobRef, Capture
from strata_fetch.blobstore import BlobStore
from strata_fetch.warc import read_records, sha1_b32, view_record


class LineageReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    capture: Capture
    upstream_url: str | None
    upstream_range: str | None
    raw_bytes_intact: bool  # our copy still hashes to the SHA-256 recorded at fetch time
    index_digest: str | None
    warc_header_digest: str | None
    recomputed_digest: str | None
    index_matches_warc_header: bool | None
    recompute_matches_header: bool | None
    http_status_line: str | None
    http_headers: list[tuple[str, str]]


def lineage(
    capture_id: str,
    lake: Lake,
    store: BlobStore,
    *,
    data_base: str = "https://data.commoncrawl.org/",
) -> LineageReport | None:
    cap = lake.get_capture(capture_id)
    if cap is None:
        return None
    raw = store.read(BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length))
    intact = hashlib.sha256(raw).hexdigest() == cap.raw_sha256
    view = view_record(raw)
    records = read_records(raw)
    recomputed = "sha1:" + sha1_b32(records[0][1]) if records else None
    index_digest = f"sha1:{cap.content_digest_sha1}" if cap.content_digest_sha1 else None
    upstream_url = (
        urljoin(data_base, cap.upstream_warc_filename) if cap.upstream_warc_filename else None
    )
    upstream_range = (
        f"bytes={cap.upstream_offset}-{cap.upstream_offset + cap.upstream_length - 1}"
        if cap.upstream_offset is not None and cap.upstream_length is not None
        else None
    )
    header = view.payload_digest
    return LineageReport(
        capture=cap,
        upstream_url=upstream_url,
        upstream_range=upstream_range,
        raw_bytes_intact=intact,
        index_digest=index_digest,
        warc_header_digest=header,
        recomputed_digest=recomputed,
        index_matches_warc_header=(index_digest == header) if index_digest and header else None,
        recompute_matches_header=(recomputed == header) if recomputed and header else None,
        http_status_line=view.http_status_line,
        http_headers=view.http_headers,
    )
