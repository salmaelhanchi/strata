from __future__ import annotations

from pathlib import Path

import pytest
from warcio.archiveiterator import ArchiveIterator

from strata_core.models import BlobRef, Verification
from strata_fetch.blobstore import LocalBlobStore, PartWriter, read_blob
from strata_fetch.warc import verify_record, view_record
from tests.fakecc import build_warc

PAGES = [
    ("https://example.org/a", b"<html>a</html>"),
    ("https://example.org/b", b"<html>bb</html>"),
]


def test_part_writer_appends_and_reads_back(tmp_path: Path) -> None:
    writer = PartWriter(tmp_path, LocalBlobStore(tmp_path), "job")
    a = writer.append(b"hello")
    b = writer.append(b"world!")
    assert (a.offset, a.length, b.offset, b.length) == (0, 5, 5, 6)
    assert read_blob(b) == b"world!"
    assert a.uri.startswith("file://") and a.uri.endswith("job/part-00000.warc.gz")


def test_store_refuses_escaping_keys_and_short_reads(tmp_path: Path) -> None:
    store = LocalBlobStore(tmp_path)
    with pytest.raises(ValueError):
        store.uri("../outside")
    with pytest.raises(ValueError):
        PartWriter(tmp_path, store, "../outside")
    ref = PartWriter(tmp_path, store, "k").append(b"abc")
    with pytest.raises(OSError):
        read_blob(BlobRef(uri=ref.uri, offset=0, length=10))


def test_writer_rotates_parts_when_full(tmp_path: Path) -> None:
    writer = PartWriter(tmp_path, LocalBlobStore(tmp_path), "job_x", rotate_bytes=10)
    refs = [writer.append(b"123456") for _ in range(3)]
    assert [Path(r.uri).name for r in refs] == [
        "part-00000.warc.gz",
        "part-00001.warc.gz",
        "part-00002.warc.gz",
    ]
    assert [p.key for p in writer.take_closed()] == [
        "job_x/part-00000.warc.gz",
        "job_x/part-00001.warc.gz",
    ]


def test_concatenated_records_are_a_valid_warc_file(tmp_path: Path) -> None:
    warc = build_warc(PAGES)
    writer = PartWriter(tmp_path, LocalBlobStore(tmp_path), "job_y")
    for ref in warc.refs:
        start = ref.warc_record_offset
        writer.append(warc.data[start : start + ref.warc_record_length])
    path = tmp_path / "job_y" / "part-00000.warc.gz"
    with path.open("rb") as fh:
        urls = [r.rec_headers.get_header("WARC-Target-URI") for r in ArchiveIterator(fh)]
    assert urls == [p[0] for p in PAGES]


def _slice(warc_data: bytes, offset: int, length: int) -> bytes:
    return warc_data[offset : offset + length]


def test_verify_accepts_a_good_record() -> None:
    warc = build_warc(PAGES)
    ref = warc.refs[0]
    result = verify_record(_slice(warc.data, ref.warc_record_offset, ref.warc_record_length), ref)
    assert result.verification is Verification.OK
    assert result.http_status == 200
    assert result.recompute_matches is True


def test_verify_catches_wrong_digest_uri_length_and_garbage() -> None:
    warc = build_warc(PAGES)
    ref = warc.refs[0]
    raw = _slice(warc.data, ref.warc_record_offset, ref.warc_record_length)
    wrong_digest = ref.model_copy(update={"content_digest": "A" * 32})
    wrong_uri = ref.model_copy(update={"url": "https://example.org/other"})
    two = warc.data  # both records in one "range"
    two_ref = ref.model_copy(update={"warc_record_length": len(two)})
    assert verify_record(raw, wrong_digest).verification is Verification.DIGEST_MISMATCH
    assert verify_record(raw, wrong_uri).verification is Verification.TARGET_URI_MISMATCH
    assert verify_record(raw[:-3], ref).verification is Verification.LENGTH_MISMATCH
    assert verify_record(two, two_ref).verification is Verification.NOT_ONE_RESPONSE_RECORD
    garbage = ref.model_copy(update={"warc_record_length": 9})
    assert verify_record(b"not a gz!", garbage).verification is Verification.UNREADABLE


def test_view_record_shows_the_archived_response() -> None:
    warc = build_warc(PAGES)
    ref = warc.refs[1]
    view = view_record(_slice(warc.data, ref.warc_record_offset, ref.warc_record_length))
    assert view.http_status_line == "HTTP/1.1 200 OK"
    assert ("Content-Type", "text/html") in view.http_headers
    assert view.payload_digest == "sha1:" + str(ref.content_digest)
