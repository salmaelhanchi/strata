"""The write-ahead log under crash-resume (S12) and the S3 store (S11, ADR-005)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from strata_core.models import BlobRef, Capture, Outcome, Source, Verification
from strata_core.settings import Settings
from strata_fetch.blobstore import (
    LocalBlobStore,
    PartWriter,
    S3BlobStore,
    ensure_buckets,
    open_store,
    recover,
    s3_client,
    seal_part,
    sha256_file,
)

NOW = datetime(2026, 11, 14, tzinfo=UTC)


def row(ref: BlobRef, data: bytes, n: int) -> Capture:
    return Capture(
        capture_id=f"c{n}",
        job_id="job",
        source=Source.COMMON_CRAWL,
        crawl="CC-MAIN-2026-43",
        url=f"https://example.org/{n}",
        url_surtkey=None,
        upstream_fetch_time=None,
        fetched_at=NOW,
        upstream_warc_filename=None,
        upstream_offset=None,
        upstream_length=None,
        content_digest_sha1=None,
        raw_uri=ref.uri,
        raw_offset=ref.offset,
        raw_length=ref.length,
        raw_sha256=hashlib.sha256(data).hexdigest(),
        http_status=200,
        mime="text/html",
        outcome=Outcome.OK,
        verification=Verification.OK,
        strata_version="test",
    )


def write(writer: PartWriter, n: int, *, log: bool = True) -> Capture:
    data = f"record {n} ".encode() * (n + 1)
    cap = row(writer.append(data), data, n)
    if log:
        writer.log("page", cap)
    return cap


def test_rows_are_logged_then_sealed_into_the_store(tmp_path: Path) -> None:
    store = LocalBlobStore(tmp_path / "store")
    writer = PartWriter(tmp_path / "staging", store, "job")
    caps = [write(writer, n) for n in range(3)]
    writer.rotate()
    (part,) = writer.take_closed()
    assert part.key == "job/part-00000.warc.gz"
    assert part.manifest.read_text().count("\n") == 3

    seal_part(store, part)

    assert store.has(part.key, part.sha256)
    assert store.path("job/part-00000.idx.jsonl").read_bytes() == part.manifest.read_bytes()
    for cap in caps:  # rows point at the sealed copy, not the staged one
        ref = BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length)
        assert hashlib.sha256(store.read(ref)).hexdigest() == cap.raw_sha256
    seal_part(store, part)  # sealing again is a no-op, not an error


def test_rotate_without_rows_closes_nothing(tmp_path: Path) -> None:
    writer = PartWriter(tmp_path, LocalBlobStore(tmp_path), "job")
    writer.rotate()
    assert writer.take_closed() == []
    write(writer, 1, log=False)  # bytes with no row yet: only a forced rotate closes them
    writer.rotate()
    assert writer.take_closed() == []
    writer.rotate(force=True)
    assert len(writer.take_closed()) == 1


def test_recover_returns_every_durable_row_and_seals_unsealed_parts(tmp_path: Path) -> None:
    staging, store = tmp_path / "staging", LocalBlobStore(tmp_path / "store")
    writer = PartWriter(staging, store, "job")
    first = [write(writer, n) for n in range(2)]
    writer.rotate()
    seal_part(store, writer.take_closed()[0])
    second = [write(writer, n) for n in range(2, 5)]  # logged, never sealed: then the crash
    write(writer, 5, log=False)  # bytes on disk, the manifest line never written

    rows, dropped = recover(staging, store, "job")

    assert [r.capture.capture_id for r in rows] == [c.capture_id for c in first + second]
    assert {r.kind for r in rows} == {"page"}
    assert dropped == 0
    assert store.has("job/part-00001.warc.gz", sha256_file(staging / "job/part-00001.warc.gz"))
    # A writer started after the crash never appends to a part that may already be sealed.
    assert PartWriter(staging, store, "job").key == "job/part-00002.warc.gz"


def test_recover_drops_changed_bytes_and_a_torn_manifest_line(tmp_path: Path) -> None:
    staging, store = tmp_path / "staging", LocalBlobStore(tmp_path / "store")
    writer = PartWriter(staging, store, "job")
    caps = [write(writer, n) for n in range(3)]
    part = staging / "job/part-00000.warc.gz"
    data = bytearray(part.read_bytes())
    data[caps[1].raw_offset] ^= 0xFF  # one record's bytes no longer hash to its row
    part.write_bytes(bytes(data))
    manifest = staging / "job/part-00000.idx.jsonl"
    with manifest.open("a") as fh:
        fh.write('{"kind": "page", "capture": {"capture_id": "c9"')  # cut short by the crash

    rows, dropped = recover(staging, store, "job")

    assert [r.capture.capture_id for r in rows] == ["c0", "c2"]
    assert dropped == 1


def test_recover_with_nothing_staged(tmp_path: Path) -> None:
    assert recover(tmp_path, LocalBlobStore(tmp_path), "job_none") == ([], 0)


# ---------------------------------------------------------------------------- S3 (moto)
def s3_store(s3_settings: dict[str, str]) -> tuple[S3BlobStore, Settings]:
    settings = Settings()
    assert ensure_buckets(settings) == [settings.s3_raw_bucket]
    assert ensure_buckets(settings) == []  # already there
    store = open_store(settings)
    assert isinstance(store, S3BlobStore)
    return store, settings


def test_s3_store_seals_parts_and_reads_ranges(tmp_path: Path, s3_settings: dict[str, str]) -> None:
    store, settings = s3_store(s3_settings)
    writer = PartWriter(tmp_path, store, "job_s3")
    caps = [write(writer, n) for n in range(3)]
    assert caps[0].raw_uri == f"s3://{settings.s3_raw_bucket}/job_s3/part-00000.warc.gz"
    writer.rotate()
    (part,) = writer.take_closed()

    seal_part(store, part)

    assert store.has(part.key, part.sha256)
    assert not store.has(part.key, "0" * 64)
    assert not store.has("job_s3/missing.warc.gz", part.sha256)
    for cap in caps:
        got = store.read(BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length))
        assert hashlib.sha256(got).hexdigest() == cap.raw_sha256
    head = s3_client(settings).head_object(
        Bucket=settings.s3_raw_bucket, Key=part.key, ChecksumMode="ENABLED"
    )
    assert head["ChecksumSHA256"]  # stored with the checksum the upload carried


@pytest.mark.parametrize("kind", ["file", "s3"])
def test_a_part_that_changed_since_its_rows_were_written_is_not_sealed(
    tmp_path: Path, kind: str, request: pytest.FixtureRequest
) -> None:
    if kind == "s3":
        store: LocalBlobStore | S3BlobStore = s3_store(request.getfixturevalue("s3_settings"))[0]
    else:
        store = LocalBlobStore(tmp_path / "store")
    source = tmp_path / "part.warc.gz"
    source.write_bytes(b"the bytes on disk")
    wrong = hashlib.sha256(b"what the rows were written against").hexdigest()
    with pytest.raises(OSError, match="content changed"):
        store.put("job/part-00000.warc.gz", source, wrong)
    assert not store.has("job/part-00000.warc.gz", sha256_file(source))


def test_recover_seals_into_s3(tmp_path: Path, s3_settings: dict[str, str]) -> None:
    store, _ = s3_store(s3_settings)
    writer = PartWriter(tmp_path, store, "job_r")
    caps = [write(writer, n) for n in range(2)]
    rows, dropped = recover(tmp_path, store, "job_r")
    assert [r.capture for r in rows] == caps and dropped == 0
    assert store.has("job_r/part-00000.warc.gz", sha256_file(tmp_path / "job_r/part-00000.warc.gz"))


def test_buckets_for_the_raw_store_and_the_lake_are_created_once(
    s3_settings: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = s3_settings["STRATA_S3_RAW_BUCKET"]
    monkeypatch.setenv("STRATA_LAKE_DATA_PATH", f"s3://{raw}-lake/bronze/")
    assert ensure_buckets(Settings()) == [raw, f"{raw}-lake"]
    assert ensure_buckets(Settings()) == []
    monkeypatch.setenv("STRATA_RAW_STORE", "file")
    monkeypatch.setenv("STRATA_LAKE_DATA_PATH", "data/lake")
    assert ensure_buckets(Settings()) == []  # nothing on S3: no client, no calls
