"""Raw bytes: staged on local disk as a write-ahead log, then sealed into a store (ADR-005).

Records are appended unchanged. Concatenated gzip members are a valid WARC file, so our copy can
be read by warcio, replay tools and Spark exactly like Common Crawl's own files.

A run writes ``{prefix}/part-NNNNN.warc.gz`` under the staging folder, and beside it
``part-NNNNN.idx.jsonl``: one line per stored record, holding the bronze row that will point at
it. Both are fsynced on every write, bytes first, so the manifest never names bytes that aren't
on disk. At each bronze flush the part is *sealed*: closed, uploaded to the store with its
SHA-256 (checked by the store and read back), and only then are its rows written to the lake.

That ordering is what crash-resume relies on (S12): after a crash, every manifest line is a row
whose bytes exist; ``recover`` re-checks each one, seals what wasn't sealed and returns the rows,
which the lake inserts idempotently. S3 has no append, so sealing a whole part is also what makes
the same code work on RustFS and AWS.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import urlparse
from urllib.request import url2pathname

from strata_core import logs
from strata_core.models import BlobRef, Capture
from strata_core.settings import Settings
from strata_fetch.crash import crashpoint

PART_SUFFIX = ".warc.gz"
MANIFEST_SUFFIX = ".idx.jsonl"

log = logs.get_logger(__name__)


def _safe_key(key: str) -> PurePosixPath:
    rel = PurePosixPath(key)
    if rel.is_absolute() or ".." in rel.parts or not rel.parts:
        raise ValueError(f"blob keys must be relative and stay inside the store: {key!r}")
    return rel


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class BlobStore(Protocol):
    def uri(self, key: str) -> str:
        """Where ``key`` lives once sealed: the URI bronze rows point at."""
        ...

    def put(self, key: str, source: Path, sha256: str) -> None:
        """Store a sealed local file under ``key``, verified against ``sha256``."""
        ...

    def has(self, key: str, sha256: str) -> bool:
        """Whether ``key`` is already stored with exactly this content."""
        ...

    def read(self, ref: BlobRef) -> bytes: ...


class LocalBlobStore:
    """Sealed parts in a folder. When it is the staging folder itself, sealing moves nothing."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def path(self, key: str) -> Path:
        return self.root.joinpath(*_safe_key(key).parts)

    def uri(self, key: str) -> str:
        return self.path(key).as_uri()

    def put(self, key: str, source: Path, sha256: str) -> None:
        target = self.path(key)
        if sha256_file(source) != sha256:
            raise OSError(f"{source}: content changed since its rows were written")
        if target.resolve() == source.resolve():
            return  # the staging folder is the store: the check above was the seal
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        shutil.copyfile(source, tmp)
        with tmp.open("rb+") as fh:
            os.fsync(fh.fileno())
        if sha256_file(tmp) != sha256:
            raise OSError(f"{target}: content changed while sealing")
        tmp.replace(target)

    def has(self, key: str, sha256: str) -> bool:
        target = self.path(key)
        return target.exists() and sha256_file(target) == sha256

    def read(self, ref: BlobRef) -> bytes:
        return read_blob(ref)


class S3BlobStore:
    """Sealed parts in an S3 bucket (RustFS locally, AWS later).

    Each seal checks two things: the staged file still hashes to the SHA-256 its rows were
    written against (checked here, before the upload), and the bytes arrived intact (the upload
    carries that SHA-256; S3 and RustFS refuse a body that doesn't match it, and HEAD must
    return it afterwards). A store that doesn't verify uploads still passes the second check,
    which is why the spike's ``bad-digest`` check records whether ours does."""

    def __init__(self, client: Any, bucket: str) -> None:
        self._s3 = client
        self.bucket = bucket

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{_safe_key(key).as_posix()}"

    def put(self, key: str, source: Path, sha256: str) -> None:
        if sha256_file(source) != sha256:
            raise OSError(f"{source}: content changed since its rows were written")
        checksum = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")
        with source.open("rb") as body:
            self._s3.put_object(
                Bucket=self.bucket,
                Key=_safe_key(key).as_posix(),
                Body=body,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum,
            )
        if not self.has(key, sha256):
            raise OSError(f"{self.uri(key)}: the stored object's checksum doesn't match")

    def has(self, key: str, sha256: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            head = self._s3.head_object(
                Bucket=self.bucket, Key=_safe_key(key).as_posix(), ChecksumMode="ENABLED"
            )
        except ClientError:
            return False
        expected = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")
        return bool(head.get("ChecksumSHA256") == expected)

    def read(self, ref: BlobRef) -> bytes:
        return read_blob(ref, self._s3)


def s3_client(settings: Settings) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
            # Send checksums only where S3 requires them: some S3-compatible stores refuse
            # boto3's newer CRC32 default. Our SHA-256 is sent explicitly on every upload.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def open_store(settings: Settings) -> BlobStore:
    if settings.raw_store == "s3":
        return S3BlobStore(s3_client(settings), settings.s3_raw_bucket)
    return LocalBlobStore(settings.raw_root)


def ensure_buckets(settings: Settings) -> list[str]:
    """Create the buckets the settings use (the raw store's, and the lake's when its data path is
    on S3) if they are missing; returns the ones created."""
    wanted = []
    if settings.raw_store == "s3":
        wanted.append(settings.s3_raw_bucket)
    if settings.lake_local_file is None and settings.lake_data_path.startswith("s3://"):
        wanted.append(urlparse(settings.lake_data_path).netloc)
    if not wanted:
        return []
    from botocore.exceptions import ClientError

    client = s3_client(settings)
    created = []
    for bucket in dict.fromkeys(wanted):
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError:
            client.create_bucket(Bucket=bucket)
            created.append(bucket)
    return created


def read_blob(ref: BlobRef, s3: Any = None) -> bytes:
    """Read a stored record by URI, from a local file or an S3 object."""
    parsed = urlparse(ref.uri)
    if parsed.scheme == "file":
        with Path(url2pathname(parsed.path)).open("rb") as fh:
            fh.seek(ref.offset)
            data = fh.read(ref.length)
    elif parsed.scheme == "s3":
        if s3 is None:
            raise ValueError(f"reading {ref.uri} needs an S3 client")
        last = ref.offset + ref.length - 1
        reply = s3.get_object(
            Bucket=parsed.netloc, Key=parsed.path.lstrip("/"), Range=f"bytes={ref.offset}-{last}"
        )
        data = reply["Body"].read()
    else:
        raise ValueError(f"unsupported blob URI: {ref.uri}")
    if len(data) != ref.length:
        raise OSError(f"short read from {ref.uri} at {ref.offset}: {len(data)}/{ref.length}")
    return bytes(data)


# ---------------------------------------------------------------------------- staged parts
@dataclass(frozen=True)
class Part:
    """A closed part waiting to be sealed: its key, local files and content hash."""

    key: str
    path: Path
    manifest: Path
    sha256: str


def _fsync_append(path: Path, data: bytes) -> int:
    with path.open("ab") as fh:
        offset = fh.seek(0, os.SEEK_END)
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    return offset


def part_number(path: Path) -> int:
    return int(path.name.removeprefix("part-").split(".", 1)[0])


class PartWriter:
    """Appends records to staged parts and their manifests.

    A part closes at each flush (``rotate``) or when the next record would pass
    ``rotate_bytes``; closed parts wait in ``take_closed`` until the flush seals them.
    """

    def __init__(
        self,
        staging_root: Path,
        store: BlobStore,
        prefix: str,
        *,
        rotate_bytes: int = 100 * 1024 * 1024,
    ) -> None:
        self._dir = staging_root.joinpath(*_safe_key(prefix).parts)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._store = store
        self._prefix = _safe_key(prefix).as_posix()
        self._rotate = rotate_bytes
        existing = [part_number(p) for p in self._dir.glob(f"part-*{PART_SUFFIX}")]
        self._part = max(existing) + 1 if existing else 0  # never append to an older part
        self._size = 0
        self._records = 0
        self._closed: list[Part] = []

    @property
    def store(self) -> BlobStore:
        return self._store

    @property
    def key(self) -> str:
        return f"{self._prefix}/part-{self._part:05d}{PART_SUFFIX}"

    def _local(self, suffix: str) -> Path:
        return self._dir / f"part-{self._part:05d}{suffix}"

    def append(self, data: bytes) -> BlobRef:
        """Write a record durably; the ref points where it will live once sealed."""
        if not data:
            raise ValueError("refusing to store an empty record")
        if self._size and self._size + len(data) > self._rotate:
            self._close(force=True)
        offset = _fsync_append(self._local(PART_SUFFIX), data)
        self._size = offset + len(data)
        crashpoint("after_append")
        return BlobRef(uri=self._store.uri(self.key), offset=offset, length=len(data))

    def log(self, kind: str, capture: Capture) -> None:
        """Record the row for bytes already appended: the manifest is the write-ahead log."""
        line = json.dumps({"kind": kind, "capture": capture.model_dump(mode="json")})
        _fsync_append(self._local(MANIFEST_SUFFIX), (line + "\n").encode("utf-8"))
        self._records += 1
        crashpoint("after_manifest")

    def rotate(self, *, force: bool = False) -> None:
        """Close the current part if it holds rows (or any bytes, with ``force``). Synchronous,
        so no record can land in a part after it was closed."""
        self._close(force=force)

    def take_closed(self) -> list[Part]:
        closed, self._closed = self._closed, []
        return closed

    def _close(self, *, force: bool) -> None:
        if self._records == 0 and not (force and self._size):
            return
        path = self._local(PART_SUFFIX)
        manifest = self._local(MANIFEST_SUFFIX)
        manifest.touch()
        self._closed.append(Part(self.key, path, manifest, sha256_file(path)))
        self._part += 1
        self._size = 0
        self._records = 0


def seal_part(store: BlobStore, part: Part) -> None:
    if not store.has(part.key, part.sha256):
        store.put(part.key, part.path, part.sha256)
    manifest_key = part.key.removesuffix(PART_SUFFIX) + MANIFEST_SUFFIX
    store.put(manifest_key, part.manifest, sha256_file(part.manifest))
    log.info("part.sealed", key=part.key, sha256=part.sha256)
    crashpoint("after_seal")


# ---------------------------------------------------------------------------- recovery
@dataclass(frozen=True)
class Recovered:
    kind: str  # "page" or "robots"
    capture: Capture


def read_manifest(path: Path) -> Iterator[tuple[str, Capture]]:
    """Manifest lines in order; a last line cut short by a crash is skipped."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
            yield str(entry["kind"]), Capture.model_validate(entry["capture"])
        except (ValueError, KeyError, TypeError):
            continue


def recover(staging_root: Path, store: BlobStore, prefix: str) -> tuple[list[Recovered], int]:
    """Rows a crashed run had made durable: every manifest entry whose bytes are in its part and
    still hash to the recorded SHA-256. Parts not yet in the store are sealed now.

    Returns the rows and how many manifest entries were dropped (bytes missing or changed).
    """
    folder = staging_root.joinpath(*_safe_key(prefix).parts)
    rows: list[Recovered] = []
    dropped = 0
    for part_path in sorted(folder.glob(f"part-*{PART_SUFFIX}")):
        manifest = part_path.with_name(part_path.name.removesuffix(PART_SUFFIX) + MANIFEST_SUFFIX)
        entries = list(read_manifest(manifest))
        if not entries:
            continue
        data = part_path.read_bytes()
        kept = []
        for kind, cap in entries:
            chunk = data[cap.raw_offset : cap.raw_offset + cap.raw_length]
            if len(chunk) == cap.raw_length and hashlib.sha256(chunk).hexdigest() == cap.raw_sha256:
                kept.append(Recovered(kind, cap))
            else:
                dropped += 1
        if kept:
            key = f"{_safe_key(prefix).as_posix()}/{part_path.name}"
            seal_part(store, Part(key, part_path, manifest, sha256_file(part_path)))
            rows += kept
    return rows, dropped
