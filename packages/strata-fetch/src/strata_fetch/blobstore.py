"""Raw byte storage behind an interface: ``file://`` now, ``s3://`` in week 2.

Records are appended unchanged. Concatenated gzip members are a valid WARC file, so our copy can be
read by warcio, replay tools and Spark exactly like Common Crawl's own files.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.parse import urlparse
from urllib.request import url2pathname

from strata_core.models import BlobRef


class BlobStore(Protocol):
    def append(self, key: str, data: bytes) -> BlobRef:
        """Append bytes durably and return where they landed."""
        ...

    def read(self, ref: BlobRef) -> bytes: ...


class LocalBlobStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def _path(self, key: str) -> Path:
        rel = PurePosixPath(key)
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"blob keys must be relative and stay inside the store: {key!r}")
        return self.root.joinpath(*rel.parts)

    def append(self, key: str, data: bytes) -> BlobRef:
        if not data:
            raise ValueError("refusing to store an empty blob")
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as fh:
            offset = fh.seek(0, os.SEEK_END)
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())  # durable before any row points at it
        return BlobRef(uri=path.as_uri(), offset=offset, length=len(data))

    def read(self, ref: BlobRef) -> bytes:
        parsed = urlparse(ref.uri)
        if parsed.scheme != "file":
            raise ValueError(f"LocalBlobStore can't read {parsed.scheme}:// URIs")
        path = Path(url2pathname(parsed.path))
        with path.open("rb") as fh:
            fh.seek(ref.offset)
            data = fh.read(ref.length)
        if len(data) != ref.length:
            raise OSError(f"short read from {ref.uri} at {ref.offset}: {len(data)}/{ref.length}")
        return data


class RawWarcWriter:
    """Appends records to ``{prefix}/part-NNNNN.warc.gz``, rotating at ``rotate_bytes``."""

    def __init__(self, store: BlobStore, prefix: str, *, rotate_bytes: int = 100 * 1024 * 1024):
        self._store = store
        self._prefix = prefix.strip("/")
        self._rotate = rotate_bytes
        self._part = 0
        self._size = 0

    @property
    def current_key(self) -> str:
        return f"{self._prefix}/part-{self._part:05d}.warc.gz"

    def append(self, data: bytes) -> BlobRef:
        if self._size and self._size + len(data) > self._rotate:
            self._part += 1
            self._size = 0
        ref = self._store.append(self.current_key, data)
        self._size = ref.offset + ref.length
        return ref
