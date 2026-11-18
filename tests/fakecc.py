"""A fake Common Crawl for tests.

Real gzip WARC records, a CDX endpoint and Range-aware file serving.

The CDX endpoint applies filters with pywb's documented semantics (no modifier: contains; ``=``:
exact; ``~``: regex matched at the start of the field; ``!``: negation) and, like pywb, answers a
regex that doesn't compile with a 500 "Internal Error". A fake that encodes our own assumptions
instead lets a wrong assumption pass every test (it did once: ``!~url:?``).

Failures are scripted per path (``fail_next``: ``(status, headers)`` or ``(status, headers,
body)``), and every request is recorded, so tests can assert exactly how many requests were made.
"""

from __future__ import annotations

import gzip
import io
import json
import re
import tempfile
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import duckdb
from pytest_httpserver import HTTPServer, URIPattern
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from werkzeug import Request, Response

from strata_core.models import CaptureRef
from strata_fetch.surt import url_key

CRAWL = "CC-MAIN-2026-17"


def build_record(
    url: str,
    body: bytes,
    *,
    content_type: str = "text/html",
    status: str = "200 OK",
    headers: list[tuple[str, str]] | None = None,
) -> tuple[bytes, str]:
    """One gzip-compressed WARC response record and its WARC-Payload-Digest."""
    out = io.BytesIO()
    writer = WARCWriter(out, gzip=True)
    http_headers = StatusAndHeaders(
        status,
        [("Content-Type", content_type), ("Content-Length", str(len(body))), *(headers or [])],
        protocol="HTTP/1.1",
    )
    record = writer.create_warc_record(
        url, "response", payload=io.BytesIO(body), http_headers=http_headers
    )
    writer.write_record(record)
    digest = record.rec_headers.get_header("WARC-Payload-Digest")
    return out.getvalue(), digest


def surtkey(url: str) -> str:
    """Keys as Common Crawl writes them (the surt library's rules: www dropped, path lowered)."""
    return url_key(url)


@dataclass
class FakeWarc:
    filename: str
    data: bytes
    refs: list[CaptureRef]


def build_warc(pages: list[tuple[str, bytes]], *, crawl: str = CRAWL, n: int = 0) -> FakeWarc:
    filename = f"crawl-data/{crawl}/segments/1/warc/{crawl}-0000{n}.warc.gz"
    data = bytearray()
    refs: list[CaptureRef] = []
    for i, (url, body) in enumerate(pages):
        record, digest = build_record(url, body)
        refs.append(
            CaptureRef(
                crawl=crawl,
                url=url,
                url_surtkey=surtkey(url),
                fetch_time=None,
                warc_filename=filename,
                warc_record_offset=len(data),
                warc_record_length=len(record),
                content_digest=digest.removeprefix("sha1:"),
                mime="text/html",
                status=200,
            )
        )
        data.extend(record)
        _ = i
    return FakeWarc(filename=filename, data=bytes(data), refs=refs)


def cdx_line(ref: CaptureRef) -> str:
    return json.dumps(
        {
            "urlkey": ref.url_surtkey,
            "timestamp": ref.fetch_time.strftime("%Y%m%d%H%M%S") if ref.fetch_time else None,
            "url": ref.url,
            "mime": ref.mime,
            "mime-detected": ref.mime,
            "status": str(ref.status),
            "digest": ref.content_digest,
            "length": str(ref.warc_record_length),
            "offset": str(ref.warc_record_offset),
            "filename": ref.warc_filename,
        }
    )


INDEX_PATH = "cc-index/table/cc-main/warc/crawl={crawl}/subset={subset}/part-00000.parquet"


@dataclass(frozen=True)
class IndexRow:
    """One URL-index row: a capture and what the crawler got back."""

    ref: CaptureRef
    redirect: str | None = None


def build_url_index(refs: list[CaptureRef] | list[IndexRow]) -> bytes:
    """A URL-index shard (Parquet, Common Crawl's column names and types) listing ``refs``."""
    rows = [r if isinstance(r, IndexRow) else IndexRow(r) for r in refs]
    with tempfile.TemporaryDirectory() as tmp:
        path = f"{tmp}/part.parquet"
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE t (url_surtkey VARCHAR, url VARCHAR, url_host_name VARCHAR,"
            " url_host_registered_domain VARCHAR, fetch_time TIMESTAMPTZ, fetch_status SMALLINT,"
            " fetch_redirect VARCHAR, content_digest VARCHAR, content_mime_type VARCHAR,"
            " warc_filename VARCHAR, warc_record_offset INTEGER, warc_record_length INTEGER)"
        )
        for i, row in enumerate(sorted(rows, key=lambda r: r.ref.url_surtkey or "")):
            ref = row.ref
            host = urlparse(ref.url).hostname or ""
            when = ref.fetch_time or f"2026-04-20 10:{i % 60:02d}:00+00"
            con.execute(
                "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ref.url_surtkey,
                    ref.url,
                    host,
                    ".".join(host.split(".")[-2:]),
                    when,
                    ref.status or 200,
                    row.redirect,
                    ref.content_digest,
                    ref.mime or "text/html",
                    ref.warc_filename,
                    ref.warc_record_offset,
                    ref.warc_record_length,
                ],
            )
        con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
        con.close()
        with open(path, "rb") as fh:  # noqa: PTH123
            return fh.read()


ROBOTS_ALLOW_ALL = b"User-agent: *\nAllow: /\n"
_STATUS_TEXT = {
    200: "OK",
    301: "Moved Permanently",
    302: "Found",
    404: "Not Found",
    503: "Unavailable",
}


@dataclass
class FakeCommonCrawl:
    server: HTTPServer
    files: dict[str, bytes] = field(default_factory=dict)
    cdx_refs: dict[str, list[CaptureRef]] = field(default_factory=dict)  # crawl -> refs
    fail_next: dict[str, deque[tuple[Any, ...]]] = field(default_factory=lambda: defaultdict(deque))
    ignore_range: bool = False
    requests: list[tuple[float, str, str | None]] = field(default_factory=list)
    queries: list[list[tuple[str, str]]] = field(default_factory=list)  # query args per request
    user_agents: list[str | None] = field(default_factory=list)
    served: list[tuple[str, str, int]] = field(default_factory=list)  # (method, path, body bytes)
    index_files: dict[str, dict[str, str]] = field(default_factory=dict)  # crawl -> subset -> path
    robots_rows: dict[str, list[IndexRow]] = field(default_factory=dict)  # crawl -> rows
    robots_warc: dict[str, bytearray] = field(default_factory=dict)  # crawl -> WARC bytes

    @property
    def base_url(self) -> str:
        return self.server.url_for("/")

    def add_warc(self, warc: FakeWarc) -> None:
        self.files["/" + warc.filename] = warc.data

    def add_url_index(
        self, refs: list[CaptureRef] | list[IndexRow], crawl: str = CRAWL, subset: str = "warc"
    ) -> str:
        """Publish a one-file URL index for ``crawl`` and ``subset``, and the manifest."""
        path = INDEX_PATH.format(crawl=crawl, subset=subset)
        self.files["/" + path] = build_url_index(refs)
        self.index_files.setdefault(crawl, {})[subset] = path
        listed = "".join(p + "\n" for p in self.index_files[crawl].values())
        self.files[f"/crawl-data/{crawl}/cc-index-table.paths.gz"] = gzip.compress(listed.encode())
        return path

    def add_robots(
        self,
        host: str,
        body: bytes = ROBOTS_ALLOW_ALL,
        *,
        status: int = 200,
        redirect: str | None = None,
        crawl: str = CRAWL,
        scheme: str = "https",
        fetch_time: datetime | None = None,
    ) -> CaptureRef:
        """Publish a robots.txt capture for ``host``: a WARC record and a row in the
        ``robotstxt`` subset of the URL index, as Common Crawl does for every host it crawls."""
        url = f"{scheme}://{host}/robots.txt"
        extra = [("Location", redirect)] if redirect else []
        record, digest = build_record(
            url,
            body,
            content_type="text/plain",
            status=f"{status} {_STATUS_TEXT.get(status, 'Status')}",
            headers=extra,
        )
        filename = f"crawl-data/{crawl}/segments/1/robotstxt/{crawl}-robots-00000.warc.gz"
        data = self.robots_warc.setdefault(crawl, bytearray())
        ref = CaptureRef(
            crawl=crawl,
            url=url,
            url_surtkey=url_key(url),
            warc_filename=filename,
            warc_record_offset=len(data),
            warc_record_length=len(record),
            content_digest=digest.removeprefix("sha1:"),
            mime="text/plain",
            status=status,
            fetch_time=fetch_time,
        )
        data.extend(record)
        self.files["/" + filename] = bytes(data)
        self.robots_rows.setdefault(crawl, []).append(IndexRow(ref, redirect))
        self.add_url_index(self.robots_rows[crawl], crawl, subset="robotstxt")
        return ref

    def install(self) -> None:
        self.server.expect_request(_AnyPath()).respond_with_handler(self._handle)

    def count(self, path_prefix: str = "/") -> int:
        return sum(1 for _, p, _ in self.requests if p.startswith(path_prefix))

    def _handle(self, request: Request) -> Response:
        self.requests.append((time.monotonic(), request.path, request.headers.get("Range")))
        self.queries.append(list(request.args.items(multi=True)))
        self.user_agents.append(request.headers.get("User-Agent"))
        scripted = self.fail_next.get(request.path)
        if scripted:
            status, headers, *body = scripted.popleft()
            return Response(
                body[0] if body else f"scripted {status}", status=status, headers=headers
            )
        if request.path.endswith("-index"):
            return self._cdx(request)
        data = self.files.get(request.path)
        if data is None:
            return Response("not found", 404)
        range_header = request.headers.get("Range")
        if self.ignore_range or not range_header:
            body_bytes = 0 if request.method == "HEAD" else len(data)
            self.served.append((request.method, request.path, body_bytes))
            return Response(data, 200)
        first_s, last_s = range_header.removeprefix("bytes=").split("-")
        if first_s == "":  # suffix range: the last N bytes
            first, last = max(0, len(data) - int(last_s)), len(data) - 1
        else:
            first, last = int(first_s), int(last_s)
        if first >= len(data):
            return Response("", 416)
        chunk = data[first : last + 1]
        self.served.append((request.method, request.path, len(chunk)))
        return Response(
            chunk,
            206,
            headers={"Content-Range": f"bytes {first}-{first + len(chunk) - 1}/{len(data)}"},
        )

    def _cdx(self, request: Request) -> Response:
        crawl = request.path.strip("/").removesuffix("-index")
        refs = self.cdx_refs.get(crawl)
        if not refs:
            return Response("No Captures found for: " + request.args.get("url", ""), 404)
        try:
            checks = [pywb_filter(spec) for spec in request.args.getlist("filter")]
        except re.error as exc:  # what pywb does with a regex that doesn't compile
            message = json.dumps({"message": f"Internal Error: {exc}"})
            return Response(message, 500, content_type="application/json")
        rows = [json.loads(cdx_line(r)) for r in refs]
        rows = [row for row in rows if all(check(row) for check in checks)]
        limit = int(request.args.get("limit", "1000"))  # applied after the filters, as in pywb
        return Response("".join(json.dumps(row) + "\n" for row in rows[:limit]), 200)


def pywb_filter(spec: str) -> Callable[[dict[str, Any]], bool]:
    """pywb's ``[!][=|~]field:expression`` filter as a predicate over a CDX JSON row."""
    invert = spec.startswith("!")
    body = spec.removeprefix("!")
    mode = body[:1] if body[:1] in ("=", "~") else ""
    field_name, _, expression = body[len(mode) :].partition(":")
    regex = re.compile(expression) if mode == "~" else None  # raises re.error, like pywb

    def check(row: dict[str, Any]) -> bool:
        value = str(row.get(field_name, ""))
        if regex is not None:
            hit = regex.match(value) is not None
        elif mode == "=":
            hit = value == expression
        else:
            hit = expression in value
        return hit != invert

    return check


class _AnyPath(URIPattern):
    """Matches every path, so one handler serves the whole fake site."""

    def match(self, uri: str) -> bool:
        return True
