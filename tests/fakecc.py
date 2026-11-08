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

import io
import json
import re
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from pytest_httpserver import HTTPServer, URIPattern
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from werkzeug import Request, Response

from strata_core.models import CaptureRef
from strata_fetch.surt import host_surt

CRAWL = "CC-MAIN-2026-17"


def build_record(url: str, body: bytes, *, content_type: str = "text/html") -> tuple[bytes, str]:
    """One gzip-compressed WARC response record and its WARC-Payload-Digest."""
    out = io.BytesIO()
    writer = WARCWriter(out, gzip=True)
    http_headers = StatusAndHeaders(
        "200 OK",
        [("Content-Type", content_type), ("Content-Length", str(len(body)))],
        protocol="HTTP/1.1",
    )
    record = writer.create_warc_record(
        url, "response", payload=io.BytesIO(body), http_headers=http_headers
    )
    writer.write_record(record)
    digest = record.rec_headers.get_header("WARC-Payload-Digest")
    return out.getvalue(), digest


def surtkey(url: str) -> str:
    parsed = urlparse(url)
    return host_surt(parsed.hostname or "") + (parsed.path or "/")


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


@dataclass
class FakeCommonCrawl:
    server: HTTPServer
    files: dict[str, bytes] = field(default_factory=dict)
    cdx_refs: dict[str, list[CaptureRef]] = field(default_factory=dict)  # crawl -> refs
    fail_next: dict[str, deque[tuple[Any, ...]]] = field(default_factory=lambda: defaultdict(deque))
    ignore_range: bool = False
    requests: list[tuple[float, str, str | None]] = field(default_factory=list)
    queries: list[list[tuple[str, str]]] = field(default_factory=list)  # query args per request

    @property
    def base_url(self) -> str:
        return self.server.url_for("/")

    def add_warc(self, warc: FakeWarc) -> None:
        self.files["/" + warc.filename] = warc.data

    def install(self) -> None:
        self.server.expect_request(_AnyPath()).respond_with_handler(self._handle)

    def count(self, path_prefix: str = "/") -> int:
        return sum(1 for _, p, _ in self.requests if p.startswith(path_prefix))

    def _handle(self, request: Request) -> Response:
        self.requests.append((time.monotonic(), request.path, request.headers.get("Range")))
        self.queries.append(list(request.args.items(multi=True)))
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
            return Response(data, 200)
        first_s, last_s = range_header.removeprefix("bytes=").split("-")
        if first_s == "":  # suffix range: the last N bytes
            first, last = max(0, len(data) - int(last_s)), len(data) - 1
        else:
            first, last = int(first_s), int(last_s)
        if first >= len(data):
            return Response("", 416)
        chunk = data[first : last + 1]
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
