"""A threaded, Range-aware static file server that measures what a client really did.

pytest-httpserver answers one request at a time, so it can't show how many requests a client
keeps in flight. This one serves each request on its own thread, can hold each answer for a
moment so overlaps become visible, and records every request: method, path, Range, body bytes,
User-Agent and the peak number of requests open at once.
"""

from __future__ import annotations

import functools
import http.server
import io
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Served:
    method: str
    path: str
    range: str | None
    body_bytes: int
    user_agent: str | None


@dataclass
class RangeServer:
    root: Path
    delay_s: float = 0.0
    port: int = 0
    requests: list[Served] = field(default_factory=list)
    peak: int = 0
    _open: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def url(self, relative: str) -> str:
        return f"http://127.0.0.1:{self.port}/{relative.lstrip('/')}"

    def reset(self) -> None:
        with self._lock:
            self.requests.clear()
            self.peak = 0

    def _enter(self) -> None:
        with self._lock:
            self._open += 1
            self.peak = max(self.peak, self._open)

    def _leave(self, served: Served) -> None:
        with self._lock:
            self._open -= 1
            self.requests.append(served)


def _handler(server: RangeServer) -> type[http.server.SimpleHTTPRequestHandler]:
    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def send_head(self) -> Any:
            server._enter()
            body = 0
            try:
                if server.delay_s:
                    time.sleep(server.delay_s)
                path = Path(self.translate_path(self.path))
                if not path.is_file():
                    self.send_error(404)
                    return None
                data = path.read_bytes()
                rng = self.headers.get("Range")
                if self.command == "HEAD" or not rng:
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Accept-Ranges", "bytes")
                    self.end_headers()
                    body = 0 if self.command == "HEAD" else len(data)
                    return io.BytesIO(b"" if self.command == "HEAD" else data)
                first_s, last_s = rng.removeprefix("bytes=").split("-")
                if first_s == "":
                    first, last = max(0, len(data) - int(last_s)), len(data) - 1
                else:
                    first = int(first_s)
                    last = min(int(last_s) if last_s else len(data) - 1, len(data) - 1)
                chunk = data[first : last + 1]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {first}-{last}/{len(data)}")
                self.send_header("Content-Length", str(len(chunk)))
                self.end_headers()
                body = len(chunk)
                return io.BytesIO(chunk)
            finally:
                server._leave(
                    Served(
                        method=self.command,
                        path=self.path,
                        range=self.headers.get("Range"),
                        body_bytes=body,
                        user_agent=self.headers.get("User-Agent"),
                    )
                )

    return Handler


@contextmanager
def serve(root: Path, *, delay_s: float = 0.0) -> Iterator[RangeServer]:
    state = RangeServer(root=root, delay_s=delay_s)
    httpd = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(_handler(state), directory=str(root))
    )
    state.port = int(httpd.server_address[1])
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        httpd.shutdown()
        httpd.server_close()
