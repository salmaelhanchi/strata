"""From a stored WARC record to a ``strata_pages`` row: title, main text, language, out-links.

The main text comes from trafilatura (boilerplate removed); title, language and links come from
the page itself: ``<title>`` (else ``og:title``, else the top headings: ``<h1>`` elements or ones
styled ``class="h1"``, as RFCs converted from plain text are), ``<html lang>``, and every
``<a href>`` resolved against the page URL.
The normalizer's version is stored on every row, so rows made by an older one can be found.
"""

from __future__ import annotations

import warnings
from html.parser import HTMLParser
from importlib.metadata import version
from urllib.parse import urldefrag, urljoin, urlsplit

from strata_core.models import Capture
from strata_core.normalize import normalize_space
from strata_core.sinks import PageRow
from strata_fetch.documents import html_of

NORMALIZER_VERSION = f"strata-pages/1+trafilatura-{version('trafilatura')}"


class _Head(HTMLParser):
    def __init__(self, base: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base = base
        self.lang: str | None = None
        self.title: list[str] = []
        self.og_title: str | None = None
        self.links: dict[str, None] = {}  # ordered set
        self.headings: list[str] = []
        self._in_title = False
        self._heading: tuple[str, int] | None = None  # (tag, nesting) while inside a top heading
        self._buf: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if self._heading is not None and tag == self._heading[0]:
            self._heading = (tag, self._heading[1] + 1)
        elif self._heading is None and (tag == "h1" or "h1" in (a.get("class") or "").split()):
            self._heading, self._buf = (tag, 1), []
        if tag == "html" and a.get("lang"):
            self.lang = (a["lang"] or "").strip().lower() or None
        elif tag == "title":
            self._in_title = True
        elif tag == "base" and a.get("href"):
            self.base = urljoin(self.base, a["href"] or "")
        elif tag == "meta" and (a.get("property") or "").lower() == "og:title":
            self.og_title = a.get("content")
        elif tag == "a" and a.get("href"):
            self._link(a["href"] or "")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if self._heading is not None and tag == self._heading[0]:
            depth = self._heading[1] - 1
            self._heading = (tag, depth) if depth else None
            if not depth and (text := normalize_space("".join(self._buf))):
                self.headings.append(text)

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title.append(data)
        if self._heading is not None:
            self._buf.append(data)

    def _link(self, href: str) -> None:
        try:
            url, _ = urldefrag(urljoin(self.base, href.strip()))
        except ValueError:
            return  # a malformed href (an unclosed IPv6 bracket, say) is not a link
        if urlsplit(url).scheme in ("http", "https"):
            self.links[url] = None


def main_text(html: str) -> str | None:
    with warnings.catch_warnings():  # trafilatura's download helpers warn about SOCKS support
        warnings.simplefilter("ignore")
        import trafilatura

    return trafilatura.extract(
        html, include_comments=False, include_tables=True, favor_precision=True
    )


def normalize(capture: Capture, record: bytes, *, job_id: str | None = None) -> PageRow:
    """``job_id``: the job that pulled the page (it may have been stored by an earlier one)."""
    html = html_of(record)
    head = _Head(capture.url)
    try:
        head.feed(html)
        head.close()
    except AssertionError:  # html.parser gives up on some broken markup; keep what it read
        pass
    title = (
        normalize_space("".join(head.title))
        or normalize_space(head.og_title or "")
        or normalize_space(" ".join(head.headings))
        or None
    )
    offset, length = capture.upstream_offset, capture.upstream_length
    return PageRow(
        url=capture.url,
        crawl=capture.crawl or "",
        captured_at=capture.upstream_fetch_time,
        http_status=capture.http_status,
        title=title,
        main_text=main_text(html) if html else None,
        lang=head.lang,
        out_links=list(head.links),
        payload_sha1=capture.content_digest_sha1,
        warc_ref=f"{capture.upstream_warc_filename}#{offset},{length}",
        robots_verdict=capture.policy_verdict.value,
        capture_id=capture.capture_id,
        job_id=job_id or capture.job_id,
        normalizer_version=NORMALIZER_VERSION,
    )
