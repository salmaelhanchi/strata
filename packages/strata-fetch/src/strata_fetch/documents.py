"""From stored pages to silver rows, checked against the publishers' own indexes (week 2, S16).

Two throwaway extractors, one per site, read the header fields a page states. Week 3's parser
registry replaces them; what stays is the check: every extracted value is normalized
(``strata_core.normalize``) and compared with the publisher's machine-readable index:

* ``https://peps.python.org/api/peps.json`` for PEP pages (peps.python.org/pep-NNNN/)
* ``https://www.rfc-editor.org/rfc-index.txt`` for RFC info pages (www.rfc-editor.org/info/rfcN/)

The extractors follow the labels the live pages showed on 2026-11-14: a PEP's heading
``PEP 457 - Title`` (an en dash on the page) and its ``Author / Status / Type / Created`` field
list; an RFC info page's ``RFC 4767: Title`` heading, ``Date published`` and ``Authors`` lines,
and a status shown in the header without a label.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field

from strata_core.models import BlobRef, Capture, Document
from strata_core.normalize import (
    PEP_STATUSES,
    RFC_STATUSES,
    normalize_space,
    normalize_status,
    parse_date,
    parse_pep_number,
    parse_rfc_number,
    people,
    person_key,
    title_key,
)
from strata_fetch.blobstore import BlobStore
from strata_fetch.warc import read_records

PEPS_JSON = "https://peps.python.org/api/peps.json"
RFC_INDEX = "https://www.rfc-editor.org/rfc-index.txt"


# ------------------------------------------------------------------------------ page text
class _Text(HTMLParser):
    """Visible text as lines: block elements break lines, inline ones don't."""

    BLOCK = frozenset(
        (
            "p", "div", "li", "dt", "dd", "tr", "td", "th", "h1", "h2", "h3", "h4", "h5", "h6",
            "br", "section", "header", "footer", "ul", "ol", "dl", "table", "article", "main",
            "nav", "aside", "title", "blockquote", "pre",
        )
    )  # fmt: skip
    SKIP = frozenset({"script", "style", "noscript", "template"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[str] = []
        self.headings: list[str] = []
        self._buf: list[str] = []
        self._skip = 0
        self._in_h1 = False
        self._h1: list[str] = []
        self.meta: dict[str, list[str]] = {}

    def _break(self) -> None:
        line = normalize_space("".join(self._buf))
        if line:
            self.lines.append(line)
        self._buf = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "meta":
            a = dict(attrs)
            key, content = a.get("name") or a.get("property"), a.get("content")
            if key and content:
                self.meta.setdefault(key.casefold(), []).append(normalize_space(content))
        if tag in self.SKIP:
            self._skip += 1
        elif tag in self.BLOCK:
            self._break()
        if tag == "h1":
            self._in_h1, self._h1 = True, []

    def handle_endtag(self, tag: str) -> None:
        if tag in self.SKIP:
            self._skip = max(0, self._skip - 1)
        elif tag in self.BLOCK:
            self._break()
        if tag == "h1" and self._in_h1:
            self._in_h1 = False
            heading = normalize_space("".join(self._h1))
            if heading:
                self.headings.append(heading)

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        self._buf.append(data)
        if self._in_h1:
            self._h1.append(data)


def _parse(html: str) -> _Text:
    parser = _Text()
    parser.feed(html)
    parser.close()
    parser._break()
    return parser


def page_text(html: str) -> tuple[list[str], list[str]]:
    parser = _parse(html)
    return parser.lines, parser.headings


def labelled(lines: Sequence[str], label: str) -> str | None:
    """The value after ``label:`` on its line, or on the next line when the label stands alone
    (a definition list renders ``Author:`` and its value as two blocks)."""
    want = label.casefold() + ":"
    for i, line in enumerate(lines):
        if line.casefold().startswith(want):
            rest = line[len(want) :].strip()
            if rest:
                return rest
            return lines[i + 1] if i + 1 < len(lines) else None
    return None


# ------------------------------------------------------------------------------ extractors
class Extracted(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: str
    number: int | None
    title: str | None
    status: str | None
    doc_type: str | None = None
    date: str | None = None
    date_precision: str | None = None
    authors: list[str] = Field(default_factory=list)
    extractor: str


def extract_pep(url: str, html: str) -> Extracted:
    lines, headings = page_text(html)
    heading = next((h for h in headings if re.match(r"^PEP\s*\d+", h)), None)
    title = re.sub(r"^PEP\s*\d+\s*[\u2013\u2014:-]\s*", "", heading) if heading else None
    status = labelled(lines, "Status")
    created = parse_date(labelled(lines, "Created") or "")
    author = labelled(lines, "Author") or labelled(lines, "Authors")
    kind = labelled(lines, "Type")
    return Extracted(
        kind="pep",
        number=parse_pep_number(url),
        title=title,
        status=normalize_status(status, PEP_STATUSES) if status else None,
        doc_type=normalize_space(kind).lower() if kind else None,
        date=created.value if created else None,
        date_precision=created.precision if created else None,
        authors=people(author) if author else [],
        extractor="pep-html/1",
    )


def _rfc_status(lines: Sequence[str]) -> str | None:
    """The status, which info pages show in the header without a label: the first line (before
    "About this RFC") that is exactly a status, else the first status named in the header."""
    end = next((i for i, line in enumerate(lines) if line.casefold().startswith("about this")), 0)
    header = lines[:end] if end else lines[:40]
    for line in header:
        found = normalize_status(line, RFC_STATUSES)
        if found:
            return found
    words = " ".join(header).casefold()
    for phrase in sorted(RFC_STATUSES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(phrase)}\b", words):
            return RFC_STATUSES[phrase]
    return None


def extract_rfc(url: str, html: str) -> Extracted:
    """Title, date and authors from the page's ``citation_*`` tags (Highwire Press metadata,
    which the redesigned info pages carry; their visible header has no date any more), falling
    back to the visible layout. The status is read from the visible header either way."""
    page = _parse(html)
    lines, meta = page.lines, page.meta
    number = parse_rfc_number(url)
    title = next(iter(meta.get("citation_title", [])), None)
    if title is None:
        for line in [*page.headings, *lines]:
            m = re.match(r"^RFC\s*0*(\d+)\s*:\s*(.+)$", line)
            if m and (number is None or int(m.group(1)) == number):
                title = m.group(2)
                break
    tagged = next(iter(meta.get("citation_publication_date", [])), None)
    published = parse_date(tagged.replace("/", "-") if tagged else "") or parse_date(
        labelled(lines, "Date published") or ""
    )
    authors = meta.get("citation_author") or people(
        labelled(lines, "Authors") or labelled(lines, "Author") or ""
    )
    return Extracted(
        kind="rfc",
        number=number,
        title=title,
        status=_rfc_status(lines),
        date=published.value if published else None,
        date_precision=published.precision if published else None,
        authors=list(authors),
        extractor="rfc-info-html/2",
    )


Extractor = Callable[[str, str], Extracted]


def extractor_for(url: str) -> Extractor | None:
    parts = urlsplit(url)
    host, path = (parts.hostname or "").lower(), parts.path
    if host == "peps.python.org" and re.fullmatch(r"/pep-\d{4}/?", path):
        return extract_pep
    if host == "www.rfc-editor.org" and re.fullmatch(r"/info/rfc\d+/?", path):
        return extract_rfc
    return None


def html_of(record: bytes) -> str:
    """The archived response body of one WARC record, decoded as the page declares (or UTF-8)."""
    records = read_records(record)
    if not records:
        return ""
    rec, body = records[0]
    http = getattr(rec, "http_headers", None)
    ctype = http.get_header("Content-Type") if http is not None else None
    m = re.search(r"charset=([\w-]+)", ctype or "", re.IGNORECASE)
    try:
        return body.decode(m.group(1) if m else "utf-8", errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def documents(
    captures: Sequence[Capture], store: BlobStore, *, job_id: str | None = None
) -> tuple[list[Document], int]:
    """Silver rows for every capture an extractor knows; also returns how many it didn't.

    ``job_id``: the job the rows are normalized for (its selected pages may have been stored by
    an earlier job); by default each capture's own job."""
    out: list[Document] = []
    unknown = 0
    now = datetime.now(UTC)
    for cap in captures:
        extract = extractor_for(cap.url)
        if extract is None:
            unknown += 1
            continue
        record = store.read(BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length))
        got = extract(cap.url, html_of(record))
        out.append(
            Document(
                capture_id=cap.capture_id,
                job_id=job_id or cap.job_id,
                url=cap.url,
                extracted_at=now,
                **got.model_dump(),
            )
        )
    return out, unknown


# ------------------------------------------------------------------------------ oracles
class OracleEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    number: int
    title: str | None = None
    status: str | None = None
    doc_type: str | None = None
    date: str | None = None
    authors: list[str] = Field(default_factory=list)
    text: str | None = None  # the whole index entry, when titles can only be matched as prefixes


def parse_peps_json(data: bytes) -> dict[int, OracleEntry]:
    out: dict[int, OracleEntry] = {}
    for key, pep in json.loads(data).items():
        created = parse_date(str(pep.get("created") or ""))
        out[int(key)] = OracleEntry(
            number=int(key),
            title=pep.get("title"),
            status=normalize_status(str(pep.get("status") or ""), PEP_STATUSES),
            doc_type=str(pep.get("type") or "").lower() or None,
            date=created.value if created else None,
            authors=people(str(pep.get("authors") or "")),
        )
    return out


_ENTRY = re.compile(r"^(\d{1,5}) (.*)$")  # "19 ..." and "0019 ..." both occur
_DATE = re.compile(
    r"\b(?:\d{1,2} )?(?:January|February|March|April|May|June|July|August|September|October"
    r"|November|December) \d{4}\b"
)


def parse_rfc_index(text: str) -> dict[int, OracleEntry]:
    """``rfc-index.txt``: one entry per RFC, starting with its number (zero-padded in older
    copies) at the start of a line and continuing on indented lines; ``Not Issued.`` entries
    are skipped."""
    entries: dict[int, list[str]] = {}
    current: int | None = None
    for line in text.splitlines():
        if m := _ENTRY.match(line):
            current = int(m.group(1))
            entries[current] = [m.group(2)]
        elif current is not None and line.startswith(" ") and line.strip():
            entries[current].append(line.strip())
        elif not line.strip():
            current = None
    out: dict[int, OracleEntry] = {}
    for number, parts in entries.items():
        body = normalize_space(" ".join(parts))
        if body.startswith("Not Issued"):
            continue
        head = re.split(r"\((?:Format|Status|Obsoletes|Updates|Also)\b", body, maxsplit=1)[0]
        dates = _DATE.findall(head)
        when = parse_date(dates[-1]) if dates else None
        status = re.search(r"\(Status: ([^)]+)\)", body)
        out[number] = OracleEntry(
            number=number,
            status=normalize_status(status.group(1), RFC_STATUSES) if status else None,
            date=when.value if when else None,
            text=body,
        )
    return out


class OracleFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str
    path: str
    fetched_at: datetime
    status: int
    bytes: int
    sha256: str
    user_agent: str


async def fetch_oracle(client: httpx.AsyncClient, url: str, folder: Path) -> OracleFile:
    """Download a publisher's index once, keeping the bytes and where and when they came from."""
    reply = await client.get(url)
    reply.raise_for_status()
    name = urlsplit(url).path.rsplit("/", 1)[-1]
    target = folder / name
    info = OracleFile(
        url=url,
        path=target.as_posix(),
        fetched_at=datetime.now(UTC),
        status=reply.status_code,
        bytes=len(reply.content),
        sha256=hashlib.sha256(reply.content).hexdigest(),
        user_agent=reply.request.headers.get("User-Agent", ""),
    )

    def save() -> None:
        folder.mkdir(parents=True, exist_ok=True)
        target.write_bytes(reply.content)
        (folder / f"{name}.provenance.json").write_text(info.model_dump_json(indent=2), "utf-8")

    await asyncio.to_thread(save)
    return info


async def ensure_oracles(client: httpx.AsyncClient, folder: Path) -> list[OracleFile]:
    """Download the publishers' indexes that ``folder`` lacks; returns the ones downloaded. A
    file is kept with its provenance and not fetched again: delete both to refresh it."""
    fetched = []
    for url in (PEPS_JSON, RFC_INDEX):
        name = urlsplit(url).path.rsplit("/", 1)[-1]
        if not (folder / f"{name}.provenance.json").exists():
            fetched.append(await fetch_oracle(client, url, folder))
    return fetched


def load_oracles(folder: Path) -> tuple[dict[int, OracleEntry], dict[int, OracleEntry]]:
    peps = folder / "peps.json"
    rfcs = folder / "rfc-index.txt"
    return (
        parse_peps_json(peps.read_bytes()) if peps.exists() else {},
        parse_rfc_index(rfcs.read_text("utf-8", errors="replace")) if rfcs.exists() else {},
    )


# ------------------------------------------------------------------------------ agreement
class FieldAgreement(BaseModel):
    model_config = ConfigDict(frozen=True)

    compared: int = 0
    agreed: int = 0

    @property
    def rate(self) -> float | None:
        return None if self.compared == 0 else self.agreed / self.compared


class Agreement(BaseModel):
    """How often a normalized page value equals the publisher's index, field by field."""

    model_config = ConfigDict(frozen=True)

    documents: int
    not_in_oracle: int
    fields: dict[str, FieldAgreement]
    disagreements: list[str]  # a few examples, "url: field page=... index=..."

    @property
    def core_rate(self) -> float | None:
        """Agreement on the gate's fields: title, status and date."""
        core = [self.fields[k] for k in self.fields if k.split(".")[1] in CORE_FIELDS]
        compared = sum(f.compared for f in core)
        return None if compared == 0 else sum(f.agreed for f in core) / compared


CORE_FIELDS = ("title", "status", "date")


def agreement(
    docs: Sequence[Document],
    peps: dict[int, OracleEntry],
    rfcs: dict[int, OracleEntry],
    *,
    examples: int = 10,
) -> Agreement:
    counts: dict[str, list[int]] = {}
    missing = 0
    shown: list[str] = []

    def check(doc: Document, name: str, ok: bool, page: object, index: object) -> None:
        key = f"{doc.kind}.{name}"
        c = counts.setdefault(key, [0, 0])
        c[0] += 1
        c[1] += int(ok)
        if not ok and len(shown) < examples:
            shown.append(f"{doc.url}: {name} page={page!r} index={index!r}")

    for doc in docs:
        oracle = (peps if doc.kind == "pep" else rfcs).get(doc.number or -1)
        if oracle is None:
            missing += 1
            continue
        if doc.kind == "pep":
            check(doc, "title", _same(doc.title, oracle.title), doc.title, oracle.title)
            check(doc, "type", doc.doc_type == oracle.doc_type, doc.doc_type, oracle.doc_type)
        else:
            text = oracle.text or ""
            ok = bool(doc.title) and title_key(text).startswith(title_key(doc.title or "") + ".")
            check(doc, "title", ok, doc.title, text[:80])
        check(doc, "status", doc.status == oracle.status, doc.status, oracle.status)
        check(doc, "date", same_date(doc.date, oracle.date), doc.date, oracle.date)
        if oracle.authors:
            ours = {person_key(a) for a in doc.authors}
            theirs = {person_key(a) for a in oracle.authors}
            check(doc, "authors", ours == theirs, sorted(ours), sorted(theirs))
    return Agreement(
        documents=len(docs),
        not_in_oracle=missing,
        fields={k: FieldAgreement(compared=c, agreed=a) for k, (c, a) in sorted(counts.items())},
        disagreements=shown,
    )


def same_date(a: str | None, b: str | None) -> bool:
    """ISO dates agree at the precision both have: the RFC index dates most RFCs to the month
    ("2022-10"), the page's metadata to the day ("2022-10-20")."""
    if a is None or b is None:
        return False
    n = min(len(a), len(b))
    return a[:n] == b[:n]


def _same(a: str | None, b: str | None) -> bool:
    return a is not None and b is not None and title_key(a) == title_key(b)
