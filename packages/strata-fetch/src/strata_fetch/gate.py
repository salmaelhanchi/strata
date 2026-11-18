"""Gate 1: one command acquires the pages a targets file asks for, and one
lineage query proves every row.

The targets file is TOML::

    crawl = "CC-MAIN-2026-43"
    budget = 1200
    index = "columnar"              # optional; auto, cdx or columnar

    [[target]]
    site = "peps.python.org"
    pattern = '^https://peps\\.python\\.org/pep-\\d{4}/$'
    pages = 175

``gate_check`` joins every row the job selected to the robots.txt capture its verdict rests on
(one SQL query), then reads each row's stored bytes back and checks them. A job's rows are the
captures it selected, whichever job stored them first: a rerun reuses stored pages instead of
fetching them again, and is proven the same way.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import tomllib
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from strata_core.lake import CAPTURE_COLUMNS, CATALOG, Lake
from strata_core.models import (
    BlobRef,
    Capture,
    CrawlSpec,
    Document,
    JobState,
    JobSummary,
    PolicyVerdict,
    RunReport,
    Target,
)
from strata_core.ops import OpsStore
from strata_fetch.blobstore import BlobStore
from strata_fetch.documents import Agreement, agreement, documents, load_oracles
from strata_fetch.surt import normalise_domain, on_site
from strata_fetch.warc import read_records, sha1_b32

AGREEMENT_TARGET = 0.95


def load_targets(path: Path) -> CrawlSpec:
    data: dict[str, Any] = tomllib.loads(path.read_text("utf-8"))
    targets = data.pop("target", [])
    return CrawlSpec.model_validate({**data, "targets": targets})


class GateCheck(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    passed: bool | None  # None: informational, or could not run
    detail: str


class GateReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    rows: int
    checks: list[GateCheck]

    @property
    def passed(self) -> bool:
        return all(c.passed is not False for c in self.checks)


# One query: every page row the job selected beside the robots.txt capture its verdict rests on.
_LINEAGE_SQL = f"""
SELECT {", ".join("c." + col for col in CAPTURE_COLUMNS)},
       r.capture_id AS robots_found, r.raw_uri, r.raw_offset, r.raw_length, r.raw_sha256
FROM {CATALOG}.bronze.captures c
LEFT JOIN {CATALOG}.bronze.robots r
       ON r.capture_id = c.robots_capture_id
WHERE list_contains(?::VARCHAR[], c.capture_id)
ORDER BY c.capture_id
"""


def lineage_rows(
    lake: Lake, capture_ids: Sequence[str]
) -> list[tuple[Capture, BlobRef | None, str | None]]:
    rows = lake.con.execute(_LINEAGE_SQL, [list(capture_ids)]).fetchall()
    out = []
    n = len(CAPTURE_COLUMNS)
    for row in rows:
        cap = Capture(**dict(zip(CAPTURE_COLUMNS, row[:n], strict=True)))
        found, uri, offset, length, sha = row[n:]
        robots = (
            BlobRef(uri=uri, offset=offset, length=length) if found and uri is not None else None
        )
        out.append((cap, robots, sha))
    return out


def classify_digest(record: bytes) -> str:
    """Why a recomputed payload digest differs from the WARC header, if it does: ``match``,
    ``truncated`` (WARC-Truncated says so) or ``unexplained``."""
    records = read_records(record)
    if not records:
        return "unexplained"
    rec, payload = records[0]
    headers = rec.rec_headers  # type: ignore[attr-defined]
    if headers.get_header("WARC-Payload-Digest") == "sha1:" + sha1_b32(payload):
        return "match"
    return "truncated" if headers.get_header("WARC-Truncated") else "unexplained"


def _belongs(url: str, target: Target) -> bool:
    if not on_site(url, normalise_domain(target.site)):
        return False
    if not target.pattern:
        return True
    try:
        # Patterns are RE2 (DuckDB's dialect); the patterns RE2 accepts are valid Python too,
        # apart from a few escapes Python lacks, in which case the site alone decides.
        return re.search(target.pattern, url) is not None
    except re.error:
        return True


def gate_check(
    job_id: str,
    spec: CrawlSpec,
    lake: Lake,
    store: BlobStore,
    summary: JobSummary,
    *,
    selected: Sequence[str],
    charged: int,
    agreement: Agreement | None = None,
) -> GateReport:
    """``selected``: the capture ids the job chose (``ops.policy_decisions``)."""
    rows = lineage_rows(lake, selected)
    checks: list[GateCheck] = []

    def add(name: str, passed: bool | None, detail: str) -> None:
        checks.append(GateCheck(name=name, passed=passed, detail=detail))

    add(
        "job finished",
        summary.state is JobState.SUCCEEDED,
        f"state {summary.state.value}"
        + (f": {summary.stop_reason}" if summary.stop_reason else ""),
    )
    own = sum(1 for c, _, _ in rows if c.job_id == job_id)
    add(
        "every selected page is in the lake",
        len(rows) == len(selected) > 0,
        f"{len(rows)}/{len(selected)}"
        f" ({own} stored by this job, {len(rows) - own} by earlier jobs)",
    )

    # Rows per target: a row belongs to the first target whose site and pattern it matches.
    per_target = Counter[int]()
    for cap, _, _ in rows:
        for i, t in enumerate(spec.targets):
            if _belongs(cap.url, t):
                per_target[i] += 1
                break
    wanted = [t.pages for t in spec.targets]
    got = [per_target[i] for i in range(len(spec.targets))]
    add(
        "pages per target",
        got == wanted,
        ", ".join(f"{t.site} {g}/{w}" for t, g, w in zip(spec.targets, got, wanted, strict=True)),
    )

    ids = [c.capture_id for c, _, _ in rows]
    urls = [c.url for c, _, _ in rows]
    add(
        "no duplicates",
        len(set(ids)) == len(ids) and len(set(urls)) == len(urls),
        f"{len(ids)} rows, {len(set(ids))} capture ids, {len(set(urls))} URLs",
    )

    ok_verdicts = {PolicyVerdict.ALLOWED}
    if spec.allow_unknown_robots:
        ok_verdicts.add(PolicyVerdict.NEEDS_REVIEW)
    verdicts = Counter(c.policy_verdict for c, _, _ in rows)
    no_robots = sum(1 for c, r, _ in rows if c.robots_capture_id is None or r is None)
    # Only a needs_review row (allowed in by allow_unknown_robots) may lack a robots.txt capture:
    # that is what needs_review means. Any other verdict must rest on stored bytes.
    unbacked = sum(
        1
        for c, r, _ in rows
        if (c.robots_capture_id is None or r is None)
        and c.policy_verdict is not PolicyVerdict.NEEDS_REVIEW
    )
    add(
        "robots verdict on every row",
        set(verdicts) <= ok_verdicts and unbacked == 0,
        ", ".join(f"{v.value}={n}" for v, n in sorted(verdicts.items()))
        + f"; rows without a stored robots.txt capture: {no_robots}"
        + (f" ({unbacked} not needs_review)" if unbacked else ""),
    )

    intact = header_ok = robots_intact = 0
    digests = Counter[str]()
    failures: list[str] = []
    robots_seen: dict[str, bool] = {}
    for cap, robots, robots_sha in rows:
        record = store.read(BlobRef(uri=cap.raw_uri, offset=cap.raw_offset, length=cap.raw_length))
        if hashlib.sha256(record).hexdigest() == cap.raw_sha256:
            intact += 1
        elif len(failures) < 5:
            failures.append(f"bytes changed: {cap.capture_id}")
        parsed = read_records(record)
        header = parsed[0][0].rec_headers.get_header("WARC-Payload-Digest") if parsed else None  # type: ignore[attr-defined]
        if cap.content_digest_sha1 and header == f"sha1:{cap.content_digest_sha1}":
            header_ok += 1
        elif len(failures) < 5:
            failures.append(f"index digest differs from the WARC header: {cap.capture_id}")
        digests[classify_digest(record)] += 1
        if robots is not None and cap.robots_capture_id is not None:
            if cap.robots_capture_id not in robots_seen:
                data = store.read(robots)
                robots_seen[cap.robots_capture_id] = hashlib.sha256(data).hexdigest() == robots_sha
            robots_intact += robots_seen[cap.robots_capture_id]
    n = len(rows)
    add("stored bytes intact (SHA-256)", n > 0 and intact == n, f"{intact}/{n}")
    add("index digest = WARC header digest", n > 0 and header_ok == n, f"{header_ok}/{n}")
    add(
        "robots.txt bytes intact",
        n > 0 and robots_intact == n - no_robots,
        f"{robots_intact}/{n - no_robots} rows, {len(robots_seen)} robots.txt capture(s)",
    )
    add(
        "recomputed payload digest",
        None,
        ", ".join(f"{k}={v}" for k, v in sorted(digests.items())) or "no rows",
    )
    add(
        "budget",
        summary.budget_spent <= summary.budget_cap and summary.budget_spent == charged,
        f"spent {summary.budget_spent}/{summary.budget_cap}; charged attempts {charged}",
    )
    if agreement is None:
        add("normalizers agree with the official indexes", None, "not run (no oracle files)")
    else:
        rate = agreement.core_rate
        add(
            "normalizers agree with the official indexes",
            rate is not None and rate >= AGREEMENT_TARGET,
            f"{'n/a' if rate is None else f'{rate:.1%}'} on title, status and date"
            f" (target {AGREEMENT_TARGET:.0%}); {agreement.not_in_oracle} page(s) not in the index",
        )
    if failures:
        add("first failures", False, "; ".join(failures))
    return GateReport(job_id=job_id, rows=n, checks=checks)


class Proof(BaseModel):
    """What ``prove`` produced for a finished job."""

    model_config = ConfigDict(frozen=True)

    documents: int  # silver.documents rows written for the job
    without_extractor: int  # selected pages no extractor knows
    agreement: Agreement | None  # None when the oracle folder holds no index
    gate: GateReport


class Gate1Run(BaseModel):
    """`strata gate1 run`'s result: the run (None when the job had already succeeded) and the
    proof (None when the job didn't succeed)."""

    model_config = ConfigDict(frozen=True)

    job_id: str
    run: RunReport | None
    proof: Proof | None


async def prove(job_id: str, ops: OpsStore, lake: Lake, store: BlobStore, oracle: Path) -> Proof:
    """Normalize the job's pages into silver.documents, compare them with the publishers'
    indexes in ``oracle``, then run every Gate 1 check over the job's selected rows."""
    record = await ops.job(job_id)
    summary = await ops.job_summary(job_id)
    if record is None or summary is None:
        raise ValueError(f"no job {job_id}")
    selected = await ops.selected_capture_ids(job_id)
    charged = sum((await ops.charged_attempts(job_id)).values())

    def normalize() -> tuple[list[Document], int, int]:
        docs, unknown = documents(lake.captures_by_ids(selected), store, job_id=job_id)
        return docs, unknown, lake.replace_documents(job_id, docs)

    docs, unknown, written = await asyncio.to_thread(normalize)
    peps, rfcs = await asyncio.to_thread(load_oracles, oracle)
    agreed = agreement(docs, peps, rfcs) if peps or rfcs else None
    gate = await asyncio.to_thread(
        gate_check,
        job_id,
        record.spec,
        lake,
        store,
        summary,
        selected=selected,
        charged=charged,
        agreement=agreed,
    )
    return Proof(documents=written, without_extractor=unknown, agreement=agreed, gate=gate)
