"""Week 2, S9 and S10: counting a site's pages, and counting DuckDB's own requests.

The HTTP tests serve Parquet files from a local threaded server and need DuckDB's httpfs
extension. CI installs it on first use; they are skipped where it can't be loaded.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from strata_core.ledger import JOB_TOTAL, MemoryLedger
from strata_core.models import Outcome, RequestClient
from strata_fetch.cc.index import (
    ColumnarIndex,
    IndexAttempt,
    IndexQueryError,
    IndexUnavailableError,
    StaticShards,
    check_pattern,
    parse_http_status,
    peak_in_flight,
    spread_sample,
)
from tests.fakecc import CRAWL
from tests.rangeserver import serve
from tests.unit.test_cc_index import write_index_fixture

UA = "strata-tests/0.1 (+https://example.invalid/strata)"
PEP = r"^https://peps\.example\.org/pep-\d{4}/$"


# ------------------------------------------------------------------------------ pure helpers
def test_parse_http_status_reads_duckdb_status_names() -> None:
    assert parse_http_status("PartialContent_206") == 206
    assert parse_http_status("OK_200") == 200
    assert parse_http_status("ServiceUnavailable_503") == 503
    assert parse_http_status(None) is None
    assert parse_http_status("Unknown") is None


def test_peak_in_flight_counts_overlaps_not_touching_intervals() -> None:
    assert peak_in_flight([]) == 0
    assert peak_in_flight([(0, 10), (10, 5)]) == 1  # one ends as the next starts
    assert peak_in_flight([(0, 10), (5, 10), (6, 1)]) == 3
    assert peak_in_flight([(7, 0)]) == 1  # a request faster than a millisecond still counts


async def test_memory_ledger_charges_what_fits() -> None:
    ledger = MemoryLedger()
    await ledger.open("j", total_cap=5, host_caps={"h": 3})
    assert await ledger.charge("j", "h", 2) == 0
    assert await ledger.charge("j", "h", 4) == 3  # the host cap (3) binds first
    assert (await ledger.usage("j"))[JOB_TOTAL] == (3, 5)
    assert await ledger.charge("j", "h", 0) == 0
    assert await ledger.charge("nope", "h", 2) == 2


def test_spread_sample_is_stable_and_not_index_order() -> None:
    urls = [f"https://peps.example.org/pep-{i:04d}/" for i in range(200)]
    sample = spread_sample(urls, 5)
    assert sample == spread_sample(list(reversed(urls)), 5)
    assert sample != urls[:5]
    assert len(set(sample)) == 5


def test_check_pattern_uses_duckdbs_regex_engine() -> None:
    check_pattern(PEP)
    with pytest.raises(IndexQueryError, match="RE2"):
        check_pattern(r"^https://x\.org/(?=a)")  # look-ahead: fine in Python, refused by RE2
    with pytest.raises(IndexQueryError):
        check_pattern("(")


# ------------------------------------------------------------------- site filter and count
def site_rows() -> list[tuple[str, str, str, int, str]]:
    """A small site with document pages, other pages, a subdomain and a neighbour."""
    rows = [
        (f"org,example,peps)/pep-{i:04d}", f"https://peps.example.org/pep-{i:04d}/", 200)
        for i in (1, 8, 20, 484, 3000)
    ]
    rows += [
        ("org,example,peps)/pep-0008", "https://peps.example.org/pep-0008/", 200),  # recrawl
        ("org,example,peps)/topic/packaging", "https://peps.example.org/topic/packaging/", 200),
        ("org,example,peps)/pep-0009", "https://peps.example.org/pep-0009/", 404),
        ("org,example,peps)/pep-0012?x=1", "https://peps.example.org/pep-0012/?x=1", 200),
        ("org,example,peps,dev)/pep-0001", "https://dev.peps.example.org/pep-0001/", 200),
        ("org,example)/about", "https://www.example.org/about/", 200),  # CC drops www
    ]
    return [(surt, url, "example.org", status, "text/html") for surt, url, status in rows]


async def test_count_answers_captures_urls_and_matches(tmp_path: Path) -> None:
    index = ColumnarIndex(StaticShards([write_index_fixture(tmp_path, rows=site_rows())]))
    found = await index.count("peps.example.org", CRAWL, PEP, sample=3)
    # 200/text-html rows on the host and its subdomains: 5 PEPs + recrawl + topic + ?x=1 + dev.
    assert (found.captures, found.urls, found.matching) == (9, 8, 5)
    assert len(found.sample) == 3
    assert all(url.startswith("https://peps.example.org/pep-") for url in found.sample)
    assert found.duckdb.requests == 0  # local files: no HTTP


async def test_a_host_finds_its_pages_though_its_registered_domain_is_wider(
    tmp_path: Path,
) -> None:
    """peps.python.org's registered domain is python.org; week 1 filtered on that and missed."""
    index = ColumnarIndex(StaticShards([write_index_fixture(tmp_path, rows=site_rows())]))
    refs = (await index.find("peps.example.org", CRAWL, limit=50)).refs
    hosts = {ref.url.split("/")[2] for ref in refs}
    assert hosts == {"peps.example.org", "dev.peps.example.org"}
    assert all("?" not in ref.url for ref in refs)


async def test_a_www_host_is_found_under_its_parent_domains_key(tmp_path: Path) -> None:
    """Common Crawl keys www.example.org as org,example): week 2's RFC count found 0 without it."""
    rows = [
        ("org,example)/", "https://example.org/", "example.org", 200, "text/html"),
        ("org,example)/info/a", "https://www.example.org/info/a", "example.org", 200, "text/html"),
        ("org,example)/info/b", "https://www.example.org/info/b", "example.org", 200, "text/html"),
        ("org,example,docs)/x", "https://docs.example.org/x", "example.org", 200, "text/html"),
    ]
    index = ColumnarIndex(StaticShards([write_index_fixture(tmp_path, rows=rows)]))
    found = await index.count("www.example.org", CRAWL, r"^https://www\.example\.org/info/\w+$")
    assert (found.captures, found.urls, found.matching) == (2, 2, 2)  # not the bare or docs host
    refs = (await index.find("www.example.org", CRAWL, limit=10)).refs
    assert sorted(ref.url for ref in refs) == [
        "https://www.example.org/info/a",
        "https://www.example.org/info/b",
    ]


async def test_a_bad_pattern_is_refused_before_any_index_work() -> None:
    class Spy:
        calls = 0

        async def shards(self, crawl: str) -> list[str]:
            Spy.calls += 1
            return []

    with pytest.raises(IndexQueryError):
        await ColumnarIndex(Spy()).count("peps.example.org", CRAWL, "(?=pep)")
    assert Spy.calls == 0


# --------------------------------------------------------------------------- over HTTP
def write_big_index(root: Path) -> str:
    """60,000 rows in 6 row groups, so a query for one host reads several column chunks."""
    part = root / "cc-index/table/cc-main/warc" / f"crawl={CRAWL}" / "subset=warc"
    part.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"""COPY (
          SELECT
            CASE WHEN i < 30000 THEN 'org,example,aaa)/p' || lpad(i::VARCHAR, 6, '0')
                 ELSE 'org,example,peps)/pep-' || lpad((i - 30000)::VARCHAR, 5, '0') END
              AS url_surtkey,
            CASE WHEN i < 30000 THEN 'https://aaa.example.org/p' || i
                 ELSE 'https://peps.example.org/pep-' || lpad((i - 30000)::VARCHAR, 5, '0') || '/'
              END AS url,
            CASE WHEN i < 30000 THEN 'aaa.example.org' ELSE 'peps.example.org' END
              AS url_host_name,
            'example.org' AS url_host_registered_domain,
            TIMESTAMPTZ '2026-04-20 10:00:00+00' + to_seconds(i) AS fetch_time,
            200::SMALLINT AS fetch_status,
            md5(i::VARCHAR) AS content_digest,
            'text/html' AS content_mime_type,
            'crawl-data/x/warc/f' || (i % 50) || '.warc.gz' AS warc_filename,
            i * 1000 AS warc_record_offset,
            500 + i % 300 AS warc_record_length
          FROM range(60000) t(i)
        ) TO '{(part / "part-00000.parquet").as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 10000)"""
    )
    con.close()
    return (part / "part-00000.parquet").relative_to(root).as_posix()


@pytest.mark.usefixtures("httpfs")
async def test_every_duckdb_request_is_counted_with_its_bytes(tmp_path: Path) -> None:
    rel = write_index_fixture(tmp_path, rows=site_rows())
    rel = Path(rel).relative_to(tmp_path).as_posix()
    reported: list[IndexAttempt] = []

    async def collect(attempt: IndexAttempt) -> None:
        reported.append(attempt)

    with serve(tmp_path) as server:
        index = ColumnarIndex(StaticShards([server.url(rel)]), user_agent=UA, on_attempt=collect)
        result = await index.find("peps.example.org", CRAWL, limit=10)
        seen = list(server.requests)

    usage = result.duckdb
    assert usage is not None and usage.requests > 0
    assert usage.requests == len(seen)  # nothing DuckDB sent is missing from its log
    assert usage.bytes == sum(r.body_bytes for r in seen)
    assert usage.bytes_read == usage.bytes
    assert usage.files == 1 and usage.failed == 0
    assert result.requests == len(result.attempts) == usage.requests
    assert reported == result.attempts
    assert all(a.client is RequestClient.DUCKDB and a.outcome is Outcome.OK for a in reported)
    assert {r.method for r in seen} <= {"HEAD", "GET"}
    assert all(r.user_agent and UA in r.user_agent for r in seen)  # ADR-003: honest UA


@pytest.mark.usefixtures("httpfs")
async def test_count_over_http_agrees_with_the_local_file(tmp_path: Path) -> None:
    rel = write_big_index(tmp_path)
    local = await ColumnarIndex(StaticShards([(tmp_path / rel).as_posix()])).count(
        "peps.example.org", CRAWL, r"^https://peps\.example\.org/pep-\d{5}/$"
    )
    with serve(tmp_path) as server:
        remote = await ColumnarIndex(StaticShards([server.url(rel)])).count(
            "peps.example.org", CRAWL, r"^https://peps\.example\.org/pep-\d{5}/$"
        )
    assert (remote.captures, remote.urls, remote.matching) == (30000, 30000, 30000)
    assert (local.captures, local.urls, local.matching, local.sample) == (
        remote.captures,
        remote.urls,
        remote.matching,
        remote.sample,
    )
    assert remote.duckdb.requests > local.duckdb.requests == 0


@pytest.mark.usefixtures("httpfs")
@pytest.mark.parametrize("threads", [1, 2])
async def test_requests_in_flight_never_exceed_the_thread_count(
    tmp_path: Path, threads: int
) -> None:
    """ADR-003 allows two requests in flight to data.commoncrawl.org.

    DuckDB's Parquet reader prefetches column chunks in parallel, so `threads` alone let six
    requests overlap with threads=1. Sizing the HTTP client pool to match is what holds the line.
    """
    rel = write_big_index(tmp_path)
    with serve(tmp_path, delay_s=0.03) as server:
        index = ColumnarIndex(StaticShards([server.url(rel)]), threads=threads)
        result = await index.find("peps.example.org", CRAWL, limit=1000)
        peak = server.peak
    assert result.duckdb is not None and result.duckdb.requests >= 3
    assert peak <= threads
    assert result.duckdb.peak_in_flight <= threads


@pytest.mark.usefixtures("httpfs")
async def test_a_failed_query_still_reports_the_requests_it_made(tmp_path: Path) -> None:
    reported: list[IndexAttempt] = []

    async def collect(attempt: IndexAttempt) -> None:
        reported.append(attempt)

    missing = f"cc-index/table/cc-main/warc/crawl={CRAWL}/subset=warc/part-00009.parquet"
    with serve(tmp_path) as server:
        index = ColumnarIndex(StaticShards([server.url(missing)]), on_attempt=collect)
        with pytest.raises(IndexUnavailableError, match="URL index query failed") as info:
            await index.find("peps.example.org", CRAWL, limit=5)
    assert reported and reported == info.value.attempts
    assert reported[0].http_status == 404
    assert reported[0].outcome is Outcome.DEFINITIVE_EMPTY
    assert all(a.client is RequestClient.DUCKDB for a in reported)
