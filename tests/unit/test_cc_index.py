from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
import pytest
from pytest_httpserver import HTTPServer

from strata_core.ledger import BudgetExhaustedError
from strata_core.models import CaptureRef, IndexChoice, Outcome
from strata_core.settings import Settings
from strata_fetch.cc.index import (
    AutoIndex,
    CdxIndex,
    ColumnarIndex,
    DiscoveryResult,
    IndexAttempt,
    IndexBlockedError,
    IndexQueryError,
    IndexUnavailableError,
    StaticShards,
    cdx_filters,
    check_filter,
    parse_cdx_line,
    rows_to_request,
    select_distinct,
    server_message,
)
from strata_fetch.cc.shardmap import LocalTail, ShardMap
from strata_fetch.cc.sources import AUTO_CDX_ATTEMPTS, make_index
from strata_fetch.ratelimit import HostGate
from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc, cdx_line

GATEWAY_TIMEOUT = (
    "<html>\r\n<head><title>504 Gateway Time-out</title></head>\r\n<body>\r\n"
    "<center><h1>504 Gateway Time-out</h1></center>\r\n<hr><center>nginx/1.31.8</center>\r\n"
    "</body>\r\n</html>\r\n"
)
REGEX_ERROR = '{"message": "Internal Error: nothing to repeat at position 0"}'
SLOW_DOWN = (
    '{"message": "Internal Error: 503 Server Error: Slow Down for url: '
    'http://commoncrawl.s3.amazonaws.com/cc-index/collections/CC-MAIN-2026-17/indexes/cdx-00294.gz"}'
)

PAGES = [(f"https://example.org/p{i}", f"<html>page {i}</html>".encode()) for i in range(5)]


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(PAGES)
    cc.add_warc(warc)
    cc.cdx_refs[CRAWL] = warc.refs
    cc.install()
    return cc


async def no_sleep(_: float) -> None:
    return None


def cdx(fake: FakeCommonCrawl, client: httpx.AsyncClient, tmp: Path, **kw: object) -> CdxIndex:
    return CdxIndex(
        client,
        HostGate(rate=1000, burst=10),
        base_url=fake.base_url,
        cache_dir=tmp,
        backoff=(0.01, 0.01),
        sleep=no_sleep,
        **kw,  # type: ignore[arg-type]
    )


async def test_finds_refs_then_serves_them_from_cache(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    async with httpx.AsyncClient() as client:
        index = cdx(fake, client, tmp_path)
        first = await index.find("example.org", CRAWL, limit=3)
        second = await index.find("example.org", CRAWL, limit=3)
    assert [r.url for r in first.refs] == [p[0] for p in PAGES[:3]]
    assert (first.from_cache, first.requests) == (False, 1)
    assert (second.from_cache, second.requests) == (True, 0)
    assert second.refs == first.refs
    assert fake.count("/" + CRAWL) == 1  # the rerun made zero requests


async def test_cache_answers_larger_limits_only_when_it_holds_enough(
    httpserver: HTTPServer, tmp_path: Path
) -> None:
    many = build_warc([(f"https://example.org/p{i:02d}", b"<html>x</html>") for i in range(30)])
    cc = FakeCommonCrawl(httpserver)
    cc.add_warc(many)
    cc.cdx_refs[CRAWL] = many.refs
    cc.install()
    async with httpx.AsyncClient() as client:
        index = cdx(cc, client, tmp_path)
        first = await index.find("example.org", CRAWL, limit=2)  # asks for 10 rows: 10 cached
        smaller = await index.find("example.org", CRAWL, limit=8)  # 8 <= 10 cached: no request
        bigger = await index.find("example.org", CRAWL, limit=20)  # needs more: refetch 100 rows
        everything = await index.find("example.org", CRAWL, limit=500)  # index had only 30
    assert (first.from_cache, len(first.refs)) == (False, 2)
    assert (smaller.from_cache, len(smaller.refs)) == (True, 8)
    assert (bigger.from_cache, len(bigger.refs)) == (False, 20)
    assert (everything.from_cache, len(everything.refs)) == (True, 30)
    assert cc.count("/" + CRAWL) == 2


async def test_a_cache_from_an_older_version_is_ignored(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    old = tmp_path / "refs" / CRAWL / "example.org.jsonl"
    old.parent.mkdir(parents=True)
    old.write_text(json.dumps({"crawl": CRAWL, "domain": "example.org", "limit": 20}) + "\n")
    async with httpx.AsyncClient() as client:
        result = await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
    assert (result.from_cache, len(result.refs)) == (False, 5)


async def test_no_captures_is_a_definitive_empty_answer(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    async with httpx.AsyncClient() as client:
        result = await cdx(fake, client, tmp_path).find("nothing-here.org", "CC-MAIN-2026-99", 10)
    assert result.refs == []


async def test_rate_limited_index_pauses_then_succeeds(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].append((503, {"Retry-After": "0"}))
    async with httpx.AsyncClient() as client:
        result = await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
    assert (len(result.refs), result.requests) == (5, 2)


@pytest.mark.parametrize(("status", "error"), [(400, IndexQueryError), (403, IndexBlockedError)])
async def test_definitive_refusals_are_not_retried(
    fake: FakeCommonCrawl, tmp_path: Path, status: int, error: type[Exception]
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].append((status, {}))
    async with httpx.AsyncClient() as client:
        with pytest.raises(error):
            await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
    assert fake.count("/" + CRAWL) == 1


async def test_index_lookups_spend_budget(fake: FakeCommonCrawl, tmp_path: Path) -> None:
    async def refuse(_host: str) -> bool:
        return False

    async with httpx.AsyncClient() as client:
        with pytest.raises(BudgetExhaustedError):
            await cdx(fake, client, tmp_path, reserve=refuse).find("example.org", CRAWL, 5)
    assert fake.count() == 0


def page(url: str, offset: int, day: int) -> CaptureRef:
    return CaptureRef(
        url=url,
        crawl=CRAWL,
        warc_filename="f.warc.gz",
        warc_record_offset=offset,
        warc_record_length=100,
        fetch_time=datetime(2026, 10, day, tzinfo=UTC),
        status=200,
        mime="text/html",
    )


def homepage_variants() -> list[CaptureRef]:
    """What python.org looked like: one page, captured repeatedly, plus ?ref= tracking variants."""
    home = [page("https://www.python.org/", i, day) for i, day in enumerate([5, 11, 17])]
    tracked = [
        page(f"https://www.python.org/?ref={ref}", 10 + i, 9)
        for i, ref in enumerate(["talkpython", "apifox.com", "4geeks.io"])
    ]
    others = [
        page(f"https://www.python.org/{path}", 20 + i, 1 + i)
        for i, path in enumerate(["about/", "about/", "downloads/", "doc/"])
    ]
    return home + tracked + others


def test_select_distinct_keeps_one_newest_capture_per_page_and_drops_variants() -> None:
    picked = select_distinct(homepage_variants(), limit=20)
    assert [r.url for r in picked] == [
        "https://www.python.org/",
        "https://www.python.org/about/",
        "https://www.python.org/downloads/",
        "https://www.python.org/doc/",
    ]
    assert picked[0].fetch_time == datetime(2026, 10, 17, tzinfo=UTC)  # newest of three captures
    assert picked[1].warc_record_offset == 21  # the later of the two /about/ captures
    assert len(select_distinct(homepage_variants(), limit=2)) == 2


def test_select_distinct_can_keep_query_strings() -> None:
    picked = select_distinct(homepage_variants(), limit=20, include_query_strings=True)
    assert len(picked) == 7
    assert sum("?ref=" in r.url for r in picked) == 3


def test_rows_to_request_overfetches_within_a_cap() -> None:
    assert rows_to_request(20) == 100
    assert rows_to_request(500) == 1000
    assert rows_to_request(5000) == 5000  # never fewer rows than pages wanted


async def test_the_query_excludes_query_strings_and_overfetches(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    async with httpx.AsyncClient() as client:
        await cdx(fake, client, tmp_path).find("example.org", CRAWL, limit=3)
        await cdx(fake, client, tmp_path / "b", include_query_strings=True).find(
            "example.org", CRAWL, limit=3
        )
    default, with_queries = fake.queries[-2], fake.queries[-1]
    assert ("filter", "!url:?") in default
    assert ("limit", "15") in default
    assert ("filter", "!url:?") not in with_queries


async def test_query_string_urls_are_filtered_by_the_server(
    httpserver: HTTPServer, tmp_path: Path
) -> None:
    warc = build_warc([*PAGES, ("https://example.org/p0?ref=newsletter", b"<html>p0</html>")])
    cc = FakeCommonCrawl(httpserver)
    cc.add_warc(warc)
    cc.cdx_refs[CRAWL] = warc.refs
    cc.install()
    async with httpx.AsyncClient() as client:
        result = await cdx(cc, client, tmp_path).find("example.org", CRAWL, limit=10)
        # the filter that shipped in the first fix, sent as-is: pywb compiles "?" as a regex
        wrong = await client.get(
            cc.base_url + f"{CRAWL}-index", params={"url": "*.example.org", "filter": "!~url:?"}
        )
    assert result.rows_scanned == 5  # the variant never left the server
    assert all("?" not in r.url for r in result.refs)
    assert wrong.status_code == 500
    assert "nothing to repeat" in wrong.text


def test_filters_are_checked_before_any_request() -> None:
    for spec in cdx_filters() + cdx_filters(include_query_strings=True):
        check_filter(spec)
    check_filter("~url:.*\\?")  # a valid regex form of the same idea
    with pytest.raises(IndexQueryError, match="not a valid regex"):
        check_filter("!~url:?")
    with pytest.raises(IndexQueryError, match="malformed"):
        check_filter("status200")


def test_server_message_keeps_errors_on_one_readable_line() -> None:
    assert server_message(GATEWAY_TIMEOUT) == "504 Gateway Time-out"
    assert server_message(REGEX_ERROR) == "Internal Error: nothing to repeat at position 0"
    assert server_message("  plain\n  text  ") == "plain text"


async def test_a_regex_error_from_the_server_is_not_retried(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].append((500, {}, REGEX_ERROR))
    async with httpx.AsyncClient() as client:
        with pytest.raises(IndexQueryError) as info:
            await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
    assert len(info.value.attempts) == 1
    assert fake.count("/" + CRAWL) == 1


async def test_throttling_relayed_as_a_500_is_rate_limited_and_retried(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].append((500, {}, SLOW_DOWN))
    async with httpx.AsyncClient() as client:
        result = await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
    assert [a.outcome for a in result.attempts] == [Outcome.RATE_LIMITED, Outcome.OK]


async def test_a_plain_500_is_retried_once(fake: FakeCommonCrawl, tmp_path: Path) -> None:
    fake.fail_next["/" + CRAWL + "-index"].extend([(500, {})] * 5)
    async with httpx.AsyncClient() as client:
        with pytest.raises(IndexUnavailableError) as info:
            await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
    assert [a.http_status for a in info.value.attempts] == [500, 500]


async def test_every_attempt_is_reported_with_the_wait_that_followed(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].extend([(503, {"Retry-After": "0"}), (500, {})])
    seen: list[IndexAttempt] = []

    async def on_attempt(a: IndexAttempt) -> None:
        seen.append(a)

    async with httpx.AsyncClient() as client:
        result = await cdx(fake, client, tmp_path, on_attempt=on_attempt).find(
            "example.org", CRAWL, 5
        )
    assert [a.outcome for a in result.attempts] == [
        Outcome.RATE_LIMITED,
        Outcome.RETRYABLE_ERROR,
        Outcome.OK,
    ]
    assert [a.http_status for a in result.attempts] == [503, 500, 200]
    assert result.attempts[0].waited_ms is not None and result.attempts[-1].waited_ms is None
    assert seen == result.attempts
    assert result.requests == 3 and result.elapsed_s >= 0


async def test_gateway_timeouts_use_every_retry_and_read_cleanly(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].extend([(504, {}, GATEWAY_TIMEOUT)] * 5)
    async with httpx.AsyncClient() as client:
        with pytest.raises(IndexUnavailableError) as info:
            await cdx(fake, client, tmp_path).find("example.org", CRAWL, 5)
        fewer = cdx(fake, client, tmp_path, max_attempts=2)
        with pytest.raises(IndexUnavailableError) as capped:
            await fewer.find("example.org", CRAWL, 5)
    assert len(info.value.attempts) == 3  # backoff (0.01, 0.01) allows three attempts
    assert {a.error for a in info.value.attempts} == {"HTTP 504: 504 Gateway Time-out"}
    assert len(capped.value.attempts) == 2


# ------------------------------------------------------------------------------ auto index
class Spy:
    """A fallback source that records whether it was asked."""

    def __init__(self, result: DiscoveryResult | None = None) -> None:
        self.calls = 0
        self.result = result or DiscoveryResult(refs=[], from_cache=False, requests=0, source="spy")

    async def find(self, domain: str, crawl: str, limit: int) -> DiscoveryResult:
        self.calls += 1
        return self.result


async def test_auto_falls_back_to_the_url_index_when_the_cdx_server_is_down(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].extend([(504, {}, GATEWAY_TIMEOUT)] * 2)
    columnar = ColumnarIndex(StaticShards([write_index_fixture(tmp_path / "idx")]))
    async with httpx.AsyncClient() as client:
        auto = AutoIndex(cdx(fake, client, tmp_path, max_attempts=2), columnar)
        result = await auto.find("example.org", CRAWL, 10)
    assert result.source == "columnar"
    assert result.fallback_reason == "CDX server unavailable (2 attempt(s): HTTP 504, HTTP 504)"
    assert [r.url for r in result.refs] == ["https://example.org/a", "https://www.example.org/b"]
    assert result.requests == 2 and len(result.attempts) == 2


async def test_auto_does_not_route_around_a_block_or_a_bad_query(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].extend([(403, {}), (500, {}, REGEX_ERROR)])
    spy = Spy()
    async with httpx.AsyncClient() as client:
        auto = AutoIndex(cdx(fake, client, tmp_path), spy)
        with pytest.raises(IndexBlockedError):
            await auto.find("example.org", CRAWL, 5)
        with pytest.raises(IndexQueryError):
            await auto.find("example.org", CRAWL, 5)
    assert spy.calls == 0


async def test_auto_reports_both_failures_when_the_fallback_fails_too(
    fake: FakeCommonCrawl, tmp_path: Path
) -> None:
    fake.fail_next["/" + CRAWL + "-index"].extend([(504, {}, GATEWAY_TIMEOUT)] * 2)

    class Down:
        async def find(self, domain: str, crawl: str, limit: int) -> DiscoveryResult:
            raise IndexUnavailableError("URL index query failed: HTTP 503")

    async with httpx.AsyncClient() as client:
        auto = AutoIndex(cdx(fake, client, tmp_path, max_attempts=2), Down())
        with pytest.raises(
            IndexUnavailableError, match="tried after: CDX server unavailable"
        ) as info:
            await auto.find("example.org", CRAWL, 5)
    assert len(info.value.attempts) == 2


# ------------------------------------------------------------------------------ columnar index
# fetch_time is timezone-aware, as DuckDB reads Common Crawl's real files (CC-MAIN-2026-43).
INDEX_COLUMNS = (
    "url_surtkey VARCHAR, url VARCHAR, url_host_registered_domain VARCHAR, fetch_time TIMESTAMPTZ,"
    " fetch_status SMALLINT, content_digest VARCHAR, content_mime_type VARCHAR,"
    " content_mime_detected VARCHAR, warc_filename VARCHAR, warc_record_offset INTEGER,"
    " warc_record_length INTEGER"
)

ROWS = [
    # (surtkey, url, registered_domain, status, mime)
    ("org,example)/a", "https://example.org/a", "example.org", 200, "text/html"),
    ("org,example)/a", "https://example.org/a", "example.org", 200, "text/html"),
    ("org,example)/a?ref=x", "https://example.org/a?ref=x", "example.org", 200, "text/html"),
    ("org,example,www)/b", "https://www.example.org/b", "example.org", 200, "text/html"),
    ("org,example)/c.pdf", "https://example.org/c.pdf", "example.org", 200, "application/pdf"),
    ("org,example)/gone", "https://example.org/gone", "example.org", 404, "text/html"),
    ("org,example-shop)/x", "https://example-shop.org/x", "example-shop.org", 200, "text/html"),
    ("org,examples)/y", "https://examples.org/y", "examples.org", 200, "text/html"),
]


def write_index_fixture(
    root: Path,
    crawl: str = CRAWL,
    rows: list[tuple[str, str, str, int, str]] | None = None,
    name: str = "part-00000.parquet",
    *,
    naive_fetch_time: bool = False,
) -> str:
    """A 'real-schema' URL index shard in Common Crawl's hive layout.

    ``naive_fetch_time`` writes a plain TIMESTAMP instead, as some older files may have.
    """
    part = root / "cc-index/table/cc-main/warc" / f"crawl={crawl}" / "subset=warc"
    part.mkdir(parents=True, exist_ok=True)
    path = part / name
    con = duckdb.connect()
    columns = INDEX_COLUMNS
    if naive_fetch_time:
        columns = columns.replace("fetch_time TIMESTAMPTZ", "fetch_time TIMESTAMP")
    con.execute(f"CREATE TABLE t ({columns})")
    for i, (surt, url, reg, status, mime) in enumerate(ROWS if rows is None else rows):
        con.execute(
            "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                surt,
                url,
                reg,
                # an explicit offset, so the value doesn't depend on the machine's time zone
                f"2026-04-20 10:{i:02d}:00" + ("" if naive_fetch_time else "+00"),
                status,
                f"DIGEST{i}" + "A" * 26,
                mime,
                mime,
                "crawl-data/x/warc/f.warc.gz",
                1000 * i,
                500 + i,
            ],
        )
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT parquet)")
    con.close()
    return path.as_posix()


async def test_columnar_reads_both_timestamp_types_without_pytz(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # DuckDB needs pytz to turn TIMESTAMPTZ into Python objects; make sure we never ask it to.
    monkeypatch.setitem(sys.modules, "pytz", None)
    aware = ColumnarIndex(StaticShards([write_index_fixture(tmp_path / "aware")]))
    naive = ColumnarIndex(
        StaticShards([write_index_fixture(tmp_path / "naive", naive_fetch_time=True)])
    )
    from_aware = (await aware.find("example.org", CRAWL, limit=10)).refs
    from_naive = (await naive.find("example.org", CRAWL, limit=10)).refs
    assert from_aware == from_naive
    assert from_aware[0].fetch_time == datetime(2026, 4, 20, 10, 1, tzinfo=UTC)


async def test_columnar_scans_only_the_domain_and_its_subdomains(tmp_path: Path) -> None:
    index = ColumnarIndex(StaticShards([write_index_fixture(tmp_path)]))
    result = await index.find("example.org", CRAWL, limit=50)
    assert [r.url for r in result.refs] == ["https://example.org/a", "https://www.example.org/b"]
    assert all(r.crawl == CRAWL and r.status == 200 for r in result.refs)


async def test_columnar_can_keep_query_string_variants(tmp_path: Path) -> None:
    index = ColumnarIndex(StaticShards([write_index_fixture(tmp_path)]), include_query_strings=True)
    result = await index.find("example.org", CRAWL, limit=50)
    assert [r.url for r in result.refs] == [
        "https://example.org/a",
        "https://example.org/a?ref=x",
        "https://www.example.org/b",
    ]
    assert result.rows_scanned == 4  # two /a rows collapsed into one page


async def test_columnar_respects_limit_and_crawl(tmp_path: Path) -> None:
    shard = write_index_fixture(tmp_path)
    index = ColumnarIndex(StaticShards([shard]))
    assert len((await index.find("example.org", CRAWL, limit=1)).refs) == 1
    assert (await index.find("example.org", "CC-MAIN-2025-01", limit=5)).refs == []


async def test_both_index_sources_return_identical_refs(tmp_path: Path) -> None:
    """Contract: the same row through the CDX server and the columnar index is the same ref."""
    shard = write_index_fixture(tmp_path)
    columnar = (await ColumnarIndex(StaticShards([shard])).find("example.org", CRAWL, 1)).refs[0]
    from_cdx = parse_cdx_line(cdx_line(columnar), CRAWL)
    assert from_cdx == columnar
    assert columnar.fetch_time == datetime(2026, 4, 20, 10, 1, tzinfo=UTC)  # newer of two /a rows


async def test_columnar_with_a_shard_map_opens_only_covering_files(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    shards = [
        write_index_fixture(root, rows=[ROWS[-2]], name="part-00000.parquet"),  # example-shop
        write_index_fixture(root, rows=ROWS[:6], name="part-00001.parquet"),  # example.org
        write_index_fixture(root, rows=[ROWS[-1]], name="part-00002.parquet"),  # examples
    ]
    first = ColumnarIndex(
        StaticShards(shards), shard_map=ShardMap(LocalTail(), cache_dir=tmp_path / "cache")
    )
    found = await first.find("example.org", CRAWL, limit=10)
    assert [r.url for r in found.refs] == ["https://example.org/a", "https://www.example.org/b"]
    assert (found.shards_total, found.shards_queried, found.footers_read) == (3, 1, 3)

    later = ColumnarIndex(
        StaticShards(shards),
        shard_map=ShardMap(LocalTail(), cache_dir=tmp_path / "cache"),
        cache_dir=tmp_path / "cache",
    )
    other = await later.find("examples.org", CRAWL, limit=10)
    again = await later.find("examples.org", CRAWL, limit=10)
    assert [r.url for r in other.refs] == ["https://examples.org/y"]
    assert (other.shards_queried, other.footers_read) == (1, 0)  # the map came from disk
    assert again.from_cache and again.source == "columnar"


async def test_make_index_builds_the_source_asked_for(tmp_path: Path) -> None:
    settings = Settings(cache_dir=tmp_path, user_agent="strata-tests/0.1")
    gate = HostGate(rate=1000, burst=10)
    async with httpx.AsyncClient() as client:

        def build(choice: IndexChoice) -> object:
            return make_index(
                choice, client=client, settings=settings, index_gate=gate, data_gate=gate
            )

        assert isinstance(build(IndexChoice.CDX), CdxIndex)
        assert isinstance(build(IndexChoice.COLUMNAR), ColumnarIndex)
        auto = build(IndexChoice.AUTO)
    assert isinstance(auto, AutoIndex)
    assert auto._primary._max_attempts == AUTO_CDX_ATTEMPTS  # type: ignore[attr-defined]
