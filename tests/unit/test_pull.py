"""Pull: normalizing stored records, the four sinks, the pull service and the SDK's page set."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import psycopg
import pytest

from strata_core.models import (
    Capture,
    CrawlSpec,
    JobState,
    Outcome,
    PolicyVerdict,
    RunReport,
    Source,
    Target,
    Verification,
)
from strata_core.sinks import (
    ChangeRow,
    DuckDbSink,
    PageRow,
    ParquetFolderSink,
    SqliteSink,
    describe,
    open_sink,
)
from strata_services.pages import NORMALIZER_VERSION, normalize
from strata_services.pull import PullService
from strata_services.sdk import Pages
from tests.fakecc import build_record

URL = "https://example.org/docs/guide/"
ARTICLE = " ".join(
    f"Paragraph {i} explains how the polite fetcher reserves budget before every request."
    for i in range(12)
)
HTML = f"""<!doctype html><html lang="EN-gb"><head><title> The  Guide </title>
<base href="https://example.org/docs/"></head><body>
<nav><a href="/">Home</a> <a href="intro#top">Intro</a> <a href="mailto:x@example.org">Mail</a>
<a href="http://[::1">broken</a></nav>
<article><h1>The Guide</h1><p>{ARTICLE}</p><p>{ARTICLE}</p></article>
<footer><a href="https://other.example/">Elsewhere</a></footer></body></html>"""


def capture(url: str = URL, cid: str = "c1", job: str = "job_old") -> Capture:
    return Capture(
        capture_id=cid,
        job_id=job,
        source=Source.COMMON_CRAWL,
        crawl="CC-MAIN-2026-43",
        url=url,
        url_surtkey=None,
        upstream_fetch_time=datetime(2026, 10, 11, 17, 42, tzinfo=UTC),
        fetched_at=datetime(2026, 11, 19, tzinfo=UTC),
        upstream_warc_filename="crawl-data/CC-MAIN-2026-43/x.warc.gz",
        upstream_offset=1000,
        upstream_length=500,
        content_digest_sha1="ABC",
        raw_uri="file:///raw/part-00000.warc.gz",
        raw_offset=0,
        raw_length=500,
        raw_sha256="0" * 64,
        http_status=200,
        mime="text/html",
        outcome=Outcome.OK,
        verification=Verification.OK,
        policy_verdict=PolicyVerdict.ALLOWED,
        strata_version="test",
    )


def record(html: str = HTML, url: str = URL) -> bytes:
    return build_record(url, html.encode("utf-8"), content_type="text/html; charset=utf-8")[0]


# ---------------------------------------------------------------------------- normalizer
def test_a_record_becomes_a_page_row_with_its_lineage() -> None:
    row = normalize(capture(), record(), job_id="job_pull")

    assert row.title == "The Guide"
    assert row.lang == "en-gb"
    assert row.out_links == [
        "https://example.org/",
        "https://example.org/docs/intro",  # resolved against <base>, fragment dropped
        "https://other.example/",
    ]
    assert row.main_text is not None
    assert "reserves budget before every request" in row.main_text
    assert "Home" not in row.main_text  # navigation is boilerplate
    assert row.warc_ref == "crawl-data/CC-MAIN-2026-43/x.warc.gz#1000,500"
    assert (row.payload_sha1, row.robots_verdict, row.capture_id) == ("ABC", "allowed", "c1")
    assert row.job_id == "job_pull"  # the job that pulled it, not the one that stored it
    assert row.normalizer_version == NORMALIZER_VERSION


def test_without_a_title_the_top_headings_name_the_page() -> None:
    legacy = """<pre>Request for Comments: 2046
   <span class="h1">Multipurpose Internet Mail Extensions</span>
   <span class="h1">(MIME) Part Two:</span>
   <span class="h1"><b>Media</b> Types</span>
Status of this Memo</pre>"""
    row = normalize(capture(), record(legacy))
    assert row.title == "Multipurpose Internet Mail Extensions (MIME) Part Two: Media Types"
    assert normalize(capture(), record("<h1>Only <i>a</i> heading</h1>")).title == "Only a heading"


def test_an_empty_page_still_gives_a_row() -> None:
    row = normalize(capture(), record("<html></html>"))
    assert (row.title, row.main_text, row.lang, row.out_links) == (None, None, None, [])
    assert row.job_id == "job_old"


# ---------------------------------------------------------------------------- sinks
def page(url: str, title: str) -> PageRow:
    return normalize(capture(url=url, cid=url), record()).model_copy(update={"title": title})


def test_sqlite_upserts_on_url_and_crawl(tmp_path: Path) -> None:
    sink = SqliteSink(tmp_path / "pages.db")
    sink.write_pages([page("https://example.org/a", "A"), page("https://example.org/b", "B")])
    sink.write_pages([page("https://example.org/a", "A, again")])
    sink.write_changes([ChangeRow(url="u", from_crawl="C1", to_crawl="C2", change="added")])
    sink.close()

    con = sqlite3.connect(tmp_path / "pages.db")
    rows = con.execute("SELECT url, title, out_links FROM strata_pages ORDER BY url").fetchall()
    assert [(u, t) for u, t, _ in rows] == [
        ("https://example.org/a", "A, again"),
        ("https://example.org/b", "B"),
    ]
    assert json.loads(rows[0][2])[0] == "https://example.org/"
    with pytest.raises(sqlite3.IntegrityError):
        con.execute(
            "INSERT INTO strata_site_changes VALUES ('v', 'C1', 'C2', 'renamed')"
        )  # the change kinds are a CHECK, as in every sink
    con.close()


def test_duckdb_keeps_links_as_a_list() -> None:
    sink = DuckDbSink(duckdb.connect())
    sink.write_pages([page("https://example.org/a", "A")])
    sink.write_pages([page("https://example.org/a", "A2")])
    rows = sink.con.execute("SELECT title, out_links[1] FROM strata_pages").fetchall()
    assert rows == [("A2", "https://example.org/")]


def test_parquet_merges_with_what_is_already_there(tmp_path: Path) -> None:
    first = ParquetFolderSink(tmp_path / "out")
    first.write_pages([page("https://example.org/a", "A")])
    first.close()
    second = ParquetFolderSink(tmp_path / "out")
    second.write_pages([page("https://example.org/a", "A2"), page("https://example.org/b", "B")])
    second.close()

    out = (tmp_path / "out" / "strata_pages.parquet").as_posix()
    rows = duckdb.sql(f"SELECT url, title FROM read_parquet('{out}') ORDER BY url").fetchall()
    assert rows == [("https://example.org/a", "A2"), ("https://example.org/b", "B")]


def test_sinks_are_chosen_by_scheme_or_suffix(tmp_path: Path) -> None:
    for target, kind in [
        (f"sqlite:///{(tmp_path / 'a.db').as_posix()}", SqliteSink),
        (str(tmp_path / "b.sqlite"), SqliteSink),
        (f"duckdb:///{(tmp_path / 'c.duckdb').as_posix()}", DuckDbSink),
        (f"parquet:///{(tmp_path / 'd').as_posix()}", ParquetFolderSink),
        (str(tmp_path / "e"), ParquetFolderSink),
    ]:
        sink = open_sink(target)
        assert isinstance(sink, kind), target
        sink.close()
    with pytest.raises(ValueError, match="unknown sink"):
        open_sink("mysql://x/y")
    with pytest.raises(psycopg.OperationalError):  # key=value is Postgres, not a folder name
        open_sink("dbname=nope host=127.0.0.1 port=1 connect_timeout=1")


def test_a_sink_is_described_without_its_password() -> None:
    assert describe("postgresql://me:s3cret@db:5433/pages") == "postgresql://me@db:5433/pages"
    assert describe("sqlite:///pages.db") == "sqlite:///pages.db"


# ---------------------------------------------------------------------------- pull service
def run_report(state: JobState) -> RunReport:
    return RunReport(
        job_id="job_pull",
        state=state,
        domain="example.org",
        crawl="CC-MAIN-2026-43",
        refs_found=3,
        refs_from_cache=False,
        outcomes={},
        rows_written=1,
        budget_cap=10,
        budget_spent=10,
        rate_limited=0,
        bytes_stored=0,
        duration_s=1.0,
        achieved_rps=1.0,
        stop_reason="budget exhausted" if state is JobState.STOPPED else None,
    )


class FakeRunner:
    def __init__(self, state: JobState, selected: list[str], rerun: bool = False) -> None:
        self._state, self._selected, self._rerun = state, selected, rerun

    async def run(self, spec: CrawlSpec) -> tuple[str, RunReport | None]:
        return "job_pull", None if self._rerun else run_report(self._state)

    async def state(self, job_id: str) -> JobState:
        return self._state

    async def selected(self, job_id: str) -> list[str]:
        return self._selected


class FakeStore:
    def __init__(self, stored: dict[str, Capture]) -> None:
        self.stored = stored

    def captures(self, capture_ids: Sequence[str]) -> list[Capture]:
        return [self.stored[c] for c in capture_ids if c in self.stored]

    def record(self, capture: Capture) -> bytes:
        return record(url=capture.url)


SPEC = CrawlSpec(crawl="latest", budget=10, targets=(Target(site="example.org", pages=3),))


async def test_a_stopped_pull_writes_what_was_stored_and_says_so() -> None:
    stored = {"c1": capture(cid="c1"), "c2": capture(url=URL + "2", cid="c2")}
    service = PullService(FakeRunner(JobState.STOPPED, ["c1", "c2", "c3"]), FakeStore(stored))
    sink = DuckDbSink(duckdb.connect())

    report = await service.pull(SPEC, sink, sink_name="memory")

    assert (report.state, report.selected, report.written, report.with_text) == (
        JobState.STOPPED,
        3,
        2,
        2,
    )
    assert report.languages == {"en-gb": 2}
    assert report.run is not None and report.run.stop_reason == "budget exhausted"
    assert sink.con.execute("SELECT count(*), min(job_id) FROM strata_pages").fetchone() == (
        2,
        "job_pull",
    )


async def test_pulling_a_finished_job_again_rewrites_the_same_rows() -> None:
    service = PullService(
        FakeRunner(JobState.SUCCEEDED, ["c1"], rerun=True), FakeStore({"c1": capture()})
    )
    sink = DuckDbSink(duckdb.connect())
    for _ in range(2):
        report = await service.pull(SPEC, sink, sink_name="memory")
    assert (report.run, report.state, report.written) == (None, JobState.SUCCEEDED, 1)
    assert sink.con.execute("SELECT count(*) FROM strata_pages").fetchone() == (1,)


# ---------------------------------------------------------------------------- SDK page set
async def test_pages_convert_to_duckdb() -> None:
    service = PullService(FakeRunner(JobState.SUCCEEDED, ["c1"]), FakeStore({"c1": capture()}))
    sink = DuckDbSink(duckdb.connect())
    report = await service.pull(SPEC, sink, sink_name="memory")
    pages = Pages([page("https://example.org/b", "B"), page("https://example.org/a", "A")], report)

    assert [p.url for p in pages] == ["https://example.org/a", "https://example.org/b"]
    assert len(pages) == 2 and pages[0].title == "A"
    con = pages.to_duckdb()
    assert con.execute("SELECT count(*) FROM strata_pages").fetchone() == (2,)
