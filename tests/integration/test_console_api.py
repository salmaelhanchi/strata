"""The console API end to end: a fake Common Crawl, a throwaway ops database, the real app."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from pytest_httpserver import HTTPServer

from strata_console.app import create_app
from strata_core.settings import Settings
from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc

pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("httpfs")]

OLD = "CC-MAIN-2026-13"
TEXT = " ".join(f"Sentence {i} of a page with enough words to be the main text." for i in range(15))


def html(i: int) -> bytes:
    return (
        f'<html lang="en"><head><title>Page {i}</title></head><body><nav><a href="/">Home</a>'
        f"</nav><article><p>{TEXT}</p><p>{TEXT}</p></article></body></html>"
    ).encode()


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    old = build_warc([(f"https://example.org/p{i}", html(i)) for i in range(4)], crawl=OLD)
    new = build_warc([(f"https://example.org/p{i}", html(i + (i == 1))) for i in range(1, 6)])
    for warc, crawl in ((old, OLD), (new, CRAWL)):
        cc.add_warc(warc)
        cc.add_url_index(warc.refs, crawl)
    cc.add_robots("example.org", b"User-agent: *\nDisallow: /p5\n")
    cc.files["/collinfo.json"] = json.dumps(
        [{"id": CRAWL, "name": "newer"}, {"id": OLD, "name": "older"}]
    ).encode()
    cc.install()
    return cc


def settings_for(fake: FakeCommonCrawl, pg_dsn: str, tmp: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        ops_dsn=pg_dsn,
        lake_local_file=tmp / "lake.duckdb",
        raw_root=tmp / "raw",
        cache_dir=tmp / "cache",
        console_db=tmp / "console.duckdb",
        cc_data_base=fake.base_url,
        cc_index_base=fake.base_url,
        backoff_s=(0.01, 0.01),
        user_agent="strata-tests/0.1",
    )


@pytest.fixture
async def api(
    fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path
) -> AsyncIterator[httpx.AsyncClient]:
    web = tmp_path / "web"
    (web / "assets").mkdir(parents=True)
    (web / "index.html").write_text("<!doctype html><div id=root></div>", encoding="utf-8")
    (web / "assets" / "app.js").write_text("console.log(1)", encoding="utf-8")
    app = create_app(settings_for(fake, pg_dsn, tmp_path), web_dist=web)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


async def test_profile_diff_and_change_pages(api: httpx.AsyncClient) -> None:
    crawls = (await api.get("/v1/crawls")).json()
    assert [c["id"] for c in crawls] == [CRAWL, OLD]

    profile = (await api.get("/v1/sites/example.org/profile", params={"crawls": 2})).json()
    assert [c["urls"] for c in profile["crawls"]] == [5, 4]
    assert profile["robots"][0]["verdicts"] == {"allowed": 4, "disallowed": 1}

    diff = (await api.get("/v1/sites/example.org/diff")).json()
    assert diff["counts"] == {"added": 2, "removed": 1, "changed": 1, "same": 2}
    assert "changes" not in diff  # the URL list pages through its own endpoint

    first = (await api.get("/v1/sites/example.org/diff/changes", params={"limit": 2})).json()
    assert [c["url"] for c in first["items"]] == [
        "https://example.org/p0",
        "https://example.org/p1",
    ]
    rest = (
        await api.get(
            "/v1/sites/example.org/diff/changes",
            params={"limit": 2, "cursor": first["next_cursor"]},
        )
    ).json()
    assert [c["url"] for c in rest["items"]] == ["https://example.org/p4", "https://example.org/p5"]
    assert rest.get("next_cursor") is None

    rates = (await api.get("/v1/sites/example.org/changes", params={"crawls": 2})).json()
    assert (rates["compared"], rates["changed"]) == (3, 1)


async def test_errors_are_problem_json(api: httpx.AsyncClient) -> None:
    missing = await api.get("/v1/jobs/job_nope")
    assert missing.status_code == 404
    assert missing.headers["content-type"] == "application/problem+json"
    assert missing.json()["title"] == "Not found"

    bad_cursor = await api.get("/v1/jobs", params={"cursor": "not-a-cursor"})
    assert (bad_cursor.status_code, bad_cursor.json()["title"]) == (400, "Bad request")

    poor = await api.get("/v1/sites/example.org/profile", params={"budget": 1})
    assert poor.status_code == 402
    assert poor.json()["title"] == "Budget exhausted"

    invalid = await api.post("/v1/pulls", json={"site": "example.org", "limit": 0})
    assert invalid.status_code == 422
    assert invalid.json()["errors"][0]["loc"] == ["body", "limit"]


async def test_a_pull_streams_its_progress_then_its_pages_trace_to_their_bytes(
    api: httpx.AsyncClient,
) -> None:
    run = await api.post("/v1/pulls", json={"site": "example.org", "limit": 3, "crawl": CRAWL})
    assert run.status_code == 202
    run_id = run.json()["run_id"]

    stream = await api.get(f"/v1/pulls/{run_id}/events")  # ends when the pull does
    assert stream.headers["content-type"].startswith("text/event-stream")
    kinds = [
        line.removeprefix("event: ")
        for line in stream.text.splitlines()
        if line.startswith("event: ")
    ]
    assert kinds[0] == "started" and kinds[-1] == "done"
    done = json.loads(stream.text.split("event: done\ndata: ")[1].split("\n")[0])
    assert (done["state"], done["written"]) == ("succeeded", 3)

    jobs = (await api.get("/v1/jobs")).json()
    assert jobs["items"][0]["job_id"] == done["job_id"]
    assert jobs["items"][0]["budget_spent"] > 0

    pages = (await api.get(f"/v1/jobs/{done['job_id']}/pages", params={"limit": 2})).json()
    assert len(pages["items"]) == 2 and pages["next_cursor"]
    first = pages["items"][0]
    assert first["title"].startswith("Page ") and first["robots_verdict"] == "allowed"

    trace = (await api.get(f"/v1/captures/{first['capture_id']}/lineage")).json()
    assert trace["lineage"]["raw_bytes_intact"] is True
    assert trace["lineage"]["index_matches_warc_header"] is True
    assert "main text" in trace["page"]["main_text"]

    metrics = (await api.get("/metrics")).text
    assert 'strata_pulls_total{state="succeeded"} 1.0' in metrics
    assert 'route="/v1/pulls"' in metrics


async def test_the_web_app_is_served_for_every_other_path(api: httpx.AsyncClient) -> None:
    for path in ("/", "/jobs/job_x"):  # routes the browser handles
        page = await api.get(path)
        assert page.status_code == 200 and "id=root" in page.text
    assert (await api.get("/assets/app.js")).text == "console.log(1)"
    assert (await api.get("/v1/nothing")).status_code == 404


async def test_without_a_build_the_console_says_how_to_make_one(
    fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path
) -> None:
    app = create_app(settings_for(fake, pg_dsn, tmp_path), web_dist=tmp_path / "missing")
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        page = await client.get("/")
    assert "make web" in page.text
