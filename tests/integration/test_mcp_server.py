"""The MCP server through the MCP SDK's own client: one conversation, every tool, a fake Crawl."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from mcp import Client
from pytest_httpserver import HTTPServer

from strata_core.settings import Settings
from strata_mcp.server import build_server
from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc

pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("httpfs")]

OLD = "CC-MAIN-2026-13"
TEXT = " ".join(f"Sentence {i} of a page with enough words to be its main text." for i in range(15))


def html(i: int) -> bytes:
    return (
        f'<html lang="en"><head><title>Doc {i}</title></head><body><article><p>{TEXT}</p>'
        f"<p>{TEXT}</p></article></body></html>"
    ).encode()


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    old = build_warc([(f"https://example.org/doc/{i}", html(i)) for i in range(4)], crawl=OLD)
    new = build_warc([(f"https://example.org/doc/{i}", html(i + (i == 1))) for i in range(1, 6)])
    for warc, crawl in ((old, OLD), (new, CRAWL)):
        cc.add_warc(warc)
        cc.add_url_index(warc.refs, crawl)
    cc.add_robots(
        "example.org", b"User-agent: *\nDisallow: /doc/5\nSitemap: https://example.org/s.xml\n"
    )
    cc.files["/collinfo.json"] = json.dumps([{"id": CRAWL}, {"id": OLD}]).encode()
    cc.install()
    return cc


def settings_for(fake: FakeCommonCrawl, pg_dsn: str, tmp: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        ops_dsn=pg_dsn,
        lake_local_file=tmp / "lake.duckdb",
        raw_root=tmp / "raw",
        cache_dir=tmp / "cache",
        console_db=tmp / "pages.duckdb",
        cc_data_base=fake.base_url,
        cc_index_base=fake.base_url,
        backoff_s=(0.01, 0.01),
        user_agent="strata-tests/0.1",
    )


async def test_an_agent_profiles_diffs_pulls_and_traces_in_one_conversation(
    fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path
) -> None:
    server = build_server(settings_for(fake, pg_dsn, tmp_path))
    async with Client(server) as agent:
        tools = {t.name: t for t in (await agent.list_tools()).tools}
        assert set(tools) == {
            "profile_site",
            "diff_site",
            "change_rates",
            "pull_pages",
            "trace_row",
        }
        assert tools["profile_site"].annotations is not None
        assert tools["profile_site"].annotations.read_only_hint is True
        assert tools["pull_pages"].annotations is not None
        assert tools["pull_pages"].annotations.read_only_hint is False
        assert tools["profile_site"].output_schema is not None

        profile = await agent.call_tool("profile_site", {"site": "example.org", "crawls": 2})
        assert not profile.is_error
        p = profile.structured_content
        assert p is not None
        assert [c["pages"] for c in p["crawls"]] == [5, 4]
        assert p["robots"][0]["rules"] == ["User-agent: *", "Disallow: /doc/5"]
        assert p["robots"][0]["sitemaps"] == ["https://example.org/s.xml"]
        assert p["shapes"][0] == {
            "shape": "/doc/N",
            "pages": 5,
            "share": 1.0,
            "allowed": 4,
            "example": "https://example.org/doc/1",
        }
        assert p["page_fetches"] == 0

        diff = (await agent.call_tool("diff_site", {"site": "example.org"})).structured_content
        assert diff is not None
        assert diff["counts"] == {"added": 2, "removed": 1, "changed": 1, "same": 2}
        assert diff["examples"]["changed"] == ["https://example.org/doc/1"]
        assert diff["requests"] == 0  # both catalogs were read by the profile

        rates = await agent.call_tool("change_rates", {"site": "example.org", "crawls": 2})
        assert rates.structured_content is not None
        assert rates.structured_content["changing"][0]["changed"] == 1

        pulled = await agent.call_tool(
            "pull_pages", {"site": "example.org", "limit": 3, "crawl": CRAWL}
        )
        assert not pulled.is_error, pulled.content
        got = pulled.structured_content
        assert got is not None
        assert (got["state"], got["written"]) == ("succeeded", 3)
        assert all(page["title"].startswith("Doc ") for page in got["pages"])
        assert "main text" in got["pages"][0]["excerpt"]

        trace = await agent.call_tool("trace_row", {"capture_id": got["pages"][0]["capture_id"]})
        t = trace.structured_content
        assert t is not None
        assert all(t["checks"].values())
        assert t["common_crawl"]["range"].startswith("bytes=")


async def test_tool_failures_come_back_as_errors_the_agent_can_read(
    fake: FakeCommonCrawl, pg_dsn: str, tmp_path: Path
) -> None:
    async with Client(build_server(settings_for(fake, pg_dsn, tmp_path))) as agent:
        poor = await agent.call_tool("profile_site", {"site": "example.org", "budget": 1})
        assert poor.is_error
        assert "larger budget" in poor.content[0].text  # type: ignore[union-attr]

        missing = await agent.call_tool("trace_row", {"capture_id": "nope"})
        assert missing.is_error
        assert "no capture nope" in missing.content[0].text  # type: ignore[union-attr]
