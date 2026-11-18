"""Paying for DuckDB's URL-index requests: estimate, reserve, run, settle.

DuckDB can't ask the budget before each request, so a query reserves its estimate first (from
the row-group statistics the shard map caches) and is charged or refunded the difference when it
ends. These tests check the estimate against what a real query sends over HTTP, and the three
outcomes: refused before it runs, refunded after, charged as far as the cap allows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import httpx
import pytest

from strata_core.ledger import JOB_TOTAL
from strata_fetch.cc.columnar import ColumnarIndex, IndexBudgetError, StaticShards
from strata_fetch.cc.shardmap import HttpTail, ShardMap
from strata_fetch.jobs import LedgerBudget
from strata_fetch.ratelimit import HostGate
from tests.fakecc import CRAWL
from tests.fakes import MemoryLedger
from tests.rangeserver import serve
from tests.unit.test_duckdb_requests import write_big_index

pytestmark = pytest.mark.usefixtures("httpfs")


async def no_sleep(_: float) -> None:
    return None


@dataclass
class SpyBudget:
    """Records what the index asked for; ``cap`` is how many requests it may reserve."""

    cap: int
    reserved: list[int] = field(default_factory=list)
    settled: list[tuple[int, int]] = field(default_factory=list)

    async def reserve(self, n: int) -> bool:
        self.reserved.append(n)
        return n <= self.cap

    async def settle(self, reserved: int, actual: int) -> int:
        self.settled.append((reserved, actual))
        return max(0, actual - self.cap)


def index_for(url: str, client: httpx.AsyncClient, cache: Path, budget: object) -> ColumnarIndex:
    tails = HttpTail(client, HostGate(rate=1000, burst=10), backoff=(0.01,), sleep=no_sleep)
    return ColumnarIndex(
        StaticShards([url]),
        shard_map=ShardMap(tails, cache_dir=cache),
        budget=budget,  # type: ignore[arg-type]
    )


def write_cc_like_index(root: Path, rows: int) -> str:
    """One file with a single large row group, its columns in the URL index's order: the
    columns a site query reads sit between ones it skips (url_path, url_query, ...)."""
    part = root / "cc-index/table/cc-main/warc" / f"crawl={CRAWL}" / "subset=warc"
    part.mkdir(parents=True, exist_ok=True)
    out = part / "part-00000.parquet"
    con = duckdb.connect()
    con.execute(
        f"""COPY (
          SELECT
            'org,example,h' || lpad((i // 1000)::VARCHAR, 5, '0') || ')/' || md5(i::VARCHAR)
              AS url_surtkey,
            'https://h' || lpad((i // 1000)::VARCHAR, 5, '0') || '.example.org/' || md5(i::VARCHAR)
              AS url,
            'h' || lpad((i // 1000)::VARCHAR, 5, '0') || '.example.org' AS url_host_name,
            'org' AS url_host_tld,
            'example.org' AS url_host_registered_domain,
            '/' || md5(i::VARCHAR) || '/' || md5((i + 1)::VARCHAR) AS url_path,
            CASE WHEN i % 7 = 0 THEN 'q=' || md5((i + 2)::VARCHAR) END AS url_query,
            TIMESTAMPTZ '2026-10-01 00:00:00+00' + to_seconds(i) AS fetch_time,
            200::SMALLINT AS fetch_status,
            NULL::VARCHAR AS fetch_redirect,
            upper(substr(md5((i + 3)::VARCHAR), 1, 32)) AS content_digest,
            'text/html' AS content_mime_type,
            'UTF-8' AS content_charset,
            'eng' AS content_languages,
            'crawl-data/x/warc/f-' || lpad((i % 90000)::VARCHAR, 5, '0') || '.warc.gz'
              AS warc_filename,
            (i * 7919) % 1000000000 AS warc_record_offset,
            2000 + i % 30000 AS warc_record_length
          FROM range({rows}) t(i)
        ) TO '{out.as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE {rows + 1}, COMPRESSION zstd)"""
    )
    con.close()
    return out.relative_to(root).as_posix()


async def test_the_estimate_holds_on_a_large_row_group(tmp_path: Path) -> None:
    """Common Crawl's row groups hold well over a million rows (the RFC count read one of 1.67
    million: 49 requests, then estimated at 6). DuckDB reads such a row group in 2 MiB blocks;
    the estimate must count those blocks, and stay above the real number."""
    rel = write_cc_like_index(tmp_path, rows=300_000)
    budget = SpyBudget(cap=10_000)
    async with httpx.AsyncClient() as client:
        with serve(tmp_path) as server:
            index = index_for(server.url(rel), client, tmp_path / "cache", budget)
            catalog, _, _, _ = await index.catalog("h00150.example.org", CRAWL)
            duck = [
                r for r in server.requests if r.user_agent and r.user_agent.startswith("duckdb/")
            ]
    assert len(catalog.refs) == 1000
    ((reserved, actual),) = budget.settled
    assert actual == len(duck) > 10  # several blocks, not one request per run
    # At most one read per run (two runs here: the URL columns, then fetch_time onward) above.
    assert actual <= reserved <= actual + 2


async def test_a_query_the_budget_cannot_cover_is_not_run(tmp_path: Path) -> None:
    rel = write_big_index(tmp_path)
    budget = SpyBudget(cap=0)
    async with httpx.AsyncClient() as client:
        with serve(tmp_path) as server:
            index = index_for(server.url(rel), client, tmp_path / "cache", budget)
            with pytest.raises(IndexBudgetError, match="so it was not run"):
                await index.find("peps.example.org", CRAWL, limit=100)
            duck = [
                r for r in server.requests if r.user_agent and r.user_agent.startswith("duckdb/")
            ]
    assert duck == []  # only the shard map's footer read went out, never DuckDB
    assert budget.settled == []


async def test_the_ledger_is_refunded_to_the_actual_count(tmp_path: Path) -> None:
    rel = write_big_index(tmp_path)
    ledger = MemoryLedger()
    await ledger.open("j", total_cap=500, host_caps={"h": 500})
    async with httpx.AsyncClient() as client:
        with serve(tmp_path) as server:
            index = index_for(
                server.url(rel), client, tmp_path / "cache", LedgerBudget(ledger, "j", "h")
            )
            found = await index.find("peps.example.org", CRAWL, limit=100)
    assert found.duckdb is not None
    usage = await ledger.usage("j")
    assert usage["h"][0] == usage[JOB_TOTAL][0] == found.duckdb.requests


async def test_ledger_budget_charges_what_fits_and_reports_the_rest() -> None:
    ledger = MemoryLedger()
    await ledger.open("j", total_cap=10, host_caps={"h": 10})
    budget = LedgerBudget(ledger, "j", "h")
    assert await budget.reserve(6)
    assert not await budget.reserve(5)  # 6 + 5 > 10: refused whole, nothing taken
    assert (await ledger.usage("j"))["h"][0] == 6
    assert await budget.settle(6, 4) == 0  # refund 2
    assert (await ledger.usage("j"))["h"][0] == 4
    assert await budget.reserve(4)
    assert await budget.settle(4, 9) == 3  # 5 more wanted, 2 fit: 3 could not be paid
    assert (await ledger.usage("j"))["h"][0] == 10
