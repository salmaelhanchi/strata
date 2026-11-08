"""The URL-index shard map: footers read once, through our own paced client, cached per crawl."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import httpx
import pytest
from pytest_httpserver import HTTPServer

from strata_core.models import Outcome
from strata_fetch.cc.index import IndexAttempt, IndexBlockedError
from strata_fetch.cc.shardmap import (
    HttpTail,
    LocalTail,
    ShardMap,
    ShardMapError,
    ShardRange,
    footer_stats,
    read_footer,
)
from strata_fetch.ratelimit import HostGate
from strata_fetch.surt import surt_range
from tests.fakecc import FakeCommonCrawl

CRAWL = "CC-MAIN-2026-43"
SHARD_PATH = f"/cc-index/table/cc-main/warc/crawl={CRAWL}/subset=warc/part-00000.parquet"


def write_keys(path: Path, keys: list[str]) -> Path:
    """A Parquet file with a url_surtkey column (and padding, so the file isn't all footer)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (url_surtkey VARCHAR, pad VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, repeat('x', 300))", [[k] for k in keys])
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 2)")
    con.close()
    return path


@pytest.fixture
def three_shards(tmp_path: Path) -> list[str]:
    return [
        write_keys(tmp_path / "a.parquet", ["com,acme)/", "com,acme)/x"]).as_posix(),
        write_keys(tmp_path / "b.parquet", ["org,example)/", "org,example,www)/b"]).as_posix(),
        write_keys(tmp_path / "c.parquet", ["org,examples)/", "org,zzz)/"]).as_posix(),
    ]


class CountingTail(LocalTail):
    def __init__(self, fail_on: str | None = None) -> None:
        self.tails: list[str] = []
        self.ranges: list[str] = []
        self.fail_on = fail_on

    async def read_tail(self, path: str, n: int) -> tuple[bytes, int, list[IndexAttempt]]:
        if path == self.fail_on:
            raise ShardMapError(f"{path}: HTTP 503")
        self.tails.append(path)
        return await super().read_tail(path, n)

    async def read_range(
        self, path: str, first: int, last: int
    ) -> tuple[bytes, list[IndexAttempt]]:
        self.ranges.append(path)
        return await super().read_range(path, first, last)


async def no_sleep(_: float) -> None:
    return None


# ----------------------------------------------------------------------------------- footers
async def test_a_footer_alone_gives_the_files_key_range(three_shards: list[str]) -> None:
    footer, attempts = await read_footer(LocalTail(), three_shards[1])
    assert footer.startswith(b"PAR1") and footer.endswith(b"PAR1")
    assert footer_stats(footer) == ("org,example)/", "org,example,www)/b")
    assert attempts == []


async def test_a_footer_larger_than_the_tail_costs_one_more_read(three_shards: list[str]) -> None:
    reader = CountingTail()
    footer, _ = await read_footer(reader, three_shards[0], tail_bytes=64)
    assert footer_stats(footer) == ("com,acme)/", "com,acme)/x")
    assert (len(reader.tails), len(reader.ranges)) == (1, 1)


async def test_not_a_parquet_file_is_an_error(tmp_path: Path) -> None:
    junk = tmp_path / "junk.parquet"
    junk.write_bytes(b"not parquet at all")
    with pytest.raises(ShardMapError, match="not a Parquet file"):
        await read_footer(LocalTail(), junk.as_posix())


async def test_a_corrupt_footer_is_a_clean_error(tmp_path: Path) -> None:
    bad = tmp_path / "bad.parquet"
    garbage = b"\x15\x00" * 20
    bad.write_bytes(b"PAR1" + garbage + len(garbage).to_bytes(4, "little") + b"PAR1")
    with pytest.raises(ShardMapError, match="unreadable Parquet footer"):
        await ShardMap(LocalTail()).ranges(CRAWL, [bad.as_posix()])


def test_missing_statistics_never_rule_a_file_out() -> None:
    unknown = ShardRange(path="x", min_key=None, max_key=None)
    known = ShardRange(path="y", min_key="com,acme)/", max_key="com,acme)/x")
    lo, hi = surt_range("example.org")
    assert unknown.covers(lo, hi)
    assert not known.covers(lo, hi)


# ----------------------------------------------------------------------------------- over HTTP
@pytest.fixture
def served(httpserver: HTTPServer, three_shards: list[str]) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    cc.files[SHARD_PATH] = Path(three_shards[1]).read_bytes()
    cc.install()
    return cc


def tails(client: httpx.AsyncClient, **kw: object) -> HttpTail:
    return HttpTail(
        client,
        HostGate(rate=1000, burst=10),
        backoff=(0.01, 0.01),
        sleep=no_sleep,
        **kw,  # type: ignore[arg-type]
    )


async def test_one_suffix_range_request_reads_a_remote_footer(served: FakeCommonCrawl) -> None:
    seen: list[IndexAttempt] = []

    async def on_attempt(a: IndexAttempt) -> None:
        seen.append(a)

    async with httpx.AsyncClient() as client:
        footer, attempts = await read_footer(
            tails(client, on_attempt=on_attempt), served.base_url + SHARD_PATH.lstrip("/")
        )
    assert footer_stats(footer) == ("org,example)/", "org,example,www)/b")
    assert [r[2] for r in served.requests] == ["bytes=-65536"]
    assert [a.outcome for a in attempts] == [Outcome.OK] and seen == attempts


async def test_a_large_remote_footer_takes_an_explicit_second_range(
    served: FakeCommonCrawl,
) -> None:
    async with httpx.AsyncClient() as client:
        footer, attempts = await read_footer(
            tails(client), served.base_url + SHARD_PATH.lstrip("/"), tail_bytes=64
        )
    assert footer_stats(footer) == ("org,example)/", "org,example,www)/b")
    first, second = [r[2] for r in served.requests]
    assert first == "bytes=-64" and second is not None and not second.startswith("bytes=-")
    assert len(attempts) == 2


async def test_a_server_ignoring_range_is_refused_not_downloaded(served: FakeCommonCrawl) -> None:
    served.ignore_range = True
    async with httpx.AsyncClient() as client:
        with pytest.raises(ShardMapError, match="ignored the Range header") as info:
            await read_footer(tails(client), served.base_url + SHARD_PATH.lstrip("/"))
    assert len(info.value.attempts) == 1  # not retried: it would do the same again


async def test_throttling_is_retried_and_a_block_stops(served: FakeCommonCrawl) -> None:
    url = served.base_url + SHARD_PATH.lstrip("/")
    served.fail_next[SHARD_PATH].append((503, {"Retry-After": "0"}))
    async with httpx.AsyncClient() as client:
        _, attempts = await read_footer(tails(client), url)
        served.fail_next[SHARD_PATH].append((403, {}))
        with pytest.raises(IndexBlockedError):
            await read_footer(tails(client), url)
    assert [a.outcome for a in attempts] == [Outcome.RATE_LIMITED, Outcome.OK]


# ----------------------------------------------------------------------------------- the map
async def test_select_returns_only_files_covering_the_domain(
    three_shards: list[str], tmp_path: Path
) -> None:
    progress: list[tuple[int, int]] = []

    def on_progress(done: int, total: int) -> None:
        progress.append((done, total))

    shard_map = ShardMap(
        LocalTail(), cache_dir=tmp_path / "cache", on_progress=on_progress, progress_every=2
    )
    lo, hi = surt_range("example.org")
    selection = await shard_map.select(CRAWL, three_shards, lo, hi)
    assert selection.files == [three_shards[1]]
    assert selection.footers_read == 3
    assert progress == [(0, 3), (2, 3), (3, 3)]


async def test_the_map_is_cached_per_crawl_and_resumes_after_an_interruption(
    three_shards: list[str], tmp_path: Path
) -> None:
    cache = tmp_path / "cache"
    broken = CountingTail(fail_on=three_shards[1])
    with pytest.raises(ShardMapError):
        await ShardMap(broken, cache_dir=cache).ranges(CRAWL, three_shards)
    assert broken.tails == [three_shards[0]]  # the first footer was read and kept

    resumed = CountingTail()
    ranges, _, read = await ShardMap(resumed, cache_dir=cache).ranges(CRAWL, three_shards)
    assert resumed.tails == three_shards[1:] and read == 2
    assert [r.min_key for r in ranges] == ["com,acme)/", "org,example)/", "org,examples)/"]

    again = CountingTail()
    _, _, read = await ShardMap(again, cache_dir=cache).ranges(CRAWL, three_shards)
    assert again.tails == [] and read == 0


async def test_a_map_from_another_format_version_is_rebuilt(
    three_shards: list[str], tmp_path: Path
) -> None:
    path = tmp_path / "cache" / "shardmaps" / f"{CRAWL}.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"v": 0, "crawl": CRAWL}) + "\n", encoding="utf-8")
    reader = CountingTail()
    await ShardMap(reader, cache_dir=tmp_path / "cache").ranges(CRAWL, three_shards)
    assert len(reader.tails) == 3
    assert json.loads(path.read_text(encoding="utf-8").splitlines()[0])["v"] == 1
