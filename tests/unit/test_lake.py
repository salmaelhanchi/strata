from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from strata_core.lake import CAPTURE_COLUMNS, Lake, sql_literal
from strata_core.models import Capture, Outcome, PolicyVerdict, Source, Verification


def make_capture(n: int, job_id: str = "job_1") -> Capture:
    return Capture(
        capture_id=f"cap{n:04d}",
        job_id=job_id,
        source=Source.COMMON_CRAWL,
        crawl="CC-MAIN-2026-17",
        url=f"https://example.org/p{n}",
        url_surtkey=f"org,example)/p{n}",
        upstream_fetch_time=datetime(2026, 4, 20, 12, 0),
        fetched_at=datetime(2026, 11, 7, 17, 0, tzinfo=UTC),
        upstream_warc_filename="crawl-data/x.warc.gz",
        upstream_offset=n * 1000,
        upstream_length=900,
        content_digest_sha1="A" * 32,
        raw_uri="file:///tmp/raw/part-00000.warc.gz",
        raw_offset=n * 900,
        raw_length=900,
        raw_sha256="b" * 64,
        http_status=200,
        mime="text/html",
        outcome=Outcome.OK,
        verification=Verification.OK,
        policy_verdict=PolicyVerdict.NOT_EVALUATED,
        strata_version="0.1.0",
    )


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    lk = Lake.attach_local(tmp_path / "lake.duckdb")
    lk.bootstrap()
    return lk


def test_bootstrap_is_idempotent(lake: Lake) -> None:
    lake.bootstrap()
    lake.bootstrap()
    assert lake.count() == 0


def test_insert_and_read_back_round_trips(lake: Lake) -> None:
    caps = [make_capture(i) for i in range(3)]
    assert lake.insert_captures(caps) == 3
    back = lake.get_capture("cap0001")
    assert back is not None
    # fetched_at is stored as naive UTC; everything else round-trips exactly
    assert back.model_dump(exclude={"fetched_at"}) == caps[1].model_dump(exclude={"fetched_at"})
    assert back.fetched_at == datetime(2026, 11, 7, 17, 0)


def test_inserts_are_idempotent_across_runs_and_within_a_batch(lake: Lake) -> None:
    assert lake.insert_captures([make_capture(1), make_capture(2), make_capture(2)]) == 2
    assert lake.insert_captures([make_capture(1), make_capture(2), make_capture(3)]) == 1
    assert lake.count() == 3
    assert lake.capture_ids() == ["cap0001", "cap0002", "cap0003"]


def test_capture_ids_filter_by_job(lake: Lake) -> None:
    lake.insert_captures([make_capture(1, "job_a"), make_capture(2, "job_b")])
    assert lake.capture_ids("job_b") == ["cap0002"]


def test_empty_batch_is_a_no_op(lake: Lake) -> None:
    assert lake.insert_captures([]) == 0


def test_lake_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "lake.duckdb"
    with Lake.attach_local(path) as lk:
        lk.bootstrap()
        lk.insert_captures([make_capture(7)])
    with Lake.attach_local(path, read_only=True) as lk:
        assert lk.capture_ids() == ["cap0007"]


def test_columns_follow_the_model() -> None:
    assert CAPTURE_COLUMNS[0] == "capture_id"
    assert set(CAPTURE_COLUMNS) == set(Capture.model_fields)


def test_sql_literal_escapes_quotes() -> None:
    assert sql_literal("it's") == "'it''s'"
