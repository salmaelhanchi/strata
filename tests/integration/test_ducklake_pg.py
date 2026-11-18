"""The real target: DuckLake with its catalog in Postgres.

Skipped where the DuckDB extensions can't be installed.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from strata_core.lake import Lake
from tests.unit.test_lake import make_capture

pytestmark = [pytest.mark.postgres, pytest.mark.ducklake]


def _extensions_available() -> bool:
    con = duckdb.connect()
    try:
        for ext in ("ducklake", "postgres"):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
    except duckdb.Error:
        return False
    finally:
        con.close()
    return True


@pytest.fixture
def catalog_conninfo(pg_dsn: str) -> str:
    if not _extensions_available():
        pytest.skip("DuckDB ducklake/postgres extensions can't be installed here")
    with psycopg.connect(pg_dsn):
        pass
    params = conninfo_to_dict(pg_dsn)
    return make_conninfo(**params)


def test_ducklake_round_trip_and_idempotent_rerun(catalog_conninfo: str, tmp_path: Path) -> None:
    data = tmp_path / "lake"
    with Lake.attach_ducklake(catalog_conninfo, data) as lake:
        lake.bootstrap()
        assert lake.insert_captures([make_capture(1), make_capture(2)]) == 2
    # A second process (here: a second attach) sees the rows and adds nothing on rerun.
    with Lake.attach_ducklake(catalog_conninfo, data) as lake:
        assert lake.insert_captures([make_capture(1), make_capture(2)]) == 0
        assert lake.capture_ids() == ["cap0001", "cap0002"]


def test_checkpoint_merges_flushes_and_retention_deletes_the_old_files(
    catalog_conninfo: str, tmp_path: Path
) -> None:
    """Small bronze flushes become one file; with a retention, the merged ones go too."""
    data = tmp_path / "lake"
    on_disk = lambda: len(list(data.rglob("*.parquet")))  # noqa: E731
    with Lake.attach_ducklake(catalog_conninfo, data) as lake:
        lake.bootstrap()
        for batch in range(4):
            lake.insert_captures([make_capture(batch * 10 + i) for i in range(10)])
        assert (lake.data_files(), on_disk()) == (4, 4)
        before, after, _ = lake.checkpoint()
        assert (before, after, on_disk()) == (4, 1, 5)  # merged; old files kept for time travel
        lake.insert_captures([make_capture(100)])
        _, after, _ = lake.checkpoint(older_than="0 seconds")
        assert after == 1 and on_disk() == 1
        assert lake.count() == 41
        with pytest.raises(duckdb.Error):
            lake.checkpoint(older_than="1 day; DROP TABLE x")


def test_inlined_flushes_write_no_files_until_checkpoint(
    catalog_conninfo: str, tmp_path: Path
) -> None:
    data = tmp_path / "lake"
    with Lake.attach_ducklake(catalog_conninfo, data, inlining_rows=50) as lake:
        lake.bootstrap()
        for batch in range(4):
            lake.insert_captures([make_capture(batch * 10 + i) for i in range(10)])
        assert lake.count() == 40
        assert list(data.rglob("*.parquet")) == []  # the rows live in the catalog for now
        lake.checkpoint()
        assert len(list(data.rglob("*.parquet"))) == 1
        assert lake.count() == 40
