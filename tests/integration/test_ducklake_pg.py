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
