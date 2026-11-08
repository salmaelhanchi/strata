"""The lake: bronze tables in DuckLake (Postgres catalog, Parquet data), used through DuckDB.

``Lake.open`` attaches DuckLake by default. ``lake_local_file`` swaps in a plain DuckDB file
with the same catalog name and SQL; tests use it, and so can a laptop without the extensions.
Writes are idempotent: rows are inserted only when their ``capture_id`` is not already present,
because the lake does not enforce unique keys.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import duckdb

from strata_core.models import Capture
from strata_core.settings import Settings

CATALOG = "lake"

# Column order is the contract between Capture, the DDL and every INSERT.
CAPTURE_COLUMNS: tuple[str, ...] = tuple(Capture.model_fields)

_CAPTURES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.captures (
    capture_id             VARCHAR NOT NULL,
    job_id                 VARCHAR NOT NULL,
    source                 VARCHAR NOT NULL,
    crawl                  VARCHAR,
    url                    VARCHAR NOT NULL,
    url_surtkey            VARCHAR,
    upstream_fetch_time    TIMESTAMP,
    fetched_at             TIMESTAMP NOT NULL,
    upstream_warc_filename VARCHAR,
    upstream_offset        BIGINT,
    upstream_length        BIGINT,
    content_digest_sha1    VARCHAR,
    raw_uri                VARCHAR NOT NULL,
    raw_offset             BIGINT NOT NULL,
    raw_length             BIGINT NOT NULL,
    raw_sha256             VARCHAR NOT NULL,
    http_status            INTEGER,
    mime                   VARCHAR,
    outcome                VARCHAR NOT NULL,
    verification           VARCHAR NOT NULL,
    policy_verdict         VARCHAR NOT NULL,
    strata_version         VARCHAR NOT NULL
)
"""


def sql_literal(value: str) -> str:
    """Quote a string for the few DuckDB statements (ATTACH) that take no parameters."""
    return "'" + value.replace("'", "''") + "'"


class Lake:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    # ------------------------------------------------------------------ opening
    @classmethod
    def open(cls, settings: Settings, *, read_only: bool = False) -> Lake:
        if settings.lake_local_file is not None:
            return cls.attach_local(settings.lake_local_file, read_only=read_only)
        return cls.attach_ducklake(
            settings.lake_catalog, settings.lake_data_path, read_only=read_only
        )

    @classmethod
    def attach_ducklake(
        cls, catalog_conn: str, data_path: Path, *, read_only: bool = False
    ) -> Lake:
        con = duckdb.connect()
        for ext in ("ducklake", "postgres"):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        data_path.mkdir(parents=True, exist_ok=True)
        options = [f"DATA_PATH {sql_literal(data_path.as_posix().rstrip('/') + '/')}"]
        if read_only:
            options.append("READ_ONLY")
        con.execute(
            f"ATTACH {sql_literal('ducklake:postgres:' + catalog_conn)} AS {CATALOG}"
            f" ({', '.join(options)})"
        )
        return cls(con)

    @classmethod
    def attach_local(cls, path: Path, *, read_only: bool = False) -> Lake:
        con = duckdb.connect()
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        suffix = " (READ_ONLY)" if read_only else ""
        con.execute(f"ATTACH {sql_literal(str(path))} AS {CATALOG}{suffix}")
        return cls(con)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------------ schema
    def bootstrap(self) -> None:
        """Create schemas and tables if missing. Safe to run any number of times."""
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
        self.con.execute(_CAPTURES_DDL)

    # ------------------------------------------------------------------ writes
    def insert_captures(self, captures: Sequence[Capture]) -> int:
        """Insert captures not already present; return how many rows were added."""
        if not captures:
            return 0
        cols = ", ".join(CAPTURE_COLUMNS)
        marks = ", ".join("?" for _ in CAPTURE_COLUMNS)
        # Stage in a temp table outside the transaction: one DuckDB transaction writes one database.
        self.con.execute(
            f"CREATE OR REPLACE TEMP TABLE _staging_captures AS"
            f" SELECT {cols} FROM {CATALOG}.bronze.captures LIMIT 0"
        )
        self.con.executemany(
            f"INSERT INTO _staging_captures ({cols}) VALUES ({marks})",
            [_row(c) for c in captures],
        )
        self.con.execute("BEGIN TRANSACTION")
        try:
            result = self.con.execute(
                f"INSERT INTO {CATALOG}.bronze.captures ({cols})"
                f" SELECT DISTINCT ON (s.capture_id) {', '.join('s.' + c for c in CAPTURE_COLUMNS)}"
                f" FROM _staging_captures s"
                f" WHERE NOT EXISTS (SELECT 1 FROM {CATALOG}.bronze.captures c"
                f" WHERE c.capture_id = s.capture_id)"
            ).fetchone()
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        finally:
            self.con.execute("DROP TABLE IF EXISTS _staging_captures")
        return int(result[0]) if result else 0

    # ------------------------------------------------------------------ reads
    def get_capture(self, capture_id: str) -> Capture | None:
        row = self.con.execute(
            f"SELECT {', '.join(CAPTURE_COLUMNS)} FROM {CATALOG}.bronze.captures"
            " WHERE capture_id = ? LIMIT 1",
            [capture_id],
        ).fetchone()
        return None if row is None else Capture(**dict(zip(CAPTURE_COLUMNS, row, strict=True)))

    def capture_ids(self, job_id: str | None = None) -> list[str]:
        if job_id is None:
            rows = self.con.execute(
                f"SELECT capture_id FROM {CATALOG}.bronze.captures ORDER BY capture_id"
            ).fetchall()
        else:
            rows = self.con.execute(
                f"SELECT capture_id FROM {CATALOG}.bronze.captures WHERE job_id = ?"
                " ORDER BY capture_id",
                [job_id],
            ).fetchall()
        return [r[0] for r in rows]

    def count(self) -> int:
        row = self.con.execute(f"SELECT count(*) FROM {CATALOG}.bronze.captures").fetchone()
        return int(row[0]) if row else 0


def _naive_utc(value: datetime) -> datetime:
    """Timestamps are stored as naive UTC, so reading them back needs no timezone library."""
    return value if value.tzinfo is None else value.astimezone(UTC).replace(tzinfo=None)


def _row(capture: Capture) -> list[Any]:
    data = capture.model_dump(mode="python")
    out: list[Any] = []
    for col in CAPTURE_COLUMNS:
        value = data[col]
        if isinstance(value, datetime):
            value = _naive_utc(value)
        elif isinstance(value, StrEnum):
            value = value.value
        out.append(value)
    return out
