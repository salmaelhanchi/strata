"""The lake: bronze tables in DuckLake (Postgres catalog, Parquet data), used through DuckDB.

``Lake.open`` attaches DuckLake by default. ``lake_local_file`` swaps in a plain DuckDB file
with the same catalog name and SQL; tests use it, and so can a laptop without the extensions.
Writes are idempotent: rows are inserted only when their ``capture_id`` is not already present,
because the lake does not enforce unique keys.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlsplit

import duckdb

from strata_core.models import Capture, Document
from strata_core.settings import Settings

CATALOG = "lake"

# Column order is the contract between Capture, the DDL and every INSERT.
CAPTURE_COLUMNS: tuple[str, ...] = tuple(Capture.model_fields)

_CAPTURES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
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
    strata_version         VARCHAR NOT NULL,
    robots_capture_id      VARCHAR
)
"""

# Tables with the captures layout: pages, and the robots.txt captures their verdicts rest on.
TABLES = ("captures", "robots")

DOCUMENT_COLUMNS: tuple[str, ...] = tuple(Document.model_fields)

_DOCUMENTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.documents (
    capture_id     VARCHAR NOT NULL,
    job_id         VARCHAR NOT NULL,
    url            VARCHAR NOT NULL,
    kind           VARCHAR NOT NULL,
    number         INTEGER,
    title          VARCHAR,
    status         VARCHAR,
    doc_type       VARCHAR,
    date           VARCHAR,
    date_precision VARCHAR,
    authors        VARCHAR[],
    extractor      VARCHAR NOT NULL,
    extracted_at   TIMESTAMP NOT NULL
)
"""


def s3_secret_sql(settings: Settings) -> str:
    """A DuckDB secret for the object store in ``settings`` (RustFS locally, AWS later)."""
    parts = ["TYPE s3", f"REGION {sql_literal(settings.s3_region)}"]
    if settings.s3_access_key:
        parts.append(f"KEY_ID {sql_literal(settings.s3_access_key)}")
    if settings.s3_secret_key:
        parts.append(f"SECRET {sql_literal(settings.s3_secret_key)}")
    if settings.s3_endpoint:
        endpoint = urlsplit(settings.s3_endpoint)
        parts += [
            f"ENDPOINT {sql_literal(endpoint.netloc)}",
            "URL_STYLE 'path'",
            f"USE_SSL {'true' if endpoint.scheme == 'https' else 'false'}",
        ]
    return f"CREATE OR REPLACE SECRET strata_s3 ({', '.join(parts)})"


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
            settings.lake_catalog,
            settings.lake_data_path,
            read_only=read_only,
            settings=settings,
            inlining_rows=settings.lake_inlining_rows,
        )

    @classmethod
    def attach_ducklake(
        cls,
        catalog_conn: str,
        data_path: str | Path,
        *,
        read_only: bool = False,
        settings: Settings | None = None,
        inlining_rows: int = 0,
    ) -> Lake:
        con = duckdb.connect()
        for ext in ("ducklake", "postgres"):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        path = str(data_path)
        if path.startswith("s3://"):
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
            con.execute(s3_secret_sql(settings or Settings()))
            data = path.rstrip("/") + "/"
        else:
            local = Path(path)
            local.mkdir(parents=True, exist_ok=True)
            data = local.as_posix().rstrip("/") + "/"
        # Always explicit: DuckLake inlines small inserts by default (10 rows in DuckDB 2.0), so
        # leaving the option out would not mean "off".
        options = [
            f"DATA_PATH {sql_literal(data)}",
            f"DATA_INLINING_ROW_LIMIT {int(inlining_rows)}",
        ]
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
        """Create schemas and tables if missing, and add columns newer versions need. Safe to run
        any number of times."""
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
        for table in TABLES:
            name = f"{CATALOG}.bronze.{table}"
            self.con.execute(_CAPTURES_DDL.format(table=name))
            # Lakes created in week 1 predate the robots link (S13).
            self.con.execute(
                f"ALTER TABLE {name} ADD COLUMN IF NOT EXISTS robots_capture_id VARCHAR"
            )
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
        self.con.execute(_DOCUMENTS_DDL)

    def replace_documents(self, job_id: str, documents: Sequence[Document]) -> int:
        """Silver rows for a job, replacing any from an earlier normalization of the same job."""
        cols = ", ".join(DOCUMENT_COLUMNS)
        marks = ", ".join("?" for _ in DOCUMENT_COLUMNS)
        self.con.execute(
            f"CREATE OR REPLACE TEMP TABLE _staging_docs AS"
            f" SELECT {cols} FROM {CATALOG}.silver.documents LIMIT 0"
        )
        if documents:
            self.con.executemany(
                f"INSERT INTO _staging_docs ({cols}) VALUES ({marks})",
                [[_plain(getattr(d, c)) for c in DOCUMENT_COLUMNS] for d in documents],
            )
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute(f"DELETE FROM {CATALOG}.silver.documents WHERE job_id = ?", [job_id])
            self.con.execute(
                f"INSERT INTO {CATALOG}.silver.documents ({cols}) SELECT {cols} FROM _staging_docs"
            )
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        finally:
            self.con.execute("DROP TABLE IF EXISTS _staging_docs")
        return len(documents)

    # ------------------------------------------------------------------ writes
    def insert_captures(self, captures: Sequence[Capture], table: str = "captures") -> int:
        """Insert captures not already present; return how many rows were added."""
        if table not in TABLES:
            raise ValueError(f"unknown bronze table {table!r}")
        if not captures:
            return 0
        name = f"{CATALOG}.bronze.{table}"
        cols = ", ".join(CAPTURE_COLUMNS)
        marks = ", ".join("?" for _ in CAPTURE_COLUMNS)
        # Stage in a temp table outside the transaction: one DuckDB transaction writes one database.
        self.con.execute(
            f"CREATE OR REPLACE TEMP TABLE _staging_captures AS SELECT {cols} FROM {name} LIMIT 0"
        )
        self.con.executemany(
            f"INSERT INTO _staging_captures ({cols}) VALUES ({marks})",
            [_row(c) for c in captures],
        )
        self.con.execute("BEGIN TRANSACTION")
        try:
            result = self.con.execute(
                f"INSERT INTO {name} ({cols})"
                f" SELECT DISTINCT ON (s.capture_id) {', '.join('s.' + c for c in CAPTURE_COLUMNS)}"
                f" FROM _staging_captures s"
                f" WHERE NOT EXISTS (SELECT 1 FROM {name} c WHERE c.capture_id = s.capture_id)"
            ).fetchone()
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        finally:
            self.con.execute("DROP TABLE IF EXISTS _staging_captures")
        return int(result[0]) if result else 0

    # ------------------------------------------------------------------ maintenance
    def data_files(self, table: str = "captures") -> int | None:
        """Parquet files behind a table (DuckLake only; None for a plain DuckDB file)."""
        try:
            row = self.con.execute(
                f"SELECT count(*) FROM ducklake_list_files('{CATALOG}', ?, schema => 'bronze')",
                [table],
            ).fetchone()
        except duckdb.Error:
            return None
        return int(row[0]) if row else 0

    def checkpoint(self, older_than: str | None = None) -> tuple[int | None, int | None, float]:
        """DuckLake maintenance: flush inlined rows, merge small files, expire old snapshots and
        clean up (CHECKPOINT). Returns active data files before and after, and the seconds it took.

        Merged files stay on the store while a snapshot still needs them. ``older_than`` (an
        interval such as ``'7 days'``) sets the lake's retention first: snapshots older than that
        expire, and files only they used are deleted. It is stored in the catalog, so later
        checkpoints keep it. Time travel reaches back only that far.
        """
        if older_than is not None:
            # Checked by DuckDB as an interval, so the value can't smuggle SQL into the call.
            (interval,) = self.con.execute("SELECT ?::INTERVAL::VARCHAR", [older_than]).fetchone()  # type: ignore[misc]
            for option in ("expire_older_than", "delete_older_than"):
                self.con.execute(f"CALL {CATALOG}.set_option(?, ?)", [option, interval])
        before = self.data_files()
        t0 = time.monotonic()
        try:
            self.con.execute(f"USE {CATALOG}")
            self.con.execute("CHECKPOINT")
        finally:
            self.con.execute("USE memory")
        return before, self.data_files(), round(time.monotonic() - t0, 3)

    # ------------------------------------------------------------------ reads
    def get_capture(self, capture_id: str, table: str = "captures") -> Capture | None:
        if table not in TABLES:
            raise ValueError(f"unknown bronze table {table!r}")
        row = self.con.execute(
            f"SELECT {', '.join(CAPTURE_COLUMNS)} FROM {CATALOG}.bronze.{table}"
            " WHERE capture_id = ? LIMIT 1",
            [capture_id],
        ).fetchone()
        return None if row is None else Capture(**dict(zip(CAPTURE_COLUMNS, row, strict=True)))

    def existing_ids(self, capture_ids: Sequence[str], table: str = "captures") -> set[str]:
        """Which of ``capture_ids`` are already in the table (stored by any job)."""
        if table not in TABLES:
            raise ValueError(f"unknown bronze table {table!r}")
        if not capture_ids:
            return set()
        rows = self.con.execute(
            f"SELECT capture_id FROM {CATALOG}.bronze.{table}"
            " WHERE list_contains(?::VARCHAR[], capture_id)",
            [list(capture_ids)],
        ).fetchall()
        return {str(r[0]) for r in rows}

    def captures_by_ids(self, capture_ids: Sequence[str]) -> list[Capture]:
        if not capture_ids:
            return []
        rows = self.con.execute(
            f"SELECT {', '.join(CAPTURE_COLUMNS)} FROM {CATALOG}.bronze.captures"
            " WHERE list_contains(?::VARCHAR[], capture_id) ORDER BY capture_id",
            [list(capture_ids)],
        ).fetchall()
        return [Capture(**dict(zip(CAPTURE_COLUMNS, row, strict=True))) for row in rows]

    def captures_for_job(self, job_id: str, table: str = "captures") -> list[Capture]:
        if table not in TABLES:
            raise ValueError(f"unknown bronze table {table!r}")
        rows = self.con.execute(
            f"SELECT {', '.join(CAPTURE_COLUMNS)} FROM {CATALOG}.bronze.{table}"
            " WHERE job_id = ? ORDER BY capture_id",
            [job_id],
        ).fetchall()
        return [Capture(**dict(zip(CAPTURE_COLUMNS, row, strict=True))) for row in rows]

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


def _plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return _naive_utc(value)
    if isinstance(value, StrEnum):
        return value.value
    return value


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
