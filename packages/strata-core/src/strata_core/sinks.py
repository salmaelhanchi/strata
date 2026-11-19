"""Sinks: where pulled pages and site changes land in the user's own database.

One protocol (``write_pages``, ``write_changes``), four stores: Postgres, SQLite, DuckDB and
Parquet. Every write is an upsert on the table's key, so pulling again replaces rows instead of
duplicating them. Each row keeps its lineage: the capture id (``strata lineage``), Common
Crawl's WARC location, the payload digest, the robots.txt verdict, the job and the normalizer.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import duckdb
import psycopg
from pydantic import BaseModel, ConfigDict

PAGES = "strata_pages"
CHANGES = "strata_site_changes"


class PageRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str
    crawl: str
    captured_at: datetime | None  # when Common Crawl fetched the page
    http_status: int | None
    title: str | None
    main_text: str | None
    lang: str | None
    out_links: list[str]
    payload_sha1: str | None  # Common Crawl's digest, checked against the bytes Strata stored
    warc_ref: str  # Common Crawl's WARC file, offset and length: anyone can re-read it
    robots_verdict: str
    capture_id: str  # `strata lineage CAPTURE_ID` traces the row to the stored bytes
    job_id: str
    normalizer_version: str


class ChangeRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str
    from_crawl: str
    to_crawl: str
    change: str  # added, removed, changed (unchanged URLs are not written)


PAGE_COLUMNS = tuple(PageRow.model_fields)
CHANGE_COLUMNS = tuple(ChangeRow.model_fields)


class Sink(Protocol):
    def write_pages(self, pages: Sequence[PageRow]) -> int: ...

    def write_changes(self, changes: Sequence[ChangeRow]) -> int: ...

    def close(self) -> None: ...


def _ddl(text: str, links: str, ts: str) -> tuple[str, str]:
    pages = f"""CREATE TABLE IF NOT EXISTS {PAGES} (
    url TEXT NOT NULL, crawl TEXT NOT NULL, captured_at {ts}, http_status INTEGER,
    title {text}, main_text {text}, lang {text}, out_links {links}, payload_sha1 {text},
    warc_ref {text} NOT NULL, robots_verdict {text} NOT NULL, capture_id {text} NOT NULL,
    job_id {text} NOT NULL, normalizer_version {text} NOT NULL,
    PRIMARY KEY (url, crawl))"""
    changes = f"""CREATE TABLE IF NOT EXISTS {CHANGES} (
    url TEXT NOT NULL, from_crawl TEXT NOT NULL, to_crawl TEXT NOT NULL,
    change TEXT NOT NULL CHECK (change IN ('added', 'removed', 'changed')),
    PRIMARY KEY (url, from_crawl, to_crawl))"""
    return pages, changes


def _upsert(table: str, columns: Sequence[str], keys: Sequence[str], mark: str) -> str:
    updates = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in keys)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join([mark] * len(columns))})"
        f" ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
    )


def _values(row: BaseModel, columns: Sequence[str], links: Any) -> list[Any]:
    data = row.model_dump()
    return [links(data[c]) if c == "out_links" else data[c] for c in columns]


class PostgresSink:
    def __init__(self, dsn: str) -> None:
        self._con = psycopg.connect(dsn, autocommit=False)
        pages, changes = _ddl("TEXT", "JSONB", "TIMESTAMPTZ")
        with self._con.transaction():
            self._con.execute(pages)
            self._con.execute(changes)

    def _write(self, table: str, columns: Sequence[str], keys: Sequence[str], rows: Any) -> int:
        sql = _upsert(table, columns, keys, "%s")
        with self._con.transaction(), self._con.cursor() as cur:
            cur.executemany(sql, [_values(r, columns, json.dumps) for r in rows])
        return len(rows)

    def write_pages(self, pages: Sequence[PageRow]) -> int:
        return self._write(PAGES, PAGE_COLUMNS, ("url", "crawl"), pages)

    def write_changes(self, changes: Sequence[ChangeRow]) -> int:
        return self._write(CHANGES, CHANGE_COLUMNS, ("url", "from_crawl", "to_crawl"), changes)

    def close(self) -> None:
        self._con.close()


class SqliteSink:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Opened here, written from the pull's worker thread; one writer at a time, never two.
        self._con = sqlite3.connect(path, check_same_thread=False)
        pages, changes = _ddl("TEXT", "TEXT", "TEXT")  # links as a JSON array, times as ISO 8601
        with self._con:
            self._con.execute(pages)
            self._con.execute(changes)

    def _write(self, table: str, columns: Sequence[str], keys: Sequence[str], rows: Any) -> int:
        def plain(row: BaseModel) -> list[Any]:
            out = _values(row, columns, json.dumps)
            return [v.isoformat() if isinstance(v, datetime) else v for v in out]

        with self._con:
            self._con.executemany(_upsert(table, columns, keys, "?"), [plain(r) for r in rows])
        return len(rows)

    def write_pages(self, pages: Sequence[PageRow]) -> int:
        return self._write(PAGES, PAGE_COLUMNS, ("url", "crawl"), pages)

    def write_changes(self, changes: Sequence[ChangeRow]) -> int:
        return self._write(CHANGES, CHANGE_COLUMNS, ("url", "from_crawl", "to_crawl"), changes)

    def close(self) -> None:
        self._con.close()


class DuckDbSink:
    """A DuckDB database file; also what the Parquet sink and the SDK's ``to_duckdb`` use."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        pages, changes = _ddl("VARCHAR", "VARCHAR[]", "TIMESTAMPTZ")
        self.con.execute(pages)
        self.con.execute(changes)

    @classmethod
    def open(cls, path: Path) -> DuckDbSink:
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(duckdb.connect(str(path)))

    def _write(self, table: str, columns: Sequence[str], keys: Sequence[str], rows: Any) -> int:
        if rows:
            self.con.executemany(
                _upsert(table, columns, keys, "?"), [_values(r, columns, list) for r in rows]
            )
        return len(rows)

    def write_pages(self, pages: Sequence[PageRow]) -> int:
        return self._write(PAGES, PAGE_COLUMNS, ("url", "crawl"), pages)

    def write_changes(self, changes: Sequence[ChangeRow]) -> int:
        return self._write(CHANGES, CHANGE_COLUMNS, ("url", "from_crawl", "to_crawl"), changes)

    def close(self) -> None:
        self.con.close()


class ParquetFolderSink:
    """``strata_pages.parquet`` and ``strata_site_changes.parquet`` in a folder. Each write
    merges with the file already there, through DuckDB, and replaces it atomically."""

    def __init__(self, folder: Path) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        self._folder = folder
        self._db = DuckDbSink(duckdb.connect())
        for table in (PAGES, CHANGES):
            existing = folder / f"{table}.parquet"
            if existing.exists():
                self._db.con.execute(
                    f"INSERT INTO {table} SELECT * FROM read_parquet(?)", [existing.as_posix()]
                )

    def _flush(self, table: str) -> None:
        target = self._folder / f"{table}.parquet"
        tmp = target.with_suffix(".tmp")
        self._db.con.execute(f"COPY {table} TO '{tmp.as_posix()}' (FORMAT parquet)")
        tmp.replace(target)

    def write_pages(self, pages: Sequence[PageRow]) -> int:
        n = self._db.write_pages(pages)
        self._flush(PAGES)
        return n

    def write_changes(self, changes: Sequence[ChangeRow]) -> int:
        n = self._db.write_changes(changes)
        self._flush(CHANGES)
        return n

    def close(self) -> None:
        self._db.close()


SCHEMES = ("postgresql", "postgres", "sqlite", "duckdb", "parquet")


def open_sink(target: str) -> Sink:
    """``postgresql://…``, ``sqlite:///path.db``, ``duckdb:///path.duckdb`` or
    ``parquet:///folder``. As in SQLAlchemy, three slashes start a relative path and four an
    absolute one (``sqlite:///C:/data/x.db`` on Windows). A bare path is taken by its suffix
    (.db/.sqlite, .duckdb, or a folder for Parquet); ``dbname=... host=...`` is Postgres."""
    if "://" not in target and re.match(r"^\s*\w+\s*=", target):
        return PostgresSink(target)  # libpq key=value form: "dbname=pages host=localhost"
    scheme, _, rest = target.partition("://")
    if not rest:
        path = Path(target)
        suffix = path.suffix.lower()
        scheme = {".db": "sqlite", ".sqlite": "sqlite", ".duckdb": "duckdb"}.get(suffix, "parquet")
        rest = "/" + target
    if scheme in ("postgresql", "postgres"):
        return PostgresSink(target)
    path = Path(rest[1:] if rest.startswith("/") else rest)
    if scheme == "sqlite":
        return SqliteSink(path)
    if scheme == "duckdb":
        return DuckDbSink.open(path)
    if scheme == "parquet":
        return ParquetFolderSink(path)
    raise ValueError(f"unknown sink {target!r}: use one of {', '.join(SCHEMES)}")


def describe(target: str) -> str:
    """The sink without its password, for reports and logs."""
    parts = urlsplit(target)
    if parts.password is None:
        return target
    host = parts.hostname or ""
    if parts.port:
        host += f":{parts.port}"
    user = f"{parts.username}@" if parts.username else ""
    return urlunsplit((parts.scheme, user + host, parts.path, parts.query, parts.fragment))
