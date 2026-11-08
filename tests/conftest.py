"""Shared fixtures.

Postgres: tests use STRATA_TEST_PG_DSN when set (CI service container, or `make up` locally).
Otherwise they start a throwaway cluster from local PostgreSQL binaries, and skip if there are none.
Each test gets its own freshly migrated database.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from strata_core.db import migrate

if sys.platform == "win32":

    def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> dict[str, Any]:
        """psycopg's async driver needs a selector loop; Windows defaults to the proactor loop."""
        return {"selector": asyncio.SelectorEventLoop}


def _pg_bin_dir() -> Path | None:
    found = shutil.which("initdb")
    if found:
        return Path(found).parent
    candidates = sorted(Path("/usr/lib/postgresql").glob("*/bin"), reverse=True)
    return next((c for c in candidates if (c / "initdb").exists()), None)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _run_as_kwargs() -> dict[str, Any]:
    """initdb refuses to run as root; drop to the 'postgres' user when we are root."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        return {"user": "postgres", "group": "postgres"}
    return {}


@pytest.fixture(scope="session")
def pg_server() -> Iterator[str]:
    dsn = os.environ.get("STRATA_TEST_PG_DSN")
    if dsn:
        yield dsn
        return
    bin_dir = _pg_bin_dir()
    if bin_dir is None:
        pytest.skip("no Postgres: set STRATA_TEST_PG_DSN or install PostgreSQL binaries")
    tmp = Path(tempfile.mkdtemp(prefix="strata-pg-"))
    run_as = _run_as_kwargs()
    if run_as:
        shutil.chown(tmp, user="postgres", group="postgres")
    data = tmp / "data"
    port = _free_port()
    subprocess.run(
        [bin_dir / "initdb", "-D", data, "-U", "postgres", "--auth=trust", "-E", "UTF8"],
        check=True,
        capture_output=True,
        **run_as,
    )
    subprocess.run(
        [
            bin_dir / "pg_ctl",
            "-D",
            data,
            "-o",
            f"-p {port} -k {tmp} -c listen_addresses=127.0.0.1",
            "-l",
            tmp / "server.log",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
        **run_as,
    )
    try:
        yield f"postgresql://postgres@127.0.0.1:{port}/postgres"
    finally:
        subprocess.run(
            [bin_dir / "pg_ctl", "-D", data, "-m", "immediate", "stop"],
            check=False,
            capture_output=True,
            **run_as,
        )
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def pg_dsn(pg_server: str) -> Iterator[str]:
    """A fresh, empty database for one test."""
    name = f"strata_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_server, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    params = conninfo_to_dict(pg_server)
    params["dbname"] = name
    try:
        yield make_conninfo(**params)
    finally:
        with psycopg.connect(pg_server, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
async def ops_pool(pg_dsn: str) -> AsyncIterator[AsyncConnectionPool]:
    """A migrated ops database behind an open pool."""
    await migrate(pg_dsn)
    async with AsyncConnectionPool(pg_dsn, min_size=1, max_size=20, open=False) as pool:
        yield pool
