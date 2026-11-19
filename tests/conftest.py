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

import duckdb
import psycopg
import pytest
from hypothesis import HealthCheck, settings
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from strata_core.db import migrate
from strata_core.settings import Settings

# Property tests check properties, not speed: on a busy machine (the whole suite, a CI runner) one
# slow draw tripped the "input generation is slow" health check. Timing checks are off.
settings.register_profile("strata", deadline=None, suppress_health_check=[HealthCheck.too_slow])
settings.load_profile("strata")

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


@pytest.fixture(autouse=True)
def _isolated_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests see only the settings they set: no STRATA_* variables from the shell and no .env
    file from the working folder (a developer's .env may point at real services)."""
    for name in list(os.environ):
        if name.startswith("STRATA_") and name != "STRATA_TEST_PG_DSN":
            monkeypatch.delenv(name)
    monkeypatch.setitem(Settings.model_config, "env_file", None)


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


@pytest.fixture(scope="session")
def httpfs() -> None:
    """DuckDB's httpfs extension, for tests that read Parquet over HTTP from a local server.

    DuckDB installs it on first use where it can reach its extension repository (CI does);
    elsewhere these tests are skipped.
    """
    con = duckdb.connect()
    try:
        con.execute("LOAD httpfs")
    except duckdb.Error:
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        except duckdb.Error:
            pytest.skip("DuckDB's httpfs extension can't be installed here")
    finally:
        con.close()


@pytest.fixture(scope="session")
def s3_endpoint() -> Iterator[str]:
    """An in-process S3 server (moto) for the object-store code: the same boto3 calls RustFS
    and AWS answer, with SHA-256 checksums verified on upload."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=_free_port(), verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def s3_settings(s3_endpoint: str, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Settings for a fresh bucket on the moto server, as STRATA_* environment variables."""
    env = {
        "STRATA_RAW_STORE": "s3",
        "STRATA_S3_ENDPOINT": s3_endpoint,
        "STRATA_S3_ACCESS_KEY": "test",
        "STRATA_S3_SECRET_KEY": "test-secret",
        "STRATA_S3_RAW_BUCKET": f"raw-{uuid.uuid4().hex[:10]}",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return env
