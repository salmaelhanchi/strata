"""Crash-resume and a local Gate 1 rehearsal.

Each job runs as a real ``strata gate1 run`` process against the fake Common Crawl, because a
crash has to be a real one: ``STRATA_CRASH_AT`` makes the process exit on the spot (like
``kill -9``) at a chosen point of the write path, and one test kills the process from outside.
Running the same command again must finish the same job with no lost rows, no duplicates, no
lost budget and exactly the pages an uninterrupted run would have chosen, then prove it.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pytest_httpserver import HTTPServer

from strata_fetch.gate import Gate1Run
from tests.fakecc import CRAWL, FakeCommonCrawl, build_warc
from tests.pages import pep_page, peps_json, rfc_index_entry, rfc_page

pytestmark = [pytest.mark.postgres, pytest.mark.usefixtures("httpfs")]

PAGES_WANTED = 24
DOCS = [f"https://example.org/doc/{i:03d}" for i in range(PAGES_WANTED + 8)]
DISALLOWED = {"https://example.org/doc/003", "https://example.org/doc/017"}


def run_cli(
    args: list[str], env: dict[str, str], cwd: Path, *, timeout: float = 300
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "strata_cli.main", *args],
        env=env,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def finish(targets: Path, env: dict[str, str], cwd: Path) -> Gate1Run:
    """`strata gate1 run --json` that must end with the gate passed."""
    done = run_cli(["gate1", "run", str(targets), "--json"], env, cwd, timeout=600)
    assert done.returncode == 0, done.stdout + done.stderr
    return Gate1Run.model_validate_json(done.stdout)


def base_env(pg_dsn: str, fake: FakeCommonCrawl, tmp: Path) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("STRATA_")}
    env.update(
        {
            "STRATA_OPS_DSN": pg_dsn,
            "STRATA_LAKE_LOCAL_FILE": str(tmp / "lake.duckdb"),
            "STRATA_RAW_ROOT": str(tmp / "raw"),
            "STRATA_CACHE_DIR": str(tmp / "cache"),
            "STRATA_CC_DATA_BASE": fake.base_url,
            "STRATA_CC_INDEX_BASE": fake.base_url,
            "STRATA_BACKOFF_S": "[0.01, 0.01]",
            "STRATA_BRONZE_BATCH_SIZE": "4",
            "STRATA_HEARTBEAT_S": "0.2",
            "STRATA_USER_AGENT": "strata-tests/0.2",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def write_oracle_stub(folder: Path) -> Path:
    """Empty publisher indexes with their provenance files, so `gate1 run` finds them and never
    downloads the real ones (tests don't touch the internet)."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "peps.json").write_text("{}", "utf-8")
    (folder / "rfc-index.txt").write_text("", "utf-8")
    for name in ("peps.json", "rfc-index.txt"):
        (folder / f"{name}.provenance.json").write_text('{"source": "test stub"}', "utf-8")
    return folder


def write_targets(path: Path, pages: int, *, budget: int = 200) -> Path:
    path.write_text(
        f'crawl = "{CRAWL}"\nbudget = {budget}\nindex = "columnar"\n\n'
        f'[[target]]\nsite = "example.org"\npages = {pages}\n',
        "utf-8",
    )
    return path


def set_policy(pg_dsn: str, max_rps: float, burst: int, in_flight: int) -> None:
    with psycopg.connect(pg_dsn) as conn:
        conn.execute(
            "INSERT INTO ops.policies (host, max_rps, burst, max_in_flight)"
            " VALUES ('localhost', %s, %s, %s)"
            " ON CONFLICT (host) DO UPDATE SET max_rps = EXCLUDED.max_rps,"
            " burst = EXCLUDED.burst, max_in_flight = EXCLUDED.max_in_flight",
            (max_rps, burst, in_flight),
        )


def jobs_in(pg_dsn: str) -> list[tuple[str, str]]:
    with psycopg.connect(pg_dsn) as conn:
        rows = conn.execute("SELECT job_id, state FROM ops.jobs ORDER BY created_at").fetchall()
    return [(str(a), str(b)) for a, b in rows]


def ledger_and_charged(pg_dsn: str, job_id: str) -> tuple[int, int]:
    """What the budget ledger says the job spent, and how many of its attempts were charged."""
    with psycopg.connect(pg_dsn) as conn:
        spent = conn.execute(
            "SELECT spent FROM ops.budget_ledger WHERE job_id = %s AND host = '*'", (job_id,)
        ).fetchone()
        charged = conn.execute(
            "SELECT count(*) FROM ops.fetch_attempts WHERE job_id = %s AND charged", (job_id,)
        ).fetchone()
    assert spent is not None and charged is not None
    return int(spent[0]), int(charged[0])


def expected_selection(urls: list[str], disallowed: set[str], n: int) -> list[str]:
    """What an uninterrupted run picks: the first n allowed URLs in md5 order."""
    allowed = [u for u in urls if u not in disallowed]
    return sorted(sorted(allowed, key=lambda u: hashlib.md5(u.encode()).hexdigest())[:n])


def lake_rows(lake_file: Path, job_id: str) -> list[tuple[str, str]]:
    con = duckdb.connect(str(lake_file), read_only=True)
    try:
        rows = con.execute(
            "SELECT capture_id, url FROM bronze.captures WHERE job_id = ? ORDER BY url", [job_id]
        ).fetchall()
    finally:
        con.close()
    return [(str(a), str(b)) for a, b in rows]


@pytest.fixture
def fake(httpserver: HTTPServer) -> FakeCommonCrawl:
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc([(u, f"<html><p>{u}</p></html>".encode() * 20) for u in DOCS])
    cc.add_warc(warc)
    cc.add_url_index(warc.refs)
    robots = "".join(f"Disallow: {u.removeprefix('https://example.org')}\n" for u in DISALLOWED)
    cc.add_robots("example.org", f"User-agent: *\n{robots}".encode())
    cc.install()
    return cc


@pytest.fixture
def cli(pg_dsn: str, fake: FakeCommonCrawl, tmp_path: Path) -> dict[str, str]:
    env = base_env(pg_dsn, fake, tmp_path)
    migrated = run_cli(["migrate"], env, tmp_path)  # so the test can set a policy first
    assert migrated.returncode == 0, migrated.stderr
    write_oracle_stub(tmp_path / "data" / "oracle")
    return env


@pytest.fixture
def targets(tmp_path: Path) -> Path:
    return write_targets(tmp_path / "targets.toml", PAGES_WANTED)


def assert_clean(pg_dsn: str, tmp_path: Path, result: Gate1Run) -> None:
    rows = lake_rows(tmp_path / "lake.duckdb", result.job_id)
    ids, urls = [r[0] for r in rows], [r[1] for r in rows]
    assert len(set(ids)) == len(ids) == PAGES_WANTED  # no lost rows, no duplicates
    assert len(set(urls)) == len(urls)
    assert sorted(urls) == expected_selection(DOCS, DISALLOWED, PAGES_WANTED)
    spent, charged = ledger_and_charged(pg_dsn, result.job_id)
    assert spent == charged  # every unit of budget matches a request that went out
    assert result.proof is not None and result.proof.gate.passed


@pytest.mark.parametrize(
    "point",
    ["after_append:7", "after_manifest:7", "after_seal:2", "after_insert:3", "after_insert:1"],
)
def test_a_crash_at_any_point_of_the_write_path_is_finished_by_the_same_command(
    point: str, cli: dict[str, str], pg_dsn: str, tmp_path: Path, targets: Path
) -> None:
    set_policy(pg_dsn, 500, 10, 2)
    gate1 = ["gate1", "run", str(targets)]
    crashed = run_cli(gate1, {**cli, "STRATA_CRASH_AT": point}, tmp_path)
    assert crashed.returncode == 137, crashed.stdout + crashed.stderr
    ((job_id, state),) = jobs_in(pg_dsn)
    assert state == "running"  # the process died without a word

    # Its heartbeat is recent, so the same command refuses: the process might still be alive.
    refused = run_cli(gate1, cli, tmp_path)
    assert refused.returncode == 1 and "still running" in refused.stderr

    time.sleep(0.5)  # the heartbeat (every 0.2 s) is now older than STRATA_STALE_AFTER_S
    result = finish(targets, {**cli, "STRATA_STALE_AFTER_S": "0.4"}, tmp_path)
    assert result.job_id == job_id and len(jobs_in(pg_dsn)) == 1  # continued, not restarted
    assert result.run is not None and result.run.resumed
    if point.startswith(("after_manifest", "after_seal")):
        # Rows the dead run had logged (and maybe sealed) but never inserted: replayed from the
        # write-ahead manifests, not fetched again.
        assert result.run.recovered_rows >= 1
    assert_clean(pg_dsn, tmp_path, result)


def test_a_process_killed_part_way_is_finished_by_the_same_command(
    cli: dict[str, str], pg_dsn: str, tmp_path: Path, targets: Path
) -> None:
    set_policy(pg_dsn, 12, 1, 1)  # slow enough to be caught in the middle
    proc = subprocess.Popen(
        [sys.executable, "-m", "strata_cli.main", "gate1", "run", str(targets)],
        env=cli,
        cwd=tmp_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 60
        fetched = 0
        while time.monotonic() < deadline and proc.poll() is None:
            with psycopg.connect(pg_dsn) as conn:
                row = conn.execute(
                    "SELECT count(*) FROM ops.fetch_attempts"
                    " WHERE outcome = 'ok' AND url LIKE 'https://example.org/doc/%'"
                ).fetchone()
            fetched = int(row[0]) if row else 0
            if fetched >= 0.4 * PAGES_WANTED:
                break
            time.sleep(0.02)
        proc.kill()  # SIGKILL: no handler runs, nothing is flushed
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert 0.4 * PAGES_WANTED <= fetched < PAGES_WANTED

    time.sleep(0.5)
    stale = {**cli, "STRATA_STALE_AFTER_S": "0.4"}
    ((job_id, _),) = jobs_in(pg_dsn)
    listed = run_cli(["jobs"], stale, tmp_path)
    assert listed.stdout.startswith(f"{job_id}  interrupted "), listed.stdout + listed.stderr
    result = finish(targets, stale, tmp_path)
    assert result.job_id == job_id and result.run is not None and result.run.resumed
    assert_clean(pg_dsn, tmp_path, result)


def test_a_finished_job_is_only_proven_again_and_a_smaller_one_reuses_its_pages(
    cli: dict[str, str], pg_dsn: str, fake: FakeCommonCrawl, tmp_path: Path, targets: Path
) -> None:
    set_policy(pg_dsn, 500, 10, 2)
    first = finish(targets, cli, tmp_path)
    requests = len(fake.requests)

    again = finish(targets, cli, tmp_path)
    assert (again.job_id, again.run) == (first.job_id, None)  # nothing ran
    assert len(fake.requests) == requests

    smaller = finish(write_targets(tmp_path / "smaller.toml", 20), cli, tmp_path)
    assert smaller.job_id != first.job_id and smaller.run is not None
    assert (smaller.run.already_stored, smaller.run.budget_spent) == (20, 0)
    assert len(fake.requests) == requests  # catalog, robots and pages all came from storage
    assert smaller.proof is not None
    checks = {c.name: c.detail for c in smaller.proof.gate.checks}
    assert checks["every selected page is in the lake"] == (
        "20/20 (0 stored by this job, 20 by earlier jobs)"
    )


def test_a_job_continues_only_on_the_storage_it_started_with(
    cli: dict[str, str], pg_dsn: str, tmp_path: Path, targets: Path
) -> None:
    set_policy(pg_dsn, 500, 10, 2)
    crashed = run_cli(
        ["gate1", "run", str(targets)], {**cli, "STRATA_CRASH_AT": "after_manifest:3"}, tmp_path
    )
    assert crashed.returncode == 137
    ((job_id, _),) = jobs_in(pg_dsn)
    elsewhere = {**cli, "STRATA_RAW_ROOT": str(tmp_path / "other-raw")}

    refused = run_cli(["resume", job_id, "--force"], elsewhere, tmp_path)
    assert refused.returncode == 1 and "keeps its data in" in refused.stderr
    assert str(tmp_path / "raw") in refused.stderr  # it says where the job's data is

    # The same targets file on other storage is a different job, not this one continued.
    other = finish(targets, {**elsewhere, "STRATA_STALE_AFTER_S": "0.4"}, tmp_path)
    assert other.job_id != job_id and len(jobs_in(pg_dsn)) == 2


# ------------------------------------------------------------------------------ Gate 1
PEP_NUMBERS = list(range(1, 211))  # 210 PEP pages for 175 wanted
RFC_NUMBERS = list(range(1000, 1900))  # 900 RFC info pages for 825 wanted
PEP_PATTERN = r"^https://peps\.python\.org/pep-\d{4}/$"
RFC_PATTERN = r"^https://www\.rfc-editor\.org/info/rfc\d+/$"
RFC_DISALLOWED = {1003, 1500}
STATUS_CHANGED = {7, 8}  # the index says Draft, the pages say Final: two real disagreements


def gate1_pages() -> list[tuple[str, bytes]]:
    pages = [
        (f"https://peps.python.org/pep-{n:04d}/", pep_page(n, f"Proposal Number {n}").encode())
        for n in PEP_NUMBERS
    ]
    pages += [
        (
            f"https://www.rfc-editor.org/info/rfc{n}/",
            rfc_page(n, f"Protocol Number {n}", authors=f"A. Writer{n}, B. Editor").encode(),
        )
        for n in RFC_NUMBERS
    ]
    # Pages on the same sites that the patterns must leave out.
    pages += [(f"https://peps.python.org/topic/t{n}/", b"<h1>topic</h1>") for n in range(20)]
    pages += [(f"https://www.rfc-editor.org/rfc/rfc{n}.html", b"<pre>rfc</pre>") for n in range(20)]
    return pages


def write_oracles(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    peps = {
        n: {"title": f"Proposal Number {n}", "status": "Draft" if n in STATUS_CHANGED else "Final"}
        for n in PEP_NUMBERS
    }
    (folder / "peps.json").write_bytes(peps_json(peps))
    entries = [
        rfc_index_entry(
            n, f"Protocol Number {n}", f"A. Writer{n}, B. Editor", "March 2007", "EXPERIMENTAL"
        )
        for n in RFC_NUMBERS
    ]
    (folder / "rfc-index.txt").write_text("RFC INDEX\n\n" + "\n\n".join(entries) + "\n", "utf-8")
    for name in ("peps.json", "rfc-index.txt"):  # present, so `gate1 run` doesn't download
        (folder / f"{name}.provenance.json").write_text('{"source": "test"}', "utf-8")


def ducklake_available() -> bool:
    con = duckdb.connect()
    try:
        for ext in ("ducklake", "postgres", "httpfs"):
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
    except duckdb.Error:
        return False
    finally:
        con.close()
    return True


@pytest.mark.ducklake
def test_gate1_rehearsal_two_sites_1000_pages_on_s3_killed_then_the_same_command(
    httpserver: HTTPServer, pg_dsn: str, tmp_path: Path, s3_endpoint: str
) -> None:
    """Gate 1 as you run it: one targets file, one command, killed at about 40 % and run again.
    Raw parts and the lake on an S3 store (moto here, RustFS on a laptop), the lake in a catalog
    database that setup has to create, robots.txt verdicts, normalizers against the indexes."""
    if not ducklake_available():
        pytest.skip("DuckDB ducklake/postgres/httpfs extensions can't be installed here")
    cc = FakeCommonCrawl(httpserver)
    warc = build_warc(gate1_pages())
    cc.add_warc(warc)
    cc.add_url_index(warc.refs)
    cc.add_robots("peps.python.org")
    disallow = "".join(f"Disallow: /info/rfc{n}/\n" for n in sorted(RFC_DISALLOWED))
    cc.add_robots("www.rfc-editor.org", f"User-agent: *\n{disallow}".encode())
    cc.install()

    tag = f"{os.getpid()}-{int(time.time() * 1000) % 100000}"
    env = base_env(pg_dsn, cc, tmp_path)
    del env["STRATA_LAKE_LOCAL_FILE"]
    env.update(
        {
            # A catalog database that doesn't exist yet: `gate1 run` creates it.
            "STRATA_LAKE_CATALOG": make_conninfo(
                **{**conninfo_to_dict(pg_dsn), "dbname": f"ducklake_{tag.replace('-', '_')}"}
            ),
            "STRATA_LAKE_DATA_PATH": f"s3://gate1-{tag}-lake/",  # bucket created by `gate1 run`
            "STRATA_LAKE_INLINING_ROWS": "50",
            "STRATA_RAW_STORE": "s3",
            "STRATA_S3_ENDPOINT": s3_endpoint,
            "STRATA_S3_ACCESS_KEY": "test",
            "STRATA_S3_SECRET_KEY": "test-secret",
            "STRATA_S3_RAW_BUCKET": f"gate1-{tag}",
            "STRATA_BRONZE_BATCH_SIZE": "25",
        }
    )
    targets = tmp_path / "gate1.toml"
    targets.write_text(
        f'crawl = "{CRAWL}"\nbudget = 1200\nindex = "columnar"\n\n'
        f"[[target]]\nsite = \"peps.python.org\"\npattern = '{PEP_PATTERN}'\npages = 175\n\n"
        f"[[target]]\nsite = \"www.rfc-editor.org\"\npattern = '{RFC_PATTERN}'\npages = 825\n",
        "utf-8",
    )
    oracle = tmp_path / "data" / "oracle"
    write_oracles(oracle)
    migrated = run_cli(["migrate"], env, tmp_path)
    assert migrated.returncode == 0, migrated.stderr
    assert "created the lake catalog database" in migrated.stdout
    set_policy(pg_dsn, 150, 5, 2)

    proc = subprocess.Popen(
        [sys.executable, "-m", "strata_cli.main", "gate1", "run", str(targets)],
        env=env,
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    fetched = 0
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and proc.poll() is None:
            with psycopg.connect(pg_dsn) as conn:
                row = conn.execute(
                    "SELECT count(*) FROM ops.fetch_attempts"
                    " WHERE outcome = 'ok' AND url NOT LIKE '%robots.txt'"
                    " AND client = 'strata' AND url NOT LIKE '%.parquet%'"
                ).fetchone()
            fetched = int(row[0]) if row else 0
            if fetched >= 400:
                break
            time.sleep(0.05)
        proc.kill()
        output = proc.communicate(timeout=30)[0]
    finally:
        if proc.poll() is None:
            proc.kill()
    assert 400 <= fetched < 1000, output

    time.sleep(0.5)
    result = finish(targets, {**env, "STRATA_STALE_AFTER_S": "0.4"}, tmp_path)
    assert result.run is not None and result.run.resumed
    assert result.run.state.value == "succeeded", result.run.stop_reason
    assert [(t.site, t.rows) for t in result.run.targets] == [
        ("peps.python.org", 175),
        ("www.rfc-editor.org", 825),
    ]
    assert result.run.budget_spent <= 1200
    assert result.proof is not None and result.proof.documents == 1000
    gate = result.proof.gate
    assert gate.passed and gate.rows == 1000, gate.model_dump_json(indent=2)
    details = {c.name: c.detail for c in gate.checks}
    assert details["pages per target"] == "peps.python.org 175/175, www.rfc-editor.org 825/825"
    assert "rows without a stored robots.txt capture: 0" in details["robots verdict on every row"]
    assert "2 robots.txt capture(s)" in details["robots.txt bytes intact"]
    assert details["normalizers agree with the official indexes"].startswith("99.")
