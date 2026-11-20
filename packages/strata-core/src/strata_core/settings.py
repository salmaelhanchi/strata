"""Settings from the environment (prefix STRATA_) or a .env file.

Defaults match deploy/docker-compose.yml.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from psycopg.conninfo import conninfo_to_dict
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from strata_core import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STRATA_", env_file=".env", extra="ignore")

    # Postgres holding the ops schema (jobs, budget ledger, policies, fetch attempts)
    ops_dsn: str = "postgresql://strata:strata@localhost:5432/strata_ops"
    # Postgres database holding the DuckLake catalog (libpq key=value form, as DuckLake expects)
    lake_catalog: str = (
        "dbname=ducklake_catalog host=localhost port=5432 user=strata password=strata"
    )
    # Where DuckLake writes Parquet: a local folder, or s3://bucket/prefix/ on the object store
    lake_data_path: str = "data/lake"
    # Use a plain DuckDB file instead of DuckLake (no extensions needed; single process only)
    lake_local_file: Path | None = None
    # DuckLake keeps flushes of up to this many rows inside the catalog instead of writing a
    # Parquet file for each, until CHECKPOINT (0 = off; always passed, since DuckLake's own
    # default inlines up to 10 rows). Bronze flushes are small, so this saves many tiny files.
    lake_inlining_rows: int = Field(default=0, ge=0)

    # Raw WARC parts are written here first (the write-ahead log of a run, ADR-005), then
    # sealed into the raw store: this same folder ("file") or a bucket ("s3").
    raw_root: Path = Path("data/raw")
    raw_store: Literal["file", "s3"] = "file"
    cache_dir: Path = Path("data/cache")
    # Crash-resume rehearsals only: die at a crash point, e.g. after_manifest:12 (crash.py)
    crash_at: str | None = None
    raw_rotate_bytes: int = Field(default=100 * 1024 * 1024, gt=0)

    # S3-compatible object store (RustFS locally, ADR-005; any S3 API in the cloud)
    s3_endpoint: str | None = None  # e.g. http://localhost:9000; None means AWS
    s3_region: str = "us-east-1"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_raw_bucket: str = "strata-raw"

    cc_data_base: str = "https://data.commoncrawl.org/"
    cc_index_base: str = "https://index.commoncrawl.org/"
    # ADR-003: an honest User-Agent naming the project. A fork should set its own repo URL.
    user_agent: str = f"strata/{__version__} (+https://github.com/salmaelhanchi/strata)"
    http_timeout_s: float = 30.0
    # Common Crawl's CDX gateway answers 504 after about 10 s, so waiting longer than the normal
    # HTTP timeout gains nothing (measured 2026-11-08; ADR-004).
    index_timeout_s: float = 30.0
    # DuckDB threads for URL-index queries. Each thread can hold a range request open, so ADR-003's
    # limit of two requests in flight to data.commoncrawl.org caps this at 2.
    index_threads: int = Field(default=1, ge=1, le=2)
    # Backoff after a retryable failure, in seconds (ADR-003); jitter adds up to +25%
    backoff_s: tuple[float, ...] = (2.0, 10.0, 30.0, 60.0)
    bronze_batch_size: int = Field(default=25, gt=0)
    # A running job writes a heartbeat this often; one silent for `stale_after_s` is interrupted.
    heartbeat_s: float = Field(default=5.0, gt=0)
    stale_after_s: float = Field(default=60.0, gt=0)
    # Logs on stderr (strata_core.logs): debug, info, warning or error; console or json lines
    log_level: Literal["debug", "info", "warning", "error"] = "warning"
    log_format: Literal["console", "json"] = "console"
    # The console keeps the pages it pulls here (a DuckDB file with a strata_pages table)
    console_db: Path = Path("data/console.duckdb")
    # The fetch pool (ADR-009): where `strata scheduler` listens, where `strata worker` connects,
    # how long a worker may hold a lease before its page goes to another, and how many leases a
    # worker holds at once (the data host's policy still caps the total).
    pool_listen: str = "127.0.0.1:50061"
    pool_scheduler: str = "127.0.0.1:50061"
    lease_ttl_s: float = Field(default=60.0, gt=0)
    worker_slots: int = Field(default=1, ge=1, le=16)
    pool_metrics_port: int = Field(default=9461, ge=0, le=65535)  # 0 turns /metrics off

    @property
    def version(self) -> str:
        return __version__

    def storage(self) -> dict[str, str]:
        """Where a job run with these settings keeps its data, without credentials: the staging
        folder of its write-ahead log, the raw store and the lake. Stored on the job and compared
        on resume, so a job never continues against other storage than it started with."""
        raw = (
            f"s3://{self.s3_raw_bucket} at {self.s3_endpoint or 'aws ' + self.s3_region}"
            if self.raw_store == "s3"
            else f"file {self.raw_root.resolve()}"
        )
        if self.lake_local_file is not None:
            lake = f"duckdb {self.lake_local_file.resolve()}"
        else:
            info = conninfo_to_dict(self.lake_catalog)
            data = self.lake_data_path
            if not data.startswith("s3://"):
                data = str(Path(data).resolve())
            lake = (
                f"ducklake {info.get('dbname', '')}@{info.get('host', '')}:{info.get('port', '')}"
                f", data {data}"
            )
        return {"staging": str(self.raw_root.resolve()), "raw": raw, "lake": lake}
