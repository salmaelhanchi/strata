"""Settings from the environment (prefix STRATA_) or a .env file.

Defaults match deploy/docker-compose.yml.
"""

from __future__ import annotations

from pathlib import Path

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
    lake_data_path: Path = Path("data/lake")
    # Use a plain DuckDB file instead of DuckLake (no extensions needed; single process only)
    lake_local_file: Path | None = None

    raw_root: Path = Path("data/raw")
    cache_dir: Path = Path("data/cache")
    raw_rotate_bytes: int = Field(default=100 * 1024 * 1024, gt=0)

    cc_data_base: str = "https://data.commoncrawl.org/"
    cc_index_base: str = "https://index.commoncrawl.org/"
    user_agent: str = f"strata/{__version__} (+https://github.com/YOUR_GITHUB_USER/strata)"
    http_timeout_s: float = 30.0
    # Common Crawl's CDX gateway answers 504 after about 10 s, so waiting longer than the normal
    # HTTP timeout gains nothing (measured 2026-11-08; ADR-004).
    index_timeout_s: float = 30.0
    # Backoff after a retryable failure, in seconds (ADR-003); jitter adds up to +25%
    backoff_s: tuple[float, ...] = (2.0, 10.0, 30.0, 60.0)
    bronze_batch_size: int = Field(default=25, gt=0)

    @property
    def version(self) -> str:
        return __version__
