"""What every command shares: the Typer apps, options, settings, HTTP client and host gates."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from strata_core import logs
from strata_core.models import (
    IndexChoice,
)
from strata_core.settings import Settings
from strata_services.runtime import data_gate, http_client, index_gate, run_sync

app = typer.Typer(
    no_args_is_help=True,
    help="Strata: a Common Crawl toolkit. Before you scrape a site, ask Strata.",
)
run_app = typer.Typer(no_args_is_help=True, help="Run an acquisition job.")
app.add_typer(run_app, name="run")
gate_app = typer.Typer(no_args_is_help=True, help="Gate 1: run a targets file, then prove it.")
app.add_typer(gate_app, name="gate1")


DomainOpt = Annotated[
    str, typer.Option(help="Site: a domain or host, with its subdomains, e.g. peps.python.org")
]
CrawlOpt = Annotated[str, typer.Option(help="Crawl id such as CC-MAIN-2026-17, or 'latest'")]
LimitOpt = Annotated[int, typer.Option(min=1, max=5000, help="Maximum distinct pages")]
BudgetOpt = Annotated[
    int,
    typer.Option(min=1, help="Maximum requests: retries and DuckDB's URL-index requests included"),
]
# A fresh URL-index lookup for python.org cost 48 DuckDB requests (2026-11-08), and those are
# charged (ADR-004, amendment): 20 pages need about 70, so 60 would stop a demo halfway.
DEFAULT_BUDGET = 150
JsonOpt = Annotated[bool, typer.Option("--json", help="Print JSON instead of text")]
PatternOpt = Annotated[
    str | None,
    typer.Option(help="RE2 regex the whole URL must match, e.g. '^https://x\\.org/a/$'"),
]
OracleDirOpt = Annotated[
    Path, typer.Option("--oracle", help="Folder with peps.json and rfc-index.txt")
]
QueryOpt = Annotated[
    bool,
    typer.Option(
        "--include-query-strings",
        help="Keep URLs with a query string (by default /?ref=... style variants are dropped)",
    ),
]
ThreadsOpt = Annotated[
    int | None,
    typer.Option(
        "--threads",
        min=1,
        max=2,
        help="DuckDB threads for URL-index queries (default STRATA_INDEX_THREADS or 1; ADR-003"
        " allows two requests in flight)",
    ),
]
IndexOpt = Annotated[
    IndexChoice,
    typer.Option(
        "--index",
        help="auto: CDX server, then the Parquet URL index if it is unavailable; "
        "cdx or columnar: only that one (ADR-004)",
    ),
]


def load_settings(threads: int | None = None) -> Settings:
    """Settings for this run; also starts its logs, each line tagged with a new ``run_id``."""
    settings = Settings()
    logs.configure(settings.log_level, settings.log_format)
    logs.bind(run_id=logs.new_run_id())
    if threads is not None:
        settings = settings.model_copy(update={"index_threads": threads})
    return settings


__all__ = ["data_gate", "http_client", "index_gate", "run_sync"]
