"""Long-running servers: ``strata console`` (REST API and web app) and ``strata mcp``."""

from __future__ import annotations

from typing import Annotated

import typer
import uvicorn

from strata_cli.common import app, load_settings, run_sync
from strata_console.app import create_app
from strata_mcp.server import build_server


@app.command()
def console(
    host: Annotated[str, typer.Option(help="Interface to listen on")] = "127.0.0.1",
    port: Annotated[int, typer.Option(min=1, max=65535)] = 8040,
) -> None:
    """Serve the console: profile, diff, pull and lineage in the browser, the API under /v1,
    its schema at /docs and Prometheus metrics at /metrics."""
    settings = load_settings()
    typer.echo(f"Strata console on http://{host}:{port}  (API docs: /docs, metrics: /metrics)")
    server = uvicorn.Server(
        uvicorn.Config(create_app(settings), host=host, port=port, log_level="warning")
    )
    run_sync(server.serve())  # the selector loop on Windows, which psycopg's async driver needs


@app.command()
def mcp() -> None:
    """Serve Strata to an AI agent over MCP (stdio): profile_site, diff_site, change_rates,
    pull_pages and trace_row. Add it to an MCP client as the command `strata mcp`."""
    settings = load_settings()  # logs go to stderr; stdout carries the protocol only
    run_sync(build_server(settings).run_stdio_async())
