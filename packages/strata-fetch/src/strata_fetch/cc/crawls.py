"""The list of Common Crawl crawls (``collinfo.json``), cached for a day."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx

from strata_fetch.ratelimit import HostGate

CACHE_TTL_S = 24 * 3600


async def list_crawls(
    client: httpx.AsyncClient,
    gate: HostGate,
    *,
    index_base: str = "https://index.commoncrawl.org/",
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Crawls newest first, as published by the index server."""
    cache = cache_dir / "collinfo.json" if cache_dir else None
    if cache is not None and cache.exists() and time.time() - cache.stat().st_mtime < CACHE_TTL_S:
        data: list[dict[str, Any]] = json.loads(cache.read_text(encoding="utf-8"))
        return data
    async with gate.slot():
        resp = await client.get(urljoin(index_base, "collinfo.json"))
    resp.raise_for_status()
    data = resp.json()
    if cache is not None:
        cache.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        tmp.replace(cache)
    return data


async def resolve_crawl(
    crawl: str, client: httpx.AsyncClient, gate: HostGate, **kwargs: Any
) -> str:
    """``latest`` -> the newest crawl id; anything else is returned unchanged."""
    if crawl != "latest":
        return crawl
    crawls = await list_crawls(client, gate, **kwargs)
    if not crawls:
        raise RuntimeError("collinfo.json listed no crawls")
    return str(crawls[0]["id"])
