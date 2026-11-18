"""Everything a run needs in place, set up idempotently in one call.

`strata migrate` and every command that writes call ``prepare``
before touching storage, so no command depends on another having been run first.
"""

from __future__ import annotations

import asyncio

from strata_core import db
from strata_core.lake import Lake
from strata_core.settings import Settings
from strata_fetch.blobstore import ensure_buckets


async def prepare(settings: Settings) -> list[str]:
    """Apply ops migrations, create the lake's catalog database and the S3 buckets if missing,
    and create the lake's schemas and tables. Returns what it changed, one line each."""
    done = [f"ops schema: applied {v}" for v in await db.migrate(settings.ops_dsn)]
    if settings.lake_local_file is None and await db.ensure_database(settings.lake_catalog):
        done.append("created the lake catalog database")
    done += [f"created bucket {b}" for b in await asyncio.to_thread(ensure_buckets, settings)]

    def bootstrap() -> None:
        with Lake.open(settings) as lake:
            lake.bootstrap()

    await asyncio.to_thread(bootstrap)
    return done
