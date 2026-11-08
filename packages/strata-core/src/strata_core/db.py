"""Postgres access: connection pools and a small, transparent migration runner (no ORM).

Migrations are numbered SQL files shipped inside this package. Each one runs in its own
transaction and is recorded in public.schema_migrations, so running ``migrate`` twice is safe.
"""

from __future__ import annotations

from importlib import resources

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

_MIGRATIONS_PACKAGE = "strata_core.migrations"


def migration_files() -> list[tuple[str, str]]:
    """(version, sql) pairs in order, e.g. ("001_init", "CREATE SCHEMA ...")."""
    root = resources.files(_MIGRATIONS_PACKAGE)
    files = sorted(
        (entry for entry in root.iterdir() if entry.name.endswith(".sql")),
        key=lambda entry: entry.name,
    )
    return [(entry.name.removesuffix(".sql"), entry.read_text(encoding="utf-8")) for entry in files]


async def migrate(dsn: str) -> list[str]:
    """Apply pending migrations; return the versions applied by this call."""
    applied: list[str] = []
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS public.schema_migrations ("
            " version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        # Serialise concurrent migrators (two CLI invocations, CI jobs) on one advisory lock.
        await conn.execute("SELECT pg_advisory_lock(hashtext('strata_migrations'))")
        try:
            cur = await conn.execute("SELECT version FROM public.schema_migrations")
            done = {row[0] for row in await cur.fetchall()}
            for version, sql in migration_files():
                if version in done:
                    continue
                async with conn.transaction():
                    await conn.execute(sql.encode("utf-8"))
                    await conn.execute(
                        "INSERT INTO public.schema_migrations (version) VALUES (%s)", (version,)
                    )
                applied.append(version)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext('strata_migrations'))")
    return applied


def make_pool(dsn: str, *, max_size: int = 10) -> AsyncConnectionPool:
    """A pool that is opened by the caller (``async with make_pool(...) as pool``)."""
    return AsyncConnectionPool(dsn, min_size=1, max_size=max_size, open=False)
