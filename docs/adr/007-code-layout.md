# ADR-007: Code layout: a uv workspace, with the use cases in their own package

- Status: accepted

## Context

The usual layout for these projects is one package with `domain/`, `services/`, `adapters/` and
`api/` folders. Strata grew as a uv workspace of three packages instead: `strata-core` (models,
settings, Postgres ops, ledger, lake), `strata-fetch` (Common Crawl index sources, polite fetch,
robots.txt, WARC verification, raw storage, jobs) and `strata-cli`. The next milestones add four
interfaces over the same use cases (CLI, Python SDK, web console, MCP server), so the use cases
need a home that none of the interfaces owns. Before this change, the CLI built adapters itself
and printed engine models directly, and each new interface would have repeated that wiring.

## Decision

Keep the workspace and add a fourth package, `strata-services`, between the engine and the
interfaces:

```
strata-cli, console, MCP, SDK  ->  strata-services  ->  strata-fetch  ->  strata-core
 (interfaces: parse, print)        (use cases)          (adapters)        (models, settings,
                                                                           stores)
```

- `strata-services` holds the use cases (`profile` now; `diff`, `changes`, `pull` next), the
  models they return and `wiring.py`, the one place adapters are built from settings.
- A service receives its adapters through its constructor as small protocols (`CatalogSource`,
  `RobotsSource`), so unit tests pass fakes and need no network.
- Interfaces only parse input and present the returned models.

The workspace is kept because its package boundaries already enforce the dependency direction
(`strata-core` can't import the fetcher; nothing below the services can import an interface),
which folders inside one package would only state. The existing job runner (`strata_fetch.jobs`)
stays where it is; `pull` will wrap it.

## Consequences

- One more package to version; all four move together, from one lockfile.
- Modules over about 300 lines were split along the same lines: the CDX client, the URL-index
  queries, the site catalog, DuckDB request logging and request records each have their own
  module under `strata_fetch.cc`; the CLI's printing lives in `strata_cli.output`.
