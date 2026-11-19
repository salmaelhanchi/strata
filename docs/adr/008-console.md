# ADR-008: The console: FastAPI and a React single-page app

- Status: accepted

## Context

`strata console` is the web face of the same use cases the CLI and SDK call: profile a site,
diff two crawls, see what changes, pull pages, watch the job, and click any row through to its
lineage. Two choices: the server framework, and how the browser side is built.

| Option | For | Against |
| --- | --- | --- |
| FastAPI + React SPA | The engine and services are async Python; FastAPI calls them directly, validates with the same pydantic models and documents itself (OpenAPI). A typed client is one fetch wrapper. | Two toolchains (uv, npm) |
| Django + templates | Admin, ORM, auth out of the box | Strata has no ORM models to administer; the services are async and Django's async story is partial; templates make the interactive parts (live job progress, a lineage drawer) awkward |
| FastAPI + server-rendered HTML (htmx) | One toolchain | Less to show for a front-end-heavy portfolio piece; live progress and client state get ad hoc |

## Decision

- **Server:** FastAPI in a new `strata-console` package, an interface over `strata-services`
  like the CLI (ADR-007). REST under `/v1`; list endpoints page with an opaque cursor; errors are
  `application/problem+json` (RFC 9457); a running pull streams its progress over Server-Sent
  Events, since only the server talks; `/metrics` exposes Prometheus counters and histograms.
- **Browser:** React 18 + TypeScript + Vite + Tailwind, TanStack Query for server state (caching,
  refetch, loading and error states), Zustand for the little client state there is (the site
  being looked at, the open lineage drawer). Built to static files that FastAPI serves, so one
  process and one port serve everything.
- The build output is not committed. `make web` builds it; the Dockerfile builds it in a Node
  stage. Without a build, `strata console` serves the API and a page that says how to build.

## Consequences

- One process to run (`strata console`), one origin: no CORS, no second server.
- The API is usable without the browser app (curl, scripts), and its schema is generated.
- CI gains a web job: type-check and build.
