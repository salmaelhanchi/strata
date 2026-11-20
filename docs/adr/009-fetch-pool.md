# ADR-009: A fetch pool: a gRPC scheduler leases pages to workers

- Status: accepted

## Context

One `strata run` process fetches a job's pages through one HTTP client. To spread the page
requests over several processes or machines, two of Strata's rules have to survive: the budget is
spent before a request is sent, and each host's politeness (a token bucket, a cap on requests in
flight, a pause for the whole host on 429 or 503) applies to all requests together. If every
worker kept its own limiter, three workers would send three times the policy's rate. And a worker
can die at any moment: holding a page, mid-request, or after storing bytes but before saying so.

Options for the channel between the scheduler and its workers:

| Option | For | Against |
| --- | --- | --- |
| Workers poll Postgres directly | No new protocol | Every worker needs database credentials; politeness would have to live in SQL (a shared token bucket row every request contends on); a dead worker is noticed only by a timeout |
| REST (FastAPI) | Already used by the console | Leases and results are a two-way conversation; polling for work adds latency and load, and a closed connection says nothing about the worker |
| gRPC, one bidirectional stream per worker | A typed contract (`pool.proto`) between two services that both stream; the stream's end tells the scheduler at once that the worker is gone | Generated code, one more dependency |
| A message broker (Redis, RabbitMQ) | Mature redelivery | Another service to run; the politeness state still needs one owner, which a broker is not |

## Decision

- A new package, `strata-pool`, holds the scheduler and the worker. The job itself does not move:
  `strata scheduler` runs the targets file's job as `strata run` would (discovery, robots.txt
  verdicts, selection, the bronze rows) and only its page requests go to the pool, through a
  `PagePool` the job runner accepts (`strata_fetch.pooled`).
- **Everything that must be decided once stays in the scheduler.** A lease is granted only after
  a free in-flight slot for the data host, the next pending page (claimed in Postgres), a token
  from the host's bucket and one request reserved in the ledger, in that order. A 429 or 503
  reported by any worker pauses the host for all of them. Workers hold no budget, no limiter and no
  database connection.
- **The queue is a table, `ops.work_items`**, with a state machine checked by SQL (`pending`,
  `leased`, `fetched`, `stored`, `empty`, `failed`). Each lease gets a new id, and every change to a
  leased row names it (`WHERE lease_id = ... AND state = 'leased'`): a fencing token, so a result
  from a worker presumed dead matches no row and changes nothing.
- **A lost lease goes back at once.** When a worker's stream closes, its unanswered leases return
  to `pending`; a lease nobody answers within `STRATA_LEASE_TTL_S` (a hung worker, a network
  partition) is taken back the same way. Either is logged as a failed attempt and its budget stays
  spent, since the request may have gone out. After the attempts the backoff schedule allows, the
  page ends `failed`.
- **Durable before visible across processes.** A worker appends a verified record to its own part
  (with its manifest line) and seals the part into the raw store before it reports. The scheduler
  then keeps the row in `work_items` (`fetched`) until it is in the lake (`stored`); a scheduler
  that dies in between writes those rows when the job resumes. Rows are keyed by the capture's
  upstream location, so a page fetched twice is still one row.
- **One job per scheduler, one data host.** Common Crawl's WARC files all live on one host, and a
  scheduler runs one targets file. Cross-job sharing of a host's limiter would need a long-running
  scheduler that accepts jobs; nothing needs that yet.

## Consequences

- Throughput grows with workers only up to the host's policy: with the seeded 2 requests/s and 2
  in flight for `data.commoncrawl.org`, the pool's value is resilience and spreading the storage
  work, not speed against Common Crawl. A host with a looser policy gains more.
- A killed worker costs one extra charged request per page it held; the attempt log and the ledger
  still agree exactly.
- A worker's orphaned bytes (stored but never reported) stay in its part, referenced by no row.
- `pool.proto` is the contract; `make proto` regenerates the Python messages. The service glue is
  written by hand on `grpc.aio` so that it type-checks under `mypy --strict`.
- The scheduler serves Prometheus metrics (`STRATA_POOL_METRICS_PORT`): leases granted and taken
  back, results by outcome, results refused, leases in flight, workers connected.
