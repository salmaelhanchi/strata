# The console API (v1)

`strata console` serves this API, the web app and `/metrics` on one port (default 8040). The
generated schema is at `/docs` (OpenAPI). Everything here is what the web app itself calls.

## Endpoints

| Method and path | What it returns |
| --- | --- |
| `GET /v1/crawls?limit=N` | The newest Common Crawl crawls |
| `GET /v1/sites/{site}/profile?crawls=N&budget=B` | Pages per crawl, URL shapes, statuses, types, languages, hosts, depth, robots.txt (no page fetched) |
| `GET /v1/sites/{site}/diff?from=A&to=B` | Added, removed, changed and unchanged counts, overall and per URL shape (default: the two newest crawls) |
| `GET /v1/sites/{site}/diff/changes?from=A&to=B&change=K&cursor=C&limit=N` | The changed URLs of that diff, a page at a time |
| `GET /v1/sites/{site}/changes?crawls=N` | Change rate per URL shape over the last N crawls |
| `POST /v1/pulls` `{site, pattern, limit, crawl, budget}` | 202 and a pull run; the pull runs in the background as a job |
| `GET /v1/pulls/{run_id}` | The run: state, job id, events so far |
| `GET /v1/pulls/{run_id}/events` | Server-Sent Events: `started`, `progress` (once a second), then `done` or `error` |
| `GET /v1/jobs?cursor=C&limit=N` | Jobs, newest first |
| `GET /v1/jobs/{job_id}` | A job's report: state, budget, attempts by outcome, host and client, robots verdicts |
| `GET /v1/jobs/{job_id}/pages?cursor=C&limit=N` | Pages the console pulled for the job, in URL order |
| `GET /v1/captures/{capture_id}/lineage` | The row back to its bytes, both digests re-checked, and the normalized text when the console pulled it |
| `GET /metrics` | Prometheus: requests by route and status, latency, pulls by state, requests sent upstream |

## Pagination

List endpoints return `{"items": [...], "next_cursor": "..."}`. Pass `next_cursor` back as
`?cursor=` for the next page; it is absent on the last page. A cursor encodes the sort key of the
last item, so a page stays stable while new jobs or pages appear before it.

## Errors

Errors are `application/problem+json` (RFC 9457): `type`, `title`, `status`, `detail`, and for
upstream failures the number of `attempts` made.

### not-found
`404`: no such job, capture, pull run or route.

### bad-request
`400`: a malformed cursor.

### invalid-request
`422`: the request did not validate; `errors` lists each field.

### query-refused
`422`: Common Crawl refused the query, or the URL pattern is not a valid RE2 expression. Retrying
gets the same answer.

### budget-exhausted
`402`: the request budget can't pay for the index query; nothing was run. Raise `budget`.

### index-unavailable
`503` with `Retry-After`: Common Crawl's index kept failing after every retry the policy allows.

### blocked-by-common-crawl
`502`: Common Crawl answered 403. Strata stops and leaves it to a person (ADR-003).

### job-conflict
`409`: the job can't be continued here (it is running elsewhere, or keeps its data on other
storage).
