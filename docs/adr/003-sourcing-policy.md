# ADR-003: Sourcing policy

- Status: accepted
- Date: 2026-11-07

## Context

Strata collects web data that may end up in AI datasets. In 2026 that is contested ground:
publishers restrict archive crawlers over AI-training concerns, the Internet Archive has tightened
automated access to the Wayback Machine, and Common Crawl asks clients sending small requests (index
lookups, single WARC records) to stay below 10 requests per second and to back off after
`503 Slow Down`, by at least 10 seconds per retry. Its index server is strictly rate-limited and
asks clients to cache. How a sourcing engine behaves is part of what it delivers.

## Decision

1. **Sources.** Bulk pages come from Common Crawl. The Wayback Machine is used only for targeted
   history on a few URLs. Where an official API exists (EPO OPS for patents, Wikidata for entities)
   we use it instead of automating the website.
2. **Rates.** Per-host policies live in `ops.policies`: `data.commoncrawl.org` at 2 requests/s with
   at most 2 in flight; `index.commoncrawl.org` at one request every 2 s, one at a time, every answer
   cached. Hosts without a policy get 1 request/s, one at a time.
3. **Retries.** Back off 2, 10, 30 and 60 s with up to 25% jitter, longer when `Retry-After` says so.
   A 429 or 503 pauses every request to that host, not just the one that saw it.
4. **Stops.** A 403 or a challenge stops the host for the job and flags it for a human. No CAPTCHA
   solving, proxy rotation or fingerprint evasion.
5. **Identity.** An honest User-Agent naming the project and its repository URL.
6. **Budgets.** Every job has a request budget, retries included, reserved and committed before each
   request is sent. A breach stops the job; it is never just logged.
7. **Provenance.** Every row records where its bytes came from, when, and the policy verdict at fetch
   time (`not_evaluated` until robots.txt and AI-preference checks land in week 2).
8. **Distribution.** We publish pipelines and small samples with provenance, not bulk copies of
   third-party content.

## Consequences

- Week-1 throughput is deliberately low (about 2 records/s), which is plenty for 50–1,000 pages.
- Retries spend budget, so budgets must leave headroom for retries; reports show the spend.
- The rules are tested with fake servers (429/503 with `Retry-After`, 403, budget caps), so
  regressions fail CI instead of reaching a real host.
