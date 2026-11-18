# ADR-006: robots.txt verdicts

- Status: accepted (design); implemented in week 2, step S13
- Date: 2026-11-08
- Fills in: ADR-003 §7 (provenance records the policy verdict at fetch time)

## Context

Every bronze row has a `policy_verdict`, `not_evaluated` since week 1. Strata reads Common Crawl's
copies, not the sites, but the question a data buyer asks is still the site owner's: would this
site have let a polite crawler have this page? Common Crawl fetched it as CCBot, under the rules
the site set for CCBot at the time. Strata asks the same question for itself.

RFC 9309 (the Robots Exclusion Protocol) settles the parsing and matching. Common Crawl captures
each host's robots.txt during a crawl and lists those captures in the URL index under
`subset=robotstxt`, so the robots.txt that applied when the page was crawled is available, with
the same provenance as the page.

## Decision

1. **Which robots.txt.** The newest capture of `<scheme>://<host>/robots.txt` in the same crawl as
   the page, found in the URL index's `robotstxt` subset (its own 300 files; the shard map is keyed
   by crawl and subset). It is fetched like any capture: paced, verified and stored raw. Each
   verdict records the robots capture it was based on.
2. **Which group.** Our product token is `strata`; without a matching group, the `*` group applies
   (RFC 9309 §2.2.1).
3. **Matching.** Protego, which implements RFC 9309's rules: the longest matching path wins, and
   `Allow` wins a tie. The standard library's `urllib.robotparser` predates RFC 9309 and applies the
   first matching rule instead.
4. **Status codes** (RFC 9309 §2.3.1):

   | robots.txt result | Verdict |
   | --- | --- |
   | 2xx | parse and match |
   | 4xx ("unavailable") | `allowed` |
   | 5xx or unreachable | `disallowed` (complete disallow) |
   | Redirects | followed, up to five |
   | No capture in the crawl | `needs_review`, excluded unless `--allow-unknown-robots` |

5. **Stricter wins.** A page Common Crawl captured but our rules disallow is `disallowed`.
6. **Decide before fetching.** Verdicts go to `ops.policy_decisions` (URL, verdict, the rule that
   matched, the robots capture id) before any WARC request, and only `allowed` pages are fetched.
   Bronze rows gain `robots_capture_id`.
7. **Crawl-delay is recorded, not applied.** It governs load on the site's own server, and Strata
   sends the site no traffic. Our pacing toward Common Crawl stays ADR-003's.
8. **AI-usage preferences are kept, not yet evaluated.** The IETF AIPREF drafts add a
   `Content-Usage` rule to robots.txt (for example `Content-Usage: train-ai=n`). They are still
   Internet-Drafts, so Strata stores the robots.txt bytes with every verdict and records any
   `Content-Usage` lines verbatim, and evaluates them once the vocabulary is published.

## Consequences

- Each crawl adds one more shard map (300 footer reads, once) and one robots.txt fetch per host.
- A host with no robots.txt capture produces `needs_review` rows that are counted in reports, not
  silently dropped or silently allowed.
- Verdicts are reproducible: the same crawl and the same rules give the same answer, and each one
  points at the bytes it was based on.

## Alternatives considered

- **Fetch today's robots.txt from the live site.** It answers a different question (today's rules,
  not the rules when the page was crawled) and would make Strata contact sites it otherwise never
  touches.
- **Trust Common Crawl's choice.** CCBot's rules can differ from a generic crawler's, and a site
  may allow CCBot while disallowing everyone else.

## Amendment (2026-11-14, as built in S13)

1. **The matched rule is not recorded.** Decision 6 promised "the rule that matched"; Protego
   answers allowed or not and doesn't say which line decided it. Each verdict records the
   robots.txt capture it rests on, and the capture's bytes are stored with the job, so the rule
   can be re-derived exactly. Recording it directly would mean our own matcher; not worth it now.
2. **Scheme, host and redirects.** Common Crawl's keys drop the scheme and a leading `www`, so
   `http://example.org/robots.txt`, `https://www.example.org/robots.txt` and the bare host share
   one key. The lookup keeps only captures of the exact host, prefers the scheme asked for, and
   treats a redirect as a loop only when scheme, host and key all repeat. The commonest chain,
   `example.org` to `www.example.org`, was first misread as a loop (found by a test).
   An http capture that redirects to an https file the crawl never captured is `unresolved`.
3. **Shared evidence.** A robots.txt capture is stored once, in `bronze.robots`, and reused by
   later jobs (read back from the store, not fetched again). Lookups are cached per crawl and host.
4. **Always through the URL index.** The robots.txt lookup reads the `robotstxt` subset of the
   Parquet URL index, also for jobs whose pages were found through the CDX server.
