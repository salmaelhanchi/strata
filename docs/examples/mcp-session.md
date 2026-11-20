# One MCP conversation with `strata mcp`

The MCP SDK's client started `uv run strata mcp` and talked to it over stdio, as an MCP host (Claude Desktop, Claude Code, an IDE) does. Tools it offered:

- `profile_site` (read-only): What a site holds in Common Crawl: pages per crawl (the newest `crawls` crawls), its largest URL shapes and what robots.txt allows of each, HTTP statuses, languages, hosts, and the robots.txt rules that apply.
- `diff_site` (read-only): Pages added, removed and changed between two crawls (by default the two newest), from the payload digests Common Crawl's index records; with counts per URL shape and example URLs of each kind.
- `change_rates` (read-only): How often each URL shape changed between consecutive crawls over the newest `crawls` crawls: the shapes that change are worth re-scraping, the stable ones once.
- `pull_pages` (writes): Fetch up to `limit` pages of a site from Common Crawl's archive (never from the site) as a job: the budget is spent before each request, robots.txt from the same crawl decides first, every record is verified.
- `trace_row` (read-only): Where a pulled row came from: Common Crawl's WARC file and byte range, Strata's stored copy, and the checks that tie them (bytes re-hashed, both digests compared, the robots.txt verdict).

rfc-editor.org's catalogs for these three crawls were already cached by earlier `strata profile`
and `strata changes` runs, so every answer below cost 0 requests; building them the first time
cost 99 requests for two crawls and 49 for the third (see the README). No page of the site was
fetched for any of this.

## `profile_site(site='rfc-editor.org', crawls=2)`

Answered in 1.0 s.

```json
{
  "site": "rfc-editor.org",
  "crawls": [
    {
      "crawl": "CC-MAIN-2026-43",
      "pages": 4686,
      "captures": 6975
    },
    {
      "crawl": "CC-MAIN-2026-39",
      "pages": 3560,
      "captures": 5180
    }
  ],
  "shapes": [
    {
      "shape": "/info/rfcN/…",
      "pages": 3393,
      "share": 0.7241,
      "allowed": 3393,
      "example": "https://www.rfc-editor.org/info/rfc10001/"
    },
    {
      "shape": "/rfc/rfcN.html",
      "pages": 319,
      "share": 0.0681,
      "allowed": 319,
      "example": "https://www.rfc-editor.org/rfc/rfc10001.html"
    },
    {
      "shape": "/info/bcpN/…",
      "pages": 73,
      "share": 0.0156,
      "allowed": 73,
      "example": "https://www.rfc-editor.org/info/bcp10/"
    },
    {
      "shape": "/search/?…",
      "pages": 53,
      "share": 0.0113,
      "allowed": 53,
      "example": "https://www.rfc-editor.org/search/?area=sec"
    },
    "…"
  ],
  "statuses": {
    "200": 6975
  },
  "languages": {
    "eng": 4681,
    "-": 5
  },
  "hosts": {
    "www.rfc-editor.org": 4629,
    "mailman3.rfc-editor.org": 22,
    "queue.rfc-editor.org": 14,
    "rse-wiki.rfc-editor.org": 10,
    "rpc-wiki.rfc-editor.org": 9
  },
  "robots": [
    {
      "host": "www.rfc-editor.org",
      "basis": "parsed",
      "verdicts": {
        "allowed": 4629
      },
      "rules": [
        "User-agent: *",
        "Disallow: /rfc/authors/",
        "Disallow: /rfc/rerendered/",
        "Disallow: /rfc/v3test/"
      ],
      "crawl_delay_s": null,
      "sitemaps": [
        "https://www.rfc-editor.org/sitemap.xml"
      ]
    },
    {
      "host": "mailman3.rfc-editor.org",
      "basis": "unavailable",
      "verdicts": {
        "allowed": 22
      },
      "rules": [],
      "crawl_delay_s": null,
      "sitemaps": []
    },
    {
      "host": "queue.rfc-editor.org",
      "basis": "parsed",
      "verdicts": {
        "allowed": 14
      },
      "rules": [
        "User-agent: *"
      ],
      "crawl_delay_s": null,
      "sitemaps": [
        "https://queue.rfc-editor.org/sitemap.xml"
      ]
    }
  ],
  "requests": 0,
  "page_fetches": 0
}
```

## `diff_site(site='rfc-editor.org')`

Answered in 0.4 s.

```json
{
  "site": "rfc-editor.org",
  "from_crawl": "CC-MAIN-2026-39",
  "to_crawl": "CC-MAIN-2026-43",
  "counts": {
    "added": 4058,
    "removed": 2932,
    "changed": 619,
    "same": 9
  },
  "shapes": [
    {
      "shape": "/info/rfcN/…",
      "added": 2829,
      "removed": 2407,
      "changed": 564,
      "same": 0
    },
    {
      "shape": "/rfc/rfcN.html",
      "added": 314,
      "removed": 322,
      "changed": 0,
      "same": 5
    },
    {
      "shape": "/info/bcpN/…",
      "added": 66,
      "removed": 42,
      "changed": 7,
      "same": 0
    },
    {
      "shape": "/rfc/inline-errata/…",
      "added": 45,
      "removed": 66,
      "changed": 0,
      "same": 0
    },
    "…"
  ],
  "examples": {
    "removed": [
      "https://history.rfc-editor.org/RAREreports/",
      "https://history.rfc-editor.org/imr/",
      "https://history.rfc-editor.org/mil-std-tests/",
      "https://history.rfc-editor.org/tcp-ip-digest/",
      "…"
    ],
    "added": [
      "https://history.rfc-editor.org/ien/ien2",
      "https://mailman3.rfc-editor.org/accounts/login/?next=%2Fmailman3%2Flists%2Frfc-dist.rfc-editor.org%2F",
      "https://mailman3.rfc-editor.org/accounts/login/?next=/accounts/password/reset/",
      "https://mailman3.rfc-editor.org/accounts/login/?next=/mailman3/lists/rfc-dist.rfc-editor.org/",
      "…"
    ],
    "changed": [
      "https://mailman3.rfc-editor.org/accounts/login/?next=/mailman3/lists/",
      "https://mailman3.rfc-editor.org/accounts/signup/?next=/mailman3/lists/",
      "https://mailman3.rfc-editor.org/mailman3/lists/?all-lists",
      "https://mailman3.rfc-editor.org/mailman3/lists/auth48archive.rfc-editor.org/",
      "…"
    ]
  },
  "requests": 0,
  "page_fetches": 0,
  "note": "removed = not captured in the later crawl (Common Crawl doesn't revisit every page); changed = a different payload digest, which a page with a timestamp shows every time"
}
```

## `change_rates(site='rfc-editor.org', crawls=3)`

Answered in 0.4 s.

```json
{
  "site": "rfc-editor.org",
  "crawls": [
    "CC-MAIN-2026-34",
    "CC-MAIN-2026-39",
    "CC-MAIN-2026-43"
  ],
  "changing": [
    {
      "shape": "/info/rfcN/…",
      "changed": 831,
      "recaptured": 831,
      "pages": 7584,
      "example": "https://www.rfc-editor.org/info/rfc1/"
    },
    {
      "shape": "/search/?…",
      "changed": 28,
      "recaptured": 28,
      "pages": 98,
      "example": "https://www.rfc-editor.org/search/?area=art"
    },
    {
      "shape": "/doku.php?…",
      "changed": 21,
      "recaptured": 21,
      "pages": 49,
      "example": "https://rpc-wiki.rfc-editor.org/doku.php?id=pilot_test_instructions_completing_auth48_using_kramdown"
    },
    {
      "shape": "/info/bcpN/…",
      "changed": 11,
      "recaptured": 11,
      "pages": 144,
      "example": "https://www.rfc-editor.org/info/bcp10/"
    },
    "…"
  ],
  "stable": [
    {
      "shape": "/rfc/rfcN.html",
      "changed": 0,
      "recaptured": 9,
      "pages": 919,
      "example": "https://www.rfc-editor.org/rfc/rfc10001.html"
    },
    {
      "shape": "/final-review/",
      "changed": 0,
      "recaptured": 1,
      "pages": 1,
      "example": "https://queue.rfc-editor.org/final-review/"
    }
  ],
  "requests": 0
}
```
