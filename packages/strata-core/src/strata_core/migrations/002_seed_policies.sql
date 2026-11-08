-- 002: default politeness policies (ADR-003).
-- Common Crawl asks clients sending small requests (index lookups, single WARC records) to stay
-- below 10 requests per second; Strata uses 2/s. The index server is strictly rate-limited.
INSERT INTO ops.policies (host, max_rps, burst, max_in_flight, notes) VALUES
    ('data.commoncrawl.org', 2, 2, 2,
     'Byte-range WARC reads. 5x under Common Crawl guidance (<10/s). ADR-003.'),
    ('index.commoncrawl.org', 0.5, 1, 1,
     'CDX index server: strictly rate-limited; one request at a time; cache every answer. ADR-003.')
ON CONFLICT (host) DO NOTHING;
