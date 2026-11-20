"""The fetch pool: a scheduler that leases pages to workers over gRPC (ADR-009).

The scheduler keeps everything that must be decided in one place: the work queue (Postgres), the
budget (reserved before a lease is granted) and each host's politeness (token bucket, in-flight
cap, pause on 429/503). Workers only read the records they are leased, store them, and report.
"""
