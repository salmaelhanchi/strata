# ADR-001: DuckLake with a Postgres catalog for the lake

- Status: accepted
- Date: 2026-11-07

## Context

Strata stores every capture as an immutable bronze row, then silver and gold tables derived from
it. From weeks 3–4 several processes use the lake at once: the API, the workers and the MCP query
tool. From week 5 Spark processes whole crawls. We want ACID tables over Parquet, snapshots for time
travel, and as little infrastructure as possible.

Options considered:

| Option | For | Against |
| --- | --- | --- |
| Plain Parquet files | Nothing to run | No transactions, no snapshots, manual compaction and schema evolution |
| Apache Iceberg | Broad engine support, Spark writes natively | Needs a catalog service (REST, Glue) and more moving parts |
| DuckLake, DuckDB-file catalog | Simplest possible start | Only one process can attach it for writing |
| DuckLake, Postgres catalog | Catalog in a database we already run; many processes can attach | Engine support beyond DuckDB is still narrow |

## Decision

Use DuckLake (1.x, through DuckDB 2.0 LTS) with its catalog in Postgres: the same server as the ops
schema, in a separate `ducklake_catalog` database. Data lives on local disk in week 1 and on
S3-compatible storage from week 2. A plain DuckDB file with the same catalog name (`lake`) and the
same SQL is available through `STRATA_LAKE_LOCAL_FILE` for tests and offline work.

## Consequences

- One database server to run locally and in the cloud; week 5 changes connection strings only.
- The lake does not enforce unique keys, so every insert is idempotent: an anti-join on
  `capture_id`, staged outside the transaction because one DuckDB transaction writes one database.
- Spark writes Parquet to a staging prefix and a DuckDB step inserts it in one transaction. If Spark
  ever has to write tables directly, revisit this ADR in favour of Iceberg.
- DuckDB downloads the `ducklake` and `postgres` extensions at runtime; environments without that
  download use the local-file mode, and CI runs the real DuckLake test.
