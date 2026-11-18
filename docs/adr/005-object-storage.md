# ADR-005: Object storage after MinIO

- Status: accepted (week 2 spike, S9, run 2026-11-08; results below)
- Date: 2026-11-08

## Context

Week 2 moves raw WARC parts and lake files to a local S3-compatible store, the same shape as the
AWS setup planned for week 5. The week 1 plan named MinIO. That no longer works: MinIO stopped
publishing community images in October 2025 and archived the repository in April 2026, so
`docker pull minio/minio` fails.

Strata needs a store that:

1. starts from environment variables alone, so it can run as a GitHub Actions service container
   (service containers take an image, env and ports, but no command);
2. serves byte ranges exactly, including suffix ranges, since lineage reads one record from a part;
3. accepts multipart uploads, since raw parts rotate at 100 MB;
4. works with DuckDB's S3 secret (path-style URLs, plain HTTP locally) for Parquet;
5. works as a DuckLake data path (`DATA_PATH 's3://lake/'`), including the file rewrites and
   deletes that `CHECKPOINT` performs.

Useful but not required: boto3's default CRC32 integrity checksums, stored SHA-256 checksums (S14
verifies each upload), and `If-None-Match: *` (put-if-absent, for idempotent uploads).

## Candidates

- **RustFS** (Apache-2.0, Rust). Its image is configured by `RUSTFS_ACCESS_KEY`,
  `RUSTFS_SECRET_KEY`, `RUSTFS_VOLUMES` and `RUSTFS_ADDRESS`, so it can run as a service container.
- **SeaweedFS** (Apache-2.0, Go). Its image needs a command (`server -s3 ...`); S3 credentials can
  come from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. In CI it would start from a `docker run`
  step instead of a service container.

## Method

`deploy/spike/compose.yaml` starts either candidate; `deploy/spike/check_store.py` runs the same
checks against it and writes `docs/spikes/object-store-<name>.json`:

```
docker compose -f deploy/spike/compose.yaml up -d rustfs
uv run --with boto3 python deploy/spike/check_store.py rustfs --endpoint http://localhost:19000
docker compose -f deploy/spike/compose.yaml down -v
```

Timebox: 2 hours. RustFS first; SeaweedFS only if RustFS fails a required check.

The script was first run against `moto`'s S3 server (a mock, not a candidate) to check the script
itself: all eight checks passed. Its DuckLake check also showed that `CHECKPOINT` leaves the merged
files' predecessors on the store (4 active files became 1, objects on the store 4 became 5) until
DuckLake's cleanup retention has passed. S17 will set that retention deliberately.

## Results

RustFS was run on 2026-11-08 on a Windows machine with Docker Desktop (DuckDB 2.0.0, Python
3.12.12); the full report is `docs/spikes/object-store-rustfs.json`. It passed every check, so
SeaweedFS was not run, as the method says.

| Check | RustFS | Time |
| --- | --- | --- |
| Starts from env only | yes: the compose service has no command (CI service container still to confirm in S14) | |
| bucket | pass | 93 ms |
| range-get: middle and suffix ranges, `Content-Range` | pass | 92 ms |
| multipart: 2 parts, 6 MiB, range across the part boundary | pass | 297 ms |
| duckdb-s3: 100k rows written, filtered read exact in 3 requests | pass | 1,625 ms |
| ducklake-s3: 4 flushes, `CHECKPOINT` merged 4 active files into 1 | pass | 1,937 ms |
| checksums: boto3's default CRC32 (info) | pass | 31 ms |
| sha256: stored and returned on HEAD (info) | pass | 47 ms |
| put-if-absent: second PUT refused with 412 (info) | pass | 14 ms |
| Image tested | `rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2` | |

Notes:

- RustFS sends no `Server` header, so the report can't name its version; the digest identifies
  the image.
- After `CHECKPOINT`, the store held 5 objects for 1 active file: the 4 merged files stay until
  DuckLake's cleanup retention passes. S17 sets that retention and measures it.

## Decision

**RustFS**, pinned to the digest above, for Compose and CI. Upgrades are deliberate: a new digest
is a commit that reruns `check_store.py` first.

SeaweedFS stays the documented fallback. It would need a `docker run` step in CI, since its image
needs a command.

## Consequences

- Compose gains the store and a one-shot job that creates the `raw` and `lake` buckets (S14).
- CI's integration job gets the store as a service container, or a `docker run` step.
- The image is pinned by digest: a moving `latest` tag is how MinIO's disappearance would have
  broken CI without warning.

## Built in S14 (2026-11-14)

- Compose runs RustFS on port 9000 next to Postgres, pinned to the same digest. Setup
  (`strata migrate`, and every run) creates the raw and lake buckets.
- A lake on S3 gets its own DuckLake catalog database: DuckLake records a lake's data path when
  its catalog is created and refuses to reopen it with another (measured 2026-11-15), so a local
  lake can't be moved by changing a setting. Setup creates the catalog database named in
  `STRATA_LAKE_CATALOG`.
- Each job records its storage (staging folder, raw store, lake) and is continued only there.
- A run stages each WARC part on local disk (the write-ahead log, S12) and seals it into the
  bucket at every bronze flush: the part's SHA-256 is checked on disk, then sent with the upload,
  and read back from HEAD. Rows reach the lake only after their part is sealed.
- DuckLake writes its Parquet files to `s3://` through a DuckDB S3 secret built from the same
  settings.
- Tests use moto's S3 server in-process, so CI needs no store. Moto stores a checksum it is sent
  without checking it; the spike's new `bad-digest` check records whether a real store refuses a
  body that doesn't match. Not yet run against RustFS: the spike result on file is from
  2026-11-08, before the check existed.
- `CHECKPOINT` keeps merged files while a snapshot needs them. `strata lake-checkpoint
  --older-than '7 days'` sets the retention, after which they are deleted (measured: 4 files
  merged into 1, then the 4 deleted).
