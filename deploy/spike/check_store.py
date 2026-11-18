"""Week 2, S9: does an S3-compatible store do what Strata needs? One run, one verdict.

    uv run --with boto3 python deploy/spike/check_store.py rustfs --endpoint http://localhost:19000

Checks, in the order Strata depends on them:

  required  bucket        create a bucket and see it
  required  range-get     byte ranges, including a suffix range (lineage reads one record)
  required  multipart     a multipart upload, and a range read across its part boundary
  required  duckdb-s3     DuckDB writes and reads Parquet on the store through an S3 secret
  required  ducklake-s3   DuckLake with its data path on the store: insert, read, CHECKPOINT
  info      checksums     boto3's default CRC32 integrity checksums are accepted
  info      sha256        a SHA-256 checksum is stored and returned (S14 verifies uploads)
  info      bad-digest    an upload whose SHA-256 doesn't match its bytes is refused
  info      put-if-absent PUT with If-None-Match: * refuses to overwrite

Prints a table, writes docs/spikes/object-store-<name>.json, and exits 1 if a required check
failed. Everything it creates goes into a fresh bucket that it deletes at the end.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sys
import tempfile
import time
import traceback
from base64 import b64encode
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.config import Config
from botocore.exceptions import ClientError

MIB = 1024 * 1024


@dataclass
class Check:
    name: str
    required: bool
    passed: bool = False
    ms: int = 0
    detail: str = ""


@dataclass
class Spike:
    store: str
    endpoint: str
    bucket: str
    checks: list[Check] = field(default_factory=list)
    server_header: str | None = None
    duckdb_version: str = duckdb.__version__
    python: str = platform.python_version()
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat(timespec="seconds"))

    def run(self, name: str, required: bool, fn: Callable[[], str]) -> None:
        check = Check(name, required)
        t0 = time.monotonic()
        try:
            check.detail = fn()
            check.passed = True
        except Exception as exc:  # noqa: BLE001 - a spike reports every failure and goes on
            check.detail = f"{type(exc).__name__}: {exc}"[:400]
            if os.environ.get("SPIKE_TRACEBACK"):
                traceback.print_exc()
        check.ms = int((time.monotonic() - t0) * 1000)
        self.checks.append(check)
        mark = "PASS" if check.passed else ("FAIL" if required else "no  ")
        print(f"  {mark}  {name:<14} {check.ms:>6} ms  {check.detail}")


def client(endpoint: str, key: str, secret: str, *, checksums: str = "when_required") -> Any:
    # "when_required": send integrity checksums only where S3 demands them. boto3's newer
    # default ("when_supported") adds CRC32 headers that some S3-compatible stores refuse;
    # the "checksums" check tries that default on its own.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1},
            request_checksum_calculation=checksums,
            response_checksum_validation="when_required",
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("store", help="a name for the report, e.g. rustfs or seaweedfs")
    parser.add_argument("--endpoint", required=True, help="e.g. http://localhost:19000")
    parser.add_argument("--access-key", default="strata-local")
    parser.add_argument("--secret-key", default="strata-local-secret")
    parser.add_argument("--out", default="docs/spikes", help="where the JSON report goes")
    args = parser.parse_args()

    bucket = f"strata-spike-{os.urandom(3).hex()}"
    spike = Spike(store=args.store, endpoint=args.endpoint, bucket=bucket)
    s3 = client(args.endpoint, args.access_key, args.secret_key)
    blob = os.urandom(MIB)  # incompressible, so sizes on the wire are what we wrote
    print(f"{args.store} at {args.endpoint}, bucket {bucket}")

    def bucket_check() -> str:
        reply = s3.create_bucket(Bucket=bucket)
        s3.head_bucket(Bucket=bucket)
        headers = reply["ResponseMetadata"]["HTTPHeaders"]
        spike.server_header = headers.get("server")
        return f"created; Server: {spike.server_header or '?'}"

    def range_get() -> str:
        s3.put_object(Bucket=bucket, Key="raw/part-00000.warc.gz", Body=blob)
        mid = s3.get_object(Bucket=bucket, Key="raw/part-00000.warc.gz", Range="bytes=1000-1999")
        got = mid["Body"].read()
        assert got == blob[1000:2000], "middle range returned the wrong bytes"
        assert mid["ContentRange"] == f"bytes 1000-1999/{MIB}", mid.get("ContentRange")
        tail = s3.get_object(Bucket=bucket, Key="raw/part-00000.warc.gz", Range="bytes=-100")
        assert tail["Body"].read() == blob[-100:], "suffix range returned the wrong bytes"
        return "middle and suffix ranges exact; Content-Range correct"

    def multipart() -> str:
        key = "raw/part-00001.warc.gz"
        first, second = os.urandom(5 * MIB), os.urandom(MIB)  # 5 MiB is S3's minimum part
        upload = s3.create_multipart_upload(Bucket=bucket, Key=key)
        parts = []
        for number, body in enumerate((first, second), start=1):
            part = s3.upload_part(
                Bucket=bucket, Key=key, UploadId=upload["UploadId"], PartNumber=number, Body=body
            )
            parts.append({"ETag": part["ETag"], "PartNumber": number})
        s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload["UploadId"], MultipartUpload={"Parts": parts}
        )
        size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        assert size == 6 * MIB, f"size {size}"
        edge = 5 * MIB
        got = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={edge - 10}-{edge + 9}")
        assert got["Body"].read() == first[-10:] + second[:10], "range across parts is wrong"
        return "2 parts, 6 MiB; range across the part boundary exact"

    def checksums() -> str:
        default = client(
            args.endpoint, args.access_key, args.secret_key, checksums="when_supported"
        )
        default.put_object(Bucket=bucket, Key="raw/crc.bin", Body=blob[:4096])
        return "PUT with boto3's default CRC32 checksum accepted"

    def sha256() -> str:
        digest = b64encode(hashlib.sha256(blob).digest()).decode()
        s3.put_object(
            Bucket=bucket,
            Key="raw/sha.bin",
            Body=blob,
            ChecksumAlgorithm="SHA256",
            ChecksumSHA256=digest,
        )
        head = s3.head_object(Bucket=bucket, Key="raw/sha.bin", ChecksumMode="ENABLED")
        stored = head.get("ChecksumSHA256")
        assert stored == digest, f"stored checksum {stored!r}"
        return "stored and returned on HEAD"

    def bad_digest() -> str:
        # Strata checks a part's hash on disk before sending it; this is the store's half: bytes
        # damaged on the way must be refused, not stored under the checksum we sent.
        wrong = b64encode(hashlib.sha256(blob + b"x").digest()).decode()
        try:
            s3.put_object(
                Bucket=bucket,
                Key="raw/bad.bin",
                Body=blob,
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=wrong,
            )
        except ClientError as exc:
            return f"refused with {exc.response['Error']['Code']}"
        raise AssertionError("stored bytes under a SHA-256 they don't have")

    def put_if_absent() -> str:
        key = "raw/once.bin"
        s3.put_object(Bucket=bucket, Key=key, Body=b"first", IfNoneMatch="*")
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=b"second", IfNoneMatch="*")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            assert code in ("PreconditionFailed", "412"), code
            return "second PUT refused with 412"
        raise AssertionError("second PUT overwrote the object")

    endpoint = urlparse(args.endpoint)
    secret_sql = f"""
        CREATE OR REPLACE SECRET spike (
            TYPE s3, KEY_ID '{args.access_key}', SECRET '{args.secret_key}',
            ENDPOINT '{endpoint.netloc}', URL_STYLE 'path', USE_SSL {endpoint.scheme == "https"},
            REGION 'us-east-1'
        )"""

    def duck() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute(secret_sql)
        return con

    def duckdb_s3() -> str:
        con = duck()
        try:
            url = f"s3://{bucket}/parquet/t.parquet"
            con.execute(
                f"COPY (SELECT i, md5(i::VARCHAR) AS h FROM range(100000) t(i)) TO '{url}'"
                " (FORMAT parquet, ROW_GROUP_SIZE 10000)"
            )
            con.execute("CALL enable_logging(['HTTP'])")
            count, top = _one(con, f"SELECT count(*), max(i) FROM '{url}' WHERE i >= 95000")
            (requests,) = _one(con, "SELECT count(*) FROM duckdb_logs_parsed('HTTP')")
            assert (count, top) == (5000, 99999), (count, top)
            return f"wrote 100k rows, filtered read exact; the read took {requests} request(s)"
        finally:
            con.close()

    def ducklake_s3() -> str:
        con = duck()
        with tempfile.TemporaryDirectory() as tmp:
            try:
                con.execute("INSTALL ducklake")
                con.execute("LOAD ducklake")
                meta = Path(tmp, "meta.ducklake").as_posix()
                con.execute(f"ATTACH 'ducklake:{meta}' AS lake (DATA_PATH 's3://{bucket}/lake/')")
                con.execute("CREATE TABLE lake.captures (id INTEGER, url VARCHAR)")
                for batch in range(4):  # four small flushes, like bronze batches
                    con.execute(
                        "INSERT INTO lake.captures SELECT i, 'https://example.org/' || i"
                        f" FROM range({batch * 25}, {batch * 25 + 25}) t(i)"
                    )
                active = "SELECT count(*) FROM ducklake_list_files('lake', 'captures')"
                rows = "SELECT count(*) FROM lake.captures"
                before = (_one(con, active)[0], _count_keys(s3, bucket, "lake/"))
                assert _one(con, rows) == (100,)
                con.execute("USE lake")
                con.execute("CHECKPOINT")
                assert _one(con, rows) == (100,)
                after = (_one(con, active)[0], _count_keys(s3, bucket, "lake/"))
                # Old files stay on the store until DuckLake's cleanup retention has passed.
                return (
                    f"100 rows in 4 flushes; CHECKPOINT: active files {before[0]} -> {after[0]},"
                    f" objects on the store {before[1]} -> {after[1]}"
                )
            finally:
                con.close()

    spike.run("bucket", True, bucket_check)
    spike.run("range-get", True, range_get)
    spike.run("multipart", True, multipart)
    spike.run("duckdb-s3", True, duckdb_s3)
    spike.run("ducklake-s3", True, ducklake_s3)
    spike.run("checksums", False, checksums)
    spike.run("sha256", False, sha256)
    spike.run("bad-digest", False, bad_digest)
    spike.run("put-if-absent", False, put_if_absent)

    _empty_and_delete(s3, bucket)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    report = out / f"object-store-{args.store}.json"
    report.write_text(json.dumps(asdict(spike), indent=2) + "\n", encoding="utf-8")
    failed = [c.name for c in spike.checks if c.required and not c.passed]
    print(f"{'FAILED: ' + ', '.join(failed) if failed else 'all required checks passed'}")
    print(f"report: {report}")
    return 1 if failed else 0


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...]:
    row = con.execute(sql).fetchone()
    assert row is not None, sql
    return tuple(row)


def _count_keys(s3: Any, bucket: str, prefix: str) -> int:
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return sum(len(page.get("Contents", [])) for page in pages)


def _empty_and_delete(s3: Any, bucket: str) -> None:
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket)
    except ClientError as exc:
        print(f"  (cleanup: {exc}; delete bucket {bucket} by hand)")


if __name__ == "__main__":
    sys.exit(main())
