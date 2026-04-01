import asyncio
import io
import json
import os
import secrets
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow.parquet as pq

from .async_utils import with_semaphore
from .models import RunManifest

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


def generate_hex_id() -> str:
    return secrets.token_hex(3)


async def upload_run_manifest(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    manifest: RunManifest,
) -> None:
    body = manifest.model_dump_json()
    await s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
    )


async def upload_jsonl_chunk(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    rows: list[dict],
) -> None:
    body = "\n".join(json.dumps(row) for row in rows)
    await s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
    )


async def list_jsonl_chunks(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    hex_id: str,
) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl") and hex_id in key:
                keys.append(key)
    return sorted(keys)


async def merge_jsonl_to_parquet(
    s3_client: "S3Client",
    bucket: str,
    jsonl_keys: list[str],
    output_key: str,
    deduplicate_on: list[str] | None = None,
) -> None:
    conn = duckdb.connect()

    all_rows = []
    for key in jsonl_keys:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        text = body.decode("utf-8")
        for line in text.strip().split("\n"):
            if line.strip():
                try:
                    all_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    if not all_rows:
        empty_table = conn.execute("SELECT 1 LIMIT 0").arrow()
        buf = io.BytesIO()
        pq.write_table(empty_table, buf)
        buf.seek(0)
        await s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=buf.read(),
        )
        return

    conn.execute("CREATE TABLE temp AS SELECT * FROM read_json_auto('data.jsonl')")
    conn.insert("temp", [all_rows])

    if deduplicate_on:
        cols = ", ".join(deduplicate_on)
        conn.execute(
            f"CREATE TABLE deduped AS SELECT * FROM temp WHERE rowid IN (SELECT MAX(rowid) FROM temp GROUP BY {cols})"
        )
        conn.execute("DROP TABLE temp")
        conn.execute("ALTER TABLE deduped RENAME TO temp")

    table = conn.execute("SELECT * FROM temp").arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    await s3_client.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=buf.read(),
    )

    conn.close()


async def delete_objects(
    s3_client: "S3Client",
    bucket: str,
    keys: list[str],
) -> None:
    if not keys:
        return
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        delete_spec = {"Objects": [{"Key": k} for k in chunk]}
        await s3_client.delete_objects(Bucket=bucket, Delete=delete_spec)


async def s3_object_exists(s3_client: "S3Client", bucket: str, key: str) -> bool:
    """Check if S3 object exists."""
    from botocore.exceptions import ClientError

    try:
        await s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
    except s3_client.exceptions.NoSuchKey:
        return False


async def read_parquet_columns(s3_client: "S3Client", bucket: str, key: str) -> set[str]:
    """Read column names from parquet without loading data."""
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        buf = io.BytesIO(body)
        table = pq.read_table(buf)
        return set(table.column_names)
    except Exception:
        return set()


async def read_parquet_columns_if_exists(
    s3_client: "S3Client", bucket: str, key: str
) -> set[str]:
    """Read columns if parquet exists, else return empty set."""
    if await s3_object_exists(s3_client, bucket, key):
        return await read_parquet_columns(s3_client, bucket, key)
    return set()


async def discover_batch_columns(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str,
    batch: str,
    semaphore: asyncio.Semaphore,
) -> set[str]:
    """Discover columns for a single batch (dataset + annotations)."""
    from .cleanup import enumerate_annotators

    keys = [f"{prefix}/{dataset_name}/{batch}/merged.parquet"]

    annotators = await enumerate_annotators(s3_client, bucket, prefix, dataset_name)
    keys.extend(
        f"{prefix}/{dataset_name}/annotations/{a}/{batch}/merged.parquet"
        for a in annotators
    )

    results = await asyncio.gather(
        *[
            with_semaphore(
                lambda k=key: read_parquet_columns_if_exists(s3_client, bucket, k),
                semaphore,
            )
            for key in keys
        ]
    )

    columns: set[str] = set()
    for cols in results:
        columns.update(cols)
    return columns


async def discover_dataset_columns(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str,
    max_concurrency: int | None = None,
) -> set[str]:
    """
    Discover all columns across dataset batches and annotations.

    Uses parallel processing with semaphore-limited concurrency at file level.
    """
    from .cleanup import enumerate_batches

    if max_concurrency is None:
        max_concurrency = int(os.environ.get("FILTER_MAX_CONCURRENCY", "20"))

    batches = await enumerate_batches(s3_client, bucket, prefix, dataset_name)

    if not batches:
        return set()

    semaphore = asyncio.Semaphore(max_concurrency)

    results = await asyncio.gather(
        *[
            discover_batch_columns(
                s3_client, bucket, prefix, dataset_name, batch, semaphore
            )
            for batch in batches
        ]
    )

    all_columns: set[str] = set()
    for cols in results:
        all_columns.update(cols)
    return all_columns
