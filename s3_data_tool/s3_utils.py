import asyncio
import io
import itertools
import json
import logging
import os
import secrets
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterator
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel
from .async_utils import with_semaphore
from .models import AnnotationManifest, RunManifest

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)


def generate_hex_id() -> str:
    return secrets.token_hex(3)


def transform_row_for_jsonl(row: dict[str, Any]) -> dict[str, Any]:
    """Transform row: serialize all values to JSON strings."""
    transformed = {}
    for key, value in row.items():
        if isinstance(value, (dict, list, str)):
            transformed[key] = json.dumps(value, separators=(",", ":"))
        else:
            transformed[key] = value

    transformed["id"] = row["id"]
    return transformed


def serialize_complex_values(row: dict[str, Any]) -> dict[str, Any]:
    """Serialize list and dict values to JSON strings so they can be stored in Parquet string columns."""
    for key, value in row.items():
        if isinstance(value, (dict, list)):
            row[key] = json.dumps(value, separators=(",", ":"))
    return row


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


async def upload_annotation_manifest(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    manifest: AnnotationManifest,
) -> None:
    """Write an annotation manifest to S3."""
    body = manifest.model_dump_json()
    await s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
    )


async def read_annotation_manifest(
    s3_client: "S3Client",
    bucket: str,
    key: str,
) -> AnnotationManifest | None:
    """Read an annotation manifest from S3, or None if it doesn't exist."""
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        return AnnotationManifest(**json.loads(body))
    except Exception:
        return None


def annotation_manifest_key(
    prefix: str, dataset_name: str, annotator_name: str, batch_name: str
) -> str:
    """Build the S3 key for an annotation manifest."""
    return (
        f"{prefix}/{dataset_name}/annotations/{annotator_name}/"
        f"{batch_name}/annotation_manifest.json"
    )


async def read_parquet_columns(
    s3_client: "S3Client", bucket: str, key: str
) -> set[str]:
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
    if max_concurrency is None:
        max_concurrency = int(os.environ.get("FILTER_MAX_CONCURRENCY", "20"))

    batches = await enumerate_batches(
        s3_client, bucket, prefix, dataset_name
    )

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


def batched_rows(
    row_iter: Iterator[dict[str, Any]], batch_size: int = 100_000
) -> Iterator[list[dict[str, Any]]]:
    while True:
        batch = list(itertools.islice(row_iter, batch_size))
        if not batch:
            return
        yield batch


async def async_batched_rows(
    row_iter: AsyncIterator[dict[str, Any]], batch_size: int = 100_000
) -> AsyncIterator[list[dict[str, Any]]]:
    while True:
        batch = []
        count = 0
        async for row in row_iter:
            batch.append(row)
            count += 1
            if count >= batch_size:
                break
        if not batch:
            return
        yield batch


TYPE_TO_PYARROW = {
    type(None): pa.null(),
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    dict: pa.string(),
    list: pa.string(),
}


def merge_types(type1: pa.DataType, type2: pa.DataType) -> pa.DataType:
    if pa.types.is_null(type1):
        return type2
    if pa.types.is_null(type2):
        return type1
    if type1.equals(type2):
        return type1
    return pa.string()


async def enumerate_datasets(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
) -> list[str]:
    datasets: set[str] = set()
    list_prefix = f"{prefix.rstrip('/')}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(
        Bucket=bucket, Prefix=list_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            if (p := cp.get("Prefix")) is not None:
                dataset_name = p.rstrip("/").split("/")[-1]
                if not dataset_name.startswith("annotations"):
                    datasets.add(dataset_name)
    return sorted(datasets)


async def enumerate_batches(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str,
) -> list[str]:
    batches: set[str] = set()
    dataset_prefix = f"{prefix}/{dataset_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(
        Bucket=bucket, Prefix=dataset_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            if (p := cp.get("Prefix")) is not None:
                batch_name = p.rstrip("/").split("/")[-1]
                # Skip annotations and index partitions (_index) — not batches.
                if not batch_name.startswith("annotations") and not batch_name.startswith("_"):
                    batches.add(batch_name)
    return sorted(batches)


async def enumerate_annotators(
    s3_client: "S3Client", bucket: str, prefix: str, dataset_name: str
) -> list[str]:
    annotators: set[str] = set()
    annotations_prefix = f"{prefix}/{dataset_name}/annotations/"
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(
        Bucket=bucket, Prefix=annotations_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            if (p := cp.get("Prefix")) is not None:
                annotator_name = p.rstrip("/").split("/")[-1]
                annotators.add(annotator_name)
    return sorted(annotators)


async def enumerate_merged_paths(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str,
    annotator: str | None = None,
) -> list[str]:
    """S3 URIs of a dataset's merged files (``merged.parquet`` or JSONL blocks)."""
    return await _enumerate_merged_paths(
        s3_client, bucket, prefix, dataset_name, annotator
    )


def enumerate_merged_paths_sync(
    s3_client: Any,
    bucket: str,
    prefix: str,
    dataset_name: str,
    annotator: str | None = None,
) -> list[str]:
    search_prefix = f"{prefix}/{dataset_name}/"
    if annotator:
        search_prefix += f"annotations/{annotator}/"

    paths: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if _is_merged_file(key, filename, annotator):
                paths.append(f"s3://{bucket}/{key}")
    return sorted(paths)


async def _enumerate_merged_paths(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str,
    annotator: str | None = None,
) -> list[str]:
    search_prefix = f"{prefix}/{dataset_name}/"
    if annotator:
        search_prefix += f"annotations/{annotator}/"

    paths: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if _is_merged_file(key, filename, annotator):
                paths.append(f"s3://{bucket}/{key}")
    return sorted(paths)


def _is_merged_file(key: str, filename: str, annotator: str | None) -> bool:
    if filename == "merged.parquet":
        return bool(annotator or "/annotations/" not in key)
    return (
        filename.startswith("merged_")
        and filename.endswith(".jsonl.gz")
        and (bool(annotator) or "/annotations/" not in key)
    )


async def enumerate_parquet_paths(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str,
    annotator: str | None = None,
) -> list[str]:
    search_prefix = f"{prefix}/{dataset_name}/"
    if annotator:
        search_prefix += f"annotations/{annotator}/"

    paths: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/merged.parquet") and (
                annotator or "/annotations/" not in key
            ):
                paths.append(f"s3://{bucket}/{key}")
    return sorted(paths)


def enumerate_parquet_paths_sync(
    s3_client: Any,
    bucket: str,
    prefix: str,
    dataset_name: str,
    annotator: str | None = None,
) -> list[str]:
    search_prefix = f"{prefix}/{dataset_name}/"
    if annotator:
        search_prefix += f"annotations/{annotator}/"

    paths: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/merged.parquet") and (
                annotator or "/annotations/" not in key
            ):
                paths.append(f"s3://{bucket}/{key}")
    return sorted(paths)


async def iter_jsonl_rows(
    s3_client: "S3Client", bucket: str, keys: list[str]
) -> AsyncIterator[dict[str, Any]]:
    for key in keys:
        try:
            response = await s3_client.get_object(Bucket=bucket, Key=key)
            body = await response["Body"].read()
            text = body.decode("utf-8")
            for line in text.strip().split("\n"):
                if line.strip():
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping corrupted line in {key}: {e}")
        except Exception as e:
            logger.warning(f"Failed to read JSONL {key}: {e}")


async def iter_parquet_rows(
    s3_client: "S3Client", bucket: str, key: str
) -> AsyncIterator[dict[str, Any]]:
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        buf = io.BytesIO(body)
        table = pq.read_table(buf)
        for row in table.to_pylist():
            yield row
    except Exception as e:
        logger.warning(f"Failed to read parquet {key}: {e}")


async def read_jsonl_rows(
    s3_client: "S3Client", bucket: str, keys: list[str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in keys:
        try:
            response = await s3_client.get_object(Bucket=bucket, Key=key)
            body = await response["Body"].read()
            text = body.decode("utf-8")
            for line in text.strip().split("\n"):
                if line.strip():
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping corrupted line in {key}: {e}")
        except Exception as e:
            logger.warning(f"Failed to read JSONL {key}: {e}")
    return rows


async def read_parquet_rows(
    s3_client: "S3Client", bucket: str, key: str
) -> list[dict[str, Any]]:
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        buf = io.BytesIO(body)
        table = pq.read_table(buf)
        return table.to_pylist()
    except Exception as e:
        logger.warning(f"Failed to read parquet {key}: {e}")
        return []


class MergeCandidate(BaseModel):
    jsonl_keys: list[str] = []
    manifest_keys: list[str] = []
    existing_parquet_key: str | None = None
    existing_block_keys: list[str] = []
    deduplicate_on: list[str] = []
    merged_schema: pa.Schema | None = None

    model_config = {"arbitrary_types_allowed": True}


async def discover_schema(
    s3_client: "S3Client", bucket: str, candidate: MergeCandidate
) -> pa.Schema:
    all_columns: dict[str, pa.DataType] = {}

    for key in candidate.jsonl_keys:
        async for row in iter_jsonl_rows(s3_client, bucket, [key]):
            for col_name, value in row.items():
                inferred_type = TYPE_TO_PYARROW.get(type(value), pa.string())
                if col_name in all_columns:
                    all_columns[col_name] = merge_types(
                        all_columns[col_name], inferred_type
                    )
                else:
                    all_columns[col_name] = inferred_type

    if candidate.existing_parquet_key:
        try:
            response = await s3_client.get_object(
                Bucket=bucket, Key=candidate.existing_parquet_key
            )
            body = await response["Body"].read()
            buf = io.BytesIO(body)
            existing_schema = pq.read_schema(buf)
            for field in existing_schema:
                if field.name in all_columns:
                    all_columns[field.name] = merge_types(
                        all_columns[field.name], field.type
                    )
                else:
                    all_columns[field.name] = field.type
        except Exception as e:
            logger.warning(
                f"Failed to read parquet schema {candidate.existing_parquet_key}: {e}"
            )

    if not all_columns:
        return pa.schema([])

    return pa.schema(
        [pa.field(name, dtype) for name, dtype in sorted(all_columns.items())]
    )


async def write_parquet(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    row_iter: Iterator[dict[str, Any]],
    schema: pa.Schema,
    batch_size: int = 100_000,
) -> int:
    if len(schema) == 0:
        table = pa.table({})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        await s3_client.put_object(Bucket=bucket, Key=key, Body=buf.read())
        return 0

    buf = io.BytesIO()
    row_count = 0
    with pq.ParquetWriter(buf, schema) as writer:
        for batch_rows in batched_rows(row_iter, batch_size):
            batch_rows = [serialize_complex_values(r) for r in batch_rows]
            batch = pa.RecordBatch.from_pylist(batch_rows, schema=schema)
            writer.write_batch(batch)
            row_count += len(batch_rows)

    buf.seek(0)
    await s3_client.put_object(Bucket=bucket, Key=key, Body=buf.read())
    return row_count


async def async_write_parquet(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    row_iter: AsyncIterator[dict[str, Any]],
    schema: pa.Schema,
    batch_size: int = 100_000,
) -> int:
    row_count = 0

    with tempfile.TemporaryFile(mode="w+b") as f:
        if len(schema) == 0:
            table = pa.table({})
            pq.write_table(table, f)
            f.seek(0)
            await s3_client.put_object(Bucket=bucket, Key=key, Body=f)
            return 0

        with pq.ParquetWriter(f, schema) as writer:
            async for batch_rows in async_batched_rows(row_iter, batch_size):
                batch_rows = [serialize_complex_values(r) for r in batch_rows]
                batch = pa.RecordBatch.from_pylist(batch_rows, schema=schema)
                writer.write_batch(batch)
                row_count += len(batch_rows)

        f.seek(0)
        await s3_client.put_object(Bucket=bucket, Key=key, Body=f)

    return row_count
