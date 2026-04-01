import asyncio
import hashlib
import io
import json
import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from aioboto3 import Session

from .models import RunManifest
from .s3_lock import S3Lock
from .s3_utils import delete_objects

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)


@dataclass
class MergeCandidate:
    jsonl_keys: list[str] = field(default_factory=list)
    manifest_keys: list[str] = field(default_factory=list)
    existing_parquet_key: str | None = None
    deduplicate_on: list[str] = field(default_factory=list)


async def enumerate_datasets(s3_client: "S3Client", bucket: str, prefix: str) -> list[str]:
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
    s3_client: "S3Client", bucket: str, prefix: str, dataset_name: str
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
                if not batch_name.startswith("annotations"):
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


async def discover_merge_candidate(
    s3_client: "S3Client", bucket: str, batch_prefix: str
) -> MergeCandidate:
    candidate = MergeCandidate()
    all_dedup_sets: list[set[str]] = []

    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=batch_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]

            if filename.endswith(".jsonl"):
                candidate.jsonl_keys.append(key)
            elif filename.endswith(".manifest.json"):
                candidate.manifest_keys.append(key)
                try:
                    response = await s3_client.get_object(Bucket=bucket, Key=key)
                    body = await response["Body"].read()
                    manifest_data = json.loads(body)
                    manifest = RunManifest(**manifest_data)
                    if manifest.deduplicate_on:
                        all_dedup_sets.append(set(manifest.deduplicate_on))
                except Exception as e:
                    logger.warning(f"Failed to read manifest {key}: {e}")
            elif filename == "merged.parquet":
                candidate.existing_parquet_key = key

    if all_dedup_sets:
        candidate.deduplicate_on = sorted(set.intersection(*all_dedup_sets))

    return candidate


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


def compute_sha_key(row: dict[str, Any], columns: list[str]) -> str:
    values = []
    for col in columns:
        val = row.get(col)
        if val is None:
            values.append("null")
        else:
            values.append(json.dumps(val, sort_keys=True))
    combined = "|".join(values)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def deduplicate_rows(
    rows: list[dict[str, Any]], columns: list[str]
) -> list[dict[str, Any]]:
    if not columns:
        return rows

    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = compute_sha_key(row, columns)
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


async def write_parquet(
    s3_client: "S3Client", bucket: str, key: str, rows: list[dict[str, Any]]
) -> None:
    if not rows:
        table = pa.table({})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)
        await s3_client.put_object(Bucket=bucket, Key=key, Body=buf.read())
        return

    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    await s3_client.put_object(Bucket=bucket, Key=key, Body=buf.read())


async def merge_dataset_batch(
    s3_client: "S3Client",
    bucket: str,
    batch_prefix: str,
    lock_ttl_ms: int = 3_600_000,
) -> bool:
    lock = S3Lock(batch_prefix, lock_ttl_ms, s3_client, bucket)
    async with lock:
        if not lock:
            logger.info(f"Skipping {batch_prefix}: locked by another process")
            return False

        candidate = await discover_merge_candidate(s3_client, bucket, batch_prefix)

        if not candidate.jsonl_keys and not candidate.existing_parquet_key:
            logger.info(f"No files to merge for {batch_prefix}")
            return True

        rows = await read_jsonl_rows(s3_client, bucket, candidate.jsonl_keys)

        if candidate.existing_parquet_key:
            existing_rows = await read_parquet_rows(
                s3_client, bucket, candidate.existing_parquet_key
            )
            rows.extend(existing_rows)

        deduped = deduplicate_rows(rows, candidate.deduplicate_on)

        output_key = f"{batch_prefix}/merged.parquet"
        await write_parquet(s3_client, bucket, output_key, deduped)

        keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys
        if (
            candidate.existing_parquet_key
            and candidate.existing_parquet_key != output_key
        ):
            keys_to_delete.append(candidate.existing_parquet_key)

        await delete_objects(s3_client, bucket, keys_to_delete)

        logger.info(
            f"Merged {batch_prefix}: {len(candidate.jsonl_keys)} JSONL files, "
            f"{len(rows)} rows -> {len(deduped)} deduplicated rows"
        )
        return True


async def merge_annotation_batch(
    s3_client: "S3Client",
    bucket: str,
    batch_prefix: str,
    lock_ttl_ms: int = 3_600_000,
) -> bool:
    lock = S3Lock(batch_prefix, lock_ttl_ms, s3_client, bucket)
    async with lock:
        if not lock:
            logger.info(f"Skipping {batch_prefix}: locked by another process")
            return False

        candidate = await discover_merge_candidate(s3_client, bucket, batch_prefix)

        if not candidate.jsonl_keys and not candidate.existing_parquet_key:
            logger.info(f"No files to merge for {batch_prefix}")
            return True

        rows = await read_jsonl_rows(s3_client, bucket, candidate.jsonl_keys)

        if candidate.existing_parquet_key:
            existing_rows = await read_parquet_rows(
                s3_client, bucket, candidate.existing_parquet_key
            )
            rows.extend(existing_rows)

        output_key = f"{batch_prefix}/merged.parquet"
        await write_parquet(s3_client, bucket, output_key, rows)

        keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys
        if (
            candidate.existing_parquet_key
            and candidate.existing_parquet_key != output_key
        ):
            keys_to_delete.append(candidate.existing_parquet_key)

        await delete_objects(s3_client, bucket, keys_to_delete)

        logger.info(
            f"Merged {batch_prefix}: {len(candidate.jsonl_keys)} JSONL files, "
            f"{len(rows)} total rows"
        )
        return True


async def run_cleanup(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    lock_ttl_ms: int = 3_600_000,
) -> None:
    datasets = await enumerate_datasets(s3_client, bucket, prefix)
    logger.info(f"Found {len(datasets)} datasets")

    for dataset in datasets:
        batches = await enumerate_batches(s3_client, bucket, prefix, dataset)
        logger.info(f"Dataset {dataset}: {len(batches)} batches")

        for batch in batches:
            batch_prefix = f"{prefix}/{dataset}/{batch}"
            await merge_dataset_batch(s3_client, bucket, batch_prefix, lock_ttl_ms)

        annotators = await enumerate_annotators(s3_client, bucket, prefix, dataset)
        for annotator in annotators:
            annotator_batches = await enumerate_batches(
                s3_client, bucket, f"{prefix}/{dataset}/annotations", annotator
            )
            for annotator_batch in annotator_batches:
                batch_prefix = (
                    f"{prefix}/{dataset}/annotations/{annotator}/{annotator_batch}"
                )
                await merge_annotation_batch(
                    s3_client, bucket, batch_prefix, lock_ttl_ms
                )


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "datasets")
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")

    session = Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with session.client("s3", **kwargs) as s3_client:
        await run_cleanup(s3_client, bucket, prefix)


def main() -> None:
    asyncio.run(_main())
