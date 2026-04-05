import asyncio
import hashlib
import json
import logging
import os
from typing import TYPE_CHECKING, Any, AsyncIterator

from aioboto3 import Session

from .async_utils import chain_async_iterators
from .models import RunManifest
from .s3_lock import S3Lock
from .s3_utils import (
    MergeCandidate,
    async_write_parquet,
    delete_objects,
    discover_schema,
    enumerate_annotators,
    enumerate_batches,
    enumerate_datasets,
    iter_jsonl_rows,
    iter_parquet_rows,
)

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)


async def async_deduplicate_rows(
    rows: AsyncIterator[dict[str, Any]], columns: list[str]
) -> AsyncIterator[dict[str, Any]]:
    if not columns:
        async for row in rows:
            yield row
        return

    seen: set[str] = set()
    async for row in rows:
        key = compute_sha_key(row, columns)
        if key not in seen:
            seen.add(key)
            yield row


async def discover_merge_candidate(
    s3_client: "S3Client", bucket: str, batch_prefix: str
) -> MergeCandidate:
    candidate = MergeCandidate()
    all_dedup_sets: list[set[str]] = []

    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=batch_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]  # type: ignore
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
    result = []
    for row in rows:
        key = compute_sha_key(row, columns)
        if key not in seen:
            seen.add(key)
            result.append(row)
    return result


async def merge_dataset_batch(
    s3_client: "S3Client",
    bucket: str,
    batch_prefix: str,
) -> bool:
    candidate = await discover_merge_candidate(s3_client, bucket, batch_prefix)

    if not candidate.jsonl_keys and not candidate.existing_parquet_key:
        logger.info(f"No files to merge for {batch_prefix}")
        return True

    schema = await discover_schema(s3_client, bucket, candidate)

    row_iter = chain_async_iterators(
        iter_jsonl_rows(s3_client, bucket, candidate.jsonl_keys),
        (
            iter_parquet_rows(s3_client, bucket, candidate.existing_parquet_key)
            if candidate.existing_parquet_key
            else None
        ),
    )

    dedup_iter = async_deduplicate_rows(row_iter, candidate.deduplicate_on)

    output_key = f"{batch_prefix}/merged.parquet"
    deduped_count = await async_write_parquet(
        s3_client, bucket, output_key, dedup_iter, schema
    )

    keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys
    if candidate.existing_parquet_key and candidate.existing_parquet_key != output_key:
        keys_to_delete.append(candidate.existing_parquet_key)

    await delete_objects(s3_client, bucket, keys_to_delete)

    logger.info(
        f"Merged {batch_prefix}: {len(candidate.jsonl_keys)} JSONL files, "
        f"-> {deduped_count} deduplicated rows"
    )
    return True


async def merge_annotation_batch(
    s3_client: "S3Client",
    bucket: str,
    batch_prefix: str,
) -> bool:
    candidate = await discover_merge_candidate(s3_client, bucket, batch_prefix)

    if not candidate.jsonl_keys and not candidate.existing_parquet_key:
        logger.info(f"No files to merge for {batch_prefix}")
        return True

    schema = await discover_schema(s3_client, bucket, candidate)

    row_iter = chain_async_iterators(
        iter_jsonl_rows(s3_client, bucket, candidate.jsonl_keys),
        (
            iter_parquet_rows(s3_client, bucket, candidate.existing_parquet_key)
            if candidate.existing_parquet_key
            else None
        ),
    )

    output_key = f"{batch_prefix}/merged.parquet"
    row_count = await async_write_parquet(
        s3_client, bucket, output_key, row_iter, schema
    )

    keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys
    if candidate.existing_parquet_key and candidate.existing_parquet_key != output_key:
        keys_to_delete.append(candidate.existing_parquet_key)

    await delete_objects(s3_client, bucket, keys_to_delete)

    logger.info(
        f"Merged {batch_prefix}: {len(candidate.jsonl_keys)} JSONL files, "
        f"{row_count} total rows"
    )
    return True


async def run_clean_up(
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
            lock = S3Lock(batch_prefix, lock_ttl_ms, s3_client, bucket)
            async with lock:
                if not lock:
                    logger.info(f"Skipping {batch_prefix}: locked by another process")
                    continue
                await merge_dataset_batch(s3_client, bucket, batch_prefix)

        annotators = await enumerate_annotators(s3_client, bucket, prefix, dataset)
        for annotator in annotators:
            annotator_batches = await enumerate_batches(
                s3_client, bucket, f"{prefix}/{dataset}/annotations", annotator
            )
            for annotator_batch in annotator_batches:
                batch_prefix = (
                    f"{prefix}/{dataset}/annotations/{annotator}/{annotator_batch}"
                )
                lock = S3Lock(batch_prefix, lock_ttl_ms, s3_client, bucket)
                async with lock:
                    if not lock:
                        logger.info(
                            f"Skipping {batch_prefix}: locked by another process"
                        )
                        continue
                    await merge_annotation_batch(s3_client, bucket, batch_prefix)


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

    async with session.client("s3", **kwargs) as s3_client:  # type: ignore
        await run_clean_up(s3_client, bucket, prefix)


def main() -> None:
    asyncio.run(_main())
