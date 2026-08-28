import argparse
import asyncio
import hashlib
import json
import logging
import os
from typing import TYPE_CHECKING, Any, AsyncIterator

from aioboto3 import Session

from .async_utils import chain_async_iterators
from .events import ViewerEvent, publish_event
from .index import update_batch_index
from .models import RunManifest
from .s3_lock import S3Lock
from .jsonl_merge import (
    JSONL_MERGE_ENABLED,
    JSONL_MERGE_MAX_DATASET_BYTES,
    dataset_merged_size,
    merge_to_jsonl_blocks,
    publish_blocks,
)
from .s3_utils import (
    MergeCandidate,
    annotation_manifest_key,
    async_write_parquet,
    delete_objects,
    discover_schema,
    enumerate_annotators,
    enumerate_batches,
    enumerate_datasets,
    iter_jsonl_rows,
    iter_parquet_rows,
    s3_object_exists,
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
            elif filename.startswith("merged_") and filename.endswith(".jsonl.gz"):
                candidate.existing_block_keys.append(key)

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

    has_merged = bool(candidate.existing_parquet_key or candidate.existing_block_keys)
    if not candidate.jsonl_keys:
        if has_merged:
            logger.debug(f"Skipping {batch_prefix}: already merged (no JSONL files)")
            return True
        logger.debug(f"No files to merge for {batch_prefix}")
        return True

    ann_prefix, dataset, batch = batch_prefix.rsplit("/", 2)

    # Small datasets merge to id-sorted JSONL blocks (the viewer's format);
    # datasets at/over the size threshold keep the legacy parquet merge.
    if (
        JSONL_MERGE_ENABLED
        and await dataset_merged_size(s3_client, bucket, ann_prefix, dataset)
        < JSONL_MERGE_MAX_DATASET_BYTES
    ):
        row_count = await _merge_dataset_batch_jsonl(
            s3_client, bucket, ann_prefix, dataset, batch, batch_prefix, candidate
        )
    else:
        row_count = await _merge_dataset_batch_parquet(
            s3_client, bucket, batch_prefix, candidate
        )

    # New base data invalidates annotation manifests — delete them so
    # annotators re-evaluate this batch next session.
    annotators = await enumerate_annotators(s3_client, bucket, ann_prefix, dataset)
    manifest_keys = []
    for annotator in annotators:
        key = annotation_manifest_key(ann_prefix, dataset, annotator, batch)
        if await s3_object_exists(s3_client, bucket, key):
            manifest_keys.append(key)
    if manifest_keys:
        await delete_objects(s3_client, bucket, manifest_keys)
        logger.info(
            f"Deleted {len(manifest_keys)} annotation manifests for "
            f"{dataset}/{batch}"
        )

    logger.info(
        f"Merged {batch_prefix}: {len(candidate.jsonl_keys)} JSONL files, "
        f"-> {row_count} deduplicated rows"
    )

    await publish_event(ViewerEvent(
        type="batch_merged",
        dataset=dataset,
        batch=batch,
        row_count=row_count,
        prefix=ann_prefix,
        bucket=bucket,
        source="clean_up",
    ))
    return True


async def _merge_dataset_batch_jsonl(
    s3_client: "S3Client",
    bucket: str,
    ann_prefix: str,
    dataset: str,
    batch: str,
    batch_prefix: str,
    candidate: MergeCandidate,
) -> int:
    """Merge a batch into blocked, id-sorted JSONL (the viewer's merged format)."""
    result = await asyncio.to_thread(
        merge_to_jsonl_blocks,
        bucket,
        ann_prefix,
        dataset,
        batch,
        candidate.jsonl_keys,
        candidate.existing_parquet_key,
        candidate.existing_block_keys,
        candidate.deduplicate_on,
    )

    # Publish blocks atomically (copy temp -> final), then drop temps.
    temp_keys = await publish_blocks(s3_client, bucket, result["blocks"])

    # Update the dataset index partition before deleting the chunks (index
    # failures are non-fatal — the viewer falls back to scan-based queries).
    try:
        await update_batch_index(
            s3_client,
            bucket,
            ann_prefix,
            dataset,
            batch,
            merged_jsonl_glob=f"{batch_prefix}/merged_*.jsonl.gz",
            blocks=result["blocks"],
        )
    except Exception as e:
        logger.warning(f"Failed to update index for {batch_prefix}: {e}")

    keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys + temp_keys
    if candidate.existing_parquet_key:
        keys_to_delete.append(candidate.existing_parquet_key)
    await delete_objects(s3_client, bucket, keys_to_delete)
    return result["row_count"]


async def _merge_dataset_batch_parquet(
    s3_client: "S3Client",
    bucket: str,
    batch_prefix: str,
    candidate: MergeCandidate,
) -> int:
    """Legacy merge: chunks + existing parquet -> merged.parquet (big datasets)."""
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
    temp_key = f"{batch_prefix}/merged.parquet.temp"

    deduped_count = await async_write_parquet(
        s3_client, bucket, temp_key, dedup_iter, schema
    )

    await s3_client.copy_object(
        Bucket=bucket, Key=output_key, CopySource={"Bucket": bucket, "Key": temp_key}
    )
    # Update the dataset index partition before deleting the chunks (index
    # failures are non-fatal — the viewer falls back to scan-based queries).
    prefix_parts = batch_prefix.rsplit("/", 2)
    try:
        await update_batch_index(
            s3_client,
            bucket,
            prefix_parts[0],
            prefix_parts[1],
            prefix_parts[2],
        )
    except Exception as e:
        logger.warning(f"Failed to update index for {batch_prefix}: {e}")
    keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys + [temp_key]
    await delete_objects(s3_client, bucket, keys_to_delete)
    return deduped_count


async def merge_annotation_batch(
    s3_client: "S3Client",
    bucket: str,
    batch_prefix: str,
) -> bool:
    candidate = await discover_merge_candidate(s3_client, bucket, batch_prefix)

    has_merged = bool(candidate.existing_parquet_key or candidate.existing_block_keys)
    if not candidate.jsonl_keys:
        if has_merged:
            logger.info(f"Skipping {batch_prefix}: already merged (no JSONL files)")
            return True
        logger.info(f"No files to merge for {batch_prefix}")
        return True

    ann_prefix, dataset, _, annotator, batch = batch_prefix.rsplit("/", 4)

    if (
        JSONL_MERGE_ENABLED
        and await dataset_merged_size(
            s3_client, bucket, ann_prefix, dataset, annotator
        )
        < JSONL_MERGE_MAX_DATASET_BYTES
    ):
        result = await asyncio.to_thread(
            merge_to_jsonl_blocks,
            bucket,
            ann_prefix,
            dataset,
            batch,
            candidate.jsonl_keys,
            candidate.existing_parquet_key,
            candidate.existing_block_keys,
            candidate.deduplicate_on,
            annotator,
        )
        temp_keys = await publish_blocks(s3_client, bucket, result["blocks"])
        keys_to_delete = candidate.jsonl_keys + candidate.manifest_keys + temp_keys
        if candidate.existing_parquet_key:
            keys_to_delete.append(candidate.existing_parquet_key)
        await delete_objects(s3_client, bucket, keys_to_delete)
        row_count = result["row_count"]
    else:
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

    await publish_event(ViewerEvent(
        type="annotation_updated",
        dataset=dataset,
        annotator=annotator,
        batch=batch,
        row_count=row_count,
        bucket=bucket,
        source="clean_up",
    ))
    return True


def _parse_colon_filter(
    raw_values: list[str],
) -> dict[str, set[str]]:
    """Parse a list of ``dataset:value`` strings into ``{dataset: {values}}``."""
    result: dict[str, set[str]] = {}
    for item in raw_values:
        if ":" not in item:
            raise ValueError(
                f"Expected 'dataset:value' format, got '{item}'"
            )
        dataset, value = item.split(":", 1)
        result.setdefault(dataset, set()).add(value)
    return result


async def run_clean_up(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    lock_ttl_ms: int = 3_600_000,
    batches: dict[str, set[str]] | None = None,
    annotators: dict[str, set[str]] | None = None,
) -> None:
    datasets = await enumerate_datasets(s3_client, bucket, prefix)
    logger.info(f"Found {len(datasets)} datasets")

    for dataset in datasets:
        if batches is not None:
            allowed_batches = batches.get(dataset)
            if allowed_batches is not None:
                found = await enumerate_batches(s3_client, bucket, prefix, dataset)
                dataset_batches = [b for b in found if b in allowed_batches]
                logger.info(
                    f"Dataset {dataset}: {len(dataset_batches)}/{len(found)} batches "
                    f"(filtered)"
                )
            else:
                logger.info(
                    f"Dataset {dataset}: skipping batches (not in --batch filter)"
                )
                dataset_batches = []
        else:
            dataset_batches = await enumerate_batches(s3_client, bucket, prefix, dataset)
            logger.info(f"Dataset {dataset}: {len(dataset_batches)} batches")

        for batch in dataset_batches:
            batch_prefix = f"{prefix}/{dataset}/{batch}"
            lock = S3Lock(batch_prefix, lock_ttl_ms, s3_client, bucket)
            async with lock:
                if not lock:
                    logger.info(f"Skipping {batch_prefix}: locked by another process")
                    continue
                await merge_dataset_batch(s3_client, bucket, batch_prefix)

        if annotators is not None:
            allowed_annotators = annotators.get(dataset)
            if allowed_annotators is not None:
                found = await enumerate_annotators(s3_client, bucket, prefix, dataset)
                dataset_annotators = [a for a in found if a in allowed_annotators]
            else:
                logger.info(
                    f"Dataset {dataset}: skipping annotators (not in --annotator filter)"
                )
                dataset_annotators = []
        else:
            dataset_annotators = await enumerate_annotators(
                s3_client, bucket, prefix, dataset
            )

        for annotator in dataset_annotators:
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
    parser = argparse.ArgumentParser(description="Merge JSONL files into Parquet")
    parser.add_argument(
        "--batch",
        action="append",
        default=[],
        metavar="DATASET:BATCH",
        help="Only process the given batch (format: dataset:batch). "
        "Repeatable. If omitted, all batches are processed.",
    )
    parser.add_argument(
        "--annotator",
        action="append",
        default=[],
        metavar="DATASET:ANNOTATOR",
        help="Only process the given annotator (format: dataset:annotator). "
        "Repeatable. If omitted, all annotators are processed.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    batch_filter = _parse_colon_filter(args.batch) if args.batch else None
    annotator_filter = _parse_colon_filter(args.annotator) if args.annotator else None

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
        await run_clean_up(
            s3_client,
            bucket,
            prefix,
            batches=batch_filter,
            annotators=annotator_filter,
        )


def main() -> None:
    asyncio.run(_main())
