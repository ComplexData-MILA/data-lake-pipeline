"""Migration of legacy ``merged.parquet`` files to the viewer's JSONL-block
format (see :mod:`s3_data_tool.jsonl_merge`).

Run as a cron (or one-off) alongside ``s3-data-tool-clean-up``:

- Datasets whose total merged size is at/over ``CONVERT_MAX_DATASET_BYTES``
  (default 10 GB) are skipped and marked ``oversized`` — they stay on parquet
  and the viewer keeps reading them.
- Every batch with a ``merged.parquet`` (base batches and annotation batches)
  is converted inside the same per-batch ``S3Lock`` clean_up uses, so the two
  jobs never race. Conversion reads the parquet via DuckDB (spilling to disk),
  writes id-sorted blocks, updates the batch index + meta, then deletes the
  parquet. Idempotent and resumable: a batch left with both blocks and
  parquet (crash mid-conversion) is redone from scratch.
- ``{prefix}/{dataset}/_migration/status.json`` is rewritten after every
  batch so the viewer can show conversion progress while the job runs.
"""

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from aioboto3 import Session

from .events import ViewerEvent, publish_event
from .index import update_batch_index
from .jsonl_merge import (
    dataset_merged_size,
    parquet_to_jsonl_blocks,
    publish_blocks,
)
from .s3_lock import S3Lock
from .s3_utils import (
    delete_objects,
    enumerate_annotators,
    enumerate_batches,
    enumerate_datasets,
)

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)

CONVERT_MAX_DATASET_BYTES = int(
    os.environ.get("CONVERT_MAX_DATASET_BYTES", str(10_000_000_000))
)
CONVERT_LOCK_TTL_MS = int(os.environ.get("CONVERT_LOCK_TTL_MS", "3600000"))


def migration_status_key(prefix: str, dataset: str) -> str:
    return f"{prefix.rstrip('/')}/{dataset}/_migration/status.json"


async def _list_batch_merged(
    s3_client: "S3Client", bucket: str, batch_prefix: str
) -> tuple[str | None, list[str]]:
    """Return (merged.parquet key, merged block keys) for one batch."""
    parquet_key: str | None = None
    block_keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=batch_prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if filename == "merged.parquet":
                parquet_key = key
            elif filename.startswith("merged_") and filename.endswith(".jsonl.gz"):
                block_keys.append(key)
    return parquet_key, sorted(block_keys)


async def _read_status(
    s3_client: "S3Client", bucket: str, key: str
) -> dict[str, Any] | None:
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        return json.loads(body)
    except Exception:
        return None


async def _write_status(
    s3_client: "S3Client", bucket: str, key: str, status: dict[str, Any]
) -> None:
    await s3_client.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(status).encode("utf-8")
    )


def _empty_status(dataset: str, oversized: bool = False) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "dataset": dataset,
        "total_batches": 0,
        "converted": 0,
        "in_progress_batch": None,
        "annotation_total": 0,
        "annotation_converted": 0,
        "oversized": oversized,
        "started_at": now,
        "updated_at": now,
        "error": None,
    }


async def _convert_merged_parquet(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset: str,
    batch: str,
    batch_prefix: str,
    annotator: str | None = None,
) -> int | None:
    """Convert one batch's merged.parquet to JSONL blocks. Returns row count.

    Called inside the batch's S3Lock. ``None`` when there is nothing to do.
    """
    parquet_key, block_keys = await _list_batch_merged(s3_client, bucket, batch_prefix)
    if parquet_key is None:
        return None

    # Crash recovery: blocks published but parquet not deleted -> redo.
    if block_keys:
        logger.info(f"Redoing partial conversion of {batch_prefix}")
        await delete_objects(s3_client, bucket, block_keys)

    result = await asyncio.to_thread(
        parquet_to_jsonl_blocks,
        bucket,
        prefix,
        dataset,
        batch,
        parquet_key,
        annotator,
    )

    temp_keys = await publish_blocks(s3_client, bucket, result["blocks"])

    if annotator is None:
        try:
            await update_batch_index(
                s3_client,
                bucket,
                prefix,
                dataset,
                batch,
                merged_jsonl_glob=f"{batch_prefix}/merged_*.jsonl.gz",
                blocks=result["blocks"],
            )
        except Exception as e:
            logger.warning(f"Failed to update index for {batch_prefix}: {e}")

    await delete_objects(s3_client, bucket, temp_keys + [parquet_key])
    return result["row_count"]


async def _convert_dataset(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset: str,
    max_bytes: int,
) -> None:
    status_key = migration_status_key(prefix, dataset)

    total_size = await dataset_merged_size(s3_client, bucket, prefix, dataset)
    if total_size > max_bytes:
        status = await _read_status(s3_client, bucket, status_key) or _empty_status(
            dataset
        )
        if not status.get("oversized"):
            status.update(_empty_status(dataset, oversized=True))
            await _write_status(s3_client, bucket, status_key, status)
            logger.info(
                f"Skipping {dataset}: {total_size} bytes >= {max_bytes} limit"
            )
        return

    batches = await enumerate_batches(s3_client, bucket, prefix, dataset)
    annotators = await enumerate_annotators(s3_client, bucket, prefix, dataset)

    status = _empty_status(dataset)
    status["total_batches"] = len(batches)
    annotation_total = 0
    for annotator in annotators:
        annotation_total += len(
            await enumerate_batches(
                s3_client, bucket, f"{prefix}/{dataset}/annotations", annotator
            )
        )
    status["annotation_total"] = annotation_total

    async def _convert_one(batch: str, batch_prefix: str, annotator: str | None):
        lock = S3Lock(batch_prefix, CONVERT_LOCK_TTL_MS, s3_client, bucket)
        async with lock:
            if not lock:
                logger.info(f"Skipping {batch_prefix}: locked by another process")
                return
            await _convert_merged_parquet(
                s3_client, bucket, prefix, dataset, batch, batch_prefix, annotator
            )

    for batch in batches:
        batch_prefix = f"{prefix}/{dataset}/{batch}"
        status["in_progress_batch"] = batch
        status["updated_at"] = datetime.now(timezone.utc).isoformat()
        await _write_status(s3_client, bucket, status_key, status)
        try:
            await _convert_one(batch, batch_prefix, None)
            status["converted"] += 1
        except Exception as e:  # noqa: BLE001 - record and continue
            logger.error(f"Conversion failed for {batch_prefix}: {e}")
            status["error"] = f"{batch}: {e}"
        status["in_progress_batch"] = None
        status["updated_at"] = datetime.now(timezone.utc).isoformat()
        await _write_status(s3_client, bucket, status_key, status)
        await publish_event(ViewerEvent(
            type="conversion_progress",
            dataset=dataset,
            batch=batch,
            converted=status["converted"],
            prefix=prefix,
            bucket=bucket,
            source="clean_up",
        ))

    for annotator in annotators:
        for annotator_batch in await enumerate_batches(
            s3_client, bucket, f"{prefix}/{dataset}/annotations", annotator
        ):
            batch_prefix = (
                f"{prefix}/{dataset}/annotations/{annotator}/{annotator_batch}"
            )
            try:
                await _convert_one(annotator_batch, batch_prefix, annotator)
                status["annotation_converted"] += 1
            except Exception as e:  # noqa: BLE001
                logger.error(f"Conversion failed for {batch_prefix}: {e}")
                status["error"] = f"{annotator}/{annotator_batch}: {e}"
            status["updated_at"] = datetime.now(timezone.utc).isoformat()
            await _write_status(s3_client, bucket, status_key, status)

    status["error"] = None
    status["updated_at"] = datetime.now(timezone.utc).isoformat()
    await _write_status(s3_client, bucket, status_key, status)
    logger.info(
        f"Converted {dataset}: {status['converted']}/{status['total_batches']} "
        f"batches, {status['annotation_converted']}/{status['annotation_total']} "
        f"annotation batches"
    )


async def run_conversion(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    max_bytes: int | None = None,
    datasets: list[str] | None = None,
) -> None:
    max_bytes = CONVERT_MAX_DATASET_BYTES if max_bytes is None else max_bytes
    if datasets is None:
        datasets = await enumerate_datasets(s3_client, bucket, prefix)
        logger.info(f"Found {len(datasets)} datasets")
    for dataset in datasets:
        await _convert_dataset(s3_client, bucket, prefix, dataset, max_bytes)


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert legacy merged.parquet files to JSONL blocks"
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        metavar="DATASET",
        help="Only convert the given dataset. Repeatable. "
        "If omitted, all datasets are processed.",
    )
    args = parser.parse_args()

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
        await run_conversion(
            s3_client,
            bucket,
            prefix,
            datasets=args.dataset if args.dataset else None,
        )


def main() -> None:
    asyncio.run(_main())
