#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from data_lake_pipeline.logging_utils import configure_logging
from data_lake_pipeline.merge_server import MergeServer
from data_lake_pipeline.merge_server.config import MergeServerConfig
from data_lake_pipeline.merge_server.locks import get_lock_info
from data_lake_pipeline.merge_server.merger import BatchMerger

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge server for combining filter annotations into consolidated parquet files."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single merge scan and exit.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between merge scans (default: 60). Ignored if --once is set.",
    )
    parser.add_argument(
        "--max-runtime",
        type=int,
        default=0,
        help="Maximum runtime in seconds before graceful exit (0 = unlimited).",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=10,
        help="Maximum concurrent batch merges (default: 10).",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Merge a specific batch only (skip discovery).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Log level (default: INFO).",
    )

    subparsers = parser.add_subparsers(dest="command", help="Additional commands")

    status_parser = subparsers.add_parser(
        "status", help="Show merge status for a batch"
    )
    status_parser.add_argument("--batch-id", required=True, help="Batch ID to check")
    status_parser.add_argument(
        "--log-level", type=str, default="INFO", help="Log level (default: INFO)"
    )

    unlock_parser = subparsers.add_parser(
        "unlock", help="Release a stuck lock on a batch"
    )
    unlock_parser.add_argument("--batch-id", required=True, help="Batch ID to unlock")
    unlock_parser.add_argument(
        "--force",
        action="store_true",
        help="Force release even if merge in progress",
    )
    unlock_parser.add_argument(
        "--log-level", type=str, default="INFO", help="Log level (default: INFO)"
    )

    args = parser.parse_args()

    configure_logging(args.log_level)

    if args.command == "status":
        return asyncio.run(_run_status(args))
    elif args.command == "unlock":
        return asyncio.run(_run_unlock(args))

    config = MergeServerConfig.from_env()
    config = MergeServerConfig(
        s3_bucket=config.s3_bucket,
        s3_prefix=config.s3_prefix,
        s3_endpoint_url=config.s3_endpoint_url,
        s3_access_key=config.s3_access_key,
        s3_secret_key=config.s3_secret_key,
        merge_interval_seconds=args.interval,
        max_concurrent_merges=args.max_concurrent,
        max_runtime_seconds=args.max_runtime,
        lock_ttl_seconds=config.lock_ttl_seconds,
        annotations_prefix=config.annotations_prefix,
        lock_prefix=config.lock_prefix,
    )

    server = MergeServer(config)

    if args.batch_id:
        asyncio.run(server.merge_single_batch(args.batch_id))
    elif args.once:
        asyncio.run(server.run_once())
    else:
        asyncio.run(server.run())

    return 0


async def _run_status(args) -> int:
    config = MergeServerConfig.from_env()

    from data_lake_pipeline.storage.s3 import S3Storage

    storage = S3Storage(
        bucket=config.s3_bucket,
        prefix=config.s3_prefix,
        endpoint_url=config.s3_endpoint_url,
        access_key=config.s3_access_key,
        secret_key=config.s3_secret_key,
    )

    merger = BatchMerger(
        storage=storage,
        batch_id=args.batch_id,
        annotations_prefix=config.annotations_prefix,
    )

    status = await merger.get_merge_status()

    print(f"Batch ID: {args.batch_id}")
    print(f"Filters to merge: {status.filters_to_merge}")
    print(f"No filters: {status.no_filters}")

    lock_info = await get_lock_info(args.batch_id, storage, config.lock_prefix)
    if lock_info:
        print(f"Lock held by: {lock_info.get('owner')}")
        print(f"Lock age: {lock_info.get('locked_at')}")
    else:
        print("No active lock")

    return 0


async def _run_unlock(args) -> int:
    config = MergeServerConfig.from_env()

    from data_lake_pipeline.storage.s3 import S3Storage

    storage = S3Storage(
        bucket=config.s3_bucket,
        prefix=config.s3_prefix,
        endpoint_url=config.s3_endpoint_url,
        access_key=config.s3_access_key,
        secret_key=config.s3_secret_key,
    )

    lock_key = f"{config.lock_prefix}/{args.batch_id}.json"

    if args.force:
        storage.delete_object(lock_key)
        print(f"Force-released lock for batch {args.batch_id}")
        return 0

    lock_info = await get_lock_info(args.batch_id, storage, config.lock_prefix)
    if not lock_info:
        print(f"No lock found for batch {args.batch_id}")
        return 0

    storage.delete_object(lock_key)
    print(f"Released lock for batch {args.batch_id} (owner: {lock_info.get('owner')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
