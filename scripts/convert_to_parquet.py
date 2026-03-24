#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

from data_lake_pipeline.config import Settings
from data_lake_pipeline.io_async import stream_jsonl_async, write_parquet_async
from data_lake_pipeline.logging_utils import configure_logging
from data_lake_pipeline.state import StageAwareBatchState
from data_lake_pipeline.storage.s3 import S3Storage

logger = logging.getLogger(__name__)


async def convert_batch(
    settings: Settings,
    storage: S3Storage,
    input_key: str,
    output_key: str,
) -> int:
    records = []
    count = 0
    batch_size = 10000

    async for record in stream_jsonl_async(
        settings.s3_bucket,
        f"{settings.s3_prefix}/{input_key}",
        settings.s3_endpoint_url,
    ):
        records.append(record)
        count += 1

        if len(records) >= batch_size:
            await write_parquet_async(
                settings.s3_bucket,
                f"{settings.s3_prefix}/{output_key}",
                records,
            )
            records = []

    if records:
        await write_parquet_async(
            settings.s3_bucket,
            f"{settings.s3_prefix}/{output_key}",
            records,
        )

    return count


async def convert_stage_jsonl_to_parquet(
    settings: Settings,
    stage_name: str,
    min_age_hours: int = 1,
) -> None:
    storage = S3Storage(
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    state = StageAwareBatchState(storage, stage_name)

    for manifest in state.list_all():
        if manifest.state != "completed":
            continue

        if manifest.merged_passed_key:
            continue

        age_seconds = (
            datetime.now(timezone.utc) - datetime.fromisoformat(manifest.created_at)
        ).total_seconds()

        if age_seconds < min_age_hours * 3600:
            continue

        if manifest.output_key_passed:
            input_key = manifest.output_key_passed.replace(f"{settings.s3_prefix}/", "")
            output_key = input_key.replace(".jsonl", ".parquet")

            logger.info(
                "Converting batch %s passed records: %s -> %s",
                manifest.batch_id,
                input_key,
                output_key,
            )

            count = await convert_batch(settings, storage, input_key, output_key)

            logger.info(
                "Converted %d records for batch %s",
                count,
                manifest.batch_id,
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert JSONL stage outputs to Parquet"
    )
    parser.add_argument("--stage", required=True, help="Stage name to convert")
    parser.add_argument(
        "--min-age-hours",
        type=int,
        default=1,
        help="Minimum age for JSONL files to convert",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level)

    asyncio.run(
        convert_stage_jsonl_to_parquet(settings, args.stage, args.min_age_hours)
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
