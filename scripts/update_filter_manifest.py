#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import datetime, timezone

import pandas as pd

from data_lake_pipeline.config import Settings
from data_lake_pipeline.logging_utils import configure_logging
from data_lake_pipeline.storage.s3 import S3Storage

logger = logging.getLogger(__name__)

ANNOTATIONS_PREFIX = "annotations"


def update_filter_manifest(settings: Settings, storage: S3Storage) -> int:
    filters = set()
    batch_stats = []
    failures = 0
    total_batches = 0

    for key in storage.list_objects(ANNOTATIONS_PREFIX, "merged.parquet"):
        total_batches += 1
        try:
            data = storage.read_bytes(key)
            df = pd.read_parquet(io.BytesIO(data))

            batch_filters = [
                col.rsplit("_passed", 1)[0]
                for col in df.columns
                if col.endswith("_passed")
            ]
            filters.update(batch_filters)

            parts = key.split("/")
            batch_id = parts[1] if len(parts) > 1 else None
            if batch_id:
                batch_stats.append(
                    {
                        "batch_id": batch_id,
                        "record_count": len(df),
                        "filters": batch_filters,
                    }
                )
        except Exception as e:
            failures += 1
            logger.warning("Failed to read %s: %s", key, e)

    if total_batches > 0 and len(batch_stats) == 0:
        logger.error(
            "Critical failure: no batches processed successfully out of %d",
            total_batches,
        )
        return 1

    manifest = {
        "filters": sorted(filters),
        "batch_count": len(batch_stats),
        "total_record_count": sum(b["record_count"] for b in batch_stats),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "batches": batch_stats,
    }

    manifest_key = f"{ANNOTATIONS_PREFIX}/filter_manifest.json"
    storage.put_json(manifest_key, manifest)

    logger.info(
        "Updated filter manifest: %d filters, %d batches",
        len(filters),
        len(batch_stats),
    )

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Update filter manifest for viewer")
    parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level)

    storage = S3Storage(
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    return update_filter_manifest(settings, storage)


if __name__ == "__main__":
    sys.exit(main())
