#!/usr/bin/env python
import argparse

from data_lake_pipeline.config import Settings
from data_lake_pipeline.ingestion.sources import fetch_x_community_notes
from data_lake_pipeline.ingestion.writer import save_source_posts
from data_lake_pipeline.storage.s3 import S3Storage


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest X Community Notes")
    parser.add_argument(
        "--limit",
        type=int,
        help="Max notes to fetch (default: 1000, 0 for all)",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    storage = S3Storage(
        settings.s3_bucket,
        settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    cache_prefix = "00_cache/x_community_notes"

    limit = args.limit if args.limit > 0 else None
    posts, stats = fetch_x_community_notes(
        limit=limit,
        use_example_data=settings.use_example_source_data,
        storage=storage,
        cache_prefix=cache_prefix,
        incremental=True,
        backfill_days=10,
    )

    written = save_source_posts("x_community_notes", posts, storage=storage, landing_prefix=settings.landing_prefix)

    print(f"Wrote {written} X Community Notes records to landing zone.")
    print(f"Stats: {stats.to_dict()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
