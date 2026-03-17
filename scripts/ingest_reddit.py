#!/usr/bin/env python
import argparse

from data_lake_pipeline.config import Settings
from data_lake_pipeline.ingestion.sources import fetch_reddit_top_posts
from data_lake_pipeline.ingestion.writer import save_source_posts
from data_lake_pipeline.storage.s3 import S3Storage


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Reddit top posts")
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max posts to fetch (default: 100, 0 for all)",
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

    limit = args.limit if args.limit > 0 else None
    posts = fetch_reddit_top_posts(limit=limit, use_example_data=settings.use_example_source_data)
    written = save_source_posts("reddit", posts, storage=storage, landing_prefix=settings.landing_prefix)
    print(f"Wrote {written} Reddit records to landing zone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
