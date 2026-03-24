#!/usr/bin/env python
"""Migrate S3 objects from numeric stage prefixes to semantic names.

Old prefixes: 01_landing, 02_pending, 03_processed, 04_archive
New prefixes: landing, pending, processed, archive
"""

import argparse
import logging

from data_lake_pipeline.config import Settings
from data_lake_pipeline.logging_utils import configure_logging
from data_lake_pipeline.storage.s3 import S3Storage

logger = logging.getLogger(__name__)

MIGRATIONS = [
    ("01_landing", "landing"),
    ("02_pending", "pending"),
    ("03_processed", "processed"),
    ("04_archive", "archive"),
]


def migrate_prefix(
    storage: S3Storage,
    old_prefix: str,
    new_prefix: str,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Migrate all objects from old_prefix to new_prefix.

    Returns (copied_count, deleted_count).
    """
    objects = storage.list_objects(old_prefix, "")
    if not objects:
        logger.info("No objects found under %s", old_prefix)
        return 0, 0

    copied = 0
    deleted = 0

    for old_key in objects:
        new_key = old_key.replace(old_prefix, new_prefix, 1)

        if dry_run:
            logger.info("[DRY-RUN] Would copy %s -> %s", old_key, new_key)
            copied += 1
            continue

        try:
            storage.copy_object(old_key, new_key)
            logger.info("Copied %s -> %s", old_key, new_key)
            copied += 1
        except Exception as e:
            logger.error("Failed to copy %s: %s", old_key, e)
            continue

    if dry_run:
        logger.info("[DRY-RUN] Would delete %d objects under %s", copied, old_prefix)
        return copied, 0

    for old_key in objects:
        try:
            storage.delete_object(old_key)
            logger.debug("Deleted %s", old_key)
            deleted += 1
        except Exception as e:
            logger.error("Failed to delete %s: %s", old_key, e)

    logger.info("Deleted %d objects under %s", deleted, old_prefix)
    return copied, deleted


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate stage prefixes")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes",
    )
    parser.add_argument(
        "--prefix",
        choices=["landing", "pending", "processed", "archive", "all"],
        default="all",
        help="Which prefix to migrate (default: all)",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level)

    storage = S3Storage(
        settings.s3_bucket,
        settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    migrations = MIGRATIONS
    if args.prefix != "all":
        migrations = [(o, n) for o, n in MIGRATIONS if n == args.prefix]

    total_copied = 0
    total_deleted = 0

    for old_prefix, new_prefix in migrations:
        logger.info("Migrating %s -> %s", old_prefix, new_prefix)
        copied, deleted = migrate_prefix(
            storage, old_prefix, new_prefix, dry_run=args.dry_run
        )
        total_copied += copied
        total_deleted += deleted

    if args.dry_run:
        print(f"\n[DRY-RUN] Would migrate {total_copied} objects")
    else:
        print(f"\nMigrated {total_copied} objects, deleted {total_deleted} old objects")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
