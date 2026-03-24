from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_lake_pipeline.storage.s3 import S3Storage

logger = logging.getLogger(__name__)


async def acquire_merge_lock(
    batch_id: str,
    storage: S3Storage,
    lock_prefix: str,
    owner: str,
    ttl_seconds: int = 300,
) -> bool:
    lock_key = f"{lock_prefix}/{batch_id}.json"
    lock_data = {
        "owner": owner,
        "locked_at": int(time.time()),
        "ttl": ttl_seconds,
    }

    existing = storage.get_json(lock_key)
    if existing:
        locked_at = existing.get("locked_at", 0)
        age = time.time() - locked_at
        if age < ttl_seconds:
            return False

    success = storage.put_json(lock_key, lock_data, if_none_match=False)
    if success:
        logger.debug("Acquired merge lock for batch %s (owner=%s)", batch_id, owner)
    return success


async def release_merge_lock(
    batch_id: str,
    storage: S3Storage,
    lock_prefix: str,
    owner: str,
) -> bool:
    lock_key = f"{lock_prefix}/{batch_id}.json"
    existing = storage.get_json(lock_key)

    if not existing:
        return True

    if existing.get("owner") != owner:
        logger.warning(
            "Attempted to release lock for batch %s with wrong owner (expected=%s, got=%s)",
            batch_id,
            existing.get("owner"),
            owner,
        )
        return False

    storage.delete_object(lock_key)
    logger.debug("Released merge lock for batch %s (owner=%s)", batch_id, owner)
    return True


async def get_lock_info(
    batch_id: str,
    storage: S3Storage,
    lock_prefix: str,
) -> dict | None:
    lock_key = f"{lock_prefix}/{batch_id}.json"
    return storage.get_json(lock_key)
