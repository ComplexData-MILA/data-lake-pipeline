"""Redis cache for viewer metadata, with event-driven invalidation.

Two client shapes: a sync client for cache-aside inside ``to_thread`` workers
and an async client for the pub/sub invalidation subscriber. Everything
degrades to direct S3 access when Redis is unconfigured or down.
"""

import hashlib
import json
import logging
import os
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

EVENTS_CHANNEL = "viewer:events"

T = TypeVar("T")


def cache_scope() -> str:
    from .duckdb_query import S3_BUCKET, S3_PREFIX

    return f"{S3_BUCKET}:{S3_PREFIX}"


def _hash_key(*parts: Any) -> str:
    payload = json.dumps(parts, sort_keys=True, default=str)
    return hashlib.sha1(payload.encode()).hexdigest()[:16]


def datasets_key() -> str:
    return f"viewer:{cache_scope()}:datasets"


def annotators_key(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:annotators"


def files_key(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:files"


def schema_key(dataset: str, annotators: list[str]) -> str:
    return f"viewer:{cache_scope()}:{dataset}:schema:{_hash_key(sorted(annotators))}"


def schema_keys_set(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:schema_keys"


def count_key(dataset: str, annotator_cols: dict, filter_data: dict) -> str:
    return (
        f"viewer:{cache_scope()}:{dataset}:count:"
        f"{_hash_key(annotator_cols, filter_data)}"
    )


def count_keys_set(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:count_keys"


def index_meta_key(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:index_meta"


def conversion_key(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:conversion"


def activity_key(bucket: str, minutes: int | None) -> str:
    return f"viewer:{cache_scope()}:activity:{_hash_key(bucket, minutes)}"


def activity_keys_set() -> str:
    return f"viewer:{cache_scope()}:activity_keys"


def categorical_key(
    dataset: str, column: str, mode: str, bucket: str, limit: int, minutes: int | None
) -> str:
    return (
        f"viewer:{cache_scope()}:{dataset}:categorical:"
        f"{_hash_key(column, mode, bucket, limit, minutes)}"
    )


def categorical_keys_set(dataset: str) -> str:
    return f"viewer:{cache_scope()}:{dataset}:categorical_keys"


def create_sync_redis():
    """Create the sync Redis client (None when unconfigured/unimportable)."""
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis
    except ImportError:
        logger.warning("redis package not installed; caching disabled")
        return None
    return redis.from_url(redis_url, socket_connect_timeout=2)


def create_async_redis():
    """Create the async Redis client (None when unconfigured/unimportable)."""
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis.asyncio as redis
    except ImportError:
        logger.warning("redis package not installed; pub/sub disabled")
        return None
    return redis.from_url(redis_url, socket_connect_timeout=2)


def cached_sync(
    redis_client,
    key: str,
    ttl: int,
    compute: Callable[[], T],
    encode=json.dumps,
    decode=json.loads,
    register_set: str | None = None,
) -> T:
    """Cache-aside helper for sync (thread) code paths.

    On cache miss runs *compute*, stores the encoded value with TTL, and
    (optionally) registers *key* in a Redis SET for invalidation sweeps.
    Falls back to *compute* directly on any Redis error.
    """
    if redis_client is None:
        return compute()
    try:
        raw = redis_client.get(key)
        if raw is not None:
            return decode(raw)
    except Exception as e:  # noqa: BLE001
        logger.debug(f"redis get failed for {key}: {e}")
    value = compute()
    try:
        redis_client.set(key, encode(value), ex=ttl)
        if register_set is not None:
            redis_client.sadd(register_set, key)
            redis_client.expire(register_set, ttl * 4)
    except Exception as e:  # noqa: BLE001
        logger.debug(f"redis set failed for {key}: {e}")
    return value


async def invalidation_subscriber(redis_client) -> None:
    """Subscribe to viewer:events and delete all affected cache keys.

    ``*_keys`` keys are Redis SETs of per-request cache keys (schema/count
    variants); they are expanded and cleared. Intended as a lifespan task.
    """
    if redis_client is None:
        return
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(EVENTS_CHANNEL)
    logger.info("Cache invalidation subscriber listening on %s", EVENTS_CHANNEL)
    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            try:
                event = json.loads(message["data"])
            except (json.JSONDecodeError, TypeError):
                continue
            keys = invalidate_keys_for_event(event)
            final_keys = await _expand_key_sets(redis_client, keys)
            if final_keys:
                await redis_client.delete(*final_keys)
                logger.debug("invalidated %d keys for %s", len(final_keys), event.get("type"))
    finally:
        await pubsub.unsubscribe(EVENTS_CHANNEL)


def invalidate_keys_for_event(event: dict) -> list[str]:
    """Map a viewer event to the cache keys it invalidates."""
    dataset = event.get("dataset")
    event_type = event.get("type")
    keys = [datasets_key()]
    if not dataset:
        return keys
    base = f"viewer:{cache_scope()}:{dataset}"
    keys.append(files_key(dataset))
    if event_type in ("rows_ingested", "run_completed", "batch_merged"):
        # New base data: counts, schemas (new columns may appear), the
        # dataset list (dataset may have been invisible without parquet),
        # and the base-data chart caches.
        keys.extend(
            [
                schema_keys_set(dataset),
                count_keys_set(dataset),
                activity_keys_set(),
                categorical_keys_set(dataset),
            ]
        )
    if event_type == "batch_merged":
        # Merged files changed: conversion progress may have advanced too.
        keys.extend([annotators_key(dataset), index_meta_key(dataset), conversion_key(dataset)])
    if event_type == "conversion_progress":
        keys.extend([files_key(dataset), conversion_key(dataset)])
    if event_type == "annotation_updated":
        keys.extend(
            [annotators_key(dataset), schema_keys_set(dataset), count_keys_set(dataset)]
        )
    return keys


async def _expand_key_sets(redis_client, keys: list[str]) -> list[str]:
    """Expand ``*_keys`` SET keys into their members plus the set itself."""
    final: list[str] = []
    for key in keys:
        if key.endswith("_keys"):
            try:
                members = await redis_client.smembers(key)
                final.extend(m.decode() for m in members)
            except Exception as e:  # noqa: BLE001
                logger.debug(f"smembers failed for {key}: {e}")
        final.append(key)
    return final
