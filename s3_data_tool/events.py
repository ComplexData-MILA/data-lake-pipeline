"""Viewer event publishing for ingestion writers.

Writers publish lifecycle events to a Redis pub/sub channel so viewer backends
can invalidate caches and stream updates to browsers. Publishing is opt-in
(``VIEWER_REDIS_URL``), lazy-imported, and failure-tolerant: with no Redis
configured or reachable, :func:`publish_event` is a silent no-op — the viewer's
S3-watcher covers the gap by polling object listings.

Channel: ``viewer:events``
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

EVENTS_CHANNEL = "viewer:events"

EventType = Literal[
    "rows_ingested",
    "run_completed",
    "batch_merged",
    "annotation_updated",
    "conversion_progress",
]
EventSource = Literal["generator", "clean_up", "s3_watcher"]


class ViewerEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    type: EventType
    dataset: str
    batch: str | None = None
    annotator: str | None = None
    run_id: str | None = None
    row_count: int | None = None
    converted: int | None = None
    total: int | None = None
    prefix: str | None = None
    bucket: str | None = None
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: EventSource


_client: Any = None


def _get_redis_client():
    """Lazily create the shared async Redis client (None when not configured)."""
    global _client
    if _client is not None:
        return _client
    redis_url = os.environ.get("VIEWER_REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis.asyncio as redis
    except ImportError:
        logger.debug("redis package not installed; event publishing disabled")
        return None
    _client = redis.from_url(redis_url, socket_connect_timeout=3)
    return _client


async def publish_event(event: ViewerEvent) -> None:
    """Publish a viewer event to Redis (silent no-op when Redis is unavailable)."""
    try:
        client = _get_redis_client()
        if client is None:
            return
        await client.publish(EVENTS_CHANNEL, event.model_dump_json())
    except Exception as e:  # noqa: BLE001 - event delivery is best-effort
        logger.debug(f"Failed to publish viewer event {event.type}: {e}")
