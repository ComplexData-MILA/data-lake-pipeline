"""In-process event bus with Redis fan-in/fan-out for viewer events.

- SSE connections always subscribe to the local bus.
- With Redis configured: events arriving on the Redis channel are republished
  locally, and locally-published events are fanned out to Redis (so other
  backend replicas see watcher-generated events). Loopback duplicates are
  dropped by event_id.
- Without Redis (or with Redis down), the local bus alone keeps
  watcher -> SSE flowing.
"""

import asyncio
import json
import logging
import time
from collections import OrderedDict

from .cache import EVENTS_CHANNEL

logger = logging.getLogger(__name__)

_SEEN_MAX = 1024
_SEEN_TTL = 60.0  # seconds


class EventBus:
    def __init__(self, redis_client=None):
        self._subscribers: set[asyncio.Queue] = set()
        self._redis = redis_client
        self._redis_task: asyncio.Task | None = None
        self._seen: OrderedDict[str, float] = OrderedDict()

    async def start(self) -> None:
        if self._redis is not None:
            self._redis_task = asyncio.create_task(self._redis_listener())

    async def stop(self) -> None:
        if self._redis_task is not None:
            self._redis_task.cancel()
            self._redis_task = None

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def _remember(self, event_id: str | None) -> None:
        if not event_id:
            return
        now = time.monotonic()
        self._seen[event_id] = now
        while len(self._seen) > _SEEN_MAX:
            self._seen.popitem(last=False)

    def _was_seen(self, event_id: str | None) -> bool:
        if not event_id:
            return False
        now = time.monotonic()
        stale = [k for k, t in self._seen.items() if now - t > _SEEN_TTL]
        for k in stale:
            self._seen.pop(k, None)
        return event_id in self._seen

    def _publish_local(self, event: dict) -> None:
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    queue.put_nowait(event)
                except Exception:  # noqa: BLE001
                    pass

    async def publish(self, event: dict) -> None:
        """Fan an event out locally and (best-effort) to Redis."""
        self._remember(event.get("event_id"))
        self._publish_local(event)
        if self._redis is not None:
            try:
                await self._redis.publish(
                    EVENTS_CHANNEL, json.dumps(event, default=str)
                )
            except Exception as e:  # noqa: BLE001
                logger.debug(f"redis publish failed: {e}")

    async def _redis_listener(self) -> None:
        while True:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(EVENTS_CHANNEL)
                logger.info("Event bus listening on %s", EVENTS_CHANNEL)
                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    try:
                        event = json.loads(message["data"])
                    except (json.JSONDecodeError, TypeError):
                        continue
                    # Skip events we published ourselves (loopback).
                    if not self._was_seen(event.get("event_id")):
                        self._remember(event.get("event_id"))
                        self._publish_local(event)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning(f"event bus redis listener error, retrying: {e}")
                await asyncio.sleep(3)
