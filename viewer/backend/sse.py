"""SSE endpoint bridging viewer events to browsers."""

import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import Query, Request
from sse_starlette.sse import EventSourceResponse

logger = logging.getLogger(__name__)


def _sse_format(event_type: str, payload: dict) -> dict:
    return {
        "event": event_type,
        "data": json.dumps(payload, default=str),
        "id": payload.get("event_id"),
    }


async def events_handler(request: Request, dataset: str = Query(default="")):
    """GET /events — stream viewer events for a dataset (SSE).

    Always subscribes to the in-process bus; the bus bridges Redis when
    configured. The first event is ``connected`` — clients treat reconnect
    completion as a signal to refetch (events may have been missed offline).
    """
    bus = request.app.state.bus
    watcher = request.app.state.watcher

    queue = bus.subscribe()
    watcher.add_subscriber(dataset)

    async def gen():
        try:
            yield _sse_format(
                "connected",
                {"dataset": dataset, "ts": datetime.now(timezone.utc).isoformat()},
            )
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    continue  # heartbeats handled by EventSourceResponse ping
                if dataset and event.get("dataset") != dataset:
                    continue
                yield _sse_format(event["type"], event)
        finally:
            bus.unsubscribe(queue)
            watcher.remove_subscriber(dataset)

    return EventSourceResponse(gen(), ping=15)
