"""Tests for the SSE endpoint and the S3 watcher."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from viewer.backend.events_bus import EventBus
from viewer.backend.sse import events_handler
from viewer.backend.watcher import S3Watcher

pytestmark = pytest.mark.integration


class FakeRequest:
    """Minimal request stand-in for the SSE handler."""

    def __init__(self, state: dict):
        self.app = SimpleNamespace(state=SimpleNamespace(**state))
        self._disconnected = False

    async def is_disconnected(self):
        return self._disconnected

    def disconnect(self):
        self._disconnected = True


class TestEventBus:
    @pytest.mark.asyncio
    async def test_local_fanout_and_unsubscribe(self):
        bus = EventBus(redis_client=None)
        await bus.start()
        q = bus.subscribe()
        event = {"event_id": "e1", "type": "rows_ingested", "dataset": "d"}
        await bus.publish(event)
        received = await asyncio.wait_for(q.get(), timeout=1)
        assert received == event
        bus.unsubscribe(q)
        await bus.publish({"event_id": "e2", "type": "batch_merged"})
        assert q.empty()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_slow_subscriber_drops_oldest(self):
        bus = EventBus(redis_client=None)
        await bus.start()
        q = bus.subscribe()
        # Fill the queue beyond capacity without consuming.
        for i in range(1500):
            await bus.publish({"event_id": f"e{i}", "type": "rows_ingested"})
        assert q.qsize() <= 1000
        await bus.stop()


class TestS3Watcher:
    @pytest.mark.asyncio
    async def test_poll_emits_on_change(self):
        events = []

        class FakeBus:
            async def publish(self, event):
                events.append(event)

        snapshots = [
            {"datasets/d/b1/chunk_00000.jsonl": ("a", 10, "t1")},
            {"datasets/d/b1/chunk_00000.jsonl": ("a", 10, "t1"),
             "datasets/d/b1/chunk_00001.jsonl": ("b", 20, "t2")},
        ]
        calls = {"n": 0}

        def list_fn(dataset):
            snapshot = snapshots[min(calls["n"], len(snapshots) - 1)]
            calls["n"] += 1
            return snapshot

        watcher = S3Watcher("bucket", "datasets", FakeBus(), list_fn)
        watcher.add_subscriber("d")

        await watcher.poll_once()  # baseline
        assert events == []

        await watcher.poll_once()  # new chunk appears
        assert len(events) == 1
        assert events[0]["type"] == "rows_ingested"
        assert events[0]["dataset"] == "d"
        assert events[0]["batch"] == "b1"
        assert events[0]["row_count"] is None

        await watcher.poll_once()  # no change
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_migration_changes_do_not_name_batch(self):
        """_migration/status.json writes must not surface as batch events."""
        events = []

        class FakeBus:
            async def publish(self, event):
                events.append(event)

        snapshots = [
            {"datasets/d/b1/chunk_00000.jsonl": ("a", 10, "t1")},
            {"datasets/d/b1/chunk_00000.jsonl": ("a", 10, "t1"),
             "datasets/d/_migration/status.json": ("b", 20, "t2")},
        ]
        calls = {"n": 0}

        def list_fn(dataset):
            snapshot = snapshots[min(calls["n"], len(snapshots) - 1)]
            calls["n"] += 1
            return snapshot

        watcher = S3Watcher("bucket", "datasets", FakeBus(), list_fn)
        watcher.add_subscriber("d")

        await watcher.poll_once()  # baseline
        assert events == []

        await watcher.poll_once()  # only _migration changed
        assert len(events) == 1
        assert events[0]["batch"] is None  # not "_migration"

    @pytest.mark.asyncio
    async def test_subscriber_refcounts(self):
        watcher = S3Watcher("bucket", "datasets", None, lambda d: {})
        watcher.add_subscriber("d")
        watcher.add_subscriber("d")
        assert watcher.watched_datasets == ["d"]
        watcher.remove_subscriber("d")
        assert watcher.watched_datasets == ["d"]
        watcher.remove_subscriber("d")
        assert watcher.watched_datasets == []

    @pytest.mark.asyncio
    async def test_no_poll_without_subscribers(self):
        events = []

        class FakeBus:
            async def publish(self, event):
                events.append(event)

        def list_fn(dataset):
            raise AssertionError("list_fn must not be called without subscribers")

        watcher = S3Watcher("bucket", "datasets", FakeBus(), list_fn)
        await watcher.poll_once()
        assert events == []

    @pytest.mark.asyncio
    async def test_global_subscriber_polls_all_datasets(self):
        """A '' subscriber expands to the full dataset list via list_datasets_fn."""
        events = []

        class FakeBus:
            async def publish(self, event):
                events.append(event)

        snapshots = {
            "d1": [
                {"datasets/d1/b1/chunk_00000.jsonl": ("a", 10, "t1")},
                {"datasets/d1/b1/chunk_00000.jsonl": ("a", 10, "t1")},
            ],
            "d2": [
                {"datasets/d2/b1/chunk_00000.jsonl": ("a", 10, "t1")},
                {"datasets/d2/b1/chunk_00000.jsonl": ("a", 10, "t1"),
                 "datasets/d2/b1/chunk_00001.jsonl": ("b", 20, "t2")},
            ],
        }
        calls = {"d1": 0, "d2": 0}

        def list_fn(dataset):
            snapshot = snapshots[dataset][min(calls[dataset], 1)]
            calls[dataset] += 1
            return snapshot

        watcher = S3Watcher(
            "bucket", "datasets", FakeBus(), list_fn, list_datasets_fn=lambda: ["d1", "d2"]
        )
        watcher.add_subscriber("")

        await watcher.poll_once()  # baseline for both datasets
        assert events == []

        await watcher.poll_once()  # d2 changed
        assert len(events) == 1
        assert events[0]["dataset"] == "d2"
        assert events[0]["batch"] == "b1"

    @pytest.mark.asyncio
    async def test_global_subscriber_without_list_datasets_fn_skips(self):
        """Without list_datasets_fn a global subscriber polls named datasets only."""
        events = []

        class FakeBus:
            async def publish(self, event):
                events.append(event)

        def list_fn(dataset):
            return {}

        watcher = S3Watcher("bucket", "datasets", FakeBus(), list_fn)
        watcher.add_subscriber("")
        watcher.add_subscriber("d1")

        await watcher.poll_once()
        await watcher.poll_once()
        assert events == []

    @pytest.mark.asyncio
    async def test_global_and_named_subscriber_poll_each_dataset_once(self):
        """The union of the global list and named refcounts is deduped."""
        list_calls = []

        def list_fn(dataset):
            list_calls.append(dataset)
            return {}

        watcher = S3Watcher(
            "bucket", "datasets", None, list_fn, list_datasets_fn=lambda: ["d1", "d2"]
        )
        watcher.add_subscriber("")
        watcher.add_subscriber("d2")
        watcher.add_subscriber("d3")

        await watcher.poll_once()
        assert sorted(list_calls) == ["d1", "d2", "d2", "d3"] or sorted(
            set(list_calls)
        ) == ["d1", "d2", "d3"]

    @pytest.mark.asyncio
    async def test_empty_subscriber_refcount_removed(self):
        watcher = S3Watcher("bucket", "datasets", None, lambda d: {})
        watcher.add_subscriber("")
        assert watcher.watched_datasets == [""]
        watcher.remove_subscriber("")
        assert watcher.watched_datasets == []

    @pytest.mark.asyncio
    async def test_list_datasets_failure_falls_back_to_named(self):
        def list_fn(dataset):
            return {}

        def failing_list():
            raise RuntimeError("s3 down")

        watcher = S3Watcher(
            "bucket", "datasets", None, list_fn, list_datasets_fn=failing_list
        )
        watcher.add_subscriber("")
        watcher.add_subscriber("d1")

        datasets = await watcher._datasets_to_poll()
        assert datasets == ["d1"]


class TestSseHandler:
    @pytest.mark.asyncio
    async def test_connected_and_bus_event_streamed(self):
        """The SSE stream opens with `connected` and forwards bus events."""
        bus = EventBus(redis_client=None)
        await bus.start()
        watcher = S3Watcher("bucket", "datasets", bus, lambda d: {})
        request = FakeRequest(
            {"bus": bus, "watcher": watcher, "loop": asyncio.get_running_loop()}
        )

        response = await events_handler(request, "d1")
        iterator = response.body_iterator

        first = await asyncio.wait_for(iterator.__anext__(), timeout=2)
        assert first["event"] == "connected"
        assert json.loads(first["data"])["dataset"] == "d1"

        await bus.publish(
            {
                "event_id": "evt1",
                "type": "rows_ingested",
                "dataset": "d1",
                "batch": "b1",
                "row_count": 7,
                "source": "generator",
            }
        )
        while True:
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=2)
            if chunk["event"] == "rows_ingested":
                assert json.loads(chunk["data"])["event_id"] == "evt1"
                break

        assert watcher.watched_datasets == ["d1"]
        await iterator.aclose()
        assert watcher.watched_datasets == [], "subscriber must be removed on close"
        await bus.stop()

    @pytest.mark.asyncio
    async def test_other_dataset_events_filtered(self):
        """Events for other datasets are not forwarded."""
        bus = EventBus(redis_client=None)
        await bus.start()
        watcher = S3Watcher("bucket", "datasets", bus, lambda d: {})
        request = FakeRequest(
            {"bus": bus, "watcher": watcher, "loop": asyncio.get_running_loop()}
        )

        response = await events_handler(request, "d1")
        iterator = response.body_iterator
        await asyncio.wait_for(iterator.__anext__(), timeout=2)  # connected

        await bus.publish(
            {"event_id": "e2", "type": "batch_merged", "dataset": "other"}
        )
        request.disconnect()
        # The generator checks is_disconnected before the queue wait, so the
        # next __anext__ raises StopAsyncIteration after the disconnect flag.
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(iterator.__anext__(), timeout=2)
        await iterator.aclose()
        await bus.stop()

    @pytest.mark.asyncio
    async def test_global_subscription_forwards_all_events(self):
        """An empty dataset filter subscribes to everything (global stream)."""
        bus = EventBus(redis_client=None)
        await bus.start()
        watcher = S3Watcher("bucket", "datasets", bus, lambda d: {})
        request = FakeRequest(
            {"bus": bus, "watcher": watcher, "loop": asyncio.get_running_loop()}
        )

        response = await events_handler(request, "")
        iterator = response.body_iterator

        first = await asyncio.wait_for(iterator.__anext__(), timeout=2)
        assert first["event"] == "connected"
        assert json.loads(first["data"])["dataset"] == ""
        assert watcher.watched_datasets == [""]

        await bus.publish(
            {"event_id": "g1", "type": "rows_ingested", "dataset": "any"}
        )
        while True:
            chunk = await asyncio.wait_for(iterator.__anext__(), timeout=2)
            if chunk["event"] == "rows_ingested":
                assert json.loads(chunk["data"])["event_id"] == "g1"
                break

        await iterator.aclose()
        assert watcher.watched_datasets == []
        await bus.stop()
