from __future__ import annotations

import asyncio
import time

import pytest
from unittest.mock import Mock, MagicMock

from viewer.backend.services.cache import StatusCache
from viewer.backend.services.pipeline_status import PipelineStatusService
from viewer.tests.conftest import MockStorage, MockBatchState, MockBatchManifest


class TestStatusCache:
    def test_cache_returns_cached_data(self):
        cache = StatusCache(default_ttl_seconds=60)
        call_count = 0

        def fetcher():
            nonlocal call_count
            call_count += 1
            return {"data": "value"}

        result1 = asyncio.run(cache.get_or_refresh("key1", fetcher))
        result2 = asyncio.run(cache.get_or_refresh("key1", fetcher))

        assert result1 == {"data": "value"}
        assert result2 == {"data": "value"}
        assert call_count == 1

    def test_cache_expires_after_ttl(self):
        cache = StatusCache(default_ttl_seconds=1)
        call_count = 0

        def fetcher():
            nonlocal call_count
            call_count += 1
            return {"data": f"value{call_count}"}

        result1 = asyncio.run(cache.get_or_refresh("key1", fetcher, ttl_seconds=1))
        time.sleep(1.1)
        result2 = asyncio.run(cache.get_or_refresh("key1", fetcher, ttl_seconds=1))

        assert result1 == {"data": "value1"}
        assert result2 == {"data": "value2"}
        assert call_count == 2

    def test_force_refresh_bypasses_cache(self):
        cache = StatusCache(default_ttl_seconds=60)
        call_count = 0

        def fetcher():
            nonlocal call_count
            call_count += 1
            return {"data": f"value{call_count}"}

        result1 = asyncio.run(cache.get_or_refresh("key1", fetcher))
        result2 = asyncio.run(cache.get_or_refresh("key1", fetcher, force_refresh=True))

        assert result1 == {"data": "value1"}
        assert result2 == {"data": "value2"}
        assert call_count == 2

    def test_concurrent_requests_single_fetch(self):
        cache = StatusCache(default_ttl_seconds=60)
        call_count = 0

        async def fetcher():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return {"data": "value"}

        async def run_concurrent():
            tasks = [cache.get_or_refresh("key1", fetcher) for _ in range(5)]
            results = await asyncio.gather(*tasks)
            return results

        results = asyncio.run(run_concurrent())

        assert all(r == {"data": "value"} for r in results)
        assert call_count == 1


class TestPipelineStatusService:
    def test_get_status_aggregates_batches(self):
        storage = MockStorage()
        batch_state = MockBatchState(storage)

        batch_state.add_manifest(MockBatchManifest("batch1", state="completed", row_count=100))
        batch_state.add_manifest(MockBatchManifest("batch2", state="completed", row_count=200))
        batch_state.add_manifest(MockBatchManifest("batch3", state="pending"))
        batch_state.add_manifest(MockBatchManifest("batch4", state="failed"))

        service = PipelineStatusService(storage, batch_state)
        status = asyncio.run(service.get_status())

        assert status.batches.get("completed", 0) == 2
        assert status.batches.get("pending", 0) == 1
        assert status.batches.get("failed", 0) == 1
        assert status.total_rows_processed == 300

    def test_detect_stuck_batches(self):
        storage = MockStorage()
        batch_state = MockBatchState(storage)

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
        old_time = (now - __import__("datetime").timedelta(seconds=7200)).isoformat()

        batch_state.add_manifest(MockBatchManifest("batch1", state="inflight", locked_at=old_time, locked_by="worker1"))
        batch_state.add_manifest(MockBatchManifest("batch2", state="inflight", locked_at=now.isoformat(), locked_by="worker2"))
        batch_state.add_manifest(MockBatchManifest("batch3", state="completed"))

        service = PipelineStatusService(storage, batch_state, stuck_threshold_seconds=3600)
        status = asyncio.run(service.get_status())

        assert len(status.stuck_batches) == 1
        assert status.stuck_batches[0]["batch_id"] == "batch1"

    def test_success_rate_calculation(self):
        storage = MockStorage()
        batch_state = MockBatchState(storage)

        batch_state.add_manifest(MockBatchManifest("batch1", source="twitter", state="completed", row_count=100))
        batch_state.add_manifest(MockBatchManifest("batch2", source="twitter", state="completed", row_count=200))
        batch_state.add_manifest(MockBatchManifest("batch3", source="twitter", state="failed"))
        batch_state.add_manifest(MockBatchManifest("batch4", source="reddit", state="completed", row_count=50))

        service = PipelineStatusService(storage, batch_state)
        status = asyncio.run(service.get_status())

        assert "twitter" in status.sources
        assert "reddit" in status.sources
        assert status.sources["twitter"]["success_rate"] == pytest.approx(0.75, rel=0.01)
        assert status.sources["reddit"]["success_rate"] == pytest.approx(1.0, rel=0.01)
