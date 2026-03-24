from typing import AsyncIterator

import pytest

from tests.conftest import MockFilter, MockProcessor
from data_lake_pipeline.processing.streaming_processor import StreamingStageProcessor
from data_lake_pipeline.protocols import FilterResult, ProcessorResult, StageContext


def make_record_dict(external_id: str, text: str = "test content") -> dict:
    return {
        "source": "test",
        "external_id": external_id,
        "text": text,
        "created_at": "2026-03-18T12:00:00Z",
        "url": None,
        "author": "tester",
        "score": 1.0,
        "metadata": {},
        "ingested_at": "2026-03-18T12:00:00Z",
    }


async def record_stream(records: list[dict]) -> AsyncIterator[dict]:
    for r in records:
        yield r


class TestStreamingStageProcessor:
    @pytest.mark.asyncio
    async def test_processes_all_records(self):
        input_records = [make_record_dict(str(i)) for i in range(5)]
        stream = record_stream(input_records)

        filter_plugin = MockFilter()
        processor = StreamingStageProcessor(plugin=filter_plugin, max_concurrent=2)

        ctx = StageContext(stage_name="test", batch_id="batch1")

        results = []
        async for record, result in processor.process_stream(stream, ctx):
            results.append((record, result))

        assert len(results) == 5
        assert all(isinstance(r, FilterResult) for _, r in results)
        assert all(r.passed for _, r in results)

    @pytest.mark.asyncio
    async def test_filter_rejects_with_keyword(self):
        input_records = [
            make_record_dict("1", "good content"),
            make_record_dict("2", "reject this"),
            make_record_dict("3", "another good one"),
        ]
        stream = record_stream(input_records)

        filter_plugin = MockFilter(reject_keyword="reject")
        processor = StreamingStageProcessor(plugin=filter_plugin, max_concurrent=2)

        ctx = StageContext(stage_name="test", batch_id="batch1")

        results = []
        async for record, result in processor.process_stream(stream, ctx):
            results.append((record, result))

        passed = [r for _, r in results if r.passed]
        rejected = [r for _, r in results if not r.passed]

        assert len(passed) == 2
        assert len(rejected) == 1
        assert "reject" in rejected[0].reason

    @pytest.mark.asyncio
    async def test_processor_returns_outputs(self):
        input_records = [make_record_dict("abc"), make_record_dict("xyz")]
        stream = record_stream(input_records)

        processor_plugin = MockProcessor()
        processor = StreamingStageProcessor(plugin=processor_plugin, max_concurrent=2)

        ctx = StageContext(stage_name="test", batch_id="batch1")

        results = []
        async for record, result in processor.process_stream(stream, ctx):
            results.append((record, result))

        assert len(results) == 2
        assert all(isinstance(r, ProcessorResult) for _, r in results)
        assert results[0][1].output["record_external_id"] == "abc"
        assert results[1][1].output["record_external_id"] == "xyz"

    @pytest.mark.asyncio
    async def test_concurrent_processing(self):
        input_records = [make_record_dict(str(i)) for i in range(20)]
        stream = record_stream(input_records)

        filter_plugin = MockFilter()
        processor = StreamingStageProcessor(plugin=filter_plugin, max_concurrent=5)

        ctx = StageContext(stage_name="test", batch_id="batch1")

        results = []
        async for record, result in processor.process_stream(stream, ctx):
            results.append((record, result))

        assert len(results) == 20
