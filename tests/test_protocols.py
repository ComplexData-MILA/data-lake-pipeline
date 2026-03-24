import pytest

from tests.conftest import MockFilter, MockProcessor
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


class TestMockFilter:
    @pytest.mark.asyncio
    async def test_passes_records_without_reject_keyword(self):
        ctx = StageContext(stage_name="test", batch_id="batch1")
        records = [
            make_record_dict("1", "this should pass"),
            make_record_dict("2", "this should also pass"),
        ]
        results = await MockFilter()(records, ctx)
        assert len(results) == 2
        assert all(isinstance(r, FilterResult) for r in results)
        assert all(r.passed for r in results)

    @pytest.mark.asyncio
    async def test_rejects_records_with_reject_keyword(self):
        ctx = StageContext(stage_name="test", batch_id="batch1")
        records = [
            make_record_dict("1", "this should pass"),
            make_record_dict("2", "reject this content"),
        ]
        results = await MockFilter()(records, ctx)
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is False
        assert results[1].reason and "reject" in results[1].reason

    @pytest.mark.asyncio
    async def test_reject_rate_parameter(self):
        ctx = StageContext(stage_name="test", batch_id="batch1")
        records = [make_record_dict(str(i)) for i in range(100)]
        results = await MockFilter(reject_rate=0.2)(records, ctx)
        passed = sum(1 for r in results if r.passed)
        assert 70 <= passed <= 95


class TestMockProcessor:
    @pytest.mark.asyncio
    async def test_processes_records(self):
        ctx = StageContext(stage_name="test", batch_id="batch1")
        records = [make_record_dict("1", "hello world")]
        results = await MockProcessor()(records, ctx)
        assert len(results) == 1
        assert isinstance(results[0], ProcessorResult)
        assert results[0].output is not None
        assert "mock_field" in results[0].output

    @pytest.mark.asyncio
    async def test_processor_returns_external_id(self):
        ctx = StageContext(stage_name="test", batch_id="batch1")
        records = [make_record_dict("abc123", "hello world")]
        results = await MockProcessor()(records, ctx)
        assert results[0].output["record_external_id"] == "abc123"
