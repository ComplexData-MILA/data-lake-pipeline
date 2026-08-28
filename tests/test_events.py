"""Unit tests for s3_data_tool.events (opt-in viewer event publishing)."""

import json
from datetime import datetime, timezone

import pytest

from s3_data_tool.events import ViewerEvent, publish_event


def test_viewer_event_serialization():
    event = ViewerEvent(
        type="rows_ingested", dataset="d", batch="b", source="generator"
    )
    data = json.loads(event.model_dump_json())
    assert data["type"] == "rows_ingested"
    assert data["dataset"] == "d"
    assert data["batch"] == "b"
    assert data["source"] == "generator"
    assert data["event_id"]
    assert data["ts"]


def test_conversion_progress_event_serialization():
    event = ViewerEvent(
        type="conversion_progress",
        dataset="d",
        batch="b",
        converted=3,
        total=10,
        source="clean_up",
    )
    data = json.loads(event.model_dump_json())
    assert data["type"] == "conversion_progress"
    assert data["converted"] == 3
    assert data["total"] == 10


@pytest.mark.asyncio
async def test_publish_noop_without_env(monkeypatch):
    import s3_data_tool.events as events

    monkeypatch.delenv("VIEWER_REDIS_URL", raising=False)
    monkeypatch.setattr(events, "_client", None)
    await publish_event(ViewerEvent(type="run_completed", dataset="d", source="generator"))


@pytest.mark.asyncio
async def test_publish_noop_when_redis_import_fails(monkeypatch):
    import s3_data_tool.events as events

    monkeypatch.setattr(events, "_client", None)
    monkeypatch.setenv("VIEWER_REDIS_URL", "redis://localhost:6379/0")
    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "redis.asyncio":
            raise ImportError("no redis")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)
    assert events._get_redis_client() is None
    monkeypatch.setattr(events, "_client", None)
    await publish_event(ViewerEvent(type="batch_merged", dataset="d", source="clean_up"))


@pytest.mark.asyncio
async def test_publish_swallows_client_errors(monkeypatch):
    import s3_data_tool.events as events

    class FailingClient:
        async def publish(self, channel, payload):
            raise ConnectionError("redis down")

    monkeypatch.setattr(events, "_client", FailingClient())
    await publish_event(
        ViewerEvent(type="rows_ingested", dataset="d", source="generator")
    )
    monkeypatch.setattr(events, "_client", None)


@pytest.mark.asyncio
async def test_generator_publishes_events(monkeypatch):
    """DatasetGenerator publishes rows_ingested per chunk + a final run_completed."""
    import s3_data_tool.dataset_generator as dg
    from s3_data_tool.dataset_generator import DatasetGenerator
    from s3_data_tool.models import StreamingConfigs

    uploaded = []
    published = []

    class FakeS3:
        async def put_object(self, **kwargs):
            uploaded.append((kwargs["Key"], kwargs.get("Body")))

    async def fake_publish(event):
        published.append(event)

    monkeypatch.setattr(dg, "publish_event", fake_publish)

    gen = DatasetGenerator(FakeS3(), "bucket", "prefix")

    async def rows():
        for i in range(25):
            yield {"text": f"row{i}"}

    await gen.from_async_iterator(
        rows(),
        name="ds",
        batch="b1",
        streaming_configs=StreamingConfigs(chunk_size=10),
    )

    # 25 rows / chunk_size 10 -> 3 chunks -> 3 rows_ingested + 1 run_completed
    types = [e.type for e in published]
    assert types.count("rows_ingested") == 3
    assert types[-1] == "run_completed"
    assert [e.row_count for e in published if e.type == "rows_ingested"] == [10, 10, 5]
    assert all(e.dataset == "ds" and e.batch == "b1" for e in published)
    assert all(e.source == "generator" for e in published)
    # uploads: initial manifest + 3 chunks + final manifest = 5
    assert len(uploaded) == 5

    # Every chunk row carries a parseable _created_at (JSON-stringified on disk
    # by transform_row_for_jsonl, so unquote once before parsing).
    chunk_lines = [
        line
        for key, body in uploaded
        if "chunk_" in key
        for line in body.decode("utf-8").splitlines()
    ]
    assert len(chunk_lines) == 25
    for line in chunk_lines:
        raw = json.loads(line)["_created_at"]
        created = datetime.fromisoformat(json.loads(raw))
        assert created.tzinfo is not None
        assert (datetime.now(timezone.utc) - created).total_seconds() < 300
