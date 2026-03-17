import json

from data_lake_pipeline.ingestion.writer import save_source_posts
from data_lake_pipeline.schemas import SourcePost
from tests.conftest import MockStorage


def test_save_source_posts_writes_jsonl():
    storage = MockStorage()
    posts = [
        SourcePost(
            source="bluesky",
            external_id="abc",
            text="hello world",
            created_at="2026-03-13T12:00:00Z",
            url=None,
            author="alice",
            score=1.0,
            metadata={"lang": "en"},
        )
    ]

    written = save_source_posts("bluesky", posts, storage=storage, landing_prefix="01_landing")
    assert written == 1

    files = storage.list_objects("01_landing/bluesky", ".jsonl")
    assert len(files) == 1

    rows = list(storage.stream_jsonl(files[0]))
    assert len(rows) == 1
    assert rows[0]["external_id"] == "abc"
    assert rows[0]["source"] == "bluesky"
