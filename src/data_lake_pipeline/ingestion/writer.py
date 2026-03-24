from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterable, Iterator

from tqdm import tqdm

from data_lake_pipeline.io import append_jsonl
from data_lake_pipeline.schemas import LandedRecord, SourcePost

if TYPE_CHECKING:
    from data_lake_pipeline.storage.base import StorageBackend


def save_source_posts(
    source_name: str,
    posts: Iterable[SourcePost],
    storage: StorageBackend,
    landing_prefix: str = "landing",
) -> int:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{landing_prefix}/{source_name}/{today}.jsonl"

    def _validate_and_convert() -> Iterator[LandedRecord]:
        for post in tqdm(posts, desc="Writing records to landing zone", unit="records"):
            if post.source != source_name:
                raise ValueError(
                    f"Post source mismatch: expected {source_name}, got {post.source}"
                )
            yield post.to_landed_record()

    written = append_jsonl(storage, key, _validate_and_convert())
    return written
