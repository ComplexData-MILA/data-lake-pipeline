from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from data_lake_pipeline.schemas import LandedRecord

if TYPE_CHECKING:
    from data_lake_pipeline.storage.base import StorageBackend


def stream_jsonl(storage: StorageBackend, key: str) -> Iterator[dict]:
    return storage.stream_jsonl(key)


def count_jsonl_rows(storage: StorageBackend, key: str) -> int:
    count = 0
    for _ in storage.stream_jsonl(key):
        count += 1
    return count


def append_jsonl(
    storage: StorageBackend, key: str, records: Iterator[LandedRecord]
) -> int:
    def to_dict():
        for record in records:
            yield record.model_dump(mode="json")

    return storage.append_jsonl(key, to_dict())


def write_parquet(storage: StorageBackend, key: str, records: list[dict]) -> None:
    import pandas as pd
    import smart_open

    df = pd.DataFrame(records)
    uri = storage.get_full_key(key)
    with smart_open.open(uri, "wb") as f:
        df.to_parquet(f, index=False)
