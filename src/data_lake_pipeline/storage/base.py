from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable


@runtime_checkable
class StorageBackend(Protocol):
    bucket: str
    prefix: str

    def stream_jsonl(self, key: str) -> Iterator[dict]:
        ...

    def append_jsonl(self, key: str, records: Iterator[dict]) -> int:
        ...

    def put_json(self, key: str, data: dict, if_none_match: bool = False) -> bool:
        ...

    def get_json(self, key: str) -> dict | None:
        ...

    def copy_object(self, src: str, dst: str) -> None:
        ...

    def delete_object(self, key: str) -> None:
        ...

    def list_objects(self, prefix: str, suffix: str = "") -> list[str]:
        ...

    def get_object_age_seconds(self, key: str) -> int:
        ...

    def object_exists(self, key: str) -> bool:
        ...

    def get_full_key(self, key: str) -> str:
        ...
