import json
from datetime import datetime, timezone
from typing import Iterator

from data_lake_pipeline.storage.base import StorageBackend


class MockStorage:
    def __init__(self):
        self.bucket = "test-bucket"
        self.prefix = "test-prefix"
        self._objects: dict[str, bytes] = {}
        self._metadata: dict[str, dict] = {}

    def get_full_key(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.prefix}/{key}"

    def _set_object(self, key: str, data: bytes, metadata: dict | None = None):
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        self._objects[full_key] = data
        self._metadata[full_key] = metadata or {}

    def _get_object(self, key: str) -> bytes | None:
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        return self._objects.get(full_key)

    def stream_jsonl(self, key: str) -> Iterator[dict]:
        data = self._get_object(key)
        if data:
            for line in data.decode("utf-8").splitlines():
                if line.strip():
                    yield json.loads(line)

    def append_jsonl(self, key: str, records: Iterator[dict]) -> int:
        existing = []
        data = self._get_object(key)
        if data:
            existing = data.decode("utf-8").splitlines()

        count = 0
        lines = list(existing)
        for record in records:
            lines.append(json.dumps(record, ensure_ascii=False))
            count += 1

        self._set_object(key, "\n".join(lines).encode("utf-8"))
        return count

    def put_json(self, key: str, data: dict, if_none_match: bool = False) -> bool:
        existing = self._get_object(key)
        if if_none_match and existing:
            return False
        self._set_object(key, json.dumps(data, ensure_ascii=False).encode("utf-8"))
        return True

    def get_json(self, key: str) -> dict | None:
        data = self._get_object(key)
        if data:
            return json.loads(data.decode("utf-8"))
        return None

    def copy_object(self, src: str, dst: str) -> None:
        data = self._get_object(src)
        if data:
            self._set_object(dst, data)

    def delete_object(self, key: str) -> None:
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        self._objects.pop(full_key, None)
        self._metadata.pop(full_key, None)

    def list_objects(self, prefix: str, suffix: str = "") -> list[str]:
        keys = []
        for key in self._objects:
            if self.prefix:
                key_without_prefix = key[len(self.prefix) + 1 :]
            else:
                key_without_prefix = key
            if key_without_prefix.startswith(prefix):
                if not suffix or key_without_prefix.endswith(suffix):
                    keys.append(key_without_prefix)
        return sorted(keys)

    def get_object_age_seconds(self, key: str) -> int:
        return 0

    def object_exists(self, key: str) -> bool:
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        return full_key in self._objects
