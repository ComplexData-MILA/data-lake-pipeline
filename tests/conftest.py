import json
import random
from datetime import datetime, timezone
from typing import Any, Iterator

from data_lake_pipeline.protocols import (
    AsyncFilter,
    AsyncProcessor,
    FilterResult,
    ProcessorResult,
    StageContext,
)
from data_lake_pipeline.storage.base import ObjectMetadata


class MockFilter(AsyncFilter):
    """Mock filter for testing. Optionally rejects based on keyword or random rate."""

    def __init__(
        self, reject_keyword: str = "reject", reject_rate: float = 0.0, **kwargs: Any
    ) -> None:
        self.reject_keyword = reject_keyword
        self.reject_rate = reject_rate

    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[FilterResult]:
        results = []
        for record in records:
            text = record.get("text", "")
            if self.reject_keyword and self.reject_keyword in text.lower():
                results.append(
                    FilterResult(
                        passed=False,
                        reason=f"Contains reject keyword: {self.reject_keyword}",
                        output={"rejected_by": "mock_filter"},
                    )
                )
            elif self.reject_rate > 0 and random.random() < self.reject_rate:
                results.append(
                    FilterResult(
                        passed=False,
                        reason="Random rejection",
                        output={"rejected_by": "mock_filter"},
                    )
                )
            else:
                results.append(FilterResult(passed=True, output={"mock": True}))
        return results


class MockProcessor(AsyncProcessor):
    """Mock processor that returns mock output. For testing only."""

    def __init__(self, output_field: str = "mock_field", **kwargs: Any) -> None:
        self.output_field = output_field

    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[ProcessorResult]:
        return [
            ProcessorResult(
                output={
                    self.output_field: f"processed_{i}",
                    "record_external_id": record.get("external_id"),
                }
            )
            for i, record in enumerate(records)
        ]


class MockStorage:
    def __init__(self):
        self.bucket = "test-bucket"
        self.prefix = "test-prefix"
        self._objects: dict[str, bytes] = {}
        self._metadata: dict[str, dict] = {}
        self._created_times: dict[str, datetime] = {}

    def get_full_key(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.prefix}/{key}"

    def _set_object(self, key: str, data: bytes, metadata: dict | None = None):
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        self._objects[full_key] = data
        self._metadata[full_key] = metadata or {}
        if full_key not in self._created_times:
            self._created_times[full_key] = datetime.now(timezone.utc)

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
        self._created_times.pop(full_key, None)

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

    def read_bytes(self, key: str) -> bytes:
        data = self._get_object(key)
        if data is None:
            raise FileNotFoundError(f"Object not found: {key}")
        return data

    def write_bytes(self, key: str, data: bytes) -> None:
        self._set_object(key, data)

    def get_object_metadata(self, key: str) -> ObjectMetadata | None:
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        if full_key not in self._objects:
            return None
        data = self._objects[full_key]
        last_modified = self._created_times.get(full_key, datetime.now(timezone.utc))
        now = datetime.now(timezone.utc)
        age_seconds = int((now - last_modified).total_seconds())
        return ObjectMetadata(
            key=key,
            size_bytes=len(data),
            last_modified=last_modified,
            age_seconds=age_seconds,
        )

    def list_objects_with_metadata(
        self, prefix: str, suffix: str = ""
    ) -> list[ObjectMetadata]:
        results = []
        for full_key, data in self._objects.items():
            if self.prefix:
                key = full_key[len(self.prefix) + 1 :]
            else:
                key = full_key
            if key.startswith(prefix):
                if not suffix or key.endswith(suffix):
                    last_modified = self._created_times.get(
                        full_key, datetime.now(timezone.utc)
                    )
                    now = datetime.now(timezone.utc)
                    age_seconds = int((now - last_modified).total_seconds())
                    results.append(
                        ObjectMetadata(
                            key=key,
                            size_bytes=len(data),
                            last_modified=last_modified,
                            age_seconds=age_seconds,
                        )
                    )
        return sorted(results, key=lambda x: x.key)
