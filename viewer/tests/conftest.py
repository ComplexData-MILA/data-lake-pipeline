from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, MagicMock

from data_lake_pipeline.storage.base import ObjectMetadata, StorageBackend


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

    def add_object(self, key: str, data: bytes):
        self._set_object(key, data)

    def add_jsonl_object(self, key: str, records: list[dict]):
        lines = [json.dumps(r, ensure_ascii=False) for r in records]
        self._set_object(key, "\n".join(lines).encode("utf-8"))

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


class MockBatchManifest:
    def __init__(
        self,
        batch_id: str,
        source: str = "test-source",
        state: str = "completed",
        row_count: int = 100,
        created_at: str | None = None,
        locked_at: str | None = None,
        locked_by: str | None = None,
        error: str | None = None,
    ):
        self.batch_id = batch_id
        self.source = source
        self.state = state
        self.row_count = row_count
        self.created_at = created_at or datetime.now(timezone.utc).isoformat()
        self.locked_at = locked_at
        self.locked_by = locked_by
        self.error = error

    def to_dict(self):
        return {
            "batch_id": self.batch_id,
            "source": self.source,
            "state": self.state,
            "row_count": self.row_count,
            "created_at": self.created_at,
            "locked_at": self.locked_at,
            "locked_by": self.locked_by,
            "error": self.error,
        }


class MockBatchState:
    def __init__(self, storage: MockStorage):
        self.storage = storage
        self._manifests: dict[str, MockBatchManifest] = {}

    def add_manifest(self, manifest: MockBatchManifest):
        self._manifests[manifest.batch_id] = manifest
        self.storage.put_json(f"manifests/{manifest.batch_id}.json", manifest.to_dict())

    def get_manifest(self, batch_id: str) -> MockBatchManifest | None:
        return self._manifests.get(batch_id)

    def list_pending(self) -> list[MockBatchManifest]:
        return [m for m in self._manifests.values() if m.state == "pending"]

    def list_inflight(self) -> list[MockBatchManifest]:
        return [m for m in self._manifests.values() if m.state == "inflight"]

    def list_failed(self) -> list[MockBatchManifest]:
        return [m for m in self._manifests.values() if m.state == "failed"]

    def list_all(self) -> list[MockBatchManifest]:
        return list(self._manifests.values())


@pytest.fixture
def mock_storage():
    return MockStorage()


@pytest.fixture
def mock_batch_state(mock_storage: MockStorage):
    return MockBatchState(mock_storage)


@pytest.fixture
def test_client(mock_storage: MockStorage, mock_batch_state: MockBatchState):
    from viewer.backend.main import app
    from viewer.backend.dependencies import get_storage, get_batch_state

    def override_get_storage():
        return mock_storage

    def override_get_batch_state():
        return mock_batch_state

    app.dependency_overrides[get_storage] = override_get_storage
    app.dependency_overrides[get_batch_state] = override_get_batch_state

    client = TestClient(app)
    yield client

    app.dependency_overrides.clear()
