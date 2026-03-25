from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from data_lake_pipeline.mutex import AsyncCloudflareMutex
from data_lake_pipeline.schemas import BatchManifest
from data_lake_pipeline.stage_schemas import (
    FilterCompletion,
    FilterError,
    FilterState,
    StageAwareBatchManifest,
)

if TYPE_CHECKING:
    from data_lake_pipeline.storage.base import StorageBackend

logger = logging.getLogger(__name__)

MANIFESTS_PREFIX = "manifests"

DEFAULT_LOCK_TIMEOUT_SECONDS = 600


class BatchState:
    def __init__(self, storage: StorageBackend, mutex_ws_url: str):
        self.storage = storage
        self.mutex_ws_url = mutex_ws_url

    def _manifest_key(self, batch_id: str) -> str:
        return f"{MANIFESTS_PREFIX}/{batch_id}.json"

    def _worker_id(self) -> str:
        return f"{socket.gethostname()}_{os.getpid()}"

    @asynccontextmanager
    async def _mutex(self, batch_id: str):
        lock_name = f"manifest-{batch_id}"
        async with AsyncCloudflareMutex(self.mutex_ws_url, lock_name):
            yield

    async def get_manifest(self, batch_id: str) -> BatchManifest | None:
        data = self.storage.get_json(self._manifest_key(batch_id))
        if data:
            return BatchManifest.model_validate(data)
        return None

    async def put_manifest(
        self, manifest: BatchManifest, if_none_match: bool = False
    ) -> bool:
        return self.storage.put_json(
            self._manifest_key(manifest.batch_id),
            manifest.model_dump(mode="json"),
            if_none_match=if_none_match,
        )

    async def create_batch(self, source: str, original_key: str) -> BatchManifest:
        batch_id = f"{source}__{uuid.uuid4().hex[:8]}"
        manifest = BatchManifest(
            batch_id=batch_id,
            source=source,
            original_key=original_key,
            state="pending",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        async with self._mutex(batch_id):
            if not await self.put_manifest(manifest, if_none_match=True):
                raise RuntimeError(f"Failed to create manifest for batch {batch_id}")
        logger.info("Created batch %s for %s", batch_id, original_key)
        return manifest

    async def claim_batch(self, batch_id: str) -> BatchManifest | None:
        async with self._mutex(batch_id):
            manifest = await self.get_manifest(batch_id)
            if not manifest or manifest.state != "pending":
                return None

            manifest.state = "inflight"
            manifest.locked_by = self._worker_id()
            manifest.locked_at = datetime.now(timezone.utc).isoformat()

            if await self.put_manifest(manifest):
                logger.info("Claimed batch %s", batch_id)
                return manifest
            return None

    async def complete_batch(
        self, manifest: BatchManifest, output_key: str, row_count: int
    ) -> None:
        async with self._mutex(manifest.batch_id):
            manifest.state = "completed"
            manifest.output_key = output_key
            manifest.row_count = row_count
            await self.put_manifest(manifest)
            logger.info("Completed batch %s with %d rows", manifest.batch_id, row_count)

    async def archive_batch(self, manifest: BatchManifest) -> None:
        async with self._mutex(manifest.batch_id):
            manifest.state = "archived"
            await self.put_manifest(manifest)
            logger.info("Archived batch %s", manifest.batch_id)

    async def fail_batch(self, manifest: BatchManifest, error: str) -> None:
        async with self._mutex(manifest.batch_id):
            manifest.state = "failed"
            manifest.error = error
            await self.put_manifest(manifest)
            logger.error("Failed batch %s: %s", manifest.batch_id, error)

    async def list_pending(self, min_age_seconds: int = 0) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.get_manifest(batch_id)
            if manifest and manifest.state == "pending":
                try:
                    age = self.storage.get_object_age_seconds(key)
                    if age >= min_age_seconds:
                        manifests.append(manifest)
                except Exception:
                    manifests.append(manifest)
        return manifests

    async def list_inflight(self) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.get_manifest(batch_id)
            if manifest and manifest.state == "inflight":
                manifests.append(manifest)
        return manifests

    async def list_failed(self) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.get_manifest(batch_id)
            if manifest and manifest.state == "failed":
                manifests.append(manifest)
        return manifests

    async def list_all(self) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.get_manifest(batch_id)
            if manifest:
                manifests.append(manifest)
        return manifests


class StageAwareBatchState:
    def __init__(
        self,
        storage: StorageBackend,
        stage_name: str,
        mutex_ws_url: str,
        lock_timeout_seconds: int = DEFAULT_LOCK_TIMEOUT_SECONDS,
    ):
        self.storage = storage
        self.stage_name = stage_name
        self.mutex_ws_url = mutex_ws_url
        self.lock_timeout_seconds = lock_timeout_seconds

    def _manifest_key(self, batch_id: str) -> str:
        return f"{MANIFESTS_PREFIX}/stages/{self.stage_name}/{batch_id}.json"

    def _worker_id(self) -> str:
        return f"{socket.gethostname()}_{os.getpid()}"

    @asynccontextmanager
    async def _mutex(self, batch_id: str):
        lock_name = f"manifest-{batch_id}"
        async with AsyncCloudflareMutex(self.mutex_ws_url, lock_name):
            yield

    async def get_manifest(self, batch_id: str) -> StageAwareBatchManifest | None:
        data = self.storage.get_json(self._manifest_key(batch_id))
        if data:
            return StageAwareBatchManifest.model_validate(data)
        return None

    async def put_manifest(
        self, manifest: StageAwareBatchManifest, if_none_match: bool = False
    ) -> bool:
        return self.storage.put_json(
            self._manifest_key(manifest.batch_id),
            manifest.model_dump(mode="json"),
            if_none_match=if_none_match,
        )

    async def create_batch(
        self,
        source: str,
        original_key: str,
        parent_batch_id: str | None = None,
    ) -> StageAwareBatchManifest:
        batch_id = f"{self.stage_name}__{uuid.uuid4().hex[:8]}"
        manifest = StageAwareBatchManifest(
            batch_id=batch_id,
            source=source,
            original_key=original_key,
            pipeline_stage=self.stage_name,
            parent_batch_id=parent_batch_id,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        async with self._mutex(batch_id):
            if not await self.put_manifest(manifest, if_none_match=True):
                raise RuntimeError(f"Failed to create manifest for batch {batch_id}")
        logger.info("Created stage batch %s for %s", batch_id, original_key)
        return manifest

    async def claim_filter(self, batch_id: str, filter_name: str) -> StageAwareBatchManifest | None:
        async with self._mutex(batch_id):
            manifest = await self.get_manifest(batch_id)
            if not manifest:
                return None

            if manifest.is_filter_complete(filter_name):
                return None

            if manifest.is_filter_locked(filter_name, self.lock_timeout_seconds):
                return None

            old_state = manifest.filter_states.get(filter_name)
            old_chunks = old_state.chunk_keys if old_state else []
            old_count = old_state.processed_ids_count if old_state else 0

            manifest.filter_states[filter_name] = FilterState(
                locked_by=self._worker_id(),
                locked_at=datetime.now(timezone.utc).isoformat(),
                chunk_keys=old_chunks,
                processed_ids_count=old_count,
            )

            if await self.put_manifest(manifest):
                logger.info("Claimed filter %s on batch %s", filter_name, batch_id)
                return manifest
            return None

    async def update_checkpoint(
        self,
        manifest: StageAwareBatchManifest,
        filter_name: str,
        chunk_keys: list[str],
        processed_ids_count: int,
    ) -> None:
        async with self._mutex(manifest.batch_id):
            state = manifest.filter_states.get(filter_name)
            if state:
                state.chunk_keys = chunk_keys
                state.processed_ids_count = processed_ids_count
                state.locked_at = datetime.now(timezone.utc).isoformat()
                await self.put_manifest(manifest)
                logger.debug(
                    "Checkpoint for filter %s on batch %s: %d chunks, %d processed",
                    filter_name,
                    manifest.batch_id,
                    len(chunk_keys),
                    processed_ids_count,
                )

    async def complete_filter(
        self,
        manifest: StageAwareBatchManifest,
        filter_name: str,
        completion: FilterCompletion,
    ) -> None:
        async with self._mutex(manifest.batch_id):
            if filter_name in manifest.filter_states:
                del manifest.filter_states[filter_name]
            manifest.completed_filters[filter_name] = completion
            await self.put_manifest(manifest)
            logger.info(
                "Completed filter %s on batch %s: %d passed, %d rejected",
                filter_name,
                manifest.batch_id,
                completion.passed_count,
                completion.rejected_count,
            )

    async def fail_filter(
        self,
        manifest: StageAwareBatchManifest,
        filter_name: str,
        error: str,
        attempt: int = 1,
    ) -> None:
        async with self._mutex(manifest.batch_id):
            filter_error = FilterError(
                error=error,
                failed_at=datetime.now(timezone.utc).isoformat(),
                attempt=attempt,
            )
            if filter_name not in manifest.filter_errors:
                manifest.filter_errors[filter_name] = []
            manifest.filter_errors[filter_name].append(filter_error)

            if filter_name in manifest.filter_states:
                del manifest.filter_states[filter_name]

            await self.put_manifest(manifest)
            logger.error("Failed filter %s on batch %s: %s", filter_name, manifest.batch_id, error)

    async def list_available_for_filter(self, filter_name: str, min_age_seconds: int = 0) -> list[StageAwareBatchManifest]:
        manifests = []
        prefix = f"{MANIFESTS_PREFIX}/stages/{self.stage_name}"
        for key in self.storage.list_objects(prefix, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.get_manifest(batch_id)
            if not manifest:
                continue
            if manifest.is_filter_complete(filter_name):
                continue
            if manifest.is_filter_locked(filter_name, self.lock_timeout_seconds):
                continue
            try:
                age = self.storage.get_object_age_seconds(key)
                if age >= min_age_seconds:
                    manifests.append(manifest)
            except Exception:
                manifests.append(manifest)
        return manifests

    async def list_all(self) -> list[StageAwareBatchManifest]:
        manifests = []
        prefix = f"{MANIFESTS_PREFIX}/stages/{self.stage_name}"
        for key in self.storage.list_objects(prefix, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.get_manifest(batch_id)
            if manifest:
                manifests.append(manifest)
        return manifests

    def get_existing_chunks(
        self,
        output_prefix: str,
        batch_id: str,
        filter_name: str,
    ) -> list[str]:
        chunk_prefix = f"{output_prefix}/.chunks/{filter_name}_"
        return self._list_chunks(chunk_prefix)

    def _list_chunks(self, prefix: str) -> list[str]:
        chunks = []
        for key in self.storage.list_objects(prefix, ".jsonl"):
            chunks.append(key)
        return sorted(chunks)
