from __future__ import annotations

import json
import logging
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from data_lake_pipeline.schemas import BatchManifest

if TYPE_CHECKING:
    from data_lake_pipeline.storage.base import StorageBackend

logger = logging.getLogger(__name__)

MANIFESTS_PREFIX = "manifests"


class BatchState:
    def __init__(self, storage: StorageBackend):
        self.storage = storage

    def _manifest_key(self, batch_id: str) -> str:
        return f"{MANIFESTS_PREFIX}/{batch_id}.json"

    def _worker_id(self) -> str:
        return f"{socket.gethostname()}_{os.getpid()}"

    def get_manifest(self, batch_id: str) -> BatchManifest | None:
        data = self.storage.get_json(self._manifest_key(batch_id))
        if data:
            return BatchManifest.from_dict(data)
        return None

    def put_manifest(
        self, manifest: BatchManifest, if_none_match: bool = False
    ) -> bool:
        return self.storage.put_json(
            self._manifest_key(manifest.batch_id),
            manifest.to_dict(),
            if_none_match=if_none_match,
        )

    def create_batch(self, source: str, original_key: str) -> BatchManifest:
        batch_id = f"{source}__{uuid.uuid4().hex[:8]}"
        manifest = BatchManifest(
            batch_id=batch_id,
            source=source,
            original_key=original_key,
            state="pending",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        if not self.put_manifest(manifest, if_none_match=True):
            raise RuntimeError(f"Failed to create manifest for batch {batch_id}")
        logger.info("Created batch %s for %s", batch_id, original_key)
        return manifest

    def claim_batch(self, batch_id: str) -> BatchManifest | None:
        manifest = self.get_manifest(batch_id)
        if not manifest or manifest.state != "pending":
            return None

        manifest.state = "inflight"
        manifest.locked_by = self._worker_id()
        manifest.locked_at = datetime.now(timezone.utc).isoformat()

        original = self.get_manifest(batch_id)
        if original and original.state != "pending":
            return None

        if self.put_manifest(manifest):
            logger.info("Claimed batch %s", batch_id)
            return manifest
        return None

    def complete_batch(
        self, manifest: BatchManifest, output_key: str, row_count: int
    ) -> None:
        manifest.state = "completed"
        manifest.output_key = output_key
        manifest.row_count = row_count
        self.put_manifest(manifest)
        logger.info("Completed batch %s with %d rows", manifest.batch_id, row_count)

    def archive_batch(self, manifest: BatchManifest) -> None:
        manifest.state = "archived"
        self.put_manifest(manifest)
        logger.info("Archived batch %s", manifest.batch_id)

    def fail_batch(self, manifest: BatchManifest, error: str) -> None:
        manifest.state = "failed"
        manifest.error = error
        self.put_manifest(manifest)
        logger.error("Failed batch %s: %s", manifest.batch_id, error)

    def list_pending(self, min_age_seconds: int = 0) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = self.get_manifest(batch_id)
            if manifest and manifest.state == "pending":
                try:
                    age = self.storage.get_object_age_seconds(key)
                    if age >= min_age_seconds:
                        manifests.append(manifest)
                except Exception:
                    manifests.append(manifest)
        return manifests

    def list_inflight(self) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = self.get_manifest(batch_id)
            if manifest and manifest.state == "inflight":
                manifests.append(manifest)
        return manifests

    def list_failed(self) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = self.get_manifest(batch_id)
            if manifest and manifest.state == "failed":
                manifests.append(manifest)
        return manifests

    def list_all(self) -> list[BatchManifest]:
        manifests = []
        for key in self.storage.list_objects(MANIFESTS_PREFIX, ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = self.get_manifest(batch_id)
            if manifest:
                manifests.append(manifest)
        return manifests
