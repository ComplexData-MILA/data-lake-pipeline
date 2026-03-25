from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from data_lake_pipeline.state import BatchState
from data_lake_pipeline.storage import ObjectMetadata, StorageBackend
from viewer.backend.services.cache import StatusCache

if TYPE_CHECKING:
    pass


@dataclass
class PipelineStatus:
    batches: dict[str, int]
    total_rows_processed: int
    sources: dict[str, dict]
    stuck_batches: list[dict]
    recent_errors: list[dict]
    cache_fetched_at: str


@dataclass
class LandingZoneStatus:
    files: list[dict]
    total_size_bytes: int
    cache_fetched_at: str


@dataclass
class QueueStatus:
    pending: list[dict]
    inflight: list[dict]
    failed: list[dict]
    cache_fetched_at: str


class PipelineStatusService:
    def __init__(
        self,
        storage: StorageBackend,
        batch_state: BatchState,
        cache_ttl_seconds: int = 60,
        stuck_threshold_seconds: int = 3600,
    ):
        self.storage = storage
        self.batch_state = batch_state
        self.stuck_threshold_seconds = stuck_threshold_seconds
        self._cache = StatusCache(default_ttl_seconds=cache_ttl_seconds)

    async def get_status(self, force_refresh: bool = False) -> PipelineStatus:
        return await self._cache.get_or_refresh(
            "pipeline_status",
            self._fetch_pipeline_status,
            force_refresh=force_refresh,
        )

    async def get_landing_status(
        self, source: str | None = None, force_refresh: bool = False
    ) -> LandingZoneStatus:
        cache_key = f"landing_status:{source or 'all'}"
        return await self._cache.get_or_refresh(
            cache_key,
            lambda: self._fetch_landing_status(source),
            force_refresh=force_refresh,
        )

    async def get_queue_status(self, force_refresh: bool = False) -> QueueStatus:
        return await self._cache.get_or_refresh(
            "queue_status",
            self._fetch_queue_status,
            force_refresh=force_refresh,
        )

    async def _fetch_pipeline_status(self) -> PipelineStatus:
        manifests = await self._fetch_all_manifests()
        now = datetime.now(timezone.utc)

        batches: dict[str, int] = {}
        for m in manifests:
            state = m.get("state", "unknown")
            batches[state] = batches.get(state, 0) + 1

        total_rows = sum(
            m.get("row_count", 0) or 0
            for m in manifests
            if m.get("state") in ("completed", "archived")
        )

        sources: dict[str, dict] = {}
        for m in manifests:
            src = m.get("source", "unknown")
            if src not in sources:
                sources[src] = {"rows": 0, "batches": 0, "failed": 0}
            sources[src]["batches"] += 1
            if m.get("state") in ("completed", "archived") and m.get("row_count"):
                sources[src]["rows"] += m["row_count"] or 0
            if m.get("state") == "failed":
                sources[src]["failed"] += 1

        for src in sources:
            batch_count = sources[src]["batches"]
            failed_count = sources[src]["failed"]
            total = batch_count + failed_count
            if total > 0:
                sources[src]["success_rate"] = batch_count / total
            else:
                sources[src]["success_rate"] = 0.0

        stuck_batches = self._detect_stuck_batches(
            manifests, self.stuck_threshold_seconds
        )

        recent_errors = [
            {
                "batch_id": m.get("batch_id"),
                "source": m.get("source"),
                "error": m.get("error"),
                "created_at": m.get("created_at"),
            }
            for m in manifests
            if m.get("state") == "failed" and m.get("error")
        ]
        recent_errors.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        recent_errors = recent_errors[:20]

        return PipelineStatus(
            batches=batches,
            total_rows_processed=total_rows,
            sources=sources,
            stuck_batches=stuck_batches,
            recent_errors=recent_errors,
            cache_fetched_at=now.isoformat(),
        )

    def _fetch_landing_status(self, source: str | None) -> LandingZoneStatus:
        prefix = "01_landing/"
        if source:
            prefix = f"01_landing/{source}/"

        objects = self.storage.list_objects_with_metadata(prefix)
        now = datetime.now(timezone.utc)

        files = []
        total_size = 0

        for obj in objects:
            if source and not obj.key.startswith(f"01_landing/{source}/"):
                continue

            parts = obj.key.split("/")
            file_source = parts[2] if len(parts) > 2 else "unknown"

            files.append(
                {
                    "key": obj.key,
                    "source": file_source,
                    "size_bytes": obj.size_bytes,
                    "age_seconds": obj.age_seconds,
                    "last_modified": obj.last_modified.isoformat(),
                }
            )
            total_size += obj.size_bytes

        files.sort(key=lambda x: x.get("age_seconds", 0), reverse=True)

        return LandingZoneStatus(
            files=files,
            total_size_bytes=total_size,
            cache_fetched_at=now.isoformat(),
        )

    async def _fetch_queue_status(self) -> QueueStatus:
        now = datetime.now(timezone.utc)

        pending_manifests = await self.batch_state.list_pending()
        inflight_manifests = await self.batch_state.list_inflight()
        failed_manifests = await self.batch_state.list_failed()

        pending = [
            {
                "batch_id": m.batch_id,
                "source": m.source,
                "created_at": m.created_at,
            }
            for m in pending_manifests
        ]

        inflight = [
            {
                "batch_id": m.batch_id,
                "source": m.source,
                "locked_by": m.locked_by,
                "locked_at": m.locked_at,
            }
            for m in inflight_manifests
        ]

        failed = [
            {
                "batch_id": m.batch_id,
                "source": m.source,
                "error": m.error,
                "created_at": m.created_at,
            }
            for m in failed_manifests
        ]

        return QueueStatus(
            pending=pending,
            inflight=inflight,
            failed=failed,
            cache_fetched_at=now.isoformat(),
        )

    async def _fetch_all_manifests(self) -> list[dict]:
        manifests = []
        for key in self.storage.list_objects("manifests", ".json"):
            batch_id = key.split("/")[-1].replace(".json", "")
            manifest = await self.batch_state.get_manifest(batch_id)
            if manifest:
                manifests.append(manifest.model_dump(mode="json"))
        return manifests

    def _detect_stuck_batches(
        self, manifests: list[dict], threshold_seconds: int
    ) -> list[dict]:
        now = datetime.now(timezone.utc)
        stuck = []

        for m in manifests:
            if m.get("state") != "inflight":
                continue
            locked_at_str = m.get("locked_at")
            if not locked_at_str:
                continue

            try:
                locked_at = datetime.fromisoformat(locked_at_str)
                if locked_at.tzinfo is None:
                    locked_at = locked_at.replace(tzinfo=timezone.utc)
                elapsed = (now - locked_at).total_seconds()
                if elapsed > threshold_seconds:
                    stuck.append(
                        {
                            "batch_id": m.get("batch_id"),
                            "source": m.get("source"),
                            "locked_at": locked_at_str,
                            "locked_by": m.get("locked_by"),
                            "stuck_seconds": int(elapsed),
                        }
                    )
            except (ValueError, TypeError):
                continue

        stuck.sort(key=lambda x: x.get("stuck_seconds", 0), reverse=True)
        return stuck
