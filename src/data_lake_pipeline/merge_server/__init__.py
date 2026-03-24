from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING

from data_lake_pipeline.merge_server.config import MergeServerConfig
from data_lake_pipeline.merge_server.locks import acquire_merge_lock, release_merge_lock
from data_lake_pipeline.merge_server.merger import BatchMerger
from data_lake_pipeline.storage.s3 import S3Storage

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class MergeServer:
    def __init__(self, config: MergeServerConfig):
        self.config = config
        self.storage = S3Storage(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix,
            endpoint_url=config.s3_endpoint_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
        )
        self._start_time: float | None = None

    async def run(self):
        self._start_time = time.time()

        while True:
            if self._should_exit():
                break

            try:
                await self._scan_and_merge()
            except Exception as e:
                logger.error("Merge scan failed: %s", e)

            await asyncio.sleep(self.config.merge_interval_seconds)

    async def run_once(self):
        await self._scan_and_merge()

    async def merge_single_batch(self, batch_id: str):
        await self._try_merge_batch(batch_id)

    def _should_exit(self) -> bool:
        if self.config.max_runtime_seconds <= 0:
            return False
        if self._start_time is None:
            return False
        elapsed = time.time() - self._start_time
        return elapsed >= self.config.max_runtime_seconds

    async def _scan_and_merge(self):
        batch_ids = await self._discover_pending_batches()

        semaphore = asyncio.Semaphore(self.config.max_concurrent_merges)

        async def limited_merge(batch_id: str):
            async with semaphore:
                await self._try_merge_batch(batch_id)

        tasks = [limited_merge(bid) for bid in batch_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _try_merge_batch(self, batch_id: str) -> bool:
        owner = str(uuid.uuid4())[:8]

        acquired = await acquire_merge_lock(
            batch_id=batch_id,
            storage=self.storage,
            lock_prefix=self.config.lock_prefix,
            owner=owner,
            ttl_seconds=self.config.lock_ttl_seconds,
        )

        if not acquired:
            logger.debug("Batch %s locked by another process", batch_id)
            return False

        try:
            merger = BatchMerger(
                storage=self.storage,
                batch_id=batch_id,
                annotations_prefix=self.config.annotations_prefix,
            )
            status = await merger.get_merge_status()

            if not status.filters_to_merge:
                logger.debug("Batch %s has no new filters to merge", batch_id)
                return False

            result = await merger.merge()
            logger.info(
                "Merged %d records for batch %s, filters: %s",
                result.merged_count,
                batch_id,
                result.filters_added,
            )
            return True

        finally:
            await release_merge_lock(
                batch_id=batch_id,
                storage=self.storage,
                lock_prefix=self.config.lock_prefix,
                owner=owner,
            )

    async def _discover_pending_batches(self) -> list[str]:
        batch_ids = set()

        manifests_prefix = f"{self.config.annotations_prefix}"
        for key in self.storage.list_objects(manifests_prefix, ".json"):
            parts = key.split("/")
            if len(parts) >= 2:
                batch_id = parts[1] if parts[0] == "annotations" else parts[0]
                batch_ids.add(batch_id)

        filters_prefix = f"{self.config.annotations_prefix}/filters"
        for key in self.storage.list_objects(filters_prefix, ".parquet"):
            parts = key.split("/")
            if len(parts) >= 3:
                batch_id = parts[2]
                batch_ids.add(batch_id)

        return list(batch_ids)
