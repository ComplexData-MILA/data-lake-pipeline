from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from tqdm.auto import tqdm

from .models import RunManifest, StreamingConfigs
from .s3_utils import (
    generate_hex_id,
    upload_jsonl_chunk,
    upload_run_manifest,
)


class DatasetGenerator:
    def __init__(self, s3_client: Any, bucket: str, prefix: str):
        self._s3_client = s3_client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    async def from_async_iterator(
        self,
        iterator: AsyncIterator[dict[str, Any]],
        name: str,
        batch: str,
        streaming_configs: StreamingConfigs,
        deduplicate_on: list[str] | None = None,
    ) -> None:
        run_id = generate_hex_id()
        base_path = f"{self._prefix}/{name}/{batch}"
        created_at = datetime.now(timezone.utc)

        manifest = RunManifest(
            run_id=run_id,
            deduplicate_on=deduplicate_on,
            streaming_configs=streaming_configs,
            completed=False,
            created_at=created_at,
        )
        manifest_key = f"{base_path}/{run_id}.manifest.json"
        await upload_run_manifest(self._s3_client, self._bucket, manifest_key, manifest)

        buffer: list[dict[str, Any]] = []
        chunk_idx = 0

        async for row in tqdm(iterator, ncols=80):
            buffer.append(row)
            if len(buffer) >= streaming_configs.chunk_size:
                key = f"{base_path}/{run_id}_chunk_{chunk_idx:05d}.jsonl"
                await upload_jsonl_chunk(self._s3_client, self._bucket, key, buffer)
                buffer.clear()
                chunk_idx += 1

        if buffer:
            key = f"{base_path}/{run_id}_chunk_{chunk_idx:05d}.jsonl"
            await upload_jsonl_chunk(self._s3_client, self._bucket, key, buffer)

        manifest.completed = True
        manifest.completed_at = datetime.now(timezone.utc)
        await upload_run_manifest(self._s3_client, self._bucket, manifest_key, manifest)
