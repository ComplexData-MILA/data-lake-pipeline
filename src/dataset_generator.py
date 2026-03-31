from collections.abc import AsyncIterator
from typing import Any

from .models import StreamingConfigs
from .s3_utils import (
    generate_hex_id,
    upload_jsonl_chunk,
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
        hex_id = generate_hex_id()
        base_path = f"{self._prefix}/{name}/{batch}"
        buffer: list[dict[str, Any]] = []
        chunk_idx = 0

        async for row in iterator:
            buffer.append(row)
            if len(buffer) >= streaming_configs.chunk_size:
                key = f"{base_path}/{hex_id}_chunk_{chunk_idx:05d}.jsonl"
                await upload_jsonl_chunk(self._s3_client, self._bucket, key, buffer)
                buffer.clear()
                chunk_idx += 1

        if buffer:
            key = f"{base_path}/{hex_id}_chunk_{chunk_idx:05d}.jsonl"
            await upload_jsonl_chunk(self._s3_client, self._bucket, key, buffer)
