from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncIterator, Set

if TYPE_CHECKING:
    pass


@dataclass
class CheckpointSummary:
    chunks: list[str]
    record_count: int


class CheckpointedWriter:
    def __init__(
        self,
        bucket: str,
        chunk_prefix: str,
        final_key: str,
        chunk_size: int = 1000,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        self.bucket = bucket
        self.chunk_prefix = chunk_prefix
        self.final_key = final_key
        self.chunk_size = chunk_size
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self._chunk_index = 0
        self._buffer: list[dict] = []
        self._client = None
        self._existing_chunks: list[str] = []
        self._processed_ids: Set[str] = set()

    async def __aenter__(self) -> CheckpointedWriter:
        from aiobotocore.session import get_session

        session = get_session()
        kwargs = {"service_name": "s3"}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        if self.access_key and self.secret_key:
            kwargs["aws_access_key_id"] = self.access_key
            kwargs["aws_secret_access_key"] = self.secret_key
        self._client = await session.create_client(**kwargs).__aenter__()
        return self

    async def load_existing_chunks(self, chunk_list: list[str]) -> Set[str]:
        self._existing_chunks = list(chunk_list)
        self._chunk_index = len(chunk_list)

        for chunk_key in chunk_list:
            async for record in self._stream_chunk(chunk_key):
                external_id = record.get("external_id")
                if external_id:
                    self._processed_ids.add(external_id)

        return self._processed_ids

    async def _stream_chunk(self, key: str) -> AsyncIterator[dict]:
        resp = await self._client.get_object(Bucket=self.bucket, Key=key)
        buffer = ""
        async for chunk in resp["Body"].content.iter_chunked(64 * 1024):
            buffer += chunk.decode("utf-8")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    yield json.loads(line)
        if buffer.strip():
            yield json.loads(buffer)

    @property
    def processed_ids(self) -> Set[str]:
        return self._processed_ids

    def is_processed(self, external_id: str) -> bool:
        return external_id in self._processed_ids

    async def write_record(self, record: dict) -> bool:
        self._buffer.append(record)
        external_id = record.get("external_id")
        if external_id:
            self._processed_ids.add(external_id)

        if len(self._buffer) >= self.chunk_size:
            await self._flush_chunk()
            return True
        return False

    async def _flush_chunk(self) -> str:
        chunk_key = f"{self.chunk_prefix}{self._chunk_index:04d}.jsonl"

        data = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in self._buffer)
        await self._client.put_object(
            Bucket=self.bucket,
            Key=chunk_key,
            Body=data.encode("utf-8"),
        )

        self._existing_chunks.append(chunk_key)
        self._chunk_index += 1
        self._buffer.clear()

        return chunk_key

    @property
    def existing_chunks(self) -> list[str]:
        return list(self._existing_chunks)

    async def close(self) -> CheckpointSummary:
        if self._buffer:
            await self._flush_chunk()

        return CheckpointSummary(
            chunks=list(self._existing_chunks),
            record_count=len(self._processed_ids),
        )

    async def merge_chunks(self) -> str:
        if not self._existing_chunks:
            return self.final_key

        resp = await self._client.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.final_key,
        )
        upload_id = resp["UploadId"]
        parts = []

        for i, chunk_key in enumerate(self._existing_chunks, 1):
            resp = await self._client.upload_part_copy(
                Bucket=self.bucket,
                Key=self.final_key,
                UploadId=upload_id,
                PartNumber=i,
                CopySource={"Bucket": self.bucket, "Key": chunk_key},
            )
            parts.append(
                {
                    "PartNumber": i,
                    "ETag": resp["CopyPartResult"]["ETag"],
                }
            )

        await self._client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.final_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

        return self.final_key

    async def delete_chunks(self) -> None:
        if self._existing_chunks:
            await self._client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in self._existing_chunks]},
            )

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)


async def stream_jsonl_async(
    bucket: str,
    key: str,
    endpoint_url: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    client=None,
) -> AsyncIterator[dict]:
    if client is None:
        from aiobotocore.session import get_session

        session = get_session()
        kwargs = {"service_name": "s3"}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key
        async with session.create_client(**kwargs) as c:
            async for record in _stream_from_client(c, bucket, key):
                yield record
    else:
        async for record in _stream_from_client(client, bucket, key):
            yield record


async def _stream_from_client(client, bucket: str, key: str) -> AsyncIterator[dict]:
    resp = await client.get_object(Bucket=bucket, Key=key)
    buffer = ""
    async for chunk in resp["Body"].content.iter_chunked(64 * 1024):
        buffer += chunk.decode("utf-8")
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            if line.strip():
                yield json.loads(line)
    if buffer.strip():
        yield json.loads(buffer)


async def write_parquet_async(
    bucket: str,
    key: str,
    records: list[dict],
    client=None,
) -> str:
    import io

    import pandas as pd

    own_client = client is None
    if own_client:
        from aiobotocore.session import get_session

        session = get_session()
        client = await session.create_client("s3").__aenter__()

    try:
        df = pd.DataFrame(records)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        await client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.read(),
        )
        return key
    finally:
        if own_client:
            await client.__aexit__(None, None, None)
