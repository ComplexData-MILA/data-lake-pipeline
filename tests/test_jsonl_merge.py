"""Integration tests for the JSONL block merge (s3_data_tool/jsonl_merge.py)."""

import asyncio
import gzip
import io
import json
import os
import uuid

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import s3_data_tool.jsonl_merge as jsonl_merge
from s3_data_tool.jsonl_merge import (
    dataset_merged_size,
    merge_to_jsonl_blocks,
    publish_blocks,
)
from s3_data_tool.s3_utils import delete_objects

pytestmark = pytest.mark.integration


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _jsonl_bytes(rows: list[dict]) -> bytes:
    return "\n".join(json.dumps(r) for r in rows).encode("utf-8")


async def _read_blocks(client, bucket, block_keys) -> list[dict]:
    """Download and gunzip block files, returning all rows in order."""
    rows = []
    for key in block_keys:
        resp = await client.get_object(Bucket=bucket, Key=key)
        body = gzip.decompress(await resp["Body"].read())
        for line in body.decode("utf-8").strip().split("\n"):
            if line.strip():
                rows.append(json.loads(line))
    return rows


async def _list_keys(client, bucket, prefix) -> list[str]:
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return sorted(keys)


@pytest.fixture
async def s3_env():
    session = aioboto3.Session(
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    kwargs = {}
    if os.environ.get("S3_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")
    dataset_name = f"merge_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        yield client, bucket, prefix, dataset_name

        keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")
        await delete_objects(client, bucket, keys)


class TestMergeToJsonlBlocks:
    @pytest.mark.asyncio
    async def test_merge_chunks_to_sorted_deduped_blocks(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        batch_prefix = f"{prefix}/{dataset_name}/b1"

        # Unsorted chunks with a duplicated id; stored values use the
        # pipeline's JSON-stringification convention.
        chunk1 = [
            {"id": "c", "_batch": '"b1"', "text": '"ccc"'},
            {"id": "a", "_batch": '"b1"', "text": '"aaa"'},
        ]
        chunk2 = [
            {"id": "b", "_batch": '"b1"', "text": '"bbb"'},
            {"id": "c", "_batch": '"b1"', "text": '"ccc-dup"'},
        ]
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/r1_chunk_00000.jsonl",
            Body=_jsonl_bytes(chunk1),
        )
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/r2_chunk_00000.jsonl",
            Body=_jsonl_bytes(chunk2),
        )

        result = await asyncio.to_thread(
            merge_to_jsonl_blocks,
            bucket, prefix, dataset_name, "b1",
            [f"{batch_prefix}/r1_chunk_00000.jsonl", f"{batch_prefix}/r2_chunk_00000.jsonl"],
            None, [], ["id"],
        )
        temp_keys = await publish_blocks(client, bucket, result["blocks"])
        await delete_objects(client, bucket, temp_keys)

        assert result["row_count"] == 3
        assert result["distinct_id_count"] == 3
        assert result["min_id"] == "a"
        assert result["max_id"] == "c"
        assert len(result["blocks"]) == 1

        rows = await _read_blocks(client, bucket, [b["key"] for b in result["blocks"]])
        assert [(r["id"], r["_batch"]) for r in rows] == [
            ("a", '"b1"'), ("b", '"b1"'), ("c", '"b1"'),
        ]
        # Double-encoding survives byte-for-byte.
        assert rows[0]["text"] == '"aaa"'

    @pytest.mark.asyncio
    async def test_chunk_wins_over_existing_blocks_and_parquet(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        batch_prefix = f"{prefix}/{dataset_name}/b1"

        existing_block = [{"id": "dup", "_batch": '"b1"', "text": '"old-block"'}]
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/merged_00000.jsonl.gz",
            Body=gzip.compress(_jsonl_bytes(existing_block)),
        )
        existing_parquet = [{"id": "dup", "_batch": '"b1"', "text": '"old-pq"'},
                            {"id": "keep", "_batch": '"b1"', "text": '"pq-only"'}]
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/merged.parquet",
            Body=_parquet_bytes(existing_parquet),
        )
        chunk = [{"id": "dup", "_batch": '"b1"', "text": '"from-chunk"'}]
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/r1_chunk_00000.jsonl",
            Body=_jsonl_bytes(chunk),
        )

        result = await asyncio.to_thread(
            merge_to_jsonl_blocks,
            bucket, prefix, dataset_name, "b1",
            [f"{batch_prefix}/r1_chunk_00000.jsonl"],
            f"{batch_prefix}/merged.parquet",
            [f"{batch_prefix}/merged_00000.jsonl.gz"],
            ["id"],
        )
        temp_keys = await publish_blocks(client, bucket, result["blocks"])
        await delete_objects(client, bucket, temp_keys)

        rows = await _read_blocks(client, bucket, [b["key"] for b in result["blocks"]])
        by_id = {r["id"]: r["text"] for r in rows}
        assert by_id["dup"] == '"from-chunk"'  # live chunk wins
        assert by_id["keep"] == '"pq-only"'  # parquet-only row kept
        assert result["row_count"] == 2

    @pytest.mark.asyncio
    async def test_block_split_and_meta_ranges(self, s3_env, monkeypatch):
        client, bucket, prefix, dataset_name = s3_env
        batch_prefix = f"{prefix}/{dataset_name}/b1"

        monkeypatch.setattr(jsonl_merge, "MERGE_BLOCK_SIZE", 2)
        rows = [{"id": f"r{i}", "_batch": '"b1"'} for i in range(5)]
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/r1_chunk_00000.jsonl",
            Body=_jsonl_bytes(rows),
        )

        result = await asyncio.to_thread(
            merge_to_jsonl_blocks,
            bucket, prefix, dataset_name, "b1",
            [f"{batch_prefix}/r1_chunk_00000.jsonl"], None, [], [],
        )
        assert [b["file"] for b in result["blocks"]] == [
            "merged_00000.jsonl.gz", "merged_00001.jsonl.gz", "merged_00002.jsonl.gz",
        ]
        assert [(b["row_count"], b["min_id"], b["max_id"]) for b in result["blocks"]] == [
            (2, "r0", "r1"), (2, "r2", "r3"), (1, "r4", "r4"),
        ]

    @pytest.mark.asyncio
    async def test_all_corrupt_chunks_yield_no_blocks(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        batch_prefix = f"{prefix}/{dataset_name}/b1"
        await client.put_object(
            Bucket=bucket, Key=f"{batch_prefix}/r1_chunk_00000.jsonl",
            Body=b"{not json at all\n",
        )
        result = await asyncio.to_thread(
            merge_to_jsonl_blocks,
            bucket, prefix, dataset_name, "b1",
            [f"{batch_prefix}/r1_chunk_00000.jsonl"], None, [], [],
        )
        assert result["row_count"] == 0
        assert result["blocks"] == []

    @pytest.mark.asyncio
    async def test_dataset_merged_size_counts_parquet_and_blocks(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        body = _parquet_bytes([{"id": "a", "_batch": '"b"'}])
        await client.put_object(
            Bucket=bucket, Key=f"{prefix}/{dataset_name}/b1/merged.parquet", Body=body
        )
        block = gzip.compress(_jsonl_bytes([{"id": "a", "_batch": '"b"'}]))
        await client.put_object(
            Bucket=bucket, Key=f"{prefix}/{dataset_name}/b1/merged_00000.jsonl.gz",
            Body=block,
        )
        await client.put_object(
            Bucket=bucket, Key=f"{prefix}/{dataset_name}/b1/merged_00000.jsonl.gz.temp",
            Body=block,
        )
        size = await dataset_merged_size(client, bucket, prefix, dataset_name)
        assert size == len(body) + len(block)  # .temp not counted
