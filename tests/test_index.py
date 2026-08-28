"""Integration tests for the dataset index writer (s3_data_tool/index.py)."""

import io
import json
import os
import uuid

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from s3_data_tool.index import index_meta_key, index_parquet_key, update_batch_index
from s3_data_tool.s3_utils import enumerate_batches

pytestmark = pytest.mark.integration


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


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
    dataset_name = f"idx_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        yield client, bucket, prefix, dataset_name

        paginator = client.get_paginator("list_objects_v2")
        keys = []
        async for page in paginator.paginate(
            Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"
        ):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        for key in keys:
            await client.delete_object(Bucket=bucket, Key=key)


class TestUpdateBatchIndex:
    @pytest.mark.asyncio
    async def test_writes_sorted_partition_and_meta(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        batch = "b1"
        rows = [
            {"id": "a3", "_batch": '"b1"', "text": "c"},
            {"id": "a1", "_batch": '"b1"', "text": "a"},
            {"id": "a2", "_batch": '"b1"', "text": "b"},
        ]
        await client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{dataset_name}/{batch}/merged.parquet",
            Body=_parquet_bytes(rows),
        )

        meta = await update_batch_index(client, bucket, prefix, dataset_name, batch)
        assert meta is not None
        assert meta["row_count"] == 3
        assert meta["distinct_id_count"] == 3
        assert meta["min_id"] == "a1"
        assert meta["max_id"] == "a3"

        # Index partition exists and is sorted by (id, _batch).
        index_key = index_parquet_key(prefix, dataset_name, batch)
        resp = await client.get_object(Bucket=bucket, Key=index_key)
        index_bytes = await resp["Body"].read()
        table = pq.read_table(io.BytesIO(index_bytes))
        ids = table.column("id").to_pylist()
        assert ids == ["a1", "a2", "a3"]
        assert table.column_names == ["id", "_batch"]

        # Meta file exists with the same content.
        resp = await client.get_object(
            Bucket=bucket, Key=index_meta_key(prefix, dataset_name, batch)
        )
        stored_meta = json.loads(await resp["Body"].read())
        assert stored_meta["row_count"] == 3

    @pytest.mark.asyncio
    async def test_no_merged_parquet_returns_none(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        meta = await update_batch_index(client, bucket, prefix, dataset_name, "missing")
        assert meta is None

    @pytest.mark.asyncio
    async def test_enumerate_batches_ignores_index(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        await client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{dataset_name}/b1/merged.parquet",
            Body=_parquet_bytes(
                [{"id": "a1", "_batch": '"b1"', "text": "a"}]
            ),
        )
        await update_batch_index(client, bucket, prefix, dataset_name, "b1")
        batches = await enumerate_batches(client, bucket, prefix, dataset_name)
        assert batches == ["b1"]

    @pytest.mark.asyncio
    async def test_writes_index_from_jsonl_blocks(self, s3_env):
        import gzip

        client, bucket, prefix, dataset_name = s3_env
        batch = "b1"
        blocks = [
            {"id": "a1", "_batch": '"b1"'},
            {"id": "a2", "_batch": '"b1"'},
        ]
        body = gzip.compress(
            "\n".join(json.dumps(r) for r in blocks).encode("utf-8")
        )
        await client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{dataset_name}/{batch}/merged_00000.jsonl.gz",
            Body=body,
        )

        block_meta = [
            {"file": "merged_00000.jsonl.gz", "row_count": 2, "min_id": "a1", "max_id": "a2"}
        ]
        meta = await update_batch_index(
            client,
            bucket,
            prefix,
            dataset_name,
            batch,
            merged_jsonl_glob=f"{prefix}/{dataset_name}/{batch}/merged_*.jsonl.gz",
            blocks=block_meta,
        )
        assert meta is not None
        assert meta["row_count"] == 2
        assert meta["min_id"] == "a1"
        assert meta["max_id"] == "a2"
        assert meta["format"] == "jsonl"
        assert meta["blocks"] == block_meta

        index_key = index_parquet_key(prefix, dataset_name, batch)
        resp = await client.get_object(Bucket=bucket, Key=index_key)
        table = pq.read_table(io.BytesIO(await resp["Body"].read()))
        assert table.column("id").to_pylist() == ["a1", "a2"]
