"""Integration tests for the parquet->JSONL conversion job (s3_data_tool/convert.py)."""

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

import s3_data_tool.convert as convert
from s3_data_tool.convert import migration_status_key, run_conversion
from s3_data_tool.s3_utils import delete_objects

pytestmark = pytest.mark.integration


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


async def _list_keys(client, bucket, prefix) -> list[str]:
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return sorted(keys)


async def _read_blocks(client, bucket, keys) -> list[dict]:
    rows = []
    for key in keys:
        resp = await client.get_object(Bucket=bucket, Key=key)
        body = gzip.decompress(await resp["Body"].read())
        for line in body.decode("utf-8").strip().split("\n"):
            if line.strip():
                rows.append(json.loads(line))
    return rows


async def _read_json(client, bucket, key) -> dict:
    resp = await client.get_object(Bucket=bucket, Key=key)
    return json.loads(await resp["Body"].read())


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
    dataset_name = f"convert_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        yield client, bucket, prefix, dataset_name

        keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")
        await delete_objects(client, bucket, keys)


async def _seed_dataset(client, bucket, prefix, dataset_name) -> None:
    """Two base batches + one annotation batch, all legacy parquet."""
    await client.put_object(
        Bucket=bucket, Key=f"{prefix}/{dataset_name}/b1/merged.parquet",
        Body=_parquet_bytes(
            [{"id": "a3", "_batch": '"b1"', "text": "c"},
             {"id": "a1", "_batch": '"b1"', "text": "a"}]
        ),
    )
    await client.put_object(
        Bucket=bucket, Key=f"{prefix}/{dataset_name}/b2/merged.parquet",
        Body=_parquet_bytes([{"id": "a2", "_batch": '"b2"', "text": "b"}]),
    )
    await client.put_object(
        Bucket=bucket,
        Key=f"{prefix}/{dataset_name}/annotations/ann1/b1/merged.parquet",
        Body=_parquet_bytes([{"id": "a1", "_batch": '"b1"', "label": "x"}]),
    )


class TestRunConversion:
    @pytest.mark.asyncio
    async def test_converts_parquet_to_sorted_blocks(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        await _seed_dataset(client, bucket, prefix, dataset_name)

        await run_conversion(client, bucket, prefix, datasets=[dataset_name])

        keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")
        # All merged.parquet replaced by blocks; chunks untouched.
        assert not [k for k in keys if k.endswith("merged.parquet")]
        block_keys = [k for k in keys if k.endswith(".jsonl.gz")]
        assert block_keys

        base_blocks = [k for k in block_keys if "/annotations/" not in k]
        rows = await _read_blocks(client, bucket, base_blocks)
        pairs = [(r["id"], r["_batch"]) for r in rows]
        # Blocks are id-sorted within each batch; global order is produced by
        # the viewer's keyset merge over the index partitions.
        assert sorted(pairs) == [("a1", '"b1"'), ("a2", '"b2"'), ("a3", '"b1"')]
        by_batch = {}
        for key in base_blocks:
            batch = key.split("/")[-2]
            by_batch.setdefault(batch, []).extend(
                (r["id"], r["_batch"])
                for r in await _read_blocks(client, bucket, [key])
            )
        for batch_pairs in by_batch.values():
            assert batch_pairs == sorted(batch_pairs)

        # Index + meta updated for converted batches.
        meta = await _read_json(
            client, bucket, f"{prefix}/{dataset_name}/_index/b1.meta.json"
        )
        assert meta["format"] == "jsonl"
        assert meta["row_count"] == 2
        assert meta["blocks"][0]["min_id"] == "a1"

        # Annotation batch converted too (no index for annotations).
        ann_blocks = [k for k in block_keys if "/annotations/" in k]
        assert ann_blocks

        status = await _read_json(
            client, bucket, migration_status_key(prefix, dataset_name)
        )
        assert status["converted"] == 2
        assert status["total_batches"] == 2
        assert status["annotation_converted"] == 1
        assert status["annotation_total"] == 1
        assert status["oversized"] is False

    @pytest.mark.asyncio
    async def test_idempotent_rerun(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        await _seed_dataset(client, bucket, prefix, dataset_name)

        await run_conversion(client, bucket, prefix, datasets=[dataset_name])
        first_keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")
        await run_conversion(client, bucket, prefix, datasets=[dataset_name])
        second_keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")

        assert first_keys == second_keys

    @pytest.mark.asyncio
    async def test_oversized_dataset_skipped(self, s3_env, monkeypatch):
        client, bucket, prefix, dataset_name = s3_env
        await _seed_dataset(client, bucket, prefix, dataset_name)

        monkeypatch.setattr(convert, "CONVERT_MAX_DATASET_BYTES", 1)
        await run_conversion(client, bucket, prefix, datasets=[dataset_name])

        keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")
        assert any(k.endswith("merged.parquet") for k in keys)
        assert not any(k.endswith(".jsonl.gz") for k in keys)
        status = await _read_json(
            client, bucket, migration_status_key(prefix, dataset_name)
        )
        assert status["oversized"] is True

    @pytest.mark.asyncio
    async def test_crash_recovery_redoes_partial_batch(self, s3_env):
        client, bucket, prefix, dataset_name = s3_env
        await _seed_dataset(client, bucket, prefix, dataset_name)
        # Simulate a crash mid-conversion: blocks published, parquet still there.
        await client.put_object(
            Bucket=bucket, Key=f"{prefix}/{dataset_name}/b1/merged_00000.jsonl.gz",
            Body=gzip.compress(b'{"id": "stale", "_batch": "\\"b1\\""}\n'),
        )

        await run_conversion(client, bucket, prefix, datasets=[dataset_name])

        keys = await _list_keys(client, bucket, f"{prefix}/{dataset_name}")
        assert not [k for k in keys if k.endswith("merged.parquet")]
        rows = await _read_blocks(
            client, bucket, [k for k in keys if k.endswith(".jsonl.gz") and "/annotations/" not in k]
        )
        assert sorted(r["id"] for r in rows) == ["a1", "a2", "a3"]  # stale block redone
        assert all(r["id"] != "stale" for r in rows)
