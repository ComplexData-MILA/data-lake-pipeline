"""Integration tests for GET /datasets/{d}/conversion (conversion progress)."""

import io
import json
import os
import uuid

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from viewer.backend.main import app

pytestmark = pytest.mark.integration


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


async def _put(client, bucket, key, body):
    await client.put_object(Bucket=bucket, Key=key, Body=body)


async def _cleanup(client, bucket, prefix, dataset_name):
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    async for page in paginator.paginate(
        Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"
    ):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    for key in keys:
        await client.delete_object(Bucket=bucket, Key=key)


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


class TestConversionEndpoint:
    @pytest.mark.asyncio
    async def test_returns_status_json_when_present(self, client):
        session = aioboto3.Session(
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        )
        kwargs = {}
        if os.environ.get("S3_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
        bucket = os.environ.get("S3_BUCKET", "test-bucket")
        prefix = os.environ.get("S3_PREFIX", "datasets")
        dataset_name = f"conv_ep_{uuid.uuid4().hex[:8]}"

        status = {
            "dataset": dataset_name,
            "total_batches": 12,
            "converted": 7,
            "in_progress_batch": "b8",
            "annotation_total": 3,
            "annotation_converted": 2,
            "oversized": False,
            "started_at": "2026-08-27T10:00:00+00:00",
            "updated_at": "2026-08-27T10:23:11+00:00",
            "error": None,
        }
        async with session.client("s3", **kwargs) as s3:
            await _put(
                s3, bucket,
                f"{prefix}/{dataset_name}/_migration/status.json",
                json.dumps(status).encode(),
            )
            try:
                resp = client.get(f"/datasets/{dataset_name}/conversion")
                assert resp.status_code == 200, resp.text
                data = resp.json()
                assert data["total_batches"] == 12
                assert data["converted"] == 7
                assert data["in_progress_batch"] == "b8"
                assert data["oversized"] is False
            finally:
                await _cleanup(s3, bucket, prefix, dataset_name)

    @pytest.mark.asyncio
    async def test_derives_pending_from_manifest_when_absent(self, client):
        session = aioboto3.Session(
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        )
        kwargs = {}
        if os.environ.get("S3_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
        bucket = os.environ.get("S3_BUCKET", "test-bucket")
        prefix = os.environ.get("S3_PREFIX", "datasets")
        dataset_name = f"conv_ep2_{uuid.uuid4().hex[:8]}"

        async with session.client("s3", **kwargs) as s3:
            # b1 still on parquet (pending); b2 already blocks (converted).
            await _put(
                s3, bucket,
                f"{prefix}/{dataset_name}/b1/merged.parquet",
                _parquet_bytes([{"id": "a1", "_batch": '"b1"'}]),
            )
            import gzip
            await _put(
                s3, bucket,
                f"{prefix}/{dataset_name}/b2/merged_00000.jsonl.gz",
                gzip.compress(b'{"id": "a2", "_batch": "\\"b2\\""}\n'),
            )
            try:
                resp = client.get(f"/datasets/{dataset_name}/conversion")
                assert resp.status_code == 200, resp.text
                data = resp.json()
                assert data["total_batches"] == 2
                assert data["converted"] == 1
                assert data["oversized"] is False
            finally:
                await _cleanup(s3, bucket, prefix, dataset_name)

    @pytest.mark.asyncio
    async def test_missing_dataset_returns_defaults(self, client):
        resp = client.get("/datasets/no_such_dataset_xyz/conversion")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["total_batches"] == 0
        assert data["converted"] == 0
