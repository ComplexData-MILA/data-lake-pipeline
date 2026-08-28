"""Tests for the NDJSON streaming data endpoint."""

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


@pytest.fixture
async def ndjson_dataset():
    session = aioboto3.Session(
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    kwargs = {}
    if os.environ.get("S3_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")
    dataset_name = f"ndjson_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        rows = [
            {"id": f"n{i}", "text": f"text {i}", "value": i, "_batch": "b1"}
            for i in range(4)
        ]
        await client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/{dataset_name}/b1/merged.parquet",
            Body=_parquet_bytes(rows),
        )
        yield {"dataset_name": dataset_name, "bucket": bucket, "prefix": prefix}

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


class TestNdjsonStream:
    def test_stream_returns_meta_rows_done(self, client, ndjson_dataset):
        d = ndjson_dataset["dataset_name"]
        with client.stream(
            "GET",
            f"/datasets/{d}/data",
            params={
                "format": "ndjson",
                "page": 1,
                "page_size": 50,
                "columns": "id,text,value",
            },
        ) as response:
            assert response.status_code == 200
            assert "application/x-ndjson" in response.headers.get("content-type", "")
            lines = [line for line in response.iter_lines() if line.strip()]

        messages = [json.loads(line) for line in lines]
        types = [m["type"] for m in messages]
        assert types[0] == "meta"
        assert types[-1] == "done"
        rows = [m["row"] for m in messages if m["type"] == "row"]
        assert len(rows) == 4
        assert {r["id"] for r in rows} == {"n0", "n1", "n2", "n3"}
        meta = messages[0]
        assert "id" in meta["columns"]

    def test_early_disconnect_leaves_pool_healthy(self, client, ndjson_dataset):
        d = ndjson_dataset["dataset_name"]
        with client.stream(
            "GET",
            f"/datasets/{d}/data",
            params={"format": "ndjson", "page_size": 50, "columns": "id"},
        ) as response:
            iterator = response.iter_lines()
            next(iterator)  # read the meta line, then abandon the stream
        # Pool must still serve queries after the aborted stream.
        resp = client.get(f"/datasets/{d}/count")
        assert resp.status_code == 200, resp.text
        assert resp.json()["count"] == 4

    def test_json_format_still_default(self, client, ndjson_dataset):
        d = ndjson_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data", params={"page_size": 50, "columns": "id"}
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "rows" in data
        assert len(data["rows"]) == 4
