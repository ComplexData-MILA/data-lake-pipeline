"""Integration tests for the materialized ordering cache (filtered/sorted paging)."""

import io
import json
import os
import time
import uuid

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from s3_data_tool.index import update_batch_index
from viewer.backend.main import app
from viewer.backend import db, orderings

pytestmark = pytest.mark.integration


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


async def _put(client, bucket, key, body):
    await client.put_object(Bucket=bucket, Key=key, Body=body)


@pytest.fixture
async def ordering_dataset(tmp_path):
    """One batch of merged parquet + index: ids a1..a7 with value 1..7."""
    session = aioboto3.Session(
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    kwargs = {}
    if os.environ.get("S3_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")
    dataset_name = f"ordering_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        rows = [
            {"id": f"a{i}", "_batch": '"b1"', "value": i}
            for i in range(1, 8)
        ]
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/b1/merged.parquet",
            _parquet_bytes(rows),
        )
        await update_batch_index(client, bucket, prefix, dataset_name, "b1")

        yield {
            "dataset_name": dataset_name,
            "bucket": bucket,
            "prefix": prefix,
        }

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
def client(tmp_path, monkeypatch):
    # Isolate ordering files per test run.
    monkeypatch.setattr(db, "DUCKDB_CACHE_DIR", str(tmp_path / "cache"))
    with TestClient(app) as test_client:
        yield test_client


def _filter_params(**extra):
    params = {"page_size": 3, "columns": "id,value"}
    params.update(extra)
    return params


class TestOrderingPath:
    def test_filtered_paging_with_cursor_chaining(self, client, ordering_dataset):
        d = ordering_dataset["dataset_name"]
        filters = json.dumps({"base": {"field": "value", "op": "gte", "value": 3}})
        # Filtered set: a3..a7 in id order.
        seen = []
        cursor = None
        while True:
            params = _filter_params(filters=filters)
            if cursor:
                params["cursor"] = cursor
            resp = client.get(f"/datasets/{d}/data", params=params)
            assert resp.status_code == 200, resp.text
            data = resp.json()
            seen.extend(r["id"] for r in data["rows"])
            assert data["next_cursor"] is not None
            if not data["has_more"]:
                break
            cursor = data["next_cursor"]
        assert seen == ["a3", "a4", "a5", "a6", "a7"]

    def test_page_param_without_cursor_matches_chain(self, client, ordering_dataset):
        d = ordering_dataset["dataset_name"]
        filters = json.dumps({"base": {"field": "value", "op": "gte", "value": 3}})
        resp = client.get(
            f"/datasets/{d}/data", params=_filter_params(filters=filters, page=2)
        )
        assert resp.status_code == 200, resp.text
        assert [r["id"] for r in resp.json()["rows"]] == ["a6", "a7"]

    def test_non_id_sort_desc(self, client, ordering_dataset):
        d = ordering_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params=_filter_params(sort="value", sort_dir="desc", page_size=50),
        )
        assert resp.status_code == 200, resp.text
        assert [r["id"] for r in resp.json()["rows"]] == [
            "a7", "a6", "a5", "a4", "a3", "a2", "a1",
        ]

    def test_stale_ordering_cursor_treated_as_first_page(
        self, client, ordering_dataset
    ):
        d = ordering_dataset["dataset_name"]
        filters = json.dumps({"base": {"field": "value", "op": "gte", "value": 3}})
        resp = client.get(
            f"/datasets/{d}/data",
            params=_filter_params(filters=filters, cursor=json.dumps(["deadbeef", 0])),
        )
        assert resp.status_code == 200, resp.text
        # Hash mismatch -> first page of the filtered set.
        assert [r["id"] for r in resp.json()["rows"]] == ["a3", "a4", "a5"]

    def test_filtered_count_uses_ordering(self, client, ordering_dataset):
        d = ordering_dataset["dataset_name"]
        filters = json.dumps({"base": {"field": "value", "op": "lte", "value": 4}})
        # First page materializes the ordering; then count reads from it.
        client.get(f"/datasets/{d}/data", params=_filter_params(filters=filters))
        resp = client.get(f"/datasets/{d}/count", params={"filters": filters})
        assert resp.status_code == 200, resp.text
        assert resp.json()["count"] == 4

    def test_expired_ordering_rebuilds(self, client, ordering_dataset):
        d = ordering_dataset["dataset_name"]
        filters = json.dumps({"base": {"field": "value", "op": "gte", "value": 3}})
        rsh = orderings.rowset_hash({}, json.loads(filters))
        oh = orderings.order_hash(rsh, None, "asc")
        path = orderings.ordering_path(d, rsh, oh)

        client.get(f"/datasets/{d}/data", params=_filter_params(filters=filters))
        assert path.exists()
        # Age the file past the TTL.
        old = time.time() - (orderings.ORDERING_TTL + 10)
        os.utime(path, (old, old))

        resp = client.get(f"/datasets/{d}/data", params=_filter_params(filters=filters))
        assert resp.status_code == 200, resp.text
        assert [r["id"] for r in resp.json()["rows"]] == ["a3", "a4", "a5"]
        assert path.stat().st_mtime > old  # rebuilt
