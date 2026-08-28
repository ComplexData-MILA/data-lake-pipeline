"""Integration tests for index-backed keyset pagination and fast counts (Phase 5)."""

import io
import json
import os
import uuid

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from s3_data_tool.index import update_batch_index
from viewer.backend.main import app

pytestmark = pytest.mark.integration


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


async def _put(client, bucket, key, body):
    await client.put_object(Bucket=bucket, Key=key, Body=body)


@pytest.fixture
async def keyset_dataset():
    """Two batches with a cross-batch duplicate id, index partitions, and a live chunk.

    Distinct (id, _batch) pairs sorted by id:
      (a1,b1), (a2,b2), (a3,b1), (a3,b2), (a4,b2), (a5,b1), (a6,b2-live)
    COUNT(DISTINCT id) = 6.
    """
    session = aioboto3.Session(
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    kwargs = {}
    if os.environ.get("S3_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")

    dataset_name = f"keyset_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        b1_rows = [
            {"id": "a1", "_batch": '"b1"', "text": "one", "value": 1},
            {"id": "a3", "_batch": '"b1"', "text": "three", "value": 3},
            {"id": "a5", "_batch": '"b1"', "text": "five", "value": 5},
        ]
        b2_rows = [
            {"id": "a2", "_batch": '"b2"', "text": "two", "value": 2},
            {"id": "a4", "_batch": '"b2"', "text": "four", "value": 4},
            {"id": "a3", "_batch": '"b2"', "text": "three-b", "value": 30},
        ]
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/b1/merged.parquet",
            _parquet_bytes(b1_rows),
        )
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/b2/merged.parquet",
            _parquet_bytes(b2_rows),
        )
        await update_batch_index(client, bucket, prefix, dataset_name, "b1")
        await update_batch_index(client, bucket, prefix, dataset_name, "b2")

        # Live chunk: a6 (new) + a1 again (chunk/parquet overlap).
        live_lines = [
            json.dumps({"id": "a6", "_batch": '"b2"', "text": "six", "value": 6}),
            json.dumps({"id": "a1", "_batch": '"b1"', "text": "one", "value": 1}),
        ]
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/b2/deadbeef_chunk_00000.jsonl",
            ("\n".join(live_lines) + "\n").encode(),
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


class TestKeysetPagination:
    def test_cursor_chaining_walks_whole_dataset(self, client, keyset_dataset):
        """Cursor chaining covers all (id,_batch) pairs in id order, no overlap."""
        d = keyset_dataset["dataset_name"]
        seen = []
        cursor = None
        for _ in range(10):
            params = {"page_size": 3, "columns": "id,text,value"}
            if cursor is not None:
                params["cursor"] = cursor
            resp = client.get(f"/datasets/{d}/data", params=params)
            assert resp.status_code == 200, resp.text
            data = resp.json()
            seen.extend((r["id"], r["_batch"]) for r in data["rows"])
            if not data["has_more"]:
                break
            cursor = data["next_cursor"]
            assert cursor is not None

        assert seen == [
            ("a1", '"b1"'),
            ("a2", '"b2"'),
            ("a3", '"b1"'),
            ("a3", '"b2"'),
            ("a4", '"b2"'),
            ("a5", '"b1"'),
            ("a6", '"b2"'),
        ]

    def test_overlap_row_appears_once(self, client, keyset_dataset):
        """A row present in both a chunk and merged parquet appears once."""
        d = keyset_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data", params={"page_size": 50, "columns": "id"}
        )
        data = resp.json()
        ids = [r["id"] for r in data["rows"]]
        assert ids.count("a1") == 1
        assert len(ids) == 7

    def test_first_page_next_cursor_and_has_more(self, client, keyset_dataset):
        d = keyset_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data", params={"page_size": 3, "columns": "id"}
        )
        data = resp.json()
        assert data["has_more"] is True
        assert json.loads(data["next_cursor"]) == ["a3", '"b1"']
        assert [r["id"] for r in data["rows"]] == ["a1", "a2", "a3"]

    def test_last_page_has_more_false(self, client, keyset_dataset):
        d = keyset_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data", params={"page_size": 50, "columns": "id"}
        )
        data = resp.json()
        assert data["has_more"] is False
        assert json.loads(data["next_cursor"]) == ["a6", '"b2"']


class TestKeysetCount:
    def test_count_fast_path_matches_expected(self, client, keyset_dataset):
        d = keyset_dataset["dataset_name"]
        resp = client.get(f"/datasets/{d}/count")
        assert resp.status_code == 200, resp.text
        assert resp.json()["count"] == 6  # distinct ids (a3 duplicated across batches)


class TestKeysetFallback:
    def test_filter_uses_ordering_path(self, client, keyset_dataset):
        """Filtered requests page through a materialized ordering with cursors."""
        d = keyset_dataset["dataset_name"]
        filters = json.dumps({"base": {"field": "value", "op": "gte", "value": 5}})
        resp = client.get(
            f"/datasets/{d}/data",
            params={
                "page_size": 2,
                "columns": "id,value",
                "filters": filters,
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        values = {r["id"]: r["value"] for r in data["rows"]}
        # value >= 5, id order: a3/b2=30, a5=5, a6=6 (a3/b1=3 excluded)
        assert values == {"a3": 30, "a5": 5}
        assert data["next_cursor"] is not None
        assert data["has_more"] is True

        # Cursor chains over the filtered set with no overlap.
        seen = dict(values)
        cursor = data["next_cursor"]
        while cursor:
            resp = client.get(
                f"/datasets/{d}/data",
                params={
                    "page_size": 2,
                    "columns": "id,value",
                    "filters": filters,
                    "cursor": cursor,
                },
            )
            assert resp.status_code == 200, resp.text
            data = resp.json()
            for r in data["rows"]:
                assert r["id"] not in seen
                seen[r["id"]] = r["value"]
            cursor = data["next_cursor"] if data["has_more"] else None
        assert seen == {"a3": 30, "a5": 5, "a6": 6}

    def test_sort_by_other_column_uses_ordering(self, client, keyset_dataset):
        d = keyset_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params={"page_size": 50, "columns": "id,value", "sort": "value", "sort_dir": "desc"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        # value desc: a3/b2=30, a6=6, a5=5, a4=4, a3/b1=3, a2=2, a1=1
        assert [r["id"] for r in data["rows"]] == [
            "a3", "a6", "a5", "a4", "a3", "a2", "a1",
        ]
        # The ordering path returns keyset fields.
        assert data["has_more"] is False
