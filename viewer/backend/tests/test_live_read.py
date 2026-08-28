"""Integration tests for the live JSONL read path (Phase 2).

Rows appear in the viewer the instant chunks are uploaded — merged parquet is
unioned with unmerged ``*_chunk_*.jsonl`` files (and ``.temp`` annotation
chunks), with the GROUP BY dedup absorbing chunk/parquet overlap.
"""

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
from viewer.backend.s3_files import build_file_manifest

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
async def live_dataset():
    """Dataset with merged parquet + a live JSONL chunk + a live annotation chunk."""
    session = aioboto3.Session(
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    kwargs = {}
    if os.environ.get("S3_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")

    dataset_name = f"live_test_{uuid.uuid4().hex[:8]}"

    async with session.client("s3", **kwargs) as client:
        merged_rows = [
            {"id": "row1", "text": "hello", "value": 10, "_batch": "batch1"},
            {"id": "row2", "text": "world", "value": 20, "_batch": "batch1"},
        ]
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/batch1/merged.parquet",
            _parquet_bytes(merged_rows),
        )

        # Live chunk: row3 (new), row1 again (overlap with merged), row4 with a
        # JSONL-only column, and a corrupted line that must be skipped.
        lines = [
            json.dumps({"id": "row3", "text": "live", "value": 30, "_batch": "batch1"}),
            json.dumps({"id": "row1", "text": "hello", "value": 10, "_batch": "batch1"}),
            json.dumps(
                {
                    "id": "row4",
                    "text": "extra",
                    "value": 40,
                    "_batch": "batch1",
                    "extra_col": "only-jsonl",
                }
            ),
            "{corrupted json!!!",
        ]
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/batch1/abcd1234_chunk_00000.jsonl",
            ("\n".join(lines) + "\n").encode(),
        )

        # Live annotation chunk for annotator1.
        ann_lines = [json.dumps({"id": "row1", "label": "positive"})]
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/annotations/annotator1/batch1/.temp/chunk_00000.jsonl",
            ("\n".join(ann_lines) + "\n").encode(),
        )

        yield {
            "dataset_name": dataset_name,
            "bucket": bucket,
            "prefix": prefix,
        }

        await _cleanup(client, bucket, prefix, dataset_name)


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


class TestFileManifest:
    def test_manifest_classification(self, live_dataset):
        """Manifest classifies merged, live, and annotation-live objects."""
        from viewer.backend.main import get_s3_client

        client = get_s3_client()
        manifest = build_file_manifest(
            client, live_dataset["bucket"], live_dataset["prefix"], live_dataset["dataset_name"]
        )
        assert len(manifest.merged_parquet) == 1
        assert len(manifest.live_jsonl) == 1
        assert "annotator1" in manifest.annotators
        assert len(manifest.annotators["annotator1"].live_jsonl) == 1
        assert manifest.annotators["annotator1"].merged_parquet == []


class TestLiveRead:
    def test_live_rows_returned_and_deduped(self, client, live_dataset):
        """/data returns merged + live rows; chunk/parquet overlap appears once."""
        d = live_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params={"page": 1, "page_size": 50, "columns": "id,text,value,extra_col"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        rows_by_id = {r["id"]: r for r in data["rows"]}
        assert set(rows_by_id) == {"row1", "row2", "row3", "row4"}
        assert rows_by_id["row3"]["text"] == "live"
        assert rows_by_id["row4"]["extra_col"] == "only-jsonl"

    def test_count_includes_live_rows(self, client, live_dataset):
        d = live_dataset["dataset_name"]
        resp = client.get(f"/datasets/{d}/count")
        assert resp.status_code == 200, resp.text
        assert resp.json()["count"] == 4

    def test_typed_column_from_jsonl_matches_parquet(self, client, live_dataset):
        """Integer column unifies across the parquet/JSONL union."""
        d = live_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params={"page": 1, "page_size": 50, "columns": "id,value"},
        )
        assert resp.status_code == 200, resp.text
        rows_by_id = {r["id"]: r["value"] for r in resp.json()["rows"]}
        assert rows_by_id["row1"] == 10
        assert rows_by_id["row3"] == 30

    def test_schema_includes_jsonl_only_column(self, client, live_dataset):
        d = live_dataset["dataset_name"]
        resp = client.get(f"/datasets/{d}/schema")
        assert resp.status_code == 200, resp.text
        names = {c["name"] for c in resp.json()["columns"]}
        assert "extra_col" in names

    def test_annotator_live_chunk_joined(self, client, live_dataset):
        d = live_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params={
                "page": 1,
                "page_size": 50,
                "columns": "id",
                "annotator_columns": json.dumps({"annotator1": ["label"]}),
            },
        )
        assert resp.status_code == 200, resp.text
        rows_by_id = {r["id"]: r for r in resp.json()["rows"]}
        assert rows_by_id["row1"]["annotator1.label"] == "positive"
        assert rows_by_id["row2"]["annotator1.label"] is None
        # row count unchanged: LEFT JOIN on unfiltered annotator
        assert len(rows_by_id) == 4

    def test_row_by_id_finds_live_row(self, client, live_dataset):
        d = live_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params={"row_id": "row3", "columns": "id,text"},
        )
        assert resp.status_code == 200, resp.text
        rows = resp.json()["rows"]
        assert len(rows) == 1
        assert rows[0]["text"] == "live"

    def test_data_defaults_to_all_columns(self, client, live_dataset):
        """With no `columns` param, /data returns the full base schema."""
        d = live_dataset["dataset_name"]
        resp = client.get(
            f"/datasets/{d}/data",
            params={"page": 1, "page_size": 50},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        # Not just ["id", "_batch"]: every base schema column is present.
        assert set(data["columns"]) >= {"id", "_batch", "text", "value"}
        first = data["rows"][0]
        assert "text" in first
        assert "value" in first


class TestJsonlOnlyDataset:
    @pytest.mark.asyncio
    async def test_jsonl_only_dataset_visible_and_readable(self, client):
        """A dataset with only live chunks (no merged.parquet) is listed and readable."""
        session = aioboto3.Session(
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        )
        kwargs = {}
        if os.environ.get("S3_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
        bucket = os.environ.get("S3_BUCKET", "test-bucket")
        prefix = os.environ.get("S3_PREFIX", "datasets")
        dataset_name = f"live_only_{uuid.uuid4().hex[:8]}"

        async with session.client("s3", **kwargs) as s3:
            lines = [json.dumps({"id": "only1", "text": "fresh", "_batch": "batch1"})]
            await _put(
                s3, bucket,
                f"{prefix}/{dataset_name}/batch1/beef1234_chunk_00000.jsonl",
                ("\n".join(lines) + "\n").encode(),
            )
            try:
                resp = client.get("/datasets")
                assert dataset_name in resp.json()["datasets"]
                resp = client.get(
                    f"/datasets/{dataset_name}/data",
                    params={"page": 1, "page_size": 10, "columns": "id,text"},
                )
                assert resp.status_code == 200, resp.text
                rows = resp.json()["rows"]
                assert [r["id"] for r in rows] == ["only1"]
                resp = client.get(f"/datasets/{dataset_name}/count")
                assert resp.json()["count"] == 1
            finally:
                await _cleanup(s3, bucket, prefix, dataset_name)


class TestMergedBlocks:
    """Merged JSONL blocks (the new merged format) behave like merged parquet."""

    async def _make_blocks_dataset(self):
        import gzip

        from s3_data_tool.index import update_batch_index

        session = aioboto3.Session(
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        )
        kwargs = {}
        if os.environ.get("S3_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
        bucket = os.environ.get("S3_BUCKET", "test-bucket")
        prefix = os.environ.get("S3_PREFIX", "datasets")
        dataset_name = f"blocks_{uuid.uuid4().hex[:8]}"

        client_ctx = session.client("s3", **kwargs)
        client = await client_ctx.__aenter__()

        def block_rows(rows):
            return gzip.compress(
                ("\n".join(json.dumps(r) for r in rows) + "\n").encode()
            )

        block1 = [
            {"id": "b1", "_batch": '"batch1"', "text": '"one"'},
            {"id": "b2", "_batch": '"batch1"', "text": '"two"'},
        ]
        block2 = [{"id": "b3", "_batch": '"batch1"', "text": '"three"'}]
        await _put(client, bucket, f"{prefix}/{dataset_name}/batch1/merged_00000.jsonl.gz", block_rows(block1))
        await _put(client, bucket, f"{prefix}/{dataset_name}/batch1/merged_00001.jsonl.gz", block_rows(block2))
        await update_batch_index(
            client, bucket, prefix, dataset_name, "batch1",
            merged_jsonl_glob=f"{prefix}/{dataset_name}/batch1/merged_*.jsonl.gz",
            blocks=[
                {"file": "merged_00000.jsonl.gz", "row_count": 2, "min_id": "b1", "max_id": "b2"},
                {"file": "merged_00001.jsonl.gz", "row_count": 1, "min_id": "b3", "max_id": "b3"},
            ],
        )
        # Live chunk overlapping block1 (b1 again) — dedup must absorb it.
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/batch1/cafe1234_chunk_00000.jsonl",
            ('{"id": "b1", "_batch": "\\"batch1\\"", "text": "\\"one\\""}\n'
             '{"id": "b9", "_batch": "\\"batch1\\"", "text": "\\"live\\""}\n').encode(),
        )
        # Annotation merged block.
        await _put(
            client, bucket,
            f"{prefix}/{dataset_name}/annotations/ann1/batch1/merged_00000.jsonl.gz",
            block_rows([{"id": "b1", "label": '"x"'}]),
        )

        try:
            yield {"dataset_name": dataset_name, "bucket": bucket, "prefix": prefix}
        finally:
            await _cleanup(client, bucket, prefix, dataset_name)
            await client_ctx.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_manifest_classification_and_read(self, client):
        """Blocks classify as merged_jsonl and serve through /data with dedup."""
        from viewer.backend.main import get_s3_client

        async for ds in self._make_blocks_dataset():
            s3 = get_s3_client()
            manifest = build_file_manifest(s3, ds["bucket"], ds["prefix"], ds["dataset_name"])
            assert len(manifest.merged_jsonl) == 2
            assert len(manifest.merged_parquet) == 0
            assert len(manifest.live_jsonl) == 1
            assert len(manifest.annotators["ann1"].merged_jsonl) == 1

            resp = client.get(
                f"/datasets/{ds['dataset_name']}/data",
                params={"page_size": 50, "columns": "id,text"},
            )
            assert resp.status_code == 200, resp.text
            rows = resp.json()["rows"]
            # b1..b3 from blocks + live b9; chunk/block overlap appears once.
            assert [r["id"] for r in rows] == ["b1", "b2", "b3", "b9"]

            resp = client.get(f"/datasets/{ds['dataset_name']}/count")
            assert resp.json()["count"] == 4

    @pytest.mark.asyncio
    async def test_late_block_column_not_dropped(self, client):
        """maximum_sample_files=-1 keeps columns that only appear in late blocks."""
        import gzip

        session = aioboto3.Session(
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        )
        kwargs = {}
        if os.environ.get("S3_ENDPOINT_URL"):
            kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
        bucket = os.environ.get("S3_BUCKET", "test-bucket")
        prefix = os.environ.get("S3_PREFIX", "datasets")
        dataset_name = f"latecol_{uuid.uuid4().hex[:8]}"

        async with session.client("s3", **kwargs) as s3:
            for i in range(40):
                row = {"id": f"r{i:03d}", "_batch": '"b"'}
                if i >= 35:
                    row["late_col"] = '"x"'
                await _put(
                    s3, bucket,
                    f"{prefix}/{dataset_name}/b/merged_{i:05d}.jsonl.gz",
                    gzip.compress((json.dumps(row) + "\n").encode()),
                )
            try:
                resp = client.get(
                    f"/datasets/{dataset_name}/data",
                    params={"page_size": 100, "columns": "id,late_col"},
                )
                assert resp.status_code == 200, resp.text
                rows = resp.json()["rows"]
                by_id = {r["id"]: r["late_col"] for r in rows}
                assert by_id["r039"] == '"x"'  # late column visible (stored form)
            finally:
                await _cleanup(s3, bucket, prefix, dataset_name)
