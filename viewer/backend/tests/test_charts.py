"""Integration tests for the /activity and /categorical chart endpoints.

Fixtures build datasets whose rows carry _created_at timestamps in both
storage encodings (raw strings in parquet, JSON-quoted strings in live JSONL
chunks — matching the pipeline's transform_row_for_jsonl convention) and one
row without the field (backward-compat rows must be ignored).
"""

import io
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

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
async def charts_datasets():
    """Two datasets: ds_a (merged + live chunk, mixed encodings) and ds_b."""
    session = aioboto3.Session(
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    kwargs = {}
    if os.environ.get("S3_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["S3_ENDPOINT_URL"]
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")

    names = [f"charts_{uuid.uuid4().hex[:8]}" for _ in range(3)]
    ds_a, ds_b, ds_c = names

    now = datetime.now(timezone.utc)
    ts1 = (now - timedelta(minutes=5)).isoformat()
    ts2 = (now - timedelta(minutes=4)).isoformat()

    async with session.client("s3", **kwargs) as client:
        # ds_a merged parquet: raw-string values; row3 has no _created_at.
        merged_rows = [
            {"id": "row1", "label": "positive", "_batch": "batch1", "_created_at": ts1},
            {"id": "row2", "label": "negative", "_batch": "batch1", "_created_at": ts1},
            {"id": "row3", "label": "ancient", "_batch": "batch1"},
        ]
        await _put(
            client, bucket,
            f"{prefix}/{ds_a}/batch1/merged.parquet",
            _parquet_bytes(merged_rows),
        )

        # ds_a live chunk: JSON-quoted values (pipeline convention). row1
        # overlaps the merged parquet with the same _created_at; row4/row5
        # are new.
        lines = [
            json.dumps(
                {"id": "row1", "label": "\"positive\"", "_batch": "batch1", "_created_at": json.dumps(ts1)}
            ),
            json.dumps(
                {"id": "row4", "label": "\"neutral\"", "_batch": "batch1", "_created_at": json.dumps(ts2)}
            ),
            json.dumps(
                {"id": "row5", "label": "\"positive\"", "_batch": "batch1", "_created_at": json.dumps(ts2)}
            ),
        ]
        await _put(
            client, bucket,
            f"{prefix}/{ds_a}/batch1/abcd1234_chunk_00000.jsonl",
            ("\n".join(lines) + "\n").encode(),
        )

        # ds_b: one merged row.
        await _put(
            client, bucket,
            f"{prefix}/{ds_b}/batch1/merged.parquet",
            _parquet_bytes(
                [
                    {
                        "id": "row1",
                        "label": "alpha",
                        "_batch": "batch1",
                        "_created_at": ts1,
                    }
                ]
            ),
        )

        # ds_c: only pre-_created_at rows (no file carries the column at all).
        await _put(
            client, bucket,
            f"{prefix}/{ds_c}/batch1/merged.parquet",
            _parquet_bytes(
                [{"id": "row1", "label": "old", "_batch": "batch1"}]
            ),
        )

        yield {
            "ds_a": ds_a,
            "ds_b": ds_b,
            "ds_c": ds_c,
            "bucket": bucket,
            "prefix": prefix,
            "ts1": ts1,
            "ts2": ts2,
        }

        for name in names:
            await _cleanup(client, bucket, prefix, name)


@pytest.fixture
def client(monkeypatch):
    # No Redis in tests: the endpoints must compute directly, otherwise
    # assertions become cache-dependent.
    monkeypatch.delenv("REDIS_URL", raising=False)
    with TestClient(app) as test_client:
        yield test_client


def _entry_for(resp: dict, dataset: str) -> dict:
    return next(d for d in resp["datasets"] if d["dataset"] == dataset)


class TestActivity:
    def test_buckets_across_datasets(self, client, charts_datasets):
        resp = client.get("/activity", params={"bucket": "1m", "minutes": 1440})
        assert resp.status_code == 200, resp.text
        data = resp.json()

        a = _entry_for(data, charts_datasets["ds_a"])
        total = sum(b["count"] for b in a["buckets"])
        # row1 (deduped), row2, row4, row5 — row3 has no _created_at.
        assert total == 4
        assert len(a["buckets"]) == 2  # ts1 and ts2 minute buckets

        b = _entry_for(data, charts_datasets["ds_b"])
        assert sum(x["count"] for x in b["buckets"]) == 1

        assert data["bucket"] == "1m"

    def test_window_bounds_apply(self, client, charts_datasets):
        # A window ending before all rows yields empty buckets.
        resp = client.get("/activity", params={"bucket": "1m", "minutes": 60})
        assert resp.status_code == 200
        data = resp.json()
        assert data["window"]["start"] is not None
        assert data["window"]["end"] is not None

    def test_invalid_bucket_422(self, client, charts_datasets):
        resp = client.get("/activity", params={"bucket": "2m"})
        assert resp.status_code == 422

    def test_dataset_without_created_at_has_empty_buckets(self, client, charts_datasets):
        """A dataset whose files predate _created_at yields empty buckets, not 500."""
        resp = client.get("/activity", params={"bucket": "1m", "minutes": 1440})
        assert resp.status_code == 200, resp.text
        entry = _entry_for(resp.json(), charts_datasets["ds_c"])
        assert entry["buckets"] == []

    def test_invalid_minutes_422(self, client, charts_datasets):
        resp = client.get("/activity", params={"minutes": 0})
        assert resp.status_code == 422

    def test_ndjson_stream_matches_json(self, client, charts_datasets):
        """The streaming variant yields the same per-dataset buckets."""
        json_data = client.get(
            "/activity", params={"bucket": "1m", "minutes": 1440}
        ).json()
        resp = client.get(
            "/activity", params={"bucket": "1m", "minutes": 1440, "format": "ndjson"}
        )
        assert resp.status_code == 200, resp.text
        lines = [json.loads(l) for l in resp.text.splitlines() if l.strip()]
        types = [l["type"] for l in lines]
        assert types[0] == "window"
        assert types[-1] == "done"
        streamed = {
            l["dataset"]: l["buckets"]
            for l in lines[1:-1]
            if l["type"] == "dataset"
        }
        expected = {
            d["dataset"]: d["buckets"] for d in json_data["datasets"]
        }
        assert streamed == expected

    def test_ndjson_window_line(self, client, charts_datasets):
        resp = client.get(
            "/activity", params={"bucket": "5m", "minutes": 60, "format": "ndjson"}
        )
        assert resp.status_code == 200, resp.text
        lines = [json.loads(l) for l in resp.text.splitlines() if l.strip()]
        window = lines[0]
        assert window["type"] == "window"
        assert window["bucket"] == "5m"
        assert window["window"]["start"] is not None
        assert window["window"]["end"] is not None
        assert all(l["type"] == "dataset" for l in lines[1:-1])
        assert lines[-1] == {"type": "done"}


class TestCategorical:
    def test_counts_topk_total_distinct(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "counts", "limit": 20},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["mode"] == "counts"
        assert data["total"] == 4
        assert data["distinct"] == 3
        assert data["truncated"] is False
        counts = {v["value"]: v["count"] for v in data["values"]}
        assert counts == {"positive": 2, "negative": 1, "neutral": 1}

    def test_counts_limit_truncates(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "counts", "limit": 1},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert [v["value"] for v in data["values"]] == ["positive"]
        assert data["total"] == 4
        assert data["distinct"] == 3
        assert data["truncated"] is True

    def test_trend_buckets_and_top_values(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "trend", "bucket": "1m", "limit": 8},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["mode"] == "trend"
        assert data["top_values"][0] == "positive"
        assert set(data["top_values"]) == {"positive", "negative", "neutral"}

        totals: dict[str, int] = {}
        buckets = {row["ts"] for row in data["series"]}
        assert len(buckets) == 2
        for row in data["series"]:
            assert row["value"] != "other"  # 3 values <= limit 8
            totals[row["value"]] = totals.get(row["value"], 0) + row["count"]
        assert totals == {"positive": 2, "negative": 1, "neutral": 1}

    def test_trend_folds_excess_values_into_other(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "trend", "bucket": "1m", "limit": 2},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["top_values"] == ["positive", "negative"]
        cats = {row["value"] for row in data["series"]}
        assert "other" in cats
        other_total = sum(
            row["count"] for row in data["series"] if row["value"] == "other"
        )
        assert other_total == 1  # neutral folded into other

    def test_json_quoted_values_merge_with_parquet_raw(self, client, charts_datasets):
        """The double-encoded live chunk 'positive' merges with the parquet one."""
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "counts", "limit": 20},
        )
        data = resp.json()
        counts = {v["value"]: v["count"] for v in data["values"]}
        assert counts["positive"] == 2
        assert "\"positive\"" not in counts

    def test_rows_without_created_at_ignored(self, client, charts_datasets):
        """row3 (no _created_at) must not appear in any chart."""
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "counts", "limit": 20},
        )
        data = resp.json()
        counts = {v["value"] for v in data["values"]}
        assert "ancient" not in counts
        assert data["total"] == 4

    def test_invalid_column_400(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "bad;column", "mode": "counts"},
        )
        assert resp.status_code == 400

    def test_invalid_mode_422(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "histogram"},
        )
        assert resp.status_code == 422

    def test_invalid_trend_bucket_422(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "trend", "bucket": "2m"},
        )
        assert resp.status_code == 422

    def test_limit_bounds_422(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "counts", "limit": 500},
        )
        assert resp.status_code == 422

    def test_unknown_column_400(self, client, charts_datasets):
        d = charts_datasets["ds_a"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "no_such_column", "mode": "counts"},
        )
        assert resp.status_code == 400

    def test_dataset_without_created_at_returns_empty(self, client, charts_datasets):
        """Pre-_created_at datasets return empty results, not 500."""
        d = charts_datasets["ds_c"]
        resp = client.get(
            f"/datasets/{d}/categorical",
            params={"column": "label", "mode": "counts"},
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["values"] == []
        assert data["total"] == 0
