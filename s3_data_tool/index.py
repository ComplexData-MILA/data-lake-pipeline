"""Dataset-level index maintenance for the viewer (Phase 5).

Writes one sorted partition per batch under ``{prefix}/{dataset}/_index/``:

- ``{batch}.parquet`` — columns ``id, _batch`` sorted by ``(id, _batch)``
- ``{batch}.meta.json`` — ``{row_count, distinct_id_count, min_id, max_id,
  updated_at}`` plus ``format``/``blocks`` when the merged source is JSONL
  blocks (see :mod:`s3_data_tool.jsonl_merge`).

The viewer uses these partitions for keyset pagination, index-backed counts,
and per-batch file pruning on large datasets. Updated inside the per-batch
S3Lock in clean_up, right after the merged files are published. Atomic:
write ``.temp``, server-side ``copy_object``, overwrite the meta.

Any failure here is non-fatal — the viewer falls back to scan-based queries
when index partitions are missing or stale.
"""

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


def index_parquet_key(prefix: str, dataset: str, batch: str) -> str:
    return f"{prefix.rstrip('/')}/{dataset}/_index/{batch}.parquet"


def index_meta_key(prefix: str, dataset: str, batch: str) -> str:
    return f"{prefix.rstrip('/')}/{dataset}/_index/{batch}.meta.json"


def configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    endpoint = os.environ.get("S3_ENDPOINT_URL", "")
    endpoint_host = endpoint.removeprefix("https://").removeprefix("http://").rstrip("/")
    use_ssl = endpoint.startswith("https://")
    conn.execute(
        f"""
        SET s3_access_key_id='{os.environ.get("S3_ACCESS_KEY", "")}';
        SET s3_secret_access_key='{os.environ.get("S3_SECRET_KEY", "")}';
        SET s3_endpoint='{endpoint_host}';
        SET s3_use_ssl={str(use_ssl).lower()};
        SET s3_url_style='path';
    """
    )


def _merged_source(merged_uri: str, source: str) -> str:
    """SQL FROM expression for the merged source (parquet or JSONL blocks)."""
    if source == "parquet":
        return f"read_parquet('{merged_uri}')"
    return (
        f"read_json_auto('{merged_uri}', union_by_name=true, "
        "format='newline_delimited', ignore_errors=true, maximum_sample_files=-1)"
    )


def _write_index_sync(
    bucket: str,
    merged_key: str,
    index_key: str,
    source: str = "parquet",
) -> dict:
    """Rewrite the index partition for one batch from its merged files (blocking)."""
    merged_uri = f"s3://{bucket}/{merged_key}"
    temp_key = index_key + ".temp"
    temp_uri = f"s3://{bucket}/{temp_key}"

    with tempfile.TemporaryDirectory() as tmp:
        conn = duckdb.connect(os.path.join(tmp, "index.duckdb"))
        try:
            configure_duckdb(conn)
            src = _merged_source(merged_uri, source)
            meta = conn.execute(
                f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT id) AS distinct_id_count, "
                f"MIN(id) AS min_id, MAX(id) AS max_id "
                f"FROM {src}"
            ).fetchone()
            conn.execute(
                f"COPY (SELECT id, _batch FROM {src} "
                f"ORDER BY id, _batch) TO '{temp_uri}' (FORMAT PARQUET)"
            )
        finally:
            conn.close()
    return {
        "row_count": meta[0],
        "distinct_id_count": meta[1],
        "min_id": meta[2],
        "max_id": meta[3],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


async def update_batch_index(
    s3_client,
    bucket: str,
    prefix: str,
    dataset: str,
    batch: str,
    merged_jsonl_glob: str | None = None,
    blocks: list[dict[str, Any]] | None = None,
) -> dict | None:
    """Update the index partition for one batch (called inside the S3Lock).

    By default reads ``merged.parquet``. When *merged_jsonl_glob* is given,
    builds from the batch's ``merged_*.jsonl.gz`` blocks instead and records
    them in the meta (``format: "jsonl"`` + per-block min/max id ranges).

    Returns the meta dict, or None when the batch has no merged.parquet.
    """
    if merged_jsonl_glob is not None:
        merged_key = merged_jsonl_glob
        source = "jsonl"
    else:
        merged_key = f"{prefix.rstrip('/')}/{dataset}/{batch}/merged.parquet"
        source = "parquet"
        try:
            await s3_client.head_object(Bucket=bucket, Key=merged_key)
        except Exception:
            return None

    index_key = index_parquet_key(prefix, dataset, batch)
    meta_key = index_meta_key(prefix, dataset, batch)

    meta = await asyncio.to_thread(
        _write_index_sync, bucket, merged_key, index_key, source
    )
    if blocks is not None:
        meta["format"] = "jsonl"
        meta["blocks"] = [
            {k: b[k] for k in ("file", "row_count", "min_id", "max_id")}
            for b in blocks
        ]

    await s3_client.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(meta).encode("utf-8"),
    )
    await s3_client.copy_object(
        Bucket=bucket,
        Key=index_key,
        CopySource={"Bucket": bucket, "Key": index_key + ".temp"},
    )
    await s3_client.delete_object(Bucket=bucket, Key=index_key + ".temp")
    logger.info(
        f"Updated index {index_key}: {meta['row_count']} rows "
        f"({meta['distinct_id_count']} distinct ids)"
    )
    return meta
