"""Merge JSONL chunks into blocked, id-sorted, gzipped NDJSON (the viewer's
merged format).

Replaces the parquet merge for datasets under the size threshold: instead of
``merged.parquet`` the batch gets one or more ``merged_{i:05d}.jsonl.gz``
blocks, each holding ``MERGE_BLOCK_SIZE`` rows, globally ordered by
``(id, _batch)`` within the batch. Blocks are the JSONL equivalent of parquet
row groups: the viewer can fetch the rows for a keyset page by reading only
the blocks whose id-range intersects the page (see ``_index/{batch}.meta.json``
``blocks`` entries), and stream filtered scans block by block.

Dedup runs in SQL (``QUALIFY row_number() …``) with the same semantics as
:func:`clean_up.compute_sha_key`: first occurrence wins, live chunks take
priority over existing merged data, and missing values group like NULLs.
Values keep the pipeline's JSON-stringification convention on disk.

Every block is first written under a ``.temp`` key; the caller publishes them
with server-side ``copy_object`` and deletes the temps (same atomicity pattern
as the index writer).
"""

import json
import logging
import os
import re
import tempfile
from typing import TYPE_CHECKING, Any

import duckdb

from .index import configure_duckdb

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

logger = logging.getLogger(__name__)

MERGE_BLOCK_SIZE = int(os.environ.get("MERGE_BLOCK_SIZE", "50000"))
MERGE_DUCKDB_MEMORY_LIMIT = os.environ.get("MERGE_DUCKDB_MEMORY_LIMIT", "2GB")
JSONL_MERGE_ENABLED = os.environ.get("JSONL_MERGE_ENABLED", "1") != "0"
JSONL_MERGE_MAX_DATASET_BYTES = int(
    os.environ.get("JSONL_MERGE_MAX_DATASET_BYTES", str(10_000_000_000))
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def block_file_name(index: int) -> str:
    return f"merged_{index:05d}.jsonl.gz"


def _format_uris(keys: list[str]) -> str:
    assert keys, "keys must not be empty"
    return "[" + ", ".join(f"'{k}'" for k in keys) + "]"


def _quote_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{name}"'


def merged_prefix(bucket: str, prefix: str, dataset: str, batch: str, annotator: str | None = None) -> str:
    """S3 key prefix for a batch's merged files (blocks live directly under it)."""
    base = f"{prefix.rstrip('/')}/{dataset}"
    if annotator:
        base += f"/annotations/{annotator}"
    return f"{base}/{batch}"


async def dataset_merged_size(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset: str,
    annotator: str | None = None,
) -> int:
    """Total bytes of merged files (parquet + blocks) for one dataset."""
    total = 0
    search_prefix = f"{prefix.rstrip('/')}/{dataset}/"
    if annotator:
        search_prefix += f"annotations/{annotator}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].rsplit("/", 1)[-1]
            if filename == "merged.parquet" or (
                filename.startswith("merged_") and filename.endswith(".jsonl.gz")
            ):
                total += obj["Size"]
    return total


def parquet_to_jsonl_blocks(
    bucket: str,
    prefix: str,
    dataset: str,
    batch: str,
    parquet_key: str,
    annotator: str | None = None,
) -> dict[str, Any]:
    """Convert an existing merged.parquet into id-sorted JSONL blocks.

    Used by the migration job (:mod:`s3_data_tool.convert`). The parquet is
    already deduplicated by the legacy merge, so no dedup pass is needed.
    Blocking — call via ``asyncio.to_thread``.
    """
    batch_prefix = merged_prefix(bucket, prefix, dataset, batch, annotator)
    source = (
        f"SELECT * FROM read_parquet("
        f"{_format_uris([f's3://{bucket}/{parquet_key}'])}, union_by_name=true)"
    )

    with tempfile.TemporaryDirectory() as tmp:
        conn = duckdb.connect(os.path.join(tmp, "merge.duckdb"))
        try:
            configure_duckdb(conn)
            conn.execute(f"SET memory_limit='{MERGE_DUCKDB_MEMORY_LIMIT}';")
            conn.execute(f"SET temp_directory='{tmp}';")
            conn.execute(
                f"""
                CREATE TABLE t AS
                SELECT *
                FROM (
                  SELECT *, ((row_number() OVER (ORDER BY "id", "_batch")) - 1)
                             // {MERGE_BLOCK_SIZE} AS __block
                  FROM ({source})
                  WHERE "id" IS NOT NULL AND "_batch" IS NOT NULL
                ) ORDER BY "id", "_batch";
                """
            )
            result = _write_blocks_from_table(conn, bucket, batch_prefix)
        finally:
            conn.close()
    return result


def merge_to_jsonl_blocks(
    bucket: str,
    prefix: str,
    dataset: str,
    batch: str,
    chunk_keys: list[str],
    existing_parquet_key: str | None,
    existing_block_keys: list[str],
    deduplicate_on: list[str],
    annotator: str | None = None,
) -> dict[str, Any]:
    """Merge a batch's JSONL chunks (+ existing merged data) into sorted blocks.

    Blocking — call via ``asyncio.to_thread``. Writes each block under a
    ``.temp`` key and returns the stats (including block list with temp keys)
    for the caller to publish atomically. Reading existing blocks passes
    ``maximum_sample_files=-1`` so columns that only appear in later blocks
    are not dropped by schema sampling.
    """
    batch_prefix = merged_prefix(bucket, prefix, dataset, batch, annotator)

    sources: list[str] = []
    if chunk_keys:
        sources.append(
            "SELECT *, 0 AS __prio FROM read_json_auto("
            f"{_format_uris([f's3://{bucket}/{k}' for k in chunk_keys])}, "
            "union_by_name=true, format='newline_delimited', ignore_errors=true, "
            "maximum_sample_files=-1)"
        )
    if existing_block_keys:
        sources.append(
            "SELECT *, 1 AS __prio FROM read_json_auto("
            f"{_format_uris([f's3://{bucket}/{k}' for k in existing_block_keys])}, "
            "union_by_name=true, format='newline_delimited', ignore_errors=true, "
            "maximum_sample_files=-1)"
        )
    if existing_parquet_key:
        sources.append(
            "SELECT *, 1 AS __prio FROM read_parquet("
            f"{_format_uris([f's3://{bucket}/{existing_parquet_key}'])}, union_by_name=true)"
        )
    if not sources:
        raise ValueError(f"No source files to merge for {batch_prefix}")
    # Guarantee "id"/"_batch" exist even when every chunk is corrupted
    # (read_json_auto then infers only a "json" column) — contributes 0 rows.
    sources.append('SELECT NULL AS "id", NULL AS "_batch" WHERE FALSE')

    union = " UNION ALL BY NAME ".join(f"({s})" for s in sources)
    # Each window function lives in its own CTE level — DuckDB does not
    # allow nested window functions within one SELECT.
    numbered = (
        f"numbered AS (SELECT *, row_number() OVER () AS __rn FROM ({union}) "
        'WHERE "id" IS NOT NULL AND "_batch" IS NOT NULL)'
    )
    deduped = "deduped AS (SELECT * EXCLUDE (__prio, __rn) FROM numbered"
    if deduplicate_on:
        for col in deduplicate_on:
            _quote_identifier(col)
        partition = ", ".join(_quote_identifier(c) for c in deduplicate_on)
        # First occurrence wins; __prio=0 (live chunks) beats __prio=1
        # (existing merged data). Equivalent to compute_sha_key's seen-set
        # dedup on the stored (JSON-stringified) values.
        deduped += (
            f" QUALIFY row_number() OVER (PARTITION BY {partition} "
            "ORDER BY __prio, __rn) = 1"
        )
    deduped += ")"

    with tempfile.TemporaryDirectory() as tmp:
        conn = duckdb.connect(os.path.join(tmp, "merge.duckdb"))
        try:
            configure_duckdb(conn)
            conn.execute(f"SET memory_limit='{MERGE_DUCKDB_MEMORY_LIMIT}';")
            conn.execute(f"SET temp_directory='{tmp}';")
            conn.execute(
                f"""
                CREATE TABLE t AS
                WITH {numbered}, {deduped},
                blocked AS (
                  SELECT *, ((row_number() OVER (ORDER BY "id", "_batch")) - 1)
                             // {MERGE_BLOCK_SIZE} AS __block
                  FROM deduped
                )
                SELECT * FROM blocked ORDER BY "id", "_batch";
                """
            )
            result = _write_blocks_from_table(conn, bucket, batch_prefix)
        finally:
            conn.close()

    logger.info(
        f"Merged {batch_prefix} into {len(result['blocks'])} JSONL blocks "
        f"({result['row_count']} rows)"
    )
    return result


def _write_blocks_from_table(
    conn: duckdb.DuckDBPyConnection, bucket: str, batch_prefix: str
) -> dict[str, Any]:
    """Write table ``t`` (already id-sorted, with ``__block``) as gzipped
    NDJSON blocks under ``.temp`` keys; return merge stats + block list."""
    conn.execute("CREATE INDEX t_block_idx ON t(__block);")

    overall = conn.execute(
        'SELECT COUNT(*) AS row_count, COUNT(DISTINCT "id") AS distinct_id_count, '
        'MIN("id") AS min_id, MAX("id") AS max_id FROM t'
    ).fetchone()
    block_stats = conn.execute(
        'SELECT __block, COUNT(*) AS row_count, MIN("id") AS min_id, '
        'MAX("id") AS max_id FROM t GROUP BY __block ORDER BY __block'
    ).fetchall()

    blocks: list[dict[str, Any]] = []
    for blk, row_count, min_id, max_id in block_stats:
        file_name = block_file_name(blk)
        temp_key = f"{batch_prefix}/{file_name}.temp"
        conn.execute(
            f"COPY (SELECT * EXCLUDE (__block) FROM t WHERE __block = {blk} "
            f'ORDER BY "id", "_batch") '
            f"TO 's3://{bucket}/{temp_key}' (FORMAT JSON, COMPRESSION GZIP)"
        )
        blocks.append(
            {
                "file": file_name,
                "temp_key": temp_key,
                "key": f"{batch_prefix}/{file_name}",
                "row_count": row_count,
                "min_id": min_id,
                "max_id": max_id,
            }
        )

    return {
        "row_count": overall[0],
        "distinct_id_count": overall[1],
        "min_id": overall[2],
        "max_id": overall[3],
        "blocks": blocks,
    }


async def publish_blocks(
    s3_client: "S3Client", bucket: str, blocks: list[dict[str, Any]]
) -> list[str]:
    """Promote ``.temp`` blocks to their final keys (server-side copy).

    Returns the temp keys (callers delete them after a successful publish).
    """
    temp_keys = []
    for block in blocks:
        await s3_client.copy_object(
            Bucket=bucket,
            Key=block["key"],
            CopySource={"Bucket": bucket, "Key": block["temp_key"]},
        )
        temp_keys.append(block["temp_key"])
    return temp_keys
