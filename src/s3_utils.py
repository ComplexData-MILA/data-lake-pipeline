import io
import json
import secrets
from typing import Any

import duckdb
import pyarrow.parquet as pq

from .models import RunManifest


def generate_hex_id() -> str:
    return secrets.token_hex(3)


async def upload_run_manifest(
    s3_client: Any,
    bucket: str,
    key: str,
    manifest: RunManifest,
) -> None:
    body = manifest.model_dump_json()
    await s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
    )


async def upload_jsonl_chunk(
    s3_client: Any,
    bucket: str,
    key: str,
    rows: list[dict],
) -> None:
    body = "\n".join(json.dumps(row) for row in rows)
    await s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
    )


async def list_jsonl_chunks(
    s3_client: Any,
    bucket: str,
    prefix: str,
    hex_id: str,
) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl") and hex_id in key:
                keys.append(key)
    return sorted(keys)


async def merge_jsonl_to_parquet(
    s3_client: Any,
    bucket: str,
    jsonl_keys: list[str],
    output_key: str,
    deduplicate_on: list[str] | None = None,
) -> None:
    conn = duckdb.connect()

    all_rows = []
    for key in jsonl_keys:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        text = body.decode("utf-8")
        for line in text.strip().split("\n"):
            if line.strip():
                try:
                    all_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    if not all_rows:
        empty_table = conn.execute("SELECT 1 LIMIT 0").arrow()
        buf = io.BytesIO()
        pq.write_table(empty_table, buf)
        buf.seek(0)
        await s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=buf.read(),
        )
        return

    conn.execute("CREATE TABLE temp AS SELECT * FROM read_json_auto('data.jsonl')")
    conn.insert("temp", [all_rows])

    if deduplicate_on:
        cols = ", ".join(deduplicate_on)
        conn.execute(
            f"CREATE TABLE deduped AS SELECT * FROM temp WHERE rowid IN (SELECT MAX(rowid) FROM temp GROUP BY {cols})"
        )
        conn.execute("DROP TABLE temp")
        conn.execute("ALTER TABLE deduped RENAME TO temp")

    table = conn.execute("SELECT * FROM temp").arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    await s3_client.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=buf.read(),
    )

    conn.close()


async def delete_objects(
    s3_client: Any,
    bucket: str,
    keys: list[str],
) -> None:
    if not keys:
        return
    for i in range(0, len(keys), 1000):
        chunk = keys[i : i + 1000]
        delete_spec = {"Objects": [{"Key": k} for k in chunk]}
        await s3_client.delete_objects(Bucket=bucket, Delete=delete_spec)
