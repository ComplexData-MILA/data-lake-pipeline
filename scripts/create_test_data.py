import asyncio
import json
import os
import secrets
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from aioboto3 import Session

from s3_data_tool.models import RunManifest

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


def generate_hex_id() -> str:
    return secrets.token_hex(3)


async def upload_jsonl_chunk(
    s3_client: "S3Client", bucket: str, key: str, rows: list[dict]
) -> None:
    body = "\n".join(json.dumps(row) for row in rows)
    await s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


async def upload_manifest(
    s3_client: "S3Client", bucket: str, key: str, manifest: RunManifest
) -> None:
    body = manifest.model_dump_json()
    await s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


async def create_test_dataset(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    dataset_name: str = "test_dataset",
    num_batches: int = 3,
    rows_per_batch: int = 50,
) -> None:
    print(f"Creating test dataset: {dataset_name}")

    base_texts = [
        "The quick brown fox jumps over the lazy dog.",
        "Machine learning models require large amounts of training data.",
        "Natural language processing enables computers to understand human language.",
        "Distributed systems provide scalability and fault tolerance.",
        "Cloud storage solutions offer flexible data management options.",
    ]

    for batch_idx in range(num_batches):
        batch_name = f"20260401-{batch_idx:02d}"
        batch_prefix = f"{prefix}/{dataset_name}/{batch_name}"
        print(f"  Creating batch: {batch_name}")

        num_runs = 2
        rows_per_run = rows_per_batch // num_runs

        for run_idx in range(num_runs):
            run_id = generate_hex_id()
            created_at = datetime.now(timezone.utc)

            deduplicate_on = ["text", "source_id"] if run_idx == 0 else ["text"]

            manifest = RunManifest(
                run_id=run_id,
                deduplicate_on=deduplicate_on,
                streaming_configs={"chunk_size": 20},
                completed=True,
                created_at=created_at,
                completed_at=created_at,
            )
            manifest_key = f"{batch_prefix}/{run_id}.manifest.json"
            await upload_manifest(s3_client, bucket, manifest_key, manifest)

            rows: list[dict] = []
            for i in range(rows_per_run):
                text_idx = (batch_idx * rows_per_run + i) % len(base_texts)
                text = base_texts[text_idx]

                if run_idx == 1 and i < 5:
                    text = base_texts[0]

                row = {
                    "text": text,
                    "source_id": f"source_{batch_idx}_{run_idx}_{i}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "metadata": {
                        "batch_idx": batch_idx,
                        "run_idx": run_idx,
                        "row_idx": i,
                    },
                }

                if run_idx == 1 and i >= rows_per_run - 3:
                    row["extra_column"] = f"extra_value_{i}"

                rows.append(row)

            chunk_idx = 0
            chunk_size = 20
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                chunk_key = f"{batch_prefix}/{run_id}_chunk_{chunk_idx:05d}.jsonl"
                await upload_jsonl_chunk(s3_client, bucket, chunk_key, chunk)
                chunk_idx += 1

            print(f"    Run {run_id}: {len(rows)} rows in {chunk_idx} chunks")

        corrupted_key = f"{batch_prefix}/{generate_hex_id()}_chunk_99999.jsonl"
        corrupted_content = '{"text": "valid row"}\n{invalid json}\n{"text": "another valid row"}'
        await s3_client.put_object(
            Bucket=bucket, Key=corrupted_key, Body=corrupted_content.encode("utf-8")
        )
        print(f"    Added corrupted JSONL file for error handling test")

    print(f"Test dataset creation complete!")


async def main() -> None:
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "datasets")
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")

    session = Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with session.client("s3", **kwargs) as s3_client:
        await create_test_dataset(
            s3_client,
            bucket,
            prefix,
            dataset_name="test_dataset",
            num_batches=3,
            rows_per_batch=50,
        )


if __name__ == "__main__":
    asyncio.run(main())
