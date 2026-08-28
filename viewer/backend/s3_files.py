"""S3 file manifest for the viewer: classifies every object under a dataset prefix.

One paginated ``list_objects_v2`` scan per dataset yields:

- ``{dataset}/{batch}/merged.parquet`` -> base merged parquet (legacy batches)
- ``{dataset}/{batch}/merged_*.jsonl.gz`` -> base merged JSONL blocks
- ``{dataset}/{batch}/{run_id}_chunk_*.jsonl`` -> base live JSONL (any run state,
  in-progress runs included — that is the point of the live read)
- ``{dataset}/annotations/{annotator}/.../merged.parquet`` -> annotator merged parquet
- ``{dataset}/annotations/{annotator}/.../merged_*.jsonl.gz`` -> annotator merged blocks
- ``{dataset}/annotations/{annotator}/{batch}/.temp/chunk_*.jsonl`` -> annotator live JSONL
- ``{dataset}/_index/*.parquet`` -> dataset index partitions (Phase 5)
- ``{dataset}/_migration/*`` -> conversion status (ignored)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AnnotatorFiles(BaseModel):
    merged_parquet: list[str] = Field(default_factory=list)
    merged_jsonl: list[str] = Field(default_factory=list)
    live_jsonl: list[str] = Field(default_factory=list)


class FileManifest(BaseModel):
    dataset: str
    merged_parquet: list[str] = Field(default_factory=list)
    merged_jsonl: list[str] = Field(default_factory=list)
    live_jsonl: list[str] = Field(default_factory=list)
    annotators: dict[str, AnnotatorFiles] = Field(default_factory=dict)
    index_files: list[str] = Field(default_factory=list)
    batch_meta: dict[str, dict[str, Any]] = Field(default_factory=dict)
    # s3:// URI -> ISO 8601 LastModified. Files are immutable once written
    # (chunks are uploaded whole; merged blocks are rewritten only by
    # merges/conversion, which only get *newer*), so every row's _created_at
    # is <= its file's mtime. Chart window pruning relies on that invariant.
    file_mtimes: dict[str, str] = Field(default_factory=dict)
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def _is_merged_block(filename: str) -> bool:
    return filename.startswith("merged_") and filename.endswith(".jsonl.gz")


def build_file_manifest(
    client, bucket: str, prefix: str, dataset: str
) -> FileManifest:
    """List the dataset prefix once and classify every object."""
    manifest = FileManifest(dataset=dataset)
    search_prefix = f"{prefix.rstrip('/')}/{dataset}/"

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            manifest.file_mtimes[f"s3://{bucket}/{key}"] = str(
                obj.get("LastModified", "")
            )
            rel = key[len(search_prefix):]
            parts = rel.split("/")
            filename = rel.rsplit("/", 1)[-1]

            if rel.startswith("annotations/"):
                if len(parts) >= 2:
                    annotator = parts[1]
                    files = manifest.annotators.setdefault(
                        annotator, AnnotatorFiles()
                    )
                    if rel.endswith("/merged.parquet"):
                        files.merged_parquet.append(f"s3://{bucket}/{key}")
                    elif _is_merged_block(filename):
                        files.merged_jsonl.append(f"s3://{bucket}/{key}")
                    elif "/.temp/" in rel and rel.endswith(".jsonl"):
                        files.live_jsonl.append(f"s3://{bucket}/{key}")
            elif rel.startswith("_index/"):
                if rel.endswith(".parquet"):
                    manifest.index_files.append(f"s3://{bucket}/{key}")
                elif rel.endswith(".meta.json"):
                    batch_name = rel.split("/")[1][: -len(".meta.json")]
                    try:
                        response = client.get_object(Bucket=bucket, Key=key)
                        meta = json.loads(response["Body"].read())
                        manifest.batch_meta[batch_name] = meta
                    except Exception as e:  # noqa: BLE001
                        logger.debug(f"Failed to read index meta {key}: {e}")
            elif rel.startswith("_migration/"):
                continue  # conversion status objects are not data files
            elif rel.endswith("/merged.parquet"):
                manifest.merged_parquet.append(f"s3://{bucket}/{key}")
            elif _is_merged_block(filename):
                manifest.merged_jsonl.append(f"s3://{bucket}/{key}")
            elif rel.endswith(".jsonl"):
                # {batch}/{run_id}_chunk_NNNNN.jsonl — unmerged base rows
                manifest.live_jsonl.append(f"s3://{bucket}/{key}")

    manifest.merged_parquet.sort()
    manifest.merged_jsonl.sort()
    manifest.live_jsonl.sort()
    manifest.index_files.sort()
    for files in manifest.annotators.values():
        files.merged_parquet.sort()
        files.merged_jsonl.sort()
        files.live_jsonl.sort()
    return manifest
