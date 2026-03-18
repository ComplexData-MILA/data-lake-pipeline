from __future__ import annotations

import os

from fastapi import Depends

from data_lake_pipeline.storage import S3Storage, StorageBackend
from data_lake_pipeline.state import BatchState


def get_storage() -> StorageBackend:
    s3_url = os.environ.get("PIPELINE_S3_URL", "")
    if not s3_url:
        raise ValueError("PIPELINE_S3_URL environment variable is required")

    if s3_url.startswith("s3://"):
        s3_url = s3_url[5:]

    parts = s3_url.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    endpoint_url = os.environ.get("PIPELINE_S3_ENDPOINT_URL")
    access_key = os.environ.get("PIPELINE_S3_ACCESS_KEY")
    secret_key = os.environ.get("PIPELINE_S3_SECRET_KEY")

    return S3Storage(
        bucket=bucket,
        prefix=prefix,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )


def get_batch_state(storage: StorageBackend = Depends(get_storage)) -> BatchState:
    return BatchState(storage)
