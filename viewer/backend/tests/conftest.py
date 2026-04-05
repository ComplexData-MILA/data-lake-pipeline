"""Pytest configuration and fixtures for viewer backend tests."""
import io
import os
import uuid

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def s3_config():
    """Get S3 configuration from environment."""
    return {
        "endpoint_url": os.environ.get("S3_ENDPOINT_URL"),
        "access_key": os.environ.get("S3_ACCESS_KEY"),
        "secret_key": os.environ.get("S3_SECRET_KEY"),
        "bucket": os.environ.get("S3_BUCKET", "test-bucket"),
        "prefix": os.environ.get("S3_PREFIX", "datasets"),
    }


@pytest.fixture
async def s3_client(s3_config):
    """Create async S3 client."""
    session = aioboto3.Session(
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
    )
    kwargs = {}
    if s3_config["endpoint_url"]:
        kwargs["endpoint_url"] = s3_config["endpoint_url"]

    async with session.client("s3", **kwargs) as client:
        yield client, session, kwargs


@pytest.fixture
async def clean_bucket(s3_config):
    """Clean bucket before and after tests."""
    session = aioboto3.Session(
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
    )
    kwargs = {}
    if s3_config["endpoint_url"]:
        kwargs["endpoint_url"] = s3_config["endpoint_url"]

    bucket = s3_config["bucket"]
    prefix = s3_config["prefix"]

    async with session.client("s3", **kwargs) as client:
        paginator = client.get_paginator("list_objects_v2")
        keys_to_delete = []
        async for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                keys_to_delete.append({"Key": obj["Key"]})
        if keys_to_delete:
            await client.delete_objects(
                Bucket=bucket, Delete={"Objects": keys_to_delete}
            )

    yield

    async with session.client("s3", **kwargs) as client:
        paginator = client.get_paginator("list_objects_v2")
        keys_to_delete = []
        async for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                keys_to_delete.append({"Key": obj["Key"]})
        if keys_to_delete:
            await client.delete_objects(
                Bucket=bucket, Delete={"Objects": keys_to_delete}
            )


def create_parquet_buffer(rows: list[dict]) -> io.BytesIO:
    """Create parquet file in memory from rows."""
    if not rows:
        table = pa.table({})
    else:
        table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf


@pytest.fixture
async def test_dataset(s3_client, s3_config, clean_bucket):
    """Create a test dataset with parquet data and annotations."""
    client, session, kwargs = s3_client
    bucket = s3_config["bucket"]
    prefix = s3_config["prefix"]

    test_id = str(uuid.uuid4())[:8]
    dataset_name = f"test_dataset_{test_id}"

    dataset_rows = [
        {"id": "row1", "text": "Hello world", "value": 10, "_batch": "batch1"},
        {"id": "row2", "text": "我的拼好饭失踪了", "value": 20, "_batch": "batch1"},
        {"id": "row3", "text": "Test text", "value": 30, "_batch": "batch1"},
    ]

    annotation_rows = [
        {"id": "row1", "is_valid": True, "label": "positive"},
        {"id": "row2", "is_valid": False, "label": "negative"},
    ]

    ds_key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
    buf = create_parquet_buffer(dataset_rows)
    await client.put_object(Bucket=bucket, Key=ds_key, Body=buf.read())

    ann_key = f"{prefix}/{dataset_name}/annotations/annotator1/batch1/merged.parquet"
    buf = create_parquet_buffer(annotation_rows)
    await client.put_object(Bucket=bucket, Key=ann_key, Body=buf.read())

    yield {
        "dataset_name": dataset_name,
        "rows": dataset_rows,
        "annotations": annotation_rows,
        "client": client,
        "session": session,
        "kwargs": kwargs,
        "bucket": bucket,
        "prefix": prefix,
    }

    paginator = client.get_paginator("list_objects_v2")
    keys_to_delete = []
    async for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"):
        for obj in page.get("Contents", []):
            keys_to_delete.append(obj["Key"])
    for key in keys_to_delete:
        await client.delete_object(Bucket=bucket, Key=key)