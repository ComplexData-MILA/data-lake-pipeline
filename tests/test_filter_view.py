"""Integration tests for filter_view module."""

import io
import os
import uuid
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from s3_data_tool.filter import AllFilter, BooleanFilter
from s3_data_tool.data_filtering import AnnotationLockError, DatasetNotMergedError
from s3_data_tool.models import Annotation, DataItem


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
def s3_setup():
    """Get S3 configuration from environment."""
    import aioboto3

    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    bucket = os.environ.get("S3_BUCKET", "test-bucket")
    prefix = os.environ.get("S3_PREFIX", "datasets")

    session = aioboto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    return session, kwargs, bucket, prefix


@pytest.mark.asyncio
class TestExportView:
    """Tests for ExportView."""

    async def test_iterate_empty_dataset(self, s3_setup):
        """Test iterating over dataset with no batches."""
        import asyncio

        from s3_data_tool.data_filtering import FilterForExport

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"export_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            view = FilterForExport(client, bucket, prefix, dataset_name, None)
            rows = []
            async with view:
                async for row in view:
                    rows.append(row)
            assert rows == []

    async def test_iterate_with_data(self, s3_setup):
        """Test iterating over dataset with parquet data."""
        from s3_data_tool.data_filtering import FilterForExport

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"export_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello", "is_valid": True},
            {"id": "2", "text": "World", "is_valid": False},
            {"id": "3", "text": "Test", "is_valid": True},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                view = FilterForExport(client, bucket, prefix, dataset_name, None)
                rows = []
                async with view:
                    async for row in view:
                        rows.append(row)

                assert len(rows) == 3
                ids = {r["id"] for r in rows}
                assert ids == {"1", "2", "3"}

            finally:
                await client.delete_object(Bucket=bucket, Key=key)

    async def test_iterate_with_filter(self, s3_setup):
        """Test iterating with a filter."""
        from s3_data_tool.data_filtering import FilterForExport

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"export_filter_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello", "is_valid": True},
            {"id": "2", "text": "World", "is_valid": False},
            {"id": "3", "text": "Test", "is_valid": True},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                filter_ = BooleanFilter(field="is_valid", value=True)
                view = FilterForExport(client, bucket, prefix, dataset_name, filter_)
                rows = []
                async with view:
                    async for row in view:
                        rows.append(row)

                assert len(rows) == 2
                for row in rows:
                    assert row["is_valid"] is True

            finally:
                await client.delete_object(Bucket=bucket, Key=key)

    async def test_iterate_with_annotations(self, s3_setup):
        """Test iterating with annotation data joined."""
        from s3_data_tool.data_filtering import FilterForExport

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"export_ann_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello"},
            {"id": "2", "text": "World"},
        ]

        annotation_rows = [
            {"id": "1", "data": {"sentiment": "positive"}},
            {"id": "2", "data": {"sentiment": "neutral"}},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                ds_key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=ds_key, Body=buf.read())

                ann_key = f"{prefix}/{dataset_name}/annotations/sentiment/batch1/merged.parquet"
                buf = create_parquet_buffer(annotation_rows)
                await client.put_object(Bucket=bucket, Key=ann_key, Body=buf.read())

                view = FilterForExport(client, bucket, prefix, dataset_name, None)
                rows = []
                async with view:
                    async for row in view:
                        rows.append(row)

                assert len(rows) == 2
                for row in rows:
                    if row["id"] == "1":
                        assert row.get("sentiment.sentiment") == "positive"
                    elif row["id"] == "2":
                        assert row.get("sentiment.sentiment") == "neutral"

            finally:
                await client.delete_object(Bucket=bucket, Key=ds_key)
                try:
                    await client.delete_object(Bucket=bucket, Key=ann_key)
                except Exception:
                    pass


@pytest.mark.asyncio
class TestAnnotationView:
    """Tests for AnnotationView."""

    async def test_lock_acquisition(self, s3_setup):
        """Test lock acquisition succeeds."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_lock_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            view = FilterForAnnotation(
                client, bucket, prefix, dataset_name, "test_annotator", None
            )
            async with view:
                assert view._lock is not None
                assert view._lock._acquired is True

    async def test_lock_conflict(self, s3_setup):
        """Test lock conflict raises error."""
        import asyncio

        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_conflict_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            view1 = FilterForAnnotation(
                client, bucket, prefix, dataset_name, "test_annotator", None
            )
            view2 = FilterForAnnotation(
                client, bucket, prefix, dataset_name, "test_annotator", None
            )

            async with view1:
                with pytest.raises(AnnotationLockError):
                    async with view2:
                        pass

    async def test_annotate_empty_dataset(self, s3_setup):
        """Test annotate on empty dataset."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_empty_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            view = FilterForAnnotation(
                client, bucket, prefix, dataset_name, "test_annotator", None
            )

            async def annotator(item: DataItem) -> Annotation:
                return Annotation(data={"label": "test"})

            async with view:
                await view.annotate(annotator, batch="test_batch")

    async def test_annotate_with_data(self, s3_setup):
        """Test annotate with actual data."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_data_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello"},
            {"id": "2", "text": "World"},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                view = FilterForAnnotation(
                    client, bucket, prefix, dataset_name, "test_annotator", None
                )

                call_count = 0

                async def annotator(item: DataItem) -> Annotation:
                    nonlocal call_count
                    call_count += 1
                    return Annotation(data={"label": f"label_{item.id}"})

                async with view:
                    await view.annotate(annotator, batch="test_batch")

                assert call_count == 2

                ann_key = (
                    f"{prefix}/{dataset_name}/annotations/test_annotator/test_batch"
                )
                paginator = client.get_paginator("list_objects_v2")
                jsonl_files = []
                async for page in paginator.paginate(Bucket=bucket, Prefix=ann_key):
                    for obj in page.get("Contents", []):
                        if obj["Key"].endswith(".jsonl"):
                            jsonl_files.append(obj["Key"])

                assert len(jsonl_files) >= 1

            finally:
                paginator = client.get_paginator("list_objects_v2")
                keys_to_delete = []
                async for page in paginator.paginate(
                    Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"
                ):
                    for obj in page.get("Contents", []):
                        keys_to_delete.append(obj["Key"])
                for key in keys_to_delete:
                    await client.delete_object(Bucket=bucket, Key=key)

    async def test_annotate_skips_already_annotated(self, s3_setup):
        """Test that already annotated rows are skipped."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_skip_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello"},
            {"id": "2", "text": "World"},
            {"id": "3", "text": "Test"},
        ]

        existing_annotations = [
            {"id": "1", "data": {"label": "existing"}},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                ds_key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=ds_key, Body=buf.read())

                ann_key = f"{prefix}/{dataset_name}/annotations/test_annotator/batch1/merged.parquet"
                buf = create_parquet_buffer(existing_annotations)
                await client.put_object(Bucket=bucket, Key=ann_key, Body=buf.read())

                view = FilterForAnnotation(
                    client, bucket, prefix, dataset_name, "test_annotator", None
                )

                annotated_ids = []

                async def annotator(item: DataItem) -> Annotation:
                    annotated_ids.append(item.id)
                    return Annotation(data={"label": "new"})

                async with view:
                    await view.annotate(annotator, batch="batch1")

                assert "1" not in annotated_ids
                assert "2" in annotated_ids
                assert "3" in annotated_ids

            finally:
                paginator = client.get_paginator("list_objects_v2")
                keys_to_delete = []
                async for page in paginator.paginate(
                    Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"
                ):
                    for obj in page.get("Contents", []):
                        keys_to_delete.append(obj["Key"])
                for key in keys_to_delete:
                    await client.delete_object(Bucket=bucket, Key=key)

    async def test_annotate_with_filter(self, s3_setup):
        """Test annotate with filter applied."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_filter_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello", "priority": True},
            {"id": "2", "text": "World", "priority": False},
            {"id": "3", "text": "Test", "priority": True},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                filter_ = BooleanFilter(field="priority", value=True)
                view = FilterForAnnotation(
                    client, bucket, prefix, dataset_name, "test_annotator", filter_
                )

                annotated_ids = []

                async def annotator(item: DataItem) -> Annotation:
                    annotated_ids.append(item.id)
                    return Annotation(data={"label": "new"})

                async with view:
                    await view.annotate(annotator, batch="test_batch")

                assert set(annotated_ids) == {"1", "3"}

            finally:
                paginator = client.get_paginator("list_objects_v2")
                keys_to_delete = []
                async for page in paginator.paginate(
                    Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"
                ):
                    for obj in page.get("Contents", []):
                        keys_to_delete.append(obj["Key"])
                for key in keys_to_delete:
                    await client.delete_object(Bucket=bucket, Key=key)

    async def test_dataset_not_merged_error(self, s3_setup):
        """Test error when dataset parquet doesn't exist."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_not_merged_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            marker_key = f"{prefix}/{dataset_name}/batch1/.marker"
            await client.put_object(Bucket=bucket, Key=marker_key, Body=b"")

            view = FilterForAnnotation(
                client, bucket, prefix, dataset_name, "test_annotator", None
            )

            async def annotator(item: DataItem) -> Annotation:
                return Annotation(data={"label": "test"})

            async with view:
                with pytest.raises(DatasetNotMergedError):
                    await view.annotate(annotator, batch="batch1")

            await client.delete_object(Bucket=bucket, Key=marker_key)
