"""Integration tests for dataset filter module."""

import asyncio
import io
import logging
import os
import uuid
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pytest_asyncio

from s3_data_tool.filter import AllFilter, BooleanFilter

from s3_data_tool.dataset_generator import _transform_row_for_jsonl
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


@pytest_asyncio.fixture
async def s3_setup():
    """Get S3 configuration from environment and clean bucket."""
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

    async with session.client("s3", **kwargs) as client:  # type: ignore
        paginator = client.get_paginator("list_objects_v2")
        keys_to_delete = []
        async for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                keys_to_delete.append({"Key": obj["Key"]})
        if keys_to_delete:
            await client.delete_objects(
                Bucket=bucket, Delete={"Objects": keys_to_delete}
            )

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
            view = FilterForExport(
                client, bucket, prefix, dataset_name, base_columns=["id", "text"]
            )
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
            {"id": "1", "text": "Hello", "is_valid": True, "_batch": "batch1"},
            {"id": "2", "text": "World", "is_valid": False, "_batch": "batch1"},
            {"id": "3", "text": "Test", "is_valid": True, "_batch": "batch1"},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                view = FilterForExport(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    base_columns=["id", "text", "is_valid"],
                )
                rows = []
                async with view:
                    async for row in view:
                        rows.append(row)

                assert len(rows) == 3
                ids = {r.id for r in rows}
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
            {"id": "1", "text": "Hello", "is_valid": True, "_batch": "batch1"},
            {"id": "2", "text": "World", "is_valid": False, "_batch": "batch1"},
            {"id": "3", "text": "Test", "is_valid": True, "_batch": "batch1"},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                filter_ = BooleanFilter(field="is_valid", value=True)
                view = FilterForExport(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    base_columns=["id", "text", "is_valid"],
                    base_filter=filter_,
                )
                rows = []
                async with view:
                    async for row in view:
                        rows.append(row)

                assert len(rows) == 2
                for row in rows:
                    assert row.data["is_valid"] is True

            finally:
                await client.delete_object(Bucket=bucket, Key=key)

    async def test_iterate_with_annotations(self, s3_setup):
        """Test iterating with annotation data joined."""
        from s3_data_tool.data_filtering import FilterForExport

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"export_ann_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello", "_batch": "batch1"},
            {"id": "2", "text": "World", "_batch": "batch1"},
            {"id": "3", "text": "!", "_batch": "batch1"},
        ]

        annotation_rows = [
            {"id": "1", "is_valid": True, "details": {"label": "positive"}},
            {"id": "2", "is_valid": False, "details": {"label": "neutral"}},
            {"id": "3", "is_valid": True, "details": {"label": "positive"}},
        ]
        async with session.client("s3", **kwargs) as client:
            try:
                ds_key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=ds_key, Body=buf.read())

                ann_key = f"{prefix}/{dataset_name}/annotations/sentiment/batch1/merged.parquet"
                buf = create_parquet_buffer(
                    list(map(_transform_row_for_jsonl, annotation_rows))
                )
                await client.put_object(Bucket=bucket, Key=ann_key, Body=buf.read())

                view = FilterForExport(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    base_columns=["id", "text"],
                    annotator_columns={"sentiment": ["id", "is_valid", "details"]},
                )
                rows = []
                async with view:
                    async for row in view:
                        rows.append(row)

                assert len(rows) == 3

                # Filtered based on annotation
                filtered_view = FilterForExport(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    base_columns=["id", "text"],
                    annotator_columns={"sentiment": ["id", "is_valid", "details"]},
                    annotator_filters={
                        "sentiment": BooleanFilter(field="is_valid", value=True)
                    },
                )
                filtered_rows = []
                async with filtered_view:
                    async for row in filtered_view:
                        filtered_rows.append(row)

                assert len(filtered_rows) == 2

                for row in filtered_rows:
                    assert row.data["is_valid"] == True

                    # Validate de-serialization of JSON-encoded fields
                    assert row.data["details"]["label"] == "positive"

            finally:
                await client.delete_object(Bucket=bucket, Key=ds_key)
                try:
                    await client.delete_object(Bucket=bucket, Key=ann_key)
                except Exception:
                    pass


@pytest.mark.asyncio
class TestAnnotationView:
    """Tests for AnnotationView."""

    async def test_auto_claim_no_batches(self, s3_setup):
        """Test that auto-claiming with no batches returns gracefully."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_lock_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            view = FilterForAnnotation(
                client,
                bucket,
                prefix,
                dataset_name,
                "test_annotator",
                base_columns=["id", "text"],
            )

            call_count = 0

            async def annotator(item: DataItem) -> Annotation:
                nonlocal call_count
                call_count += 1
                return Annotation(data={"label": "test"})

            async with view:
                await view.annotate(annotator)

            # No batches were available, so annotator was never called
            assert call_count == 0

    async def test_auto_claim_exclusion(self, s3_setup):
        """Test that two views auto-claiming partition the work — only one wins."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_conflict_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello", "_batch": "batch1"},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                view1 = FilterForAnnotation(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    "test_annotator",
                    base_columns=["id", "text"],
                )
                view2 = FilterForAnnotation(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    "test_annotator",
                    base_columns=["id", "text"],
                )

                count1 = 0
                count2 = 0

                async def annotator1(item: DataItem) -> Annotation:
                    nonlocal count1
                    count1 += 1
                    return Annotation(data={"label": "test"})

                async def annotator2(item: DataItem) -> Annotation:
                    nonlocal count2
                    count2 += 1
                    return Annotation(data={"label": "test"})

                async with view1:
                    async with view2:
                        await view1.annotate(annotator1)
                        await view2.annotate(annotator2)

                # Only one view claims the single batch — mutual exclusion via lock
                assert (count1 == 1 and count2 == 0) or (
                    count1 == 0 and count2 == 1
                )
            finally:
                paginator = client.get_paginator("list_objects_v2")
                keys_to_delete = []
                async for page in paginator.paginate(
                    Bucket=bucket, Prefix=f"{prefix}/{dataset_name}"
                ):
                    for obj in page.get("Contents", []):
                        keys_to_delete.append(obj["Key"])
                for k in keys_to_delete:
                    await client.delete_object(Bucket=bucket, Key=k)

    async def test_annotate_empty_dataset(self, s3_setup):
        """Test annotate on empty dataset."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_empty_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            view = FilterForAnnotation(
                client,
                bucket,
                prefix,
                dataset_name,
                "test_annotator",
                base_columns=["id", "text"],
            )

            async def annotator(item: DataItem) -> Annotation:
                return Annotation(data={"label": "test"})

            async with view:
                await view.annotate(annotator)

    async def test_annotate_with_data(self, s3_setup):
        """Test annotate with actual data."""
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_data_test_{test_id}"

        dataset_rows = [
            {"id": "1", "text": "Hello", "_batch": "batch1"},
            {"id": "2", "text": "World", "_batch": "batch1"},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                view = FilterForAnnotation(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    "test_annotator",
                    base_columns=["id", "text", "_batch"],
                )

                call_count = 0

                async def annotator(item: DataItem) -> Annotation:
                    nonlocal call_count
                    call_count += 1
                    return Annotation(data={"label": f"label_{item.id}"})

                async with view:
                    await view.annotate(annotator)

                assert call_count == 2

                ann_key = (
                    f"{prefix}/{dataset_name}/annotations/test_annotator/batch1"
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
            {"id": "1", "text": "Hello", "_batch": "batch1"},
            {"id": "2", "text": "World", "_batch": "batch1"},
            {"id": "3", "text": "Test", "_batch": "batch1"},
        ]

        existing_annotations = [
            _transform_row_for_jsonl({"id": "1", "data": {"label": "existing"}}),
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
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    "test_annotator",
                    base_columns=["id", "text"],
                    annotator_columns={"test_annotator": ["id", "data"]},
                    annotator_filters={"test_annotator": AllFilter(filters=[])},
                )

                annotated_ids = []

                async def annotator(item: DataItem) -> Annotation:
                    annotated_ids.append(item.id)
                    return Annotation(data={"label": "new"})

                async with view:
                    await view.annotate(annotator)

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
            {"id": "1", "text": "Hello", "priority": True, "_batch": "batch1"},
            {"id": "2", "text": "World", "priority": False, "_batch": "batch1"},
            {"id": "3", "text": "Test", "priority": True, "_batch": "batch1"},
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                key = f"{prefix}/{dataset_name}/batch1/merged.parquet"
                buf = create_parquet_buffer(dataset_rows)
                await client.put_object(Bucket=bucket, Key=key, Body=buf.read())

                filter_ = BooleanFilter(field="priority", value=True)
                view = FilterForAnnotation(
                    client,
                    bucket,
                    prefix,
                    dataset_name,
                    "test_annotator",
                    base_columns=["id", "text", "priority"],
                    base_filter=filter_,
                )

                annotated_ids = []

                async def annotator(item: DataItem) -> Annotation:
                    annotated_ids.append(item.id)
                    return Annotation(data={"label": "new"})

                async with view:
                    await view.annotate(annotator)

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
        """Test that annotating a batch without merged.parquet completes without error.

        DuckDB raises 'No files found', which is caught and logged. The generator
        yields nothing, so annotation completes with zero items processed.
        """
        from s3_data_tool.data_filtering import FilterForAnnotation

        session, kwargs, bucket, prefix = s3_setup
        test_id = str(uuid.uuid4())[:8]
        dataset_name = f"ann_not_merged_test_{test_id}"

        async with session.client("s3", **kwargs) as client:
            marker_key = f"{prefix}/{dataset_name}/batch1/.marker"
            await client.put_object(Bucket=bucket, Key=marker_key, Body=b"")

            view = FilterForAnnotation(
                client,
                bucket,
                prefix,
                dataset_name,
                "test_annotator",
                base_columns=["id", "text"],
            )

            async def annotator(item: DataItem) -> Annotation:
                return Annotation(data={"label": "test"})

            async with view:
                # Completes without raising — DuckDB error is caught internally
                await view.annotate(annotator)

            await client.delete_object(Bucket=bucket, Key=marker_key)


class TestDeserializeJsonFields:
    """Tests for _deserialize_json_fields function."""

    def test_deserialize_dict_field(self):
        """Test deserializing a dict field."""
        from s3_data_tool.data_filtering import deserialize_json_fields

        row = {"id": "1", "data": '{"key": "value"}', "text": '"Hello"'}
        result = deserialize_json_fields(row)

        assert result is not None
        assert result["id"] == 1
        assert result["data"] == {"key": "value"}
        assert result["text"] == "Hello"

    def test_deserialize_list_field(self):
        """Test deserializing a list field."""
        from s3_data_tool.data_filtering import deserialize_json_fields

        row = {"id": "1", "tags": '["a", "b", "c"]', "text": '"Hello"'}
        result = deserialize_json_fields(row)

        assert result is not None
        assert result["id"] == 1
        assert result["tags"] == ["a", "b", "c"]
        assert result["text"] == "Hello"

    def test_deserialize_multiple_json_fields(self):
        """Test deserializing multiple JSON fields."""
        from s3_data_tool.data_filtering import deserialize_json_fields

        row = {
            "id": "1",
            "data": '{"key": "value"}',
            "tags": '["a", "b"]',
            "text": '"Hello"',
        }
        result = deserialize_json_fields(row)

        assert result is not None
        assert result["data"] == {"key": "value"}
        assert result["tags"] == ["a", "b"]
        assert result["text"] == "Hello"

    def test_deserialize_invalid_json_preserves_string(self):
        """Test that invalid JSON string is preserved as-is."""
        from s3_data_tool.data_filtering import deserialize_json_fields

        row = {"id": "1", "data": "not valid json", "text": '"Hello"'}
        result = deserialize_json_fields(row)

        assert result is not None
        assert result["id"] == 1
        assert result["data"] == "not valid json"
        assert result["text"] == "Hello"

    def test_non_json_fields_preserved(self):
        """Test that non-string fields are preserved."""
        from s3_data_tool.data_filtering import deserialize_json_fields

        row = {"id": "1", "count": 42, "active": True}
        result = deserialize_json_fields(row)

        assert result is not None
        assert result["id"] == 1
        assert result["count"] == 42
        assert result["active"] is True
