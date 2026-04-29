"""Unit tests for s3_utils."""

from unittest.mock import MagicMock

import pytest

from s3_data_tool.s3_utils import enumerate_parquet_paths, enumerate_parquet_paths_sync


class _AsyncPageIterable:
    """An async iterable over a list of pages, for mocking paginator.paginate()."""

    def __init__(self, pages: list):
        self._pages = pages

    def __aiter__(self):
        self._iter = iter(self._pages)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


def _make_mock_s3_client(all_keys: list[str]):
    """Create a mock S3 client for the async enumerate_parquet_paths.

    The mock paginator's paginate() applies S3 prefix filtering and returns
    an async iterable, matching the real aioboto3 API.
    """
    client = MagicMock()

    def make_paginator(*args, **kwargs):
        paginator = MagicMock()

        def paginate(*, Bucket, Prefix):
            matching = [k for k in all_keys if k.startswith(Prefix)]
            page = {}
            if matching:
                page["Contents"] = [{"Key": k} for k in matching]
            return _AsyncPageIterable([page])

        paginator.paginate = paginate
        return paginator

    client.get_paginator.side_effect = make_paginator
    return client


def _make_mock_s3_client_sync(all_keys: list[str]):
    """Create a mock S3 client for the sync enumerate_parquet_paths_sync."""
    client = MagicMock()

    def make_paginator(*args, **kwargs):
        paginator = MagicMock()

        def paginate(*, Bucket, Prefix):
            matching = [k for k in all_keys if k.startswith(Prefix)]
            page = {}
            if matching:
                page["Contents"] = [{"Key": k} for k in matching]
            return [page]

        paginator.paginate = paginate
        return paginator

    client.get_paginator.side_effect = make_paginator
    return client


class TestEnumerateParquetPaths:
    """Tests for enumerate_parquet_paths (async)."""

    @pytest.mark.asyncio
    async def test_base_paths_excludes_annotations(self):
        """Annotation merged.parquet files should not appear in base results."""
        client = _make_mock_s3_client([
            "datasets/posts/batch1/merged.parquet",
            "datasets/posts/annotations/feasibility_001/default/merged.parquet",
            "datasets/posts/batch2/merged.parquet",
            "datasets/posts/annotations/feasibility_001/batch2/merged.parquet",
        ])
        paths = await enumerate_parquet_paths(client, "bucket", "datasets", "posts")
        assert paths == [
            "s3://bucket/datasets/posts/batch1/merged.parquet",
            "s3://bucket/datasets/posts/batch2/merged.parquet",
        ]

    @pytest.mark.asyncio
    async def test_annotator_paths_only_returns_that_annotator(self):
        """When an annotator is specified, only that annotator's paths are returned."""
        client = _make_mock_s3_client([
            "datasets/posts/batch1/merged.parquet",
            "datasets/posts/annotations/feasibility_001/default/merged.parquet",
            "datasets/posts/annotations/feasibility_001/batch2/merged.parquet",
            "datasets/posts/annotations/other_annotator/default/merged.parquet",
        ])
        paths = await enumerate_parquet_paths(
            client, "bucket", "datasets", "posts", annotator="feasibility_001"
        )
        assert paths == [
            "s3://bucket/datasets/posts/annotations/feasibility_001/batch2/merged.parquet",
            "s3://bucket/datasets/posts/annotations/feasibility_001/default/merged.parquet",
        ]

    @pytest.mark.asyncio
    async def test_no_keys_returns_empty_list(self):
        """Empty S3 prefix should return an empty list."""
        client = _make_mock_s3_client([])
        paths = await enumerate_parquet_paths(client, "bucket", "datasets", "posts")
        assert paths == []

    @pytest.mark.asyncio
    async def test_no_merged_parquet_files_returns_empty_list(self):
        """Keys that don't end with /merged.parquet should be ignored."""
        client = _make_mock_s3_client([
            "datasets/posts/batch1/data.parquet",
            "datasets/posts/batch1/metadata.json",
        ])
        paths = await enumerate_parquet_paths(client, "bucket", "datasets", "posts")
        assert paths == []

    @pytest.mark.asyncio
    async def test_only_annotations_no_base_returns_empty(self):
        """Dataset with only annotation files returns empty for base lookup."""
        client = _make_mock_s3_client([
            "datasets/posts/annotations/feasibility_001/default/merged.parquet",
        ])
        paths = await enumerate_parquet_paths(client, "bucket", "datasets", "posts")
        assert paths == []


class TestEnumerateParquetPathsSync:
    """Tests for enumerate_parquet_paths_sync (synchronous variant)."""

    def test_base_paths_excludes_annotations(self):
        """Annotation merged.parquet files should not appear in base results."""
        client = _make_mock_s3_client_sync([
            "datasets/posts/batch1/merged.parquet",
            "datasets/posts/annotations/feasibility_001/default/merged.parquet",
            "datasets/posts/batch2/merged.parquet",
            "datasets/posts/annotations/other_annotator/batch1/merged.parquet",
        ])
        paths = enumerate_parquet_paths_sync(client, "bucket", "datasets", "posts")
        assert paths == [
            "s3://bucket/datasets/posts/batch1/merged.parquet",
            "s3://bucket/datasets/posts/batch2/merged.parquet",
        ]

    def test_annotator_paths_only_returns_that_annotator(self):
        """When an annotator is specified, only that annotator's paths are returned."""
        client = _make_mock_s3_client_sync([
            "datasets/posts/batch1/merged.parquet",
            "datasets/posts/annotations/feasibility_001/default/merged.parquet",
            "datasets/posts/annotations/feasibility_001/batch2/merged.parquet",
            "datasets/posts/annotations/other_annotator/default/merged.parquet",
        ])
        paths = enumerate_parquet_paths_sync(
            client, "bucket", "datasets", "posts", annotator="feasibility_001"
        )
        assert paths == [
            "s3://bucket/datasets/posts/annotations/feasibility_001/batch2/merged.parquet",
            "s3://bucket/datasets/posts/annotations/feasibility_001/default/merged.parquet",
        ]

    def test_no_keys_returns_empty_list(self):
        """Empty S3 prefix should return an empty list."""
        client = _make_mock_s3_client_sync([])
        paths = enumerate_parquet_paths_sync(client, "bucket", "datasets", "posts")
        assert paths == []

    def test_no_merged_parquet_files_returns_empty_list(self):
        """Keys that don't end with /merged.parquet should be ignored."""
        client = _make_mock_s3_client_sync([
            "datasets/posts/batch1/data.parquet",
            "datasets/posts/batch1/metadata.json",
        ])
        paths = enumerate_parquet_paths_sync(client, "bucket", "datasets", "posts")
        assert paths == []

    def test_only_annotations_no_base_returns_empty(self):
        """Dataset with only annotation files returns empty for base lookup."""
        client = _make_mock_s3_client_sync([
            "datasets/posts/annotations/feasibility_001/default/merged.parquet",
        ])
        paths = enumerate_parquet_paths_sync(client, "bucket", "datasets", "posts")
        assert paths == []
