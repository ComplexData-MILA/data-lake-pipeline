import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from collections.abc import Mapping
from typing import Any

import aioboto3

from .dataset_generator import DatasetGenerator
from .filter import FilterNode
from .data_filtering import FilterForAnnotation, FilterForExport
from .models import StreamingConfigs


class S3DataTool:
    StreamingConfigs = StreamingConfigs

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str | None = None,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        self._bucket = bucket or os.environ["S3_BUCKET"]
        self._prefix = prefix or os.environ.get("S3_PREFIX", "datasets")
        self._endpoint_url = endpoint_url or os.environ.get("S3_ENDPOINT_URL")
        self._access_key = access_key or os.environ.get("S3_ACCESS_KEY")
        self._secret_key = secret_key or os.environ.get("S3_SECRET_KEY")
        self._session = aioboto3.Session(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    @asynccontextmanager
    async def dataset_generator(self) -> AsyncIterator[DatasetGenerator]:
        kwargs: dict[str, Any] = {}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        async with self._session.client("s3", **kwargs) as s3_client:  # type: ignore
            yield DatasetGenerator(s3_client, self._bucket, self._prefix)

    @asynccontextmanager
    async def filter_for_annotation(
        self,
        name: str,
        annotator_name: str,
        base_columns: list[str],
        annotator_columns: dict[str, list[str]] | None = None,
        base_filter: FilterNode | None = None,
        annotator_filters: dict[str, FilterNode] | None = None,
        lock_ttl_ms: int = 3600000,
        fraction: float = 1.0,
    ) -> AsyncIterator[FilterForAnnotation]:
        """Create annotation view with optional filter."""
        kwargs: dict[str, Any] = {}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        async with self._session.client("s3", **kwargs) as s3_client:  # type: ignore
            view = FilterForAnnotation(
                s3_client,
                self._bucket,
                self._prefix,
                dataset_name=name,
                annotator_name=annotator_name,
                annotator_columns=annotator_columns,
                base_columns=base_columns,
                base_filter=base_filter,
                annotator_filters=annotator_filters,
                lock_ttl_ms=lock_ttl_ms,
                fraction=fraction,
            )
            async with view:
                yield view

    @asynccontextmanager
    async def filter_for_export(
        self,
        name: str,
        base_columns: list[str],
        annotator_columns: Mapping[str, list[str]] | None = None,
        base_filter: FilterNode | None = None,
        annotator_filters: Mapping[str, FilterNode] | None = None,
    ) -> AsyncIterator[FilterForExport]:
        """Create read-only export view with optional filter."""
        kwargs: dict[str, Any] = {}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        async with self._session.client("s3", **kwargs) as s3_client:  # type: ignore
            view = FilterForExport(
                s3_client,
                self._bucket,
                self._prefix,
                dataset_name=name,
                base_columns=base_columns,
                annotator_columns=annotator_columns,
                base_filter=base_filter,
                annotator_filters=annotator_filters,
            )
            async with view:
                yield view
