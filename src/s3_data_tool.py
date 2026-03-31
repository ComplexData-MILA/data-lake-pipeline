import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aioboto3

from .dataset_generator import DatasetGenerator
from .models import StreamingConfigs


class S3DataTool:
    StreamingConfigs = StreamingConfigs

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str | None = None,
        endpoint_url: str | None = None,
    ):
        self._bucket = bucket or os.environ["S3_BUCKET"]
        self._prefix = prefix or os.environ.get("S3_PREFIX", "datasets")
        self._endpoint_url = endpoint_url or os.environ.get("S3_ENDPOINT_URL")
        self._session = aioboto3.Session()

    @asynccontextmanager
    async def dataset_generator(self) -> AsyncIterator[DatasetGenerator]:
        kwargs: dict[str, Any] = {}
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url

        async with self._session.client("s3", **kwargs) as s3_client:
            yield DatasetGenerator(s3_client, self._bucket, self._prefix)
