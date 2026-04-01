"""Filter views for annotation and export."""

import asyncio
import io
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from .async_utils import with_semaphore
from .filter import FilterNode
from .models import Annotation, DataItem, StreamingConfigs
from .s3_lock import S3Lock, as_completed_subject_to_renewal
from .s3_utils import generate_hex_id, s3_object_exists, upload_jsonl_chunk
from .cleanup import enumerate_batches

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


logger = logging.getLogger(__name__)


class AnnotationLockError(Exception):
    """Raised when annotation lock acquisition fails."""

    pass


class DatasetNotMergedError(Exception):
    """Raised when dataset parquet doesn't exist."""

    pass


async def read_parquet_from_s3(
    s3_client: "S3Client", bucket: str, key: str
) -> list[dict[str, Any]]:
    """Read parquet file from S3."""
    try:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        body = await response["Body"].read()
        buf = io.BytesIO(body)
        table = pq.read_table(buf)
        return table.to_pylist()
    except Exception as e:
        logger.warning(f"Failed to read parquet {key}: {e}")
        return []


async def filter_rows_with_duckdb(
    rows: list[dict[str, Any]],
    where_clause: str | None = "TRUE",
    exclude_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Filter rows using DuckDB with optional ID exclusion."""
    if exclude_ids:
        rows = [r for r in rows if r.get("id") not in exclude_ids]

    if not rows:
        return []

    if (not where_clause) or (where_clause == "TRUE"):
        return rows

    df = pd.DataFrame(rows)
    conn = duckdb.connect()
    try:
        conn.execute("CREATE TABLE data AS SELECT * FROM df")
        result = conn.execute(f"SELECT * FROM data WHERE {where_clause}").fetchall()
        col_names = [desc[0] for desc in conn.execute("DESCRIBE data").fetchall()]
        return [dict(zip(col_names, row)) for row in result]
    finally:
        conn.close()


async def annotate_single_row(
    item: DataItem,
    annotation_fn: Callable[[DataItem], Awaitable[Annotation]],
) -> dict[str, Any]:
    """Annotate a single row and return formatted result."""
    result = await annotation_fn(item)
    return {
        "id": item.id,
        "batch": item.batch,
        "data": result.data,
        "metadata": result.metadata,
        "annotated_at": datetime.now(timezone.utc).isoformat(),
    }


class FilterForExport:
    """Shared functionality for annotation and export views."""

    def __init__(
        self,
        s3_client: "S3Client",
        bucket: str,
        prefix: str,
        dataset_name: str,
        filter: FilterNode | None,
    ):
        self._s3_client = s3_client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._dataset_name = dataset_name
        self._filter = filter

    async def _discover_columns(self) -> set[str]:
        """Discover dataset columns from schema."""
        from .s3_utils import discover_dataset_columns

        return await discover_dataset_columns(
            self._s3_client, self._bucket, self._prefix, self._dataset_name
        )

    async def _read_dataset_batch(self, batch: str) -> list[dict[str, Any]]:
        """Read dataset batch parquet file."""
        key = f"{self._prefix}/{self._dataset_name}/{batch}/merged.parquet"
        return await read_parquet_from_s3(self._s3_client, self._bucket, key)

    async def _read_annotation_batch(
        self, annotator: str, batch: str
    ) -> list[dict[str, Any]]:
        """Read annotation batch parquet file."""
        key = f"{self._prefix}/{self._dataset_name}/annotations/{annotator}/{batch}/merged.parquet"
        return await read_parquet_from_s3(self._s3_client, self._bucket, key)


class FilterForAnnotation(FilterForExport):
    """View for annotating filtered dataset rows."""

    def __init__(
        self,
        s3_client: "S3Client",
        bucket: str,
        prefix: str,
        dataset_name: str,
        annotator_name: str,
        filter: FilterNode | None,
        lock_ttl_ms: int = 3_600_000
    ):
        """Add S3 lock on top of FilterForExport."""
        super().__init__(s3_client, bucket, prefix, dataset_name, filter)
        self._dataset_name = dataset_name
        self._annotator_name = annotator_name
        
        lock_path = (
            f"{self._prefix}/{dataset_name}/annotations/{annotator_name}"
        )
        
        self._lock: S3Lock = S3Lock(lock_path, lock_ttl_ms, s3_client, bucket)

    async def __aenter__(self) -> "FilterForAnnotation":
        """Acquire annotation lock."""
        acquired = await self._lock.acquire()
        if not acquired:
            raise AnnotationLockError(
                f"Failed to acquire lock for {self._annotator_name} on dataset {self._dataset_name}"
            )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool:
        """Release annotation lock."""
        if self._lock:
            await self._lock.release()
        return False

    async def annotate(
        self,
        annotation_fn: Callable[[DataItem], Awaitable[Annotation]],
        max_concurrency: int = 16,
        batch: str | None = None,
        streaming_configs: StreamingConfigs | None = None,
    ) -> None:
        """
        Annotate filtered rows.

        Args:
            annotation_fn: Async function to annotate each row
            max_concurrency: Max concurrent annotation calls
            batch: Batch name for annotation output
            streaming_configs: Streaming configuration
        """
        streaming_configs = streaming_configs or StreamingConfigs()

        batch_name = batch or "default"
        run_id = generate_hex_id()
        base_path = (
            f"{self._prefix}/{self._dataset_name}/annotations/"
            f"{self._annotator_name}/{batch_name}"
        )

        columns = await self._discover_columns()

        where_clause = "TRUE"
        if self._filter:
            where_clause = self._filter.compile(columns)
            if where_clause == "FALSE":
                return

        semaphore = asyncio.Semaphore(max_concurrency)
        batches = await enumerate_batches(
            self._s3_client, self._bucket, self._prefix, self._dataset_name
        )

        # TODO: Iterate on data from batches, reusing base logic from FilterForExport.
        tasks = [
            
        ]

        # TODO: reuse logic from dataset_generator.py for batched jsonl upload.
        # TODO: extract into a reused function in s3_utils.py if appropriate. 
        for result in as_completed_subject_to_renewal(self._lock, tasks):
            """Add result to buffer. Upload to S3 if buffer is full."""

        """Upload any training data in buffer."""