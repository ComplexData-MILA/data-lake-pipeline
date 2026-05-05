"""Filter views for annotation and export.

Producer-Processor-Uploader Architecture:
    Two async queues enable streaming annotation without loading full dataset in memory:

    [Producer] → input_queue → [Workers (N)] → output_queue → [Uploader]

    - Producer: Iterates dataset batches, filters, yields DataItems
    - Workers: Pull items, call annotation_fn, push results
    - Uploader: Buffer and upload JSONL chunks to S3

    Lock renewal is handled via gather_subject_to_lock_renewal().
"""

import asyncio
import json
import logging
import os
import random
import shutil
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping

import duckdb

from .filter import FilterNode
from .models import Annotation, DataItem, StreamingConfigs
from .s3_lock import S3Lock, gather_subject_to_lock_renewal, gather_subject_to_multi_lock_renewal
from .s3_utils import enumerate_batches, transform_row_for_jsonl, upload_jsonl_chunk
from tqdm.auto import tqdm

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


logger = logging.getLogger(__name__)

DataItemOrNone = DataItem | None
AnnotationResult = dict[str, Any] | None


def deserialize_json_fields(row_dict: dict[str, Any]) -> dict[str, Any] | None:
    """Deserialize all string values back to their original types."""
    result = {}
    for key, value in row_dict.items():
        if isinstance(value, str):
            try:
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                result[key] = value
        else:
            result[key] = value
    return result


class AnnotationLockError(Exception):
    """Raised when annotation lock acquisition fails."""

    pass


class DatasetNotMergedError(Exception):
    """Raised when dataset parquet doesn't exist."""

    pass


async def produce_items_for_annotation(
    input_queue: asyncio.Queue[DataItemOrNone],
    dataloader: AsyncIterator[DataItem],
    num_workers: int,
) -> None:
    """Producer task: Feed items from dataloader into input queue.

    Sends None sentinels at end to signal worker completion.
    """
    async for item in dataloader:
        await input_queue.put(item)
    for _ in range(num_workers):
        await input_queue.put(None)


async def annotation_worker(
    input_queue: asyncio.Queue[DataItemOrNone],
    output_queue: asyncio.Queue[AnnotationResult],
    annotation_fn: Callable[[DataItem], Awaitable[Annotation]],
    worker_id: int,
    progress_callback: Callable[[], Any] | None = None,
) -> None:
    """Worker task: Pull items, annotate, push results.

    Exits on None sentinel. Skips rows on error (allows retry in subsequent runs).
    Sends None sentinel to output queue on exit.
    """
    while True:
        item = await input_queue.get()
        if item is None:
            input_queue.task_done()
            break
        try:
            result = await _annotate_single_row(item, annotation_fn)
            await output_queue.put(result)
            if progress_callback:
                progress_callback()
        except Exception as e:
            logger.warning(
                f"Worker {worker_id}: Skipping row {item.id} due to error: {e}"
            )
        finally:
            input_queue.task_done()
    await output_queue.put(None)


async def upload_annotation_results(
    output_queue: asyncio.Queue[AnnotationResult],
    s3_client: "S3Client",
    bucket: str,
    base_path: str,
    chunk_size: int,
    num_workers: int,
) -> None:
    """Uploader task: Buffer results and upload JSONL chunks.

    Exits after receiving all worker sentinels. Upload path:
        {base_path}/.temp/chunk_{chunk_idx:05d}.jsonl
    """
    buffer: list[dict[str, Any]] = []
    chunk_idx = 0
    workers_done = 0

    while workers_done < num_workers:
        result = await output_queue.get()
        if result is None:
            workers_done += 1
            output_queue.task_done()
            continue

        buffer.append(result)
        if len(buffer) >= chunk_size:
            key = f"{base_path}/.temp/chunk_{chunk_idx:05d}.jsonl"
            await upload_jsonl_chunk(s3_client, bucket, key, buffer)
            logger.info(f"Uploaded annotation chunk: {key}")
            buffer.clear()
            chunk_idx += 1
        output_queue.task_done()

    if buffer:
        key = f"{base_path}/.temp/chunk_{chunk_idx:05d}.jsonl"
        await upload_jsonl_chunk(s3_client, bucket, key, buffer)
        logger.info(f"Uploaded final annotation chunk: {key}")


async def _annotate_single_row(
    item: DataItem,
    annotation_fn: Callable[[DataItem], Awaitable[Annotation]],
) -> dict[str, Any]:
    """Annotate a single row and return formatted result."""
    result = await annotation_fn(item)
    record = {
        **result.data,
        "id": item.id,
        "batch": item.batch,
        "metadata": result.metadata,
        "annotated_at": datetime.now(timezone.utc).isoformat(),
    }
    return transform_row_for_jsonl(record)


class FilterForExport:
    """Shared functionality for annotation and export views."""

    def __init__(
        self,
        s3_client: "S3Client",
        bucket: str,
        prefix: str,
        dataset_name: str,
        base_columns: list[str],
        annotator_columns: Mapping[str, list[str]] | None = None,
        base_filter: FilterNode | None = None,
        annotator_filters: Mapping[str, FilterNode] | None = None,
    ):
        self._s3_client = s3_client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._dataset_name = dataset_name
        self._base_columns = ["_batch", *base_columns]
        self._annotator_columns = annotator_columns or {}
        self._base_filter = base_filter  # base text dataset, as opposed to annotations
        self._annotator_filters = annotator_filters or {}

    @property
    def base_path(self) -> str:
        return f"s3://{self._bucket}/{self._prefix}/{self._dataset_name}"

    def _build_filtered_cte(
        self,
        name: str,
        path: str,
        columns: list[str],
        filter_node: FilterNode | None,
    ) -> str:
        non_id_cols = [c for c in columns if c != "id"]
        select_parts = ["id"]
        for col in non_id_cols:
            select_parts.append(f"ANY_VALUE({col}) AS {col}")
        cols_str = ", ".join(select_parts)
        where_clause = (
            f" WHERE {filter_node.compile(set(columns))}" if filter_node else ""
        )
        return f"{name} AS (SELECT {cols_str} FROM read_parquet('{path}'){where_clause} GROUP BY id)"

    async def get_duckdb_query(self) -> str:
        """Generate DuckDB query with WITH clause for filtered CTEs."""
        ctes = []

        # Base text dataset
        base_path = f"{self.base_path}/*/merged.parquet"
        ctes.append(
            self._build_filtered_cte(
                "base_filtered", base_path, self._base_columns, self._base_filter
            )
        )

        # Per-annotator parquets
        ctes.extend(
            self._build_filtered_cte(
                f"{_annotator}_filtered",
                f"{self.base_path}/annotations/{_annotator}/*/merged.parquet",
                _cols,
                self._annotator_filters.get(_annotator),
            )
            for _annotator, _cols in self._annotator_columns.items()
        )

        select_parts = ["base_filtered.*"] + [
            f"{a}_filtered.*" for a in self._annotator_columns
        ]

        if not self._annotator_columns:
            return f"WITH {', '.join(ctes)} SELECT {', '.join(select_parts)} FROM base_filtered"

        join_targets = [
            f"JOIN {a}_filtered USING (id)" for a in self._annotator_columns
        ]
        join_clause = " ".join(join_targets)

        return (
            f"WITH {', '.join(ctes)} SELECT {', '.join(select_parts)} "
            f"FROM base_filtered {join_clause}"
        )

    async def _iter_filtered_items(self) -> AsyncIterator[DataItem]:
        """Stream filtered DataItems using DuckDB query."""
        query = await self.get_duckdb_query()
        logger.debug(f"Executing DuckDB query: {query}")
        try:
            async for item in self._execute_and_stream(query):
                yield item
        except Exception as e:
            logger.error(f"Query execution failed: {e}")

    async def _execute_and_stream(self, query: str) -> AsyncIterator[DataItem]:
        """Execute query and stream results as DataItems."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "query.duckdb")
        conn = duckdb.connect(db_path, read_only=False)
        try:
            s3_endpoint = os.environ["S3_ENDPOINT_URL"]
            endpoint_host = s3_endpoint.removeprefix("https://").rstrip("/")
            endpoint_host = endpoint_host.removeprefix("http://").rstrip("/")
            use_ssl = s3_endpoint.startswith("https://")

            conn.execute(
                f"""
                SET s3_access_key_id='{os.environ['S3_ACCESS_KEY']}';
                SET s3_secret_access_key='{os.environ['S3_SECRET_KEY']}';
                SET s3_endpoint='{endpoint_host}';
                SET s3_use_ssl={str(use_ssl).lower()};
                SET s3_url_style='path';
            """
            )
            result = conn.execute(query)
            description = result.description
            while True:
                rows = result.fetchmany(int(os.environ.get("DUCK_DB_BATCH_SIZE", 1000)))
                if not rows:
                    break
                for row in rows:
                    row_dict = {description[i][0]: row[i] for i in range(len(row))}
                    row_dict = deserialize_json_fields(row_dict)
                    if row_dict is None:
                        continue
                    yield DataItem(
                        id=str(row_dict["id"]), batch=row_dict["_batch"], data=row_dict
                    )
        finally:
            conn.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

    def __iter__(self):
        return self._iter_filtered_items()

    def __aiter__(self) -> AsyncIterator[DataItem]:
        return self._iter_filtered_items()

    async def __aenter__(self) -> "FilterForExport":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool:
        return False


class FilterForAnnotation(FilterForExport):
    """View for annotating filtered dataset rows.

    Uses producer-processor-uploader pattern with two async queues:
    - input_queue: DataItems waiting to be annotated
    - output_queue: Annotated results waiting to be uploaded

    This enables streaming annotation without loading full dataset in memory.
    """

    def __init__(
        self,
        s3_client: "S3Client",
        bucket: str,
        prefix: str,
        dataset_name: str,
        annotator_name: str,
        base_columns: list[str],
        annotator_columns: dict[str, list[str]] | None = None,
        base_filter: FilterNode | None = None,
        annotator_filters: dict[str, FilterNode] | None = None,
        lock_ttl_ms: int = 3_600_000,
    ):
        super().__init__(
            s3_client,
            bucket,
            prefix,
            dataset_name,
            base_columns,
            annotator_columns,
            base_filter,
            annotator_filters,
        )
        self._annotator_name = annotator_name
        self._lock_ttl_ms = lock_ttl_ms

    async def _iter_filtered_items(
        self, batches: list[str] | None = None
    ) -> AsyncIterator[DataItem]:
        """Stream filtered DataItems excluding already annotated rows.

        Args:
            batches: If provided, query only these specific batches. Otherwise uses glob.
        """
        if batches:
            base_paths = [f"{self.base_path}/{b}/merged.parquet" for b in batches]
            annotator_paths = [
                f"{self.base_path}/annotations/{self._annotator_name}/{b}/merged.parquet"
                for b in batches
            ]
            base_paths_str = ", ".join(f"'{p}'" for p in base_paths)
            annot_paths_str = ", ".join(f"'{p}'" for p in annotator_paths)
            base_read = f"read_parquet([{base_paths_str}])"
            annot_read = f"read_parquet([{annot_paths_str}])"
        else:
            base_read = f"read_parquet('{self.base_path}/*/merged.parquet')"
            annot_read = (
                f"read_parquet('{self.base_path}/annotations/"
                f"{self._annotator_name}/*/merged.parquet')"
            )

        non_id_cols = [c for c in self._base_columns if c != "id"]
        select_parts = ["id"]
        for col in non_id_cols:
            select_parts.append(f"ANY_VALUE({col}) AS {col}")
        cols_str = ", ".join(select_parts)

        base_filter = self._base_filter
        where_clause = (
            f" WHERE {base_filter.compile(set(self._base_columns))}"
            if base_filter
            else ""
        )

        query = f"""
        WITH base_filtered AS (
            SELECT {cols_str} FROM {base_read}{where_clause} GROUP BY id
        ),
        annotator_done AS (
            SELECT id FROM {annot_read}
        )
        SELECT base_filtered.*
        FROM base_filtered
        LEFT JOIN annotator_done USING (id)
        WHERE annotator_done.id IS NULL
        """

        try:
            async for item in self._execute_and_stream(query):
                yield item
        except Exception as e:
            error_msg = str(e)
            if "No files found" in error_msg and "annotations/" in error_msg:
                fallback_query = f"""
                WITH base_filtered AS (
                    SELECT {cols_str} FROM {base_read}{where_clause} GROUP BY id
                )
                SELECT base_filtered.*
                FROM base_filtered
                """
                async for item in self._execute_and_stream(fallback_query):
                    yield item
            else:
                logger.error(f"Query execution failed: {e}")

    async def _claim_batches(self, max_batches: int) -> list[tuple[str, S3Lock]]:
        """Try to claim up to max_batches using per-batch S3 locks.

        Returns list of (batch_name, lock) for successfully claimed batches.
        """
        all_batches = await enumerate_batches(
            self._s3_client, self._bucket, self._prefix, self._dataset_name
        )
        rng = random.Random()
        rng.shuffle(all_batches)

        claimed: list[tuple[str, S3Lock]] = []
        for batch_name in all_batches:
            if len(claimed) >= max_batches:
                break
            lock_path = (
                f"{self._prefix}/{self._dataset_name}/annotations/"
                f"{self._annotator_name}/{batch_name}"
            )
            lock = S3Lock(lock_path, self._lock_ttl_ms, self._s3_client, self._bucket)
            if await lock.acquire():
                claimed.append((batch_name, lock))
        return claimed

    async def _process_single_batch(
        self,
        batch_name: str,
        annotation_fn: Callable[[DataItem], Awaitable[Annotation]],
        max_concurrency: int,
        streaming_configs: StreamingConfigs,
        held_locks: list[S3Lock],
    ) -> None:
        """Process a single batch using the producer-worker-uploader pattern."""
        base_path = (
            f"{self._prefix}/{self._dataset_name}/annotations/"
            f"{self._annotator_name}/{batch_name}"
        )

        queue_size = max_concurrency * 2
        input_queue: asyncio.Queue[DataItemOrNone] = asyncio.Queue(maxsize=queue_size)
        output_queue: asyncio.Queue[AnnotationResult] = asyncio.Queue(
            maxsize=queue_size
        )

        dataloader = self._iter_filtered_items(batches=[batch_name])

        pbar = tqdm(desc=f"Annotating {batch_name}", ncols=80)

        producer_task = asyncio.create_task(
            produce_items_for_annotation(input_queue, dataloader, max_concurrency)
        )
        worker_tasks = [
            asyncio.create_task(
                annotation_worker(
                    input_queue, output_queue, annotation_fn, i, lambda: pbar.update(1)
                )
            )
            for i in range(max_concurrency)
        ]
        uploader_task = asyncio.create_task(
            upload_annotation_results(
                output_queue,
                self._s3_client,
                self._bucket,
                base_path,
                streaming_configs.chunk_size,
                max_concurrency,
            )
        )

        if len(held_locks) == 1:
            results = await gather_subject_to_lock_renewal(
                held_locks[0],
                [producer_task, *worker_tasks, uploader_task],
            )
        else:
            results = await gather_subject_to_multi_lock_renewal(
                held_locks,
                [producer_task, *worker_tasks, uploader_task],
            )
        pbar.close()
        errors = [f"{r.__traceback__}" for r in results if isinstance(r, BaseException)]
        if errors:
            logger.warning(f"Errors encountered: \n{"\n".join(errors)}")

    async def annotate(
        self,
        annotation_fn: Callable[[DataItem], Awaitable[Annotation]],
        max_concurrency: int = 16,
        max_batches: int = 1,
        streaming_configs: StreamingConfigs | None = None,
    ) -> None:
        """Annotate filtered rows using producer-processor-uploader pattern.

        Batches are auto-claimed via per-batch S3 locks: up to ``max_batches``
        are claimed and processed sequentially. Multiple instances running the
        same annotator naturally partition the work — each instance claims
        disjoint batches via the lock mechanism.

        Args:
            annotation_fn: Async function to annotate each row
            max_concurrency: Number of concurrent annotation workers
            max_batches: Maximum batches to claim (default 1).
            streaming_configs: Streaming configuration (chunk_size)
        """
        streaming_configs = streaming_configs or StreamingConfigs()

        claimed = await self._claim_batches(max_batches)
        if not claimed:
            logger.info(
                f"No unclaimed batches for {self._annotator_name} "
                f"on {self._dataset_name}"
            )
            return

        held_locks = [l for _, l in claimed]
        try:
            for batch_name, _ in claimed:
                await self._process_single_batch(
                    batch_name, annotation_fn, max_concurrency,
                    streaming_configs, held_locks,
                )
        finally:
            for lock in held_locks:
                await lock.release()
