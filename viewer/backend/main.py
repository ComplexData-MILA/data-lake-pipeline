"""FastAPI backend for data viewer."""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import anyio
import boto3
import duckdb
import threading
from botocore.config import Config as BotoConfig
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from . import charts, db, orderings
from .cache import (
    activity_key,
    activity_keys_set,
    annotators_key,
    cached_sync,
    categorical_key,
    categorical_keys_set,
    conversion_key,
    count_key,
    count_keys_set,
    create_async_redis,
    create_sync_redis,
    datasets_key,
    files_key,
    invalidation_subscriber,
    schema_key,
    schema_keys_set,
)
from .duckdb_query import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_PREFIX,
    S3_SECRET_KEY,
    FilterSpec,
    _union_source,
    build_count_query,
    build_count_query_fast,
    build_query,
    execute_query,
    init_pool,
    keyset_eligible,
    run_keyset_page,
    shutdown_pool,
)
from .events_bus import EventBus
from .s3_files import FileManifest, build_file_manifest
from .sse import events_handler
from .watcher import S3Watcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIMING_ENABLED = os.environ.get("VIEWER_LOG_TIMINGS", "0") == "1"

s3_client = None
_redis_sync = None
_redis_async = None


def get_s3_client():
    """Get or create the S3 client."""
    global s3_client
    if s3_client is None:
        use_ssl = S3_ENDPOINT_URL.startswith("https://")
        config = BotoConfig(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        )
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            use_ssl=use_ssl,
            config=config,
        )
    return s3_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis_sync, _redis_async
    init_pool()
    _redis_sync = create_sync_redis()
    _redis_async = create_async_redis()

    bus = EventBus(_redis_async)
    await bus.start()
    watcher = S3Watcher(
        S3_BUCKET,
        S3_PREFIX,
        bus,
        list_fn=_list_dataset_objects,
        redis_client=_redis_async,
        list_datasets_fn=lambda: cached_sync(
            _redis_sync, datasets_key(), 60, list_datasets_from_s3
        ),
    )
    await watcher.start()
    app.state.bus = bus
    app.state.watcher = watcher
    app.state.loop = asyncio.get_running_loop()

    invalidation_task = None
    if _redis_async is not None:
        invalidation_task = asyncio.create_task(
            invalidation_subscriber(_redis_async)
        )
    ordering_cleanup_task = asyncio.create_task(orderings.cleanup_loop())
    logger.info("DuckDB connection pool initialized; redis=%s", _redis_sync is not None)
    yield
    if invalidation_task is not None:
        invalidation_task.cancel()
    ordering_cleanup_task.cancel()
    await watcher.stop()
    await bus.stop()
    shutdown_pool()
    _redis_sync = None
    _redis_async = None


def _list_dataset_objects(dataset: str) -> dict:
    """List one dataset's objects as {key: (etag, size, last_modified)} (blocking)."""
    client = get_s3_client()
    snapshot: dict = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX.rstrip('/')}/{dataset}/"
    ):
        for obj in page.get("Contents", []):
            snapshot[obj["Key"]] = (
                obj.get("ETag", ""),
                obj.get("Size", 0),
                str(obj.get("LastModified", "")),
            )
    return snapshot


app = FastAPI(title="Data Viewer API", lifespan=lifespan)

CORS_ORIGINS = [
    o.strip()
    for o in os.environ.get("VIEWER_CORS_ORIGINS", "*").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_request_timing(request: Request, call_next):
    """Log per-request wall time when VIEWER_LOG_TIMINGS=1 (benchmarking aid)."""
    start = time.perf_counter()
    response = await call_next(request)
    if TIMING_ENABLED:
        logger.info(
            "timing path=%s status=%d ms=%.1f",
            request.url.path,
            response.status_code,
            (time.perf_counter() - start) * 1000,
        )
    return response


class DatasetListResponse(BaseModel):
    datasets: list[str]


class AnnotationListResponse(BaseModel):
    annotators: list[str]


class SchemaColumn(BaseModel):
    name: str
    type: str


class SchemaResponse(BaseModel):
    columns: list[SchemaColumn]


class DataResponse(BaseModel):
    rows: list[dict[str, Any]]
    columns: list[str]
    annotator_columns: dict[str, list[str]] = {}
    next_cursor: str | None = None
    has_more: bool | None = None


class CountResponse(BaseModel):
    count: int


class ConversionResponse(BaseModel):
    total_batches: int = 0
    converted: int = 0
    in_progress_batch: str | None = None
    error: str | None = None
    oversized: bool = False
    started_at: str | None = None
    updated_at: str | None = None
    annotation_total: int = 0
    annotation_converted: int = 0


class ActivityBucket(BaseModel):
    ts: str
    count: int


class ActivityDataset(BaseModel):
    dataset: str
    buckets: list[ActivityBucket]


class WindowInfo(BaseModel):
    start: str | None = None
    end: str | None = None


class ActivityResponse(BaseModel):
    datasets: list[ActivityDataset]
    window: WindowInfo
    bucket: str
    generated_at: str


class CategoricalResponse(BaseModel):
    mode: str
    column: str
    values: list[dict[str, Any]] = []  # counts mode: {"value": str, "count": int}
    total: int = 0
    distinct: int | None = None  # counts mode
    truncated: bool | None = None  # counts mode
    top_values: list[str] = []  # trend mode
    series: list[dict[str, Any]] = []  # trend mode: {"ts": str, "value": str, "count": int}
    window: WindowInfo = WindowInfo()
    generated_at: str


def _prefix_contains_data(
    client,
    bucket: str,
    prefix: str,
) -> bool:
    """Return True if any key under *prefix* is a merged.parquet or a JSONL chunk.

    JSONL chunks count so datasets/annotators with only live (unmerged) data
    are still listed.
    """
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]
            if (
                key.endswith("/merged.parquet")
                or key.endswith(".jsonl")
                or (filename.startswith("merged_") and filename.endswith(".jsonl.gz"))
            ):
                return True
    return False


def list_datasets_from_s3() -> list[str]:
    """List all datasets under the S3 prefix that have merged or live data."""
    client = get_s3_client()
    list_prefix = f"{S3_PREFIX.rstrip('/')}/"

    paginator = client.get_paginator("list_objects_v2")
    candidate_names: list[str] = []
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=list_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            prefix_val = cp.get("Prefix", "")
            name = prefix_val[len(list_prefix):].rstrip("/")
            if name and not name.startswith("annotations"):
                candidate_names.append(name)

    datasets: list[str] = []
    for name in candidate_names:
        if _prefix_contains_data(client, S3_BUCKET, f"{list_prefix}{name}/"):
            datasets.append(name)
    return sorted(datasets)


def list_annotators_from_s3(dataset_name: str) -> list[str]:
    """List all annotators for a dataset that have merged or live data."""
    client = get_s3_client()
    annotations_prefix = f"{S3_PREFIX}/{dataset_name}/annotations/"

    paginator = client.get_paginator("list_objects_v2")
    candidate_names: list[str] = []
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=annotations_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            prefix_val = cp.get("Prefix", "")
            name = prefix_val[len(annotations_prefix):].rstrip("/")
            if name:
                candidate_names.append(name)

    annotators: list[str] = []
    for name in candidate_names:
        if _prefix_contains_data(
            client, S3_BUCKET, f"{annotations_prefix}{name}/"
        ):
            annotators.append(name)
    return sorted(annotators)


def _manifest_for(dataset_name: str, fresh: bool = False) -> FileManifest:
    """Build (or fetch from Redis) the file manifest for a dataset.

    With ``fresh=True`` the cached copy is dropped first, so retries after a
    transient mid-query chunk deletion re-list S3 instead of replaying the
    stale manifest.
    """
    client = get_s3_client()
    key = files_key(dataset_name)
    if fresh and _redis_sync is not None:
        try:
            _redis_sync.delete(key)
        except Exception:  # noqa: BLE001
            pass
    return cached_sync(
        _redis_sync,
        key,
        5,
        lambda: build_file_manifest(client, S3_BUCKET, S3_PREFIX, dataset_name),
        encode=lambda m: m.model_dump_json(),
        decode=lambda raw: FileManifest(**json.loads(raw)),
    )


def _manifest_attempt(fn) -> Any:
    """Run *fn(fresh)*, retrying once with a fresh S3 listing on DuckDB errors.

    clean-up can delete JSONL chunks mid-query; the retry re-lists S3 (bypassing
    the manifest cache) and re-runs.
    """
    try:
        return fn(False)
    except duckdb.Error as e:
        logger.warning(f"Retrying query after transient DuckDB error: {e}")
        return fn(True)


def _get_schema_columns(
    dataset_name: str, annotators: list[str], manifest: FileManifest
) -> list[SchemaColumn]:
    """Get combined schema from merged + live data of dataset and annotators."""
    columns: dict[str, str] = {}

    base_src = _union_source(
        manifest.merged_parquet, manifest.merged_jsonl + manifest.live_jsonl
    )
    if base_src:
        try:
            results = execute_query(f"SELECT * FROM {base_src} LIMIT 1")
            if results:
                for col_name in results[0].keys():
                    if col_name and col_name not in columns:
                        columns[col_name] = "unknown"
        except Exception as e:
            logger.warning(f"Failed to get schema from base: {e}")

    for annotator in annotators:
        files = manifest.annotators.get(annotator)
        if not files:
            continue
        src = _union_source(
            files.merged_parquet, files.merged_jsonl + files.live_jsonl
        )
        if not src:
            continue
        try:
            results = execute_query(f"SELECT * FROM {src} LIMIT 1")
            if results:
                for col_name in results[0].keys():
                    if col_name and col_name not in ["id", "_batch"]:
                        columns[f"{annotator}.{col_name}"] = "unknown"
        except Exception as e:
            logger.warning(f"Failed to get schema from annotator {annotator}: {e}")

    return [SchemaColumn(name=name, type=typ) for name, typ in sorted(columns.items())]


def _default_base_columns(dataset_name: str, manifest: FileManifest) -> list[str]:
    """All base schema columns — the default when a request omits ``columns``.

    Shares the exact cache entry the /schema endpoint uses with no annotators
    (TTL 300, invalidated on data events).
    """
    names = _schema_column_names(dataset_name, manifest)
    return names or ["id", "_batch"]


def _schema_column_names(dataset_name: str, manifest: FileManifest) -> list[str]:
    """Base schema column names (cached, shared with the /schema endpoint)."""
    cols = cached_sync(
        _redis_sync,
        schema_key(dataset_name, []),
        300,
        lambda: _get_schema_columns(dataset_name, [], manifest),
        lambda cols: json.dumps([c.model_dump() for c in cols]),
        lambda raw: [SchemaColumn(**x) for x in json.loads(raw)],
        schema_keys_set(dataset_name),
    )
    return [c.name for c in cols]


def _schema_has_created_at(dataset_name: str, manifest: FileManifest) -> bool:
    """Whether any file in *dataset_name* carries the _created_at column.

    A dataset whose files predate the field has no such column at all, and
    the chart SQL would fail to bind it — so those datasets must be skipped.
    """
    return "_created_at" in _schema_column_names(dataset_name, manifest)


def _get_annotator_columns(
    dataset_name: str, annotator: str, manifest: FileManifest
) -> list[str]:
    """Get available column names for an annotator (merged + live, all batches)."""
    files = manifest.annotators.get(annotator)
    if not files:
        raise HTTPException(status_code=404, detail="Annotator not found")
    src = _union_source(files.merged_parquet, files.merged_jsonl + files.live_jsonl)
    if not src:
        raise HTTPException(status_code=404, detail="Annotator not found")
    try:
        results = execute_query(f"SELECT * FROM {src} LIMIT 100")
        if results:
            columns = set()
            for row in results:
                for k in row.keys():
                    if k not in ["id", "_batch"]:
                        columns.add(k)
            return sorted(columns)
    except Exception as e:
        logger.error(f"Failed to get annotator columns: {e}")
    raise HTTPException(status_code=404, detail="Annotator not found")


def _get_count(
    dataset_name: str,
    annotator_cols: dict[str, list[str]],
    filter_data: dict[str, Any],
) -> int:
    """Run the count query (blocking) and return the row count."""
    def attempt(fresh: bool) -> int:
        manifest = _manifest_for(dataset_name, fresh)
        annotator_list = list(annotator_cols.keys()) if annotator_cols else []
        annot_parquet_paths = {
            a: manifest.annotators[a].merged_parquet
            for a in annotator_list
            if a in manifest.annotators
        }
        annot_live_paths = {
            a: manifest.annotators[a].merged_jsonl + manifest.annotators[a].live_jsonl
            for a in annotator_list
            if a in manifest.annotators
        }

        filter_spec = FilterSpec(filter_data)
        if not filter_data and not annotator_cols and manifest.index_files:
            # Index-backed fast path (no filters).
            query, params = build_count_query_fast(
                manifest.index_files, manifest.merged_jsonl + manifest.live_jsonl
            )
        else:
            ordering_file = orderings.fresh_ordering_file(
                dataset_name,
                orderings.rowset_hash(annotator_cols, filter_data),
            )
            if ordering_file is not None:
                # A fresh materialized ordering has the same row set — count
                # from it instead of re-scanning.
                query, params = (
                    f'SELECT COUNT(DISTINCT "id") AS cnt FROM read_parquet(\'{ordering_file}\')',
                    [],
                )
            else:
                query, params = build_count_query(
                    filter_spec,
                    manifest.merged_parquet,
                    annot_parquet_paths,
                    annotator_list,
                    annotator_cols,
                    manifest.merged_jsonl + manifest.live_jsonl,
                    annot_live_paths,
                )

        results = execute_query(query, params)
        return results[0].get("cnt", 0) if results else 0

    return _manifest_attempt(attempt)


def _get_conversion(dataset_name: str) -> "ConversionResponse":
    """Conversion progress for a dataset (blocking).

    Reads ``_migration/status.json`` written by s3-data-tool-convert; when
    absent, derives an approximate view from the file manifest (batches with
    merged.parquet but no blocks are pending). Never fails — returns defaults.
    """
    client = get_s3_client()
    status: dict[str, Any] = {}
    try:
        response = client.get_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_PREFIX.rstrip('/')}/{dataset_name}/_migration/status.json",
        )
        status = json.loads(response["Body"].read())
    except Exception:  # noqa: BLE001
        pass

    if status:
        try:
            return ConversionResponse(**status)
        except Exception:  # noqa: BLE001
            status = {}

    try:
        manifest = build_file_manifest(client, S3_BUCKET, S3_PREFIX, dataset_name)
    except Exception:  # noqa: BLE001
        manifest = None
    total = 0
    converted = 0
    if manifest is not None:
        batches: dict[str, dict[str, bool]] = {}
        for path in manifest.merged_parquet:
            batches.setdefault(path.rstrip("/").split("/")[-2], {"pq": True, "blocks": False})
        for path in manifest.merged_jsonl:
            entry = batches.setdefault(path.rstrip("/").split("/")[-2], {"pq": False, "blocks": False})
            entry["blocks"] = True
        total = len(batches)
        converted = sum(1 for b in batches.values() if b["blocks"] and not b["pq"])
    return ConversionResponse(total_batches=total, converted=converted)


def _get_data(
    dataset_name: str,
    page: int,
    page_size: int,
    column_list: list[str],
    annotator_cols: dict[str, list[str]],
    filter_data: dict[str, Any],
    sort: str | None,
    sort_dir: str,
    row_id: str | None,
    cursor: str | None,
) -> DataResponse:
    """Run the data query (blocking) and return rows."""
    annotator_list = list(annotator_cols.keys()) if annotator_cols else []

    if row_id:
        filter_data["base"] = {"field": "id", "op": "eq", "value": row_id}
        page_size = 1
        page = 1

    filter_spec = FilterSpec(filter_data)
    offset = (page - 1) * page_size

    def attempt(fresh: bool) -> DataResponse:
        manifest = _manifest_for(dataset_name, fresh)
        if not column_list:
            column_list[:] = _default_base_columns(dataset_name, manifest)

        annot_parquet_paths = {
            a: manifest.annotators[a].merged_parquet
            for a in annotator_list
            if a in manifest.annotators
        }
        annot_jsonl_paths = {
            a: manifest.annotators[a].merged_jsonl
            for a in annotator_list
            if a in manifest.annotators
        }
        annot_live_paths = {
            a: manifest.annotators[a].live_jsonl
            for a in annotator_list
            if a in manifest.annotators
        }

        if row_id is None and keyset_eligible(
            filter_spec, sort, manifest.index_files
        ):
            # Index-backed keyset pagination (no filters, id-sorted).
            (
                rows,
                selected_columns,
                selected_annotator_columns,
                next_cursor,
                has_more,
            ) = db.get_pool().run(
                lambda conn: run_keyset_page(
                    conn,
                    column_list,
                    annotator_list,
                    annot_parquet_paths,
                    annot_jsonl_paths,
                    annotator_cols,
                    annot_live_paths,
                    manifest.merged_parquet,
                    manifest.merged_jsonl,
                    manifest.live_jsonl,
                    manifest.index_files,
                    manifest.batch_meta,
                    cursor,
                    sort_dir,
                    page_size,
                )
            )
            return DataResponse(
                rows=rows,
                columns=selected_columns,
                annotator_columns=selected_annotator_columns,
                next_cursor=next_cursor,
                has_more=has_more,
            )

        if row_id is None:
            # Filtered / non-id-sorted / unindexed: page through a
            # materialized ordering when available; fall back to the scan
            # path while another worker builds it or when it fails.
            try:
                ordering_file = orderings.get_or_build_ordering(
                    dataset_name,
                    manifest,
                    filter_spec,
                    sort,
                    sort_dir,
                    annotator_list,
                    annotator_cols,
                )
            except Exception as e:  # noqa: BLE001 - fall back to the scan path
                logger.warning(
                    f"Ordering unavailable for {dataset_name}, scanning: {e}"
                )
                ordering_file = None
            if ordering_file is not None:
                oh = orderings.order_hash(
                    orderings.rowset_hash(annotator_cols, filter_data),
                    sort,
                    sort_dir,
                )
                (
                    rows,
                    selected_columns,
                    selected_annotator_columns,
                    next_cursor,
                    has_more,
                ) = db.get_pool().run(
                    lambda conn: orderings.run_ordering_page(
                        conn,
                        ordering_file,
                        oh,
                        column_list,
                        annotator_list,
                        annot_parquet_paths,
                        annot_jsonl_paths,
                        annotator_cols,
                        annot_live_paths,
                        manifest.merged_parquet,
                        manifest.merged_jsonl,
                        manifest.live_jsonl,
                        manifest.batch_meta,
                        cursor,
                        page,
                        page_size,
                    )
                )
                return DataResponse(
                    rows=rows,
                    columns=selected_columns,
                    annotator_columns=selected_annotator_columns,
                    next_cursor=next_cursor,
                    has_more=has_more,
                )

        annot_all_jsonl = {
            a: annot_jsonl_paths.get(a, []) + annot_live_paths.get(a, [])
            for a in annotator_list
        }
        query, params, selected_columns, selected_annotator_columns = build_query(
            column_list,
            annotator_list,
            filter_spec,
            manifest.merged_parquet,
            annot_parquet_paths,
            annotator_cols,
            sort,
            sort_dir,
            offset,
            page_size,
            row_id,
            manifest.merged_jsonl + manifest.live_jsonl,
            annot_all_jsonl,
        )

        rows = execute_query(query, params)
        if row_id and not rows:
            raise HTTPException(status_code=404, detail=f"Row {row_id} not found")
        return DataResponse(
            rows=rows,
            columns=selected_columns,
            annotator_columns=selected_annotator_columns,
        )

    return _manifest_attempt(attempt)


def _ndjson_line(payload: dict) -> str:
    return json.dumps(payload, default=str) + "\n"


def _stream_data_ndjson(
    request: Request,
    dataset_name: str,
    page: int,
    page_size: int,
    column_list: list[str],
    annotator_cols: dict[str, list[str]],
    filter_data: dict[str, Any],
    sort: str | None,
    sort_dir: str,
    row_id: str | None,
) -> StreamingResponse:
    """NDJSON streaming variant of /data: rows flow out as they materialize.

    Line protocol: {"type":"meta","columns":[...],"annotator_columns":{...}}
    then {"type":"row","row":{...}} * N, ending with {"type":"done"} — or
    {"type":"error","message":...} on failure.
    """
    annotator_list = list(annotator_cols.keys()) if annotator_cols else []
    if row_id:
        filter_data["base"] = {"field": "id", "op": "eq", "value": row_id}
        page_size = 1
        page = 1
    filter_spec = FilterSpec(filter_data)
    offset = (page - 1) * page_size

    def build(fresh: bool):
        manifest = _manifest_for(dataset_name, fresh)
        if not column_list:
            column_list[:] = _default_base_columns(dataset_name, manifest)
        annot_parquet_paths = {
            a: manifest.annotators[a].merged_parquet
            for a in annotator_list
            if a in manifest.annotators
        }
        annot_all_jsonl = {
            a: manifest.annotators[a].merged_jsonl + manifest.annotators[a].live_jsonl
            for a in annotator_list
            if a in manifest.annotators
        }
        return build_query(
            column_list,
            annotator_list,
            filter_spec,
            manifest.merged_parquet,
            annot_parquet_paths,
            annotator_cols,
            sort,
            sort_dir,
            offset,
            page_size,
            row_id,
            manifest.merged_jsonl + manifest.live_jsonl,
            annot_all_jsonl,
        )

    async def gen():
        try:
            query, params, columns, selected_annotator_columns = _manifest_attempt(build)
        except Exception as e:  # noqa: BLE001
            yield _ndjson_line({"type": "error", "message": str(e)})
            return
        yield _ndjson_line(
            {
                "type": "meta",
                "columns": columns,
                "annotator_columns": selected_annotator_columns,
            }
        )

        stop = threading.Event()
        loop = asyncio.get_running_loop()
        # Bounded queue: a slow client (or far-away proxy) stalls the producer
        # thread instead of buffering unbounded batches in memory.
        queue: asyncio.Queue = asyncio.Queue(
            maxsize=int(os.environ.get("VIEWER_NDJSON_QUEUE_SIZE", "2"))
        )
        deadline = time.monotonic() + float(
            os.environ.get("VIEWER_STREAM_MAX_SECONDS", "300")
        )

        def _put(item) -> bool:
            """Blocking put with backpressure; False when the stream stopped."""
            while not stop.is_set():
                fut = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
                try:
                    fut.result(timeout=0.5)
                    return True
                except Exception:
                    continue
            return False

        def producer():
            try:
                for batch in db.get_pool().execute_stream(query, params, 5000):
                    if not _put(batch):
                        break
            except Exception as e:  # noqa: BLE001
                _put(e)
            finally:
                _put(None)

        task = asyncio.create_task(asyncio.to_thread(producer))
        try:
            while True:
                if await request.is_disconnected():
                    stop.set()
                    break
                if time.monotonic() > deadline:
                    stop.set()
                    yield _ndjson_line(
                        {"type": "error", "message": "stream timed out"}
                    )
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    continue
                if item is None:
                    yield _ndjson_line({"type": "done"})
                    break
                if isinstance(item, Exception):
                    yield _ndjson_line({"type": "error", "message": str(item)})
                    break
                for row in item:
                    yield _ndjson_line({"type": "row", "row": row})
        finally:
            await asyncio.wait_for(task, timeout=10.0)

    return StreamingResponse(
        gen(),
        media_type="application/x-ndjson",
        headers={"X-Accel-Buffering": "no"},
    )


@app.get("/events")
async def get_events(request: Request, dataset: str = Query(default="")):
    """Stream viewer events for a dataset (SSE)."""
    return await events_handler(request, dataset)


@app.get("/activity", response_model=ActivityResponse)
async def get_activity(
    bucket: str = Query(default="1m"),
    minutes: int | None = Query(default=1440, ge=1, le=43200),
):
    """Rows created per time bucket for every dataset (from _created_at)."""
    if bucket not in charts.BUCKET_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"bucket must be one of {sorted(charts.BUCKET_INTERVALS)}",
        )

    def compute() -> dict:
        now = datetime.now(timezone.utc)
        window_end = now.isoformat()
        window_start = (now - timedelta(minutes=minutes)).isoformat() if minutes else None
        datasets = cached_sync(_redis_sync, datasets_key(), 60, list_datasets_from_s3)
        interval = charts.BUCKET_INTERVALS[bucket]
        out: list[dict] = []
        for ds in datasets:
            manifest = _manifest_for(ds, False)
            if not _schema_has_created_at(ds, manifest):
                # All rows predate the _created_at field — nothing to chart.
                out.append({"dataset": ds, "buckets": []})
                continue
            src = _union_source(
                manifest.merged_parquet, manifest.merged_jsonl + manifest.live_jsonl
            )
            if not src:
                out.append({"dataset": ds, "buckets": []})
                continue
            query, params = charts.build_activity_query(
                manifest.merged_parquet,
                manifest.merged_jsonl + manifest.live_jsonl,
                interval,
                window_start,
                window_end,
            )
            rows = execute_query(query, params)
            out.append(
                {
                    "dataset": ds,
                    "buckets": [{"ts": str(r["ts"]), "count": r["cnt"]} for r in rows],
                }
            )
        return {
            "datasets": out,
            "window": {"start": window_start, "end": window_end},
            "bucket": bucket,
            "generated_at": now.isoformat(),
        }

    try:
        return await anyio.to_thread.run_sync(
            cached_sync,
            _redis_sync,
            activity_key(bucket, minutes),
            60,
            compute,
            lambda payload: json.dumps(payload, default=str),
            json.loads,
            activity_keys_set(),
        )
    except db.PoolTimeout:
        raise HTTPException(status_code=503, detail="Server busy; retry shortly")
    except Exception as e:
        logger.error(f"Activity query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/datasets/{dataset_name}/categorical", response_model=CategoricalResponse
)
async def get_categorical(
    dataset_name: str,
    column: str = Query(...),
    mode: str = Query(default="counts", pattern="^(counts|trend)$"),
    bucket: str = Query(default="1h"),
    limit: int = Query(default=20, ge=1, le=100),
    minutes: int | None = Query(default=None, ge=1),
):
    """Categorical value counts / per-bucket trends for a column (from _created_at)."""
    if mode == "trend" and bucket not in charts.BUCKET_INTERVALS:
        raise HTTPException(
            status_code=422,
            detail=f"bucket must be one of {sorted(charts.BUCKET_INTERVALS)}",
        )
    try:
        charts._value_expr("base", column)  # identifier validation
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    def attempt(fresh: bool) -> dict:
        manifest = _manifest_for(dataset_name, fresh)
        merged_jsonl = manifest.merged_jsonl + manifest.live_jsonl
        now = datetime.now(timezone.utc)
        window_end = now.isoformat()
        window_start = (
            (now - timedelta(minutes=minutes)).isoformat() if minutes else None
        )
        window = {"start": window_start, "end": window_end}
        generated_at = now.isoformat()

        if not _schema_has_created_at(dataset_name, manifest):
            # All rows predate the _created_at field — nothing to chart.
            return {
                "mode": mode, "column": column, "values": [], "total": 0,
                "distinct": 0, "truncated": False, "top_values": [],
                "series": [], "window": window, "generated_at": generated_at,
            }
        if column not in _schema_column_names(dataset_name, manifest):
            raise HTTPException(status_code=400, detail=f"Unknown column: {column}")

        if mode == "counts":
            query, params = charts.build_categorical_counts_query(
                manifest.merged_parquet,
                merged_jsonl,
                column,
                limit,
                window_start,
                window_end,
            )
            if not query:
                return {
                    "mode": mode, "column": column, "values": [], "total": 0,
                    "distinct": 0, "truncated": False, "window": window,
                    "generated_at": generated_at,
                }
            rows = execute_query(query, params)
            values = [{"value": r["v"], "count": r["cnt"]} for r in rows]
            total = rows[0]["total"] if rows else 0
            distinct = rows[0]["distinct_count"] if rows else 0
            return {
                "mode": mode, "column": column, "values": values, "total": total,
                "distinct": distinct, "truncated": len(values) < distinct,
                "window": window, "generated_at": generated_at,
            }

        # Trend mode: series of (bucket, category) counts; top values are
        # derived from the series (topk ∪ {"other"} are its only categories).
        query, params = charts.build_categorical_trend_query(
            manifest.merged_parquet,
            merged_jsonl,
            column,
            charts.BUCKET_INTERVALS[bucket],
            limit,
            window_start,
            window_end,
        )
        if not query:
            return {
                "mode": mode, "column": column, "top_values": [], "series": [],
                "window": window, "generated_at": generated_at,
            }
        rows = execute_query(query, params)
        series = [{"ts": str(r["ts"]), "value": r["cat"], "count": r["cnt"]} for r in rows]
        totals: dict[str, int] = {}
        for r in rows:
            totals[r["cat"]] = totals.get(r["cat"], 0) + r["cnt"]
        top_values = sorted(
            (cat for cat in totals if cat != "other"),
            key=lambda cat: (-totals[cat], cat),
        )[:limit]
        return {
            "mode": mode, "column": column, "top_values": top_values,
            "series": series, "window": window, "generated_at": generated_at,
        }

    try:
        return await anyio.to_thread.run_sync(
            cached_sync,
            _redis_sync,
            categorical_key(dataset_name, column, mode, bucket, limit, minutes),
            300,
            lambda: _manifest_attempt(attempt),
            lambda payload: json.dumps(payload, default=str),
            json.loads,
            categorical_keys_set(dataset_name),
        )
    except HTTPException:
        raise
    except db.PoolTimeout:
        raise HTTPException(status_code=503, detail="Server busy; retry shortly")
    except Exception as e:
        logger.error(f"Categorical query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/datasets", response_model=DatasetListResponse)
async def get_datasets():
    """List all available datasets."""
    datasets = await anyio.to_thread.run_sync(
        cached_sync, _redis_sync, datasets_key(), 60, list_datasets_from_s3
    )
    return DatasetListResponse(datasets=datasets)


@app.get("/datasets/{dataset_name}/annotations", response_model=AnnotationListResponse)
async def get_annotations(dataset_name: str):
    """List all available annotators for a dataset."""
    annotators = await anyio.to_thread.run_sync(
        cached_sync,
        _redis_sync,
        annotators_key(dataset_name),
        60,
        lambda: list_annotators_from_s3(dataset_name),
    )
    return AnnotationListResponse(annotators=annotators)


@app.get("/datasets/{dataset_name}/annotations/{annotator}/columns")
async def get_annotator_columns(dataset_name: str, annotator: str):
    """Get available column names for a specific annotator (union across all batches)."""
    def attempt(fresh: bool) -> list[str]:
        manifest = _manifest_for(dataset_name, fresh)
        return _get_annotator_columns(dataset_name, annotator, manifest)

    columns = await anyio.to_thread.run_sync(_manifest_attempt, attempt)
    return {"columns": columns}


@app.get("/datasets/{dataset_name}/schema", response_model=SchemaResponse)
async def get_schema(
    dataset_name: str,
    annotators: str = Query(default="", description="Comma-separated annotator names"),
):
    """Get schema (columns and types) for a dataset."""
    annotator_list = [a.strip() for a in annotators.split(",") if a.strip()]

    def attempt(fresh: bool) -> list[SchemaColumn]:
        manifest = _manifest_for(dataset_name, fresh)
        return _get_schema_columns(dataset_name, annotator_list, manifest)

    columns = await anyio.to_thread.run_sync(
        cached_sync,
        _redis_sync,
        schema_key(dataset_name, annotator_list),
        300,
        lambda: _manifest_attempt(attempt),
        lambda cols: json.dumps([c.model_dump() for c in cols]),
        lambda raw: [SchemaColumn(**x) for x in json.loads(raw)],
        schema_keys_set(dataset_name),
    )
    return SchemaResponse(columns=columns)


@app.get("/datasets/{dataset_name}/count")
async def get_count(
    dataset_name: str,
    annotator_columns: str = Query(
        default="{}", description="JSON dict mapping annotator to column names"
    ),
    filters: str = Query(default="{}", description="JSON-encoded filter spec"),
):
    """Get approximate row count."""
    try:
        annotator_cols = json.loads(annotator_columns) if annotator_columns else {}
    except json.JSONDecodeError:
        annotator_cols = {}
    try:
        filter_data = json.loads(filters) if filters else {}
    except json.JSONDecodeError:
        filter_data = {}

    try:
        count = await anyio.to_thread.run_sync(
            cached_sync,
            _redis_sync,
            count_key(dataset_name, annotator_cols, filter_data),
            30,
            lambda: _get_count(dataset_name, annotator_cols, filter_data),
            str,
            int,
            count_keys_set(dataset_name),
        )
        return {"count": count}
    except db.PoolTimeout:
        raise HTTPException(status_code=503, detail="Server busy; retry shortly")
    except Exception as e:
        logger.error(f"Count query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/datasets/{dataset_name}/conversion", response_model=ConversionResponse)
async def get_conversion(dataset_name: str):
    """Conversion progress for a dataset (batches converted from parquet to JSONL)."""
    try:
        return await anyio.to_thread.run_sync(
            cached_sync,
            _redis_sync,
            conversion_key(dataset_name),
            5,
            lambda: _get_conversion(dataset_name),
            lambda r: r.model_dump_json(),
            lambda raw: ConversionResponse(**json.loads(raw)),
        )
    except db.PoolTimeout:
        raise HTTPException(status_code=503, detail="Server busy; retry shortly")


@app.get("/datasets/{dataset_name}/data", response_model=DataResponse)
async def get_data(
    request: Request,
    dataset_name: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=1000),
    columns: str = Query(default="", description="Comma-separated column names"),
    annotator_columns: str = Query(
        default="{}", description="JSON dict mapping annotator to column names"
    ),
    filters: str = Query(default="{}", description="JSON-encoded filter spec"),
    sort: str | None = Query(default=None),
    # Newest-first is the default: rows arrive over time with increasing ids,
    # so an id-descending default shows the most recent batch first.
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    row_id: str | None = Query(default=None, description="Fetch single row by ID"),
    cursor: str | None = Query(default=None, description="Keyset pagination cursor"),
    format: str = Query(default="json", pattern="^(json|ndjson)$"),
):
    """Get paginated data rows (JSON by default; format=ndjson streams rows)."""
    column_list = [c.strip() for c in columns.split(",") if c.strip()]
    try:
        annotator_cols = json.loads(annotator_columns) if annotator_columns else {}
    except json.JSONDecodeError:
        annotator_cols = {}

    try:
        filter_data = json.loads(filters) if filters else {}
    except json.JSONDecodeError:
        filter_data = {}

    if format == "ndjson":
        return _stream_data_ndjson(
            request,
            dataset_name,
            page,
            page_size,
            column_list,
            annotator_cols,
            filter_data,
            sort,
            sort_dir,
            row_id,
        )

    try:
        return await anyio.to_thread.run_sync(
            _get_data,
            dataset_name,
            page,
            page_size,
            column_list,
            annotator_cols,
            filter_data,
            sort,
            sort_dir,
            row_id,
            cursor,
        )
    except HTTPException:
        raise
    except db.PoolTimeout:
        raise HTTPException(status_code=503, detail="Server busy; retry shortly")
    except Exception as e:
        logger.error(f"Data query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
