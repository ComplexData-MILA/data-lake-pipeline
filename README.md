# Distributed S3 Dataset Tool

Library for creating and annotating text-heavy datasets. This library uses S3 as its storage backend, with merged datasets stored as id-sorted gzipped NDJSON blocks (parquet retained for legacy data and oversized datasets), and supports a distributed computing setup- dataset created on one server might be annotated on another.

## Design Choices

The parquet tables shall be kept "lean" in the number of columns- each annotator would create an annotation parquet table of its own, with references to IDs in the original text parquet. When a new annotator is added, a new parquet file is created, without duplicating any existing fields. No original text would be duplicated. This approach sacrifices random read and join efficiency for the ability to add new annotator columns with minimal overhead.

During merge and clean-up, all data rows in a batch are kept in memory at the same time- a limitation of the parquet format Rows in the main dataset parquet are "streamed" from S3 using DuckDB and not loaded fully into memory (pandas, etc. should not be used.)

Coordination primitives are used solely within the library: `WSSMutex` (s3_data_tool/mutex.py) for short-lived atomic locks (max 60 seconds), and `S3Lock` (s3_data_tool/s3_lock.py) for long-running locks (hours) with TTL-based expiry.

## Example Usage

Copy `example.env` to `.env` and fill in:

- S3 credentials
- WSS Mutex API Base URL

Note that the following are examples only- this library is agnostic to the exact data format, as long as it is JSON serializable/de-serializable using the standard Python JSON library.

### Example: Creating dataset from iterator

```python
streaming_configs = S3DataTool.StreamingConfigs(
    chunk_size=100,  # Update S3 jsonl buffer every 100 rows
)

async def data_generator() -> AsyncIterator[dict[str, _JSONSerializable]]:
    async for data in dataset:
        yield {
            "text": data["content"],
            "source_id": data["id"],
            "timestamp": _parse_datetime(data["timestamp"]),
            "metadata": data["metadata"],
        }

async def main():
    # Load secrets from env.
    async with S3DataTool().dataset_generator as dataset_generator:
        # Add rows to the dataset named "example_dataset"
        await dataset_generator.from_async_iterator(
            data_generator(),
            name="example_dataset",
            batch=batch_string, # e.g., YYYYMMDD-HH
            streaming_configs=streaming_configs,
            deduplicate_on=["text", "source_id"]  # list of columns
        )
```

Data rows are "streamed" to S3 in JSONLines format every chunk_size lines. The JSONL files are left for the automated clean-up job to merge into parquet tables. Files are named with a random 6-character hex string, so that multiple instances of the same writer name and batch would not collide.

Multiple data generation workers might run at the same time for the same dataset name and batch. Duplicates would be eliminated during automated clean-up and merge (see below.)

### Annotating dataset using async filter-map

Filter-map allows layered annotation- each subsequent annotation run would be on a narrower subset, filtered based on existing annotations.

```python
from s3_data_tool import Annotation, S3DataTool

streaming_configs = S3DataTool.StreamingConfigs(...)  # same as in data generation

async def annotate(item: DataItem) -> Annotation:
    # Example async data annotation function
    # "text" and "query_filters" are columns of the dataset
    result = await custom_search(
        item.data.get("text"), filters=item.data.get("query_filters")
    )

    # id, batch, etc. are automatically assigned and transparent to annotator.
    return Annotation(
        data={
            "found": result.found, # bool
            "summary": result.summary, # str
        },
        metadata=result.metadata,
    )

async def main():
    # If another annotation worker with the same annotator_name is active and not expired,
    # the following should raise an Error.
    async with S3DataTool().filter_for_annotation(
        name="example_dataset",
        annotator_name="custom_search",  # Name of the current annotator
        base_columns=["text"],
        # Optionally, specify filters; these are compiled into DuckDB query.
        # supports AllFilter for AND and AnyFilter for OR. Nesting is supported.
        # Only boolean comparison is supported. All other comparisons shall be defined
        # using raw DuckDB filters. See edge cases regarding handling missing values.
    ) as annotator_view:
        await annotator_view.annotate(
            annotate,
            max_concurrency=16, # concurrency limits for the "annotate" function
            batch=timestamp_str,
            streaming_configs=streaming_configs,
        )
```

Similar to data generation, data annotation also uses S3 jsonl streaming to keep partial work. However, unlike data generation, the filter-map client would skip over rows for which annotation (same annotator name) already exists.

For efficient "join", each row of the annotation table references the exact source dataset and batch.

If the annotation function raises an Error, that particular data row should be skipped, so that the same row can be retried in subsequent runs.

### Automated Clean-up and Merge

Create a cronjob for the clean-up script:

```bash
uv run --env-file .env s3-data-tool-clean-up
```

The clean-up job merges all JSONL files into parquet tables. This is the only place where merging occurs - data generation workers leave JSONL files as output, and the clean-up job consolidates them.

For data generation, the result would be one parquet for each name-batch pair. Edge case: if within the same name-batch pair, some jsonl/parquet contains a column while others don't, the columns are merged.

- Merge a mix of multiple jsonl files and parquet files into one parquet for each name-batch pair.
- deduplicate using sha digest and a built-in set() within each name-batch pair.
- Delete partial files.
- If new partial files for the same name-batch pair in a subsequent clean-up run, these shall be merged with the existing rows, and the previous parquet file would be replaced or overwritten.

For annotation, the result would be one parquet for each name, batch, and annotator-name pair.

- Convert jsonl to parquet.
- Similarly, if a jsonl file exists alongside a merged parquet file, merge the two into a new parquet file.

### Filtering and merging for export

```python
async def main():
    async with S3DataTool().filter_for_export(
        # Same as specified in filter_for_annotation
        filter=AllFilter(filters=[...])
    ) as read_only_view:
        # read_only_view is an iterator of dict[str, _JSONSerializable]
        # annotator columns are named as {annotator_name}.{field_name}
        for _row in read_only_view:
            assert "text" in _row.keys()
            assert "custom_search.summary" in _row.keys()
```

## Edge Cases

### Schema Shifts

For a particular dataset name, if different batchs contain different columns, resultant views (e.g., for annotation) should present the columns in a best-effort manner. There is no need to determine or store the list of columns ahead of time. Rather, annotators (not this library) are responsible for handling missing columns. For DuckDB filtering purposes, missing columns should be considered as None/null and neither True nor False.

If different jsonl files for the same batch contain different "deduplicate_on" values, the deduplication during merge should use the intersection of all values. If a column is missing in some rows, that column should be "None". Two rows are considered "duplicates" if and only if values ar ethe same across all "deduplicate_on" columns.

### Filtering Scope

Filtering happens only within each dataset across batchs, but not across dataset (different names).

### Partial/Corrupted jsonl files

Rows in JSONLine files that cannot be processed are ignored. The rest of the file shall be processed as normal.

### S3 Lock Timeout during work

For data generation, concurrent work is acceptable, and deduplication happens at merge time. S3 Lock is not required.

For annotation, "annotator_view.annotate" should spawn a lock-renewal async task alongside the annotation tasks. 60 seconds before lock expires, the lock renewal task should try to renew the lock. If that is not successful, stop the annotation worker tasks and exit.

### Annotating an empty or undefined dataset

Trying to merge empty jsonl temporary files (including ones where no row could be parsed) would produce an empty parquet file.

If the dataset parquet file exists but is empty (zero rows), annotation would produce a parquet file containing zero rows.

If the dataset parquet file does not exist (e.g., jsonl chunks are produced but not yet merged in cron job,) filtering (annotation or export) would raise an error.

## S3 Storage Layout

```yaml
- dataset_name/
    - batch_name/
        # Temporary files, where 123abc denotes a random 6-character hex string
        - 123abc.manifest.json
        - 123abc_chunk_00000.jsonl
        - ...
        # Merged files: id-sorted gzipped NDJSON blocks (50k rows each) for
        # datasets under JSONL_MERGE_MAX_DATASET_BYTES, else merged.parquet
        - merged_00000.jsonl.gz
        - merged_00001.jsonl.gz
        - merged.parquet          # legacy / oversized datasets only
        - manifest.yaml
        # Annotations for this batch
    - _index/
        - batch_name.parquet      # sorted (id, _batch) index partition
        - batch_name.meta.json    # row counts, id ranges, block list
    - _migration/
        - status.json             # parquet->JSONL conversion progress
    - annotations/
        - annotator_name/
            - .temp/
                # Temporary files
                - .lock
                - chunk_00000.jsonl
            - batch_name/ # one folder per dataset batch
                - manifest.yaml
                - ...
                # Merged files (same block/parquet convention as base)
                - merged_00000.jsonl.gz
                - merged.parquet
```

## Viewer (web interface)

`viewer/` is a React + FastAPI web UI for browsing datasets. It reads merged
data (id-sorted gzipped JSONL blocks, with parquet for legacy/oversized
batches) and unmerged JSONL chunks **directly from S3** (live rows appear the
instant chunks are uploaded — no clean-up needed), uses Redis for metadata
caching and real-time events, and maintains a per-batch index plus a
materialized-ordering cache so every view paginates in bounded memory on
10M+ row datasets.

### Architecture

```
browser ──HTTPS──> [institutional reverse proxy] ──HTTP──> nginx (:8080)
                                                             ├── /api/ ──> FastAPI backend (:8000)
                                                             └── /api/events (SSE, unbuffered)
FastAPI backend ──> S3 (parquet + live JSONL, via DuckDB httpfs + boto3)
                └──> Redis (metadata cache + viewer:events pub/sub)

writers (s3_data_tool): dataset_generator / clean_up
  └── VIEWER_REDIS_URL set? ──> publish to Redis viewer:events (best-effort)
S3 watcher (in the backend): polls listings of datasets with SSE subscribers
  and emits events when objects change — the PRIMARY producer when writers
  cannot reach Redis (e.g., firewalled deployments)
```

- **Query engine**: persistent DuckDB pool (viewer/backend/db.py) with a
  disk httpfs cache (`DUCKDB_CACHE_DIR`), a per-connection memory limit
  (`DUCKDB_MEMORY_LIMIT`, default 2GB — big sorts/aggregations spill to disk),
  and a bounded pool-acquire timeout (503 instead of hanging under load);
  all filter values are bound params, all identifiers validated/quoted
  (no SQL injection).
- **Live reads**: every query unions the merged files (parquet or JSONL
  blocks) with unmerged `*_chunk_*.jsonl` (and annotation `.temp` chunks);
  the GROUP BY dedup absorbs chunk/merged overlap.
- **Merged JSONL blocks** (`merged_*.jsonl.gz`): clean-up merges each batch
  into id-sorted gzipped NDJSON blocks of ~50k rows for datasets under
  `JSONL_MERGE_MAX_DATASET_BYTES` (default 10GB); larger datasets keep
  parquet. Block id-ranges in `_index/{batch}.meta.json` let a keyset page
  fetch only the blocks its window intersects.
- **Dataset index** (`_index/{batch}.parquet` + `.meta.json`): maintained by
  `s3-data-tool-clean-up` on every batch merge. Enables keyset pagination
  (`/data?cursor=…`), the index-backed count, and per-batch file pruning.
- **Ordering cache** (filtered/sorted views): the first request for a
  (filters, sort) combination materializes the ordered id list once
  (`DUCKDB_CACHE_DIR/orderings/…`, 30s TTL) — pages and counts then walk it
  with `[order_hash, position]` cursors instead of re-scanning the dataset.
- **Conversion progress**: `GET /datasets/{d}/conversion` reports parquet→
  JSONL migration progress from `_migration/status.json`; the UI shows a
  banner while a conversion job is running.
- **Streaming**: `/data?format=ndjson` streams rows as NDJSON
  (`meta`/`row`/`done`/`error` lines) for API consumers (bounded queue with
  backpressure); the UI itself pages with keyset cursors. `GET /events`
  streams ingestion events as SSE (`connected` first, then
  `rows_ingested` / `run_completed` / `batch_merged` / `annotation_updated`
  / `conversion_progress`). A subscription with an empty `dataset` filter
  watches **all** datasets (the watcher expands it via the dataset list).
- **Dashboard charts** (from the `_created_at` column the pipeline now
  injects on every row; rows ingested before the field existed are ignored):
  - `GET /activity?bucket=1m&minutes=1440` — rows created per time bucket
    for every dataset (id/batch-deduped, `_created_at`-normalized across the
    parquet/JSONL encodings).
  - `GET /datasets/{d}/categorical?column=X&mode=counts|trend&limit=20` —
    top-K value counts, or per-bucket counts of the top-K categories
    (`trend`, `bucket`-bucketed; non-top values fold into `other`).
  - Both are Redis-cached and invalidated by data events; both return empty
    results (not errors) for datasets with no `_created_at` rows.
- **UI**: the Data tab shows all base columns by default (omitting
  `columns` selects the full schema server-side) and infinite-scrolls with
  virtualized rows via keyset cursors; the Activity and Charts tabs are
  plotly.js views, lazily loaded.

### Redis keys

All scoped by `S3_BUCKET:S3_PREFIX`:

| Key | TTL | Content |
|---|---|---|
| `viewer:{scope}:datasets` | 60s | dataset list |
| `viewer:{scope}:{d}:annotators` | 60s | annotator list |
| `viewer:{scope}:{d}:schema:{hash}` | 300s | column list (per annotator-set hash) |
| `viewer:{scope}:{d}:files` | 5s | FileManifest (S3 listing) |
| `viewer:{scope}:{d}:count:{hash}` | 30s | row count (per filter hash) |
| `viewer:{scope}:activity:{hash}` | 60s | per-dataset activity buckets |
| `viewer:{scope}:{d}:categorical:{hash}` | 300s | categorical counts/trend results |
| `viewer:{scope}:{d}:index_meta` | — | per-batch index metadata (Phase 5) |
| `viewer:watcher:leader` | 10s | watcher leader election |

Schema/count keys are also registered in `{d}:schema_keys` / `{d}:count_keys`
SETs so event-driven invalidation can clear all variants.

### Reverse proxy assumptions

TLS terminates upstream (institutional proxy). nginx listens on :80 and sets
`X-Forwarded-For` / `X-Forwarded-Proto`; uvicorn runs with `--proxy-headers`.
The upstream proxy must NOT buffer `/api/events` (SSE); nginx already sets
`proxy_buffering off` for it. If the viewer is exposed behind another hop,
repeat that setting there.

### Ops notes

- Deploy: `cd viewer && docker compose up -d --build` (backend + frontend +
  redis). Env file: `viewer/backend/.prod.env` (see `.prod.env.example`).
- Clean-up cron is unchanged (`uv run --env-file .env s3-data-tool-clean-up`);
  each merge now also refreshes the dataset index partition and writes merged
  data as JSONL blocks (unless the dataset is over the size threshold).
- Conversion cron: `uv run --env-file .env s3-data-tool-convert` migrates
  legacy `merged.parquet` to JSONL blocks for datasets under
  `CONVERT_MAX_DATASET_BYTES` (default 10GB), updating
  `_migration/status.json` per batch; run it alongside clean-up until the
  backlog is converted. Idempotent and resumable.
- Set `VIEWER_REDIS_URL` in writer environments to push events directly;
  otherwise the backend's S3 watcher (5s poll) covers real-time updates.
- Latency tracking: `python scripts/bench_viewer.py --base-url http://localhost:8080/api`
  (p50/p95 per endpoint); backend request timings with `VIEWER_LOG_TIMINGS=1`.

### Troubleshooting

- **SSE stuck at "connected"**: a proxy between the browser and nginx is
  buffering `/api/events` — disable buffering for that path.
- **Stale counts after ingestion**: normal — counts are cached up to 30s and
  invalidated by events (or the watcher); the UI shows a live-data hint.
- **One DuckDB cache dir per backend instance**: `DUCKDB_CACHE_DIR` must be
  unique per process (DuckDB file lock); the compose volume handles this.
- **Tests that need S3**: run them against a disposable bucket — the
  fixtures delete everything in the configured bucket/prefix, so never point
  them at a real one (e.g. the jump host's `dry-run-20260401` bucket on
  arbutus-s3).
