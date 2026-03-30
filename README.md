# Distributed S3 Dataset Tool

Library for creating and annotating text-heavy datasets. This library uses S3 and parquet as storage backends, and supports a distributed- dataset created on one server might be annotated on another.

## Design Choices

The parquet tables shall be kept "lean" in the number of columns- each annotator would create an annotation parquet table of its own, with references to IDs in the original text parquet. When a new annotator is added, a new parquet file is created, without duplicating any existing fields. No original text would be duplicated. This approach sacrifices random read and join efficiency for the ability to add new annotator columns with minimal overhead.

The dataset should be "streamed" from S3 (e.g., using DuckDB) and should not loaded fully into memory (pandas, etc. should not be used.)

## Coordination Primitives

Atomic WSS-based Mutex (max: 60 seconds)

```python
async with WSSMutex("example-url-safe-lock-name"):
    ...
```

S3-based long-running lock (hours), built on top of the atomic WSS-based Mutex. No renew is required, and lock expiry is based entirely on timestamp. To account for drifts, clients compare own time with S3 last-modified time to derive offset. S3 lock files are JSON files that contain a timestamp as well as the hostname of the machine.

```python
async def _renew_lock_task(lock: S3Lock):
    """Renew lock; returns if renewal is not successful."""
    await asyncio.sleep((lock.ttl_ms / 1000) - 60)
    try:
        await lock.renew():
    except:
        return
    

async def merge(path: str):  # path might not be URL-safe
    async with S3Lock(path, ttl_ms=3_600_000) as lock:
        # Lock is currently granted to another worker
        # skip and try again next time (cron)
        if not lock:
            return

        renewal_task = ...
        # S3 Lock is free or expired- perform actual merge logic
        # as soon as the renewal task exits (renewal fails), 
        # interrupt the task doing work. 
        ...
```

These primitives are used solely within the library to provide the functionalities described below.

## Example Usage

Copy `example.env` to `.env` and fill in:

- S3 credentials
- WSS Mutex API Base URL

Note that the following are examples only- this library is agnostic to the exact data format, as long as it is JSON serializable/de-serializable using the standard Python JSON library.

### Example: Creating dataset from iterator

```python
streaming_configs = S3DataTool.StreamingConfigs(
    chunk_size=100,  # Update S3 jsonl buffer every 100 rows
    should_merge=True,  # Merge into a parquet file after completing.
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

Data rows are "streamed" to S3 in JSONLines format every chunk_size lines. If "should_merge" is set to True, the client would create merge the jsonl file into a parquet table, and delete the jsonl. Otherwise, the client would leave a jsonl file as output. The jsonl file and output parquet files are named with a random 6-character hex string, so that multiple instances of the same writer name and batch would not collide.

Multiple data generation workers might run at the same time for the same dataset name and batch. Duplicates would be eliminated during automated clean-up and merge (see below.)

### Annotating dataset using async filter-map

Filter-map allows layered annotation- each subsequent annotation run would be on a narrower subset, filtered based on existing annotations.

```python
from s3_data_tool import Annotation, Filter, S3DataTool

streaming_configs = S3DataTool.StreamingConfigs(...)  # same as in data generation

async def annotate(item: DataItem) -> Annotation:
    # Example async data annotation function
    # "text" and "query_filters" are columns of the dataset
    result = await custom_search(
        item.data.get("text"), filters=item.data.get("query_filters")
    )

    # id, batch, etc. are automatically assigned and transparent to annotator.
    return Annotation(
        {
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
        # Optionally, specify filters; these are compiled into DuckDB query.
        # supports Filter.all for AND and Filter.any for OR. Nesting is supported.
        # Only boolean comparison is supported. All other comparisons shall be defined
        # using raw DuckDB filters. See edge cases regarding handling missing values.
        filter=Filter.all([
            Filter.boolean(annotator="validity_filter", field="is_valid", value=True),
            Filter.raw_duck("..."),
        ])
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
uv run --env-file .env s3_data_tool.cleanup
```

The above would merge the jsonl and parquet files from exited and timed-out data generation/annotation workers. For data generation, the result would be one parquet for each name-batch pair. Edge case: if within the same name-batch pair, some jsonl/parquet contains a column while others don't, the columns are merged.

- Merge a mix of multiple jsonl files and parquet files into one parquet for each name-batch pair.
- De-duplicate using sha digest and a built-in set() within each name-batch pair.
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
        filter=Filter.all(...)
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

### Filtering Scope

Filtering happens only within each dataset across batchs, but not across dataset (different names).

### Partial/Corrupted jsonl files

Rows in JSONLine files that cannot be processed are ignored. The rest of the file shall be processed as normal.

### S3 Lock Timeout during work

For data generation, concurrent work is acceptable, and de-duplication happens at merge time. S3 Lock is not required.

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
        - 123abc.manifest
        - 123abc_chunk_00000.jsonl
        - ...
        # Merged files
        - merged.parquet
        - manifest.yaml
        # Annotations for this batch
    - annotations/
        - annotator_name/
            - batch_name/ # one folder per dataset batch
                - manifest.yaml
                # Temporary files
                - .lock
                - chunk_00000.jsonl
                - ...
                # Merged files
                - merged.parquet
```
