# Progress

- Added core dependencies to pyproject.toml: aioboto3, duckdb, pyarrow, pyyaml, python-dotenv, pydantic
- Implement S3-based long-running lock (src/s3_lock.py)
  - S3Lock class with acquire/renew/release methods
  - Uses WSSMutex for atomic coordination during S3 operations
  - Non-blocking acquire with warning log on failure
  - LockRenewalError exception for failed renewals
  - Lock file contains timestamp, hostname, and lock_id for ownership verification
  - Module docstring with example usage including asyncio cancellation logic
  - Merge "Coordination Primitives" into "Design Choices" section in README.md
- Implement dataset_generator.from_async_iterator capability
  - src/models.py: StreamingConfigs Pydantic model with chunk_size
  - src/s3_utils.py: S3 utility functions (generate_hex_id, upload_jsonl_chunk, list_jsonl_chunks, merge_jsonl_to_parquet, delete_objects)
  - src/dataset_generator.py: DatasetGenerator class with from_async_iterator method
  - src/s3_data_tool.py: S3DataTool entry point with async context manager for dataset_generator
  - Random 6-char hex ID for file naming to avoid collisions
  - JSONL chunked streaming with configurable chunk_size
  - JSONL files left for cleanup job to merge; merging deferred to automated cleanup

- Remove JSONL-to-parquet merge from dataset generation
  - Removed should_merge parameter from StreamingConfigs model
  - Removed merge logic from dataset_generator.from_async_iterator
  - Removed unused imports (merge_jsonl_to_parquet, delete_objects, list_jsonl_chunks)
  - Updated README.md examples and documentation to clarify merge only happens in cleanup job
  - Eliminates duplication between data generation and cleanup job

- Implement run manifest for tracking deduplication and streaming configs
  - src/models.py: RunManifest and BatchManifest Pydantic models
  - src/s3_utils.py: upload_run_manifest function
  - src/dataset_generator.py: Create and upload manifest at start/end of run
  - Manifest tracks run_id, deduplicate_on, streaming_configs, completion status, timestamps

- Add missing S3 environment variables to example.env
  - Added S3_BUCKET (required), S3_PREFIX (optional), S3_ENDPOINT_URL (optional)

- Add test suite for WSSMutex (tests/test_mutex.py)
  - Added pytest and pytest-asyncio as test dependencies in pyproject.toml
  - Test cases: connect/handshake, acquire/release, context manager, concurrent contention, TTL expiration warning, custom base_url, env fallback

- Fix bugs discovered by WSSMutex tests
  - Fixed acquire() missing return after "granted" (infinite loop bug)
  - Fixed release() not setting ws=None after close
  - Added pytest-timeout with 10s limit to prevent hanging tests

- Implement Filter DSL for dataset and annotation filtering
  - s3_data_tool/filter.py: Pydantic models for filter types (BooleanFilter, AllFilter, AnyFilter, RawDuckFilter)
  - Each filter has compile(available_columns: set[str]) -> str method
  - Missing columns compile to "FALSE"
  - Empty AllFilter compiles to "TRUE", empty AnyFilter compiles to "FALSE"
  - FilterNode union type for type hints
  - 25 unit tests pass (TestBooleanFilterCompilation, TestAllFilterCompilation, TestAnyFilterCompilation, TestNestedFilters, TestRawDuckFilter, TestFilterSerialization)

- Implement async utilities for semaphore-limited concurrency
  - s3_data_tool/async_utils.py: with_semaphore helper function
  - TypeVar for generic return type
  - Used by schema discovery functions

- Implement parallel schema discovery for dataset columns
  - s3_data_tool/s3_utils.py: Added functions:
    - s3_object_exists: Check if S3 object exists
    - read_parquet_columns: Read column names from parquet without loading data
    - read_parquet_columns_if_exists: Safe wrapper
    - discover_batch_columns: Discover columns for a single batch (dataset + annotations)
    - discover_dataset_columns: Main entry point with parallel processing
  - Semaphore-limited concurrency at file level only (not batch level)
  - Lambda with default argument to capture value: lambda k=key: ...
  - Configurable via FILTER_MAX_CONCURRENCY env var (default 20)
  - 3 schema discovery tests pass

- Add Annotation and DataItem models
  - s3_data_tool/models.py: Annotation and DataItem Pydantic models
  - Annotation: data dict + optional metadata
  - DataItem: data dict + id + batch

- Add filter view scaffolding (Phase 2)
  - s3_data_tool/data_filtering.py: AnnotationView and ExportView classes
  - AnnotationView: Lock-based annotation flow
  - ExportView: Read-only async iterator
  - Methods not yet implemented (scaffolding only)

- Update S3DataTool with filter methods
  - Added filter_for_annotation() context manager
  - Added filter_for_export() context manager

- Update exports in __init__.py
  - Export all filter types: BooleanFilter, AllFilter, AnyFilter, RawDuckFilter, FilterNode
  - Export Annotation and DataItem models

- Add pytest configuration for .env loading
  - tests/conftest.py: Load .env file with python-dotenv
  - Fixes integration tests that require S3 credentials

- Implement data_filtering.py methods
  - s3_data_tool/data_filtering.py: AnnotationView and ExportView implementations
  - AnnotationLockError and DatasetNotMergedError exceptions
  - AnnotationView.__aenter__: Acquire S3Lock for annotator with configurable TTL
  - AnnotationView.__aexit__: Release lock, return False
  - AnnotationView.annotate(): Stream rows, filter, annotate with concurrency limit
    - Discover columns using discover_dataset_columns()
    - Compile filter to SQL WHERE clause
    - Load dataset parquet, skip already annotated rows
    - Apply user filter using DuckDB
    - Call annotation_fn with semaphore-limited concurrency
    - Write results to JSONL chunks
    - Spawn lock renewal task (renew 60s before expiry)
  - ExportView.__aiter__: Async iterator over filtered rows
    - Join dataset with all annotations on id/batch
    - Apply filter and yield rows as dict
  - Added pandas dependency to pyproject.toml for DuckDB DataFrame queries
  - Fixed s3_object_exists to catch ClientError for 404 responses
  - 11 integration tests pass (test_data_filtering.py)
