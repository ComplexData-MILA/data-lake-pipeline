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
  - JSONL files left for clean-up job to merge; merging deferred to automated clean-up

- Remove JSONL-to-parquet merge from dataset generation
  - Removed should_merge parameter from StreamingConfigs model
  - Removed merge logic from dataset_generator.from_async_iterator
  - Removed unused imports (merge_jsonl_to_parquet, delete_objects, list_jsonl_chunks)
  - Updated README.md examples and documentation to clarify merge only happens in clean_up job
  - Eliminates duplication between data generation and clean_up job

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

- Update exports in **init**.py
  - Export all filter types: BooleanFilter, AllFilter, AnyFilter, RawDuckFilter, FilterNode
  - Export Annotation and DataItem models

- Add pytest configuration for .env loading
  - tests/conftest.py: Load .env file with python-dotenv
  - Fixes integration tests that require S3 credentials

- Implement data_filtering.py methods
  - s3_data_tool/data_filtering.py: AnnotationView and ExportView implementations
  - AnnotationLockError and DatasetNotMergedError exceptions
  - AnnotationView.**aenter**: Acquire S3Lock for annotator with configurable TTL
  - AnnotationView.**aexit**: Release lock, return False
  - AnnotationView.annotate(): Stream rows, filter, annotate with concurrency limit
    - Discover columns using discover_dataset_columns()
    - Compile filter to SQL WHERE clause
    - Load dataset parquet, skip already annotated rows
    - Apply user filter using DuckDB
    - Call annotation_fn with semaphore-limited concurrency
    - Write results to JSONL chunks
    - Spawn lock renewal task (renew 60s before expiry)
  - ExportView.**aiter**: Async iterator over filtered rows
    - Join dataset with all annotations on id/batch
    - Apply filter and yield rows as dict
  - Fixed s3_object_exists to catch ClientError for 404 responses
  - 11 integration tests pass (test_data_filtering.py)

- Implement producer-processor-uploader pattern for annotation
  - s3_data_tool/data_filtering.py: Two-queue async streaming architecture
  - Top-level task functions:
    - produce_items_for_annotation: Feeds DataItems from dataloader into input queue
    - annotation_worker: Pulls items, annotates, pushes results to output queue
    - upload_annotation_results: Buffers and uploads JSONL chunks to S3
  - FilterForExport.\_iter_filtered_items: Placeholder method (raises NotImplementedError)
    - Should enumerate batches, read parquet, apply filter, yield DataItems
  - Queue sizing derived from max_concurrency (queue_size = max_concurrency \* 2)
  - Sentinel pattern: None values signal task completion
  - Error handling: Workers catch exceptions and skip rows (allows retry)
  - Upload path: annotations/{annotator*name}/.temp/chunk*{00000}.jsonl
  - Lock renewal via gather_subject_to_lock_renewal()
  - No full dataset loading in memory; streaming via async queues

- Refactor clean_up.py for memory-efficient streaming
  - s3_data_tool/clean_up.py: Two-pass architecture with streaming iterators
  - Pass 1: Schema discovery via discover_schema() - reads all rows but discards after inferring types
  - Pass 2: Processing via async iterators - only keeps "seen" set + current batch in memory
  - Added batched_rows() and async_batched_rows() helpers for batch processing
  - Added TYPE_TO_PYARROW dict mapping Python types to PyArrow types
  - Added merge_types() function to handle type conflicts (non-null conflicts resolve to pa.string())
  - Added iter_jsonl_rows() and iter_parquet_rows() async generators (replaces read_* functions)
  - Modified deduplicate_rows() to accept iterator and yield deduplicated rows
  - Added async_deduplicate_rows() for async iterator support
  - Modified write_parquet() and added async_write_parquet() to accept iterator + schema
  - Updated MergeCandidate to Pydantic model with merged_schema field
  - Updated merge_dataset_batch() and merge_annotation_batch() to use streaming approach
  - Schema inference: Collects all columns from JSONL + existing parquet, merges schemas
  - Type conflict resolution: pa.null() gets overwritten, non-null conflicts resolve to pa.string()
  - Empty rows produce empty schema (pa.schema([]))
  - Kept backward-compatible read_jsonl_rows() and read_parquet_rows() functions

- Update filter tests for new filter pattern (no annotator field in BooleanFilter)
  - tests/test_filter.py: Removed annotator field from BooleanFilter tests
  - Removed tests: test_compile_with_existing_annotation_column, test_dataset_vs_annotation_column_qualification
  - Updated AllFilter, AnyFilter, NestedFilters, serialization tests to match new pattern
  - All 20 unit tests pass
- Implemented Data Viewer Web Interface (viewer/)
  - /viewer/backend: FastAPI backend with endpoints:
    - GET /api/datasets - list all datasets
    - GET /api/datasets/{name}/annotations - list annotators for a dataset
    - GET /api/datasets/{name}/schema - get columns with types
    - GET /api/datasets/{name}/count - approximate row count
    - GET /api/datasets/{name}/data - paginated data with filters
    - GET /api/datasets/{name}/row/{id} - single row by ID
  - /viewer/backend/duckdb_query.py: Query builder with FilterSpec class
    - Supports boolean, number (comparison), and text (contains/startswith/endswith) filters
    - Compiles to DuckDB WHERE clauses
    - LEFT JOIN base data with selected annotators
  - /viewer/frontend: React+Vite+shadcn-ui SPA
    - URL state via base64-encoded JSON (single ?s= param)
    - Dataset selector, column selector (popup), filter panel (popup)
    - Paginated data table with sort, row details modal
    - All state in URL for sharing
  - docker-compose.yml: 2 services (backend, frontend)
    - Backend: Python FastAPI on port 8000
    - Frontend: nginx serving React build on port 8080
    - Frontend proxies /api/ to backend
  - FilterForExport base class with DuckDB query generation via CTE pattern
  - base_columns (required), annotator_columns (dict), base_filter, annotator_filters (dict)
  - get_duckdb_query generates WITH clause with pre-filtered CTEs per annotator
  - \_iter_filtered_items streams results using fetchmany(1000) for memory efficiency
  - FilterForAnnotation extends FilterForExport, adds S3 lock for annotation
