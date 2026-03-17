# S3 Data Lake Pipeline

A repository for an **S3-based data lake / state queue** that decouples:

1. **Ingestion** of heterogeneous social data sources on independent schedules
2. **Batch annotation** on a SLURM GPU cluster using a self-hosted Qwen model via **vLLM** (with an extension point for **SGLang** / an Agent SDK)
3. **Serving / querying** of the processed "stash" as Parquet

This repo is designed around a **Landing → Processing Queue → Processed → Archive** pattern on S3, enabling components to run on different machines without a shared filesystem.

## Architecture

```text
s3://bucket/prefix/
├── 00_cache/                # Bloom filters and other cache data
│   └── x_community_notes/
│       └── seen_notes.bloom
├── 01_landing/              # Raw data from ingestion
│   ├── bluesky/
│   ├── x_community_notes/
│   └── reddit/
├── 02_pending/              # Batches awaiting processing
├── 03_processed/            # Annotated Parquet outputs
├── 04_archive/              # Archived raw batches
└── manifests/               # Batch state manifests
    ├── batch_id_1.json
    └── batch_id_2.json
```

## State Management

Batch state is tracked via **manifest files** (one JSON file per batch). Each manifest contains:

```json
{
  "batch_id": "reddit__2026-03-13__f2f53cd8",
  "source": "reddit",
  "original_key": "01_landing/reddit/2026-03-13.jsonl",
  "state": "pending",
  "created_at": "2026-03-13T12:00:00Z",
  "locked_by": null,
  "locked_at": null,
  "row_count": null,
  "output_key": null,
  "error": null
}
```

States: `pending` → `inflight` → `completed` → `archived` (or `failed`)

## Distributed Locking

Uses **S3 conditional writes** (`IfNoneMatch: *`) for optimistic locking when:
- Creating new batch manifests
- Claiming pending batches for processing

This allows multiple workers to safely claim batches without a distributed lock service.

## What is implemented

- Source-specific ingestion entrypoints:
  - `scripts/ingest_bluesky.py`
  - `scripts/ingest_x_notes.py`
  - `scripts/ingest_reddit.py`
- **O(1) memory streaming** for reading/writing large JSONL files
- A **launcher / batcher** that:
  - finds stable landing files older than a configurable age
  - creates manifests for them and moves them to pending
  - optionally submits a SLURM job with `sbatch`
- A **batch processor** that:
  - claims pending batches via conditional writes
  - runs annotation using either:
    - `mock` annotator for development
    - `vllm` annotator for offline batch inference
    - `sglang` stub / extension point
  - writes Parquet outputs
  - archives successful raw batches
  - marks failed batches

## Data model

### Source adapter output shape

Each source adapter returns a list of `SourcePost` objects:

```python
@dataclass
class SourcePost:
    source: str
    external_id: str
    text: str
    created_at: str
    url: str | None
    author: str | None
    score: float | None
    metadata: dict[str, Any]
```

### Landed JSONL record shape

Each raw line written to `01_landing/<source>/<YYYY-MM-DD>.jsonl`:

```json
{
  "source": "bluesky",
  "external_id": "at://did:plc:example/app.bsky.feed.post/abc",
  "text": "example content",
  "created_at": "2026-03-13T12:00:00Z",
  "url": "https://bsky.app/profile/example/post/abc",
  "author": "example_user",
  "score": 123.0,
  "metadata": {"lang": "en", "labels": []},
  "ingested_at": "2026-03-13T12:03:14.000000+00:00"
}
```

### Annotation output shape

The processing job emits Parquet rows:

```json
{
  "source": "reddit",
  "external_id": "t3_abc123",
  "annotation": "{\"topic\":\"finance\",\"risk\":\"low\"}",
  "model_name": "Qwen/Qwen3.5-9B-Instruct",
  "processor_backend": "vllm",
  "source_file": "reddit__2026-03-13__f2f53cd8",
  "processed_at": "2026-03-13T18:42:00.000000+00:00",
  "raw_text": "original post text"
}
```

## Directory layout in this repo

```text
shared-data-lake-pipeline/
├── scripts/                # CLI entrypoints
├── slurm/                  # SLURM job scripts
├── src/data_lake_pipeline/
│   ├── storage/            # S3 storage backend
│   ├── ingestion/          # Source adapters, writer, bloom filter
│   ├── orchestration/      # Batch promotion logic
│   ├── processing/         # Batch processor, annotators
│   ├── config.py           # Settings from environment
│   ├── state.py            # Manifest management
│   ├── io.py               # Streaming JSONL/Parquet IO
│   └── schemas.py          # Data models
├── tests/
├── .env.example
├── Makefile
├── pyproject.toml
└── README.md
```

## Quick start

### 1) Create an environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2) Configure S3

Copy `.env.example` into `.env` or export variables directly:

```bash
export PIPELINE_S3_URL=s3://my-bucket/data-project/
export PIPELINE_ANNOTATOR_BACKEND=mock
export PIPELINE_SLURM_ENABLED=false
```

### 3) Configure AWS credentials

Ensure AWS credentials are available via one of:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM role (for EC2/ECS)

### 4) Run ingestion jobs

```bash
python scripts/ingest_bluesky.py
python scripts/ingest_x_notes.py
python scripts/ingest_reddit.py
```

### 5) Promote stable landing files and submit processing

```bash
python scripts/launch_pipeline.py --min-age-minutes 30
```

### 6) Run the processor locally

```bash
python scripts/process_batch.py
```

### 7) Or submit the SLURM job

```bash
sbatch slurm/slurm_job.sh
```

## Cron examples

```cron
*/5 * * * * cd /path/to/shared-data-lake-pipeline && /path/to/.venv/bin/python scripts/ingest_bluesky.py >> /var/log/cron_bluesky.log 2>&1
17 2 * * * cd /path/to/shared-data-lake-pipeline && /path/to/.venv/bin/python scripts/ingest_x_notes.py >> /var/log/cron_x_notes.log 2>&1
9 3 * * * cd /path/to/shared-data-lake-pipeline && /path/to/.venv/bin/python scripts/ingest_reddit.py >> /var/log/cron_reddit.log 2>&1
7 * * * * cd /path/to/shared-data-lake-pipeline && /path/to/.venv/bin/python scripts/launch_pipeline.py --min-age-minutes 30 >> /var/log/cron_launcher.log 2>&1
```

## Notes on robustness

- **Ingestion and processing are decoupled** through S3.
- **Manifest-based state** replaces filesystem directories for tracking batch status.
- **S3 conditional writes** provide distributed locking without additional services.
- **O(1) memory usage** when processing large files via streaming.
- Raw data is archived after success instead of being deleted immediately.
- A `mock` annotator allows local validation without a GPU or model runtime.

## vLLM / SGLang / Agent SDK integration

### vLLM
The repo includes a `VLLMAnnotator` that uses offline-style batch generation. Install the `processing` extras in a GPU-capable environment.

### SGLang
`SGLangAnnotator` is provided as an extension point. Replace the stub in `src/data_lake_pipeline/processing/annotator.py` with your preferred SGLang client or Agent SDK integration.

### Agent SDK
If your annotation flow requires tool use, multi-step reasoning, or structured agent outputs, implement it behind the `BaseAnnotator` interface.

## Querying the stash

```python
import duckdb

df = duckdb.query(
    "SELECT source, external_id, annotation "
    "FROM 's3://my-bucket/data-project/03_processed/*.parquet' "
    "WHERE annotation ILIKE '%fraud%'"
).df()

print(df.head())
```

## Repository contents

- `src/data_lake_pipeline/schemas.py` — input/output record shapes, BatchManifest
- `src/data_lake_pipeline/storage/` — S3 storage backend abstraction
- `src/data_lake_pipeline/state.py` — manifest-based batch state management
- `src/data_lake_pipeline/io.py` — streaming JSONL/Parquet IO
- `src/data_lake_pipeline/ingestion/writer.py` — append-safe JSONL landing writer
- `src/data_lake_pipeline/orchestration/batcher.py` — landing → queue promotion + optional `sbatch`
- `src/data_lake_pipeline/processing/batch_processor.py` — queue claim + annotation + Parquet output + archive/failure handling
- `src/data_lake_pipeline/processing/annotator.py` — mock / vLLM / SGLang annotators
- `slurm/slurm_job.sh` — example SLURM job script
