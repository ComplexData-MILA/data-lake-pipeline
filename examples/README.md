# Data Lake Pipeline Examples

This directory contains example implementations and configurations for the data lake pipeline.

## Directory Structure

```
examples/
├── openai_plugins/         # Example standalone filter/processor packages
│   └── __init__.py
├── slurm/
│   └── sglang_stage.sh     # SLURM job script template
└── README.md
```

## Architecture

Each filter/processor is a **standalone package** with its own CLI entrypoint. The `data-lake-pipeline` package provides the `run_stage()` utility for processing batches.

### Flow

1. Each filter/processor package has its own CLI command
2. The CLI uses `run_stage()` to claim and process batches
3. Configuration (prompts, thresholds, models) via environment variables
4. Each filter/processor can have its own cron job or SLURM job

## OpenAI Plugins Example (`openai_plugins/`)

Example filter/processor implementations using OpenAI SDK. These can be used as templates for creating your own packages.

### QualityFilter Example

```python
# quality_filter/cli.py
import asyncio
import os
from data_lake_pipeline import Settings, run_stage
from data_lake_pipeline.protocols import AsyncFilter, FilterResult, StageContext
from openai import AsyncOpenAI

class QualityFilter(AsyncFilter):
    def __init__(self, threshold: float = 0.7, model: str | None = None):
        self.threshold = threshold
        self.model = model or os.getenv("QUALITY_FILTER_MODEL", "gpt-4o-mini")
        self.prompt = os.getenv(
            "QUALITY_FILTER_PROMPT",
            "Rate quality (0.0-1.0). Return JSON: {\"score\": <float>}\n\nPost: {text}"
        )
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = AsyncOpenAI()
        return self._client

    async def __call__(self, records, context) -> list[FilterResult]:
        # ... implementation

def main():
    settings = Settings.from_env()
    plugin = QualityFilter(
        threshold=float(os.getenv("QUALITY_FILTER_THRESHOLD", "0.7"))
    )
    asyncio.run(run_stage(
        plugin=plugin,
        stage_name="quality_filter",
        input_prefix=os.getenv("QUALITY_FILTER_INPUT_PREFIX", "02_pending"),
        output_prefix_base=os.getenv("QUALITY_FILTER_OUTPUT_PREFIX", "03_quality_filtered"),
        settings=settings,
    ))

if __name__ == "__main__":
    main()
```

### pyproject.toml

```toml
[project]
name = "quality-filter"
version = "1.0.0"
dependencies = ["data-lake-pipeline", "openai>=1.0.0"]

[project.scripts]
quality-filter = "quality_filter.cli:main"
```

## SLURM Integration (`slurm/`)

Example SLURM job script for GPU-based filtering.

```bash
#!/bin/bash
#SBATCH --job-name=quality-filter
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --time=02:00:00

export PIPELINE_S3_URL="s3://my-bucket/data-project/"
export QUALITY_FILTER_THRESHOLD="0.7"
export QUALITY_FILTER_MODEL="Qwen/Qwen3.5-9B-Instruct"

# Start local inference server
python -m sglang.launch_server --model $QUALITY_FILTER_MODEL --port 8000 &
sleep 60
export OPENAI_BASE_URL="http://localhost:8000/v1"
export OPENAI_API_KEY="dummy"

# Run the filter
quality-filter
```

## Environment Variables

### Pipeline Core

| Variable | Description | Required |
|----------|-------------|----------|
| `PIPELINE_S3_URL` | S3 bucket URL | Yes |
| `PIPELINE_S3_ENDPOINT_URL` | Custom S3 endpoint | No |
| `PIPELINE_S3_ACCESS_KEY` | S3 access key | No |
| `PIPELINE_S3_SECRET_KEY` | S3 secret key | No |
| `PIPELINE_LOG_LEVEL` | Log level (default: INFO) | No |

### Filter/Processor Config

Each filter/processor uses its own prefixed environment variables:

| Pattern | Example |
|---------|---------|
| `{NAME}_THRESHOLD` | `QUALITY_FILTER_THRESHOLD=0.7` |
| `{NAME}_MODEL` | `QUALITY_FILTER_MODEL=gpt-4o-mini` |
| `{NAME}_PROMPT` | `QUALITY_FILTER_PROMPT=...` |
| `{NAME}_INPUT_PREFIX` | `QUALITY_FILTER_INPUT_PREFIX=02_pending` |
| `{NAME}_OUTPUT_PREFIX` | `QUALITY_FILTER_OUTPUT_PREFIX=03_quality_filtered` |

## Creating a New Filter/Processor

1. **Create a package**:

```
my-filter/
├── src/my_filter/
│   ├── __init__.py
│   └── cli.py
└── pyproject.toml
```

2. **Implement the protocol**:

```python
from data_lake_pipeline.protocols import AsyncFilter, FilterResult, StageContext

class MyFilter(AsyncFilter):
    def __init__(self, threshold: float = 0.5):
        self.threshold = threshold

    async def __call__(self, records, context) -> list[FilterResult]:
        return [FilterResult(passed=True) for _ in records]
```

3. **Create CLI**:

```python
# my_filter/cli.py
import asyncio, os
from data_lake_pipeline import Settings, run_stage
from my_filter import MyFilter

def main():
    settings = Settings.from_env()
    plugin = MyFilter(threshold=float(os.getenv("MY_FILTER_THRESHOLD", "0.5")))
    asyncio.run(run_stage(
        plugin=plugin,
        stage_name="my_filter",
        input_prefix=os.getenv("MY_FILTER_INPUT_PREFIX", "02_pending"),
        output_prefix_base=os.getenv("MY_FILTER_OUTPUT_PREFIX", "03_my_filter"),
        settings=settings,
    ))
```

4. **Register CLI** in `pyproject.toml`:

```toml
[project.scripts]
my-filter = "my_filter.cli:main"
```

## run_stage() Parameters

```python
async def run_stage(
    plugin: AsyncFilter | AsyncProcessor,  # Your filter/processor instance
    *,
    stage_name: str,                        # Unique stage identifier
    input_prefix: str,                      # S3 prefix to read from
    output_prefix_base: str,                # S3 prefix base for outputs
    settings: Settings,                     # Pipeline settings
    batch_id: str | None = None,            # Specific batch, or None for any
    max_concurrent: int = 100,              # Concurrency limit
    checkpoint_interval: int = 1000,        # Records per checkpoint
    is_filter: bool = True,                 # True for filters, False for processors
    create_batches: bool = True,            # Auto-create manifests from input files
    min_batch_age_seconds: int = 0,         # Min file age before processing
) -> bool:                                  # True if batch processed
```
