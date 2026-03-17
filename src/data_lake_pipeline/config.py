from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def parse_s3_url(url: str) -> tuple[str, str]:
    if url.startswith("s3://"):
        url = url[5:]
    parts = url.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix.rstrip("/")


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    s3_prefix: str
    s3_endpoint_url: str | None
    s3_access_key: str | None
    s3_secret_key: str | None
    log_level: str
    stable_file_age_minutes: int
    slurm_enabled: bool
    slurm_command: str
    slurm_script: str
    annotator_backend: str
    model_name: str
    prompt_template: str
    vllm_tensor_parallel_size: int
    vllm_temperature: float
    vllm_max_tokens: int
    use_example_source_data: bool

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        s3_url = os.getenv("PIPELINE_S3_URL", "")
        if not s3_url:
            raise ValueError("PIPELINE_S3_URL must be set (e.g., s3://my-bucket/data-project/)")
        bucket, prefix = parse_s3_url(s3_url)
        return cls(
            s3_bucket=bucket,
            s3_prefix=prefix,
            s3_endpoint_url=os.getenv("PIPELINE_S3_ENDPOINT_URL") or None,
            s3_access_key=os.getenv("PIPELINE_S3_ACCESS_KEY") or None,
            s3_secret_key=os.getenv("PIPELINE_S3_SECRET_KEY") or None,
            log_level=os.getenv("PIPELINE_LOG_LEVEL", "INFO"),
            stable_file_age_minutes=int(
                os.getenv("PIPELINE_STABLE_FILE_AGE_MINUTES", "30")
            ),
            slurm_enabled=os.getenv("PIPELINE_SLURM_ENABLED", "false").lower()
            == "true",
            slurm_command=os.getenv("PIPELINE_SLURM_COMMAND", "sbatch"),
            slurm_script=os.getenv("PIPELINE_SLURM_SCRIPT", "slurm/slurm_job.sh"),
            annotator_backend=os.getenv("PIPELINE_ANNOTATOR_BACKEND", "mock"),
            model_name=os.getenv("PIPELINE_MODEL_NAME", "Qwen/Qwen3.5-9B-Instruct"),
            prompt_template=os.getenv(
                "PIPELINE_PROMPT_TEMPLATE",
                "Annotate the following post and return a compact JSON object with topic, sentiment, safety_flags, and rationale_short.\n\nPost:\n{text}",
            ),
            vllm_tensor_parallel_size=int(
                os.getenv("PIPELINE_VLLM_TENSOR_PARALLEL_SIZE", "1")
            ),
            vllm_temperature=float(os.getenv("PIPELINE_VLLM_TEMPERATURE", "0.1")),
            vllm_max_tokens=int(os.getenv("PIPELINE_VLLM_MAX_TOKENS", "256")),
            use_example_source_data=os.getenv(
                "PIPELINE_USE_EXAMPLE_SOURCE_DATA", "false"
            ).lower()
            == "true",
        )

    @property
    def landing_prefix(self) -> str:
        return "01_landing"

    @property
    def pending_prefix(self) -> str:
        return "02_pending"

    @property
    def inflight_prefix(self) -> str:
        return "02_inflight"

    @property
    def failed_prefix(self) -> str:
        return "02_failed"

    @property
    def processed_prefix(self) -> str:
        return "03_processed"

    @property
    def archive_prefix(self) -> str:
        return "04_archive"
