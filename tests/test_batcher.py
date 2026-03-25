import asyncio
from data_lake_pipeline.state import BatchState
from tests.conftest import MockStorage


def _settings():
    from data_lake_pipeline.config import Settings

    return Settings(
        s3_bucket="test-bucket",
        s3_prefix="test-prefix",
        s3_endpoint_url=None,
        s3_access_key=None,
        s3_secret_key=None,
        log_level="INFO",
        stable_file_age_minutes=30,
        filter_lock_timeout_seconds=600,
        annotator_backend="mock",
        model_name="mock-model",
        prompt_template="Annotate: {text}",
        vllm_tensor_parallel_size=1,
        vllm_temperature=0.1,
        vllm_max_tokens=256,
        use_example_source_data=True,
        slurm_enabled=False,
        slurm_command="sbatch",
        slurm_script="slurm/slurm_job.sh",
        mutex_ws_url="wss://test-mutex.example.com",
    )


async def test_promote_stable_landing_files_creates_manifest():
    storage = MockStorage()
    state = BatchState(storage, mutex_ws_url="wss://test-mutex.example.com")

    storage.append_jsonl(
        "01_landing/reddit/2026-03-13.jsonl", iter([{"hello": "world"}])
    )

    manifest = await state.create_batch("reddit", "01_landing/reddit/2026-03-13.jsonl")

    assert manifest.batch_id.startswith("reddit__")
    assert manifest.state == "pending"
    assert manifest.source == "reddit"

    manifests = await state.list_pending()
    assert len(manifests) == 1
    assert manifests[0].batch_id == manifest.batch_id
