from __future__ import annotations

import logging
import subprocess

from data_lake_pipeline.config import Settings
from data_lake_pipeline.logging_utils import configure_logging
from data_lake_pipeline.state import BatchState
from data_lake_pipeline.storage.s3 import S3Storage

logger = logging.getLogger(__name__)


def _iter_landing_jsonl_files(storage: S3Storage, landing_prefix: str) -> list[str]:
    keys = storage.list_objects(landing_prefix, ".jsonl")
    return sorted(keys)


def _is_stable(storage: S3Storage, key: str, min_age_minutes: int) -> bool:
    age_seconds = storage.get_object_age_seconds(key)
    return age_seconds >= min_age_minutes * 60


def _extract_source_from_key(key: str, landing_prefix: str) -> str:
    prefix_len = len(landing_prefix) + 1
    rest = key[prefix_len:]
    parts = rest.split("/")
    return parts[0] if parts else "unknown"


def promote_stable_landing_files(settings: Settings, min_age_minutes: int) -> list[str]:
    configure_logging(settings.log_level)
    storage = S3Storage(
        settings.s3_bucket,
        settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    state = BatchState(storage)
    promoted: list[str] = []

    landing_prefix = settings.landing_prefix
    for key in _iter_landing_jsonl_files(storage, landing_prefix):
        if not _is_stable(storage, key, min_age_minutes=min_age_minutes):
            continue

        source_name = _extract_source_from_key(key, landing_prefix)
        try:
            manifest = state.create_batch(source_name, key)
            storage.copy_object(key, f"{settings.pending_prefix}/{manifest.batch_id}.jsonl")
            storage.delete_object(key)
            promoted.append(manifest.batch_id)
            logger.info("Promoted %s -> %s", key, manifest.batch_id)
        except Exception as e:
            logger.warning("Failed to promote %s: %s", key, e)

    return promoted


def submit_slurm_if_needed(settings: Settings) -> None:
    configure_logging(settings.log_level)
    storage = S3Storage(
        settings.s3_bucket,
        settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    state = BatchState(storage)

    pending = state.list_pending()

    if not pending:
        logger.info("No pending queue items. Not submitting SLURM.")
        return

    if not settings.slurm_enabled:
        logger.info(
            "SLURM submission disabled. Would have run: %s %s",
            settings.slurm_command,
            settings.slurm_script,
        )
        return

    cmd = [settings.slurm_command, settings.slurm_script]
    logger.info("Submitting SLURM job: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
