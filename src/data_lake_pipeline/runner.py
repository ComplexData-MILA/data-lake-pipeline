from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from data_lake_pipeline.config import Settings
from data_lake_pipeline.io_async import CheckpointedWriter, stream_jsonl_async
from data_lake_pipeline.protocols import AsyncFilter, AsyncProcessor, StageContext
from data_lake_pipeline.stage_schemas import StageAwareBatchManifest
from data_lake_pipeline.state import StageAwareBatchState
from data_lake_pipeline.storage.s3 import S3Storage

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


async def run_stage(
    plugin: AsyncFilter | AsyncProcessor,
    *,
    stage_name: str,
    input_prefix: str,
    output_prefix_base: str,
    settings: Settings,
    batch_id: str | None = None,
    max_concurrent: int = 100,
    checkpoint_interval: int = 1000,
    is_filter: bool = True,
    create_batches: bool = True,
    min_batch_age_seconds: int = 0,
) -> bool:
    """
    Run a filter or processor stage.

    1. Optionally discovers/creates pending batches from input_prefix
    2. Claims a batch (specific or any pending)
    3. Processes records through the plugin
    4. Writes results with checkpointing

    Args:
        plugin: AsyncFilter or AsyncProcessor instance
        stage_name: Unique name for this stage (used for manifest namespacing)
        input_prefix: S3 prefix to read input JSONL files from
        output_prefix_base: S3 prefix base for output (passed/ rejected/ subdirs)
        settings: Pipeline settings
        batch_id: Specific batch ID to process, or None to claim any pending
        max_concurrent: Max concurrent async operations
        checkpoint_interval: Records per checkpoint chunk
        is_filter: True for filters (pass/reject), False for processors (all pass)
        create_batches: If True, scan input_prefix and create manifests for new files
        min_batch_age_seconds: Minimum age in seconds for a file to be considered stable

    Returns:
        True if a batch was processed, False if no pending batches.
    """
    storage = S3Storage(
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    state = StageAwareBatchState(storage, stage_name)

    if create_batches:
        await _ensure_pending_batches(
            state, storage, input_prefix, settings, min_batch_age_seconds
        )

    manifest = state.claim_batch(batch_id)
    if not manifest:
        logger.info("No pending batches for stage %s", stage_name)
        return False

    await _process_batch(
        plugin=plugin,
        manifest=manifest,
        state=state,
        settings=settings,
        output_prefix_base=output_prefix_base,
        stage_name=stage_name,
        max_concurrent=max_concurrent,
        checkpoint_interval=checkpoint_interval,
        is_filter=is_filter,
    )

    return True


async def _ensure_pending_batches(
    state: StageAwareBatchState,
    storage: S3Storage,
    input_prefix: str,
    settings: Settings,
    min_age_seconds: int,
) -> None:
    """Scan input prefix and create manifests for unprocessed files."""
    existing_manifests = {m.original_key for m in state.list_all()}

    for key in storage.list_objects(input_prefix, ".jsonl"):
        if key in existing_manifests:
            continue

        if min_age_seconds > 0:
            try:
                age = storage.get_object_age_seconds(key)
                if age < min_age_seconds:
                    logger.debug(
                        "Skipping %s: age %ds < min %ds", key, age, min_age_seconds
                    )
                    continue
            except Exception:
                pass

        source = key.split("/")[-1].replace(".jsonl", "")
        try:
            state.create_batch(
                source=source,
                original_key=key,
            )
            logger.info("Created manifest for %s", key)
        except Exception as e:
            logger.warning("Failed to create manifest for %s: %s", key, e)


async def _process_batch(
    plugin: AsyncFilter | AsyncProcessor,
    manifest: StageAwareBatchManifest,
    state: StageAwareBatchState,
    settings: Settings,
    output_prefix_base: str,
    stage_name: str,
    max_concurrent: int,
    checkpoint_interval: int,
    is_filter: bool,
) -> None:
    """Process a single batch through the plugin."""
    from data_lake_pipeline.processing.streaming_processor import (
        StreamingStageProcessor,
    )

    processor = StreamingStageProcessor(
        plugin=plugin,
        max_concurrent=max_concurrent,
    )

    context = StageContext(
        stage_name=stage_name,
        batch_id=manifest.batch_id,
    )

    chunk_base = f"{settings.s3_prefix}/{output_prefix_base}".strip("/")

    passed_writer = CheckpointedWriter(
        bucket=settings.s3_bucket,
        chunk_prefix=f"{chunk_base}/passed/{manifest.batch_id}/chunk_",
        final_key=f"{chunk_base}/passed/{manifest.batch_id}.jsonl",
        chunk_size=checkpoint_interval,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    rejected_writer = CheckpointedWriter(
        bucket=settings.s3_bucket,
        chunk_prefix=f"{chunk_base}/rejected/{manifest.batch_id}/chunk_",
        final_key=f"{chunk_base}/rejected/{manifest.batch_id}.jsonl",
        chunk_size=checkpoint_interval,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    try:
        async with passed_writer, rejected_writer:
            existing_passed, existing_rejected = state.get_existing_chunks(
                output_prefix_base,
                manifest.batch_id,
            )

            if existing_passed or existing_rejected:
                logger.info(
                    "Recovering: %d passed chunks, %d rejected chunks",
                    len(existing_passed),
                    len(existing_rejected),
                )
                await passed_writer.load_existing_chunks(existing_passed)
                await rejected_writer.load_existing_chunks(existing_rejected)

            processed_ids = passed_writer.processed_ids | rejected_writer.processed_ids

            input_key = manifest.original_key
            record_idx = 0
            checkpoint_counter = 0

            async for record, result in processor.process_stream(
                stream_jsonl_async(
                    settings.s3_bucket,
                    input_key,
                    settings.s3_endpoint_url,
                    settings.s3_access_key,
                    settings.s3_secret_key,
                ),
                context,
            ):
                external_id = record.get("external_id")
                if external_id and external_id in processed_ids:
                    record_idx += 1
                    continue

                passed = getattr(result, "passed", True)
                enriched = {
                    **record,
                    "stage_outputs": {
                        **record.get("stage_outputs", {}),
                        stage_name: {
                            "passed": passed,
                            "output": result.output,
                        },
                    },
                }

                if is_filter and not passed:
                    await rejected_writer.write_record(enriched)
                else:
                    await passed_writer.write_record(enriched)

                record_idx += 1
                checkpoint_counter += 1

                if checkpoint_counter >= checkpoint_interval * 5:
                    state.update_checkpoint(
                        manifest,
                        passed_chunks=passed_writer.existing_chunks,
                        rejected_chunks=rejected_writer.existing_chunks,
                        processed_ids_count=len(processed_ids),
                    )
                    checkpoint_counter = 0

        passed_summary, rejected_summary = await asyncio.gather(
            passed_writer.close(),
            rejected_writer.close(),
        )

        merged_passed, merged_rejected = await asyncio.gather(
            passed_writer.merge_chunks(),
            rejected_writer.merge_chunks(),
        )

        await asyncio.gather(
            passed_writer.delete_chunks(),
            rejected_writer.delete_chunks(),
        )

        state.complete_batch(
            manifest,
            output_key_passed=merged_passed,
            output_key_rejected=merged_rejected
            if rejected_summary.record_count > 0
            else None,
            passed_count=passed_summary.record_count,
            rejected_count=rejected_summary.record_count,
        )

        logger.info(
            "Completed stage %s batch %s: %d passed, %d rejected",
            stage_name,
            manifest.batch_id,
            passed_summary.record_count,
            rejected_summary.record_count,
        )

    except Exception as e:
        logger.exception("Failed to process batch %s: %s", manifest.batch_id, e)
        state.fail_batch(manifest, str(e))
        raise
