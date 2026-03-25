from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from data_lake_pipeline.config import Settings
from data_lake_pipeline.io_async import CheckpointedWriter, stream_jsonl_async
from data_lake_pipeline.protocols import AsyncFilter, AsyncProcessor, StageContext
from data_lake_pipeline.stage_schemas import FilterCompletion, StageAwareBatchManifest
from data_lake_pipeline.state import StageAwareBatchState
from data_lake_pipeline.storage.s3 import S3Storage

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


async def run_stage(
    handler: AsyncFilter | AsyncProcessor,
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
    2. Claims a batch for a specific filter
    3. Processes records through the handler
    4. Writes results as parquet with checkpointing

    Args:
        handler: AsyncFilter or AsyncProcessor instance
        stage_name: Unique name for this stage (used for manifest namespacing and filter_name)
        input_prefix: S3 prefix to read input JSONL files from
        output_prefix_base: S3 prefix base for output (annotations/{batch_id}/filters/{stage_name}.parquet)
        settings: Pipeline settings
        batch_id: Specific batch ID to process, or None to claim any available
        max_concurrent: Max concurrent async operations
        checkpoint_interval: Records per checkpoint chunk
        is_filter: True for filters (track pass/fail), False for processors (all pass)
        create_batches: If True, scan input_prefix and create manifests for new files
        min_batch_age_seconds: Minimum age in seconds for a file to be considered stable

    Returns:
        True if a batch was processed, False if no available batches.
    """
    storage = S3Storage(
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    state = StageAwareBatchState(
        storage, stage_name,
        mutex_ws_url=settings.mutex_ws_url,
        lock_timeout_seconds=settings.filter_lock_timeout_seconds,
    )

    if create_batches:
        await _ensure_pending_batches(
            state, storage, input_prefix, settings, min_batch_age_seconds
        )

    manifest = await _claim_batch_for_filter(state, stage_name, batch_id)
    if not manifest:
        logger.info("No available batches for filter %s", stage_name)
        return False

    await _process_batch(
        handler=handler,
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


async def _claim_batch_for_filter(
    state: StageAwareBatchState, filter_name: str, batch_id: str | None = None
) -> StageAwareBatchManifest | None:
    if batch_id:
        return await state.claim_filter(batch_id, filter_name)

    available = await state.list_available_for_filter(filter_name)
    for manifest in available:
        claimed = await state.claim_filter(manifest.batch_id, filter_name)
        if claimed:
            return claimed
    return None


async def _ensure_pending_batches(
    state: StageAwareBatchState,
    storage: S3Storage,
    input_prefix: str,
    settings: Settings,
    min_age_seconds: int,
) -> None:
    """Scan input prefix and create manifests for unprocessed files."""
    existing_manifests = {m.original_key for m in await state.list_all()}

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
            await state.create_batch(
                source=source,
                original_key=key,
            )
            logger.info("Created manifest for %s", key)
        except Exception as e:
            logger.warning("Failed to create manifest for %s: %s", key, e)


async def _process_batch(
    handler: AsyncFilter | AsyncProcessor,
    manifest: StageAwareBatchManifest,
    state: StageAwareBatchState,
    settings: Settings,
    output_prefix_base: str,
    stage_name: str,
    max_concurrent: int,
    checkpoint_interval: int,
    is_filter: bool,
) -> None:
    """Process a single batch through the handler."""
    from data_lake_pipeline.processing.streaming_processor import (
        StreamingStageProcessor,
    )

    processor = StreamingStageProcessor(
        handler=handler,
        max_concurrent=max_concurrent,
    )

    context = StageContext(
        stage_name=stage_name,
        batch_id=manifest.batch_id,
    )

    output_prefix = f"{output_prefix_base}/{manifest.batch_id}/filters"
    chunk_prefix = f"{settings.s3_prefix}/{output_prefix}/.chunks/{stage_name}_"
    final_key = f"{settings.s3_prefix}/{output_prefix}/{stage_name}.jsonl"

    writer = CheckpointedWriter(
        bucket=settings.s3_bucket,
        chunk_prefix=chunk_prefix,
        final_key=final_key,
        chunk_size=checkpoint_interval,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )

    passed_count = 0
    rejected_count = 0

    try:
        async with writer:
            existing_chunks = state.get_existing_chunks(
                output_prefix, manifest.batch_id, stage_name
            )

            if existing_chunks:
                logger.info("Recovering: %d existing chunks", len(existing_chunks))
                await writer.load_existing_chunks(existing_chunks)

            processed_ids = writer.processed_ids

            input_key = (
                f"{settings.s3_prefix}/{manifest.original_key}"
                if settings.s3_prefix
                else manifest.original_key
            )
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
                external_id = record.get("external_id") or record.get("id")
                if external_id and external_id in processed_ids:
                    continue

                passed = getattr(result, "passed", True)

                if is_filter:
                    if passed:
                        passed_count += 1
                    else:
                        rejected_count += 1

                enriched = {
                    "id": external_id,
                    f"{stage_name}_passed": passed,
                    f"{stage_name}_score": getattr(result, "score", None),
                    f"{stage_name}_reason": getattr(result, "reason", None),
                }

                await writer.write_record(enriched)

                checkpoint_counter += 1

                if checkpoint_counter >= checkpoint_interval * 5:
                    await state.update_checkpoint(
                        manifest,
                        filter_name=stage_name,
                        chunk_keys=writer.existing_chunks,
                        processed_ids_count=len(processed_ids),
                    )
                    checkpoint_counter = 0

        await writer.close()

        parquet_key = await writer.merge_to_parquet()

        await writer.delete_chunks()

        completion = FilterCompletion(
            completed_at=datetime.now(timezone.utc).isoformat(),
            output_key=parquet_key.replace(f"{settings.s3_prefix}/", ""),
            passed_count=passed_count,
            rejected_count=rejected_count,
        )

        await state.complete_filter(manifest, stage_name, completion)

        logger.info(
            "Completed stage %s batch %s: %d passed, %d rejected -> %s",
            stage_name,
            manifest.batch_id,
            passed_count,
            rejected_count,
            parquet_key,
        )

    except Exception as e:
        logger.exception("Failed to process batch %s: %s", manifest.batch_id, e)
        await state.fail_filter(manifest, stage_name, str(e))
        raise
