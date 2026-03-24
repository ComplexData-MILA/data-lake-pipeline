from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from data_lake_pipeline.config import Settings
from data_lake_pipeline.io import stream_jsonl, write_parquet
from data_lake_pipeline.logging_utils import configure_logging
from data_lake_pipeline.processing.annotator import AnnotationRequest, build_annotator
from data_lake_pipeline.schemas import AnnotationResult, ProcessingSummary
from data_lake_pipeline.state import BatchState
from data_lake_pipeline.storage.s3 import S3Storage

if TYPE_CHECKING:
    from data_lake_pipeline.schemas import BatchManifest

logger = logging.getLogger(__name__)


def _claim_pending_batch(state: BatchState) -> BatchManifest | None:
    pending = state.list_pending()
    for manifest in pending:
        claimed = state.claim_batch(manifest.batch_id)
        if claimed:
            return claimed
    return None


def process_pending_batches(settings: Settings) -> ProcessingSummary:
    configure_logging(settings.log_level)
    storage = S3Storage(
        settings.s3_bucket,
        settings.s3_prefix,
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    state = BatchState(storage)

    manifest = _claim_pending_batch(state)

    if not manifest:
        return ProcessingSummary(
            claimed_files=[],
            archived_files=[],
            failed_files=[],
            output_file=None,
            processed_records=0,
        )

    claimed_batch_id = manifest.batch_id

    try:
        annotator = build_annotator(settings)
        requests: list[AnnotationRequest] = []

        data_key = f"{settings.pending_prefix}/{manifest.batch_id}.jsonl"
        if not storage.object_exists(data_key):
            data_key = manifest.original_key

        for row in stream_jsonl(storage, data_key):
            requests.append(
                AnnotationRequest(
                    source=row["source"],
                    external_id=row["external_id"],
                    text=row["text"],
                    source_file=manifest.batch_id,
                )
            )

        if not requests:
            state.fail_batch(manifest, "No records found in batch")
            return ProcessingSummary(
                claimed_files=[claimed_batch_id],
                archived_files=[],
                failed_files=[claimed_batch_id],
                output_file=None,
                processed_records=0,
            )

        annotations = annotator.annotate(requests, settings)
        if len(annotations) != len(requests):
            raise RuntimeError(
                f"Annotator output length mismatch: expected {len(requests)}, got {len(annotations)}"
            )

        processed_at = datetime.now(timezone.utc).isoformat()
        results: list[dict] = []
        for req, annotation in zip(requests, annotations, strict=True):
            results.append(
                AnnotationResult(
                    source=req.source,
                    external_id=req.external_id,
                    annotation=annotation,
                    model_name=settings.model_name,
                    processor_backend=annotator.backend_name,
                    source_file=req.source_file,
                    processed_at=processed_at,
                    raw_text=req.text,
                ).model_dump(mode="json")
            )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_key = f"{settings.processed_prefix}/annotated_{timestamp}_{claimed_batch_id}.parquet"
        write_parquet(storage, output_key, results)

        archive_key = f"{settings.archive_prefix}/{manifest.batch_id}.jsonl"
        storage.copy_object(data_key, archive_key)
        storage.delete_object(data_key)

        state.complete_batch(manifest, output_key, len(results))

        return ProcessingSummary(
            claimed_files=[claimed_batch_id],
            archived_files=[manifest.batch_id],
            failed_files=[],
            output_file=output_key,
            processed_records=len(results),
        )

    except Exception as e:
        logger.exception("Batch processing failed for %s", claimed_batch_id)
        state.fail_batch(manifest, str(e))

        return ProcessingSummary(
            claimed_files=[claimed_batch_id],
            archived_files=[],
            failed_files=[claimed_batch_id],
            output_file=None,
            processed_records=0,
        )
