from __future__ import annotations

import io

import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, Query

from data_lake_pipeline.storage import StorageBackend
from data_lake_pipeline.state import BatchState
from viewer.backend.dependencies import get_storage, get_batch_state
from viewer.backend.services.pipeline_status import PipelineStatusService

router = APIRouter()

ANNOTATIONS_PREFIX = "annotations"


def get_status_service(
    storage: StorageBackend = Depends(get_storage),
    batch_state: BatchState = Depends(get_batch_state),
) -> PipelineStatusService:
    return PipelineStatusService(storage=storage, batch_state=batch_state)


@router.get("/status")
async def get_status(
    refresh: bool = Query(False),
    service: PipelineStatusService = Depends(get_status_service),
):
    status = await service.get_status(force_refresh=refresh)
    return status


@router.post("/cache/invalidate")
async def invalidate_cache(
    service: PipelineStatusService = Depends(get_status_service),
):
    service._cache.invalidate_all()
    return {"status": "ok"}


@router.get("/sources")
async def get_sources(
    storage: StorageBackend = Depends(get_storage),
    batch_state: BatchState = Depends(get_batch_state),
):
    manifests = await batch_state.list_all()
    sources = {
        parts[1]
        for key in storage.list_objects("01_landing")
        if len(parts := key.split("/")) > 1
    } | {manifest.source for manifest in manifests if manifest.source}
    return sorted(sources)


@router.get("/filters")
async def get_available_filters(storage: StorageBackend = Depends(get_storage)):
    manifest = storage.get_json(f"{ANNOTATIONS_PREFIX}/filter_manifest.json")
    if manifest:
        return {
            "filters": manifest.get("filters", []),
            "last_updated": manifest.get("last_updated"),
        }

    filters = {
        col.rsplit("_passed", 1)[0]
        for key in storage.list_objects(ANNOTATIONS_PREFIX, "merged.parquet")
        for col in pq.read_schema(io.BytesIO(storage.read_bytes(key))).names
        if col.endswith("_passed")
    }

    return {"filters": sorted(filters), "last_updated": None}


@router.get("/batch-filters")
async def get_batch_filter_status(storage: StorageBackend = Depends(get_storage)):
    manifests_prefix = "manifests/stages"

    batches = [
        {
            "batch_id": data["batch_id"],
            "pipeline_stage": data.get("pipeline_stage"),
            "state": data["state"],
            "completed_filters": [
                f["filter_name"] for f in data.get("completed_filters", [])
            ],
            "has_merged": storage.object_exists(
                f"annotations/{data['batch_id']}/merged.parquet"
            ),
        }
        for key in storage.list_objects(manifests_prefix, ".json")
        if (data := storage.get_json(key))
    ]

    return batches


@router.get("/schema/{stage}")
async def get_stage_schema(stage: str):
    schemas = {
        "landing": {
            "columns": [
                {"name": "source", "type": "string", "filterable": True},
                {"name": "external_id", "type": "string", "filterable": False},
                {"name": "text", "type": "string", "filterable": True},
                {"name": "created_at", "type": "datetime", "filterable": True},
                {"name": "url", "type": "string", "filterable": False},
                {"name": "author", "type": "string", "filterable": True},
                {"name": "score", "type": "number", "filterable": True},
                {"name": "metadata", "type": "json", "filterable": False},
                {"name": "ingested_at", "type": "datetime", "filterable": False},
            ]
        },
        "queue": {
            "columns": [
                {"name": "batch_id", "type": "string", "filterable": False},
                {"name": "source", "type": "string", "filterable": True},
                {"name": "state", "type": "string", "filterable": True},
                {"name": "created_at", "type": "datetime", "filterable": True},
                {"name": "locked_by", "type": "string", "filterable": False},
                {"name": "locked_at", "type": "datetime", "filterable": False},
                {"name": "row_count", "type": "number", "filterable": True},
                {"name": "error", "type": "string", "filterable": True},
            ]
        },
        "processed": {
            "columns": [
                {"name": "source", "type": "string", "filterable": True},
                {"name": "external_id", "type": "string", "filterable": False},
                {"name": "annotation", "type": "string", "filterable": True},
                {"name": "model_name", "type": "string", "filterable": False},
                {"name": "processor_backend", "type": "string", "filterable": False},
                {"name": "source_file", "type": "string", "filterable": False},
                {"name": "processed_at", "type": "datetime", "filterable": True},
                {"name": "raw_text", "type": "string", "filterable": True},
            ]
        },
    }

    if stage not in schemas:
        return {"columns": []}

    return schemas[stage]
