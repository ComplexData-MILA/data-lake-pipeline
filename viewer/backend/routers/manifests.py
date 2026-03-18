from __future__ import annotations

from fastapi import APIRouter, Depends, Query, HTTPException

from data_lake_pipeline.storage import StorageBackend
from data_lake_pipeline.state import BatchState
from viewer.backend.dependencies import get_storage, get_batch_state

router = APIRouter()

MANIFESTS_PREFIX = "manifests"


@router.get("/manifests")
async def list_manifests(
    state: str | None = Query(None),
    refresh: bool = Query(False),
    storage: StorageBackend = Depends(get_storage),
    batch_state: BatchState = Depends(get_batch_state),
):
    states = None
    if state:
        states = [s.strip() for s in state.split(",") if s.strip()]
    
    manifests = []
    for key in storage.list_objects(MANIFESTS_PREFIX, ".json"):
        batch_id = key.split("/")[-1].replace(".json", "")
        manifest = batch_state.get_manifest(batch_id)
        if manifest:
            manifest_dict = manifest.to_dict()
            if states is None or manifest_dict.get("state") in states:
                manifests.append(manifest_dict)
    
    return manifests


@router.get("/manifests/{batch_id}")
async def get_manifest(
    batch_id: str,
    batch_state: BatchState = Depends(get_batch_state),
):
    manifest = batch_state.get_manifest(batch_id)
    if not manifest:
        raise HTTPException(status_code=404, detail="Manifest not found")
    return manifest.to_dict()
