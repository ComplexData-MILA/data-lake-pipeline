from __future__ import annotations

from fastapi import FastAPI

from viewer.backend.routers import (
    status,
    landing,
    manifests,
    processed,
    browse,
    records,
)

app = FastAPI(title="Data Lake Viewer")

app.include_router(status.router, prefix="/api")
app.include_router(landing.router, prefix="/api")
app.include_router(manifests.router, prefix="/api")
app.include_router(processed.router, prefix="/api")
app.include_router(browse.router, prefix="/api")
app.include_router(records.router, prefix="/api")
