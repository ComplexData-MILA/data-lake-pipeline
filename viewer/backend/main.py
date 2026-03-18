from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from viewer.backend.routers import status, landing, manifests, processed, browse

app = FastAPI(title="Data Lake Viewer")

app.include_router(status.router, prefix="/api")
app.include_router(landing.router, prefix="/api")
app.include_router(manifests.router, prefix="/api")
app.include_router(processed.router, prefix="/api")
app.include_router(browse.router, prefix="/api")

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    index_path = static_dir / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"error": "Frontend not built"}
