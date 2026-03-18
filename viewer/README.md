# Data Lake Viewer

A web-based viewer for exploring the shared data lake pipeline. Provides a React frontend and FastAPI backend for browsing landing data, manifests, and querying processed parquet files.

## Prerequisites

- Docker
- S3-compatible storage access (AWS S3 or MinIO)

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PIPELINE_S3_URL` | Yes | S3 bucket URL (e.g., `s3://my-bucket/prefix`) |
| `PIPELINE_S3_ENDPOINT_URL` | No | Custom S3 endpoint for MinIO or other S3-compatible storage |
| `PIPELINE_S3_ACCESS_KEY` | No | S3 access key (defaults to AWS credentials chain) |
| `PIPELINE_S3_SECRET_KEY` | No | S3 secret key |
| `VIEWER_CACHE_TTL_SECONDS` | No | Cache TTL in seconds (default: 60) |

## Building

```bash
docker build -t data-lake-viewer ./viewer
```

## Running

```bash
docker run -p 8080:8080 \
  -e PIPELINE_S3_URL=s3://my-bucket/data \
  -e PIPELINE_S3_ENDPOINT_URL=http://localhost:9000 \
  -e PIPELINE_S3_ACCESS_KEY=minioadmin \
  -e PIPELINE_S3_SECRET_KEY=minioadmin \
  data-lake-viewer
```

Access the viewer at http://localhost:8080

## Local Development

Run frontend and backend separately:

```bash
# Terminal 1 - Frontend
cd viewer/frontend
npm install
npm run dev

# Terminal 2 - Backend
cd viewer
uv sync
uv run uvicorn backend.main:app --reload --port 8080
```

## Running Tests

```bash
cd viewer
uv sync --dev
uv run pytest tests -v
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Get pipeline status summary |
| POST | `/api/cache/invalidate` | Invalidate all cached data |
| GET | `/api/landing/status` | Get landing zone status by source |
| GET | `/api/landing/{source}/{filename}` | Get records from a landing file |
| GET | `/api/manifests` | List all manifests (optional `state` filter) |
| GET | `/api/manifests/{batch_id}` | Get a specific manifest by batch ID |
| GET | `/api/processed` | List processed parquet files |
| POST | `/api/query` | Execute a SELECT query on processed data |
| GET | `/api/browse` | Browse objects by prefix |

## Architecture

Single-container design with:
- **Frontend**: React + Vite, served as static files by the backend
- **Backend**: FastAPI with DuckDB for querying parquet files directly from S3
- **Cache**: In-memory cache with configurable TTL for S3 metadata operations
- **Dependency Management**: uv for fast, reproducible Python dependency resolution

The frontend is built during the Docker image build and embedded in the container. All data flows through the FastAPI backend which communicates with S3 storage.
