# Viewer bare deployment runbook (mcgill-rr-storage)

Topology: static frontend on Cloudflare Pages (`data-lake-viewer` project, custom domain
`data-lake-viewer.ai4.institute`), FastAPI backend bare on mcgill-rr-storage
(sm-data-rr.cs.mcgill.ca, user jtian15) behind the machine's remotely-managed cloudflared
tunnel at `api-data-lake-viewer.ai4.institute` → `http://localhost:8000`. Redis (built
from source) at `127.0.0.1:6379`. The old Docker stack on the Dashboard VM is the fallback.

## Layout on the machine

- Repo checkout: `~/20260826-data-lake-pipeline` (NOT a git repo — rsync'd from the workstation).
- Backend env: `viewer/backend/.env` (loaded automatically via `load_dotenv`). Built by
  copying `~/20260826-data-lake-pipeline/.env` (S3_* point at `http://localhost:20001`)
  and appending `REDIS_URL=redis://127.0.0.1:6379/0`,
  `DUCKDB_CACHE_DIR=/mnt/projects/jtian15/viewer-duckdb-cache`, and
  `VIEWER_CORS_ORIGINS=https://data-lake-viewer.ai4.institute,https://data-lake-viewer.pages.dev`.
- Venv: `/mnt/projects/jtian15/venvs/dlp` (Python 3.13; shared with the conversion job).
- Redis: `/mnt/projects/jtian15/redis` (build with `scripts/build-redis.sh`),
  data in `/mnt/projects/jtian15/redis-data` (AOF on, RDB off).
- DuckDB cache: `/mnt/projects/jtian15/viewer-duckdb-cache` (orderings, httpfs cache).

## Deploy / update

```bash
# from the workstation
rsync -av --exclude '.env' --exclude 'backend/.env' --exclude 'frontend/node_modules' \
  --exclude 'frontend/dist' --exclude '.venv' \
  viewer/ mcgill-rr-storage:20260826-data-lake-pipeline/viewer/

# on the machine, restart the backend
systemctl --user restart viewer-backend        # if linger is enabled
# or, tmux fallback:
tmux kill-session -t viewer-backend 2>/dev/null || true
tmux new -d -s viewer-backend 'cd ~/20260826-data-lake-pipeline/viewer && \
  /mnt/projects/jtian15/venvs/dlp/bin/uvicorn backend.main:app \
  --host 127.0.0.1 --port 8000 --proxy-headers --forwarded-allow-ips=*'
```

## Frontend deploy (Cloudflare Pages, wrangler from the workstation)

```bash
cd viewer/frontend
npm ci
VITE_API_BASE=https://api-data-lake-viewer.ai4.institute npm run build
npx wrangler pages deploy dist --project-name=data-lake-viewer --branch=main
```

## Smoke checks

```bash
curl -s http://127.0.0.1:8000/api/datasets                     # on the machine
curl -s https://api-data-lake-viewer.ai4.institute/api/datasets
curl -sN --max-time 25 'https://api-data-lake-viewer.ai4.institute/api/events?dataset='
/mnt/projects/jtian15/redis/bin/redis-cli ping                # PONG
```

## Cutover / rollback

- Cutover: remove `data-lake-viewer.ai4.institute` from the VM's tunnel (Zero Trust
  dashboard), then add it as a Pages custom domain. Brief 522 window between the two.
- Rollback: reverse both dashboard changes (VM stack is never stopped, so re-adding the
  hostname to the VM tunnel → `http://localhost:8080` restores the old UI instantly).

## Supervision

systemd user units (`systemd/viewer-*.service` → `~/.config/systemd/user/`) give
auto-restart and reboot survival **only if an admin has run
`loginctl enable-linger jtian15`** (Linger=no by default). Until then the tmux sessions
`viewer-redis` / `viewer-backend` are the running mechanism (survive logout, not reboot).
Check: `loginctl show-user jtian15 -p Linger`.

## Tests

Run the viewer backend suite on the machine against a throwaway MinIO bucket — NEVER the
production bucket:

```bash
cd ~/20260826-data-lake-pipeline/viewer
S3_ENDPOINT_URL=http://localhost:20001 S3_ACCESS_KEY=cdl S3_SECRET_KEY=cdl-rrabba \
S3_BUCKET=test-bucket S3_PREFIX=datasets TMPDIR=/tmp env -u REDIS_URL \
/mnt/projects/jtian15/venvs/dlp/bin/pytest backend/tests -q
```
