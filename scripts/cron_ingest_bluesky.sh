#!/bin/bash
cd /mnt/data-lake-pipeline
set -a
source .env
set +a
/home/ubuntu/.local/bin/uv run scripts/ingest_bluesky.py --limit 10
