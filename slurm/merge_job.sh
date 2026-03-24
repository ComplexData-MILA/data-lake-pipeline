#!/bin/bash
#SBATCH --job-name=merge_server
#SBATCH --partition=cpu-only
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=/shared/data_project/logs/merge_server_%j.out
#SBATCH --error=/shared/data_project/logs/merge_server_%j.err

set -euo pipefail

if [[ -f ".venv/bin/activate" ]]; then
  source .venv/bin/activate
fi

if [[ -n "${BATCH_ID:-}" ]]; then
  merge-server --once --batch-id "$BATCH_ID"
else
  merge-server --once --max-concurrent 4
fi
