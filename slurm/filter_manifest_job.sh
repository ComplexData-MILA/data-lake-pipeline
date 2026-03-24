#!/bin/bash
#SBATCH --job-name=filter_manifest
#SBATCH --partition=cpu-only
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=00:15:00
#SBATCH --output=/shared/data_project/logs/filter_manifest_%j.out
#SBATCH --error=/shared/data_project/logs/filter_manifest_%j.err

set -euo pipefail

if [[ -f ".venv/bin/activate" ]]; then
  source .venv/bin/activate
fi

python scripts/update_filter_manifest.py
