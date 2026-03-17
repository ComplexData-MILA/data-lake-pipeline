#!/bin/bash
#SBATCH --job-name=qwen_annotation
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --output=/shared/data_project/logs/slurm_qwen_%j.out
#SBATCH --error=/shared/data_project/logs/slurm_qwen_%j.err

set -euo pipefail

# Adjust to your cluster/module environment
if [[ -f ".venv/bin/activate" ]]; then
  source .venv/bin/activate
fi

python scripts/process_batch.py
