#!/bin/bash
#SBATCH --job-name=pipeline_stage
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --output=/shared/data_project/logs/stage_%j.out
#SBATCH --error=/shared/data_project/logs/stage_%j.err

set -euo pipefail

# Configuration
SGLANG_MODEL="${PIPELINE_MODEL:-Qwen/Qwen3.5-9B-Instruct}"
SGLANG_GPU_MEMORY_UTILIZATION="${SGLANG_GPU_MEMORY_UTILIZATION:-0.9}"
MAX_WAIT_SECONDS=300

# Find random available port
find_free_port() {
    python -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()"
}

PORT=$(find_free_port)
echo "Selected port: $PORT"

# Activate virtual environment
if [[ -f ".venv/bin/activate" ]]; then
    source .venv/bin/activate
fi

# Start SGLang server in background
echo "Starting SGLang server with model: $SGLANG_MODEL"
python -m sglang.launch_server \
    --model-path "$SGLANG_MODEL" \
    --port "$PORT" \
    --gpu-memory-utilization "$SGLANG_GPU_MEMORY_UTILIZATION" \
    --host 0.0.0.0 \
    &

SGLANG_PID=$!

# Wait for server to be ready
wait_for_server() {
    local url="http://localhost:$PORT/v1/models"
    local start_time=$(date +%s)

    while true; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo "SGLang server ready at port $PORT"
            return 0
        fi

        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [ $elapsed -ge $MAX_WAIT_SECONDS ]; then
            echo "ERROR: SGLang server did not become ready within ${MAX_WAIT_SECONDS}s"
            return 1
        fi

        echo "Waiting for SGLang server... (${elapsed}s)"
        sleep 5
    done
}

wait_for_server

# Set environment variables for OpenAI SDK
export OPENAI_API_KEY="sglang-dummy-key"
export OPENAI_BASE_URL="http://localhost:$PORT/v1"

# Run the stage processor
echo "Running stage processor: ${STAGE_NAME:-default}"
python scripts/process_stage.py --stage "${STAGE_NAME:-quality_filter}" --batch-id "${BATCH_ID:-}"

# Capture exit code
STAGE_EXIT_CODE=$?

# Cleanup: stop SGLang server
echo "Stopping SGLang server (PID: $SGLANG_PID)"
kill $SGLANG_PID 2>/dev/null || true
wait $SGLANG_PID 2>/dev/null || true

exit $STAGE_EXIT_CODE
