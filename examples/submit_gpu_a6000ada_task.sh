#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
TASK_NAME=${TASK_NAME:-llm-a6000ada}
REMOTE_CWD=${REMOTE_CWD:-/remote/llm/project}
TASK_COMMAND=${TASK_COMMAND:-python run_inference.py --model /models/model-name --prompt-file prompts/input.txt}
REQUIRED_CAPABILITY=${REQUIRED_CAPABILITY:-conda:pytorch_cuda118}
ENV_PROFILE=${ENV_PROFILE:-pytorch_cuda118}
ACCOUNT_NAME=${ACCOUNT_NAME:-}
CPUS=${CPUS:-8}
MEMORY_MB=${MEMORY_MB:-32768}
GPU_MODEL=${GPU_MODEL:-a6000ada}

curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F "name=$TASK_NAME" \
  -F "remote_cwd=$REMOTE_CWD" \
  -F "command=$TASK_COMMAND" \
  -F "required_capability=$REQUIRED_CAPABILITY" \
  -F "env_profile=$ENV_PROFILE" \
  -F "account_name=$ACCOUNT_NAME" \
  -F "cpus=$CPUS" \
  -F "memory_mb=$MEMORY_MB" \
  -F gpus=1 \
  -F "gpu_model=$GPU_MODEL" \
  -F partition=auto \
  -D - \
  -o /dev/null
