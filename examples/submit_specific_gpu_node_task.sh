#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
TASK_NAME=${TASK_NAME:-llm-specific-node}
REMOTE_CWD=${REMOTE_CWD:-/remote/llm/project}
TASK_COMMAND=${TASK_COMMAND:-python run.py}
ACCOUNT_NAME=${ACCOUNT_NAME:-}
CPUS=${CPUS:-8}
MEMORY_MB=${MEMORY_MB:-32768}
GPU_MODEL=${GPU_MODEL:-a6000ada}
PARTITION=${PARTITION:-gpu3}
NODE_NAME=${NODE_NAME:-gpu-node-name}

curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F "name=$TASK_NAME" \
  -F "remote_cwd=$REMOTE_CWD" \
  -F "command=$TASK_COMMAND" \
  -F "account_name=$ACCOUNT_NAME" \
  -F "cpus=$CPUS" \
  -F "memory_mb=$MEMORY_MB" \
  -F gpus=1 \
  -F "gpu_model=$GPU_MODEL" \
  -F "partition=$PARTITION" \
  -F "node_name=$NODE_NAME" \
  -D - \
  -o /dev/null
