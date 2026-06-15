#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
TASK_NAME=${TASK_NAME:-fea-case-001}
REMOTE_CWD=${REMOTE_CWD:-/remote/project/path}
TASK_COMMAND=${TASK_COMMAND:-python run_fea.py --case case001 --out results/case001.json}
ACCOUNT_NAME=${ACCOUNT_NAME:-}
CPUS=${CPUS:-4}
MEMORY_MB=${MEMORY_MB:-8192}

curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F "name=$TASK_NAME" \
  -F "remote_cwd=$REMOTE_CWD" \
  -F "command=$TASK_COMMAND" \
  -F "account_name=$ACCOUNT_NAME" \
  -F "cpus=$CPUS" \
  -F "memory_mb=$MEMORY_MB" \
  -F gpus=0 \
  -D - \
  -o /dev/null
