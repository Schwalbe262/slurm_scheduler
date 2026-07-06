#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
JOB_NAME=${JOB_NAME:-git-cpu-demo}
REPO_URL=${REPO_URL:-https://github.com/example/project.git}
GIT_REF=${GIT_REF:-main}
ENTRYPOINT=${ENTRYPOINT:-scripts/run.py}
ARGUMENTS=${ARGUMENTS:---case demo}
REQUIRED_CAPABILITY=${REQUIRED_CAPABILITY:-}
ENV_PROFILE=${ENV_PROFILE:-}
ACCOUNT_NAME=${ACCOUNT_NAME:-}
PARTITION=${PARTITION:-auto}
CPUS=${CPUS:-4}
MEMORY=${MEMORY:-8G}
GPUS=${GPUS:-0}
GPU_MODEL=${GPU_MODEL:-}

curl -sS -X POST "$SCHEDULER_URL/tasks/git" \
  -F "job_name=$JOB_NAME" \
  -F "repo_url=$REPO_URL" \
  -F "git_ref=$GIT_REF" \
  -F "entrypoint=$ENTRYPOINT" \
  -F "arguments=$ARGUMENTS" \
  -F "required_capability=$REQUIRED_CAPABILITY" \
  -F "env_profile=$ENV_PROFILE" \
  -F "account_name=$ACCOUNT_NAME" \
  -F "partition=$PARTITION" \
  -F "cpus=$CPUS" \
  -F "memory=$MEMORY" \
  -F "gpus=$GPUS" \
  -F "gpu_model=$GPU_MODEL" \
  -D - \
  -o /dev/null
