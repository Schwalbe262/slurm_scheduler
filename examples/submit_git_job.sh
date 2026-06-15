#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
JOB_NAME=${JOB_NAME:-git-cpu-demo}
REPO_URL=${REPO_URL:-https://github.com/example/project.git}
GIT_REF=${GIT_REF:-main}
ENTRYPOINT=${ENTRYPOINT:-scripts/run.py}
ARGUMENTS=${ARGUMENTS:---case demo}
CPUS=${CPUS:-4}
MEMORY=${MEMORY:-8G}
GPUS=${GPUS:-0}

curl -sS -X POST "$SCHEDULER_URL/tasks/git" \
  -F "job_name=$JOB_NAME" \
  -F "repo_url=$REPO_URL" \
  -F "git_ref=$GIT_REF" \
  -F "entrypoint=$ENTRYPOINT" \
  -F "arguments=$ARGUMENTS" \
  -F "cpus=$CPUS" \
  -F "memory=$MEMORY" \
  -F "gpus=$GPUS" \
  -D - \
  -o /dev/null
