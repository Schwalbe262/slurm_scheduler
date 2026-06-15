#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
JOB_NAME=${JOB_NAME:-fea-rl}
REMOTE_PATH=${REMOTE_PATH:-/remote/project/path}
ENTRYPOINT=${ENTRYPOINT:-scripts/run_fea.py}
ARGUMENTS=${ARGUMENTS:---campaign rl-loop-001}
PARTITION=${PARTITION:-auto}
TIME_LIMIT=${TIME_LIMIT:-48:00:00}
TOTAL_SIMULATIONS=${TOTAL_SIMULATIONS:-20}
CPUS_PER_SIMULATION=${CPUS_PER_SIMULATION:-4}
MEM_PER_SIMULATION_GB=${MEM_PER_SIMULATION_GB:-8}
MAX_WORKERS_PER_JOB=${MAX_WORKERS_PER_JOB:-20}
MAX_NEW_JOBS=${MAX_NEW_JOBS:-10}

curl -sS -X POST "$SCHEDULER_URL/jobs" \
  -F job_mode=dynamic_packed_srun \
  -F "remote_path=$REMOTE_PATH" \
  -F "entrypoint=$ENTRYPOINT" \
  -F "arguments=$ARGUMENTS" \
  -F "partition=$PARTITION" \
  -F "time_limit=$TIME_LIMIT" \
  -F "total_simulations=$TOTAL_SIMULATIONS" \
  -F "cpus_per_simulation=$CPUS_PER_SIMULATION" \
  -F "mem_per_simulation_gb=$MEM_PER_SIMULATION_GB" \
  -F "max_workers_per_job=$MAX_WORKERS_PER_JOB" \
  -F "max_new_jobs=$MAX_NEW_JOBS" \
  -F "job_name=$JOB_NAME" \
  -D - \
  -o /dev/null
