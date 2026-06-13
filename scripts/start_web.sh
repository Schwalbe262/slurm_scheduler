#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export SLURM_SCHEDULER_CONFIG="${SLURM_SCHEDULER_CONFIG:-config/app.yaml}"

if [ -n "${SLURM_SCHEDULER_PYTHON:-}" ]; then
  PYTHON="$SLURM_SCHEDULER_PYTHON"
elif [ -x "$ROOT_DIR/.venv/bin/python" ]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
elif [ -x /tmp/slurm_scheduler_smoke_venv/bin/python ]; then
  PYTHON="/tmp/slurm_scheduler_smoke_venv/bin/python"
else
  echo "No Python environment found. Run: bash scripts/setup_and_smoke.sh" >&2
  exit 1
fi

mkdir -p logs
exec "$PYTHON" -m slurm_scheduler
