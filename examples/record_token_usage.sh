#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"
PROVIDER=${PROVIDER:-codex}
PROJECT=${PROJECT:-slurm_scheduler}
INPUT_TOKENS=${INPUT_TOKENS:-1000}
OUTPUT_TOKENS=${OUTPUT_TOKENS:-500}
RESET_CYCLE=${RESET_CYCLE:-2026-W24}
NOTE=${NOTE:-example token usage record}

curl -sS -X POST "$SCHEDULER_URL/token-usage" \
  -F "provider=$PROVIDER" \
  -F "project=$PROJECT" \
  -F "input_tokens=$INPUT_TOKENS" \
  -F "output_tokens=$OUTPUT_TOKENS" \
  -F "reset_cycle=$RESET_CYCLE" \
  -F "note=$NOTE" \
  -D - \
  -o /dev/null
