#!/usr/bin/env bash
set -euo pipefail

: "${SCHEDULER_URL:?Set SCHEDULER_URL, for example http://scheduler-host:8000}"

curl -sS "$SCHEDULER_URL/api/health"
printf '\n'
curl -sS "$SCHEDULER_URL/api/accounts/status"
printf '\n'
curl -sS "$SCHEDULER_URL/api/allocations"
printf '\n'
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
printf '\n'
