#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/slurm-scheduler.service"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is not available. Use Windows Task Scheduler instead." >&2
  exit 1
fi

mkdir -p "$SERVICE_DIR"
sed "s#%h/NEC/slurm_scheduler#$ROOT_DIR#g" "$ROOT_DIR/deploy/slurm-scheduler.service" > "$SERVICE_FILE"

systemctl --user daemon-reload
systemctl --user enable --now slurm-scheduler.service

echo "Installed user service: slurm-scheduler.service"
echo "Check status with: systemctl --user status slurm-scheduler.service"
