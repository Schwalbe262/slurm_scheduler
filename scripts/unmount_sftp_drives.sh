#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="${SFTP_MOUNT_BASE:-$HOME/mnt}"

for path in "$BASE_DIR/share_a" "$BASE_DIR/share_b"; do
  if mountpoint -q "$path"; then
    fusermount3 -u "$path"
    echo "unmounted $path"
  fi
done
