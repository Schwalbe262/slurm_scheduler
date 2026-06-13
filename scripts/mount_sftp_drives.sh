#!/usr/bin/env bash
set -euo pipefail

HOST="${SFTP_HOST:-}"
PORT="${SFTP_PORT:-22}"
USER_NAME="${SFTP_USER:-}"
BASE_DIR="${SFTP_MOUNT_BASE:-$HOME/mnt}"
REMOTE_A="${SFTP_REMOTE_A:-}"
REMOTE_B="${SFTP_REMOTE_B:-}"

MOUNT_A="$BASE_DIR/share_a"
MOUNT_B="$BASE_DIR/share_b"

if [ -z "$HOST" ] || [ -z "$USER_NAME" ] || [ -z "$REMOTE_A" ]; then
  echo "Set SFTP_HOST, SFTP_USER, and SFTP_REMOTE_A before mounting." >&2
  exit 1
fi

if ! command -v sshfs >/dev/null 2>&1; then
  echo "sshfs is not installed. Run: sudo apt update && sudo apt install sshfs" >&2
  exit 1
fi

mkdir -p "$MOUNT_A"

if ! mountpoint -q "$MOUNT_A"; then
  sshfs -p "$PORT" "$USER_NAME@$HOST:$REMOTE_A" "$MOUNT_A"
fi

if [ -n "$REMOTE_B" ]; then
  mkdir -p "$MOUNT_B"
  if ! mountpoint -q "$MOUNT_B"; then
    sshfs -p "$PORT" "$USER_NAME@$HOST:$REMOTE_B" "$MOUNT_B"
  fi
fi

echo "SFTP share A is mounted at $MOUNT_A"
if [ -n "$REMOTE_B" ]; then
  echo "SFTP share B is mounted at $MOUNT_B"
fi
