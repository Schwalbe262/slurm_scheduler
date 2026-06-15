# Recommended Improvements

Korean summary: 현재 프로젝트에서 추가로 개선하면 가장 효과가 큰 항목들입니다. 당장 사용에는 필수는 아니지만 운영 디버깅과 안정성을 크게 높입니다.

## 1. Placement Decision Trace

Record why the scheduler chose or rejected each account, partition, node, and allocation.

Useful fields:

- request type: CPU task, GPU task, CPU pool, GPU pool, direct job.
- candidate accounts and account-limit rejection reasons.
- candidate partitions/nodes and capacity rejection reasons.
- inventory timestamp used for the decision.
- final Slurm request shape.

This would make questions like "why did it choose gpu2?" answerable from the Web UI without reading logs.

## 2. Inventory Freshness Warnings

Show warnings when Slurm inventory or `pestat` rows are older than `cluster_refresh_interval_seconds`.

Recommended UI locations:

- dashboard account/capacity section.
- GPU capacity table.
- allocation pool table.

## 3. Dry-Run Placement API

Add a read-only endpoint that accepts hypothetical task resources and returns the placement decision trace without submitting work.

Example:

```bash
curl -sS -X POST "$SCHEDULER_URL/api/placement/dry-run" \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada \
  -F partition=auto
```

This is especially useful for LLM agents because they can check whether a request is likely to run before creating scheduler state.

## 4. Optional Authentication

The service currently assumes a trusted VPN/private network. If it is exposed beyond Tailscale or a trusted LAN, add authentication before exposing write endpoints.

Reasonable options:

- reverse proxy with SSO/basic auth;
- API token middleware for write endpoints;
- separate read-only and write-capable bind addresses.

## 5. Structured Event Log

Persist scheduler events in the database and expose them in the UI/API:

- allocation submitted, warmed, drained, closed;
- task assigned, started, completed, failed;
- inventory refresh failed;
- account limit reached;
- Slurm pending reason changed.

This would complement `note.md`, which is for development history rather than runtime history.
