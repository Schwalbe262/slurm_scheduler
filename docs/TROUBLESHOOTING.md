# Troubleshooting

Korean summary: 접속 문제는 Tailscale/WSL/서비스 상태를 먼저 보고, Slurm pending 문제는 `squeue`, `scontrol show job`, inventory freshness를 확인합니다.

Set the URL:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
```

## `/api/health` Fails

Check from the scheduler host:

```bash
systemctl --user status slurm-scheduler.service
journalctl --user -u slurm-scheduler.service -n 100 --no-pager
curl -sS http://127.0.0.1:8000/api/health
```

Check Tailscale:

```bash
tailscale status
tailscale ip -4
```

If local health works but remote health fails, the issue is usually Tailscale routing, Windows/WSL port exposure, firewall, or a stale portproxy rule.

## Browser Shows `404 Not Found`

Use the exact root URL:

```text
http://<scheduler-host>:8000/
```

API paths start with `/api/`, for example:

```text
http://<scheduler-host>:8000/api/health
```

If the root returns 404, confirm the process is this FastAPI app and not another service on port 8000:

```bash
ss -ltnp | rg ':8000'
systemctl --user status slurm-scheduler.service
```

## Service Does Not Restart After Reboot

For Linux user systemd:

```bash
loginctl enable-linger "$USER"
systemctl --user enable slurm-scheduler.service
systemctl --user status slurm-scheduler.service
```

For Windows/WSL startup:

```powershell
Get-ScheduledTask -TaskName SlurmSchedulerWeb
Get-ScheduledTaskInfo -TaskName SlurmSchedulerWeb
```

The scheduled task should run `scripts/start_web.sh` through WSL and write logs to `logs/web.log`.

## Slurm Reason: `(QOSMaxCpuPerNode)`

The request exceeds the CPU limit allowed by the partition/QOS for one node.

Actions:

- Keep `allocation_cpus` at or below the per-node/QOS limit. Current policy uses 64 for CPU pools.
- Lower `cpus`, `cpus_per_simulation`, or `max_workers_per_job` for the submitted job.
- Check the generated job request with `scontrol show job <jobid>`.

## Slurm Reason: `(Resources)`

Slurm cannot currently place the job on requested resources.

Common causes:

- The requested node is already occupied.
- The request pinned `--nodelist` too tightly.
- Stored inventory or `pestat` data is stale.
- Requested CPU/memory/GPU shape does not fit any currently available node.

Checks:

```bash
squeue -u <account>
scontrol show job <jobid>
scontrol -o show node <node_name>
```

Scheduler-side checks:

```bash
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
curl -sS "$SCHEDULER_URL/api/allocations"
```

Current policy avoids node pinning for CPU pools on normal partitions. It only pins nodes for `single_job_per_node_partitions` such as `cpu2`.

## Slurm Reason: `(Priority)`

The request is valid but waiting behind higher-priority jobs. This is common on busy GPU partitions.

Actions:

- Keep at least one GPU warm allocation configured if the workload needs GPUs frequently.
- Prefer A6000ADA first, then A6000 fallback.
- Avoid over-pinning `partition` and `node_name`.
- Check `cluster_free_gpus` and `scheduler_free_gpus` from `/api/gpu-capacity`.

## GPU Warm Allocation Stays Pending

The scheduler records Slurm pending reasons in the dashboard Allocation Pool table and `/api/allocations`.

Safeguards:

- A `PENDING` allocation older than `allocation_pending_timeout_seconds` is cancelled with `scancel`.
- The same resource pool, for example `gpu:a6000ada`, is put into `allocation_pending_backoff_seconds` cooldown before a replacement is submitted.
- Other pools can still run; a GPU cooldown does not block CPU warm pools.

Tuning:

```yaml
allocation_pending_timeout_seconds: 1800
allocation_pending_backoff_seconds: 1800
gpu_prewarm:
  gpus_per_allocation: 2
```

If pending reason repeatedly shows `(Resources)`, the two-GPU shape may not fit current nodes. If it repeatedly shows `(Priority)`, the request is valid but queue priority is delaying it.

When preferred A6000-class warm jobs are pending, the scheduler can still open a lower-priority GPU allocation if `gpu_prewarm.max_warm_allocations` has spare room. This keeps some GPU capacity ready while the preferred request remains queued.

## GPU Capacity Looks Wrong

GPU capacity is not just physical GPU count. It must account for GPUs already allocated by other users.

Check:

```bash
python3 scripts/refresh_inventory.py --account account_a
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

If `scontrol -o show nodes` is unavailable, inventory may fall back to `sinfo` and live `GresUsed` can be incomplete.

## Task Stays Queued

Check whether an allocation matches all constraints:

```bash
curl -sS "$SCHEDULER_URL/api/tasks"
curl -sS "$SCHEDULER_URL/api/allocations"
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

Common blockers:

- `required_capability` does not exist on any account.
- `env_profile` name is missing from the selected account config.
- Requested `gpu_model`, `partition`, or `node_name` is too narrow.
- Requested `cpus` or `memory_mb` is larger than free allocation capacity.
- Account job limits prevent scale-out.

If you are unsure about `memory_mb`, remember that it is a scheduling request and possible Slurm limit, not preallocated RAM. Too low can cause OOM failures; too high can leave capacity idle because fewer tasks fit in the warm allocation.

## Conda Or Module Environment Fails

Use account `env_profiles` instead of repeating setup in every task.

Check `config/accounts.yaml` locally:

```yaml
capabilities: ["conda:pytorch_cuda118"]
env_profiles:
  pytorch_cuda118: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate pytorch_cuda118
```

Submit with both fields:

```bash
-F required_capability=conda:pytorch_cuda118
-F env_profile=pytorch_cuda118
```

This prevents the scheduler from placing the task on an account without that environment.

## Token Usage Missing From Dashboard

Record input and output tokens separately:

```bash
curl -sS -X POST "$SCHEDULER_URL/token-usage" \
  -F provider=codex \
  -F project=slurm_scheduler \
  -F input_tokens=1000 \
  -F output_tokens=500 \
  -F reset_cycle=2026-W24 \
  -F note='run note'
```

Then check:

```bash
curl -sS "$SCHEDULER_URL/api/token-usage"
```
