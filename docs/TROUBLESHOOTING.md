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

- Keep `allocation_cpus` at or below the per-node/QOS limit. It is the target/cap for shared CPU pools; the scheduler avoids opening tiny CPU-only fragments, uses smaller CPU-only node sizes when needed, and can use GPU-node free CPUs after `gpu_cpu_reserve`.
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
- Use A6000 for default GPU warm pools; A6000ADA is usually fully occupied on this cluster.
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
  preferred_models: ["a6000"]
  min_warm_allocations: 2
  max_warm_allocations: 4
  gpus_per_allocation: 4
  min_gpus_per_allocation: 4
  cpus_per_allocation: 4
  stagger_seconds: 86400
  pinned_pending_timeout_seconds: 300
```

If a node-pinned warm allocation remains pending, the scheduler retries a different node after `pinned_pending_timeout_seconds`. If an unpinned request repeatedly shows `(Resources)`, the 4-GPU/4-CPU A6000 warm shape may not fit current partitions. If it repeatedly shows `(Priority)`, the request is valid but queue priority is delaying it.

GPU warm fallback stays inside `preferred_models`. With the default config the scheduler keeps A6000 requests queued, but it does not open A6000ADA, RTX 3090, or A10 warm pools just because A6000 jobs are pending.

## Git Clone Fails Before Slurm Starts

Direct `/jobs` submissions clone and check out the repo before `sbatch`. If this pre-submit phase fails, Slurm stdout/stderr may not exist yet. Read the scheduler's submit logs instead:

```bash
curl -sS "$SCHEDULER_URL/api/jobs/<job_id>"
curl -sS "$SCHEDULER_URL/api/jobs/<job_id>/remote-file?base=remote_job_dir&path=submit.stderr.log"
curl -sS "$SCHEDULER_URL/api/jobs/<job_id>/remote-file?base=remote_job_dir&path=submit.stdout.log"
```

Common causes:

- `Host key verification failed`: configure `git_credentials.strict_host_key_checking: "accept-new"` or provide a `known_hosts_path` / `source_known_hosts_path`.
- `Permission denied (publickey)`: the configured deploy key is missing, unreadable from `source_account`, or not registered on the GitHub repo.
- `Could not resolve hostname github.com-...`: the task used an SSH alias from one account's `~/.ssh/config`. Add a `git_credentials` entry with `clone_url: "git@github.com:org/repo.git"` so the scheduler rewrites the clone to a canonical host and injects `GIT_SSH_COMMAND`.
- `Repository not found`: the deploy key is installed on the wrong repository or does not have read access.

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
- Requested `gpu_model`, `partition`, or `node_name` is too narrow. If multiple GPU models are acceptable, use an ordered list such as `gpu_model=a6000ada,a6000`.
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
