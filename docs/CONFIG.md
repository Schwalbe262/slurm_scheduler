# Configuration Guide

Korean summary: 실제 운영 설정은 Git에 올리지 않는 `config/app.yaml`과 `config/accounts.yaml`에 둡니다. 예제 파일은 안전한 placeholder만 포함합니다.

## Files

- `config/app.example.yaml`: sanitized scheduler policy defaults.
- `config/accounts.example.yaml`: sanitized account template.
- `config/app.yaml`: local runtime config, ignored by Git.
- `config/accounts.yaml`: local account/secret config, ignored by Git.

Create local files:

```bash
cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
```

## App Config

Important fields:

```yaml
database_path: "data/slurm_scheduler.db"
accounts_path: "config/accounts.yaml"
poll_interval_seconds: 30
bind_host: "127.0.0.1"
bind_port: 8000
cluster_refresh_interval_seconds: 120
min_warm_allocations: 1
allocation_partition: "auto"
allocation_cpus: 64
allocation_memory: "0"
allocation_time_limit: "48:00:00"
allocation_attach_stop_before_drain_seconds: 1800
allocation_pending_timeout_seconds: 1800
allocation_pending_backoff_seconds: 1800
allocation_reserved_job_slots: 0
cpu_pool_allow_gpu_partitions: true
warm_pool_preferred_accounts: ["account_a"]
gpu_warm_pool_preferred_accounts: ["account_a"]
single_job_per_node_partitions: ["cpu2"]
gpu_cpu_reserve: 4
fea_bursty:
  soft_memory_free_percent: 60
  hard_memory_free_percent: 40
  load_target: 0.75
  max_attach_per_loop: 8
cleanup:
  enabled: true
  interval_seconds: 3600
  finished_task_ttl_seconds: 604800
  finished_job_ttl_seconds: 604800
  closed_allocation_ttl_seconds: 86400
git_credentials:
  - id: private-project
    url_patterns: ["*org/private-project*"]
    clone_url: "git@github.com:org/private-project.git"
    source_account: "account_a"
    source_private_key_path: "~/.ssh/private_project_deploy"
    strict_host_key_checking: "accept-new"
```

Field meanings:

- `cluster_refresh_interval_seconds`: how often the background loop refreshes Slurm node inventory and `pestat` data.
- `min_warm_allocations`: minimum CPU warm allocation count.
- `allocation_partition`: `auto` lets the scheduler rank partitions from inventory.
- `allocation_cpus`: CPU cores requested by CPU warm pool allocations. Keep this at or below the per-node cluster/QOS limit.
- `allocation_memory`: memory for CPU warm pool allocations. `0` means Slurm partition default/all available behavior depending on cluster policy.
- `allocation_attach_stop_before_drain_seconds`: stop attaching new tasks to an allocation this many seconds before `allocation_drain_after_seconds`.
- `allocation_pending_timeout_seconds`: how long an allocation job may stay Slurm `PENDING` before the scheduler cancels it.
- `allocation_pending_backoff_seconds`: cooldown before the same resource pool is submitted again after a pending timeout.
- `cpu_pool_allow_gpu_partitions`: allows CPU pools to use GPU partitions when their CPU profile is stronger.
- `warm_pool_preferred_accounts`: preferred accounts for CPU pools. This is preference, not a hard lock.
- `gpu_warm_pool_preferred_accounts`: preferred accounts for GPU pools.
- `single_job_per_node_partitions`: partitions where the scheduler should pin an idle node and avoid more than one scheduler job per node.
- `gpu_cpu_reserve`: CPU cores left unrequested on GPU nodes. CPU pools always apply this reserve. GPU warm allocations apply it when they leave some GPUs unclaimed, so the remaining GPUs still have CPU available for other users.
- `fea_bursty`: adaptive attached-task policy for bursty FEA workloads.
- `cleanup`: automatic removal of scheduler-created remote artifact directories. Only paths under each account's `remote_workspace` whose basename starts with `task-`, `job-`, or `allocation-` are deleted.
- `git_credentials`: central Git credentials for `/tasks/git`. `source_account` can point at a master account that already has a read-only deploy key; the scheduler reads that key and injects it into each task's temporary directory, so target accounts do not need GitHub SSH setup.

## FEA Bursty Scheduling Config

```yaml
fea_bursty:
  soft_memory_free_percent: 60
  hard_memory_free_percent: 40
  load_target: 0.75
  max_attach_per_loop: 8
```

Meanings:

- `soft_memory_free_percent`: stop attaching new `fea_bursty` tasks when the allocation node's `pestat` free memory is below this percentage.
- `hard_memory_free_percent`: fail and cancel the newest running `fea_bursty` task on a pressured allocation when free memory drops below this percentage.
- `load_target`: attach only while `pestat` CPU load is at or below `cpu_total * load_target`.
- `max_attach_per_loop`: maximum new `fea_bursty` tasks the scheduler starts in one tick.

## Cleanup Config

```yaml
cleanup:
  enabled: true
  interval_seconds: 3600
  finished_task_ttl_seconds: 604800
  finished_job_ttl_seconds: 604800
  closed_allocation_ttl_seconds: 86400
```

Meanings:

- `enabled`: turns automatic cleanup on or off.
- `interval_seconds`: how often the scheduler scans for old artifacts.
- `finished_task_ttl_seconds`: how long completed, failed, or cancelled attached-task directories are kept. Default is 7 days.
- `finished_job_ttl_seconds`: how long completed, failed, or cancelled direct-job directories are kept. Default is 7 days.
- `closed_allocation_ttl_seconds`: how long closed allocation directories are kept. Default is 1 day.

Cleanup clears the DB log path fields after deleting the remote directory. If a client needs stdout, stderr, or result files, read them through the API before the TTL expires.

## GPU Prewarm Config

```yaml
gpu_prewarm:
  enabled: true
  preferred_models: ["a6000ada", "a6000"]
  min_warm_allocations: 1
  max_warm_allocations: 3
  gpus_per_allocation: 4
  min_gpus_per_allocation: 2
  cpu_reserve_per_free_gpu: 8
  partition: "auto"
  time_limit: "48:00:00"
```

Meanings:

- `preferred_models`: GPU warm pool model allow-list and priority. The default keeps warm pools on A6000ADA or A6000 only.
- `min_warm_allocations`: number of GPU allocations to keep warm even before GPU work arrives.
- `max_warm_allocations`: demand-based upper bound for scheduler-owned GPU allocations.
- `gpus_per_allocation`: target GPUs requested per GPU warm allocation. The default is `4`, so each GPU warm allocation tries to hold a full four-GPU node.
- `min_gpus_per_allocation`: minimum GPU count for a warm allocation. The default is `2`, so warm placement may take four, three, or two GPUs on an A6000-class node, but not one.
- `cpu_reserve_per_free_gpu`: CPU cores reserved inside GPU allocations for future GPU tasks.
- `partition`: `auto` lets scheduler choose from live capacity.
- `time_limit`: Slurm time limit for GPU warm allocations.

## Account Config

Example:

```yaml
accounts:
  - name: account_a
    host: login.example.edu
    port: 22
    username: account_a
    private_key_path: "secrets/cluster/account_a.pem"
    remote_workspace: "slurm_scheduler"
    max_running_jobs: 10
    max_pending_jobs: 10
    max_total_jobs: 10
    storage_path: "slurm_scheduler"
    storage_quota_gb: 0
    partition_allowlist: []
    capabilities: ["conda:pyaedt2026v1"]
    env_profiles:
      pyaedt2026v1: |
        source ~/miniconda3/etc/profile.d/conda.sh
        conda activate pyaedt2026v1
```

Field meanings:

- `name`: scheduler-visible account name.
- `host`, `port`, `username`, `private_key_path`: SSH connection information.
- `remote_workspace`: scheduler workspace path on that account.
- `max_running_jobs`, `max_pending_jobs`, `max_total_jobs`: account job-slot limits enforced before submission.
- `storage_path`, `storage_quota_gb`: optional storage usage monitoring.
- `partition_allowlist`: optional restriction to allowed partitions.
- `capabilities`: labels used by tasks/jobs to require account-local software.
- `env_profiles`: named shell snippets prepended before `env_setup` and `command`.

## Environment Profiles

Use profiles when one account has a prepared environment and other accounts do not.

```yaml
capabilities:
  - "conda:pyaedt2026v1"
  - "conda:pytorch_cuda118"
  - "conda:flight-searcher"
env_profiles:
  pyaedt2026v1: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate pyaedt2026v1
  pytorch_cuda118: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate pytorch_cuda118
  flight-searcher: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate flight-searcher
  factorio_vllm: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate pytorch_cuda118
    export VLLM_USE_FLASHINFER_SAMPLER=0
```

Then submit with:

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=env-profile-demo \
  -F remote_cwd=/remote/project/path \
  -F command='python run.py' \
  -F required_capability=conda:pytorch_cuda118 \
  -F env_profile=pytorch_cuda118 \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada
```

## Example Policy Defaults

- CPU warm pool account priority: `account_a` first in example config.
- GPU warm pool account priority: `account_a` first in example config.
- CPU warm pool allocation size: 64 CPU cores.
- `cpu2` is treated as one scheduler job per node.
- A6000ADA is preferred over A6000 for GPU prewarm.
- GPU capacity decisions use live `GresUsed` when `scontrol -o show nodes` is available.

Do not commit local `config/app.yaml`, `config/accounts.yaml`, `data/`, or `secrets/`.
