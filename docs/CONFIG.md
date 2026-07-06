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
web_remote_file_default_max_bytes: 262144
web_remote_file_hard_max_bytes: 1048576
web_remote_command_timeout_seconds: 5
web_remote_read_concurrency: 2
web_remote_read_cache_seconds: 3
web_timeout_keep_alive_seconds: 5
web_timeout_graceful_shutdown_seconds: 15
web_limit_concurrency: 64
ssh_command_timeout_seconds: 30
ssh_slow_command_timeout_seconds: 300
scheduler_watchdog_enabled: true
scheduler_watchdog_stall_seconds: 0
scheduler_ssh_parallelism: 4
reconcile_on_start: true
backup_enabled: true
backup_interval_seconds: 86400
backup_keep: 7
backup_dir: "data/backups"
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
allocation_max_new_per_loop: 8
cpu_pool_allow_gpu_partitions: true
warm_pool_preferred_accounts: ["account_a"]
gpu_warm_pool_preferred_accounts: ["account_a"]
single_job_per_node_partitions: ["cpu2"]
cpu_partition_allocation_limits:
  cpu2: 2
gpu_cpu_reserve: 4
fea_bursty:
  soft_memory_free_percent: 60
  hard_memory_free_percent: 40
  load_target: 0.75
  max_attach_per_loop: 24
  node_name_policy: preferred
cleanup:
  enabled: true
  interval_seconds: 3600
  finished_task_ttl_seconds: 259200
  finished_job_ttl_seconds: 259200
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
- `web_remote_file_default_max_bytes`: default maximum bytes returned by web log/remote-file endpoints when the request does not specify `max_bytes`.
- `web_remote_file_hard_max_bytes`: absolute response cap for web log/remote-file endpoints. Set to `0` to disable the hard cap.
- `web_remote_command_timeout_seconds`: timeout for web-triggered SSH log/remote-file/glob commands. Set to `0` to disable.
- `web_remote_read_concurrency`: maximum simultaneous web-triggered SSH log/remote-file/glob reads. Extra requests return `429` quickly instead of blocking the dashboard.
- `web_remote_read_cache_seconds`: short TTL cache for repeated remote log/file reads.
- `web_timeout_keep_alive_seconds`, `web_timeout_graceful_shutdown_seconds`, `web_limit_concurrency`: Uvicorn stability limits for slow VPN/client connections and request bursts.
- `ssh_command_timeout_seconds`: default deadline for every scheduler SSH command. A hung remote raises `RemoteCommandTimeout` instead of blocking the scheduler loop forever.
- `ssh_slow_command_timeout_seconds`: deadline for legitimately slow remote work (`du -sk` storage scans, `git clone` during submit).
- `scheduler_watchdog_enabled` / `scheduler_watchdog_stall_seconds`: a watchdog thread force-closes SSH transports when a tick stalls past the threshold (`0` = auto `max(300, 3 * poll_interval_seconds)`), then exits the process for the supervisor restart loop if the tick is still stuck one interval later.
- `scheduler_ssh_parallelism`: bounded thread pool for per-account state refreshes so one slow account does not serialize the others.
- `reconcile_on_start`: on startup, cancel Slurm jobs named `pool` that the database no longer tracks (orphaned warm pools after a DB reset or lost rows).
- `backup_*`: online SQLite backup into `backup_dir` every `backup_interval_seconds`, keeping the newest `backup_keep` files.
- `min_warm_allocations`: minimum CPU warm allocation count.
- `allocation_partition`: `auto` lets the scheduler rank partitions from inventory.
- `allocation_cpus`: CPU target/cap for shared CPU pool allocations. CPU-only nodes avoid tiny fragments by requiring this many usable CPUs when the node can provide it, smaller CPU-only nodes use their full node size, and GPU nodes use their currently free CPUs after leaving `gpu_cpu_reserve` cores unrequested for other GPU users.
- `allocation_memory`: memory for CPU warm pool allocations. `0` means Slurm partition default/all available behavior depending on cluster policy.
- `allocation_attach_stop_before_drain_seconds`: stop attaching new tasks to an allocation this many seconds before `allocation_drain_after_seconds`.
- `allocation_pending_timeout_seconds`: how long an allocation job may stay Slurm `PENDING` before the scheduler cancels it.
- `allocation_pending_backoff_seconds`: cooldown before the same resource pool is submitted again after a pending timeout.
- `allocation_max_new_per_loop`: maximum demand allocations the scheduler may submit in one loop while walking queued tasks.
- `cpu_pool_allow_gpu_partitions`: allows CPU pools to use GPU partitions when their CPU profile is stronger.
- `warm_pool_preferred_accounts`: preferred accounts for CPU pools. This is preference, not a hard lock.
- `gpu_warm_pool_preferred_accounts`: preferred accounts for GPU pools.
- `single_job_per_node_partitions`: partitions where the scheduler should pin an idle node and avoid more than one scheduler job per node.
- `cpu_partition_allocation_limits`: maximum live CPU pool allocations per physical node for matching partitions. The default `cpu2: 2` allows multiple CPU2 nodes while avoiding more than two scheduler CPU pools on one lower-memory-per-core CPU2 node.
- `gpu_cpu_reserve`: CPU cores left unrequested on GPU nodes. CPU pools always apply this reserve. GPU warm allocations apply it when they leave some GPUs unclaimed, so the remaining GPUs still have CPU available for other users. A6000 task allocations keep at least 4 CPU cores per requested GPU unless a GPU warm pool CPU override is configured.
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
  node_name_policy: preferred
  overload_scale_out_load_factor: 2.0
  overload_scale_out_seconds: 300
  pressure_max_attempts: 3
  max_attach_per_node_per_loop: 8
```

Meanings:

- `soft_memory_free_percent`: stop attaching new `fea_bursty` tasks when the allocation node's `pestat` free memory is below this percentage.
- `hard_memory_free_percent`: cancel the newest running `fea_bursty` task on each pressured allocation when free memory drops below this percentage. The task is requeued (not failed) so the simulation reruns elsewhere, up to `pressure_max_attempts`.
- `pressure_max_attempts`: how many memory-pressure kills a task survives before it is marked failed for good.
- `max_attach_per_node_per_loop`: per-node cap on new FEA attaches in one tick. Together with the attach ledger (budgets are discounted by attaches issued since the last `pestat` snapshot), this prevents dogpiling one node on stale data.
- `load_target`: attach only while `pestat` CPU load is at or below `cpu_total * load_target`. For `fea_bursty`, the scheduler may exceed a task's `max_workers_per_node` baseline when both load budget and free-memory budget are still healthy.
- `max_attach_per_loop`: maximum new `fea_bursty` tasks the scheduler starts in one tick.
- `node_name_policy`: `preferred` treats `node_name` on CPU `fea_bursty` tasks as a preferred node with healthy-node fallback; `strict` preserves exact-node matching.
- `overload_scale_out_load_factor`: when scheduler-owned running/attaching FEA requested CPU on a physical node is greater than owned CPU by this factor, the node is considered overloaded for FEA scale-out. This does not use node-wide `pestat` load, because that includes other users.
- `overload_scale_out_seconds`: sustained overload duration before opening one additional CPU pool for FEA distribution.

## Cleanup Config

```yaml
cleanup:
  enabled: true
  interval_seconds: 3600
  finished_task_ttl_seconds: 259200
  finished_job_ttl_seconds: 259200
  closed_allocation_ttl_seconds: 86400
  orphan_sweep_enabled: true
  orphan_sweep_interval_seconds: 86400
  orphan_min_age_seconds: 604800
  db_row_ttl_seconds: 1209600
  event_ttl_seconds: 604800
```

Meanings:

- `enabled`: turns automatic cleanup on or off.
- `interval_seconds`: how often the scheduler scans for old artifacts.
- `finished_task_ttl_seconds`: how long completed, failed, or cancelled attached-task directories are kept. Default is 3 days.
- `finished_job_ttl_seconds`: how long completed, failed, or cancelled direct-job directories are kept. Default is 3 days.
- `closed_allocation_ttl_seconds`: how long closed allocation directories are kept. Default is 1 day.
- `orphan_sweep_*`: a daily sweep lists `task-*`/`job-*`/`allocation-*` (and `env-sync/job-*`) directories in each account workspace and removes the ones no database row references and whose mtime is older than `orphan_min_age_seconds` (default 7 days). This catches directories left behind by DB resets, deleted rows, or wedged tasks that the TTL cleanups above cannot see.
- `db_row_ttl_seconds`: terminal task/job/allocation rows whose remote directories were already cleaned are deleted from the database after this long (default 14 days), followed by a WAL checkpoint. Older run history disappears from the finished lists after this window.
- `event_ttl_seconds`: retention for `scheduler_events` rows.

Cleanup clears the DB log path fields after deleting the remote directory. If a client needs stdout, stderr, or result files, read them through the API before the TTL expires.

Conda env-sync also cleans after itself: the remote pack/install directories are deleted on success, only the newest `<env>.bak.<timestamp>` backup is kept per environment, and orphaned local sync tarballs are removed at service startup.

## GPU Prewarm Config

```yaml
gpu_prewarm:
  enabled: true
  preferred_models: ["a6000"]
  min_warm_allocations: 2
  max_warm_allocations: 4
  gpus_per_allocation: 4
  min_gpus_per_allocation: 4
  cpus_per_allocation: 4
  cpu_reserve_per_free_gpu: 8
  stagger_seconds: 86400
  memory: "128G"
  partition: "auto"
  time_limit: "48:00:00"
  pinned_pending_timeout_seconds: 300
```

Meanings:

- `preferred_models`: GPU warm pool model allow-list and priority. The default keeps warm pools on A6000 only.
- `min_warm_allocations`: number of full-size GPU warm allocations to keep in rotation even before GPU work arrives.
- `max_warm_allocations`: upper bound for scheduler-owned GPU allocations, including warm pools that have since been used by tasks.
- `gpus_per_allocation`: target GPUs requested per GPU warm allocation. The default is `4`, so each A6000 warm allocation tries to hold a full 4-GPU node.
- `min_gpus_per_allocation`: minimum GPU count for a warm allocation. The default is `4`, so A6000 warm placement does not open partial-GPU warm pools.
- `cpus_per_allocation`: optional CPU override for GPU warm allocations. The default config requests 4 CPU cores for a 4-GPU A6000 warm pool.
- `cpu_reserve_per_free_gpu`: CPU cores reserved inside GPU allocations for future GPU tasks.
- `stagger_seconds`: minimum spacing before opening another full-size warm allocation for the same GPU model. The default is one day, so two A6000 warm pools do not expire together.
- `memory`: memory requested by GPU warm allocations. The default is `128G`, so a 4-GPU A6000 warm pool has enough RAM for service workloads.
- `partition`: `auto` lets scheduler choose from live capacity.
- `time_limit`: Slurm time limit for GPU warm allocations.
- `pinned_pending_timeout_seconds`: if a node-pinned GPU warm allocation stays pending this long, the scheduler cancels that pinned request, temporarily avoids the node, and tries another fitting node or an unpinned partition request.

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
- CPU warm pools avoid CPU-only fragments: `allocation_cpus` is the default target/cap, smaller CPU-only nodes use their full node size, and GPU nodes can contribute current free CPU after reserve.
- `cpu2` is treated as one scheduler job per node.
- A6000 is the default GPU prewarm target. A6000ADA is intentionally not used for warm pools by default because it is usually fully occupied.
- GPU capacity decisions use live `GresUsed` when `scontrol -o show nodes` is available.

Do not commit local `config/app.yaml`, `config/accounts.yaml`, `data/`, or `secrets/`.
