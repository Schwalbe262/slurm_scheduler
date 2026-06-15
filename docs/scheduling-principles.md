# Scheduling Principles

The scheduler reduces Slurm queue overhead by keeping allocation jobs warm and attaching individual work with `srun --jobid`.

Korean summary: 이 스케줄러는 Slurm job을 매번 새로 열지 않고 warm allocation을 유지한 뒤 task를 붙여 실행합니다. CPU, GPU, mixed capacity를 분리해서 관리하지만 필요한 경우 안전하게 재사용합니다.

## Resource Pools

The scheduler manages three practical pools:

- CPU pool: warm allocations with `resource_pool=cpu`, placed on the best CPU candidates even if the partition also has GPUs.
- GPU pool: warm allocations such as `resource_pool=gpu:a6000ada`.
- Mixed capacity: spare CPU and memory inside GPU allocations that CPU-only tasks may borrow.

Direct Slurm jobs still exist for compatibility, but attached tasks are preferred for iterative workloads.

## Policy Matrix

| Workload | Preferred path | Placement policy |
| --- | --- | --- |
| Existing remote CPU command | `POST /tasks` | CPU allocation first, then borrowable GPU-allocation CPU |
| Existing remote GPU command | `POST /tasks` | Matching GPU allocation, model, partition, node, CPU, and memory |
| Git command | `POST /tasks/git` | Same as attached task after repo setup in account workspace |
| Many FEA/RL simulations | `POST /jobs` with `dynamic_packed_srun` | `pestat`-based allocation planning and worker ramping |
| Token accounting | `POST /token-usage` | Stored locally and shown in Web UI/API |

## Allocation Lifecycle

Allocation states:

- `pending`: submitted to Slurm but not running.
- `warm`: running and idle.
- `active`: at least one attached task is running.
- `draining`: too old for new work; existing work may finish.
- `closing`: scheduler has requested cancellation.
- `closed`: released.
- `failed`: submission or lifecycle management failed.

The scheduler keeps the configured minimum CPU warm allocation count. It also keeps the configured GPU prewarm count when GPU prewarm is enabled.

Warm allocation account selection can use preferred account lists:

- `warm_pool_preferred_accounts`: preferred accounts for CPU warm pools.
- `gpu_warm_pool_preferred_accounts`: preferred accounts for GPU warm pools.

The preference is not absolute. If the preferred account is at its job limit, lacks a required capability, or is otherwise unavailable, the scheduler chooses another eligible account.

For an exact account constraint, submit the task or job with `account_name`. For example, `account_name=account_a` means the request will only use existing allocations from `account_a` or open a new allocation under `account_a`; it will remain queued if that account has no job slot.

## Account Capabilities

Accounts may declare local software capabilities and environment profiles:

```yaml
capabilities: ["conda:pyaedt2026v1"]
env_profiles:
  pyaedt2026v1: |
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate pyaedt2026v1
```

Jobs and tasks may request `required_capability` and `env_profile`. A request only runs on accounts that declare the capability and profile. The profile commands are prepended to the generated Slurm or task script before the per-job `env_setup`.

## CPU Scheduling

CPU-only tasks use this order:

1. Warm or active CPU allocations with enough CPU and memory.
2. GPU allocations with borrowable CPU and enough memory.

Borrowable CPU in a GPU allocation is:

```text
free_cpus - (free_gpus * gpu_prewarm.cpu_reserve_per_free_gpu)
```

With the default reserve of 8, a GPU allocation with 1 free GPU and 32 free CPUs exposes only 24 CPUs to CPU-only tasks.

CPU pool placement is controlled by `cpu_pool_allow_gpu_partitions`. When it is true, `resource_pool=cpu` allocations are allowed on GPU partitions and are ranked by CPU profile score first. This lets the scheduler prefer `cpu2`, then CPU-strong GPU partitions such as `gpu5`, before weaker CPU partitions.

For non-single-job partitions, CPU pool allocations do not pin `#SBATCH --nodelist`; they request the selected partition, CPU count, and memory and let Slurm choose a currently available node. Single-job partitions such as `cpu2` still pin an idle node so the scheduler can enforce one job per node.

This means a CPU pool on a GPU partition is still a CPU pool. It is not a request by the end user to consume GPUs; it is a scheduler placement choice based on CPU quality and current capacity.

## Single-Job Nodes

Partitions in `single_job_per_node_partitions` are handled as one scheduler job per node. The current local policy includes `cpu2` because concurrent jobs on the same node can exhaust memory.

For those partitions the scheduler:

- avoids nodes already used by live scheduler allocations or submitted jobs;
- avoids nodes where `pestat` reports nonzero CPU use;
- assigns `node_name` before submission and emits `#SBATCH --nodelist=<node>`.

If no matching node is free, the job or warm pool request remains queued until a later scheduler tick.

## GPU Scheduling

GPU tasks require all of these to match:

- enough `free_gpus`
- enough `free_cpus`
- enough `free_memory_mb`
- requested `gpu_model`, if specified
- requested `partition`, if not `auto`
- requested `node_name`, if specified

If no matching allocation exists, the scheduler may submit another GPU allocation up to `gpu_prewarm.max_warm_allocations`.

## Capacity Signals

GPU capacity has five separate meanings:

- `cluster_total_gpus`: physical GPUs observed in Slurm inventory.
- `cluster_used_gpus`: GPUs allocated by all users according to Slurm `GresUsed`.
- `cluster_free_gpus`: GPUs not currently allocated and on usable nodes.
- `scheduler_owned_gpus`: GPUs held by scheduler allocation jobs.
- `scheduler_free_gpus`: GPUs held by the scheduler and not currently used by attached tasks.

Schedulers and agents should use `scheduler_free_gpus` for immediate placement and `cluster_free_gpus` for scale-out likelihood.

The scheduler refreshes Slurm inventory and `pestat` periodically through `cluster_refresh_interval_seconds`. Capacity decisions should not rely on stale rows, especially when choosing warm pool nodes.

If a pending Slurm job shows `(Resources)` on a pinned node, compare the job's `ReqNodeList` and `scontrol -o show node <node>` against the inventory timestamp. Stale inventory can make an already-occupied node look available.

## Scale Out And Scale In

Scale out happens when:

- the minimum CPU or GPU warm allocation count is not met;
- queued work cannot fit in pending, warm, or active allocations;
- CPU/memory utilization crosses the configured scale-out threshold and no spare CPU allocation exists.

Scale in happens when:

- a warm allocation is above the configured minimum for its pool;
- it has been idle longer than `allocation_scale_in_idle_seconds`.

Old allocations drain after `allocation_drain_after_seconds` and are force-cancelled after `allocation_force_cancel_after_seconds`.

## Documentation For Operators

- `docs/API.md`: endpoint and form-field reference.
- `docs/EXAMPLES.md`: copy-paste task/job/token examples.
- `docs/CONFIG.md`: config fields and policy defaults.
- `docs/TROUBLESHOOTING.md`: Slurm pending reasons and remote access issues.
- `docs/ROADMAP.md`: recommended next improvements.
