# GPU Scheduling

GPU scheduling is optimized for scarce shared GPUs. The scheduler should not wait until an LLM job arrives before trying to acquire GPU hardware.

Korean summary: GPU는 물리적으로 몇 개 있느냐보다 지금 다른 사용자가 얼마나 쓰고 있는지가 중요합니다. 이 문서는 A6000ADA/A6000 우선순위와 live capacity 해석 방법을 설명합니다.

## Model Priority

Default priority:

```text
a6000ada > a6000 > rtx3090 > a10
```

`a6000ada` means RTX 6000 Ada Generation. It is generally better than RTX A6000 for LLM inference, mixed precision AI, and rendering because it has newer Ada architecture cores, higher FP32 throughput, newer tensor cores, and higher memory bandwidth. Both A6000ADA and A6000 usually provide 48 GB VRAM.

Use A6000 only when:

- A6000ADA is unavailable;
- the workload explicitly requires A6000;
- a specific node or partition only has A6000;
- a rare multi-GPU workflow depends on RTX A6000 NVLink behavior.

## GRES Names

The scheduler normalizes common Slurm GPU names:

- `a6000ada`
- `rtx6000ada`
- `rtx_6000_ada`
- `a6000`
- `rtxa6000`
- `rtx3090`
- `a10`

If the cluster uses a new spelling, add it to `normalize_gpu_model()` in `slurm_scheduler/inventory.py`.

Task and job submissions may pass one GPU model or an ordered candidate list. For example, `gpu_model=a6000ada,a6000` prefers A6000 ADA and allows A6000 when that is the available matching capacity.

## Inventory And Usage

`scripts/refresh_inventory.py` first tries:

```bash
scontrol -o show nodes
```

This exposes both:

- `Gres`: total configured GPU resources.
- `GresUsed`: currently allocated GPU resources.

If `scontrol` is unavailable, the script falls back to `sinfo`, but then live GPU usage may be incomplete.

Refresh live CPU/memory load separately:

```bash
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
```

## Prewarm Policy

Default config:

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
  partition: "auto"
  time_limit: "48:00:00"
  pinned_pending_timeout_seconds: 300
```

The scheduler keeps two A6000 GPU warm allocations in rotation. By default each A6000 warm allocation requests four GPUs and 4 CPU cores. Additional same-model warm pools are staggered by `stagger_seconds` so two warm allocations do not age out together.

GPU warm pool fallback stays inside the configured models. With the default policy it queues A6000 only; it will not open A6000ADA, RTX 3090, or A10 warm pools just because A6000 jobs are pending.

GPU warm placement prioritizes holding the GPU, while keeping CPU requests small enough to fit busy nodes. The default A6000 warm pool overrides the normal A6000 task CPU floor and requests 4 CPU cores for a 4-GPU warm allocation.

When a 4-GPU A6000 warm allocation can fit a node immediately, the scheduler pins that node first. If no current node has enough free GPUs, CPU, and memory, it submits an unpinned `gpu4,gpu5` request so Slurm can start it on either A6000 partition. If a pinned request stays pending longer than `pinned_pending_timeout_seconds`, the scheduler cancels that pinned request, avoids the node briefly, and tries another current-fit node or the unpinned queue path.

Attached GPU tasks require enough free CPUs inside the selected allocation. CPU overlap is reserved for same-node CPU clients, FEA bursty tasks, and vLLM service tasks that need to coexist with localhost clients inside the same allocation. Other GPU tasks keep exclusive Slurm steps.

If an unpinned GPU warm allocation stays Slurm `PENDING` longer than `allocation_pending_timeout_seconds`, the scheduler normally cancels it and applies `allocation_pending_backoff_seconds` to that GPU pool before trying again. A6000 warm pools waiting with Slurm reason `(Priority)` are exempt and stay queued, because cancelling them would lose queue position. `(Resources)` pending still times out. The dashboard Allocation Pool reason column shows the Slurm pending reason.

The account preference is configured separately:

```yaml
gpu_warm_pool_preferred_accounts: ["account_a"]
```

That account is tried first for GPU warm pools because it can have the most prepared LLM/software environment, but scheduler job limits and required capabilities still take precedence.

## Capacity Interpretation

GPU capacity should be read as operational capacity, not hardware inventory only:

- `cluster_total_gpus`: GPUs physically observed in Slurm inventory.
- `cluster_used_gpus`: GPUs already allocated by all Slurm users.
- `cluster_free_gpus`: GPUs currently available according to Slurm `GresUsed`.
- `scheduler_owned_gpus`: GPUs already held by scheduler allocation jobs.
- `scheduler_free_gpus`: scheduler-owned GPUs not currently consumed by attached tasks.

For immediate GPU work, prefer `scheduler_free_gpus`. For demand-based scale-out, inspect `cluster_free_gpus` and pending allocation reasons.

If capacity looks wrong, refresh inventory and load:

```bash
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

The background scheduler also refreshes these signals every `cluster_refresh_interval_seconds`.

## Slurm Directives

GPU job or allocation with model:

```bash
#SBATCH --gres=gpu:a6000ada:1
```

GPU job or allocation without model:

```bash
#SBATCH --gres=gpu:1
```

Specific node:

```bash
#SBATCH --partition=gpu3
#SBATCH --nodelist=gpu-node-name
```

Attached GPU task:

```bash
srun --jobid=<allocation_job_id> --gres=gpu:a6000ada:1 --cpus-per-task=8 --mem=32768M ...
```

## CPU Borrowing From GPU Allocations

GPU allocations intentionally request usable CPU and memory from the same node. CPU-only tasks may borrow that capacity, but each free GPU reserves CPU for future GPU work:

```text
borrowable_cpus = free_cpus - free_gpus * 8
```

This prevents CPU-only FEA or utility tasks from occupying all CPU cores in a GPU allocation and blocking an urgent LLM job that already has a free GPU.
