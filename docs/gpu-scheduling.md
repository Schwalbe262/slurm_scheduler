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
  preferred_models: ["a6000ada", "a6000"]
  min_warm_allocations: 1
  max_warm_allocations: 3
  gpus_per_allocation: 4
  min_gpus_per_allocation: 2
  cpu_reserve_per_free_gpu: 8
  partition: "auto"
  time_limit: "48:00:00"
```

The scheduler keeps at least one A6000-class GPU allocation warm. By default each GPU warm allocation tries to request four GPUs on one node. If four are not free but three are, it requests three; if only two are free, it requests two. One GPU is below the warm-pool minimum and is left for direct task demand instead.

GPU warm pool fallback stays inside the configured A6000-class models. With the default policy it may queue A6000ADA or A6000, but it will not open RTX 3090 or A10 warm pools just because A6000 jobs are pending.

GPU warm placement prioritizes holding the GPU, but it should not make the remaining GPUs unusable. If a GPU warm allocation requests only part of a node's free GPUs, the scheduler leaves `gpu_cpu_reserve` CPU cores unrequested for other users of the remaining GPUs. For example, on a 48-core, 4-GPU node, a 2-GPU warm allocation requests 44 CPU cores rather than all 48.

If an A6000-class node has enough free GPUs but only four free CPU cores, the scheduler may still open the GPU warm allocation with those four CPU cores. This low-CPU exception keeps GPU capture possible when the node is otherwise nearly full.

For attached GPU tasks, GPU availability is treated as the scarce resource. If the requested GPU model and count match an already owned allocation, the scheduler may attach the task even when the allocation has fewer free CPU cores than requested. This exception is limited to GPU tasks requesting 4 CPU cores or fewer, and the attached `srun` step uses `--overlap` when it must share already allocated CPU capacity. CPU-only tasks still require enough borrowable CPU.

If a GPU warm allocation stays Slurm `PENDING` longer than `allocation_pending_timeout_seconds`, the scheduler cancels it and applies `allocation_pending_backoff_seconds` to that GPU pool before trying again. The dashboard Allocation Pool reason column shows the Slurm pending reason, such as `(Resources)` or `(Priority)`.

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
