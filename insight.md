# Implementation Insights

## 2026-06-15 18:32:42 KST

- Problem: Free CPU/RAM can drift if task attachment fails or status refresh runs more than once.
- Discovery: Capacity should not be incrementally adjusted from attach/finish events alone.
- Improvement: Recalculate allocation free capacity each scheduler tick from currently attaching/running tasks.

## 2026-06-15 18:32:42 KST

- Problem: A 36h allocation with no running task should not wait for another scheduler cycle before releasing an account job slot.
- Discovery: The drain transition can immediately check attached task count.
- Improvement: If a newly draining allocation is already empty, close it in the same lifecycle pass.

## 2026-06-15 18:32:42 KST

- Problem: A remote attach command containing `&` can be parsed by the outer shell in a way that backgrounds more than intended.
- Discovery: The detached command should be quoted as a single remote shell command.
- Improvement: Wrap the `nohup srun` launch inside an explicit `bash -lc` command.

## 2026-06-15 18:39:33 KST

- Problem: Queued demand could repeatedly prewarm allocations while an earlier allocation was still pending in Slurm.
- Discovery: Pending allocations already represent future capacity and should count as spare capacity for scale-out decisions.
- Improvement: Include `PENDING` allocations in inflight capacity checks and add regression tests for duplicate prewarm prevention.

## 2026-06-15 20:53:00 KST

- Problem: GPU capacity was previously treated as static inventory, which ignores GPUs already used by other Slurm users.
- Discovery: Slurm `GresUsed` must be stored separately from total GRES so the scheduler can distinguish cluster-free capacity from scheduler-owned capacity.
- Improvement: Track `cluster_total_gpus`, `cluster_used_gpus`, `cluster_free_gpus`, `scheduler_owned_gpus`, and `scheduler_free_gpus`.

## 2026-06-15 20:53:00 KST

- Problem: Small GPU allocations such as 4 CPU cores with 4 GPUs waste account job slots and cannot absorb CPU-side work.
- Discovery: GPU allocations should request usable CPU and memory on the selected node, but CPU-only tasks must not consume CPU needed by future GPU tasks.
- Improvement: Add mixed-capacity matching where CPU-only tasks may borrow GPU allocation CPU after reserving 8 CPU cores per free GPU.

## 2026-06-15 21:33:59 KST

- Problem: Some useful conda/software stacks exist only on specific cluster accounts, and duplicating them per account wastes quota.
- Discovery: Environment availability should be a scheduling constraint, not a per-job manual convention.
- Improvement: Add account capabilities and environment profiles so jobs/tasks can request a profile and only run on compatible accounts.

## 2026-06-15 21:33:59 KST

- Problem: `cpu2` nodes can run out of memory when multiple scheduler jobs share one node even if CPU counts appear available.
- Discovery: For memory-sensitive partitions, free CPU is not enough; node-level exclusivity within scheduler policy is required.
- Improvement: Add `single_job_per_node_partitions` and assign idle `cpu2` nodes explicitly with `#SBATCH --nodelist`.

## 2026-06-15 22:13:43 KST

- Problem: Treating CPU warm pools as CPU-partition-only can push work to weak CPU nodes when `cpu2` is unavailable.
- Discovery: A CPU pool is a CPU-capacity pool, not necessarily a CPU-partition pool; some GPU partitions have better CPU profiles than `cpu1`.
- Improvement: Allow CPU pools to consider GPU partitions and rank CPU candidates by CPU profile score first.

## 2026-06-15 22:13:43 KST

- Problem: Closed allocations made the dashboard pool table noisy and buried the current runnable capacity.
- Discovery: Closed rows are audit history, while active/pending/warm rows are operational state.
- Improvement: Show active allocations by default and fold closed history into a recent-20 details section.

## 2026-06-15 22:38:24 KST

- Problem: A CPU pool job waited on `Resources` because it pinned a GPU node that was already fully allocated by another user's job.
- Discovery: The scheduler selected the node from stale `pestat` data, and that partition was not actually the best CPU candidate under the configured CPU profile ranking.
- Improvement: Refresh cluster inventory and `pestat` periodically inside the scheduler, and avoid node pinning for CPU pools except on single-job partitions.

## 2026-06-15 23:08:40 KST

- Problem: A GitHub link alone was not enough for a remote human or LLM to safely operate the scheduler because setup, API usage, examples, and troubleshooting were spread across multiple documents.
- Discovery: LLM-friendly docs need a stable entrypoint plus task-oriented examples, exact endpoint fields, and operational failure modes.
- Improvement: Split documentation into README entrypoint, API reference, examples, config guide, troubleshooting guide, scheduling policy, GPU policy, and roadmap.

## 2026-06-15 23:20:05 KST

- Problem: Public GitHub documentation can accidentally leak private operational details if live URLs, account names, node names, or Slurm job IDs are copied from local debugging notes.
- Discovery: README and docs should use placeholders, while local ignored config should hold deploy-specific values.
- Improvement: Sanitize tracked documentation and make example scripts require `SCHEDULER_URL` explicitly.

## 2026-06-15 23:20:05 KST

- Problem: Users may interpret `memory_mb` as preallocated RAM rather than a scheduler reservation and possible Slurm limit.
- Discovery: This misunderstanding can lead to either under-requested tasks that fail with OOM or over-requested tasks that unnecessarily block capacity.
- Improvement: Document memory request semantics and practical starting guidance in README, API, examples, and troubleshooting docs.

## 2026-06-16 03:22:07 KST

- Problem: One GPU per warm allocation can underutilize account job slots when frequent GPU work benefits from holding a larger ready slice.
- Discovery: GPU prewarm capacity is controlled by `gpu_prewarm.gpus_per_allocation` plus the scheduler constructor default.
- Improvement: Set the default GPU warm allocation size to two GPUs and update tests/docs so the policy is explicit.

## 2026-06-16 03:36:37 KST

- Problem: A GPU warm allocation can stay Slurm `PENDING` indefinitely and continue occupying one of the account's limited job slots.
- Discovery: The scheduler previously treated pending allocations as live capacity but did not age them out or record Slurm's pending reason.
- Improvement: Record pending reasons, cancel stale pending allocations, and apply resource-pool backoff so the scheduler does not immediately submit the same stuck GPU warm request again.

## 2026-06-16 03:36:37 KST

- Problem: External agents could create tasks through the scheduler but still needed direct SSH knowledge to retrieve task stdout or result files.
- Discovery: Task rows already contain account and remote path metadata, so the scheduler can safely proxy file reads for known task outputs.
- Improvement: Add task stdout, stderr, and safe relative remote-file APIs while keeping execution placement and allocation ownership inside the scheduler.

## 2026-06-16 03:40:12 KST

- Problem: GPU warm pools can miss usable A6000 nodes when the node has free GPUs but only a small number of CPU cores left.
- Discovery: `gpu_cpu_reserve` was being subtracted from GPU warm allocation candidates even though that reserve is intended for CPU pools placed on GPU nodes.
- Improvement: Apply `gpu_cpu_reserve` only to CPU pools on GPU nodes, allowing GPU warm allocations to hold available GPUs with as few as four CPU cores.

## 2026-06-16 03:43:22 KST

- Problem: Preferred warm-pool accounts do not guarantee a submitted smoke task will run on a specific Slurm account.
- Discovery: The API exposed capability and profile filters, but no explicit account constraint for job/task submissions.
- Improvement: Add `account_name` as a hard placement constraint so external agents can require one account without relying on node names or unique capabilities.

## 2026-06-16 04:07:50 KST

- Problem: Treating pending preferred GPU jobs as sufficient warm capacity can leave the scheduler with no immediately usable GPU.
- Discovery: Preferred GPU queueing and ready fallback capacity are separate goals and should not block each other.
- Improvement: Keep A6000-class requests queued while opening a lower-priority GPU fallback when no preferred GPU allocation is ready.
