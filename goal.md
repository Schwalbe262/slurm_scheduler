# Goal

Build a persistent Slurm scheduler for CPU FEA/RL loops, remote command tasks, and GPU/LLM workloads.

## Target Behavior

- Keep warm Slurm allocations available before user work arrives.
- Attach remote-command tasks to existing allocations with `srun --jobid`.
- Keep CPU and GPU resource pools separate while allowing safe mixed-capacity reuse.
- Prefer configured warm-pool accounts when account limits and requirements allow.
- Allow CPU warm pools to use CPU-strong GPU partitions when that gives better CPU capacity than CPU-only partitions.
- Route work that needs account-local conda/software through account capabilities and environment profiles.
- Run CPU-heavy FEA/RL batches through packed jobs that use `pestat` CPU load and memory data.
- Keep `cpu2` to one scheduler job per node because concurrent jobs on that partition can exhaust memory.
- Prewarm scarce GPU capacity with A6000ADA preferred and A6000 as fallback.
- Track GPU capacity as total, used, cluster-free, scheduler-owned, and scheduler-free.
- Allow CPU-only tasks to borrow CPU from GPU allocations only after reserving CPU for free GPUs.
- Expand dynamically when queued work cannot fit or pool utilization is high.
- Shrink idle excess allocations without dropping below configured CPU/GPU warm minimums.
- Expose jobs, tasks, allocations, GPU capacity, and token usage through Web UI and JSON APIs.
- Keep GitHub documentation clear enough for a remote LLM agent to operate the scheduler over Tailscale.
- Keep README, docs, and examples sufficient for a human or LLM to start from only the GitHub link.

## Success Criteria

- CPU tasks prefer CPU allocations and can use GPU allocation CPU only within the borrowable capacity rule.
- GPU tasks match requested GPU model, partition, node name, CPU, memory, and free GPU count.
- Jobs and tasks with `required_capability` or `env_profile` only run on compatible accounts.
- `cpu2` allocation/direct jobs are assigned to idle nodes and do not stack on a node already used by scheduler work.
- Closed allocations are hidden by default in the dashboard and limited to the 20 most recent closed rows.
- A6000ADA is chosen before A6000 when both have effective capacity.
- A6000 is used as fallback when A6000ADA is unavailable.
- Scheduler-owned GPUs are distinguished from cluster-free GPUs in UI/API.
- FEA/RL packed jobs can target around 20 simulations in parallel when hardware capacity allows.
- Remote clients can verify health and submit work using only documented Web/API endpoints.
- Token usage is recorded and visible as both a graph and a table.
- README links to API, examples, config, troubleshooting, scheduling policy, GPU policy, remote access, and roadmap docs.
- Example shell scripts demonstrate health checks, CPU tasks, GPU tasks, specific-node GPU tasks, Git tasks, and token usage without embedding secrets.

## Operating Defaults

- Scheduler URL: `http://<scheduler-host>:8000/`
- Preferred GPU models: `a6000ada`, then `a6000`
- GPU prewarm minimum: 1 allocation
- GPU prewarm maximum: 3 allocations
- GPU per prewarm allocation: 2
- CPU reserve per free GPU: 8 cores
- GPU allocation time limit: 48 hours
- CPU/GPU warm pool preferred account: configured through local `config/app.yaml`
- Single-job-per-node partition: `cpu2`
- CPU warm allocation size: 64 cores
- CPU pool may use GPU partitions when `cpu_pool_allow_gpu_partitions` is enabled and CPU profile ranking favors them.
- Cluster inventory and `pestat` refresh periodically through `cluster_refresh_interval_seconds`.

## Implementation Notes

- Local secrets and real cluster account config stay outside Git.
- Runtime policy defaults live in `config/app.example.yaml` and `AppConfig`.
- Development execution records are appended to `note.md`.
- Meaningful improvements are appended to `insight.md` in chronological order.
- Recommended future work is tracked in `docs/ROADMAP.md`.
