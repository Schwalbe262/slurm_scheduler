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

## 2026-06-16 04:22:31 KST

- Problem: A scheduler-owned allocation can make the remaining GPUs on a shared GPU node unusable if it consumes every CPU core while using only some GPUs.
- Discovery: GPU warm allocations need a CPU reserve when they leave GPUs unclaimed, while still allowing low-CPU capture when that is the only way to hold a GPU.
- Improvement: Reserve `gpu_cpu_reserve` CPU cores on partial-GPU warm allocations and fold terminal jobs in the dashboard to keep active work visible.

## 2026-06-16 04:28:58 KST

- Problem: One queued task waiting for a pending A6000 allocation can block unrelated ready tasks behind it.
- Discovery: `assign_queued_tasks()` only inspected the oldest queued task and returned when it could not attach.
- Improvement: Iterate across queued tasks in order and skip currently blocked tasks, so available CPU and fallback-GPU capacity is used immediately.

## 2026-06-16 04:37:28 KST

- Problem: Large attached-task payloads can fail before `srun` starts if the scheduler writes the generated script through an SSH command-line `printf`.
- Discovery: The RTX3090 allocation accepted a small manual `srun` step, while failed RTX3090 tasks had about 935 KB commands and no stdout/stderr/wrapper paths in the DB.
- Improvement: Upload generated scripts through SFTP and persist pre-submit/attach log paths on failure, so payload size no longer depends on SSH argument limits and clone stderr can be inspected through the API.

## 2026-06-16 04:46:20 KST

- Problem: Relative remote workspaces break when multiple `cd <relative-path> && ...` steps are chained in a single remote shell.
- Discovery: Job 52 cloned successfully, but the later checkout tried to resolve the same relative job path from inside the job directory.
- Improvement: Run direct-job pre-submit steps as separate SSH exec calls after uploading `run.sbatch`, keeping each command's starting directory predictable.

## 2026-06-16 04:56:00 KST

- Problem: Attached tasks can fail if user code relies on `dirname "$0"` while the scheduler invokes `task.sh` through a relative path.
- Discovery: Task 25 attached correctly but failed inside the script because `cd slurm_scheduler` changed the base for `$0=slurm_scheduler/task-.../task.sh`.
- Improvement: Invoke task scripts via home-rooted paths for stable `$0` resolution.

- Problem: GPU capacity and scheduling were overestimating free GPUs because cluster used GPUs were always parsed as zero.
- Discovery: This cluster exposes GPU allocation in `AllocTRES` rather than `GresUsed`.
- Improvement: Parse `AllocTRES` GPU counts and support ordered GPU/account candidates so placement can be both accurate and flexible.

## 2026-06-16 05:03:13 KST

- Problem: Free-capacity columns can make a running system look idle when users are trying to understand current utilization.
- Discovery: Allocation Pool stores enough data to display either free or used capacity without changing scheduler behavior.
- Improvement: Show used CPU/GPU/memory in the dashboard while preserving free-capacity fields for placement logic and APIs.

## 2026-06-16 05:12:00 KST

- Problem: Users think in terms of scheduler jobs, but creating a separate Slurm job for every Git request defeats the warm-pool design and consumes account job slots.
- Discovery: The existing Git task wrapper can represent a `python_git` job request as an attached task without losing repo checkout, entrypoint, account, GPU, or node constraints.
- Improvement: Route compatibility `POST /jobs job_mode=python_git` requests into the attached-task scheduler, preserving the virtual-job interface while keeping Slurm job count concentrated in warm allocations.

## 2026-06-16 05:18:42 KST

- Problem: Exclusive attached tasks were either blocked by shared warm pools or could make oversized 64-core demand allocations for 12-core work.
- Discovery: Existing inflight-capacity checks treated one pending exclusive allocation as reusable for every queued exclusive task, and allocation shape selection used the global warm-pool size instead of the task request.
- Improvement: Treat exclusive demand as one allocation per queued task and size demand allocations from the task's CPU/memory request, preserving special-purpose isolation without wasting account job slots or triggering avoidable QOS limits.

## 2026-06-16 05:34:30 KST

- Problem: GPU warm allocations could sit with idle CPU while CPU-only tasks remained queued, and quoted `~/.../task.sh` paths failed on compute nodes before user code started.
- Discovery: The scheduler reserved CPU for free GPUs even when no GPU task was running, and `shlex.quote("~/...")` prevented tilde expansion inside `srun`.
- Improvement: Let idle GPU pools lend their CPU to CPU-only tasks, preserve `$HOME` expansion for task script paths, order allocation rows by operational state, and cancel pending demand pools that no queued task can use.

## 2026-06-16 05:55:25 KST

- Problem: Scheduler-created remote directories can accumulate indefinitely, while GPU work can remain queued even when the matching GPU is available but free CPU is slightly below the request.
- Discovery: Task, job, and allocation rows already store enough account and remote path metadata to safely remove only scheduler-owned artifacts; for GPU tasks, the GPU is the scarce resource and strict CPU matching can be counterproductive.
- Improvement: Add TTL-based cleanup for safe scheduler artifact paths, exclude pending allocations from dashboard utilization totals, and allow matching GPU tasks to attach to tight-CPU GPU allocations when memory and at least one CPU core remain.

## 2026-06-16 06:00:24 KST

- Problem: The dashboard can show stale allocation/task/job state while the scheduler loop is actively moving work.
- Discovery: A full-page refresh is sufficient for the current server-rendered UI, but it must not interrupt users filling out submission forms.
- Improvement: Add 15-second dashboard auto-refresh with guards for focused or edited form fields.

## 2026-06-16 06:06:08 KST

- Problem: After `python_git` became an attached task, external agents lost reliable result-file retrieval and had no API to clean up accidentally duplicated tasks.
- Discovery: Random `mktemp` git task directories are invisible to the task model, and near-drain allocations can accept work shortly before the scheduler closes them.
- Improvement: Make git task workdirs deterministic from task id, expose `git_repo`/`git_workdir` remote-file bases, add task cancel and bulk-cancel APIs, and stop attaching new tasks to allocations near their drain threshold.

## 2026-06-16 06:11:09 KST

- Problem: GPU tasks can remain queued while a matching GPU is idle because scheduler-owned CPU capacity is fully reserved by other attached steps.
- Discovery: For small GPU tasks, the GPU is the scarce resource and a 4-core CPU request is often a soft companion requirement.
- Improvement: Allow matching GPU tasks with 4 CPU cores or fewer to attach despite zero scheduler-free CPU, using `srun --overlap` for the shared-CPU step.

## 2026-06-16 06:16:06 KST

- Problem: Exclusive demand allocations can survive as `warm` after their triggering task fails or is cancelled, wasting job slots.
- Discovery: The scale-in logic only checked unneeded demand allocations while they were still `pending`; once Slurm started them and they became `warm`, they fell through to normal warm-pool retention.
- Improvement: Treat `queued ... demand` allocations as demand-owned in both `pending` and `warm` states, and close them whenever no queued task still matches.

## 2026-06-16 06:38:31 KST

- Problem: Service clients need a stable task-broker contract rather than browser redirects and scattered stdout/result retrieval.
- Discovery: The existing task table and attach lifecycle already had most execution metadata, but lacked payload, dedupe, timeout, priority, worker-cap, and normalized polling response fields.
- Improvement: Add JSON task submission, payload file injection, enriched task status JSON, stdout final-JSON parsing, and scheduling controls for Flight-style polling clients.

## 2026-06-16 06:48:11 KST

- Problem: Operators had to launch extra diagnostic tasks to inspect logs, cancel could block on remote SSH, and CPU-tight GPU tasks failed because `--exclusive` and `--overlap` were emitted together.
- Discovery: Log reads should be tolerant while files are being created, cancel state can be recorded before remote cleanup, and capability labels are hard placement gates independent of visible CPU capacity.
- Improvement: Add log tailing and remote glob APIs, fast cancel responses, stderr-derived failure messages, task fit-capacity reporting, mutually exclusive `srun` flags, and local `flight-crawl`/Factorio vLLM capability/profile configuration.

## 2026-06-16 06:51:39 KST

- Problem: Using an abstract `flight-crawl` capability for Flight tasks hides the real dependency and can keep tasks queued when operators only know the conda environment name.
- Discovery: The scheduler treats `required_capability` as an exact account label and only `env_profile` performs shell setup, so a conda dependency should be expressed as both a capability and a profile.
- Improvement: Prefer `required_capability=conda:flight-searcher` with `env_profile=flight-searcher`, while keeping `flight-crawl` locally for compatibility with already submitted tasks.

## 2026-06-16 07:00:20 KST

- Problem: A ready A6000 allocation can sit idle behind a large higher-priority CPU-only backlog.
- Discovery: The GPU task was schedulable on `n104`, but queued after thousands of `flight-crawl` CPU tasks that could not fit into the remaining CPU slots.
- Improvement: Order attached-task assignment by scarce GPU demand first, then by priority and id, so available GPUs are not starved by bulk CPU queues.

## 2026-06-16 07:04:35 KST

- Problem: Demand scale-out can stop after finding one pending allocation that fits a task, even when that allocation has only a few finite slots and the queue is much larger.
- Discovery: A pending 48-core GPU warm pool was counted as inflight capacity for thousands of 16-core CPU tasks, so no additional CPU pool was opened.
- Improvement: Reserve finite inflight CPU, memory, and GPU slots while scanning queued tasks; once queued demand exceeds reserved capacity, open another demand allocation.

## 2026-06-16 07:09:00 KST

- Problem: Pool creation depends on account snapshots, so one unreachable account can block allocation decisions for otherwise healthy accounts.
- Discovery: Snapshot collection raised out of the whole refresh loop instead of skipping only the failing account.
- Improvement: Treat account snapshot failures independently and continue scheduling with the accounts that still report status.

## 2026-06-16 07:13:35 KST

- Problem: Dashboard recent-row limits can hide older long-running tasks behind a flood of newer queued or finished tasks.
- Discovery: Active and finished task sections were split after fetching only the latest generic task rows.
- Improvement: Fetch `running`/`attaching` tasks by status so they are always displayed, while limiting only queued and finished task rows for UI size.

## 2026-06-16 07:15:14 KST

- Problem: Scheduler ticks and API reads can fail under task submission bursts because SQLite's default lock handling is too short for concurrent scheduler/UI/client traffic.
- Discovery: The scale-out loop stopped before pool maintenance with `sqlite3.OperationalError: database is locked`, and `/api/tasks/{id}` also returned 500 during the same lock window.
- Improvement: Open SQLite connections with a longer timeout, set `busy_timeout`, enable WAL mode, and use `synchronous=NORMAL` so readers and writers interfere less.

## 2026-06-16 07:23:17 KST

- Problem: A single queued 16-core task can accidentally shape the next CPU allocation as a small one-task pool.
- Discovery: Non-exclusive CPU demand reused task-level CPU and memory requests during allocation creation, while scale-out waited until pools were mostly full.
- Improvement: Treat non-exclusive CPU demand as pool demand, choose the largest available shared CPU pool, and prewarm another pool once usage reaches 50%.

## 2026-06-16 07:27:42 KST

- Problem: Existing undersized pending CPU demand allocations can survive a policy fix and continue suppressing larger pool creation.
- Discovery: A pending `16 CPU` allocation still counts as matching one queued 16-core task, so the scheduler may treat it as valid spare capacity.
- Improvement: Close non-exclusive queued CPU demand allocations when current inventory can open a larger shared CPU pool, letting the next tick submit the larger shape.

## 2026-06-16 07:33:12 KST

- Problem: Shared CPU pool selection can fall back to GPU partitions even when CPU partitions have mixed nodes with enough free cores.
- Discovery: The single-job partition guard excluded any `cpu2` node with nonzero `cpu_used`, so the best remaining candidate became a 48-core GPU node.
- Improvement: Allow non-exclusive CPU pools onto mixed CPU nodes, keep the request at configured pool size, and rank CPU partitions ahead of GPU fallback capacity.

## 2026-06-16 14:49:53 KST

- Problem: GPU warm pool fallback can submit an impossible CPU request when no concrete candidate node is selected.
- Discovery: The fallback path reused `allocation_cpus=64` for a `gpu:a6000ada` Slurm request even though the target GPU partition's nodes have fewer CPUs.
- Improvement: Cap fallback GPU allocation CPU requests by observed node capacity and classify failed allocations with closed allocations in the dashboard.

## 2026-06-16 16:23:12 KST

- Problem: Full-page dashboard refreshes can collapse sections and move the operator away from the row/table they were inspecting.
- Discovery: The refresh loop reloaded the page without storing any UI state beyond avoiding active form edits.
- Improvement: Persist details open states and scroll positions in localStorage across auto-refreshes so passive monitoring does not disturb the current view.

## 2026-06-16 18:23:53 KST

- Problem: Account-local conda environments were manually prepared and then reflected in `accounts.yaml`, making multi-account scheduling brittle.
- Discovery: The scheduler already has SSH account access and capability/profile placement gates, so synced environments can be represented as DB overlays instead of rewriting local config.
- Improvement: Track conda-pack sync jobs and expose completed target environments as dynamic capabilities/profiles; add task detail pages that show how a task was submitted and how to recreate it.
## 2026-06-16 18:39:31 KST

- GPU warm pool should distinguish task GPU requests from scheduler-owned prewarm requests. A user task with `gpus=1` must still fit one GPU, but an internal warm-pool request can leave `gpus=0` and let the scheduler choose the best available 4/3/2 A6000-class shape.
- Lower-tier GPU fallback is useful for explicit task demand, but harmful for a warm pool whose purpose is to reserve scarce A6000-class capacity. The fallback model set should therefore be the configured warm `preferred_models`, not every GPU model in cluster capacity.
## 2026-06-16 18:57:59 KST

- Private repo access should be attached to the scheduler's task execution context, not to each Slurm account's home directory. A master-account deploy key plus per-task temporary `GIT_SSH_COMMAND` keeps capacity scheduling account-independent while avoiding long-lived key copies in every account.
- SSH aliases are convenient for humans but brittle for distributed scheduler tasks. Rewriting matched repo URLs to canonical `git@github.com:org/repo.git` removes dependency on per-account `~/.ssh/config`.
## 2026-06-16 19:19:17 KST

- Capability labels are scheduling constraints, so they need first-class observability. Showing capability -> account/profile/source mappings directly in the dashboard prevents users from mistaking idle CPU in an ineligible account for usable capacity.

## 2026-07-12 19:53:04 KST

- Source loop: true physical `exclusive_node` scheduler audit.
- Improvement: enforce exclusivity at the Slurm allocation, warm-pool compatibility, and physical-node candidate layers together.
- Before: the DB flag did not add `#SBATCH --exclusive`, opposite pool types could match, and cpu1 mixed nodes remained exclusive candidates.
- After: exclusive allocations request Slurm exclusivity, both flag directions must match, and exclusive shapes accept only idle/unused nodes while shared FEA behavior is unchanged.
- Evidence: focused 6/6 and full scheduler core 331/331 tests passed.
- Remaining risk: deployment and a live Slurm allocation smoke are intentionally pending operator review.
