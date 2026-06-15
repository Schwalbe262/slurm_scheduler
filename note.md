# Development Notes

## 2026-06-15 18:32:42 KST

- Confirmed the repository was already current with GitHub `main`.
- Added allocation/task data models, SQLite tables, and repository methods.
- Added allocation pool settings to app config defaults.
- Added Slurm adapter methods for long-running allocation jobs and `srun --jobid` task attachment.
- Added scheduler logic for refresh, assignment, minimum warm pool, utilization-based prewarm, idle scale-in, 36h drain, and empty-drain cancellation.
- Added dashboard task submission, allocation display, task display, and JSON APIs.
- Added unit coverage for warm allocation creation, task attach, scale-out, drain-close behavior, and account job-limit protection.

Remaining verification:

- Run unit tests and compile checks.
- Fix any regressions found by the test suite.

## 2026-06-15 18:32:42 KST

- Ran unit tests, compile checks, and shell syntax checks.
- Adjusted task attachment command wrapping so background `srun` launch is isolated inside a remote `bash -lc`.
- Tightened GPU node allocation planning so the configured CPU reserve is actually left unused.
- Updated `HANDOFF.md` so the persistent allocation lifecycle section reflects the implemented allocation/task workflow.

## 2026-06-15 18:39:33 KST

- Audited the implementation against `goal.md`.
- Fixed allocation scale-out so pending allocations count as inflight spare capacity.
- Added tests that prevent duplicate prewarming while a suitable pending allocation already exists.
- Verified with `python3 -m unittest discover -s tests`, `/tmp/slurm_scheduler_smoke_venv/bin/python -m unittest discover -s tests`, `python3 -m compileall slurm_scheduler scripts tests`, `bash -n` for shell helpers, `git diff --check`, and a smoke-venv FastAPI app factory route check.

## 2026-06-15 18:41:42 KST

- Added direct tests for one-node allocation scripts, `srun --jobid` attach command generation, pestat-based allocation shape selection, and GPU CPU reserve behavior.
- Reverified with 26 passing tests in both the default Python environment and the smoke virtualenv.
- Reverified compile checks, shell syntax checks, `git diff --check`, and smoke-venv FastAPI app factory route creation.

## 2026-06-15 20:53:00 KST

- Extended jobs, tasks, allocations, and node inventory with GPU model, GPU count, GPU used count, resource pool, node constraint, and exclusive-node fields.
- Added Slurm GPU directive generation for `#SBATCH --gres=gpu:<model>:<count>` and attached `srun --gres=...` GPU tasks.
- Added A6000ADA-first GPU prewarm policy with A6000 fallback, scheduler-owned/free GPU accounting, and CPU borrowing from GPU allocations with GPU CPU reserve.
- Added `scontrol -o show nodes` inventory parsing so `GresUsed` contributes to effective GPU capacity.
- Added `/api/gpu-capacity` and `/api/health`, expanded the dashboard GPU capacity and allocation/task tables, and documented remote LLM operation.
- Ran `python3 -m unittest discover -s tests`: 35 tests passed.
- Reverified with `/tmp/slurm_scheduler_smoke_venv/bin/python -m unittest discover -s tests`, `python3 -m compileall slurm_scheduler scripts tests`, `bash -n scripts/*.sh`, `git diff --check`, and a smoke FastAPI route creation check.
- Restarted `slurm-scheduler.service` and verified local plus private-network `/api/health` checks.
- Confirmed live GPU prewarm opened an A6000ADA allocation in the expected pool.

## 2026-06-15 21:33:59 KST

- Fixed the CPU warm pool default to 64 cores per allocation and updated runtime config.
- Added CPU and GPU warm pool preferred account lists; local config can prefer the prepared account for both pools.
- Changed allocation Slurm job names from `pool-<allocation_id>` to plain `pool`.
- Added account `capabilities` and `env_profiles`, plus job/task `required_capability` and `env_profile` routing.
- Added `single_job_per_node_partitions`; local config includes `cpu2`, so the scheduler avoids stacking scheduler jobs on one `cpu2` node and assigns an idle `--nodelist`.
- Added real conda profiles in local ignored `config/accounts.yaml`.
- Updated Web UI fields, README, LLM operator guide, scheduling principles, GPU scheduling docs, and scheduler goal text.
- Ran `python3 -m unittest discover -s tests` and `/tmp/slurm_scheduler_smoke_venv/bin/python -m unittest discover -s tests`: 43 tests passed.
- Ran `python3 -m compileall slurm_scheduler tests`.

## 2026-06-15 22:13:43 KST

- Added `cpu_pool_allow_gpu_partitions`; CPU warm pools can now use GPU partitions when their CPU profile is stronger.
- Changed CPU pool node ranking to prefer CPU profile score first instead of limiting auto CPU pools to CPU-only partitions.
- Updated dashboard allocation display so active allocations are shown by default and closed allocations are folded into a recent-20 section.
- Added regression tests for CPU pool placement on CPU-strong GPU partitions and for disabling that behavior.
- Ran `python3 -m unittest discover -s tests` and `/tmp/slurm_scheduler_smoke_venv/bin/python -m unittest discover -s tests`: 45 tests passed.
- Restarted `slurm-scheduler.service`, closed an old CPU pool job, and confirmed a replacement CPU pool job opened.

## 2026-06-15 22:38:24 KST

- Investigated a CPU pool job pending with Slurm reason `Resources`.
- Found the requested node was already fully allocated by another user's job.
- Found scheduler capacity data was stale: stored `pestat` and inventory rows were from 2026-06-13 before manual refresh.
- Added automatic Slurm inventory and `pestat` refresh through `cluster_refresh_interval_seconds`.
- Changed CPU pool allocation on non-single-job partitions to avoid pinning `#SBATCH --nodelist`; only single-job partitions such as `cpu2` keep explicit node pinning.
- Reverified CPU profile order: `cpu2` > `gpu5` > `gpu2/gpu3` > `cpu1/gpu1/gpu6`.
- Closed the stale CPU pool job and confirmed a replacement CPU pool job opened on a single-job CPU partition with 64 CPUs.
- Ran `python3 -m unittest discover -s tests` and `/tmp/slurm_scheduler_smoke_venv/bin/python -m unittest discover -s tests`: 45 tests passed.

## 2026-06-15 23:08:40 KST

- Reworked `README.md` as the GitHub entrypoint for humans and LLM agents.
- Added `docs/API.md`, `docs/EXAMPLES.md`, `docs/CONFIG.md`, `docs/TROUBLESHOOTING.md`, and `docs/ROADMAP.md`.
- Added shell examples for health checks, CPU tasks, A6000ADA GPU tasks, specific-node GPU tasks, Git tasks, and token usage records.
- Updated LLM, scheduling, and GPU docs so CPU-pool-on-GPU-partition behavior, GPU capacity meanings, and safe placement rules are explicit.
- Updated `goal.md` to include GitHub-link-only onboarding as a success criterion.

## 2026-06-15 23:20:05 KST

- Sanitized README, docs, examples, goal, notes, and insights for public-safe GitHub sharing.
- Replaced real private-network URLs, real account names, node names, and live Slurm job IDs with placeholders.
- Added `examples/submit_git_task.sh` and `examples/submit_dynamic_packed_job.sh`.
- Changed example scripts to require `SCHEDULER_URL` instead of defaulting to a private URL.
- Documented how `memory_mb` works for attached tasks: it is a scheduling/reservation and possible Slurm enforcement limit, not a preallocated RAM block.
- Verified markdown links, public-safe string scan, example shell syntax, documented FastAPI routes, tests, compile checks, and `git diff --check`.

## 2026-06-16 03:22:07 KST

- Changed GPU warm prewarm policy from 1 GPU per allocation to 2 GPUs per allocation.
- Updated `AppConfig`, `Scheduler` defaults, example app config, local runtime config, docs, and GPU prewarm tests.
- Verified default and smoke virtualenv unit tests, compile checks, shell syntax checks, and `git diff --check`.

## 2026-06-16 03:36:37 KST

- Added allocation pending safeguards: Slurm pending reason capture, pending timeout cancellation, and per-resource-pool backoff before resubmission.
- Added task file read APIs so clients can fetch task stdout, stderr, or safe relative result files through the scheduler while allocation/job creation remains scheduler-managed.
- Updated the dashboard Allocation Pool reason column to show pending reasons.
- Changed Attached Tasks so the log path is exposed through a hover/click `?` control and the table column shows elapsed runtime in `HH:MM:SS`.
- Documented pending timeout/backoff settings and task file read endpoints.

## 2026-06-16 03:40:12 KST

- Changed GPU warm placement so A6000-class nodes with free GPUs remain eligible even when only four CPU cores are free.
- Kept `gpu_cpu_reserve` for CPU pools on GPU nodes only; GPU warm allocations now prioritize holding the GPU.
- Added a regression test for a two-GPU A6000 warm allocation on a node with four effective free CPU cores.

## 2026-06-16 03:43:22 KST

- Added explicit `account_name` constraints for jobs, remote tasks, and Git tasks.
- Changed scheduler placement so a requested account is a hard filter, not a preference.
- Updated the dashboard forms and docs to show that `account_name=account_a` keeps the request on that account or queued if unavailable.

## 2026-06-16 03:50:43 KST

- Replaced the old one-GPU A6000ADA warm allocation with a new two-GPU warm allocation on `r1jae262`.
- Added `account_name` usage to examples, shell scripts, and the LLM operator guide so external agents can force a specific Slurm account.
- Verified shell example syntax, unit tests, and whitespace checks.

## 2026-06-16 04:07:50 KST

- Changed GPU prewarm policy so preferred A6000-class allocations can remain queued while a lower-priority GPU allocation is opened as ready fallback capacity.
- Changed the dashboard so completed, failed, and cancelled attached tasks are folded by default under a finished-tasks details section.
- Added regression tests for preferred GPU queue preservation and lower-GPU fallback behavior.

## 2026-06-16 04:11:59 KST

- Expanded `README.md` with an explicit client submission flow for `/tasks`, `/tasks/git`, and `/jobs`.
- Added copy-paste examples for health checks, account-constrained submissions, private Git repo submissions, packed jobs, polling, stdout retrieval, and remote result file reads.

## 2026-06-16 04:22:31 KST

- Updated GPU warm allocation CPU shaping so partial-GPU allocations leave `gpu_cpu_reserve` CPU cores for other users of the remaining GPUs.
- Kept the low-CPU exception so an A6000-class GPU can still be captured when only a few CPU cores are free.
- Changed the dashboard Jobs table so completed, failed, and cancelled jobs are folded by default and limited to the most recent 50.

## 2026-06-16 04:28:58 KST

- Fixed queued attached-task head-of-line blocking.
- The scheduler now scans all queued tasks and skips tasks that are waiting for unavailable capacity, allowing later CPU or fallback-GPU tasks to attach immediately.
- Added a regression test where a blocked A6000ADA task no longer prevents a ready CPU task from running.

## 2026-06-16 04:37:28 KST

- Investigated RTX3090 attached task failures.
- Confirmed the RTX3090 allocation itself was healthy by manually attaching a small `srun` step to Slurm job 680352 on n002 and running `nvidia-smi -L`.
- Found failed task commands were about 935 KB each, and the scheduler was writing `task.sh` through a single SSH exec command with `printf`.
- Changed remote script creation for jobs, allocations, and attached tasks to use SFTP instead of embedding large scripts in the SSH command line.
- Added remote execution errors that carry log paths back to the scheduler, so failed task attach attempts can still expose `remote_dir`, `stdout_path`, and `stderr_path`.
- Added pre-submit logs for direct jobs: `submit.stdout.log` and `submit.stderr.log` under `remote_job_dir`.
- Documented how to read submit logs through `/api/jobs/{job_id}/remote-file`.
- Restarted the scheduler service and confirmed a new 939 KB RTX3090 task attached to allocation 11 with populated `remote_dir`, `stdout_path`, `stderr_path`, and `wrapper_pid`.

## 2026-06-16 04:46:20 KST

- Investigated failed job 52 (`crypto-smoke-ssh3`).
- Confirmed the private Git clone succeeded: `repo/` existed under the remote job directory and contained the expected project files.
- Found the failure was caused by the new submit-log implementation joining clone, checkout, and sbatch commands with `&&` in one shell while `remote_job_dir` is relative.
- The checkout step attempted to `cd slurm_scheduler/job-52-.../repo` from inside the job directory, producing a duplicated relative path and `No such file or directory`.
- Changed direct job submission to execute each pre-submit step in a fresh SSH exec command so relative workspace paths do not compound across `cd` commands.
