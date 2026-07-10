# API Reference

Korean summary: 이 문서는 LLM이나 사람이 Web UI 없이 scheduler를 호출할 때 쓰는 HTTP API 기준입니다. 모든 `POST` form endpoint는 성공 시 대시보드(`/`)로 `303` redirect를 반환합니다.

Set the endpoint once:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
```

## Health And Monitoring

### `GET /api/health`

Checks the FastAPI service, local database, and the scheduler loop itself. Returns `503` with `"ok": false` when the scheduler thread is dead or a tick has stalled past the watchdog threshold.

```bash
curl -sS "$SCHEDULER_URL/api/health"
```

Typical response:

```json
{
  "ok": true, "accounts": 6, "jobs": 12, "tasks": 0, "allocations": 7,
  "scheduler_thread_alive": true, "scheduler_stalled": false, "scheduler_ok": true,
  "last_tick_completed_at": "2026-07-07T00:00:00+00:00",
  "last_tick_duration_seconds": 2.6, "tick_in_progress_seconds": null,
  "consecutive_tick_failures": 0
}
```

### `GET /api/events`

Returns recent scheduler events (allocation open/warm/close/fail, task complete/fail/requeue, account SSH failures, watchdog firings, reconcile actions, orphan sweeps), newest first.

```bash
curl -sS "$SCHEDULER_URL/api/events?limit=100"
```

### `GET /api/licenses`

Latest FlexLM license snapshot from the license monitor (empty object until the first check completes).

```bash
curl -sS "$SCHEDULER_URL/api/licenses"
```

```json
{"checked_at": "...", "server": "1055@...", "server_up": true, "in_use": [{"feature": "electronics_desktop", "total": 550, "used": 12}], "features": [...], "error": ""}
```

### `GET /api/dashboard-summary`

Lightweight aggregate used by the dashboard's live headline refresh: task activity counters and allocation pool usage.

```bash
curl -sS "$SCHEDULER_URL/api/dashboard-summary"
```

### `POST /api/placement/dry-run`

Explains where a hypothetical task would land without creating any state: aggregate queue diagnostics plus per-account eligibility and per-allocation fit slots with rejection reasons.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/placement/dry-run" \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000 \
  -F partition=auto
```

Response shape: `{"queue_state", "queue_reason", "capacity", "accounts": [{"name", "eligible", "reasons"}], "allocations": [{"id", "state", "node_name", "fit_slots", "reasons"}]}`.

### `GET /api/scheduler/gpu-prewarm` / `POST /api/scheduler/gpu-prewarm`

Reads or sets the GPU warm pool toggle. The setting persists in the database and overrides `gpu_prewarm.enabled` from the config file.

```bash
curl -sS "$SCHEDULER_URL/api/scheduler/gpu-prewarm"
curl -sS -X POST "$SCHEDULER_URL/api/scheduler/gpu-prewarm" \
  -H "Content-Type: application/json" -d '{"enabled": false}'
```

### `GET /api/accounts/status`

Returns cached account snapshots from the background scheduler loop. Prefer this for frequent polling.

```bash
curl -sS "$SCHEDULER_URL/api/accounts/status"
```

### `GET /api/accounts/status/live`

Refreshes account status immediately through SSH. Use sparingly because it touches the cluster.

```bash
curl -sS "$SCHEDULER_URL/api/accounts/status/live"
```

### `GET /api/capabilities`

Lists scheduler placement capabilities, eligible accounts, matching env profiles, and whether each rule came from `accounts.yaml` or a conda sync overlay.

```bash
curl -sS "$SCHEDULER_URL/api/capabilities"
```

Use this before submitting a task with `required_capability` or `env_profile`. A task can only attach to allocations owned by an account listed for that capability.

### `POST /api/conda-env-sync`

Clones a conda environment from a reference account to explicit target accounts with `conda-pack`. Existing target environments with the same name are moved aside with a timestamp backup suffix before the packed environment is installed.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/conda-env-sync" \
  -H 'Content-Type: application/json' \
  --data '{
    "reference_account": "r1jae262",
    "source_env_name": "pyaedt2026v1",
    "target_accounts": ["account_b", "account_c"]
  }'
```

After a target completes, the scheduler records a dynamic capability/profile overlay. Jobs and tasks can then use:

```text
required_capability=conda:pyaedt2026v1
env_profile=pyaedt2026v1
```

### `GET /api/conda-env-sync`

Lists recent conda environment sync jobs and target-account states.

```bash
curl -sS "$SCHEDULER_URL/api/conda-env-sync"
```

### `GET /api/conda-env-sync/{sync_job_id}`

Returns one sync job with target statuses, remote log paths, backup paths, installed prefixes, and failure messages.

```bash
curl -sS "$SCHEDULER_URL/api/conda-env-sync/1"
```

### `POST /api/conda-env-sync/{sync_job_id}/cancel`

Marks unfinished sync targets cancelled. Remote work is stopped best-effort; already completed target overlays remain recorded.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/conda-env-sync/1/cancel"
```

### `GET /api/allocations`

Lists scheduler-owned Slurm allocation pools.

```bash
curl -sS "$SCHEDULER_URL/api/allocations"
```

Important fields:

- `slurm_job_id`: Slurm allocation job ID.
- `account_name`: account that owns the allocation.
- `partition`, `node_name`: Slurm placement.
- `resource_pool`: `cpu`, `gpu:a6000ada`, `gpu:a6000`, or similar.
- `state`: `pending`, `warm`, `active`, `draining`, `closing`, `closed`, or `failed`.
- `free_cpus`, `free_memory_mb`, `free_gpus`: currently attachable capacity.

### `POST /api/allocations/{allocation_id}/close`

Manually closes a scheduler-owned allocation pool and cancels the backing Slurm allocation job.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/allocations/123/close"
```

By default, the scheduler rejects the close with `409` if the allocation has `attaching` or `running` tasks. Use `force=true` only when you intentionally want to fail those active tasks and release the allocation:

```bash
curl -sS -X POST "$SCHEDULER_URL/api/allocations/123/close?force=true"
```

### `GET /api/gpu-capacity`

Lists cluster and scheduler GPU capacity grouped by partition/model.

```bash
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

### `GET /api/task-capacity`

Estimates how many currently owned allocation slots can run a task shape.

```bash
curl -sS "$SCHEDULER_URL/api/task-capacity?cpus=16&memory_mb=32768&gpus=0&required_capability=conda:flight-searcher"
curl -sS "$SCHEDULER_URL/api/task-capacity?cpus=4&memory_mb=32768&gpus=1&gpu_model=a6000"
curl -sS "$SCHEDULER_URL/api/task-capacity?cpus=4&memory_mb=32768&scheduling_profile=fea_bursty"
```

The response includes total ready `fit_slots`, `ready_fit_slots`, `pending_fit_slots`, `inflight_fit_slots`, `queue_state`, `queue_reason`, `preferred_node_relaxed`, `memory_pressure_state`, and per-allocation free CPU, memory, GPU, and fit slots. For `scheduling_profile=fea_bursty`, `memory_pressure_state` is `ok`, `soft_blocked`, or `hard_pressure`.

Use:

- `scheduler_free_gpus` for immediate placement.
- `cluster_free_gpus` to estimate whether scale-out can acquire another GPU allocation.
- `cluster_used_gpus` to understand capacity already consumed by all Slurm users.
- `single_node_max_free_gpus` to see the largest free GPU count available on one node for that model.
- `single_node_max_free_cpus` and `single_node_max_free_gpu_node` to see the best `pestat` scheduler-free CPU capacity among nodes with that largest one-node GPU opening.

## Submit Existing Remote Commands

### `POST /tasks`

Use this when the project already exists on the remote cluster filesystem.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-case-001 \
  -F remote_cwd=/remote/project/path \
  -F command='python run_fea.py --case case001' \
  -F account_name=account_a \
  -F cpus=4 \
  -F memory_mb=8192 \
  -F scheduling_profile=fea_bursty \
  -F gpus=0
```

Form fields:

- `name`: task name shown in the UI.
- `remote_cwd`: working directory on the selected cluster account.
- `command`: command executed through the attached `srun` wrapper.
- `env_setup`: optional commands inserted before `command`.
- `required_capability`: optional account capability, such as `conda:pyaedt2026v1`.
- `env_profile`: optional named account profile to prepend before `env_setup`.
- `account_name`: optional exact account constraint. Use one account such as `account_a`, or ordered candidates such as `account_a,account_b`.
- `cpus`: CPU cores requested from an allocation.
- `memory_mb`: memory requested from an allocation. This is a scheduling/reservation and possible Slurm enforcement limit; it does not physically allocate RAM before the process uses it.
- `scheduling_profile`: `standard` keeps the existing hard CPU/memory slot accounting. `fea_bursty` keeps `--cpus-per-task` and `--mem` on the Slurm step, uses `--overlap`, skips hard CPU/memory slot subtraction, and gates new attaches from live `pestat` load/free-memory data.
- `gpus`: GPU count, normally `0` or `1`.
- `gpu_model`: optional normalized model such as `a6000ada` or `a6000`. Ordered candidates such as `a6000ada,a6000` are accepted.
- `partition`: `auto` or a specific Slurm partition.
- `node_name`: optional specific node constraint.
- `same_node_as_task_id` or `same_node_as`: optional task id to co-locate this task on the same actual node as a currently `attaching` or `running` task. Use this for localhost-bound service tasks such as vLLM. If the reference task is not running or has no resolved node yet, the new task stays queued. Same-node CPU clients and vLLM service tasks are launched with Slurm step overlap so service and client steps can coexist.
- `exclusive_node`: use only when a task cannot share a node.

## Submit Git-Based Commands

### `POST /tasks/git`

Use this when the scheduler should clone or update a Git repo in the account workspace before running a Python entrypoint.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks/git" \
  -F job_name=git-smoke \
  -F repo_url=https://github.com/example/project.git \
  -F git_ref=main \
  -F account_name=account_a \
  -F entrypoint=scripts/run.py \
  -F arguments='--case demo' \
  -F cpus=4 \
  -F memory=8G \
  -F gpus=0
```

Additional fields match `/tasks`, except memory is supplied as `memory` with values such as `4096`, `4096M`, or `8G`.

If `repo_url` matches `git_credentials` in `config/app.yaml`, the scheduler injects the configured deploy key for the task. The assigned account does not need its own `~/.ssh/config` or GitHub key.

### `POST /api/tasks/git`

JSON equivalent of `/tasks/git`:

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks/git" \
  -H 'Content-Type: application/json' \
  --data '{"name":"git-smoke","repo_url":"git@github.com:org/private-project.git","git_ref":"main","entrypoint":"scripts/run.py","cpus":4,"memory_mb":8192}'
```

Optional `git_credential_id` forces a specific configured credential. If omitted, the scheduler matches by `url_patterns`.

## Submit Batch Jobs And Compatibility Jobs

### `POST /jobs`

Use this endpoint for compatibility with older clients and packed-FEA workflows. Normal Git work submitted with `job_mode=python_git` is not launched as a separate Slurm job; it is converted into an attached task equivalent to `/tasks/git` and appears in `/api/tasks`.

Compatibility Git Python job:

```bash
curl -sS -X POST "$SCHEDULER_URL/jobs" \
  -F job_mode=python_git \
  -F repo_url=https://github.com/example/project.git \
  -F git_ref=main \
  -F entrypoint=scripts/run.py \
  -F arguments='--case demo' \
  -F partition=auto \
  -F cpus=4 \
  -F memory=8G \
  -F gpus=0 \
  -F job_name=git-attached-demo
```

Use `/tasks/git` for new clients because the response and status model are clearer. Keep `/jobs job_mode=python_git` only when an existing integration already posts there.

Dynamic packed FEA/RL job:

```bash
curl -sS -X POST "$SCHEDULER_URL/jobs" \
  -F job_mode=dynamic_packed_srun \
  -F remote_path=/remote/project/path \
  -F entrypoint=scripts/run_fea.py \
  -F arguments='--campaign rl-loop-001' \
  -F partition=auto \
  -F time_limit=48:00:00 \
  -F total_simulations=20 \
  -F cpus_per_simulation=4 \
  -F mem_per_simulation_gb=8 \
  -F max_workers_per_job=20 \
  -F max_new_jobs=10 \
  -F job_name=fea-rl
```

`job_mode=packed_srun` and `job_mode=dynamic_packed_srun` still create packed Slurm jobs because they orchestrate many simulation workers inside a batch allocation. Porting these modes to attached-task execution is possible, but it requires moving the packed worker launcher into the warm allocation task wrapper.

## Job And Task Status

### `GET /api/jobs`

```bash
curl -sS "$SCHEDULER_URL/api/jobs"
```

### `GET /api/jobs/{job_id}`

```bash
curl -sS "$SCHEDULER_URL/api/jobs/123"
```

### `GET /api/jobs/{job_id}/remote-file`

Reads a safe relative file path from the job account over SSH. `base=remote_job_dir` is useful for submission diagnostics such as Git clone stderr.

```bash
curl -sS "$SCHEDULER_URL/api/jobs/123/remote-file?base=remote_job_dir&path=submit.stderr.log"
```

The response is capped by `web_remote_file_default_max_bytes` unless `max_bytes` is supplied, and never exceeds `web_remote_file_hard_max_bytes` when that cap is nonzero. Slow SSH reads return `504`.

### `GET /api/tasks`

```bash
curl -sS "$SCHEDULER_URL/api/tasks"
curl -sS "$SCHEDULER_URL/api/tasks?include_diagnostics=true"
```

By default this endpoint returns lightweight task metadata suitable for frequent polling. Use `include_diagnostics=true` only when you need queued capacity fields such as `queue_reason`, `ready_fit_slots`, `pending_fit_slots`, and `inflight_fit_slots`; that mode performs scheduler fit checks and is intentionally heavier.

### `POST /api/tasks`

Creates an attached pool task from a JSON request and returns a polling-ready JSON response. Use this endpoint for service-to-service clients such as Flight.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "flight-crawl-icn-sfo",
    "remote_cwd": "/remote/flight-searcher",
    "command": "python worker.py --payload \"$SLURM_SCHEDULER_PAYLOAD_PATH\"",
    "payload_json": {"from": "ICN", "to": "SFO", "date": "2026-07-01"},
    "required_capability": "conda:flight-searcher",
    "env_profile": "flight-searcher",
    "cpus": 1,
    "memory_mb": 1024,
    "priority": 10,
    "timeout_seconds": 300,
    "dedupe_key": "flight:ICN:SFO:2026-07-01",
    "max_workers_per_node": 200
  }'
```

Co-locate a client task with a running service task's node:

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"name":"vllm-client","remote_cwd":"/remote/project","command":"python call_local_vllm.py","cpus":1,"memory_mb":1024,"same_node_as_task_id":123}'
```

Response:

```json
{
  "task_id": 123,
  "state": "queued",
  "assigned_allocation": null,
  "slurm_job_id": "",
  "created_at": "2026-06-16 06:20:00",
  "urls": {
    "status": "/api/tasks/123",
    "stdout": "/api/tasks/123/stdout",
    "stderr": "/api/tasks/123/stderr",
    "remote_file": "/api/tasks/123/remote-file"
  },
  "deduped": false
}
```

`urls` contains scheduler API endpoints only. It is not an externally reachable URL for a service started by the task.

Fields:

- `payload_json`: optional JSON object, array, or string. The scheduler writes it to `payload.json` under the task remote directory and exports `SLURM_SCHEDULER_PAYLOAD_PATH`.
- `required_capability`: account capability constraint, for example `conda:flight-searcher`.
- `env_profile`: optional shell setup profile, for example `flight-searcher` to activate the matching conda environment.
- `priority`: higher values attach before lower values.
- `timeout_seconds`: nonzero value marks a running task failed with exit code `124` after timeout.
- `dedupe_key`: if another non-terminal task has the same key, the API returns that task instead of creating a duplicate.
- `max_workers_per_node`: baseline per-node worker limit. For `fea_bursty`, the scheduler can exceed this baseline when live `pestat` CPU load and free-memory budget show the node can safely accept more tasks.
- `same_node_as_task_id`: co-locates this task with the referenced running task's actual node. `same_node_as` is accepted as a shorter alias.
- `cleanup_globs`: list (or comma string) of basename patterns, e.g. `["simulation", "aedt_temp"]`. When the task reaches ANY terminal state — completed, failed, cancelled, timed out, or its allocation was lost — the scheduler deletes matching entries directly under the task's working directory. Use this instead of shell-level `rm` at the end of your command: a killed process never reaches its trailing cleanup, but the scheduler sees every exit path. Bare wildcards and path separators are rejected.

### `GET /api/tasks/{task_id}`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123"
curl -sS "$SCHEDULER_URL/api/tasks/123?include_output=true"
curl -sS "$SCHEDULER_URL/api/tasks/123?include_diagnostics=true"
```

The JSON includes `state`, `status`, `exit_code`, `failure_message`, `assigned_allocation`, `slurm_job_id`, stdout/stderr paths, requested `node_name`, actual `allocation_node_name`/`actual_node_name`, `same_node_as_task_id`, and timestamps. With `include_diagnostics=true`, queued tasks also include computed capacity diagnostics and `queue_reason`. With `include_output=true`, it also includes `stdout`, `stderr`, and `result_json`, where `result_json` is parsed from the final JSON object or array in stdout. Included output is read with the requested `output_limit` and the configured web remote-file caps.

The Web UI task detail page is available at `/tasks/{task_id}`. It shows the stored task fields and equivalent JSON/curl/Python examples for submitting the same task again.

### `GET /api/tasks/{task_id}/stdout`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/stdout"
```

### `GET /api/tasks/{task_id}/stderr`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/stderr"
```

### `POST /api/tasks/{task_id}/cancel`

Cancels a queued, attaching, or running attached task. The API marks the task cancelled first and returns quickly; remote wrapper termination is best-effort in the background. Pass `expected_statuses` to make cancellation conditional on the task still having one of the statuses observed by the caller.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks/123/cancel"
curl -sS -X POST "$SCHEDULER_URL/api/tasks/123/cancel?expected_statuses=queued"
```

Response:

```json
{"ok": true, "cancelled": true, "id": 123, "previous_status": "running", "status": "cancelled"}
```

If a conditional cancellation loses a status race, the task is left unchanged:

```json
{"ok": true, "cancelled": false, "id": 123, "previous_status": "running", "status": "running", "reason": "status_mismatch"}
```

### `POST /api/tasks/cancel`

Bulk-cancels tasks matching a name substring and status list, or an explicit id list. `task_ids` takes precedence when supplied (this is what the dashboard's "Cancel selected" checkboxes use). In both modes, `statuses` is checked atomically when each task is cancelled.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks/cancel?name_contains=crypto-sweep&statuses=queued,attaching,running"
curl -sS -X POST "$SCHEDULER_URL/api/tasks/cancel?task_ids=101,102,103"
```

### `GET /api/tasks/summary`

Counts by status, optionally scoped to a campaign name prefix. Use this for progress polling instead of listing every task.

```bash
curl -sS "$SCHEDULER_URL/api/tasks/summary?name_prefix=mft-camp-w1"
```

```json
{"name_prefix": "mft-camp-w1", "total": 400, "statuses": {"queued": 120, "running": 40, "completed": 235, "failed": 5}}
```

`GET /api/tasks` also accepts `limit` and `name_prefix` query parameters for bounded campaign harvesting.

### `POST /api/tasks/{task_id}/priority`

Changes the priority of a queued task (higher runs first; ties are FIFO). Returns `409` for tasks that already left the queue. The dashboard's queued rows expose the same control inline.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks/123/priority" \
  -H "Content-Type: application/json" -d '{"priority": 10}'
```

### `GET /api/tasks/{task_id}/remote-file`

Reads a safe relative file path from the task account over SSH. `base` can be `remote_cwd`, `remote_dir`, `stdout`, `stderr`, `git_workdir`, or `git_repo`.

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/remote-file?base=remote_cwd&path=result.json"
curl -sS "$SCHEDULER_URL/api/tasks/123/remote-file?base=git_repo&path=results/best.json"
curl -sS "$SCHEDULER_URL/api/tasks/123/remote-file?base=remote_cwd&path=logs/vllm.err&tail_lines=100"
curl -sS "$SCHEDULER_URL/api/tasks/123/remote-file?base=remote_cwd&path=logs/vllm.err&max_bytes=20000"
```

If the file does not exist yet, task file endpoints return an empty body instead of failing. `stdout` and `stderr` endpoints support the same tail parameters:

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/stdout?tail_lines=100"
curl -sS "$SCHEDULER_URL/api/tasks/123/stderr?max_bytes=20000"
```

Remote task file responses are capped by `web_remote_file_default_max_bytes` unless `max_bytes` is supplied, and never exceed `web_remote_file_hard_max_bytes` when that cap is nonzero. Slow SSH reads return `504`.

### `GET /api/tasks/{task_id}/remote-files`

Lists matching files below a task base path. Use this when the exact log filename is unknown.

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/remote-files?base=remote_cwd&glob=logs/vllm-scheduler*.err"
```

For `/tasks/git` and `POST /jobs job_mode=python_git`, the scheduler clones into:

```text
<account remote_workspace>/git_tasks/task-<task_id>/repo
```

Use `base=git_repo` to read result files written inside the cloned repository.

Task states:

- `queued`: waiting for matching allocation capacity.
- `attaching`: remote `srun` wrapper was launched.
- `running`: remote wrapper is still running.
- `completed`: command exited with code `0`.
- `failed`: command exited nonzero or attach failed.

## Token Usage

### `POST /token-usage`

```bash
curl -sS -X POST "$SCHEDULER_URL/token-usage" \
  -F provider=codex \
  -F project=slurm_scheduler \
  -F input_tokens=1000 \
  -F output_tokens=500 \
  -F reset_cycle=2026-W24 \
  -F note='implementation run'
```

Fields:

- `provider`: `codex`, `claude`, `openai`, or another provider label.
- `project`: project or repo name.
- `input_tokens`: read/input tokens.
- `output_tokens`: write/output tokens.
- `total_tokens`: optional override; if omitted, the scheduler stores input plus output.
- `reset_cycle`: quota/reset window label such as `2026-W24`.
- `note`: short run description.

### `GET /api/token-usage`

```bash
curl -sS "$SCHEDULER_URL/api/token-usage"
```

The dashboard renders the same records as a graph and table.
