# API Reference

Korean summary: 이 문서는 LLM이나 사람이 Web UI 없이 scheduler를 호출할 때 쓰는 HTTP API 기준입니다. 모든 `POST` form endpoint는 성공 시 대시보드(`/`)로 `303` redirect를 반환합니다.

Set the endpoint once:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
```

## Health And Monitoring

### `GET /api/health`

Checks that the FastAPI service and local database are reachable.

```bash
curl -sS "$SCHEDULER_URL/api/health"
```

Typical response:

```json
{"ok": true, "accounts": 6, "jobs": 12, "tasks": 0, "allocations": 7}
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
```

The response includes total `fit_slots` and per-allocation free CPU, memory, GPU, and fit slots.

Use:

- `scheduler_free_gpus` for immediate placement.
- `cluster_free_gpus` to estimate whether scale-out can acquire another GPU allocation.
- `cluster_used_gpus` to understand capacity already consumed by all Slurm users.

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
- `gpus`: GPU count, normally `0` or `1`.
- `gpu_model`: optional normalized model such as `a6000ada` or `a6000`. Ordered candidates such as `a6000ada,a6000` are accepted.
- `partition`: `auto` or a specific Slurm partition.
- `node_name`: optional specific node constraint.
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

### `GET /api/tasks`

```bash
curl -sS "$SCHEDULER_URL/api/tasks"
```

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

Fields:

- `payload_json`: optional JSON object, array, or string. The scheduler writes it to `payload.json` under the task remote directory and exports `SLURM_SCHEDULER_PAYLOAD_PATH`.
- `required_capability`: account capability constraint, for example `conda:flight-searcher`.
- `env_profile`: optional shell setup profile, for example `flight-searcher` to activate the matching conda environment.
- `priority`: higher values attach before lower values.
- `timeout_seconds`: nonzero value marks a running task failed with exit code `124` after timeout.
- `dedupe_key`: if another non-terminal task has the same key, the API returns that task instead of creating a duplicate.
- `max_workers_per_node`: caps the number of attaching/running tasks on the chosen allocation for this task.

### `GET /api/tasks/{task_id}`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123"
curl -sS "$SCHEDULER_URL/api/tasks/123?include_output=true"
```

The JSON includes `state`, `status`, `exit_code`, `failure_message`, `assigned_allocation`, `slurm_job_id`, stdout/stderr paths, and timestamps. With `include_output=true`, it also includes `stdout`, `stderr`, and `result_json`, where `result_json` is parsed from the final JSON object or array in stdout.

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

Cancels a queued, attaching, or running attached task. The API marks the task cancelled first and returns quickly; remote wrapper termination is best-effort in the background.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks/123/cancel"
```

Response:

```json
{"ok": true, "id": 123, "previous_status": "running", "status": "cancelled"}
```

### `POST /api/tasks/cancel`

Bulk-cancels tasks matching a name substring and status list. This is intended for external agents that accidentally submitted duplicate task batches.

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks/cancel?name_contains=crypto-sweep&statuses=queued,attaching,running"
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
