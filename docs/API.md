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

## Submit Direct Slurm Jobs

### `POST /jobs`

Use direct jobs for compatibility with the original Git-job and packed-FEA workflows. Attached `/tasks` are preferred for iterative remote commands.

Minimal Git Python job:

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
  -F job_name=git-direct-demo
```

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

### `GET /api/tasks/{task_id}`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123"
```

### `GET /api/tasks/{task_id}/stdout`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/stdout"
```

### `GET /api/tasks/{task_id}/stderr`

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/stderr"
```

### `GET /api/tasks/{task_id}/remote-file`

Reads a safe relative file path from the task account over SSH. `base` can be `remote_cwd`, `remote_dir`, `stdout`, or `stderr`.

```bash
curl -sS "$SCHEDULER_URL/api/tasks/123/remote-file?base=remote_cwd&path=result.json"
```

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
