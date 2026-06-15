# LLM Operator Guide

This document is written for an LLM agent running on another computer. Follow the steps in order. Do not assume local Slurm access from the client machine; the scheduler host owns SSH keys and Slurm submission.

Korean summary: 원격 LLM은 Slurm 명령을 직접 실행하지 말고 scheduler API만 호출합니다. 먼저 health와 capacity를 확인한 뒤 `/tasks`, `/tasks/git`, `/token-usage`를 사용하세요.

## Endpoint

Scheduler URL:

```text
http://<scheduler-host>:8000/
```

Use these checks before submitting work:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
curl "$SCHEDULER_URL/api/health"
curl "$SCHEDULER_URL/api/accounts/status"
curl "$SCHEDULER_URL/api/allocations"
curl "$SCHEDULER_URL/api/gpu-capacity"
```

If `/api/health` fails, do not submit work. See `docs/remote-access.md`.

For full field-level API details, read `docs/API.md`. For copy-paste examples, read `docs/EXAMPLES.md`.

## Submit CPU Work

Use attached tasks for commands that already exist on the cluster filesystem:

```bash
curl -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-case-001 \
  -F remote_cwd=/remote/project/path \
  -F command='python run_fea.py --case case001' \
  -F cpus=4 \
  -F memory_mb=8192 \
  -F gpus=0
```

The scheduler first tries CPU allocations. If those are full, it may borrow CPU from GPU allocations while preserving GPU CPU reserve. Also, when `cpu_pool_allow_gpu_partitions` is enabled, the scheduler itself may place a CPU warm pool on a GPU partition if that partition has a stronger CPU profile. Agents should still submit CPU work as CPU work and let the scheduler choose placement.

If the command needs an environment that exists only on one account, request the account capability and profile:

```bash
curl -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-r1jae-env \
  -F remote_cwd=/remote/project/path \
  -F command='python run_fea.py --case case001' \
  -F account_name=account_a \
  -F required_capability=conda:pyaedt2026v1 \
  -F env_profile=pyaedt2026v1 \
  -F cpus=4 \
  -F memory_mb=8192
```

The scheduler will not place that task on accounts missing the requested capability/profile.

Use `account_name` when the account itself is the hard constraint. Preferred warm-pool accounts are only preferences; `account_name=account_a` keeps the request on that account or queued if the account has no job slot.

## Submit GPU Work

Use `a6000ada` unless the workload explicitly requires `a6000`.

```bash
curl -X POST "$SCHEDULER_URL/tasks" \
  -F name=llm-inference \
  -F remote_cwd=/remote/llm/project \
  -F command='python serve_or_run.py --model /models/model-name' \
  -F account_name=account_a \
  -F env_setup='module load cuda' \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada \
  -F partition=auto
```

For a specific node:

```bash
curl -X POST "$SCHEDULER_URL/tasks" \
  -F name=llm-specific-node \
  -F remote_cwd=/remote/llm/project \
  -F command='python run.py' \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada \
  -F partition=gpu3 \
  -F node_name=gpu-node-name
```

## Observe Work

```bash
curl "$SCHEDULER_URL/api/tasks"
curl "$SCHEDULER_URL/api/allocations"
curl "$SCHEDULER_URL/api/gpu-capacity"
```

Interpret GPU capacity as:

- `scheduler_free_gpus`: GPUs already held by the scheduler and immediately usable.
- `cluster_free_gpus`: GPUs observed as free in Slurm and likely usable for scale-out.
- `cluster_used_gpus`: GPUs already allocated by all Slurm users.

For `cpu2`, the scheduler intentionally limits itself to one scheduler job per node. If `cpu2` is full, CPU work may remain queued until an idle node appears or the auto partition selector finds another suitable CPU partition.

Task states:

- `queued`: waiting for matching allocation capacity.
- `attaching`: scheduler launched the remote `srun` wrapper.
- `running`: remote wrapper is still running.
- `completed`: command exited with code 0.
- `failed`: command exited non-zero or attach failed.

## Record Token Usage

```bash
curl -X POST "$SCHEDULER_URL/token-usage" \
  -F provider=codex \
  -F project=slurm_scheduler \
  -F input_tokens=1000 \
  -F output_tokens=500 \
  -F reset_cycle=2026-W24 \
  -F note='implementation run'
```

Read records:

```bash
curl "$SCHEDULER_URL/api/token-usage"
```

## Rules For Agents

- Prefer `/tasks` for existing remote directories and long-running iterative work.
- Prefer `/tasks/git` for Git-backed work. `POST /jobs job_mode=python_git` is only a compatibility alias and will appear as an attached task.
- Prefer `dynamic_packed_srun` jobs for FEA/RL batches with many simulations.
- Check `/api/gpu-capacity` before GPU work.
- Do not request `exclusive_node=true` unless a workload cannot share the node.
- Do not force `partition` or `node_name` for CPU-only work unless the workload requires it. Let the scheduler decide whether CPU, GPU-partition CPU pool, or safe GPU-allocation borrowing is appropriate.
- Prefer `a6000ada` for LLM work. Use `a6000` only when A6000ADA is unavailable or explicitly required.
