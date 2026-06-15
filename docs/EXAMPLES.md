# Examples

Korean summary: 이 문서는 바로 복사해서 실행할 수 있는 scheduler 사용 예제입니다. 실제 경로, repo URL, model path는 환경에 맞게 바꾸세요.

Set the scheduler URL:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
```

## 1. Check Service Health

```bash
curl -sS "$SCHEDULER_URL/api/health"
curl -sS "$SCHEDULER_URL/api/accounts/status"
curl -sS "$SCHEDULER_URL/api/allocations"
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

If `/api/health` fails, stop and read `docs/TROUBLESHOOTING.md`.

## 2. CPU FEA Task In An Existing Remote Directory

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-case-001 \
  -F remote_cwd=/remote/project/path \
  -F command='python run_fea.py --case case001 --out results/case001.json' \
  -F account_name=account_a,account_b \
  -F cpus=4 \
  -F memory_mb=8192 \
  -F gpus=0
```

Observe:

```bash
curl -sS "$SCHEDULER_URL/api/tasks"
curl -sS "$SCHEDULER_URL/api/allocations"
```

Memory guidance:

- `memory_mb` should be the expected peak memory plus headroom.
- The scheduler subtracts it from the selected warm allocation's free memory.
- Slurm may kill the step if the process exceeds the requested memory, depending on cluster policy.
- If you are unsure, start with `8192` for light CPU work and increase based on real failures or solver reports.

## 3. CPU Task Requiring A Specific Conda Environment

Use this when the environment exists on only one or a few accounts.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=fea-pyaedt \
  -F remote_cwd=/remote/project/path \
  -F command='python run_ansys_case.py --case case001' \
  -F required_capability=conda:pyaedt2026v1 \
  -F env_profile=pyaedt2026v1 \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=0
```

The scheduler will not place this task on accounts that do not declare the requested capability and profile.

If you must force one Slurm account rather than just a capability, add `-F account_name=account_a`. The request will stay queued instead of falling back to another account.

## 4. Git-Based CPU Task

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks/git" \
  -F job_name=git-cpu-demo \
  -F repo_url=https://github.com/example/project.git \
  -F git_ref=main \
  -F account_name=account_a \
  -F entrypoint=scripts/run.py \
  -F arguments='--case demo' \
  -F cpus=4 \
  -F memory=8G \
  -F gpus=0
```

## 5. A6000ADA GPU Task

Use `a6000ada` by default for LLM inference unless the workload explicitly needs A6000.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=llm-a6000ada \
  -F remote_cwd=/remote/llm/project \
  -F command='python run_inference.py --model /models/model-name --prompt-file prompts/input.txt' \
  -F account_name=account_a \
  -F env_profile=pytorch_cuda118 \
  -F required_capability=conda:pytorch_cuda118 \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada,a6000 \
  -F partition=auto
```

Check capacity before and after:

```bash
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
curl -sS "$SCHEDULER_URL/api/tasks"
```

## 6. Specific GPU Partition Or Node

Only pin a node when the workload truly requires that node.

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=llm-specific-node \
  -F remote_cwd=/remote/llm/project \
  -F command='python run.py' \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada,a6000 \
  -F partition=gpu3 \
  -F node_name=n071
```

If the node is busy, the task may stay queued until capacity appears.

## 7. Dynamic Packed FEA/RL Batch

Refresh live cluster information first:

```bash
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
```

Submit a 20-simulation batch:

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

The scheduler uses stored `pestat` data to plan allocation shape and then runs packed workers inside Slurm.

## 8. Record Token Usage

```bash
curl -sS -X POST "$SCHEDULER_URL/token-usage" \
  -F provider=codex \
  -F project=slurm_scheduler \
  -F input_tokens=1000 \
  -F output_tokens=500 \
  -F reset_cycle=2026-W24 \
  -F note='documentation update'

curl -sS "$SCHEDULER_URL/api/token-usage"
```

## 9. Use The Shell Scripts

The same patterns are available as scripts:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000

bash examples/health.sh
bash examples/submit_cpu_task.sh
bash examples/submit_gpu_a6000ada_task.sh
bash examples/submit_specific_gpu_node_task.sh
bash examples/submit_git_task.sh
bash examples/submit_dynamic_packed_job.sh
bash examples/record_token_usage.sh
```

Each script accepts environment variables for paths, repo URLs, and resource requests.
