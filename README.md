# slurm_scheduler

Web-based Slurm scheduler for CPU FEA/RL batches, remote command tasks, and GPU/LLM work.

This project keeps Slurm allocation jobs warm and attaches user work with `srun --jobid`, so lightweight jobs do not consume one account-limited Slurm job slot each. Git-backed work is treated as an attached task by default, while packed simulation batches remain available for compatibility.

This repository is public-safe by design. Real hostnames, account names, IP addresses, private key names, Slurm job IDs, passwords, and local `config/*.yaml` files must stay outside Git.

## What It Does

- Maintains warm CPU and GPU allocation pools.
- Attaches remote commands to existing allocations through `/tasks`.
- Clones/updates Git repos and runs entrypoints through `/tasks/git`.
- Runs dynamic packed FEA/RL batches through `/jobs` with `dynamic_packed_srun`.
- Tracks accounts, jobs, tasks, allocations, GPU capacity, and token usage in the Web UI and API.
- Supports account-local software routing through `capabilities` and `env_profiles`.

## Quickstart

```bash
git clone https://github.com/Schwalbe262/slurm_scheduler.git
cd slurm_scheduler

sudo apt update
sudo apt install -y python3.12-venv python3-pip

cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
```

Edit `config/accounts.yaml` with your real Slurm login accounts, SSH key paths, account job limits, and optional environment profiles.

Then run:

```bash
bash scripts/setup_and_smoke.sh
. .venv/bin/activate
python3 -m slurm_scheduler
```

Open the configured bind address:

```text
http://127.0.0.1:8000/
```

For another machine on a trusted LAN/VPN/Tailscale network, set:

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
curl -sS "$SCHEDULER_URL/api/health"
curl -sS "$SCHEDULER_URL/api/allocations"
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

The web UI has no built-in login page. Keep it on a trusted private network or put it behind an authenticated reverse proxy.

## Choose The Right Submission Path

- Use `POST /tasks` when the project already exists on the cluster filesystem.
- Use `POST /tasks/git` when the scheduler should clone/update a Git repo before running a Python entrypoint.
- Use `POST /jobs` with `job_mode=python_git` only as a compatibility alias for `/tasks/git`; it still creates an attached task.
- Use `POST /jobs` with `job_mode=dynamic_packed_srun` for many simulation cases in one packed Slurm job.
- Use the Web UI for interactive operation and quick status checks.

## How Clients Submit Work

Clients do not call Slurm directly. They send `multipart/form-data` HTTP requests to the scheduler, then poll the scheduler APIs for placement and result paths.

Basic client flow:

1. Set the scheduler URL.
2. Check health and capacity.
3. Choose `/tasks` or `/tasks/git` for normal virtual jobs. Use `/jobs` only for compatibility or packed simulation batches.
4. Include resource requests such as `cpus`, `memory_mb` or `memory`, `gpus`, and `gpu_model`.
5. Include `account_name` only when the job must stay on a specific Slurm account. Use comma-separated values such as `account_a,account_b` when either account is acceptable in that preference order.
6. Poll `/api/tasks`, `/api/jobs`, and `/api/allocations`.
7. Read task output through `/api/tasks/{task_id}/stdout` or `/api/tasks/{task_id}/remote-file`.
8. For attached task output, read `/api/tasks/{task_id}/stdout`, `/api/tasks/{task_id}/stderr`, or `/api/tasks/{task_id}/remote-file`.

```bash
export SCHEDULER_URL=http://<scheduler-host>:8000
curl -sS "$SCHEDULER_URL/api/health"
curl -sS "$SCHEDULER_URL/api/accounts/status"
curl -sS "$SCHEDULER_URL/api/allocations"
curl -sS "$SCHEDULER_URL/api/gpu-capacity"
```

Use `/tasks` for a project that already exists on the cluster account:

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=case-001 \
  -F remote_cwd=/remote/project/path \
  -F command='python run.py --case case001 --out results/case001.json' \
  -F account_name=account_a,account_b \
  -F cpus=4 \
  -F memory_mb=8192 \
  -F gpus=0
```

Use `/tasks/git` when the scheduler should clone or update a Git repo before running a Python entrypoint:

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks/git" \
  -F job_name=git-case-001 \
  -F repo_url=git@github.com-project:org/private-repo.git \
  -F git_ref=main \
  -F entrypoint=scripts/run.py \
  -F arguments='--case case001' \
  -F account_name=account_a,account_b \
  -F cpus=4 \
  -F memory=8G \
  -F gpus=0
```

Private Git repos must be cloneable from the selected cluster account. Prefer a read-only GitHub deploy key and an SSH alias in that account's `~/.ssh/config`; avoid putting personal access tokens in scheduler form fields.

GPU model constraints also accept comma-separated ordered candidates. For example, `gpu_model=a6000ada,a6000` tries A6000 ADA first and can fall back to A6000 if that is the available matching pool or node.

`POST /jobs` with `job_mode=python_git` is kept for old clients, but it is routed into the attached-task scheduler and appears under Attached Tasks. This is the intended "virtual job" path: the client submits a job-like request, and the scheduler places it inside an existing warm allocation.

Use `/jobs` with `dynamic_packed_srun` when the scheduler should split many simulation cases into packed Slurm jobs:

```bash
curl -sS -X POST "$SCHEDULER_URL/jobs" \
  -F job_mode=dynamic_packed_srun \
  -F remote_path=/remote/project/path \
  -F entrypoint=scripts/run_fea.py \
  -F arguments='--campaign sweep-001' \
  -F account_name=account_a \
  -F total_simulations=20 \
  -F cpus_per_simulation=4 \
  -F mem_per_simulation_gb=8 \
  -F max_workers_per_job=20 \
  -F max_new_jobs=10 \
  -F time_limit=48:00:00 \
  -F partition=auto
```

Useful polling commands:

```bash
curl -sS "$SCHEDULER_URL/api/tasks"
curl -sS "$SCHEDULER_URL/api/jobs"
curl -sS "$SCHEDULER_URL/api/allocations"
curl -sS "$SCHEDULER_URL/api/tasks/<task_id>/stdout"
curl -sS "$SCHEDULER_URL/api/tasks/<task_id>/remote-file?base=remote_cwd&path=results/case001.json"
curl -sS "$SCHEDULER_URL/api/tasks/<task_id>/remote-file?base=git_repo&path=results/best.json"
curl -sS -X POST "$SCHEDULER_URL/api/tasks/cancel?name_contains=crypto-sweep&statuses=queued,attaching,running"
curl -sS "$SCHEDULER_URL/api/jobs/<job_id>/remote-file?base=remote_job_dir&path=submit.stderr.log"
```

The scheduler automatically cleans old scheduler-created remote directories such as `task-*`, `job-*`, and `allocation-*` under each account's `remote_workspace`. By default, finished task/job artifacts are kept for 7 days and closed allocation artifacts for 1 day. Read stdout, stderr, and result files through the API before the cleanup TTL expires.

## CPU And Memory Requests

`cpus` and `memory_mb` are scheduling requests for an attached task. They are not a Python virtual environment or a preallocated RAM block. The process uses physical memory only when it actually allocates memory, but the scheduler reserves that amount from the warm allocation's available capacity and Slurm may enforce the limit with cgroups/OOM handling depending on cluster configuration.

If you do not know memory usage:

- Start with a conservative estimate such as `memory_mb=8192` for a small CPU task.
- For ANSYS/FEA, use the best observed peak RSS or solver memory report from a similar case, then add headroom.
- If tasks fail with OOM or Slurm memory errors, increase `memory_mb`.
- If allocations look underused and many tasks are queued only because of memory, lower future requests after confirming actual usage.
- Do not set memory unrealistically high; it reduces how many tasks can attach to the same warm allocation.

## Minimal API Examples

Existing remote directory:

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

JSON pool task for service clients:

```bash
curl -sS -X POST "$SCHEDULER_URL/api/tasks" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "flight-crawl-icn-sfo",
    "remote_cwd": "/remote/flight-searcher",
    "command": "python worker.py --payload \"$SLURM_SCHEDULER_PAYLOAD_PATH\"",
    "payload_json": {"from": "ICN", "to": "SFO"},
    "required_capability": "flight-crawl",
    "cpus": 1,
    "memory_mb": 1024,
    "priority": 10,
    "timeout_seconds": 300,
    "dedupe_key": "flight:ICN:SFO",
    "max_workers_per_node": 200
  }'

curl -sS "$SCHEDULER_URL/api/tasks/<task_id>?include_output=true"
```

Git-based task:

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

GPU task:

```bash
curl -sS -X POST "$SCHEDULER_URL/tasks" \
  -F name=llm-a6000ada \
  -F remote_cwd=/remote/llm/project \
  -F command='python run_inference.py --model /models/model-name' \
  -F cpus=8 \
  -F memory_mb=32768 \
  -F gpus=1 \
  -F gpu_model=a6000ada \
  -F partition=auto
```

Dynamic packed FEA/RL batch:

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

Token usage:

```bash
curl -sS -X POST "$SCHEDULER_URL/token-usage" \
  -F provider=codex \
  -F project=slurm_scheduler \
  -F input_tokens=1000 \
  -F output_tokens=500 \
  -F reset_cycle=2026-W24 \
  -F note='example run'
```

## Shell Examples

All scripts require `SCHEDULER_URL`:

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

Override inputs with environment variables such as `REMOTE_CWD`, `TASK_COMMAND`, `REPO_URL`, `GPU_MODEL`, `CPUS`, and `MEMORY_MB`.

## Configuration

Local runtime files are ignored by Git:

```bash
cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
```

Example account:

```yaml
accounts:
  - name: account_a
    host: login.example.edu
    port: 22
    username: account_a
    private_key_path: "secrets/cluster/account_a.pem"
    remote_workspace: "slurm_scheduler"
    max_running_jobs: 10
    max_pending_jobs: 10
    max_total_jobs: 10
    capabilities: ["conda:pyaedt2026v1"]
    env_profiles:
      pyaedt2026v1: |
        source ~/miniconda3/etc/profile.d/conda.sh
        conda activate pyaedt2026v1
```

Example scheduler policy:

```yaml
cluster_refresh_interval_seconds: 120
allocation_cpus: 64
allocation_pending_timeout_seconds: 1800
allocation_pending_backoff_seconds: 1800
cpu_pool_allow_gpu_partitions: true
warm_pool_preferred_accounts: ["account_a"]
gpu_warm_pool_preferred_accounts: ["account_a"]
single_job_per_node_partitions: ["cpu2"]
gpu_prewarm:
  enabled: true
  preferred_models: ["a6000ada", "a6000"]
  min_warm_allocations: 1
  max_warm_allocations: 3
  gpus_per_allocation: 2
cleanup:
  enabled: true
  interval_seconds: 3600
  finished_task_ttl_seconds: 604800
  finished_job_ttl_seconds: 604800
  closed_allocation_ttl_seconds: 86400
```

Read [docs/CONFIG.md](docs/CONFIG.md) before changing scheduling policy.

## Documentation Map

- [docs/API.md](docs/API.md): HTTP endpoints, form fields, and examples.
- [docs/EXAMPLES.md](docs/EXAMPLES.md): Copy-paste operational recipes.
- [docs/CONFIG.md](docs/CONFIG.md): App and account configuration.
- [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md): Network, Slurm, inventory, and task issues.
- [docs/scheduling-principles.md](docs/scheduling-principles.md): CPU/GPU/mixed-capacity scheduling policy.
- [docs/gpu-scheduling.md](docs/gpu-scheduling.md): GPU model priority, prewarm, and capacity meaning.
- [docs/remote-access.md](docs/remote-access.md): Trusted remote access through LAN/VPN/Tailscale.
- [docs/llm-operator-guide.md](docs/llm-operator-guide.md): Short guide for remote LLM agents.
- [docs/ROADMAP.md](docs/ROADMAP.md): Recommended future improvements.

## LLM Operator Prompt

Give a remote LLM agent this repository link plus:

```text
Use the Slurm scheduler documented in this repository.
Set SCHEDULER_URL to http://<scheduler-host>:8000.
First call /api/health, /api/accounts/status, /api/allocations, and /api/gpu-capacity.
Use /tasks for existing remote directories, /tasks/git for Git-based work, and /jobs dynamic_packed_srun only for packed simulation batches.
Do not assume direct Slurm access from the client machine.
Do not submit GPU work until /api/gpu-capacity has been checked.
```

## Live Checks

After local config and keys are prepared:

```bash
python3 scripts/check_ssh.py --account account_a
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
python3 scripts/live_sleep_test.py --account account_a --count 1 --partition cpu_partition
python3 scripts/live_distributed_sleep_test.py --count 6 --partition cpu_partition
```

Live checks submit real Slurm jobs.

## Tests

```bash
python3 -m unittest discover -s tests
python3 -m compileall slurm_scheduler tests scripts
bash -n scripts/*.sh examples/*.sh
git diff --check
```

## Recommended Next Improvements

- Add placement decision traces for accounts, partitions, nodes, and allocations.
- Add inventory freshness warnings in the Web UI.
- Add a dry-run placement API for LLM agents and operators.
- Add optional authentication before exposing write endpoints outside a private network.
