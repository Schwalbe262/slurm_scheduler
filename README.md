# slurm_scheduler

Web-based Slurm scheduler for CPU FEA/RL batches, remote command tasks, and GPU/LLM work.

This project keeps Slurm allocation jobs warm and attaches user work with `srun --jobid`, so lightweight jobs do not consume one account-limited Slurm job slot each. It also keeps compatibility with direct Git-based Slurm jobs and packed simulation batches.

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
- Use `POST /jobs` with `job_mode=dynamic_packed_srun` for many simulation cases in one Slurm job.
- Use the Web UI for interactive operation and quick status checks.

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
Use /tasks for existing remote directories, /tasks/git for Git-based work, and /jobs dynamic_packed_srun for simulation batches.
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
