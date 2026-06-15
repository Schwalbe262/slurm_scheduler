# Slurm Scheduler Handoff

## Original Plan

Build a local PC/server web service that distributes Python Slurm jobs across multiple SSH accounts.

- Users register jobs from a web UI.
- Job code is delivered through Git checkout on the remote cluster account.
- The scheduler checks account capacity and automatically submits to the account with the most free slots.
- SQLite stores jobs, status, Slurm IDs, paths, inventory, and token usage records.
- SSH key contents are never stored in the DB; config stores only key paths.
- The web UI currently has no login page; deploy it only on a trusted network or behind a separate authenticated proxy.
- Each account is capped by `max_total_jobs`, with the local deployment using a 10-job account limit.

## Current Implementation

Implemented in this repository:

- FastAPI app factory in `slurm_scheduler/app.py`
- SQLite schema and repository methods in `slurm_scheduler/db.py`
- account/app YAML config loaders in `slurm_scheduler/config.py`
- SSH and Slurm adapter in `slurm_scheduler/slurm.py`
- cluster inventory parsing and partition ranking in `slurm_scheduler/inventory.py`
- background scheduling loop in `slurm_scheduler/scheduler.py`
- Jinja web UI templates in `templates/`
- live-check helper scripts in `scripts/`
- unit tests in `tests/test_core.py`

The web UI supports:

- job submission
- job list and detail pages
- account capacity display
- account storage usage display when configured
- partition ranking display
- cancellation through `scancel`
- token usage recording by provider/project/reset cycle
- token usage time-axis SVG chart

## Latest Session Notes

As of the latest handoff update:

- The working tree was clean before editing this file.
- The local web server was running through the smoke-test virtualenv with `python -m slurm_scheduler`.
- The dashboard is served directly at `/`; `/login` is intentionally absent.
- Local SQLite job history was checked and contained no scheduler-managed jobs at that time.
- `scancel` is only reachable through the manual web cancel route:
  - `POST /jobs/{job_id}/cancel`
  - `Scheduler.cancel(...)`
  - `SlurmAccountClient.cancel(...)`
- The scheduler loop does not currently auto-cancel jobs based on age, walltime, account name, or queue pressure.
- Periodic account refresh uses `squeue`; submitted job refresh uses `squeue`/`sacct`; optional storage refresh uses cached `du -sk`.

If a cluster job is reported as cancelled externally, first check Slurm accounting from the affected account:

```bash
sacct -u "$USER" -X -o JobID,JobName,State,ExitCode,Reason,Submit,Start,End
```

Do not record the resulting real job IDs, account names, or hostnames in tracked files.

## Sensitive Data Policy

The Git repository must remain sanitized. Do not commit:

- real account names
- real hostnames or IP addresses
- real SFTP paths
- private key filenames tied to real accounts
- passwords
- Slurm job IDs from live runs
- local `config/app.yaml` or `config/accounts.yaml`
- anything under `secrets/` or `data/`

Use sanitized examples in tracked files and keep real deployment values in ignored local config or environment variables.

## Token Usage Tracking

Token usage is stored in the `token_usage` SQLite table.

Fields:

- `provider`: `codex` now, `claude` later
- `project`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- `reset_cycle`
- `recorded_at`
- `note`

The current implementation records usage manually through the web form and exposes it at `/api/token-usage`. A future Claude/Codex integration can insert rows automatically using the same DB method or API.

## Inventory And Ranking

Inventory is refreshed by:

```bash
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/refresh_pestat.py --account account_a
```

It stores sanitized node inventory fields in SQLite:

- node name
- partition
- CPU count
- memory
- GPU model/count
- node state
- representative CPU profile fields

When a job uses `partition=auto`, scheduler behavior is:

- CPU-only job: choose the highest-ranked CPU partition from stored inventory.
- GPU job: choose the highest-ranked GPU partition from stored inventory.
- If inventory is empty, fall back to generic CPU/GPU defaults.

`dynamic_packed_srun` uses cached `pestat` rows to size each allocation dynamically. For each candidate node it computes available workers from free Slurm CPUs, CPU load, free memory, `cpus_per_simulation`, and `mem_per_simulation_gb`.

The generated packed allocation script starts with a conservative worker count and launches simulation subprocesses inside one Slurm allocation. It monitors `os.getloadavg()` and `/proc/meminfo`; if CPU load and memory leave room, it raises the worker limit up to `max_workers_per_job`.

Persistent allocation lifecycle support is now implemented in the scheduler loop:

- keep at least one warm allocation open
- attach remote-command tasks with `srun --jobid`
- prewarm another allocation when queued demand cannot fit or pool usage is high
- stop assigning new work to allocations after about 36 hours
- close empty draining allocations immediately so account job slots are released
- force-clean stuck allocations around 39 hours old
- scale in extra warm allocations after an idle interval

The older packed job mode still exists. New persistent allocation work uses the `allocations` and `tasks` tables and the remote-command task form/API.

GPU ranking is encoded in `slurm_scheduler/inventory.py`. CPU profiles should be updated in local code/config only with non-sensitive labels if the repository is public.

## Local Setup

```bash
cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
```

Put real accounts, hosts, and key paths into ignored `config/accounts.yaml`.

The helper below performs venv creation, dependency installation, and FastAPI route import smoke testing:

```bash
bash scripts/setup_and_smoke.sh
```

It requires `python3.12-venv` and `python3-pip`.

## Autostart

Runtime entrypoint:

```bash
bash scripts/start_web.sh
```

Autostart options:

- `scripts/install_user_systemd.sh` installs a user systemd service when user systemd is available.
- `scripts/install_windows_startup.ps1` installs a Windows logon scheduled task that starts WSL and runs the web service.
- `scripts/install_windows_portproxy.ps1` must be run from Administrator PowerShell to expose the WSL web server to other internal-network machines.

The current Codex sandbox could not access the user systemd bus, so service installation must be run from a normal terminal/PowerShell session.

## Verification So Far

Local verification that does not require FastAPI dependencies:

```bash
python3 -m unittest discover -s tests
python3 -m compileall slurm_scheduler tests scripts
bash -n scripts/setup_and_smoke.sh scripts/mount_sftp_drives.sh scripts/unmount_sftp_drives.sh
```

Live verification was performed in the local environment with ignored config and keys. Details are intentionally not recorded here because this file is tracked.

Latest local checks that should be rerun after code edits:

```bash
python3 -m unittest discover -s tests
python3 -m compileall slurm_scheduler scripts
bash -n scripts/setup_and_smoke.sh scripts/mount_sftp_drives.sh scripts/unmount_sftp_drives.sh scripts/start_web.sh
```

Before committing, run a targeted sensitive-value scan using the real local values as patterns. Keep the command itself out of tracked docs if it contains real values.

## Recommended Next Work

- Live-test persistent allocations on the cluster with a harmless sleep command before running ANSYS workloads.
- Add a config flag that disables manual `scancel` unless explicitly enabled, or require a stronger confirmation for manual cancellation.
- Add richer task log browsing for remote-command tasks.
- Add per-simulation status rows so the UI can show running/completed/failed simulation counts inside each packed allocation.
- Add live log links for `slurm-%j.out`, `slurm-%j.err`, and `simul_log/*`.
- Move `pestat` refresh into a background task instead of relying only on manual `scripts/refresh_pestat.py`.

## GitHub Hygiene

Before committing:

```bash
git status --short --ignored
rg -n "real-secret-patterns-here" -g '!secrets/**' -g '!config/app.yaml' -g '!config/accounts.yaml'
```

If sensitive values are accidentally pushed, rewrite the published branch history and force-push a sanitized commit immediately.
