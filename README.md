# slurm_scheduler

Web-based Slurm job scheduler for distributing Python jobs across multiple SSH accounts.

This repository intentionally contains only sanitized examples. Real hostnames, account names, private key names, SFTP paths, passwords, job IDs, and local config files must stay outside Git.

## Setup

Install Python venv support on Ubuntu/WSL if needed:

```bash
sudo apt update
sudo apt install -y python3.12-venv python3-pip
```

Create local config from sanitized examples:

```bash
cp config/app.example.yaml config/app.yaml
cp config/accounts.example.yaml config/accounts.yaml
python3 -m slurm_scheduler.security '<admin-password>'
```

Set `admin_username` and `admin_password_hash` only in ignored `config/app.yaml`. Edit `config/accounts.yaml` with the real cluster accounts and key paths. `config/app.yaml`, `config/accounts.yaml`, `data/`, and `secrets/` are ignored by Git.

You can run setup and a FastAPI route import smoke test with:

```bash
bash scripts/setup_and_smoke.sh
```

## Run

```bash
. .venv/bin/activate
python3 -m slurm_scheduler
```

Open the configured bind address, default `http://127.0.0.1:8000`.

To use a non-default config path:

```bash
SLURM_SCHEDULER_CONFIG=/path/to/app.yaml python3 -m slurm_scheduler
```

## Autostart

For Linux systems with user systemd:

```bash
bash scripts/install_user_systemd.sh
systemctl --user status slurm-scheduler.service
```

For WSL on Windows, install a Windows logon task from PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install_windows_startup.ps1
```

The startup task launches:

```bash
bash scripts/start_web.sh
```

and writes logs to `logs/web.log` when started through the Windows task.

## Account Config

Example:

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
    storage_path: "slurm_scheduler"
    storage_quota_gb: 0
```

`max_total_jobs: 10` caps open jobs per account. The scheduler treats `running + pending >= max_total_jobs` as full.

Storage monitoring is optional. Set `storage_quota_gb` and `storage_path` if you want dashboard usage tracking. The scheduler uses `du -sk` on `storage_path` and caches snapshots for `poll_interval_seconds`, so avoid pointing it at a huge home directory unless a cluster quota command is integrated later.

## Job Model

Jobs are submitted from the web UI as:

- Git repository URL
- branch, tag, or commit
- Python entrypoint
- arguments
- optional environment setup commands
- Slurm resources such as partition, time, CPUs, memory, and GPUs

When the partition field is `auto`, the scheduler uses stored node inventory to choose a partition. CPU jobs prefer the strongest CPU profile in the inventory. GPU jobs prefer the configured GPU ranking in `slurm_scheduler/inventory.py`.

## SFTP Helpers

SFTP helpers require real values through command-line arguments or environment variables.

Check SFTP:

```bash
SFTP_HOST=<sftp-host> SFTP_PORT=<sftp-port> SFTP_USER=<sftp-user> SFTP_PATHS=<path-a>,<path-b> \
  python3 scripts/check_sftp.py
```

Download selected PEM files:

```bash
SFTP_HOST=<sftp-host> SFTP_PORT=<sftp-port> SFTP_USER=<sftp-user> SFTP_KEY_DIR=<remote-key-dir> \
  python3 scripts/download_cluster_keys.py --key account_a.pem --key account_b.pem
```

Mount SFTP shares through `sshfs`:

```bash
SFTP_HOST=<sftp-host> SFTP_PORT=<sftp-port> SFTP_USER=<sftp-user> \
SFTP_REMOTE_A=<remote-path-a> SFTP_REMOTE_B=<remote-path-b> \
  bash scripts/mount_sftp_drives.sh
```

Unmount:

```bash
bash scripts/unmount_sftp_drives.sh
```

## Live Checks

After local config and keys are prepared:

```bash
python3 scripts/check_ssh.py --account account_a
python3 scripts/refresh_inventory.py --account account_a
python3 scripts/live_sleep_test.py --account account_a --count 1 --partition cpu_partition
python3 scripts/live_distributed_sleep_test.py --count 6 --partition cpu_partition
```

Use live checks carefully because they submit real Slurm jobs.

## Token Usage

The dashboard can record token usage by provider and project. Use `provider=codex` now and `provider=claude` later. Weekly reset windows can be labeled with values such as `2026-W24`.

## Tests

```bash
python3 -m unittest discover -s tests
python3 -m compileall slurm_scheduler tests scripts
```
