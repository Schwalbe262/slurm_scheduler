from __future__ import annotations

import argparse
import posixpath
import shlex
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from slurm_scheduler.config import AccountConfig, load_accounts
from slurm_scheduler.models import AccountSnapshot
from slurm_scheduler.slurm import SSHSession, parse_sbatch_job_id, parse_squeue_counts


def snapshot(account: AccountConfig) -> AccountSnapshot:
    with SSHSession(account) as ssh:
        result = ssh.run("squeue -h -u \"$USER\" -o \"%T\"")
    if result.exit_code != 0:
        raise RuntimeError(result.stderr.strip() or "squeue failed")
    running, pending = parse_squeue_counts(result.stdout)
    return AccountSnapshot(
        account_name=account.name,
        running=running,
        pending=pending,
        max_running=account.max_running_jobs,
        max_pending=account.max_pending_jobs,
        max_total=account.max_total_jobs,
    )


def choose_account(accounts: list[AccountConfig]) -> tuple[AccountConfig, AccountSnapshot]:
    snapshots = [(account, snapshot(account)) for account in accounts]
    available = [(account, item) for account, item in snapshots if item.available]
    if not available:
        states = ", ".join(f"{item.account_name}={item.running + item.pending}/{item.max_total}" for _, item in snapshots)
        raise RuntimeError(f"no account has capacity: {states}")
    return min(available, key=lambda pair: pair[1].score)


def sleep_script(job_name: str, cpus: int, seconds: int, time_limit: str, memory: str, partition: str) -> str:
    lines = [
            "#!/usr/bin/env bash",
            f"#SBATCH --job-name={job_name}",
            f"#SBATCH --cpus-per-task={cpus}",
            f"#SBATCH --time={time_limit}",
            f"#SBATCH --mem={memory}",
            "#SBATCH --output=sleep-%j.out",
            "#SBATCH --error=sleep-%j.err",
    ]
    if partition:
        lines.append(f"#SBATCH --partition={partition}")
    lines.extend(["", f"sleep {seconds}", ""])
    return "\n".join(lines)


def submit_sleep(account: AccountConfig, index: int, script: str) -> tuple[str, str]:
    remote_dir = posixpath.join(account.remote_workspace, f"distributed-sleep-test-{int(time.time())}-{index}")
    script_path = posixpath.join(remote_dir, "sleep.sbatch")
    with SSHSession(account) as ssh:
        result = ssh.run(
            f"mkdir -p {shlex.quote(remote_dir)} && "
            f"printf %s {shlex.quote(script)} > {shlex.quote(script_path)} && "
            f"cd {shlex.quote(remote_dir)} && sbatch {shlex.quote(posixpath.basename(script_path))}"
        )
    if result.exit_code != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "sbatch failed")
    return parse_sbatch_job_id(result.stdout), remote_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit 4-core sleep jobs across available Slurm accounts.")
    parser.add_argument("--accounts", default="config/accounts.yaml")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--cpus", type=int, default=4)
    parser.add_argument("--sleep-seconds", type=int, default=300)
    parser.add_argument("--time-limit", default="00:10:00")
    parser.add_argument("--memory", default="1G")
    parser.add_argument("--partition", default="")
    args = parser.parse_args()

    accounts = load_accounts(args.accounts)
    for account in accounts:
        if not Path(account.private_key_path).exists():
            raise SystemExit(f"private key not found for {account.name}: {account.private_key_path}")

    script = sleep_script(
        "scheduler-dist-sleep",
        args.cpus,
        args.sleep_seconds,
        args.time_limit,
        args.memory,
        args.partition,
    )
    for index in range(args.count):
        account, before = choose_account(accounts)
        slurm_job_id, remote_dir = submit_sleep(account, index, script)
        print(
            f"submitted index={index} account={account.name} "
            f"before={before.running + before.pending}/{before.max_total} "
            f"slurm_job_id={slurm_job_id} remote_dir={remote_dir}"
        )
        time.sleep(1)


if __name__ == "__main__":
    main()
