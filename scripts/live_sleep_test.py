from __future__ import annotations

import argparse
import posixpath
import shlex
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from slurm_scheduler.config import load_accounts
from slurm_scheduler.slurm import SSHSession, parse_sbatch_job_id


def build_sleep_script(
    job_name: str,
    cpus: int,
    sleep_seconds: int,
    time_limit: str,
    memory: str,
    partition: str = "",
) -> str:
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
    lines.extend(["", f"sleep {sleep_seconds}", ""])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit meaningless 4-core Slurm sleep jobs for integration testing.")
    parser.add_argument("--accounts", default="config/accounts.yaml")
    parser.add_argument("--account", required=True)
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--cpus", type=int, default=4)
    parser.add_argument("--sleep-seconds", type=int, default=300)
    parser.add_argument("--time-limit", default="00:10:00")
    parser.add_argument("--memory", default="1G")
    parser.add_argument("--partition", default="")
    args = parser.parse_args()

    account = next((item for item in load_accounts(args.accounts) if item.name == args.account), None)
    if not account:
        raise SystemExit(f"account not found: {args.account}")
    if not Path(account.private_key_path).exists():
        raise SystemExit(f"private key not found: {account.private_key_path}")

    remote_dir = posixpath.join(account.remote_workspace, f"live-sleep-test-{int(time.time())}")
    script = build_sleep_script(
        "scheduler-sleep-test",
        args.cpus,
        args.sleep_seconds,
        args.time_limit,
        args.memory,
        args.partition,
    )
    submitted: list[str] = []

    with SSHSession(account) as ssh:
        result = ssh.run(f"mkdir -p {shlex.quote(remote_dir)}")
        if result.exit_code != 0:
            raise SystemExit(result.stderr or result.stdout)
        for index in range(args.count):
            script_path = posixpath.join(remote_dir, f"sleep-{index}.sbatch")
            result = ssh.run(
                f"printf %s {shlex.quote(script)} > {shlex.quote(script_path)} && "
                f"cd {shlex.quote(remote_dir)} && sbatch {shlex.quote(posixpath.basename(script_path))}"
            )
            if result.exit_code != 0:
                raise SystemExit(result.stderr or result.stdout)
            submitted.append(parse_sbatch_job_id(result.stdout))
        queue = ssh.run("squeue -u \"$USER\" -o \"%.18i %.9T %.8C %.20j\"")

    print(f"remote_dir={remote_dir}")
    print("submitted=" + ",".join(submitted))
    print(queue.stdout)


if __name__ == "__main__":
    main()
