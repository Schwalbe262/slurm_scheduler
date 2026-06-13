from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from slurm_scheduler.config import load_accounts
from slurm_scheduler.slurm import SSHSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a command on a configured SSH account.")
    parser.add_argument("--accounts", default="config/accounts.yaml")
    parser.add_argument("--account", required=True)
    parser.add_argument("command")
    args = parser.parse_args()

    account = next((item for item in load_accounts(args.accounts) if item.name == args.account), None)
    if not account:
        raise SystemExit(f"account not found: {args.account}")

    with SSHSession(account) as ssh:
        result = ssh.run(args.command)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    raise SystemExit(result.exit_code)


if __name__ == "__main__":
    main()
