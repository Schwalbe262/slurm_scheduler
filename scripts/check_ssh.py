from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from slurm_scheduler.config import load_accounts
from slurm_scheduler.slurm import SSHSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Check SSH connectivity for a configured Slurm account.")
    parser.add_argument("--accounts", default="config/accounts.yaml")
    parser.add_argument("--account", required=True)
    args = parser.parse_args()

    account = next((item for item in load_accounts(args.accounts) if item.name == args.account), None)
    if not account:
        raise SystemExit(f"account not found: {args.account}")
    if not Path(account.private_key_path).exists():
        raise SystemExit(f"private key not found: {account.private_key_path}")

    with SSHSession(account) as ssh:
        whoami = ssh.run("whoami")
        hostname = ssh.run("hostname")
        squeue = ssh.run("command -v squeue")

    if whoami.exit_code != 0:
        raise SystemExit(whoami.stderr or whoami.stdout)
    if hostname.exit_code != 0:
        raise SystemExit(hostname.stderr or hostname.stdout)
    if squeue.exit_code != 0:
        raise SystemExit("SSH works, but squeue was not found in PATH")

    print(f"ssh_ok account={account.name} user={whoami.stdout.strip()} host={hostname.stdout.strip()}")
    print(f"squeue={squeue.stdout.strip()}")


if __name__ == "__main__":
    main()
