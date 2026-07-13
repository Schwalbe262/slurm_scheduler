from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from slurm_scheduler.config import load_accounts, load_app_config
from slurm_scheduler.db import Database
from slurm_scheduler.pestat import parse_pestat
from slurm_scheduler.slurm import SSHSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh pestat node state into SQLite.")
    parser.add_argument("--config", default="config/app.yaml")
    parser.add_argument("--accounts", default="")
    parser.add_argument("--account", default="")
    args = parser.parse_args()

    app_config = load_app_config(args.config)
    accounts = load_accounts(args.accounts or app_config.accounts_path)
    account = next((item for item in accounts if item.name == args.account), accounts[0])

    with SSHSession(account) as ssh:
        result = ssh.run("pestat")
    if result.exit_code != 0:
        raise SystemExit(result.stderr or result.stdout)

    nodes = parse_pestat(result.stdout)
    db = Database(app_config.database_path, journal_mode=app_config.sqlite_journal_mode)
    db.init()
    db.replace_pestat_nodes(nodes)
    print(f"stored_pestat_nodes={len(nodes)}")


if __name__ == "__main__":
    main()
