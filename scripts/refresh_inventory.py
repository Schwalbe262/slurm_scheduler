from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from slurm_scheduler.config import load_accounts, load_app_config
from slurm_scheduler.db import Database
from slurm_scheduler.inventory import parse_sinfo_nodes, partition_rank
from slurm_scheduler.slurm import SSHSession


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh Slurm node inventory into SQLite.")
    parser.add_argument("--config", default="config/app.yaml")
    parser.add_argument("--accounts", default="")
    parser.add_argument("--account", default="")
    args = parser.parse_args()

    app_config = load_app_config(args.config)
    accounts = load_accounts(args.accounts or app_config.accounts_path)
    account = next((item for item in accounts if item.name == args.account), accounts[0])

    with SSHSession(account) as ssh:
        result = ssh.run('sinfo -N -h -o "%N|%P|%c|%m|%G|%t"')
    if result.exit_code != 0:
        raise SystemExit(result.stderr or result.stdout)

    nodes = parse_sinfo_nodes(result.stdout)
    db = Database(app_config.database_path)
    db.init()
    db.replace_node_inventory(nodes)
    print(f"stored_nodes={len(nodes)}")
    print("cpu_rank=" + ",".join(item["partition"] for item in partition_rank(db.list_node_inventory(), False)))
    print("gpu_rank=" + ",".join(item["partition"] for item in partition_rank(db.list_node_inventory(), True)))


if __name__ == "__main__":
    main()
