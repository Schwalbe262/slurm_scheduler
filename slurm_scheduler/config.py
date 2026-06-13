from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class AccountConfig:
    name: str
    host: str
    port: int
    username: str
    private_key_path: str
    remote_workspace: str
    max_running_jobs: int = 1
    max_pending_jobs: int = 10
    max_total_jobs: int = 10
    storage_path: str = ""
    storage_quota_gb: float = 0.0
    partition_allowlist: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AppConfig:
    database_path: str = "data/slurm_scheduler.db"
    accounts_path: str = "config/accounts.yaml"
    poll_interval_seconds: int = 30
    admin_username: str = "admin"
    admin_password_hash: str = ""
    bind_host: str = "127.0.0.1"
    bind_port: int = 8000


def _read_yaml(path: str | Path) -> dict[str, Any]:
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"configuration file not found: {yaml_path}")
    with yaml_path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return loaded


def load_app_config(path: str | Path = "config/app.yaml") -> AppConfig:
    data = _read_yaml(path)
    return AppConfig(**{k: v for k, v in data.items() if k in AppConfig.__dataclass_fields__})


def load_accounts(path: str | Path) -> list[AccountConfig]:
    data = _read_yaml(path)
    raw_accounts = data.get("accounts", [])
    if not isinstance(raw_accounts, list):
        raise ValueError("accounts must be a list")
    accounts = [AccountConfig(**item) for item in raw_accounts]
    names = [account.name for account in accounts]
    if len(names) != len(set(names)):
        raise ValueError("account names must be unique")
    if not accounts:
        raise ValueError("at least one account must be configured")
    return accounts
