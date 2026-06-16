from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class GitCredentialConfig:
    id: str
    url_patterns: list[str]
    private_key_path: str = ""
    clone_url: str = ""
    known_hosts_path: str = ""
    source_account: str = ""
    source_private_key_path: str = ""
    source_known_hosts_path: str = ""
    strict_host_key_checking: str = "accept-new"


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
    capabilities: list[str] = field(default_factory=list)
    env_profiles: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class AppConfig:
    database_path: str = "data/slurm_scheduler.db"
    accounts_path: str = "config/accounts.yaml"
    poll_interval_seconds: int = 30
    bind_host: str = "127.0.0.1"
    bind_port: int = 8000
    cluster_refresh_interval_seconds: int = 120
    min_warm_allocations: int = 1
    allocation_partition: str = "auto"
    allocation_cpus: int = 64
    allocation_memory: str = "0"
    allocation_time_limit: str = "48:00:00"
    allocation_scale_out_usage_threshold: float = 0.70
    allocation_scale_in_idle_seconds: int = 600
    allocation_drain_after_seconds: int = 129600
    allocation_attach_stop_before_drain_seconds: int = 1800
    allocation_force_cancel_after_seconds: int = 140400
    allocation_pending_timeout_seconds: int = 1800
    allocation_pending_backoff_seconds: int = 1800
    allocation_reserved_job_slots: int = 0
    cpu_pool_allow_gpu_partitions: bool = True
    warm_pool_preferred_accounts: list[str] = field(default_factory=list)
    gpu_warm_pool_preferred_accounts: list[str] = field(default_factory=list)
    single_job_per_node_partitions: list[str] = field(default_factory=lambda: ["cpu2"])
    gpu_cpu_reserve: int = 4
    gpu_prewarm_enabled: bool = True
    gpu_prewarm_preferred_models: list[str] = field(default_factory=lambda: ["a6000ada", "a6000"])
    gpu_prewarm_min_warm_allocations: int = 1
    gpu_prewarm_max_warm_allocations: int = 3
    gpu_prewarm_gpus_per_allocation: int = 4
    gpu_prewarm_min_gpus_per_allocation: int = 2
    gpu_prewarm_cpu_reserve_per_free_gpu: int = 8
    gpu_prewarm_partition: str = "auto"
    gpu_prewarm_time_limit: str = "48:00:00"
    cleanup_enabled: bool = True
    cleanup_interval_seconds: int = 3600
    cleanup_finished_task_ttl_seconds: int = 604800
    cleanup_finished_job_ttl_seconds: int = 604800
    cleanup_closed_allocation_ttl_seconds: int = 86400
    git_credentials: list[GitCredentialConfig] = field(default_factory=list)


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
    gpu_prewarm = data.pop("gpu_prewarm", None)
    if isinstance(gpu_prewarm, dict):
        mapping = {
            "enabled": "gpu_prewarm_enabled",
            "preferred_models": "gpu_prewarm_preferred_models",
            "min_warm_allocations": "gpu_prewarm_min_warm_allocations",
            "max_warm_allocations": "gpu_prewarm_max_warm_allocations",
            "gpus_per_allocation": "gpu_prewarm_gpus_per_allocation",
            "min_gpus_per_allocation": "gpu_prewarm_min_gpus_per_allocation",
            "cpu_reserve_per_free_gpu": "gpu_prewarm_cpu_reserve_per_free_gpu",
            "partition": "gpu_prewarm_partition",
            "time_limit": "gpu_prewarm_time_limit",
        }
        for source, target in mapping.items():
            if source in gpu_prewarm:
                data[target] = gpu_prewarm[source]
    cleanup = data.pop("cleanup", None)
    if isinstance(cleanup, dict):
        mapping = {
            "enabled": "cleanup_enabled",
            "interval_seconds": "cleanup_interval_seconds",
            "finished_task_ttl_seconds": "cleanup_finished_task_ttl_seconds",
            "finished_job_ttl_seconds": "cleanup_finished_job_ttl_seconds",
            "closed_allocation_ttl_seconds": "cleanup_closed_allocation_ttl_seconds",
        }
        for source, target in mapping.items():
            if source in cleanup:
                data[target] = cleanup[source]
    credentials = data.pop("git_credentials", [])
    if credentials is None:
        credentials = []
    if not isinstance(credentials, list):
        raise ValueError("git_credentials must be a list")
    data["git_credentials"] = [GitCredentialConfig(**item) for item in credentials]
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
