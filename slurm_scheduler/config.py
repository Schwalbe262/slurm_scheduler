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
    web_remote_file_default_max_bytes: int = 262144
    web_remote_file_hard_max_bytes: int = 1048576
    web_remote_command_timeout_seconds: int = 5
    web_remote_read_concurrency: int = 2
    web_remote_read_cache_seconds: int = 3
    web_timeout_keep_alive_seconds: int = 5
    web_timeout_graceful_shutdown_seconds: int = 15
    web_limit_concurrency: int = 64
    ssh_command_timeout_seconds: int = 30
    ssh_slow_command_timeout_seconds: int = 300
    scheduler_watchdog_enabled: bool = True
    scheduler_watchdog_stall_seconds: int = 0
    scheduler_ssh_parallelism: int = 4
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
    allocation_max_new_per_loop: int = 8
    cpu_pool_allow_gpu_partitions: bool = True
    cpu_pool_partition_spread: bool = True
    warm_pool_preferred_accounts: list[str] = field(default_factory=list)
    gpu_warm_pool_preferred_accounts: list[str] = field(default_factory=list)
    single_job_per_node_partitions: list[str] = field(default_factory=lambda: ["cpu2"])
    cpu_partition_allocation_limits: dict[str, int] = field(default_factory=lambda: {"cpu2": 2})
    gpu_cpu_reserve: int = 4
    gpu_prewarm_enabled: bool = True
    gpu_prewarm_preferred_models: list[str] = field(default_factory=lambda: ["a6000"])
    gpu_prewarm_min_warm_allocations: int = 2
    gpu_prewarm_max_warm_allocations: int = 4
    gpu_prewarm_gpus_per_allocation: int = 2
    gpu_prewarm_min_gpus_per_allocation: int = 2
    gpu_prewarm_cpus_per_allocation: int = 0
    gpu_prewarm_cpu_reserve_per_free_gpu: int = 8
    gpu_prewarm_stagger_seconds: int = 86400
    gpu_prewarm_memory: str = "128G"
    gpu_prewarm_partition: str = "auto"
    gpu_prewarm_time_limit: str = "48:00:00"
    gpu_prewarm_pinned_pending_timeout_seconds: int = 300
    fea_soft_memory_free_percent: float = 60.0
    fea_hard_memory_free_percent: float = 40.0
    fea_load_target: float = 0.75
    fea_max_attach_per_loop: int = 24
    fea_node_name_policy: str = "preferred"
    fea_overload_scale_out_load_factor: float = 2.0
    fea_overload_scale_out_seconds: int = 300
    fea_pressure_max_attempts: int = 3
    fea_max_attach_per_node_per_loop: int = 8
    fea_node_requested_cpu_factor: float = 1.0
    fea_footprint_maturity_seconds: int = 900
    cleanup_enabled: bool = True
    cleanup_interval_seconds: int = 3600
    cleanup_finished_task_ttl_seconds: int = 259200
    cleanup_finished_job_ttl_seconds: int = 259200
    cleanup_closed_allocation_ttl_seconds: int = 86400
    reconcile_on_start: bool = True
    backup_enabled: bool = True
    backup_interval_seconds: int = 86400
    backup_keep: int = 7
    backup_dir: str = "data/backups"
    cleanup_orphan_sweep_enabled: bool = True
    cleanup_orphan_sweep_interval_seconds: int = 86400
    cleanup_orphan_min_age_seconds: int = 604800
    cleanup_db_row_ttl_seconds: int = 1209600
    cleanup_event_ttl_seconds: int = 604800
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
            "cpus_per_allocation": "gpu_prewarm_cpus_per_allocation",
            "cpu_reserve_per_free_gpu": "gpu_prewarm_cpu_reserve_per_free_gpu",
            "stagger_seconds": "gpu_prewarm_stagger_seconds",
            "memory": "gpu_prewarm_memory",
            "partition": "gpu_prewarm_partition",
            "time_limit": "gpu_prewarm_time_limit",
            "pinned_pending_timeout_seconds": "gpu_prewarm_pinned_pending_timeout_seconds",
        }
        for source, target in mapping.items():
            if source in gpu_prewarm:
                data[target] = gpu_prewarm[source]
    fea_bursty = data.pop("fea_bursty", None)
    if isinstance(fea_bursty, dict):
        mapping = {
            "soft_memory_free_percent": "fea_soft_memory_free_percent",
            "hard_memory_free_percent": "fea_hard_memory_free_percent",
            "load_target": "fea_load_target",
            "max_attach_per_loop": "fea_max_attach_per_loop",
            "node_name_policy": "fea_node_name_policy",
            "overload_scale_out_load_factor": "fea_overload_scale_out_load_factor",
            "overload_scale_out_seconds": "fea_overload_scale_out_seconds",
            "pressure_max_attempts": "fea_pressure_max_attempts",
            "max_attach_per_node_per_loop": "fea_max_attach_per_node_per_loop",
            "node_requested_cpu_factor": "fea_node_requested_cpu_factor",
            "footprint_maturity_seconds": "fea_footprint_maturity_seconds",
        }
        for source, target in mapping.items():
            if source in fea_bursty:
                data[target] = fea_bursty[source]
    cleanup = data.pop("cleanup", None)
    if isinstance(cleanup, dict):
        mapping = {
            "enabled": "cleanup_enabled",
            "interval_seconds": "cleanup_interval_seconds",
            "finished_task_ttl_seconds": "cleanup_finished_task_ttl_seconds",
            "finished_job_ttl_seconds": "cleanup_finished_job_ttl_seconds",
            "closed_allocation_ttl_seconds": "cleanup_closed_allocation_ttl_seconds",
            "orphan_sweep_enabled": "cleanup_orphan_sweep_enabled",
            "orphan_sweep_interval_seconds": "cleanup_orphan_sweep_interval_seconds",
            "orphan_min_age_seconds": "cleanup_orphan_min_age_seconds",
            "db_row_ttl_seconds": "cleanup_db_row_ttl_seconds",
            "event_ttl_seconds": "cleanup_event_ttl_seconds",
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
