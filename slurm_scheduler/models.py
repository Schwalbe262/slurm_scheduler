from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AllocationStatus(StrEnum):
    PENDING = "pending"
    WARM = "warm"
    ACTIVE = "active"
    DRAINING = "draining"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


class TaskStatus(StrEnum):
    QUEUED = "queued"
    ATTACHING = "attaching"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class JobCreate:
    repo_url: str
    git_ref: str
    entrypoint: str
    arguments: str = ""
    env_setup: str = ""
    required_capability: str = ""
    env_profile: str = ""
    account_name: str = ""
    partition: str = "auto"
    time_limit: str = "01:00:00"
    cpus: int = 1
    memory: str = "4G"
    gpus: int = 0
    gpu_model: str = ""
    job_name: str = "web-job"
    job_mode: str = "python_git"
    remote_path: str = ""
    simulations_per_job: int = 1
    cpus_per_simulation: int = 1
    simulation_start: int = 1
    simulation_count: int = 1
    node_name: str = ""
    exclusive_node: bool = False
    mem_per_simulation_gb: float = 1.0
    max_workers_per_job: int = 32
    initial_workers: int = 1
    load_target: float = 0.75
    ramp_interval_seconds: int = 900


@dataclass(frozen=True)
class TaskCreate:
    name: str
    remote_cwd: str
    command: str
    env_setup: str = ""
    required_capability: str = ""
    env_profile: str = ""
    account_name: str = ""
    cpus: int = 1
    memory_mb: int = 4096
    gpus: int = 0
    gpu_model: str = ""
    partition: str = "auto"
    node_name: str = ""
    exclusive_node: bool = False
    priority: int = 0
    timeout_seconds: int = 0
    dedupe_key: str = ""
    max_workers_per_node: int = 0
    payload_json: str = ""


@dataclass(frozen=True)
class AccountSnapshot:
    account_name: str
    running: int
    pending: int
    max_running: int
    max_pending: int
    max_total: int
    storage_path: str = ""
    storage_used_gb: float | None = None
    storage_quota_gb: float | None = None

    @property
    def available(self) -> bool:
        return (
            self.running < self.max_running
            and self.pending < self.max_pending
            and (self.running + self.pending) < self.max_total
        )

    @property
    def score(self) -> tuple[int, int, int]:
        return (self.running + self.pending, self.running, self.pending)

    @property
    def storage_percent(self) -> float | None:
        if self.storage_used_gb is None or not self.storage_quota_gb:
            return None
        return min(100.0, (self.storage_used_gb / self.storage_quota_gb) * 100.0)
