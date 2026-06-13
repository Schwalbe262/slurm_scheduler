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


@dataclass(frozen=True)
class JobCreate:
    repo_url: str
    git_ref: str
    entrypoint: str
    arguments: str = ""
    env_setup: str = ""
    partition: str = "auto"
    time_limit: str = "01:00:00"
    cpus: int = 1
    memory: str = "4G"
    gpus: int = 0
    job_name: str = "web-job"


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
