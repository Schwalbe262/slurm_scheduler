from __future__ import annotations

import logging
import posixpath
import re
import threading
import time
from datetime import datetime, timezone
from typing import Callable

from .config import AccountConfig
from .db import Database
from .inventory import CPU_PROFILES_BY_PARTITION, GPU_PRIORITY, gpu_model_candidates, normalize_gpu_model, parse_scontrol_nodes, parse_sinfo_nodes, partition_rank
from .models import AccountSnapshot, AllocationStatus, JobStatus, TaskStatus
from .pestat import PestatNode, parse_pestat
from .slurm import RemoteExecutionError, SSHSession, SlurmAccountClient

LOGGER = logging.getLogger(__name__)
ClientFactory = Callable[[AccountConfig], SlurmAccountClient]


class Scheduler:
    def __init__(
        self,
        db: Database,
        accounts: list[AccountConfig],
        poll_interval_seconds: int,
        client_factory: ClientFactory = SlurmAccountClient,
        cluster_refresh_interval_seconds: int = 120,
        min_warm_allocations: int = 1,
        allocation_partition: str = "auto",
        allocation_cpus: int = 64,
        allocation_memory: str = "0",
        allocation_time_limit: str = "48:00:00",
        allocation_scale_out_usage_threshold: float = 0.70,
        allocation_scale_in_idle_seconds: int = 600,
        allocation_drain_after_seconds: int = 129600,
        allocation_attach_stop_before_drain_seconds: int = 1800,
        allocation_force_cancel_after_seconds: int = 140400,
        allocation_pending_timeout_seconds: int = 1800,
        allocation_pending_backoff_seconds: int = 1800,
        allocation_reserved_job_slots: int = 0,
        cpu_pool_allow_gpu_partitions: bool = True,
        warm_pool_preferred_accounts: list[str] | None = None,
        gpu_warm_pool_preferred_accounts: list[str] | None = None,
        single_job_per_node_partitions: list[str] | None = None,
        gpu_cpu_reserve: int = 4,
        gpu_prewarm_enabled: bool = False,
        gpu_prewarm_preferred_models: list[str] | None = None,
        gpu_prewarm_min_warm_allocations: int = 1,
        gpu_prewarm_max_warm_allocations: int = 3,
        gpu_prewarm_gpus_per_allocation: int = 2,
        gpu_prewarm_cpu_reserve_per_free_gpu: int = 8,
        gpu_prewarm_partition: str = "auto",
        gpu_prewarm_time_limit: str = "48:00:00",
        cleanup_enabled: bool = True,
        cleanup_interval_seconds: int = 3600,
        cleanup_finished_task_ttl_seconds: int = 604800,
        cleanup_finished_job_ttl_seconds: int = 604800,
        cleanup_closed_allocation_ttl_seconds: int = 86400,
    ):
        self.db = db
        self.accounts = accounts
        self.poll_interval_seconds = poll_interval_seconds
        self.client_factory = client_factory
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_cache: tuple[float, list[AccountSnapshot]] | None = None
        self._storage_cache: dict[str, tuple[float, float | None]] = {}
        self._storage_refresh_interval_seconds = max(900, poll_interval_seconds * 20)
        self.cluster_refresh_interval_seconds = cluster_refresh_interval_seconds
        self._last_cluster_refresh_at = 0.0
        self.min_warm_allocations = min_warm_allocations
        self.allocation_partition = allocation_partition
        self.allocation_cpus = allocation_cpus
        self.allocation_memory = allocation_memory
        self.allocation_time_limit = allocation_time_limit
        self.allocation_scale_out_usage_threshold = allocation_scale_out_usage_threshold
        self.allocation_scale_in_idle_seconds = allocation_scale_in_idle_seconds
        self.allocation_drain_after_seconds = allocation_drain_after_seconds
        self.allocation_attach_stop_before_drain_seconds = allocation_attach_stop_before_drain_seconds
        self.allocation_force_cancel_after_seconds = allocation_force_cancel_after_seconds
        self.allocation_pending_timeout_seconds = allocation_pending_timeout_seconds
        self.allocation_pending_backoff_seconds = allocation_pending_backoff_seconds
        self.allocation_reserved_job_slots = allocation_reserved_job_slots
        self.cpu_pool_allow_gpu_partitions = cpu_pool_allow_gpu_partitions
        self.warm_pool_preferred_accounts = warm_pool_preferred_accounts or []
        self.gpu_warm_pool_preferred_accounts = gpu_warm_pool_preferred_accounts or []
        self.single_job_per_node_partitions = {
            partition.strip() for partition in (single_job_per_node_partitions if single_job_per_node_partitions is not None else ["cpu2"]) if partition.strip()
        }
        self.gpu_cpu_reserve = gpu_cpu_reserve
        self.gpu_prewarm_enabled = gpu_prewarm_enabled
        self.gpu_prewarm_preferred_models = [
            normalize_gpu_model(model) for model in (gpu_prewarm_preferred_models or ["a6000ada", "a6000"])
        ]
        self.gpu_prewarm_min_warm_allocations = gpu_prewarm_min_warm_allocations
        self.gpu_prewarm_max_warm_allocations = gpu_prewarm_max_warm_allocations
        self.gpu_prewarm_gpus_per_allocation = gpu_prewarm_gpus_per_allocation
        self.gpu_prewarm_cpu_reserve_per_free_gpu = gpu_prewarm_cpu_reserve_per_free_gpu
        self.gpu_prewarm_partition = gpu_prewarm_partition
        self.gpu_prewarm_time_limit = gpu_prewarm_time_limit
        self._allocation_backoff_until_by_pool: dict[str, float] = {}
        self.cleanup_enabled = cleanup_enabled
        self.cleanup_interval_seconds = cleanup_interval_seconds
        self.cleanup_finished_task_ttl_seconds = cleanup_finished_task_ttl_seconds
        self.cleanup_finished_job_ttl_seconds = cleanup_finished_job_ttl_seconds
        self.cleanup_closed_allocation_ttl_seconds = cleanup_closed_allocation_ttl_seconds
        self._last_cleanup_at = 0.0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self.run_forever, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                LOGGER.exception("scheduler tick failed")
            self._stop.wait(self.poll_interval_seconds)

    def tick(self) -> None:
        self.refresh_cluster_state_if_due()
        self.refresh_allocations()
        self.refresh_tasks()
        self.apply_allocation_lifecycle()
        self.assign_queued_tasks()
        self.maintain_allocation_pool()
        self.refresh_submitted_jobs()
        self.submit_next_queued_job()
        self.cleanup_remote_artifacts_if_due()

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _timestamp(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                return None

    def _age_seconds(self, row: dict) -> float:
        started = self._timestamp(row.get("started_at") or row.get("submitted_at") or row.get("created_at"))
        if not started:
            return 0
        return max(0.0, (self._now() - started).total_seconds())

    def _finished_age_seconds(self, row: dict, field: str) -> float | None:
        finished = self._timestamp(row.get(field))
        if not finished:
            return None
        return max(0.0, (self._now() - finished).total_seconds())

    def cleanup_remote_artifacts_if_due(self) -> None:
        if not self.cleanup_enabled:
            return
        now = time.time()
        if self.cleanup_interval_seconds > 0 and now - self._last_cleanup_at < self.cleanup_interval_seconds:
            return
        self._last_cleanup_at = now
        self.cleanup_finished_tasks()
        self.cleanup_finished_jobs()
        self.cleanup_closed_allocations()

    def cleanup_finished_tasks(self) -> None:
        terminal = {TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in terminal or not task.get("remote_dir"):
                continue
            age = self._finished_age_seconds(task, "finished_at")
            if age is None or age < self.cleanup_finished_task_ttl_seconds:
                continue
            account = self.account_by_name(str(task.get("account_name") or ""))
            if not self.remove_scheduler_artifact(account, str(task.get("remote_dir") or ""), ("task-",)):
                continue
            self.db.update_task(
                task["id"],
                remote_dir="",
                stdout_path="",
                stderr_path="",
                exit_code_path="",
                wrapper_pid="",
            )

    def cleanup_finished_jobs(self) -> None:
        terminal = {JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value}
        for job in self.db.list_jobs(limit=5000):
            if job["status"] not in terminal or not job.get("remote_job_dir"):
                continue
            age = self._finished_age_seconds(job, "finished_at")
            if age is None or age < self.cleanup_finished_job_ttl_seconds:
                continue
            account = self.account_by_name(str(job.get("account_name") or ""))
            if not self.remove_scheduler_artifact(account, str(job.get("remote_job_dir") or ""), ("job-",)):
                continue
            self.db.update_job(job["id"], remote_job_dir="", stdout_path="", stderr_path="")

    def cleanup_closed_allocations(self) -> None:
        terminal = {AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value}
        for allocation in self.db.list_allocations(limit=5000):
            if allocation["state"] not in terminal or not allocation.get("remote_dir"):
                continue
            age = self._finished_age_seconds(allocation, "closed_at")
            if age is None or age < self.cleanup_closed_allocation_ttl_seconds:
                continue
            account = self.account_by_name(str(allocation.get("account_name") or ""))
            if not self.remove_scheduler_artifact(account, str(allocation.get("remote_dir") or ""), ("allocation-",)):
                continue
            self.db.update_allocation(allocation["id"], remote_dir="", stdout_path="", stderr_path="")

    def remove_scheduler_artifact(self, account: AccountConfig | None, remote_path: str, prefixes: tuple[str, ...]) -> bool:
        if not account or not self.is_safe_scheduler_artifact_path(account, remote_path, prefixes):
            return False
        try:
            self.client_factory(account).remove_tree(remote_path)
        except Exception as exc:
            LOGGER.warning("failed to clean remote artifact %s on %s: %s", remote_path, account.name, exc)
            return False
        return True

    def is_safe_scheduler_artifact_path(self, account: AccountConfig, remote_path: str, prefixes: tuple[str, ...]) -> bool:
        artifact = self._normalize_remote_path(remote_path)
        workspace = self._normalize_remote_path(account.remote_workspace)
        if not artifact or not workspace or workspace in {".", "/"}:
            return False
        artifact_parts = [part for part in artifact.split("/") if part]
        workspace_parts = [part for part in workspace.split("/") if part]
        if ".." in artifact_parts or ".." in workspace_parts:
            return False
        basename = posixpath.basename(artifact.rstrip("/"))
        if not any(basename.startswith(prefix) for prefix in prefixes):
            return False
        workspace_prefix = workspace.rstrip("/") + "/"
        return artifact.startswith(workspace_prefix)

    def _normalize_remote_path(self, value: str) -> str:
        path = (value or "").strip()
        for prefix in ("$HOME/", "~/"):
            if path.startswith(prefix):
                path = path[len(prefix):]
        return posixpath.normpath(path)

    def refresh_cluster_state_if_due(self) -> None:
        if self.cluster_refresh_interval_seconds <= 0:
            return
        now = time.time()
        if now - self._last_cluster_refresh_at < self.cluster_refresh_interval_seconds:
            return
        self._last_cluster_refresh_at = now
        preferred = self.warm_pool_preferred_accounts or self.gpu_warm_pool_preferred_accounts
        preferred_names = set(preferred)
        account = next((item for item in self.accounts if item.name in preferred_names), None) or (self.accounts[0] if self.accounts else None)
        if not account:
            return
        try:
            with SSHSession(account) as ssh:
                inventory_result = ssh.run("scontrol -o show nodes")
                if inventory_result.exit_code == 0 and inventory_result.stdout.strip():
                    self.db.replace_node_inventory(parse_scontrol_nodes(inventory_result.stdout))
                else:
                    sinfo = ssh.run('sinfo -N -h -o "%N|%P|%c|%m|%G|%t"')
                    if sinfo.exit_code == 0 and sinfo.stdout.strip():
                        self.db.replace_node_inventory(parse_sinfo_nodes(sinfo.stdout))
                pestat_result = ssh.run("pestat")
                if pestat_result.exit_code == 0 and pestat_result.stdout.strip():
                    self.db.replace_pestat_nodes(parse_pestat(pestat_result.stdout))
        except Exception as exc:
            LOGGER.warning("failed to refresh cluster state through %s: %s", account.name, exc)

    def account_by_name(self, name: str) -> AccountConfig | None:
        return next((item for item in self.accounts if item.name == name), None)

    def account_supports(
        self,
        account: AccountConfig | None,
        required_capability: str = "",
        env_profile: str = "",
    ) -> bool:
        if not account:
            return False
        capability = (required_capability or "").strip()
        profile = (env_profile or "").strip()
        if capability and capability not in (account.capabilities or []):
            return False
        if profile and profile not in (account.env_profiles or {}):
            return False
        return True

    def is_single_job_partition(self, partition: str) -> bool:
        return (partition or "").strip() in self.single_job_per_node_partitions

    def occupied_single_job_nodes(
        self,
        partition: str,
        exclude_job_id: int | None = None,
        include_queued_jobs: bool = False,
    ) -> set[str]:
        if not self.is_single_job_partition(partition):
            return set()
        occupied = set()
        allocation_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        for allocation in self.db.list_allocations(limit=1000):
            if allocation["state"] in allocation_states and allocation.get("partition") == partition and allocation.get("node_name"):
                occupied.add(str(allocation["node_name"]))
        job_states = {JobStatus.SUBMITTING.value, JobStatus.SUBMITTED.value, JobStatus.RUNNING.value}
        if include_queued_jobs:
            job_states.add(JobStatus.QUEUED.value)
        for job in self.db.list_jobs(limit=5000):
            if exclude_job_id is not None and int(job["id"]) == int(exclude_job_id):
                continue
            if job["status"] in job_states and job.get("partition") == partition and job.get("node_name"):
                occupied.add(str(job["node_name"]))
        return occupied

    def partition_has_live_allocation(self, partition: str, resource_pool: str = "") -> bool:
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        for allocation in self.db.list_allocations(limit=1000):
            if allocation["state"] not in live_states:
                continue
            if allocation.get("partition") != partition:
                continue
            if resource_pool and (allocation.get("resource_pool") or "cpu") != resource_pool:
                continue
            return True
        return False

    def refresh_allocations(self) -> None:
        accounts_by_name = {account.name: account for account in self.accounts}
        active_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in active_states or not allocation.get("slurm_job_id"):
                continue
            account = accounts_by_name.get(allocation["account_name"])
            if not account:
                continue
            try:
                client = self.client_factory(account)
                status = client.state(allocation["slurm_job_id"])
            except Exception as exc:
                LOGGER.warning("failed to refresh allocation %s: %s", allocation["id"], exc)
                continue
            if status == JobStatus.RUNNING and allocation["state"] == AllocationStatus.PENDING.value:
                self.db.update_allocation(
                    allocation["id"],
                    state=AllocationStatus.WARM.value,
                    started_at="CURRENT_TIMESTAMP",
                    pending_reason="",
                )
            elif status == JobStatus.SUBMITTED and allocation["state"] == AllocationStatus.PENDING.value:
                try:
                    reason = client.pending_reason(allocation["slurm_job_id"])
                except Exception as exc:
                    LOGGER.debug("failed to read pending reason for allocation %s: %s", allocation["id"], exc)
                    reason = ""
                if reason and reason != (allocation.get("pending_reason") or ""):
                    self.db.update_allocation(allocation["id"], pending_reason=reason)
            elif status in {JobStatus.COMPLETED, JobStatus.CANCELLED}:
                self.db.update_allocation(allocation["id"], state=AllocationStatus.CLOSED.value, closed_at="CURRENT_TIMESTAMP")
            elif status == JobStatus.FAILED:
                self.db.update_allocation(allocation["id"], state=AllocationStatus.FAILED.value, closed_at="CURRENT_TIMESTAMP")
        self.recalculate_allocation_capacity()

    def refresh_tasks(self) -> None:
        accounts_by_name = {account.name: account for account in self.accounts}
        for task in self.db.list_tasks(limit=1000):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not task.get("account_name"):
                continue
            account = accounts_by_name.get(task["account_name"])
            if not account:
                continue
            try:
                status = self.client_factory(account).task_state(task)
            except Exception as exc:
                LOGGER.warning("failed to refresh task %s: %s", task["id"], exc)
                continue
            if status == JobStatus.RUNNING:
                if task["status"] != TaskStatus.RUNNING.value:
                    self.db.update_task(task["id"], status=TaskStatus.RUNNING.value, started_at="CURRENT_TIMESTAMP")
                continue
            if status == JobStatus.COMPLETED:
                self.db.update_task(task["id"], status=TaskStatus.COMPLETED.value, finished_at="CURRENT_TIMESTAMP")
                self.close_allocation_after_exclusive_task(task)
            elif status == JobStatus.CANCELLED:
                self.db.update_task(task["id"], status=TaskStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
                self.close_allocation_after_exclusive_task(task)
            elif status == JobStatus.FAILED:
                self.db.update_task(task["id"], status=TaskStatus.FAILED.value, finished_at="CURRENT_TIMESTAMP")
                self.close_allocation_after_exclusive_task(task)
        self.recalculate_allocation_capacity()

    def close_allocation_after_exclusive_task(self, task: dict) -> None:
        if not int(task.get("exclusive_node") or 0):
            return
        allocation_id = int(task.get("allocation_id") or 0)
        if not allocation_id:
            return
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            return
        self.close_allocation(allocation, f"exclusive task {task['id']} finished")

    def recalculate_allocation_capacity(self) -> None:
        tasks = self.db.list_tasks(limit=5000)
        running_by_allocation: dict[int, tuple[int, int, int]] = {}
        for task in tasks:
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not task.get("allocation_id"):
                continue
            cpus, mem, gpus = running_by_allocation.get(int(task["allocation_id"]), (0, 0, 0))
            running_by_allocation[int(task["allocation_id"])] = (
                cpus + int(task.get("cpus") or 0),
                mem + int(task.get("memory_mb") or 0),
                gpus + int(task.get("gpus") or 0),
            )
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            used_cpus, used_mem, used_gpus = running_by_allocation.get(allocation["id"], (0, 0, 0))
            free_cpus = max(0, int(allocation["total_cpus"]) - used_cpus)
            free_mem = max(0, int(allocation["total_memory_mb"]) - used_mem)
            free_gpus = max(0, int(allocation.get("total_gpus") or 0) - used_gpus)
            state = allocation["state"]
            if state != AllocationStatus.DRAINING.value:
                state = AllocationStatus.ACTIVE.value if used_cpus or used_mem or used_gpus else AllocationStatus.WARM.value
            self.db.update_allocation(
                allocation["id"],
                state=state,
                free_cpus=free_cpus,
                free_memory_mb=free_mem,
                free_gpus=free_gpus,
                last_active_at="CURRENT_TIMESTAMP" if used_cpus or used_mem or used_gpus else allocation.get("last_active_at"),
            )

    def apply_allocation_lifecycle(self) -> None:
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] == AllocationStatus.PENDING.value:
                self.expire_pending_allocation_if_stale(allocation)
                continue
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            age = self._age_seconds(allocation)
            if age >= self.allocation_drain_after_seconds and allocation["state"] != AllocationStatus.DRAINING.value:
                self.db.update_allocation(
                    allocation["id"],
                    state=AllocationStatus.DRAINING.value,
                    drain_at="CURRENT_TIMESTAMP",
                    drain_reason="age limit",
                )
                allocation = self.db.get_allocation(allocation["id"]) or allocation
                if self._running_task_count(allocation["id"]) == 0:
                    self.close_allocation(allocation, "drained")
                continue
            if allocation["state"] == AllocationStatus.DRAINING.value and self._running_task_count(allocation["id"]) == 0:
                self.close_allocation(allocation, "drained")
            elif age >= self.allocation_force_cancel_after_seconds:
                self.fail_running_tasks(allocation["id"], "allocation force-cancelled near walltime")
                self.close_allocation(allocation, "force timeout")

    def expire_pending_allocation_if_stale(self, allocation: dict) -> None:
        if self.allocation_pending_timeout_seconds <= 0:
            return
        submitted_at = self._timestamp(allocation.get("submitted_at") or allocation.get("created_at"))
        if not submitted_at:
            return
        age = (self._now() - submitted_at).total_seconds()
        if age < self.allocation_pending_timeout_seconds:
            return
        pool = allocation.get("resource_pool") or "cpu"
        reason = allocation.get("pending_reason") or "unknown Slurm pending reason"
        self._allocation_backoff_until_by_pool[pool] = time.monotonic() + max(0, self.allocation_pending_backoff_seconds)
        self.close_allocation(allocation, f"pending timeout after {int(age)}s: {reason}")

    def _running_task_count(self, allocation_id: int) -> int:
        return sum(
            1
            for task in self.db.list_tasks(limit=5000)
            if task.get("allocation_id") == allocation_id
            and task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        )

    def fail_running_tasks(self, allocation_id: int, message: str) -> None:
        for task in self.db.list_tasks(limit=5000):
            if task.get("allocation_id") != allocation_id:
                continue
            if task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                self.db.update_task(
                    task["id"],
                    status=TaskStatus.FAILED.value,
                    failure_message=message,
                    finished_at="CURRENT_TIMESTAMP",
                )

    def close_allocation(self, allocation: dict, reason: str) -> None:
        if allocation["state"] in {AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value, AllocationStatus.CLOSING.value}:
            return
        account = next((item for item in self.accounts if item.name == allocation["account_name"]), None)
        self.db.update_allocation(allocation["id"], state=AllocationStatus.CLOSING.value, drain_reason=reason)
        if account and allocation.get("slurm_job_id"):
            try:
                self.client_factory(account).cancel(allocation["slurm_job_id"])
            except Exception as exc:
                self.db.update_allocation(
                    allocation["id"],
                    state=AllocationStatus.FAILED.value,
                    failure_message=str(exc),
                    closed_at="CURRENT_TIMESTAMP",
                )
                return
        self.db.update_allocation(allocation["id"], state=AllocationStatus.CLOSED.value, closed_at="CURRENT_TIMESTAMP")

    def allocation_pool_in_backoff(self, resource_pool: str) -> bool:
        until = self._allocation_backoff_until_by_pool.get(resource_pool)
        if not until:
            return False
        if until <= time.monotonic():
            self._allocation_backoff_until_by_pool.pop(resource_pool, None)
            return False
        return True

    def assign_queued_tasks(self) -> None:
        queued_tasks = sorted(
            [task for task in self.db.list_tasks(limit=5000) if task["status"] == TaskStatus.QUEUED.value],
            key=lambda item: int(item["id"]),
        )
        for task in queued_tasks:
            allocation = self.best_allocation_for_task(task)
            if not allocation:
                continue
            account = next((item for item in self.accounts if item.name == allocation["account_name"]), None)
            if not account:
                continue
            self.db.update_task(
                task["id"],
                status=TaskStatus.ATTACHING.value,
                allocation_id=allocation["id"],
                account_name=allocation["account_name"],
                attached_at="CURRENT_TIMESTAMP",
            )
            try:
                result = self.client_factory(account).attach_task(task, allocation)
            except RemoteExecutionError as exc:
                self.db.update_task(
                    task["id"],
                    status=TaskStatus.FAILED.value,
                    failure_message=str(exc),
                    finished_at="CURRENT_TIMESTAMP",
                    **exc.result_fields,
                )
                self.recalculate_allocation_capacity()
                continue
            except Exception as exc:
                self.db.update_task(
                    task["id"],
                    status=TaskStatus.FAILED.value,
                    failure_message=str(exc),
                    finished_at="CURRENT_TIMESTAMP",
                )
                self.recalculate_allocation_capacity()
                continue
            self.db.update_task(task["id"], status=TaskStatus.RUNNING.value, started_at="CURRENT_TIMESTAMP", **result)
            self.recalculate_allocation_capacity()

    def best_allocation_for_task(self, task: dict) -> dict | None:
        cpu_candidates = []
        gpu_candidates = []
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}:
                continue
            if not self.allocation_accepts_new_tasks(allocation):
                continue
            if not allocation.get("slurm_job_id"):
                continue
            if not self.allocation_can_run_task(allocation, task, include_pending=False):
                continue
            if self.task_requires_gpu(task):
                gpu_candidates.append(allocation)
            elif int(allocation.get("total_gpus") or 0) > 0:
                gpu_candidates.append(allocation)
            else:
                cpu_candidates.append(allocation)
        candidates = gpu_candidates if self.task_requires_gpu(task) else (cpu_candidates or gpu_candidates)
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda item: (
                self.allocation_model_score(item),
                int(item.get("free_gpus") or 0),
                self.borrowable_cpus(item) if not self.task_requires_gpu(task) else int(item["free_cpus"]),
                int(item["free_memory_mb"]),
            ),
        )

    def allocation_accepts_new_tasks(self, allocation: dict) -> bool:
        if self.allocation_drain_after_seconds <= 0:
            return True
        stop_before = max(0, self.allocation_attach_stop_before_drain_seconds)
        cutoff = max(0, self.allocation_drain_after_seconds - stop_before)
        return self._age_seconds(allocation) < cutoff

    def has_inflight_capacity_for_task(self, task: dict) -> bool:
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }:
                continue
            if self.allocation_can_run_task(allocation, task, include_pending=True):
                return True
        return False

    def task_requires_gpu(self, task: dict) -> bool:
        return int(task.get("gpus") or 0) > 0

    def requested_accounts(self, account_name: str) -> list[str]:
        return [part.strip() for part in re.split(r"[\s,;/|]+", account_name or "") if part.strip()]

    def allocation_model_score(self, allocation: dict) -> int:
        return GPU_PRIORITY.get(normalize_gpu_model(str(allocation.get("gpu_model") or "")), 0)

    def borrowable_cpus(self, allocation: dict) -> int:
        free_cpus = int(allocation.get("free_cpus") or 0)
        total_gpus = int(allocation.get("total_gpus") or 0)
        free_gpus = int(allocation.get("free_gpus") or 0)
        if total_gpus > 0 and free_gpus == total_gpus:
            return free_cpus
        reserve = free_gpus * self.gpu_prewarm_cpu_reserve_per_free_gpu
        return max(0, free_cpus - reserve)

    def allocation_has_active_exclusive_task(self, allocation_id: int) -> bool:
        for task in self.db.list_tasks(limit=5000):
            if int(task.get("allocation_id") or 0) != allocation_id:
                continue
            if task.get("status") not in {
                TaskStatus.ATTACHING.value,
                TaskStatus.RUNNING.value,
            }:
                continue
            if int(task.get("exclusive_node") or 0):
                return True
        return False

    def allocation_has_active_task(self, allocation_id: int) -> bool:
        for task in self.db.list_tasks(limit=5000):
            if int(task.get("allocation_id") or 0) != allocation_id:
                continue
            if task.get("status") in {
                TaskStatus.ATTACHING.value,
                TaskStatus.RUNNING.value,
            }:
                return True
        return False

    def allocation_can_run_task(self, allocation: dict, task: dict, include_pending: bool) -> bool:
        requested_accounts = self.requested_accounts(str(task.get("account_name") or ""))
        if requested_accounts and allocation.get("account_name") not in requested_accounts:
            return False
        account = self.account_by_name(str(allocation.get("account_name") or ""))
        if not self.account_supports(
            account,
            str(task.get("required_capability") or ""),
            str(task.get("env_profile") or ""),
        ):
            return False
        if int(allocation["free_memory_mb"]) < int(task["memory_mb"]):
            return False
        if int(task.get("exclusive_node") or 0):
            if not include_pending and self.allocation_has_active_task(int(allocation["id"])):
                return False
            if int(allocation.get("free_cpus") or 0) != int(allocation.get("total_cpus") or 0):
                return False
            if int(allocation.get("free_memory_mb") or 0) != int(allocation.get("total_memory_mb") or 0):
                return False
            if int(allocation.get("free_gpus") or 0) != int(allocation.get("total_gpus") or 0):
                return False
        elif not include_pending and self.allocation_has_active_exclusive_task(int(allocation["id"])):
            return False
        if (task.get("partition") or "auto") not in {"", "auto"} and allocation.get("partition") != task.get("partition"):
            return False
        if task.get("node_name") and allocation.get("node_name") != task.get("node_name"):
            return False
        if self.task_requires_gpu(task):
            task_models = gpu_model_candidates(str(task.get("gpu_model") or ""))
            allocation_model = normalize_gpu_model(str(allocation.get("gpu_model") or ""))
            if task_models and allocation_model not in task_models:
                return False
            if int(allocation.get("free_gpus") or 0) < int(task.get("gpus") or 0):
                return False
            if int(allocation["free_cpus"]) >= int(task["cpus"]):
                return True
            return not include_pending and int(task.get("cpus") or 0) <= 4
        if int(allocation.get("total_gpus") or 0) > 0:
            return self.borrowable_cpus(allocation) >= int(task["cpus"])
        return int(allocation["free_cpus"]) >= int(task["cpus"])

    def maintain_allocation_pool(self) -> None:
        self.prewarm_gpu_for_minimum()
        self.prewarm_cpu_for_minimum()
        self.prewarm_for_demand()
        self.scale_in_idle_allocations()

    def prewarm_cpu_for_minimum(self) -> None:
        live_count = sum(
            1
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
            and (allocation.get("resource_pool") or "cpu") == "cpu"
        )
        while live_count < self.min_warm_allocations:
            if self.allocation_pool_in_backoff("cpu"):
                return
            if not self.open_allocation(
                "minimum CPU warm pool",
                resource_pool="cpu",
                preferred_accounts=self.warm_pool_preferred_accounts,
            ):
                return
            live_count += 1

    def prewarm_gpu_for_minimum(self) -> None:
        if not self.gpu_prewarm_enabled or self.gpu_prewarm_min_warm_allocations <= 0:
            return
        opened_preferred = self.ensure_preferred_gpu_queue()
        if opened_preferred:
            return
        live_allocations = self.live_gpu_allocations()
        ready_count = sum(
            1 for allocation in live_allocations if allocation["state"] in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        )
        if ready_count >= self.gpu_prewarm_min_warm_allocations:
            return
        if len(live_allocations) >= self.gpu_prewarm_max_warm_allocations:
            return
        live_models = {normalize_gpu_model(str(allocation.get("gpu_model") or "")) for allocation in live_allocations}
        model = self.choose_gpu_model_for_fallback(live_models)
        if not model:
            return
        resource_pool = f"gpu:{model}"
        if self.allocation_pool_in_backoff(resource_pool):
            return
        self.open_allocation(
            f"fallback GPU warm pool {model}",
            resource_pool=resource_pool,
            gpu_model=model,
            gpus=self.gpu_prewarm_gpus_per_allocation,
            preferred_accounts=self.gpu_warm_pool_preferred_accounts or self.warm_pool_preferred_accounts,
            account_name=self.preferred_gpu_warm_account_constraint(),
        )

    def ensure_preferred_gpu_queue(self) -> bool:
        live_allocations = self.live_gpu_allocations()
        if len(live_allocations) >= self.gpu_prewarm_max_warm_allocations:
            return False
        live_models = {normalize_gpu_model(str(allocation.get("gpu_model") or "")) for allocation in live_allocations}
        for model in self.gpu_prewarm_preferred_models:
            if not model or model in live_models:
                continue
            resource_pool = f"gpu:{model}"
            if self.allocation_pool_in_backoff(resource_pool):
                continue
            if not self.open_allocation(
                f"minimum GPU warm pool {model}",
                resource_pool=resource_pool,
                gpu_model=model,
                gpus=self.gpu_prewarm_gpus_per_allocation,
                preferred_accounts=self.gpu_warm_pool_preferred_accounts or self.warm_pool_preferred_accounts,
                account_name=self.preferred_gpu_warm_account_constraint(),
            ):
                continue
            return True
        return False

    def preferred_gpu_warm_account_constraint(self) -> str:
        accounts = self.gpu_warm_pool_preferred_accounts or []
        return ",".join(accounts)

    def prewarm_for_demand(self) -> None:
        if self.prewarm_exclusive_demand():
            return
        task = self.next_queued_task_without_inflight_capacity()
        if task:
            self.open_allocation_for_task(task)
            return
        if any(task["status"] == TaskStatus.QUEUED.value for task in self.db.list_tasks(limit=5000)):
            return
        allocations = [
            item
            for item in self.db.list_allocations(limit=500)
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        if not allocations:
            return
        cpu_used = sum(int(item["total_cpus"]) - int(item["free_cpus"]) for item in allocations)
        cpu_total = sum(int(item["total_cpus"]) for item in allocations) or 1
        mem_used = sum(int(item["total_memory_mb"]) - int(item["free_memory_mb"]) for item in allocations)
        mem_total = sum(int(item["total_memory_mb"]) for item in allocations) or 1
        usage = max(cpu_used / cpu_total, mem_used / mem_total)
        spares = [
            item
            for item in allocations
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value}
            and (item.get("resource_pool") or "cpu") == "cpu"
        ]
        if usage >= self.allocation_scale_out_usage_threshold and not spares:
            if self.allocation_pool_in_backoff("cpu"):
                return
            self.open_allocation(
                "high CPU utilization",
                resource_pool="cpu",
                preferred_accounts=self.warm_pool_preferred_accounts,
            )

    def next_queued_task_without_inflight_capacity(self) -> dict | None:
        queued_tasks = sorted(
            [task for task in self.db.list_tasks(limit=5000) if task["status"] == TaskStatus.QUEUED.value],
            key=lambda item: int(item["id"]),
        )
        for task in queued_tasks:
            if int(task.get("exclusive_node") or 0):
                continue
            if not self.has_inflight_capacity_for_task(task):
                return task
        return None

    def prewarm_exclusive_demand(self) -> bool:
        queued_tasks = sorted(
            [
                task
                for task in self.db.list_tasks(limit=5000)
                if task["status"] == TaskStatus.QUEUED.value and int(task.get("exclusive_node") or 0)
            ],
            key=lambda item: int(item["id"]),
        )
        if not queued_tasks:
            return False
        reserved_allocation_ids: set[int] = set()
        opened = False
        pending_exclusive = 0
        for task in queued_tasks:
            allocation = self.find_unreserved_exclusive_capacity(task, reserved_allocation_ids)
            if allocation:
                reserved_allocation_ids.add(int(allocation["id"]))
                if allocation["state"] == AllocationStatus.PENDING.value:
                    pending_exclusive += 1
                continue
            if pending_exclusive:
                break
            if self.open_allocation_for_task(task):
                opened = True
                allocation = self.find_unreserved_exclusive_capacity(task, reserved_allocation_ids)
                if allocation:
                    reserved_allocation_ids.add(int(allocation["id"]))
                    if allocation["state"] == AllocationStatus.PENDING.value:
                        pending_exclusive += 1
            else:
                break
        return opened

    def find_unreserved_exclusive_capacity(self, task: dict, reserved_allocation_ids: set[int]) -> dict | None:
        for allocation in self.db.list_allocations(limit=500):
            if int(allocation["id"]) in reserved_allocation_ids:
                continue
            if allocation["state"] not in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }:
                continue
            if not int(allocation.get("exclusive_node") or 0):
                continue
            if self.allocation_can_run_task(allocation, task, include_pending=True):
                return allocation
        return None

    def open_allocation_for_task(self, task: dict) -> bool:
        if self.task_requires_gpu(task):
            model = self.choose_gpu_model_for_task(task) or self.choose_gpu_model_for_prewarm()
            resource_pool = f"gpu:{model}" if model else ""
            if not model or self.allocation_pool_in_backoff(resource_pool):
                return False
            return self.open_allocation(
                f"queued GPU demand {model}",
                resource_pool=resource_pool,
                gpu_model=model,
                gpus=max(1, int(task.get("gpus") or self.gpu_prewarm_gpus_per_allocation)),
                exclusive_node=bool(task.get("exclusive_node")),
                required_capability=str(task.get("required_capability") or ""),
                env_profile=str(task.get("env_profile") or ""),
                account_name=str(task.get("account_name") or ""),
                requested_cpus=int(task.get("cpus") or 0),
                requested_memory_mb=int(task.get("memory_mb") or 0),
            )
        if self.allocation_pool_in_backoff("cpu"):
            return False
        return self.open_allocation(
            "queued CPU demand",
            resource_pool="cpu",
            exclusive_node=bool(task.get("exclusive_node")),
            required_capability=str(task.get("required_capability") or ""),
            env_profile=str(task.get("env_profile") or ""),
            account_name=str(task.get("account_name") or ""),
            requested_cpus=int(task.get("cpus") or 0),
            requested_memory_mb=int(task.get("memory_mb") or 0),
        )

    def scale_in_idle_allocations(self) -> None:
        self.scale_in_unneeded_demand_allocations()
        warm_allocations = [
            item
            for item in self.db.list_allocations(limit=500)
            if item["state"] == AllocationStatus.WARM.value
        ]
        self.scale_in_pool(
            [item for item in warm_allocations if (item.get("resource_pool") or "cpu") == "cpu"],
            self.min_warm_allocations,
            self.allocation_scale_in_idle_seconds,
        )
        self.scale_in_pool(
            [item for item in warm_allocations if (item.get("resource_pool") or "cpu").startswith("gpu:")],
            self.gpu_prewarm_min_warm_allocations if self.gpu_prewarm_enabled else 0,
            self.allocation_scale_in_idle_seconds,
        )

    def scale_in_pool(self, warm_allocations: list[dict], minimum: int, idle_seconds: int) -> None:
        if len(warm_allocations) <= minimum:
            return
        warm_allocations.sort(key=lambda item: item.get("last_active_at") or item.get("started_at") or item.get("created_at") or "")
        excess = len(warm_allocations) - minimum
        for allocation in warm_allocations[:excess]:
            last_active = self._timestamp(allocation.get("last_active_at") or allocation.get("started_at") or allocation.get("created_at"))
            if not last_active:
                continue
            if (self._now() - last_active).total_seconds() >= idle_seconds:
                self.close_allocation(allocation, "idle scale-in")

    def scale_in_unneeded_demand_allocations(self) -> None:
        queued_tasks = [task for task in self.db.list_tasks(limit=5000) if task["status"] == TaskStatus.QUEUED.value]
        reserved_task_ids: set[int] = set()
        demand_allocations = [
            allocation
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value}
            and str(allocation.get("drain_reason") or "").startswith("queued ")
        ]
        demand_allocations.sort(key=lambda item: int(item.get("id") or 0))
        for allocation in demand_allocations:
            matched_task = None
            for task in queued_tasks:
                if int(task["id"]) in reserved_task_ids:
                    continue
                if self.allocation_can_run_task(allocation, task, include_pending=True):
                    matched_task = task
                    break
            if matched_task:
                reserved_task_ids.add(int(matched_task["id"]))
            else:
                self.close_allocation(allocation, "demand allocation no longer needed")

    def open_allocation(
        self,
        reason: str,
        resource_pool: str = "cpu",
        gpu_model: str = "",
        gpus: int = 0,
        exclusive_node: bool = False,
        preferred_accounts: list[str] | None = None,
        required_capability: str = "",
        env_profile: str = "",
        account_name: str = "",
        requested_cpus: int = 0,
        requested_memory_mb: int = 0,
    ) -> bool:
        account = self.choose_account_for_allocation(
            preferred_accounts=preferred_accounts,
            required_capability=required_capability,
            env_profile=env_profile,
            account_name=account_name,
        )
        if not account:
            return False
        shape = self.choose_allocation_shape(
            resource_pool=resource_pool,
            gpu_model=gpu_model,
            gpus=gpus,
            exclusive_node=exclusive_node,
            requested_cpus=requested_cpus,
            requested_memory_mb=requested_memory_mb,
        )
        if not shape:
            return False
        allocation_id = self.db.create_allocation(
            account_name=account.name,
            partition=shape["partition"],
            node_name=shape["node_name"],
            total_cpus=shape["cpus"],
            total_memory_mb=shape["memory_mb"],
            total_gpus=shape["gpus"],
            gpu_model=shape["gpu_model"],
            resource_pool=resource_pool,
            exclusive_node=shape["exclusive_node"],
        )
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            return False
        try:
            time_limit = self.gpu_prewarm_time_limit if resource_pool.startswith("gpu:") else self.allocation_time_limit
            result = self.client_factory(account).submit_allocation(allocation, time_limit)
        except Exception as exc:
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.FAILED.value,
                failure_message=f"{reason}: {exc}",
                closed_at="CURRENT_TIMESTAMP",
            )
            return False
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            submitted_at="CURRENT_TIMESTAMP",
            drain_reason=reason,
            **result,
        )
        return True

    def choose_account_for_allocation(
        self,
        preferred_accounts: list[str] | None = None,
        required_capability: str = "",
        env_profile: str = "",
        account_name: str = "",
    ) -> AccountConfig | None:
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in self.snapshots()}
        open_by_account: dict[str, int] = {}
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
                AllocationStatus.CLOSING.value,
            }:
                open_by_account[allocation["account_name"]] = open_by_account.get(allocation["account_name"], 0) + 1
        candidates = []
        requested_accounts = self.requested_accounts(account_name)
        for account in self.accounts:
            if requested_accounts and account.name not in requested_accounts:
                continue
            if not self.account_supports(account, required_capability, env_profile):
                continue
            snapshot = snapshots_by_name.get(account.name)
            if not snapshot:
                continue
            max_total = max(0, account.max_total_jobs - self.allocation_reserved_job_slots)
            if snapshot.running + snapshot.pending >= max_total:
                continue
            if open_by_account.get(account.name, 0) >= max_total:
                continue
            candidates.append(account)
        if not candidates:
            return None
        ordered_preferences = preferred_accounts or self.requested_accounts(account_name)
        preferred_index = {name: index for index, name in enumerate(ordered_preferences)}
        return min(
            candidates,
            key=lambda account: (
                0 if account.name in preferred_index else 1,
                preferred_index.get(account.name, len(preferred_index)),
                snapshots_by_name[account.name].score,
            ),
        )

    def live_gpu_allocations(self) -> list[dict]:
        return [
            allocation
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
            and (allocation.get("resource_pool") or "").startswith("gpu:")
        ]

    def choose_gpu_model_for_prewarm(self) -> str:
        capacity = self.gpu_capacity_summary()
        by_model = {item["gpu_model"]: item for item in capacity}
        live_count = len(self.live_gpu_allocations())
        if live_count >= self.gpu_prewarm_max_warm_allocations:
            return ""
        for model in self.gpu_prewarm_preferred_models:
            item = by_model.get(model)
            if item and int(item["cluster_free_gpus"]) >= self.gpu_prewarm_gpus_per_allocation:
                return model
        return self.gpu_prewarm_preferred_models[0] if self.gpu_prewarm_preferred_models else ""

    def choose_gpu_model_for_task(self, task: dict) -> str:
        candidates = gpu_model_candidates(str(task.get("gpu_model") or ""))
        if not candidates:
            return ""
        if len(candidates) == 1:
            return candidates[0]
        capacity = {item["gpu_model"]: item for item in self.gpu_capacity_summary()}
        requested_gpus = max(1, int(task.get("gpus") or self.gpu_prewarm_gpus_per_allocation))
        for model in candidates:
            item = capacity.get(model)
            if item and int(item.get("cluster_free_gpus") or 0) >= requested_gpus:
                return model
        return candidates[0]

    def choose_gpu_model_for_fallback(self, excluded_models: set[str]) -> str:
        capacity = self.gpu_capacity_summary()
        candidates = [
            item
            for item in capacity
            if normalize_gpu_model(str(item.get("gpu_model") or "")) not in excluded_models
            and int(item.get("cluster_free_gpus") or 0) >= self.gpu_prewarm_gpus_per_allocation
        ]
        if not candidates:
            return ""
        preferred_index = {model: index for index, model in enumerate(self.gpu_prewarm_preferred_models)}
        candidates.sort(
            key=lambda item: (
                0 if item["gpu_model"] in preferred_index else 1,
                preferred_index.get(item["gpu_model"], len(preferred_index)),
                -int(item.get("score") or 0),
                -int(item.get("cluster_free_gpus") or 0),
            )
        )
        return normalize_gpu_model(str(candidates[0].get("gpu_model") or ""))

    def choose_allocation_shape(
        self,
        resource_pool: str = "cpu",
        gpu_model: str = "",
        gpus: int = 0,
        exclusive_node: bool = False,
        requested_cpus: int = 0,
        requested_memory_mb: int = 0,
    ) -> dict | None:
        inventory_by_node = {row["node_name"]: row for row in self.db.list_node_inventory()}
        nodes = [
            PestatNode(
                hostname=row["hostname"],
                partition=row["partition"],
                state=row["state"],
                cpu_used=row["cpu_used"],
                cpu_total=row["cpu_total"],
                cpu_load=row["cpu_load"],
                memory_mb=row["memory_mb"],
                free_memory_mb=row["free_memory_mb"],
            )
            for row in self.db.list_pestat_nodes()
        ]
        if not nodes:
            nodes = [
                PestatNode(
                    hostname=row["node_name"],
                    partition=row["partition"],
                    state=row["state"],
                    cpu_used=0,
                    cpu_total=int(row["cpus"]),
                    cpu_load=0.0,
                    memory_mb=int(row["memory_mb"]),
                    free_memory_mb=int(row["memory_mb"]),
                )
                for row in self.db.list_node_inventory()
            ]
        candidates = []
        wants_gpu = resource_pool.startswith("gpu:") or int(gpus or 0) > 0
        target_models = gpu_model_candidates(gpu_model)
        target_partition = self.gpu_prewarm_partition if wants_gpu else self.allocation_partition
        occupied_by_partition: dict[str, set[str]] = {}
        for node in nodes:
            if not node.usable:
                continue
            if self.is_single_job_partition(node.partition):
                if exclusive_node and not wants_gpu and self.partition_has_live_allocation(node.partition, resource_pool="cpu"):
                    continue
                occupied = occupied_by_partition.setdefault(
                    node.partition,
                    self.occupied_single_job_nodes(node.partition, include_queued_jobs=True),
                )
                if node.hostname in occupied or int(node.cpu_used) > 0:
                    continue
            inventory = inventory_by_node.get(node.hostname, {})
            node_gpu_count = int(inventory.get("gpu_count") or 0)
            node_gpu_used = int(inventory.get("gpu_used_count") or 0)
            node_gpu_model = normalize_gpu_model(str(inventory.get("gpu_model") or ""))
            if wants_gpu:
                if target_partition != "auto" and node.partition != target_partition:
                    continue
                if target_models and node_gpu_model not in target_models:
                    continue
                if node_gpu_count <= 0:
                    continue
                if max(0, node_gpu_count - node_gpu_used) < max(1, int(gpus or 1)):
                    continue
            else:
                if target_partition != "auto" and node.partition != target_partition:
                    continue
                if target_partition == "auto" and node_gpu_count > 0 and not self.cpu_pool_allow_gpu_partitions:
                    continue
            if target_partition != "auto" and node.partition != target_partition:
                continue
            gpu_free = max(0, node_gpu_count - node_gpu_used)
            requested_gpus = min(max(1, int(gpus or self.gpu_prewarm_gpus_per_allocation)), gpu_free) if wants_gpu else 0
            leaves_unclaimed_gpus = wants_gpu and gpu_free > requested_gpus
            reserve = self.gpu_cpu_reserve if node.partition.startswith("gpu") and (not wants_gpu or leaves_unclaimed_gpus) else 0
            available_cpus = node.effective_free_cpus - reserve
            if wants_gpu and available_cpus <= 0 and node.effective_free_cpus > 0:
                available_cpus = node.effective_free_cpus
            if available_cpus <= 0:
                continue
            if requested_cpus and available_cpus < requested_cpus:
                continue
            cpus = requested_cpus or self.allocation_cpus or available_cpus
            cpus = max(1, min(cpus, available_cpus))
            if requested_memory_mb and node.free_memory_mb < requested_memory_mb:
                continue
            memory_mb = requested_memory_mb or self._memory_mb(self.allocation_memory) or node.free_memory_mb
            memory_mb = max(1024, min(memory_mb, node.free_memory_mb))
            if cpus > 0 and memory_mb > 0:
                cpu_profile = CPU_PROFILES_BY_PARTITION.get(node.partition, {})
                cpu_score = int(inventory.get("cpu_score") or cpu_profile.get("cpu_score") or 0)
                score = GPU_PRIORITY.get(node_gpu_model, 0) if wants_gpu else cpu_score
                candidates.append((node, cpus, memory_mb, node_gpu_model, gpu_free, score, cpu_score))
        if candidates:
            if wants_gpu:
                candidates.sort(key=lambda item: (item[5], item[4], item[1], item[2], item[0].effective_free_cpus), reverse=True)
            elif exclusive_node:
                candidates.sort(
                    key=lambda item: (
                        0 if item[0].partition.startswith("gpu") else 1,
                        item[6],
                        item[0].effective_free_cpus,
                        item[1],
                        item[2],
                    ),
                    reverse=True,
                )
            else:
                candidates.sort(key=lambda item: (item[6], item[0].effective_free_cpus, item[1], item[2]), reverse=True)
            node, cpus, memory_mb, chosen_gpu_model, gpu_free, _score, _cpu_score = candidates[0]
            chosen_gpus = min(max(1, int(gpus or self.gpu_prewarm_gpus_per_allocation)), gpu_free) if wants_gpu else 0
            node_name = node.hostname
            if not wants_gpu and not self.is_single_job_partition(node.partition):
                node_name = ""
            return {
                "partition": node.partition,
                "node_name": node_name,
                "cpus": cpus,
                "memory_mb": memory_mb,
                "gpus": chosen_gpus,
                "gpu_model": chosen_gpu_model if wants_gpu else "",
                "exclusive_node": exclusive_node,
            }
        if target_partition != "auto" and self.is_single_job_partition(target_partition):
            return None
        partition = target_partition if target_partition != "auto" else self.choose_partition({"gpus": max(1, int(gpus or 1)) if wants_gpu else 0})
        if self.is_single_job_partition(partition):
            return None
        return {
            "partition": partition,
            "node_name": "",
            "cpus": requested_cpus or self.allocation_cpus or 4,
            "memory_mb": requested_memory_mb or self._memory_mb(self.allocation_memory) or 16384,
            "gpus": max(1, int(gpus or self.gpu_prewarm_gpus_per_allocation)) if wants_gpu else 0,
            "gpu_model": (target_models[0] if target_models else "") if wants_gpu else "",
            "exclusive_node": exclusive_node,
        }

    def _memory_mb(self, value: str) -> int:
        raw = (value or "").strip().lower()
        if not raw or raw == "0":
            return 0
        try:
            if raw.endswith("gb") or raw.endswith("g"):
                return int(float(raw.rstrip("gb")) * 1024)
            if raw.endswith("mb") or raw.endswith("m"):
                return int(float(raw.rstrip("mb")))
            return int(float(raw))
        except ValueError:
            return 0

    def gpu_capacity_summary(self) -> list[dict]:
        summaries: dict[str, dict] = {}
        for row in self.db.list_node_inventory():
            model = normalize_gpu_model(str(row.get("gpu_model") or ""))
            if not model:
                continue
            total = int(row.get("gpu_count") or 0)
            used = min(total, max(0, int(row.get("gpu_used_count") or 0)))
            state = str(row.get("state") or "").lower()
            available_state = state in {"idle", "mix", "mixed"}
            item = summaries.setdefault(
                model,
                {
                    "gpu_model": model,
                    "cluster_total_gpus": 0,
                    "cluster_used_gpus": 0,
                    "cluster_free_gpus": 0,
                    "scheduler_owned_gpus": 0,
                    "scheduler_free_gpus": 0,
                    "nodes": 0,
                    "available_nodes": 0,
                    "pending_gpu_tasks": 0,
                    "pending_gpu_jobs": 0,
                    "score": GPU_PRIORITY.get(model, 0),
                },
            )
            item["nodes"] += 1
            item["cluster_total_gpus"] += total
            item["cluster_used_gpus"] += used
            if available_state:
                item["available_nodes"] += 1
                item["cluster_free_gpus"] += max(0, total - used)
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }:
                continue
            model = normalize_gpu_model(str(allocation.get("gpu_model") or ""))
            if not model:
                continue
            item = summaries.setdefault(
                model,
                {
                    "gpu_model": model,
                    "cluster_total_gpus": 0,
                    "cluster_used_gpus": 0,
                    "cluster_free_gpus": 0,
                    "scheduler_owned_gpus": 0,
                    "scheduler_free_gpus": 0,
                    "nodes": 0,
                    "available_nodes": 0,
                    "pending_gpu_tasks": 0,
                    "pending_gpu_jobs": 0,
                    "score": GPU_PRIORITY.get(model, 0),
                },
            )
            item["scheduler_owned_gpus"] += int(allocation.get("total_gpus") or 0)
            item["scheduler_free_gpus"] += int(allocation.get("free_gpus") or 0)
        for task in self.db.list_tasks(limit=5000):
            if task["status"] != TaskStatus.QUEUED.value or int(task.get("gpus") or 0) <= 0:
                continue
            model = normalize_gpu_model(str(task.get("gpu_model") or "")) or "unspecified"
            item = summaries.setdefault(
                model,
                {
                    "gpu_model": model,
                    "cluster_total_gpus": 0,
                    "cluster_used_gpus": 0,
                    "cluster_free_gpus": 0,
                    "scheduler_owned_gpus": 0,
                    "scheduler_free_gpus": 0,
                    "nodes": 0,
                    "available_nodes": 0,
                    "pending_gpu_tasks": 0,
                    "pending_gpu_jobs": 0,
                    "score": GPU_PRIORITY.get(model, 0),
                },
            )
            item["pending_gpu_tasks"] += int(task.get("gpus") or 0)
        for job in self.db.list_jobs(limit=5000):
            if job["status"] != JobStatus.QUEUED.value or int(job.get("gpus") or 0) <= 0:
                continue
            model = normalize_gpu_model(str(job.get("gpu_model") or "")) or "unspecified"
            item = summaries.setdefault(
                model,
                {
                    "gpu_model": model,
                    "cluster_total_gpus": 0,
                    "cluster_used_gpus": 0,
                    "cluster_free_gpus": 0,
                    "scheduler_owned_gpus": 0,
                    "scheduler_free_gpus": 0,
                    "nodes": 0,
                    "available_nodes": 0,
                    "pending_gpu_tasks": 0,
                    "pending_gpu_jobs": 0,
                    "score": GPU_PRIORITY.get(model, 0),
                },
            )
            item["pending_gpu_jobs"] += int(job.get("gpus") or 0)
        return sorted(summaries.values(), key=lambda item: (item["score"], item["cluster_free_gpus"]), reverse=True)

    def snapshots(self) -> list[AccountSnapshot]:
        now = time.time()
        if self._snapshot_cache and now - self._snapshot_cache[0] < self.poll_interval_seconds:
            return self._snapshot_cache[1]
        snapshots = []
        for account in self.accounts:
            client = self.client_factory(account)
            storage_used = self.cached_storage(account, client, now)
            snapshots.append(client.snapshot(storage_used_gb=storage_used))
        self._snapshot_cache = (now, snapshots)
        return snapshots

    def cached_storage(self, account: AccountConfig, client: SlurmAccountClient, now: float) -> float | None:
        cached = self._storage_cache.get(account.name)
        if cached and now - cached[0] < self._storage_refresh_interval_seconds:
            return cached[1]
        try:
            value = client.storage_used_gb()
        except Exception:
            value = cached[1] if cached else None
        self._storage_cache[account.name] = (now, value)
        return value

    def cached_snapshots(self) -> list[AccountSnapshot]:
        if not self._snapshot_cache:
            return []
        return self._snapshot_cache[1]

    def choose_account(self, required_capability: str = "", env_profile: str = "", account_name: str = "") -> AccountConfig | None:
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in self.snapshots()}
        requested_accounts = self.requested_accounts(account_name)
        candidates = [
            account
            for account in self.accounts
            if (not requested_accounts or account.name in requested_accounts)
            and snapshots_by_name.get(account.name) and snapshots_by_name[account.name].available
            and self.account_supports(account, required_capability, env_profile)
        ]
        if not candidates:
            return None
        requested_index = {name: index for index, name in enumerate(requested_accounts)}
        return min(
            candidates,
            key=lambda account: (
                requested_index.get(account.name, len(requested_index)),
                snapshots_by_name[account.name].score,
            ),
        )

    def submit_next_queued_job(self) -> None:
        job = self.db.next_queued_job()
        if not job:
            return
        account = self.choose_account(
            str(job.get("required_capability") or ""),
            str(job.get("env_profile") or ""),
            str(job.get("account_name") or ""),
        )
        if not account:
            return
        if int(job.get("gpus") or 0) > 0:
            model = self.choose_gpu_model_for_task(job)
            if model and job.get("gpu_model") != model:
                job["gpu_model"] = model
                self.db.update_job(job["id"], gpu_model=model)
        partition = self.choose_partition(job)
        if partition and job.get("partition") != partition:
            job["partition"] = partition
            self.db.update_job(job["id"], partition=partition)
        if not self.prepare_single_job_node(job):
            return
        self.db.update_job(job["id"], status=JobStatus.SUBMITTING.value, account_name=account.name)
        try:
            result = self.client_factory(account).submit(job)
        except RemoteExecutionError as exc:
            self.db.update_job(
                job["id"],
                status=JobStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
                **exc.result_fields,
            )
            return
        except Exception as exc:
            self.db.update_job(
                job["id"],
                status=JobStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
            )
            return
        self.db.update_job(
            job["id"],
            status=JobStatus.SUBMITTED.value,
            submitted_at="CURRENT_TIMESTAMP",
            **result,
        )

    def choose_partition(self, job: dict) -> str:
        requested = (job.get("partition") or "").strip()
        if requested and requested.lower() != "auto":
            return requested
        rows = self.db.list_node_inventory()
        requested_models = gpu_model_candidates(str(job.get("gpu_model") or ""))
        if requested_models:
            rows = [row for row in rows if normalize_gpu_model(str(row.get("gpu_model") or "")) in requested_models]
        ranked = partition_rank(rows, needs_gpu=int(job.get("gpus") or 0) > 0)
        for item in ranked:
            partition = item["partition"]
            if self.is_single_job_partition(partition) and not self.choose_single_job_node(partition, exclude_job_id=job.get("id")):
                continue
            return partition
        return "gpu3" if int(job.get("gpus") or 0) > 0 else "cpu1"

    def prepare_single_job_node(self, job: dict) -> bool:
        partition = str(job.get("partition") or "")
        if not self.is_single_job_partition(partition):
            return True
        node_name = self.choose_single_job_node(
            partition,
            requested_node=str(job.get("node_name") or ""),
            exclude_job_id=int(job["id"]),
        )
        if not node_name:
            return False
        if job.get("node_name") != node_name:
            job["node_name"] = node_name
            self.db.update_job(job["id"], node_name=node_name)
        return True

    def choose_single_job_node(
        self,
        partition: str,
        requested_node: str = "",
        exclude_job_id: int | None = None,
    ) -> str:
        occupied = self.occupied_single_job_nodes(partition, exclude_job_id=exclude_job_id)
        pestat_rows = [
            PestatNode(
                hostname=row["hostname"],
                partition=row["partition"],
                state=row["state"],
                cpu_used=row["cpu_used"],
                cpu_total=row["cpu_total"],
                cpu_load=row["cpu_load"],
                memory_mb=row["memory_mb"],
                free_memory_mb=row["free_memory_mb"],
            )
            for row in self.db.list_pestat_nodes()
            if row["partition"] == partition
        ]
        if requested_node:
            if requested_node in occupied:
                return ""
            matching = [node for node in pestat_rows if node.hostname == requested_node]
            if matching:
                node = matching[0]
                return requested_node if node.usable and int(node.cpu_used) == 0 else ""
            inventory = {row["node_name"]: row for row in self.db.list_node_inventory() if row["partition"] == partition}
            row = inventory.get(requested_node)
            if row:
                return requested_node if str(row.get("state") or "").lower() == "idle" else ""
            return requested_node
        candidates = [
            node
            for node in pestat_rows
            if node.hostname not in occupied and node.usable and int(node.cpu_used) == 0
        ]
        if candidates:
            candidates.sort(key=lambda node: (node.effective_free_cpus, node.free_memory_mb), reverse=True)
            return candidates[0].hostname
        inventory_candidates = [
            row
            for row in self.db.list_node_inventory()
            if row["partition"] == partition
            and row["node_name"] not in occupied
            and str(row.get("state") or "").lower() == "idle"
        ]
        if inventory_candidates:
            inventory_candidates.sort(key=lambda row: (int(row.get("cpus") or 0), int(row.get("memory_mb") or 0)), reverse=True)
            return str(inventory_candidates[0]["node_name"])
        return ""

    def refresh_submitted_jobs(self) -> None:
        accounts_by_name = {account.name: account for account in self.accounts}
        for job in self.db.list_jobs(limit=500):
            if job["status"] not in {JobStatus.SUBMITTED.value, JobStatus.RUNNING.value}:
                continue
            if not job["account_name"] or not job["slurm_job_id"]:
                continue
            account = accounts_by_name.get(job["account_name"])
            if not account:
                continue
            try:
                status = self.client_factory(account).state(job["slurm_job_id"])
            except Exception as exc:
                LOGGER.warning("failed to refresh job %s: %s", job["id"], exc)
                continue
            updates = {"status": status.value}
            if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}:
                updates["finished_at"] = "CURRENT_TIMESTAMP"
            self.db.update_job(job["id"], **updates)

    def cancel(self, job_id: int) -> None:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError("job not found")
        if not job["account_name"] or not job["slurm_job_id"]:
            self.db.update_job(job_id, status=JobStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
            return
        account = next((item for item in self.accounts if item.name == job["account_name"]), None)
        if not account:
            raise ValueError("account not found")
        self.client_factory(account).cancel(job["slurm_job_id"])
        self.db.update_job(job_id, status=JobStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")

    def cancel_task(self, task_id: int) -> None:
        task = self.db.get_task(task_id)
        if not task:
            raise ValueError("task not found")
        if task["status"] in {TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value}:
            return
        account = self.account_by_name(str(task.get("account_name") or ""))
        if account and task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
            try:
                self.client_factory(account).cancel_task(task)
            except Exception as exc:
                LOGGER.warning("failed to cancel task %s remotely: %s", task_id, exc)
        self.db.update_task(task_id, status=TaskStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
        if task.get("allocation_id"):
            self.recalculate_allocation_capacity()

    def cancel_tasks(
        self,
        name_contains: str = "",
        statuses: set[str] | None = None,
        limit: int = 5000,
    ) -> list[int]:
        wanted_statuses = statuses or {TaskStatus.QUEUED.value, TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        needle = name_contains.strip().lower()
        cancelled: list[int] = []
        for task in self.db.list_tasks(limit=limit):
            if task["status"] not in wanted_statuses:
                continue
            if needle and needle not in str(task.get("name") or "").lower():
                continue
            self.cancel_task(int(task["id"]))
            cancelled.append(int(task["id"]))
        return sorted(cancelled)
