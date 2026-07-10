from __future__ import annotations

import fnmatch
import logging
import os
import posixpath
import re
import shlex
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .config import AccountConfig
from .db import Database
from .inventory import CPU_PROFILES_BY_PARTITION, GPU_PRIORITY, gpu_model_candidates, normalize_gpu_model, parse_scontrol_nodes, parse_sinfo_nodes, partition_rank
from .models import AccountSnapshot, AllocationStatus, JobStatus, SchedulingProfile, TaskStatus, normalize_scheduling_profile
from .pestat import PestatNode, parse_pestat
from .slurm import (
    JobStateInfo,
    RemoteExecutionError,
    SSHSession,
    SlurmAccountClient,
    StorageQuotaProbe,
    TaskProbe,
    resolve_task_placeholders,
    shell_path,
)

LOGGER = logging.getLogger(__name__)
ClientFactory = Callable[[AccountConfig], SlurmAccountClient]


LMSTAT_FEATURE_RE = re.compile(
    r"Users of (\S+):\s+\(Total of (\d+) licenses? issued;\s+Total of (\d+) licenses? in use\)"
)


def parse_lmstat_features(output: str) -> list[dict]:
    features = []
    for match in LMSTAT_FEATURE_RE.finditer(output or ""):
        features.append(
            {"feature": match.group(1), "total": int(match.group(2)), "used": int(match.group(3))}
        )
    return features


class AccountUnavailableThisTick(RuntimeError):
    """The account already failed once this tick; skip it until the next tick."""


class _TickClientCache:
    """Per-thread cache of clients (and their shared SSH sessions) for one tick.

    Real clients get one SSH session reused across all their commands; fakes
    without bind_shared_session are cached as-is.
    """

    def __init__(self, client_factory: ClientFactory):
        self._client_factory = client_factory
        self._clients: dict[str, Any] = {}
        self._sessions: dict[str, SSHSession] = {}
        self._failed: set[str] = set()

    def client(self, account: AccountConfig) -> Any:
        if account.name in self._failed:
            raise AccountUnavailableThisTick(f"account {account.name} marked unavailable this tick")
        cached = self._clients.get(account.name)
        if cached is not None:
            return cached
        client = self._client_factory(account)
        bind = getattr(client, "bind_shared_session", None)
        if callable(bind):
            session = SSHSession(account, default_timeout=getattr(client, "command_timeout", None))
            bind(session)
            self._sessions[account.name] = session
        self._clients[account.name] = client
        return client

    def mark_failed(self, account_name: str) -> None:
        self._failed.add(account_name)
        self._clients.pop(account_name, None)
        session = self._sessions.pop(account_name, None)
        if session is not None:
            try:
                session.close()
            except Exception:
                pass

    def close_all(self) -> None:
        for session in self._sessions.values():
            try:
                session.close()
            except Exception:
                pass
        self._sessions.clear()
        self._clients.clear()
        self._failed.clear()

    def force_close_all(self) -> None:
        for session in list(self._sessions.values()):
            session.force_close()


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
        allocation_max_new_per_loop: int = 8,
        cpu_pool_allow_gpu_partitions: bool = True,
        cpu_pool_partition_spread: bool = False,
        warm_pool_preferred_accounts: list[str] | None = None,
        gpu_warm_pool_preferred_accounts: list[str] | None = None,
        single_job_per_node_partitions: list[str] | None = None,
        cpu_partition_allocation_limits: dict[str, int] | None = None,
        gpu_cpu_reserve: int = 4,
        gpu_prewarm_enabled: bool = False,
        gpu_prewarm_preferred_models: list[str] | None = None,
        gpu_prewarm_min_warm_allocations: int = 1,
        gpu_prewarm_max_warm_allocations: int = 3,
        gpu_prewarm_gpus_per_allocation: int = 2,
        gpu_prewarm_min_gpus_per_allocation: int = 2,
        gpu_prewarm_cpus_per_allocation: int = 0,
        gpu_prewarm_cpu_reserve_per_free_gpu: int = 8,
        gpu_prewarm_stagger_seconds: int = 86400,
        gpu_prewarm_memory: str = "128G",
        gpu_prewarm_partition: str = "auto",
        gpu_prewarm_time_limit: str = "48:00:00",
        gpu_prewarm_pinned_pending_timeout_seconds: int = 300,
        fea_soft_memory_free_percent: float = 60.0,
        fea_hard_memory_free_percent: float = 40.0,
        fea_load_target: float = 0.75,
        fea_max_attach_per_loop: int = 8,
        fea_node_name_policy: str = "preferred",
        fea_overload_scale_out_load_factor: float = 2.0,
        fea_overload_scale_out_seconds: int = 300,
        fea_pressure_max_attempts: int = 3,
        fea_max_attach_per_node_per_loop: int = 8,
        fea_node_requested_cpu_factor: float = 1.0,
        fea_footprint_maturity_seconds: int = 900,
        task_refresh_max_per_tick: int = 32,
        cleanup_enabled: bool = True,
        cleanup_interval_seconds: int = 3600,
        cleanup_finished_task_ttl_seconds: int = 259200,
        cleanup_finished_job_ttl_seconds: int = 259200,
        cleanup_closed_allocation_ttl_seconds: int = 86400,
        orphan_process_sweep_enabled: bool = False,
        orphan_process_sweep_interval_seconds: int = 600,
        orphan_process_min_age_seconds: int = 1800,
        orphan_process_name_patterns: list[str] | None = None,
        reconcile_on_start: bool = False,
        backup_enabled: bool = False,
        backup_interval_seconds: int = 86400,
        backup_keep: int = 7,
        backup_dir: str = "data/backups",
        cleanup_orphan_sweep_enabled: bool = True,
        cleanup_orphan_sweep_interval_seconds: int = 86400,
        cleanup_orphan_min_age_seconds: int = 604800,
        cleanup_workspace_prune_globs: list[str] | None = None,
        cleanup_workspace_prune_interval_seconds: int = 21600,
        cleanup_workspace_prune_min_age_seconds: int = 86400,
        cleanup_finished_task_log_max_bytes: int = 0,
        cleanup_finished_task_log_trim_after_seconds: int = 86400,
        storage_guard_min_free_gb: float = 0.0,
        license_monitor_enabled: bool = False,
        license_monitor_account: str = "",
        license_monitor_lmutil_path: str = "",
        license_monitor_license_server: str = "",
        license_monitor_interval_seconds: int = 300,
        license_monitor_watch_features: list[str] | None = None,
        license_monitor_display: dict[str, str] | None = None,
        cleanup_db_row_ttl_seconds: int = 1209600,
        cleanup_event_ttl_seconds: int = 604800,
        watchdog_enabled: bool = True,
        watchdog_stall_seconds: int = 0,
        ssh_parallelism: int = 4,
    ):
        self.db = db
        self.accounts = accounts
        self.poll_interval_seconds = poll_interval_seconds
        self.client_factory = client_factory
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_cache: tuple[float, list[AccountSnapshot]] | None = None
        self._storage_cache: dict[str, tuple[float, float | None]] = {}
        self._storage_quota_cache: dict[str, tuple[float, StorageQuotaProbe]] = {}
        self._storage_refresh_interval_seconds = max(900, poll_interval_seconds * 20)
        self._storage_quota_refresh_interval_seconds = max(10, min(30, poll_interval_seconds))
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
        self.allocation_max_new_per_loop = max(1, int(allocation_max_new_per_loop))
        self.cpu_pool_allow_gpu_partitions = cpu_pool_allow_gpu_partitions
        self.cpu_pool_partition_spread = cpu_pool_partition_spread
        self.warm_pool_preferred_accounts = warm_pool_preferred_accounts or []
        self.gpu_warm_pool_preferred_accounts = gpu_warm_pool_preferred_accounts or []
        self.single_job_per_node_partitions = {
            partition.strip() for partition in (single_job_per_node_partitions if single_job_per_node_partitions is not None else ["cpu2"]) if partition.strip()
        }
        self.cpu_partition_allocation_limits = {
            str(partition).strip(): max(0, int(limit or 0))
            for partition, limit in (cpu_partition_allocation_limits or {"cpu2": 2}).items()
            if str(partition).strip() and int(limit or 0) > 0
        }
        self.gpu_cpu_reserve = gpu_cpu_reserve
        self.gpu_prewarm_enabled_default = gpu_prewarm_enabled
        self.gpu_prewarm_preferred_models = [
            normalize_gpu_model(model) for model in (gpu_prewarm_preferred_models or ["a6000"])
        ]
        self.gpu_prewarm_min_warm_allocations = gpu_prewarm_min_warm_allocations
        self.gpu_prewarm_max_warm_allocations = gpu_prewarm_max_warm_allocations
        self.gpu_prewarm_gpus_per_allocation = gpu_prewarm_gpus_per_allocation
        self.gpu_prewarm_min_gpus_per_allocation = gpu_prewarm_min_gpus_per_allocation
        self.gpu_prewarm_cpus_per_allocation = max(0, int(gpu_prewarm_cpus_per_allocation or 0))
        self.gpu_prewarm_cpu_reserve_per_free_gpu = gpu_prewarm_cpu_reserve_per_free_gpu
        self.gpu_prewarm_stagger_seconds = max(0, int(gpu_prewarm_stagger_seconds))
        self.gpu_prewarm_memory = gpu_prewarm_memory
        self.gpu_prewarm_partition = gpu_prewarm_partition
        self.gpu_prewarm_time_limit = gpu_prewarm_time_limit
        self.gpu_prewarm_pinned_pending_timeout_seconds = max(0, int(gpu_prewarm_pinned_pending_timeout_seconds))
        self.fea_soft_memory_free_percent = max(0.0, float(fea_soft_memory_free_percent))
        self.fea_hard_memory_free_percent = max(0.0, float(fea_hard_memory_free_percent))
        self.fea_load_target = max(0.0, float(fea_load_target))
        self.fea_max_attach_per_loop = max(1, int(fea_max_attach_per_loop))
        node_policy = (fea_node_name_policy or "preferred").strip().lower()
        self.fea_node_name_policy = node_policy if node_policy in {"preferred", "strict"} else "preferred"
        self.fea_overload_scale_out_load_factor = max(0.0, float(fea_overload_scale_out_load_factor))
        self.fea_overload_scale_out_seconds = max(0, int(fea_overload_scale_out_seconds))
        self.fea_pressure_max_attempts = max(1, int(fea_pressure_max_attempts))
        self.fea_max_attach_per_node_per_loop = max(1, int(fea_max_attach_per_node_per_loop))
        self.fea_node_requested_cpu_factor = max(0.0, float(fea_node_requested_cpu_factor))
        self.fea_footprint_maturity_seconds = max(0, int(fea_footprint_maturity_seconds))
        self._fea_footprint_cache: tuple[int, dict[str, dict[str, float]]] | None = None
        self._tick_attach_workers_by_node: dict[str, int] = {}
        self._fea_pressures_cache: tuple[int, dict[str, dict[str, int]]] | None = None
        self._fea_alloc_pressures_cache: tuple[int, dict[int, dict[str, int]]] | None = None
        self._pestat_nodes_cache: tuple[int, dict[str, dict]] | None = None
        self._fea_overload_since_by_node: dict[str, float] = {}
        self._fea_overload_scaled_nodes: set[str] = set()
        self.task_refresh_max_per_tick = max(1, int(task_refresh_max_per_tick))
        self._fea_task_refresh_cursor_id = 0
        self._background_attach_semaphore = threading.BoundedSemaphore(max(1, min(4, self.fea_max_attach_per_loop)))
        self._allocation_backoff_until_by_pool: dict[str, float] = {}
        self._allocation_node_backoff_until: dict[tuple[str, str], float] = {}
        self._allocation_shape_backoff_until: dict[tuple[str, str], float] = {}
        self.cleanup_enabled = cleanup_enabled
        self.cleanup_interval_seconds = cleanup_interval_seconds
        self.cleanup_finished_task_ttl_seconds = cleanup_finished_task_ttl_seconds
        self.cleanup_finished_job_ttl_seconds = cleanup_finished_job_ttl_seconds
        self.cleanup_closed_allocation_ttl_seconds = cleanup_closed_allocation_ttl_seconds
        self._last_cleanup_at = 0.0
        self.cleanup_orphan_sweep_enabled = cleanup_orphan_sweep_enabled
        self.cleanup_orphan_sweep_interval_seconds = max(3600, int(cleanup_orphan_sweep_interval_seconds))
        self.cleanup_orphan_min_age_seconds = max(3600, int(cleanup_orphan_min_age_seconds))
        self.cleanup_db_row_ttl_seconds = max(86400, int(cleanup_db_row_ttl_seconds))
        self.cleanup_event_ttl_seconds = max(3600, int(cleanup_event_ttl_seconds))
        self._last_orphan_sweep_at = 0.0
        self.orphan_process_sweep_enabled = bool(orphan_process_sweep_enabled)
        self.orphan_process_sweep_interval_seconds = max(60, int(orphan_process_sweep_interval_seconds))
        self.orphan_process_min_age_seconds = max(60, int(orphan_process_min_age_seconds))
        self.orphan_process_name_patterns = list(orphan_process_name_patterns or [])
        self._last_orphan_process_sweep_at = 0.0
        self.cleanup_workspace_prune_globs = list(cleanup_workspace_prune_globs or [])
        self.cleanup_workspace_prune_interval_seconds = max(3600, int(cleanup_workspace_prune_interval_seconds))
        self.cleanup_workspace_prune_min_age_seconds = max(3600, int(cleanup_workspace_prune_min_age_seconds))
        self._last_workspace_prune_at = 0.0
        self.cleanup_finished_task_log_max_bytes = max(0, int(cleanup_finished_task_log_max_bytes))
        self.cleanup_finished_task_log_trim_after_seconds = max(3600, int(cleanup_finished_task_log_trim_after_seconds))
        self.storage_guard_min_free_gb = max(0.0, float(storage_guard_min_free_gb))
        self._storage_guard_warned_at: dict[str, float] = {}
        self.license_monitor_enabled = license_monitor_enabled
        self.license_monitor_account = license_monitor_account
        self.license_monitor_lmutil_path = license_monitor_lmutil_path
        self.license_monitor_license_server = license_monitor_license_server
        self.license_monitor_interval_seconds = max(60, int(license_monitor_interval_seconds))
        self.license_monitor_watch_features = list(license_monitor_watch_features or [])
        self.license_monitor_display = dict(license_monitor_display or {})
        self._license_usage: dict = {}
        self._last_license_refresh_at = 0.0
        self._license_refresh_inflight = False
        self.reconcile_on_start = reconcile_on_start
        self._needs_reconcile = False
        self.backup_enabled = backup_enabled
        self.backup_interval_seconds = max(3600, int(backup_interval_seconds))
        self.backup_keep = max(1, int(backup_keep))
        self.backup_dir = backup_dir
        self._last_backup_at = 0.0
        self._last_tick_completed_monotonic: float | None = None
        self._last_tick_completed_at: str = ""
        self._last_tick_duration: float | None = None
        self._last_tick_stage_seconds: dict[str, float] = {}
        self._consecutive_tick_failures = 0
        self.watchdog_enabled = watchdog_enabled
        self.watchdog_stall_seconds = max(0, int(watchdog_stall_seconds))
        self._watchdog_thread: threading.Thread | None = None
        self._watchdog_exit: Callable[[int], None] = os._exit
        self._tick_started_at: float | None = None
        self._tick_seq = 0
        self._tick_local = threading.local()
        self._tick_caches: set[_TickClientCache] = set()
        self._tick_caches_lock = threading.Lock()
        self.ssh_parallelism = max(1, int(ssh_parallelism))
        self._ssh_executor = ThreadPoolExecutor(
            max_workers=self.ssh_parallelism, thread_name_prefix="ssh-fanout"
        )

    @property
    def gpu_prewarm_enabled(self) -> bool:
        override = self.db.get_setting("gpu_prewarm_enabled")
        if override is None:
            return self.gpu_prewarm_enabled_default
        return override.strip().lower() in {"1", "true", "on", "yes"}

    def set_gpu_prewarm_enabled(self, enabled: bool) -> None:
        self.db.set_setting("gpu_prewarm_enabled", "1" if enabled else "0")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.recover_transient_states()
        self._needs_reconcile = self.reconcile_on_start
        self._stop.clear()
        self._thread = threading.Thread(target=self.run_forever, name="scheduler", daemon=True)
        self._thread.start()
        if self.watchdog_enabled and (self._watchdog_thread is None or not self._watchdog_thread.is_alive()):
            self._watchdog_thread = threading.Thread(target=self._watchdog_loop, name="scheduler-watchdog", daemon=True)
            self._watchdog_thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        self._ssh_executor.shutdown(wait=False, cancel_futures=True)

    def _fan_out_by_account(
        self,
        items_by_account: dict[str, list],
        probe: Callable[[str, list], Any],
        budget_seconds: float = 120.0,
    ) -> dict[str, Any]:
        """Run one probe per account concurrently; SSH stays in the workers,
        the caller applies DB writes sequentially. A probe failure (or budget
        overrun) is returned as the Exception instead of raising."""
        results: dict[str, Any] = {}
        if not items_by_account:
            return results
        if len(items_by_account) == 1:
            account_name, items = next(iter(items_by_account.items()))
            try:
                results[account_name] = probe(account_name, items)
            except Exception as exc:
                results[account_name] = exc
            return results
        futures = {
            account_name: self._ssh_executor.submit(probe, account_name, items)
            for account_name, items in items_by_account.items()
        }
        for account_name, future in futures.items():
            try:
                results[account_name] = future.result(timeout=budget_seconds)
            except Exception as exc:
                results[account_name] = exc
        return results

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
                self._consecutive_tick_failures = 0
            except Exception:
                self._consecutive_tick_failures += 1
                LOGGER.exception("scheduler tick failed")
            self._stop.wait(self.poll_interval_seconds)

    def record_event(
        self,
        kind: str,
        message: str,
        entity_type: str = "",
        entity_id: int | str = "",
        account_name: str = "",
    ) -> None:
        try:
            self.db.record_event(
                kind, message, entity_type=entity_type, entity_id=str(entity_id), account_name=account_name
            )
        except Exception:
            LOGGER.debug("failed to record scheduler event %s", kind, exc_info=True)

    def health_status(self) -> dict:
        thread_alive = bool(self._thread and self._thread.is_alive())
        now = time.monotonic()
        stall_after = self._watchdog_stall_seconds()
        tick_in_progress_seconds = (
            now - self._tick_started_at if self._tick_started_at is not None else None
        )
        seconds_since_last_tick = (
            now - self._last_tick_completed_monotonic
            if self._last_tick_completed_monotonic is not None
            else None
        )
        stalled = bool(
            (tick_in_progress_seconds is not None and tick_in_progress_seconds >= stall_after)
            or (
                tick_in_progress_seconds is None
                and thread_alive
                and seconds_since_last_tick is not None
                and seconds_since_last_tick >= stall_after
            )
        )
        return {
            "scheduler_thread_alive": thread_alive,
            "scheduler_stalled": stalled,
            "scheduler_ok": thread_alive and not stalled,
            "last_tick_completed_at": self._last_tick_completed_at,
            "last_tick_duration_seconds": round(self._last_tick_duration, 3)
            if self._last_tick_duration is not None
            else None,
            "last_tick_stage_seconds": dict(self._last_tick_stage_seconds),
            "tick_in_progress_seconds": round(tick_in_progress_seconds, 1)
            if tick_in_progress_seconds is not None
            else None,
            "consecutive_tick_failures": self._consecutive_tick_failures,
        }

    def recover_transient_states(self) -> None:
        """Reset states that only make sense mid-operation; they orphan when the
        process dies between the DB write and the remote call completing."""
        for job in self.db.list_jobs(limit=5000):
            if job.get("status") == JobStatus.SUBMITTING.value:
                LOGGER.warning("recovering job %s stuck in submitting; resetting to queued", job["id"])
                self.db.update_job(job["id"], status=JobStatus.QUEUED.value, failure_message="")
                self.record_event(
                    "recovered", "job stuck in submitting reset to queued", entity_type="job", entity_id=job["id"]
                )
        for task in self.db.list_tasks_by_statuses([TaskStatus.ATTACHING.value], limit=5000):
            if not str(task.get("exit_code_path") or ""):
                LOGGER.warning("recovering task %s stuck in attaching without exit_code_path; requeueing", task["id"])
                self.db.update_task(
                    task["id"],
                    status=TaskStatus.QUEUED.value,
                    allocation_id=None,
                    failure_message="",
                )
                self.record_event(
                    "recovered",
                    "task stuck in attaching requeued",
                    entity_type="task",
                    entity_id=task["id"],
                )

    def reconcile_slurm_state(self) -> None:
        """Cancel scheduler-created 'pool' allocation jobs the DB no longer
        tracks (rows lost or DB reset while the Slurm job kept running).
        Only jobs named 'pool' are touched — that name is set exclusively by
        build_allocation_script."""
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        known_ids = {
            str(allocation.get("slurm_job_id"))
            for allocation in self.db.list_allocations(limit=1000)
            if allocation["state"] in live_states and allocation.get("slurm_job_id")
        }
        for account in self.accounts:
            try:
                with SSHSession(account, default_timeout=30) as ssh:
                    result = ssh.run('squeue -h -u "$USER" -o "%i|%j"')
                if result.exit_code != 0:
                    continue
                orphan_ids = []
                for line in result.stdout.splitlines():
                    job_id, sep, job_name = line.strip().partition("|")
                    if not sep or job_name.strip() != "pool":
                        continue
                    if job_id.strip() and job_id.strip() not in known_ids:
                        orphan_ids.append(job_id.strip())
                if not orphan_ids:
                    continue
                client = self._client(account)
                for job_id in orphan_ids:
                    try:
                        client.cancel(job_id)
                    except Exception as exc:
                        LOGGER.warning("failed to cancel orphan pool job %s on %s: %s", job_id, account.name, exc)
                        continue
                    LOGGER.warning("cancelled orphan pool job %s on %s (not tracked in DB)", job_id, account.name)
                    self.record_event(
                        "reconcile",
                        f"cancelled orphan pool job {job_id} not tracked in the DB",
                        entity_type="account",
                        entity_id=account.name,
                        account_name=account.name,
                    )
            except Exception as exc:
                LOGGER.warning("slurm reconcile skipped for %s: %s", account.name, exc)

    def _watchdog_stall_seconds(self) -> float:
        if self.watchdog_stall_seconds > 0:
            return float(self.watchdog_stall_seconds)
        return float(max(300, 3 * self.poll_interval_seconds))

    def _watchdog_loop(self) -> None:
        suspect_seq = -1
        while not self._stop.wait(self.poll_interval_seconds):
            suspect_seq = self._watchdog_check_once(suspect_seq)

    def _watchdog_check_once(self, suspect_seq: int) -> int:
        started = self._tick_started_at
        seq = self._tick_seq
        if started is None:
            return -1
        stalled_for = time.monotonic() - started
        if stalled_for < self._watchdog_stall_seconds():
            return -1
        if seq != suspect_seq:
            LOGGER.critical(
                "scheduler tick %s stalled for %.0fs; force-closing SSH transports", seq, stalled_for
            )
            self._dump_scheduler_stack()
            self._tick_sessions_force_close()
            self.record_event(
                "watchdog", f"tick {seq} stalled for {int(stalled_for)}s; SSH transports force-closed"
            )
            return seq
        LOGGER.critical(
            "scheduler tick %s still stalled after transport close (%.0fs); exiting for supervisor restart",
            seq,
            stalled_for,
        )
        self.record_event(
            "watchdog", f"tick {seq} still stalled after transport close; exiting for supervisor restart"
        )
        self._watchdog_exit(70)
        return seq

    def _dump_scheduler_stack(self) -> None:
        thread = self._thread
        if not thread or thread.ident is None:
            return
        frame = sys._current_frames().get(thread.ident)
        if frame is None:
            return
        stack = "".join(traceback.format_stack(frame))
        LOGGER.critical("scheduler thread stack:\n%s", stack)

    def _tick_sessions_force_close(self) -> None:
        with self._tick_caches_lock:
            caches = list(self._tick_caches)
        for cache in caches:
            cache.force_close_all()

    def _client(self, account: AccountConfig) -> Any:
        """Client for tick-path calls: reuses this thread's per-tick session
        cache when one is active, otherwise falls back to a fresh client (web
        threads and tests)."""
        cache = getattr(self._tick_local, "cache", None)
        if cache is not None:
            return cache.client(account)
        return self.client_factory(account)

    def _mark_account_failed_this_tick(self, account_name: str) -> None:
        cache = getattr(self._tick_local, "cache", None)
        if cache is not None:
            cache.mark_failed(account_name)
            self.record_event(
                "account_unavailable",
                "account skipped for the rest of this tick after an SSH failure",
                entity_type="account",
                entity_id=account_name,
                account_name=account_name,
            )

    @contextmanager
    def _tick_client_cache(self):
        cache = _TickClientCache(self.client_factory)
        with self._tick_caches_lock:
            self._tick_caches.add(cache)
        self._tick_local.cache = cache
        try:
            yield cache
        finally:
            self._tick_local.cache = None
            with self._tick_caches_lock:
                self._tick_caches.discard(cache)
            cache.close_all()

    def tick(self) -> None:
        self._tick_seq += 1
        self._tick_started_at = time.monotonic()
        self._tick_attach_workers_by_node.clear()
        stage_seconds: dict[str, float] = {}

        def run_stage(name: str, operation: Callable[[], Any]) -> Any:
            stage_started_at = time.monotonic()
            try:
                return operation()
            finally:
                stage_seconds[name] = time.monotonic() - stage_started_at

        try:
            with self._tick_client_cache():
                if self._needs_reconcile:
                    self._needs_reconcile = False
                    run_stage("reconcile_slurm_state", self.reconcile_slurm_state)
                run_stage("fail_stale_same_node_tasks", self.fail_stale_same_node_tasks)
                run_stage("enforce_cpu_partition_limits", self.enforce_cpu_partition_allocation_limits)
                run_stage("update_fea_overload_before", self.update_fea_overload_state)
                run_stage("assign_ready_same_node", self.assign_ready_same_node_tasks)
                run_stage("assign_ready_gpu", self.assign_ready_gpu_tasks)
                run_stage("assign_ready_standard", self.assign_ready_standard_tasks)
                run_stage("assign_ready_fea", lambda: self.assign_ready_fea_tasks(background=True))
                run_stage("refresh_cluster_state", self.refresh_cluster_state_if_due)
                run_stage("refresh_allocations", self.refresh_allocations)
                run_stage("refresh_tasks", self.refresh_tasks)
                run_stage("allocation_lifecycle", self.apply_allocation_lifecycle)
                run_stage("fea_memory_pressure", self.handle_fea_memory_pressure)
                run_stage("fea_cpu_cap", self.enforce_fea_node_cpu_cap)
                run_stage("update_fea_overload_after", self.update_fea_overload_state)
                run_stage("assign_queued_standard", lambda: self.assign_queued_tasks(include_fea=False))
                run_stage("maintain_allocation_pool", self.maintain_allocation_pool)
                run_stage("refresh_submitted_jobs", self.refresh_submitted_jobs)
                run_stage("submit_next_job", self.submit_next_queued_job)
                run_stage("cleanup", self.cleanup_remote_artifacts_if_due)
                run_stage("orphan_process_sweep", self.sweep_orphan_processes_if_due)
                run_stage("backup", self.backup_database_if_due)
                run_stage("license_refresh", self.refresh_license_usage_if_due)
        finally:
            started = self._tick_started_at
            if started is not None:
                self._last_tick_duration = time.monotonic() - started
            self._last_tick_stage_seconds = {
                name: round(seconds, 3)
                for name, seconds in stage_seconds.items()
            }
            self._last_tick_completed_monotonic = time.monotonic()
            self._last_tick_completed_at = self._now().isoformat()
            self._tick_started_at = None
            if self._last_tick_duration is not None and (
                self._last_tick_duration >= self.poll_interval_seconds
                or any(seconds >= 5.0 for seconds in stage_seconds.values())
            ):
                LOGGER.info(
                    "scheduler tick %s completed in %.3fs; stages=%s",
                    self._tick_seq,
                    self._last_tick_duration,
                    ", ".join(
                        f"{name}:{seconds:.3f}s"
                        for name, seconds in sorted(stage_seconds.items(), key=lambda item: item[1], reverse=True)
                        if seconds >= 0.05
                    ),
                )

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

    def _cleanup_cutoff_timestamp(self, ttl_seconds: int) -> str:
        cutoff = self._now() - timedelta(seconds=max(0, int(ttl_seconds)))
        return cutoff.strftime("%Y-%m-%d %H:%M:%S")

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
        self.prune_database_rows()
        self.trim_finished_task_logs()
        self.sweep_orphan_remote_artifacts_if_due()
        self.prune_workspace_artifacts_if_due()
        self.prune_project_sim_artifacts()

    def trim_finished_task_logs(self) -> None:
        """Terminal tasks keep full stdout/stderr (used for CSV harvesting)
        only briefly; after the trim window the logs are truncated to a tail
        so the TTL window does not accumulate multi-MB logs per task."""
        max_bytes = self.cleanup_finished_task_log_max_bytes
        if max_bytes <= 0:
            return
        cutoff = self._cleanup_cutoff_timestamp(self.cleanup_finished_task_log_trim_after_seconds)
        terminal = [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value]
        by_account: dict[str, tuple[AccountConfig, list[str]]] = {}
        for task in self.db.list_finished_tasks_for_cleanup(terminal, cutoff, limit=500):
            account = self.account_by_name(str(task.get("account_name") or ""))
            remote_dir = str(task.get("remote_dir") or "")
            if not account or not self.is_safe_scheduler_artifact_path(account, remote_dir, ("task-",)):
                continue
            paths = [str(path) for path in (task.get("stdout_path"), task.get("stderr_path")) if path]
            if paths:
                by_account.setdefault(account.name, (account, []))[1].extend(paths)
        for account, paths in by_account.values():
            pieces = [
                f'f={shlex.quote(path)}; if [ -f "$f" ] && [ "$(wc -c < "$f")" -gt {max_bytes} ]; '
                f'then tail -c {max_bytes} "$f" > "$f.trim" && mv "$f.trim" "$f"; fi'
                for path in paths
            ]
            try:
                with SSHSession(account, default_timeout=300) as ssh:
                    for index in range(0, len(pieces), 100):
                        ssh.run("; ".join(pieces[index : index + 100]))
            except Exception as exc:
                LOGGER.warning("failed to trim finished task logs on %s: %s", account.name, exc)

    @staticmethod
    def _workspace_prune_glob_ok(glob_pattern: str) -> bool:
        pattern = (glob_pattern or "").strip()
        if not pattern or "/" in pattern or "\\" in pattern or ".." in pattern:
            return False
        # Require real characters beyond wildcards so a bare '*' cannot slip in.
        return bool(set(pattern) - {"*", "?", ".", "["})

    def prune_workspace_artifacts_if_due(self) -> None:
        """Delete user-declared disposable artifacts (e.g. FEA solution dirs
        like *.aedtresults) anywhere in the workspace. Only explicitly
        configured name globs are touched, and anything containing a file
        modified within the min-age window is skipped so running simulations
        are never disturbed."""
        globs = [g.strip() for g in self.cleanup_workspace_prune_globs if self._workspace_prune_glob_ok(g)]
        if not globs:
            return
        now = time.time()
        if now - self._last_workspace_prune_at < self.cleanup_workspace_prune_interval_seconds:
            return
        self._last_workspace_prune_at = now
        minutes = max(60, int(self.cleanup_workspace_prune_min_age_seconds // 60))
        # Deleting tens of GB can take minutes; keep it off the tick thread so
        # the watchdog never mistakes it for a stalled tick.
        threading.Thread(
            target=self._prune_workspace_artifacts,
            args=(globs, minutes),
            name="workspace-prune",
            daemon=True,
        ).start()

    def _prune_workspace_artifacts(self, globs: list[str], minutes: int) -> None:
        name_expr = " -o ".join(f"-name {shlex.quote(g)}" for g in globs)
        for account in self.accounts:
            workspace = str(account.remote_workspace or "").strip()
            if not workspace:
                continue
            list_command = (
                f"find {shlex.quote(workspace)} -mindepth 1 \\( {name_expr} \\) -prune -print 2>/dev/null | "
                "while IFS= read -r d; do "
                f"if [ -z \"$(find \"$d\" -mmin -{minutes} -print -quit 2>/dev/null)\" ]; "
                "then printf '%s\\n' \"$d\"; fi; done"
            )
            try:
                with SSHSession(account, default_timeout=600) as ssh:
                    result = ssh.run(list_command)
                    if result.exit_code != 0:
                        continue
                    candidates = []
                    workspace_prefix = self._normalize_remote_path(workspace).rstrip("/") + "/"
                    for line in result.stdout.splitlines():
                        path = line.strip()
                        if not path:
                            continue
                        normalized = self._normalize_remote_path(path)
                        if not normalized.startswith(workspace_prefix):
                            continue
                        basename = posixpath.basename(normalized.rstrip("/"))
                        if not any(fnmatch.fnmatch(basename, g) for g in globs):
                            continue
                        candidates.append(path)
                    if not candidates:
                        continue
                    for index in range(0, len(candidates), 20):
                        chunk = candidates[index : index + 20]
                        ssh.run("rm -rf -- " + " ".join(shlex.quote(path) for path in chunk), timeout=600)
            except Exception as exc:
                LOGGER.warning("workspace prune failed on %s: %s", account.name, exc)
                continue
            LOGGER.info("workspace prune removed %d artifacts on %s", len(candidates), account.name)
            self.record_event(
                "workspace_prune",
                f"removed {len(candidates)} disposable artifacts matching {', '.join(globs)}",
                entity_type="account",
                entity_id=account.name,
                account_name=account.name,
            )

    def prune_project_sim_artifacts(self) -> None:
        """For each deployed project, sweep its
        <workspace>/projects/<name>/<sim_subdir> for the project's own cleanup
        globs. mtime-guarded so files a running simulation still touches are
        left alone. Scoped strictly inside the project's sim dir."""
        minutes = max(1, int(self.cleanup_workspace_prune_min_age_seconds // 60))
        for project in self.db.list_projects():
            globs = [
                g.strip()
                for g in str(project.get("cleanup_globs") or "").split(",")
                if self._workspace_prune_glob_ok(g.strip())
            ]
            if not globs:
                continue
            sim_subdir = str(project.get("sim_subdir") or "simulation").strip().strip("/")
            if not sim_subdir:
                continue
            name = str(project.get("name") or "").strip()
            if not name:
                continue
            name_expr = " -o ".join(f"-name {shlex.quote(g)}" for g in globs)
            deployments = [
                dep for dep in self.db.list_project_deployments(int(project["id"]))
                if dep.get("status") == "deployed"
            ]
            for dep in deployments:
                account = self.account_by_name(str(dep.get("account_name") or ""))
                if not account:
                    continue
                workspace = str(account.remote_workspace or "").strip()
                if not workspace:
                    continue
                projects_root = posixpath.join(workspace, "projects")
                sim_dir = posixpath.join(projects_root, name, sim_subdir)
                # Containment: the sweep dir must live under <workspace>/projects/.
                projects_prefix = self._normalize_remote_path(projects_root).rstrip("/") + "/"
                if not self._normalize_remote_path(sim_dir).startswith(projects_prefix):
                    continue
                list_command = (
                    f"test -d {shlex.quote(sim_dir)} || exit 0; "
                    f"find {shlex.quote(sim_dir)} -mindepth 1 \\( {name_expr} \\) -prune -print 2>/dev/null | "
                    "while IFS= read -r d; do "
                    f"if [ -z \"$(find \"$d\" -mmin -{minutes} -print -quit 2>/dev/null)\" ]; "
                    "then printf '%s\\n' \"$d\"; fi; done"
                )
                try:
                    with SSHSession(account, default_timeout=600) as ssh:
                        result = ssh.run(list_command)
                        if result.exit_code != 0:
                            continue
                        sim_prefix = self._normalize_remote_path(sim_dir).rstrip("/") + "/"
                        candidates = []
                        for line in result.stdout.splitlines():
                            path = line.strip()
                            if not path:
                                continue
                            normalized = self._normalize_remote_path(path)
                            if not normalized.startswith(sim_prefix):
                                continue
                            basename = posixpath.basename(normalized.rstrip("/"))
                            if not any(fnmatch.fnmatch(basename, g) for g in globs):
                                continue
                            candidates.append(path)
                        if not candidates:
                            continue
                        for index in range(0, len(candidates), 20):
                            chunk = candidates[index : index + 20]
                            ssh.run("rm -rf -- " + " ".join(shlex.quote(path) for path in chunk), timeout=600)
                except Exception as exc:
                    LOGGER.warning("project sim prune failed on %s/%s: %s", account.name, name, exc)
                    continue
                LOGGER.info("project sim prune removed %d artifacts in %s on %s", len(candidates), name, account.name)

    def license_usage(self) -> dict:
        return dict(self._license_usage)

    def refresh_license_usage_if_due(self) -> None:
        if not self.license_monitor_enabled:
            return
        if not (self.license_monitor_lmutil_path and self.license_monitor_license_server):
            return
        now = time.time()
        if now - self._last_license_refresh_at < self.license_monitor_interval_seconds:
            return
        if self._license_refresh_inflight:
            return
        self._last_license_refresh_at = now
        account = self.account_by_name(self.license_monitor_account) or (
            self.accounts[0] if self.accounts else None
        )
        if not account:
            return
        self._license_refresh_inflight = True
        threading.Thread(
            target=self._refresh_license_usage,
            args=(account,),
            name="license-monitor",
            daemon=True,
        ).start()

    def _refresh_license_usage(self, account: AccountConfig) -> None:
        try:
            command = (
                f"{shlex.quote(self.license_monitor_lmutil_path)} lmstat "
                f"-c {shlex.quote(self.license_monitor_license_server)} -a 2>&1"
            )
            with SSHSession(account, default_timeout=90) as ssh:
                result = ssh.run(command)
                features = parse_lmstat_features(result.stdout)
                # The vendor daemon intermittently answers without the feature
                # block while license checkouts churn; retry before trusting an
                # empty snapshot, and fall back to the last good one.
                for _attempt in range(2):
                    if features:
                        break
                    time.sleep(5)
                    result = ssh.run(command)
                    features = parse_lmstat_features(result.stdout)
            if not features and self._license_usage.get("features"):
                previous = dict(self._license_usage)
                previous["error"] = "lmstat returned no feature block; showing the last good snapshot"
                self._license_usage = previous
                return
            server_up = "license server UP" in result.stdout
            error = ""
            if not features and not server_up:
                error = result.stdout.strip().splitlines()[-1][:200] if result.stdout.strip() else "no lmstat output"
            by_name = {item["feature"]: item for item in features}
            display = [
                {
                    "label": label,
                    "feature": feature,
                    "used": int(by_name.get(feature, {}).get("used") or 0),
                    "total": int(by_name.get(feature, {}).get("total") or 0),
                }
                for label, feature in self.license_monitor_display.items()
            ]
            self._license_usage = {
                "checked_at": self._now().isoformat(),
                "server": self.license_monitor_license_server,
                "server_up": server_up,
                "features": features,
                "in_use": [item for item in features if item["used"] > 0],
                "display": display,
                "error": error,
            }
        except Exception as exc:
            self._license_usage = {
                "checked_at": self._now().isoformat(),
                "server": self.license_monitor_license_server,
                "server_up": False,
                "features": [],
                "in_use": [],
                "error": str(exc)[:200],
            }
            LOGGER.warning("license monitor refresh failed: %s", exc)
        finally:
            self._license_refresh_inflight = False

    def backup_database_if_due(self) -> None:
        if not self.backup_enabled:
            return
        now = time.time()
        if self._last_backup_at == 0.0:
            # Survive restarts: resume the cadence from the newest backup file.
            try:
                mtimes = [
                    os.path.getmtime(os.path.join(self.backup_dir, entry))
                    for entry in os.listdir(self.backup_dir)
                    if entry.startswith("slurm_scheduler-") and entry.endswith(".db")
                ]
                if mtimes:
                    self._last_backup_at = max(mtimes)
            except OSError:
                pass
        if now - self._last_backup_at < self.backup_interval_seconds:
            return
        self._last_backup_at = now
        stamp = self._now().strftime("%Y%m%d-%H%M%S")
        backup_path = os.path.join(self.backup_dir, f"slurm_scheduler-{stamp}.db")
        try:
            self.db.backup_to(backup_path)
        except Exception as exc:
            LOGGER.warning("database backup failed: %s", exc)
            return
        LOGGER.info("database backed up to %s", backup_path)
        try:
            backups = sorted(
                entry
                for entry in os.listdir(self.backup_dir)
                if entry.startswith("slurm_scheduler-") and entry.endswith(".db")
            )
            for stale in backups[: -self.backup_keep]:
                os.unlink(os.path.join(self.backup_dir, stale))
        except OSError as exc:
            LOGGER.warning("failed to rotate database backups: %s", exc)

    def prune_database_rows(self) -> None:
        try:
            deleted = self.db.prune_old_rows(
                self._cleanup_cutoff_timestamp(self.cleanup_db_row_ttl_seconds),
                self._cleanup_cutoff_timestamp(self.cleanup_event_ttl_seconds),
            )
        except Exception as exc:
            LOGGER.warning("failed to prune old database rows: %s", exc)
            return
        if any(deleted.values()):
            LOGGER.info("pruned old database rows: %s", deleted)

    def sweep_orphan_remote_artifacts_if_due(self) -> None:
        """Remove workspace directories the DB no longer references (rows
        deleted, DB reset, or tasks wedged in a non-terminal state). The
        TTL cleanups above only see directories still recorded in the DB."""
        if not self.cleanup_orphan_sweep_enabled:
            return
        now = time.time()
        if now - self._last_orphan_sweep_at < self.cleanup_orphan_sweep_interval_seconds:
            return
        self._last_orphan_sweep_at = now
        try:
            referenced: set[str] = set()
            for path in self.db.list_referenced_remote_paths():
                normalized = self._normalize_remote_path(path)
                referenced.add(normalized)
                # env-sync targets live one level below the swept job dir.
                referenced.add(posixpath.dirname(normalized))
        except Exception as exc:
            LOGGER.warning("orphan sweep skipped; failed to list referenced paths: %s", exc)
            return
        age_minutes = max(60, int(self.cleanup_orphan_min_age_seconds // 60))
        prefixes = ("task-", "job-", "allocation-")
        for account in self.accounts:
            workspace = str(account.remote_workspace or "").strip()
            if not workspace:
                continue
            env_sync_dir = posixpath.join(workspace, "env-sync")
            runs_dir = posixpath.join(workspace, "runs")
            artifact_names = "\\( -name 'task-*' -o -name 'job-*' -o -name 'allocation-*' \\)"
            command = (
                # Current layout: run artifacts live under runs/<date>/ (depth 2).
                f"find {shlex.quote(runs_dir)} -mindepth 2 -maxdepth 2 -type d {artifact_names} -mmin +{age_minutes} 2>/dev/null; "
                # Legacy layout: artifacts written directly under the workspace.
                f"find {shlex.quote(workspace)} -mindepth 1 -maxdepth 1 -type d {artifact_names} -mmin +{age_minutes} 2>/dev/null; "
                f"find {shlex.quote(env_sync_dir)} -mindepth 1 -maxdepth 1 -type d -name 'job-*' -mmin +{age_minutes} 2>/dev/null; "
                # Reap now-empty dated run folders left behind by earlier sweeps.
                f"find {shlex.quote(runs_dir)} -mindepth 1 -maxdepth 1 -type d -empty -exec rmdir {{}} + 2>/dev/null; "
                "true"
            )
            try:
                with SSHSession(account, default_timeout=120) as ssh:
                    result = ssh.run(command)
            except Exception as exc:
                LOGGER.warning("orphan sweep failed to list %s workspace: %s", account.name, exc)
                continue
            candidates = []
            for line in result.stdout.splitlines():
                path = line.strip()
                if not path:
                    continue
                if self._normalize_remote_path(path) in referenced:
                    continue
                if not self.is_safe_scheduler_artifact_path(account, path, prefixes):
                    continue
                candidates.append(path)
            if not candidates:
                continue
            try:
                self._client(account).remove_trees(candidates)
            except Exception as exc:
                LOGGER.warning("orphan sweep failed to remove %d dirs on %s: %s", len(candidates), account.name, exc)
                continue
            LOGGER.info("orphan sweep removed %d stale dirs on %s", len(candidates), account.name)
            self.record_event(
                "orphan_sweep",
                f"removed {len(candidates)} stale workspace dirs older than {age_minutes // 60}h",
                entity_type="account",
                entity_id=account.name,
                account_name=account.name,
            )

    def cleanup_finished_tasks(self) -> None:
        terminal = [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value]
        cutoff = self._cleanup_cutoff_timestamp(self.cleanup_finished_task_ttl_seconds)
        candidates_by_account: dict[str, tuple[AccountConfig, list[tuple[dict, str]]]] = {}
        for task in self.db.list_finished_tasks_for_cleanup(terminal, cutoff):
            account = self.account_by_name(str(task.get("account_name") or ""))
            remote_dir = str(task.get("remote_dir") or "")
            if not account or not self.is_safe_scheduler_artifact_path(account, remote_dir, ("task-",)):
                continue
            candidates_by_account.setdefault(account.name, (account, []))[1].append((task, remote_dir))
        for account, items in candidates_by_account.values():
            removed = self.remove_scheduler_artifacts(account, [path for _task, path in items], ("task-",))
            for task, remote_dir in items:
                if remote_dir not in removed:
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
        terminal = [JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value]
        cutoff = self._cleanup_cutoff_timestamp(self.cleanup_finished_job_ttl_seconds)
        candidates_by_account: dict[str, tuple[AccountConfig, list[tuple[dict, str]]]] = {}
        for job in self.db.list_finished_jobs_for_cleanup(terminal, cutoff):
            account = self.account_by_name(str(job.get("account_name") or ""))
            remote_dir = str(job.get("remote_job_dir") or "")
            if not account or not self.is_safe_scheduler_artifact_path(account, remote_dir, ("job-",)):
                continue
            candidates_by_account.setdefault(account.name, (account, []))[1].append((job, remote_dir))
        for account, items in candidates_by_account.values():
            removed = self.remove_scheduler_artifacts(account, [path for _job, path in items], ("job-",))
            for job, remote_dir in items:
                if remote_dir in removed:
                    self.db.update_job(job["id"], remote_job_dir="", stdout_path="", stderr_path="")

    def cleanup_closed_allocations(self) -> None:
        terminal = [AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value]
        cutoff = self._cleanup_cutoff_timestamp(self.cleanup_closed_allocation_ttl_seconds)
        candidates_by_account: dict[str, tuple[AccountConfig, list[tuple[dict, str]]]] = {}
        for allocation in self.db.list_closed_allocations_for_cleanup(terminal, cutoff):
            account = self.account_by_name(str(allocation.get("account_name") or ""))
            remote_dir = str(allocation.get("remote_dir") or "")
            if not account or not self.is_safe_scheduler_artifact_path(account, remote_dir, ("allocation-",)):
                continue
            candidates_by_account.setdefault(account.name, (account, []))[1].append((allocation, remote_dir))
        for account, items in candidates_by_account.values():
            removed = self.remove_scheduler_artifacts(account, [path for _allocation, path in items], ("allocation-",))
            for allocation, remote_dir in items:
                if remote_dir in removed:
                    self.db.update_allocation(allocation["id"], remote_dir="", stdout_path="", stderr_path="")

    def remove_scheduler_artifact(self, account: AccountConfig | None, remote_path: str, prefixes: tuple[str, ...]) -> bool:
        if not account or not self.is_safe_scheduler_artifact_path(account, remote_path, prefixes):
            return False
        try:
            self._client(account).remove_tree(remote_path)
        except Exception as exc:
            LOGGER.warning("failed to clean remote artifact %s on %s: %s", remote_path, account.name, exc)
            return False
        return True

    def remove_scheduler_artifacts(self, account: AccountConfig, remote_paths: list[str], prefixes: tuple[str, ...]) -> set[str]:
        safe_paths = [
            remote_path
            for remote_path in remote_paths
            if self.is_safe_scheduler_artifact_path(account, remote_path, prefixes)
        ]
        if not safe_paths:
            return set()
        try:
            client = self._client(account)
            remove_trees = getattr(client, "remove_trees", None)
            if callable(remove_trees):
                remove_trees(safe_paths)
            else:
                for remote_path in safe_paths:
                    client.remove_tree(remote_path)
        except Exception as exc:
            LOGGER.warning("failed to clean %d remote artifacts on %s: %s", len(safe_paths), account.name, exc)
            return set()
        return set(safe_paths)

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
        candidates = sorted(self.accounts, key=lambda item: item.name not in preferred_names)
        for account in candidates[:3]:
            try:
                with SSHSession(account, default_timeout=60) as ssh:
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
                        self._pestat_nodes_cache = None
                return
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
        overlays = self.db.list_account_env_overlays(account.name)
        overlay_capabilities = {str(item.get("capability") or "") for item in overlays}
        overlay_profiles = {str(item.get("env_profile") or "") for item in overlays}
        if capability and capability not in (account.capabilities or []) and capability not in overlay_capabilities:
            return False
        if profile and profile not in (account.env_profiles or {}) and profile not in overlay_profiles:
            return False
        return True

    def apply_dynamic_env_profile(self, payload: dict, account: AccountConfig) -> dict:
        profile = str(payload.get("env_profile") or "").strip()
        if not profile or profile in (account.env_profiles or {}):
            return payload
        overlay = self.db.get_account_env_overlay(account.name, profile)
        if not overlay:
            return payload
        setup = str(overlay.get("env_setup") or "").strip()
        if not setup:
            return payload
        existing = str(payload.get("env_setup") or "").strip()
        return {**payload, "env_setup": setup if not existing else f"{setup}\n{existing}"}

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
        for allocation in self.db.list_allocations_with_live(limit=0, live_limit=10000):
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
        for allocation in self.db.list_allocations_with_live(limit=0, live_limit=10000):
            if allocation["state"] not in live_states:
                continue
            if allocation.get("partition") != partition:
                continue
            if resource_pool and (allocation.get("resource_pool") or "cpu") != resource_pool:
                continue
            return True
        return False

    def live_allocation_count_for_partition(self, partition: str, resource_pool: str = "") -> int:
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        count = 0
        for allocation in self.db.list_allocations_with_live(limit=0, live_limit=10000):
            if allocation["state"] not in live_states:
                continue
            if allocation.get("partition") != partition:
                continue
            if resource_pool and (allocation.get("resource_pool") or "cpu") != resource_pool:
                continue
            count += 1
        return count

    def live_allocation_count_for_partition_node(self, partition: str, node_name: str, resource_pool: str = "") -> int:
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        node = str(node_name or "")
        if not node:
            return 0
        count = 0
        for allocation in self.db.list_allocations_with_live(limit=0, live_limit=10000):
            if allocation["state"] not in live_states:
                continue
            if allocation.get("partition") != partition:
                continue
            if str(allocation.get("node_name") or "") != node:
                continue
            if resource_pool and (allocation.get("resource_pool") or "cpu") != resource_pool:
                continue
            count += 1
        return count

    def cpu_allocation_node_limit(self, partition: str) -> int:
        limit = int(self.cpu_partition_allocation_limits.get(str(partition or ""), 0) or 0)
        if limit > 0:
            return limit
        return 1 if self.is_single_job_partition(partition) else 0

    def cpu_partition_allocation_limit_reached(self, partition: str, node_name: str = "") -> bool:
        limit = self.cpu_allocation_node_limit(partition)
        if limit <= 0:
            return False
        return self.live_allocation_count_for_partition_node(partition, node_name, resource_pool="cpu") >= limit

    def cpu_partition_allocation_partition_saturated(self, partition: str, nodes: list[PestatNode] | None = None) -> bool:
        limit = self.cpu_allocation_node_limit(partition)
        if limit <= 0:
            return False
        candidate_nodes: set[str] = set()
        if nodes is not None:
            candidate_nodes.update(
                node.hostname
                for node in nodes
                if node.partition == partition and node.state in {"idle", "mix"} and node.effective_free_cpus > 0
            )
        if not candidate_nodes:
            candidate_nodes.update(
                str(row.get("node_name") or "")
                for row in self.db.list_node_inventory()
                if row.get("partition") == partition
                and str(row.get("state") or "").lower() in {"idle", "mix", "mixed"}
                and int(row.get("cpus") or 0) > 0
            )
            candidate_nodes.discard("")
        return bool(candidate_nodes) and all(
            self.cpu_partition_allocation_limit_reached(partition, node_name)
            for node_name in candidate_nodes
        )

    def _job_states(self, client, slurm_job_ids: list[str]) -> dict[str, JobStateInfo]:
        """Batched job-state lookup with a per-id fallback for clients (fakes)
        that only implement the singular methods."""
        batched = getattr(client, "job_states", None)
        if callable(batched):
            return batched(slurm_job_ids)
        out: dict[str, JobStateInfo] = {}
        for slurm_job_id in slurm_job_ids:
            status = client.state(slurm_job_id)
            reason = client.pending_reason(slurm_job_id) if status == JobStatus.SUBMITTED else ""
            node_name = client.allocation_node_name(slurm_job_id) if status == JobStatus.RUNNING else ""
            out[slurm_job_id] = JobStateInfo(status=status, pending_reason=reason, node_name=node_name)
        return out

    def _task_probes(self, client, tasks: list[dict]) -> dict[int, TaskProbe]:
        batched = getattr(client, "task_probes", None)
        if callable(batched):
            return batched(tasks)
        out: dict[int, TaskProbe] = {}
        for task in tasks:
            status = client.task_state(task)
            exit_code = client.task_exit_code(task) if status in {JobStatus.COMPLETED, JobStatus.FAILED} else None
            out[int(task["id"])] = TaskProbe(status=status, exit_code=exit_code)
        return out

    def refresh_allocations(self) -> None:
        accounts_by_name = {account.name: account for account in self.accounts}
        active_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        by_account: dict[str, list[dict]] = {}
        for allocation in self.db.list_allocations_with_live(limit=500, live_limit=10000):
            if allocation["state"] not in active_states or not allocation.get("slurm_job_id"):
                continue
            if allocation["account_name"] not in accounts_by_name:
                continue
            by_account.setdefault(allocation["account_name"], []).append(allocation)
        outcomes = self._fan_out_by_account(
            by_account,
            lambda account_name, allocations: self._job_states(
                self._client(accounts_by_name[account_name]),
                [str(item["slurm_job_id"]) for item in allocations],
            ),
        )
        for account_name, outcome in outcomes.items():
            if isinstance(outcome, Exception):
                LOGGER.warning(
                    "failed to refresh %d allocations on %s: %s",
                    len(by_account[account_name]),
                    account_name,
                    outcome,
                )
                self._mark_account_failed_this_tick(account_name)
                continue
            for allocation in by_account[account_name]:
                info = outcome.get(str(allocation["slurm_job_id"]))
                if info is None:
                    continue
                self._apply_allocation_state(allocation, info)
        self.recalculate_allocation_capacity()

    def _apply_allocation_state(self, allocation: dict, info: JobStateInfo) -> None:
        status = info.status
        if status == JobStatus.RUNNING and allocation["state"] == AllocationStatus.PENDING.value:
            updates = {}
            # Spread submissions store the partition candidate list; replace it
            # with the partition Slurm actually granted once the job starts.
            if "," in str(allocation.get("partition") or "") and info.partition and "," not in info.partition:
                updates["partition"] = info.partition
            self.db.update_allocation(
                allocation["id"],
                state=AllocationStatus.WARM.value,
                started_at="CURRENT_TIMESTAMP",
                pending_reason="",
                node_name=allocation.get("node_name") or info.node_name or "",
                **updates,
            )
            self.record_event(
                "allocation_warm",
                f"allocation started on {allocation.get('node_name') or info.node_name or 'unknown node'}",
                entity_type="allocation",
                entity_id=allocation["id"],
                account_name=str(allocation.get("account_name") or ""),
            )
        elif status == JobStatus.RUNNING and not allocation.get("node_name"):
            if info.node_name:
                self.db.update_allocation(allocation["id"], node_name=info.node_name)
        elif status == JobStatus.SUBMITTED and allocation["state"] == AllocationStatus.PENDING.value:
            reason = info.pending_reason
            if reason and reason != (allocation.get("pending_reason") or ""):
                self.db.update_allocation(allocation["id"], pending_reason=reason)
        elif status in {JobStatus.COMPLETED, JobStatus.CANCELLED}:
            self.db.update_allocation(allocation["id"], state=AllocationStatus.CLOSED.value, closed_at="CURRENT_TIMESTAMP")
            self.record_event(
                "allocation_closed",
                f"slurm job {allocation.get('slurm_job_id')} left the queue ({status.value})",
                entity_type="allocation",
                entity_id=allocation["id"],
                account_name=str(allocation.get("account_name") or ""),
            )
        elif status == JobStatus.FAILED:
            self.db.update_allocation(allocation["id"], state=AllocationStatus.FAILED.value, closed_at="CURRENT_TIMESTAMP")
            self.record_event(
                "allocation_failed",
                f"slurm job {allocation.get('slurm_job_id')} failed",
                entity_type="allocation",
                entity_id=allocation["id"],
                account_name=str(allocation.get("account_name") or ""),
            )

    def refresh_tasks(self, max_tasks: int | None = None) -> None:
        accounts_by_name = {account.name: account for account in self.accounts}
        candidates = [
            task
            for task in self.db.list_tasks(limit=5000)
            if task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        ]
        by_account: dict[str, list[dict]] = {}
        for task in self.tasks_to_refresh(candidates, max_tasks=max_tasks):
            if self.task_timed_out(task):
                self.cancel_timed_out_task(task)
                continue
            if task["status"] == TaskStatus.ATTACHING.value and not task.get("exit_code_path"):
                continue
            if not task.get("account_name") or task["account_name"] not in accounts_by_name:
                continue
            by_account.setdefault(task["account_name"], []).append(task)
        outcomes = self._fan_out_by_account(
            by_account,
            lambda account_name, tasks: self._task_probes(
                self._client(accounts_by_name[account_name]), tasks
            ),
        )
        for account_name, outcome in outcomes.items():
            if isinstance(outcome, Exception):
                LOGGER.warning(
                    "failed to refresh %d tasks on %s: %s",
                    len(by_account[account_name]),
                    account_name,
                    outcome,
                )
                self._mark_account_failed_this_tick(account_name)
                continue
            client = self._client(accounts_by_name[account_name])
            for task in by_account[account_name]:
                probe = outcome.get(int(task["id"]))
                if probe is None:
                    continue
                self._apply_task_probe(task, probe, client)
        self.recalculate_allocation_capacity()

    def _apply_task_probe(self, task: dict, probe: TaskProbe, client) -> None:
        status = probe.status
        if status == JobStatus.RUNNING:
            if task["status"] != TaskStatus.RUNNING.value:
                self.db.update_task(task["id"], status=TaskStatus.RUNNING.value, started_at="CURRENT_TIMESTAMP")
            return
        if status == JobStatus.COMPLETED:
            self.db.update_task(task["id"], status=TaskStatus.COMPLETED.value, exit_code=probe.exit_code, finished_at="CURRENT_TIMESTAMP")
            self.record_event(
                "task_completed",
                f"task {task.get('name') or task['id']} completed",
                entity_type="task",
                entity_id=task["id"],
                account_name=str(task.get("account_name") or ""),
            )
            self.on_task_terminal(task, "completed")
            self.close_allocation_after_exclusive_task(task)
        elif status == JobStatus.CANCELLED:
            self.db.update_task(task["id"], status=TaskStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
            self.on_task_terminal(task, "cancelled")
            self.close_allocation_after_exclusive_task(task)
        elif status == JobStatus.FAILED:
            try:
                failure_message = task.get("failure_message") or self.task_stderr_failure_message(task, client)
            except Exception:
                failure_message = task.get("failure_message") or ""
            self.db.update_task(
                task["id"],
                status=TaskStatus.FAILED.value,
                exit_code=probe.exit_code,
                failure_message=failure_message,
                finished_at="CURRENT_TIMESTAMP",
            )
            self.record_event(
                "task_failed",
                f"task {task.get('name') or task['id']} failed (exit {probe.exit_code}): {failure_message[:200]}",
                entity_type="task",
                entity_id=task["id"],
                account_name=str(task.get("account_name") or ""),
            )
            self.on_task_terminal(task, "failed")
            self.close_allocation_after_exclusive_task(task)

    def tasks_to_refresh(self, tasks: list[dict], max_tasks: int | None = None) -> list[dict]:
        limit = self.task_refresh_max_per_tick if max_tasks is None else int(max_tasks)
        if limit <= 0:
            return sorted(tasks, key=lambda item: int(item.get("id") or 0))
        non_fea = sorted(
            [task for task in tasks if not self.task_is_fea_bursty(task)],
            key=lambda item: int(item.get("id") or 0),
        )
        selected = non_fea[:limit]
        remaining = limit - len(selected)
        if remaining <= 0:
            return selected
        fea = sorted(
            [task for task in tasks if self.task_is_fea_bursty(task)],
            key=lambda item: int(item.get("id") or 0),
        )
        if not fea:
            return selected
        after_cursor = [task for task in fea if int(task.get("id") or 0) > self._fea_task_refresh_cursor_id]
        before_cursor = [task for task in fea if int(task.get("id") or 0) <= self._fea_task_refresh_cursor_id]
        fea_selected = (after_cursor + before_cursor)[:remaining]
        if fea_selected:
            self._fea_task_refresh_cursor_id = int(fea_selected[-1].get("id") or 0)
        return selected + fea_selected

    def task_stderr_failure_message(self, task: dict, client: SlurmAccountClient) -> str:
        stderr_path = task.get("stderr_path") or ""
        if not stderr_path:
            return ""
        try:
            text = client.read_text_file(stderr_path)
        except Exception:
            return ""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines[:3])

    def task_timed_out(self, task: dict) -> bool:
        timeout = int(task.get("timeout_seconds") or 0)
        if timeout <= 0:
            return False
        started = self._timestamp(task.get("started_at") or task.get("attached_at") or task.get("created_at"))
        if not started:
            return False
        return (self._now() - started).total_seconds() >= timeout

    def cancel_timed_out_task(self, task: dict) -> None:
        account = self.account_by_name(str(task.get("account_name") or ""))
        if account:
            try:
                self._client(account).cancel_task(task, self._task_allocation_job_id(task))
            except Exception as exc:
                LOGGER.warning("failed to cancel timed out task %s remotely: %s", task["id"], exc)
        self.db.update_task(
            task["id"],
            status=TaskStatus.FAILED.value,
            failure_message=f"task timed out after {int(task.get('timeout_seconds') or 0)}s",
            exit_code=124,
            finished_at="CURRENT_TIMESTAMP",
        )
        self.on_task_terminal(task, "timed out")
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
        running_by_allocation: dict[int, dict[str, int]] = {}
        for task in tasks:
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not task.get("allocation_id"):
                continue
            stats = running_by_allocation.setdefault(
                int(task["allocation_id"]),
                {"active_tasks": 0, "reserved_cpus": 0, "reserved_mem": 0, "reserved_gpus": 0},
            )
            stats["active_tasks"] += 1
            if self.task_scheduling_profile(task) == SchedulingProfile.FEA_BURSTY.value:
                stats["reserved_gpus"] += int(task.get("gpus") or 0)
                continue
            stats["reserved_cpus"] += int(task.get("cpus") or 0)
            stats["reserved_mem"] += int(task.get("memory_mb") or 0)
            stats["reserved_gpus"] += int(task.get("gpus") or 0)
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            stats = running_by_allocation.get(
                allocation["id"],
                {"active_tasks": 0, "reserved_cpus": 0, "reserved_mem": 0, "reserved_gpus": 0},
            )
            free_cpus = max(0, int(allocation["total_cpus"]) - stats["reserved_cpus"])
            free_mem = max(0, int(allocation["total_memory_mb"]) - stats["reserved_mem"])
            free_gpus = max(0, int(allocation.get("total_gpus") or 0) - stats["reserved_gpus"])
            state = allocation["state"]
            if state != AllocationStatus.DRAINING.value:
                state = AllocationStatus.ACTIVE.value if stats["active_tasks"] else AllocationStatus.WARM.value
            self.db.update_allocation(
                allocation["id"],
                state=state,
                free_cpus=free_cpus,
                free_memory_mb=free_mem,
                free_gpus=free_gpus,
                last_active_at="CURRENT_TIMESTAMP" if stats["active_tasks"] else allocation.get("last_active_at"),
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
        reason = allocation.get("pending_reason") or "unknown Slurm pending reason"
        if self.retry_pinned_gpu_warm_allocation(allocation, age, reason):
            return
        if age < self.allocation_pending_timeout_seconds:
            return
        pool = allocation.get("resource_pool") or "cpu"
        if self.pending_allocation_timeout_exempt(allocation, reason):
            return
        self._allocation_backoff_until_by_pool[pool] = time.monotonic() + max(0, self.allocation_pending_backoff_seconds)
        self.close_allocation(allocation, f"pending timeout after {int(age)}s: {reason}")

    def retry_pinned_gpu_warm_allocation(self, allocation: dict, age: float, reason: str) -> bool:
        if self.gpu_prewarm_pinned_pending_timeout_seconds <= 0:
            return False
        if age < self.gpu_prewarm_pinned_pending_timeout_seconds:
            return False
        if not self.protected_gpu_warm_pool(allocation):
            return False
        node_name = str(allocation.get("node_name") or "").strip()
        if not node_name:
            return False
        pool = allocation.get("resource_pool") or "cpu"
        self._allocation_node_backoff_until[(str(pool), node_name)] = time.monotonic() + max(
            self.gpu_prewarm_pinned_pending_timeout_seconds,
            min(max(0, self.allocation_pending_backoff_seconds), 1800),
        )
        self.close_allocation(allocation, f"pinned warm pool retry after {int(age)}s: {reason}")
        return True

    def pending_allocation_timeout_exempt(self, allocation: dict, reason: str) -> bool:
        normalized_reason = (reason or "").strip().lower()
        if "priority" not in normalized_reason:
            return False
        if normalize_gpu_model(str(allocation.get("gpu_model") or "")) != "a6000":
            return False
        if (allocation.get("resource_pool") or "") != "gpu:a6000":
            return False
        if int(allocation.get("total_gpus") or 0) < int(self.gpu_prewarm_gpus_per_allocation or 1):
            return False
        return "warm pool" in (allocation.get("drain_reason") or "").lower()

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
                self.on_task_terminal(task, "allocation lost")

    def fail_stale_same_node_tasks(self) -> None:
        terminal = {TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in {
                TaskStatus.QUEUED.value,
                TaskStatus.ATTACHING.value,
                TaskStatus.RUNNING.value,
            }:
                continue
            reference_id = self.same_node_as_task_id(task)
            if reference_id <= 0:
                continue
            reference = self.db.get_task(reference_id)
            message = ""
            if not reference:
                message = f"same_node_as task {reference_id} not found"
            elif reference.get("status") in terminal:
                message = f"same_node_as task {reference_id} is {reference.get('status')}"
            if not message:
                continue
            self.db.update_task(
                task["id"],
                status=TaskStatus.FAILED.value,
                failure_message=message,
                finished_at="CURRENT_TIMESTAMP",
            )
            self.on_task_terminal(task, "failed")
            if task.get("allocation_id"):
                self.recalculate_allocation_capacity()

    def close_allocation(self, allocation: dict, reason: str) -> None:
        if allocation["state"] in {AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value, AllocationStatus.CLOSING.value}:
            return
        previous_state = allocation["state"]
        account = next((item for item in self.accounts if item.name == allocation["account_name"]), None)
        self.db.update_allocation(allocation["id"], state=AllocationStatus.CLOSING.value, drain_reason=reason)
        if account and allocation.get("slurm_job_id"):
            try:
                self._client(account).cancel(allocation["slurm_job_id"])
            except Exception as exc:
                self.db.update_allocation(
                    allocation["id"],
                    state=previous_state,
                    failure_message=str(exc),
                )
                return
        self.db.update_allocation(
            allocation["id"],
            state=AllocationStatus.CLOSED.value,
            failure_message="",
            closed_at="CURRENT_TIMESTAMP",
        )
        self.record_event(
            "allocation_closed",
            reason,
            entity_type="allocation",
            entity_id=allocation["id"],
            account_name=str(allocation.get("account_name") or ""),
        )

    def active_task_ids_for_allocation(self, allocation_id: int) -> list[int]:
        return [
            int(task["id"])
            for task in self.db.list_tasks(limit=5000)
            if int(task.get("allocation_id") or 0) == allocation_id
            and task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        ]

    def request_close_allocation(self, allocation_id: int, force: bool = False, allow_protected: bool = False) -> dict:
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            raise ValueError("allocation not found")
        previous_state = allocation["state"]
        if previous_state in {AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value}:
            return {
                "ok": True,
                "id": allocation_id,
                "previous_state": previous_state,
                "state": previous_state,
                "force": force,
                "closed_task_ids": [],
            }
        if self.external_close_protected_allocation(allocation) and not allow_protected:
            raise RuntimeError(
                f"allocation {allocation_id} is a protected GPU warm pool; "
                "the scheduler keeps it for minimum GPU warm capacity"
            )
        active_task_ids = self.active_task_ids_for_allocation(allocation_id)
        if active_task_ids and not force:
            raise RuntimeError(
                f"allocation {allocation_id} has active tasks: {', '.join(str(item) for item in active_task_ids)}"
            )
        if active_task_ids:
            self.fail_running_tasks(allocation_id, "allocation manually closed")
        self.close_allocation(allocation, "manual close")
        updated = self.db.get_allocation(allocation_id) or allocation
        return {
            "ok": updated["state"] == AllocationStatus.CLOSED.value,
            "id": allocation_id,
            "previous_state": previous_state,
            "state": updated["state"],
            "force": force,
            "allow_protected": allow_protected,
            "closed_task_ids": active_task_ids,
        }

    def external_close_protected_allocation(self, allocation: dict) -> bool:
        return self.protected_gpu_warm_pool(allocation)

    def protected_gpu_warm_pool(self, allocation: dict) -> bool:
        resource_pool = str(allocation.get("resource_pool") or "")
        if not resource_pool.startswith("gpu:"):
            return False
        return "warm pool" in str(allocation.get("drain_reason") or "").lower()

    def allocation_pool_in_backoff(self, resource_pool: str) -> bool:
        until = self._allocation_backoff_until_by_pool.get(resource_pool)
        if not until:
            return False
        if until <= time.monotonic():
            self._allocation_backoff_until_by_pool.pop(resource_pool, None)
            return False
        return True

    def allocation_node_in_backoff(self, resource_pool: str, node_name: str) -> bool:
        key = (str(resource_pool or "cpu"), str(node_name or ""))
        until = self._allocation_node_backoff_until.get(key)
        if not until:
            return False
        if until <= time.monotonic():
            self._allocation_node_backoff_until.pop(key, None)
            return False
        return True

    @classmethod
    def allocation_shape_backoff_key(cls, resource_pool: str, partition: str) -> tuple[str, str]:
        normalized_partition = ",".join(sorted(set(cls.partition_spec_names(partition))))
        return str(resource_pool or "cpu"), normalized_partition

    def backoff_rejected_allocation_shape(self, allocation: dict) -> None:
        key = self.allocation_shape_backoff_key(
            str(allocation.get("resource_pool") or "cpu"),
            str(allocation.get("partition") or ""),
        )
        if not key[1]:
            return
        backoff_seconds = max(
            self.poll_interval_seconds * 2,
            min(max(0, self.allocation_pending_backoff_seconds), 1800),
        )
        self._allocation_shape_backoff_until[key] = time.monotonic() + backoff_seconds

    def allocation_shape_in_backoff(self, resource_pool: str, shape: dict) -> bool:
        key = self.allocation_shape_backoff_key(resource_pool, str(shape.get("partition") or ""))
        until = self._allocation_shape_backoff_until.get(key)
        if not until:
            return False
        if until <= time.monotonic():
            self._allocation_shape_backoff_until.pop(key, None)
            return False
        return True

    def reserved_allocation_nodes(self) -> set[str]:
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        return {
            str(allocation.get("node_name") or "")
            for allocation in self.db.list_allocations(limit=1000)
            if allocation.get("state") in live_states and str(allocation.get("node_name") or "")
        }

    def assign_queued_tasks(self, include_fea: bool = True) -> None:
        queued_tasks = sorted(
            [task for task in self.db.list_tasks(limit=5000) if task["status"] == TaskStatus.QUEUED.value],
            key=lambda item: (
                0 if self.task_requires_gpu(item) else 1,
                -int(item.get("priority") or 0),
                int(item["id"]),
            ),
        )
        fea_attached_this_loop = 0
        for task in queued_tasks:
            if self.task_is_fea_bursty(task) and not include_fea:
                continue
            if self.task_is_fea_bursty(task) and fea_attached_this_loop >= self.fea_max_attach_per_loop:
                continue
            attached = self.assign_queued_task(task)
            if attached and self.task_is_fea_bursty(task):
                fea_attached_this_loop += 1

    def assign_ready_same_node_tasks(self) -> None:
        for task in sorted(
            [
                item
                for item in self.db.list_tasks(limit=5000)
                if item["status"] == TaskStatus.QUEUED.value and self.same_node_as_task_id(item)
            ],
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        ):
            self.assign_queued_task(task)

    def assign_ready_standard_tasks(self) -> None:
        attached = 0
        for task in sorted(
            [
                item
                for item in self.db.list_tasks(limit=5000)
                if item["status"] == TaskStatus.QUEUED.value
                and not self.same_node_as_task_id(item)
                and not self.task_is_fea_bursty(item)
                and not self.task_requires_gpu(item)
            ],
            key=lambda item: (-int(item.get("priority") or 0), -int(item.get("cpus") or 0), int(item["id"])),
        ):
            if attached >= self.allocation_max_new_per_loop:
                return
            if self.assign_queued_task(task):
                attached += 1

    def assign_ready_gpu_tasks(self) -> None:
        attached = 0
        for task in sorted(
            [
                item
                for item in self.db.list_tasks(limit=5000)
                if item["status"] == TaskStatus.QUEUED.value
                and not self.same_node_as_task_id(item)
                and not self.task_is_fea_bursty(item)
                and self.task_requires_gpu(item)
            ],
            key=lambda item: (-int(item.get("priority") or 0), -int(item.get("gpus") or 0), int(item["id"])),
        ):
            if attached >= self.allocation_max_new_per_loop:
                return
            if self.assign_queued_task(task):
                attached += 1

    def assign_ready_fea_tasks(self, background: bool = False) -> None:
        attached = 0
        for task in sorted(
            [
                item
                for item in self.db.list_tasks(limit=5000)
                if item["status"] == TaskStatus.QUEUED.value and self.task_is_fea_bursty(item)
            ],
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        ):
            if attached >= self.fea_max_attach_per_loop:
                return
            if self.assign_queued_task(task, background=background):
                attached += 1

    def _warn_storage_guard(self, account: AccountConfig, detail: str) -> None:
        now = time.monotonic()
        if now - self._storage_guard_warned_at.get(account.name, 0.0) <= 3600:
            return
        self._storage_guard_warned_at[account.name] = now
        LOGGER.warning("storage guard holding work on %s: %s", account.name, detail)
        self.record_event(
            "storage_guard",
            detail,
            entity_type="account",
            entity_id=account.name,
            account_name=account.name,
        )

    def account_storage_blocked(self, account: AccountConfig, *, for_fea: bool = False) -> bool:
        """Quota guard: hold new attaches when the account's storage headroom
        is below the threshold, instead of letting tasks start and cascade
        into disk-quota-exceeded failures."""
        if self.storage_guard_min_free_gb <= 0:
            return False
        if for_fea:
            probe = self.cached_storage_quota(account, self._client(account), time.time())
            if probe.error:
                detail = f"FEA work held: storage quota probe failed ({probe.error[:200]})"
                self._warn_storage_guard(account, detail)
                return True
            if probe.is_gpfs:
                if probe.quota is None:
                    self._warn_storage_guard(account, "FEA work held: GPFS quota status is unavailable")
                    return True
                free_gb = probe.quota.free_gb
                if free_gb is None:
                    return False
                if free_gb >= self.storage_guard_min_free_gb:
                    return False
                detail = (
                    f"FEA work held: GPFS {probe.quota.quota_type.lower()} block quota "
                    f"for fileset {probe.fileset_name or probe.quota.fileset_name or 'unknown'} "
                    f"has {free_gb:.1f} GB free "
                    f"(< {self.storage_guard_min_free_gb:g} GB; "
                    f"used+in_doubt {probe.quota.effective_used_gb:.1f} / "
                    f"{probe.quota.block_limit_gb:.1f} GB)"
                )
                self._warn_storage_guard(account, detail)
                return True
        if not account.storage_quota_gb:
            return False
        cached = self._storage_cache.get(account.name)
        used = cached[1] if cached else None
        if used is None:
            return False
        free_gb = float(account.storage_quota_gb) - float(used)
        if free_gb >= self.storage_guard_min_free_gb:
            return False
        self._warn_storage_guard(
            account,
            f"attaches held: {free_gb:.1f} GB free is below the {self.storage_guard_min_free_gb:g} GB threshold",
        )
        return True

    def assign_queued_task(self, task: dict, background: bool = False) -> bool:
        if task.get("status") != TaskStatus.QUEUED.value:
            return False
        allocation = self.best_allocation_for_task(task)
        if not allocation:
            return False
        account = next((item for item in self.accounts if item.name == allocation["account_name"]), None)
        if not account:
            return False
        if self.account_storage_blocked(account, for_fea=self.task_is_fea_bursty(task)):
            return False
        task = self.reserve_task_on_allocation(task, allocation, account)
        if background:
            self.start_background_task_attach(task, allocation, account)
            return True
        return self.finish_reserved_task_attach(task, allocation, account)

    def reserve_task_on_allocation(self, task: dict, allocation: dict, account: AccountConfig) -> dict:
        task = self.apply_dynamic_env_profile(task, account)
        self.db.update_task(
            task["id"],
            status=TaskStatus.ATTACHING.value,
            allocation_id=allocation["id"],
            account_name=allocation["account_name"],
            attached_at="CURRENT_TIMESTAMP",
        )
        self._record_attach_delta(allocation, task)
        self.recalculate_allocation_capacity()
        return {**task, "status": TaskStatus.ATTACHING.value, "allocation_id": allocation["id"], "account_name": allocation["account_name"]}

    def start_background_task_attach(self, task: dict, allocation: dict, account: AccountConfig) -> None:
        thread = threading.Thread(
            target=self.finish_background_task_attach,
            args=(task, allocation, account),
            name=f"attach-task-{task['id']}",
            daemon=True,
        )
        thread.start()

    def finish_background_task_attach(self, task: dict, allocation: dict, account: AccountConfig) -> bool:
        with self._background_attach_semaphore:
            return self.finish_reserved_task_attach(task, allocation, account)

    def finish_reserved_task_attach(self, task: dict, allocation: dict, account: AccountConfig) -> bool:
        """Complete a reserved attach. All task updates are conditional on the
        task still being ATTACHING: a concurrent requeue (rebalance, pressure
        drain) or cancel owns the row once it transitions, and stomping it left
        tasks running with no allocation."""
        try:
            result = self._client(account).attach_task(task, allocation)
        except RemoteExecutionError as exc:
            if self.db.update_task_if_status(
                task["id"],
                [TaskStatus.ATTACHING.value],
                status=TaskStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
                **exc.result_fields,
            ):
                self.on_task_terminal(task, "attach failed")
            else:
                LOGGER.info("attach failure for task %s ignored; task already transitioned", task["id"])
            self.recalculate_allocation_capacity()
            return False
        except Exception as exc:
            if self.db.update_task_if_status(
                task["id"],
                [TaskStatus.ATTACHING.value],
                status=TaskStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
            ):
                self.on_task_terminal(task, "attach failed")
            else:
                LOGGER.info("attach failure for task %s ignored; task already transitioned", task["id"])
            self.recalculate_allocation_capacity()
            return False
        if not self.db.update_task_if_status(
            task["id"],
            [TaskStatus.ATTACHING.value],
            status=TaskStatus.RUNNING.value,
            started_at="CURRENT_TIMESTAMP",
            **result,
        ):
            # The task was requeued or cancelled while the attach was in
            # flight; the remote worker just started, so stop it again.
            LOGGER.warning(
                "task %s transitioned during attach; cancelling the freshly started worker", task["id"]
            )
            try:
                self._client(account).cancel_task({**task, **result}, self._task_allocation_job_id(task))
            except Exception as exc:
                LOGGER.warning("failed to cancel raced attach for task %s: %s", task["id"], exc)
            self.recalculate_allocation_capacity()
            return False
        return True

    def best_allocation_for_task(self, task: dict) -> dict | None:
        exact = self.best_allocation_for_effective_task(task)
        if exact or not self.task_can_relax_preferred_node(task):
            return exact
        return self.best_allocation_for_effective_task(self.relaxed_preferred_node_task(task))

    def best_allocation_for_effective_task(self, task: dict) -> dict | None:
        cpu_candidates = []
        gpu_candidates = []
        active_task_allocation_ids, active_exclusive_allocation_ids = self.active_task_allocation_sets()
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}:
                continue
            if not self.allocation_accepts_new_tasks(allocation):
                continue
            if not allocation.get("slurm_job_id"):
                continue
            if not self.allocation_can_run_task(
                allocation,
                task,
                include_pending=False,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            ):
                continue
            # FEA tasks intentionally ignore the allocation's bookkeeping
            # free_cpus/free_memory values. They must still respect the hard
            # per-allocation requested-CPU cap, though. Without this check the
            # background attach loop repeatedly overfills an allocation and
            # the retroactive rebalancer kills/requeues the same workers.
            if self.task_is_fea_bursty(task):
                cap_remaining = self.fea_node_cpu_cap_remaining(allocation, task)
                if cap_remaining is not None and cap_remaining <= 0:
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
        if self.task_is_fea_bursty(task):
            self.annotate_fea_node_worker_counts(candidates)
            return max(
                candidates,
                key=lambda item: (
                    -self.allocation_worker_count_for_task(item, task),
                    self.fit_slots_for_allocation(item, task),
                    self.fea_memory_free_percent(item) or 0.0,
                    int(item.get("free_memory_mb") or 0),
                ),
            )
        return max(
            candidates,
            key=lambda item: (
                self.allocation_model_score(item),
                int(item.get("free_gpus") or 0),
                self.borrowable_cpus(item) if not self.task_requires_gpu(task) else int(item["free_cpus"]),
                int(item["free_memory_mb"]),
            ),
        )

    def allocation_worker_count(self, allocation_id: int) -> int:
        return sum(
            1
            for task in self.db.list_tasks(limit=5000)
            if int(task.get("allocation_id") or 0) == allocation_id
            and task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        )

    def node_worker_counts(self) -> dict[str, int]:
        allocation_node_by_id = {
            int(allocation["id"]): str(allocation.get("node_name") or "")
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }
            and str(allocation.get("node_name") or "")
        }
        counts: dict[str, int] = {}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            node_name = allocation_node_by_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            counts[node_name] = counts.get(node_name, 0) + 1
        return counts

    def node_fea_worker_counts(self) -> dict[str, int]:
        allocation_node_by_id = {
            int(allocation["id"]): str(allocation.get("node_name") or "")
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }
            and str(allocation.get("node_name") or "")
        }
        counts: dict[str, int] = {}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            node_name = allocation_node_by_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            counts[node_name] = counts.get(node_name, 0) + 1
        return counts

    def annotate_fea_node_worker_counts(self, allocations: list[dict]) -> None:
        counts = self.node_fea_worker_counts()
        for allocation in allocations:
            node_name = str(allocation.get("node_name") or "")
            if node_name:
                allocation["_node_worker_count"] = counts.get(node_name, 0)

    def allocation_worker_count_for_task(self, allocation: dict, task: dict) -> int:
        node_name = str(allocation.get("node_name") or "")
        if self.task_is_fea_bursty(task) and node_name:
            if "_node_worker_count" in allocation:
                return int(allocation.get("_node_worker_count") or 0)
            return self.node_fea_worker_counts().get(node_name, 0)
        return self.allocation_worker_count(int(allocation["id"]))

    def reserved_fea_slots_for_node(self, allocations: list[dict] | None, node_name: str) -> int:
        if not allocations or not node_name:
            return 0
        return sum(
            int(allocation.get("_reserved_fea_slots") or 0)
            for allocation in allocations
            if str(allocation.get("node_name") or "") == node_name
        )

    def task_fit_capacity(
        self,
        task: dict,
        allocation_rows: list[dict] | None = None,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> dict:
        if active_task_allocation_ids is None or active_exclusive_allocation_ids is None:
            active_task_allocation_ids, active_exclusive_allocation_ids = self.active_task_allocation_sets()
        if self.task_is_fea_bursty(task) and allocation_rows is None:
            allocation_rows = self.db.list_allocations(limit=500)
        if self.task_is_fea_bursty(task) and allocation_rows is not None:
            has_node_rows = any(str(allocation.get("node_name") or "") for allocation in allocation_rows)
            already_annotated = any("_node_worker_count" in allocation for allocation in allocation_rows)
            if has_node_rows and not already_annotated:
                self.annotate_fea_node_worker_counts(allocation_rows)
        summaries = [
            self.task_fit_capacity_for_effective_task(
                effective_task,
                preferred_node_relaxed=relaxed,
                allocation_rows=allocation_rows,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            )
            for effective_task, relaxed in self.effective_task_variants(task)
        ]
        if len(summaries) == 1:
            return summaries[0]
        exact, relaxed = summaries[0], summaries[1]
        if int(exact.get("ready_fit_slots") or 0) > 0 or int(exact.get("pending_fit_slots") or 0) > 0:
            return exact
        return relaxed

    def task_fit_capacity_for_effective_task(
        self,
        task: dict,
        preferred_node_relaxed: bool = False,
        allocation_rows: list[dict] | None = None,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> dict:
        allocations = []
        ready_slots = 0
        pending_slots = 0
        pressure_states: list[str] = []
        for allocation in allocation_rows if allocation_rows is not None else self.db.list_allocations(limit=500):
            if allocation["state"] not in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}:
                continue
            is_pending = allocation["state"] == AllocationStatus.PENDING.value
            if not is_pending and not self.allocation_accepts_new_tasks(allocation):
                continue
            if not allocation.get("slurm_job_id"):
                continue
            allocation_pressure_state = "ok"
            if self.task_is_fea_bursty(task) and self.allocation_matches_task_constraints(
                allocation,
                task,
                include_pending=is_pending,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            ):
                allocation_pressure_state = self.fea_memory_pressure_state(allocation)
                if not is_pending:
                    pressure_states.append(allocation_pressure_state)
            if not self.allocation_can_run_task(
                allocation,
                task,
                include_pending=is_pending,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            ):
                continue
            slots = self.fit_slots_for_allocation(allocation, task)
            if slots <= 0:
                continue
            if is_pending:
                pending_slots += slots
            else:
                ready_slots += slots
            allocations.append(
                {
                    "allocation_id": allocation["id"],
                    "account_name": allocation.get("account_name") or "",
                    "slurm_job_id": allocation.get("slurm_job_id") or "",
                    "state": allocation.get("state") or "",
                    "partition": allocation.get("partition") or "",
                    "node_name": allocation.get("node_name") or "",
                    "free_cpus": int(allocation.get("free_cpus") or 0),
                    "free_memory_mb": int(allocation.get("free_memory_mb") or 0),
                    "free_gpus": int(allocation.get("free_gpus") or 0),
                    "gpu_model": allocation.get("gpu_model") or "",
                    "fit_slots": slots,
                    "memory_pressure_state": allocation_pressure_state,
                    "node_memory_free_percent": self.fea_memory_free_percent(allocation) if self.task_is_fea_bursty(task) else None,
                }
            )
        pressure_rank = {"ok": 0, "soft_blocked": 1, "hard_pressure": 2}
        memory_pressure_state = "ok"
        if pressure_states:
            memory_pressure_state = max(pressure_states, key=lambda item: pressure_rank.get(item, 0))
        inflight_slots = ready_slots + pending_slots
        return {
            "fit_slots": ready_slots,
            "ready_fit_slots": ready_slots,
            "pending_fit_slots": pending_slots,
            "inflight_fit_slots": inflight_slots,
            "memory_pressure_state": memory_pressure_state,
            "preferred_node_relaxed": preferred_node_relaxed,
            "allocations": allocations,
        }

    def placement_dry_run(self, task: dict) -> dict:
        """Explain, without submitting anything, where a hypothetical task
        would land: per-account eligibility, per-allocation fit and rejection
        reasons, plus the aggregate queue diagnostics."""
        allocation_rows = self.db.list_allocations_with_live(limit=500)
        active_ids, active_exclusive_ids = self.active_task_allocation_sets()
        capacity = self.task_fit_capacity(
            task,
            allocation_rows=allocation_rows,
            active_task_allocation_ids=active_ids,
            active_exclusive_allocation_ids=active_exclusive_ids,
        )
        diagnostics = self.task_queue_diagnostics(
            task,
            capacity=capacity,
            allocation_rows=allocation_rows,
            active_task_allocation_ids=active_ids,
            active_exclusive_allocation_ids=active_exclusive_ids,
        )
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in self.snapshots()}
        requested = self.requested_accounts(str(task.get("account_name") or ""))
        accounts = []
        for account in self.accounts:
            reasons: list[str] = []
            if requested and account.name not in requested:
                reasons.append("not in the requested account list")
            if not self.account_supports(
                account, str(task.get("required_capability") or ""), str(task.get("env_profile") or "")
            ):
                reasons.append("missing required capability or env profile")
            snapshot = snapshots_by_name.get(account.name)
            if snapshot is None:
                reasons.append("no account snapshot yet")
            elif not snapshot.available:
                reasons.append(
                    f"job limit reached ({snapshot.running} running + {snapshot.pending} pending of {snapshot.max_total})"
                )
            accounts.append({"name": account.name, "eligible": not reasons, "reasons": reasons})
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
        }
        allocations = []
        for allocation in allocation_rows:
            if allocation["state"] not in live_states:
                continue
            slots = self.fit_slots_for_allocation(allocation, task)
            allocations.append(
                {
                    "id": allocation["id"],
                    "state": allocation["state"],
                    "account_name": allocation.get("account_name") or "",
                    "node_name": allocation.get("node_name") or "",
                    "resource_pool": allocation.get("resource_pool") or "cpu",
                    "fit_slots": slots,
                    "reasons": [] if slots > 0 else self.allocation_rejection_reasons(allocation, task),
                }
            )
        return {
            "task": {key: task.get(key) for key in (
                "cpus", "memory_mb", "gpus", "gpu_model", "partition", "node_name",
                "scheduling_profile", "required_capability", "env_profile", "account_name",
            )},
            "queue_state": diagnostics.get("queue_state"),
            "queue_reason": diagnostics.get("queue_reason"),
            "capacity": capacity,
            "accounts": accounts,
            "allocations": allocations,
        }

    def allocation_rejection_reasons(self, allocation: dict, task: dict) -> list[str]:
        reasons: list[str] = []
        if self.task_is_fea_bursty(task):
            if allocation.get("state") != AllocationStatus.PENDING.value:
                node = self.pestat_node_for_allocation(allocation)
                if node is None:
                    if self.fea_stale_node_recently_ok(allocation):
                        reasons.append("pestat stale; conservative single slot only")
                    else:
                        reasons.append("pestat stale or missing and last snapshot was not healthy")
                else:
                    pressure = self.fea_memory_pressure_state(allocation)
                    if pressure != "ok":
                        percent = self.fea_memory_free_percent(allocation)
                        reasons.append(
                            f"memory pressure {pressure}"
                            + (f" (free {percent:.0f}%)" if percent is not None else "")
                        )
                    if not self.fea_node_load_ok(allocation):
                        reasons.append("node CPU load above target")
                if self.fea_allocation_sustained_overloaded(allocation):
                    reasons.append("node in sustained FEA overload")
            return reasons or ["no free FEA slots"]
        requested_cpus = int(task.get("cpus") or 1)
        requested_memory = int(task.get("memory_mb") or 0)
        requested_gpus = int(task.get("gpus") or 0)
        if requested_gpus > 0:
            if int(allocation.get("free_gpus") or 0) < requested_gpus:
                reasons.append(
                    f"insufficient free GPUs ({allocation.get('free_gpus') or 0} of {requested_gpus} requested)"
                )
            wanted_model = normalize_gpu_model(str(task.get("gpu_model") or ""))
            if wanted_model and normalize_gpu_model(str(allocation.get("gpu_model") or "")) != wanted_model:
                reasons.append(f"gpu model mismatch (allocation has '{allocation.get('gpu_model') or 'none'}')")
        if int(allocation.get("free_cpus") or 0) < requested_cpus:
            reasons.append(f"insufficient free CPUs ({allocation.get('free_cpus') or 0} of {requested_cpus} requested)")
        if requested_memory and int(allocation.get("free_memory_mb") or 0) < requested_memory:
            reasons.append(
                f"insufficient free memory ({allocation.get('free_memory_mb') or 0} of {requested_memory} MB requested)"
            )
        partition = str(task.get("partition") or "auto")
        if partition not in {"", "auto"} and str(allocation.get("partition") or "") != partition:
            reasons.append(f"partition mismatch (allocation on '{allocation.get('partition') or ''}')")
        node_name = str(task.get("node_name") or "")
        if node_name and str(allocation.get("node_name") or "") != node_name:
            reasons.append(f"node mismatch (allocation on '{allocation.get('node_name') or ''}')")
        return reasons or ["not eligible for this task"]

    def task_queue_diagnostics(
        self,
        task: dict,
        capacity: dict | None = None,
        allocation_rows: list[dict] | None = None,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> dict:
        capacity = capacity if capacity is not None else self.task_fit_capacity(
            task,
            allocation_rows=allocation_rows,
            active_task_allocation_ids=active_task_allocation_ids,
            active_exclusive_allocation_ids=active_exclusive_allocation_ids,
        )
        ready_slots = int(capacity.get("ready_fit_slots") or 0)
        pending_slots = int(capacity.get("pending_fit_slots") or 0)
        inflight_slots = int(capacity.get("inflight_fit_slots") or 0)
        preferred_node_relaxed = bool(capacity.get("preferred_node_relaxed") or False)
        reason = ""
        queue_state = "ready" if ready_slots > 0 else "pending" if pending_slots > 0 else "opening"
        same_node_reference_id = self.same_node_as_task_id(task)
        same_node_target = self.same_node_target_for_task(task) if same_node_reference_id else None

        if self.task_can_relax_preferred_node(task):
            if active_task_allocation_ids is None or active_exclusive_allocation_ids is None:
                active_task_allocation_ids, active_exclusive_allocation_ids = self.active_task_allocation_sets()
            exact_capacity = self.task_fit_capacity_for_effective_task(
                task,
                allocation_rows=allocation_rows,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            )
            relaxed_capacity = self.task_fit_capacity_for_effective_task(
                self.relaxed_preferred_node_task(task),
                preferred_node_relaxed=True,
                allocation_rows=allocation_rows,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            )
            exact_blocked = (
                int(exact_capacity.get("ready_fit_slots") or 0) <= 0
                and (exact_capacity.get("memory_pressure_state") or "") != "ok"
            )
            if exact_blocked and int(relaxed_capacity.get("inflight_fit_slots") or 0) > 0:
                preferred_node_relaxed = True
                queue_state = "ready" if int(relaxed_capacity.get("ready_fit_slots") or 0) > 0 else "pending"
                reason = (
                    f"preferred node {task.get('node_name')} soft-blocked; "
                    "eligible fallback nodes available"
                )

        if not reason:
            if same_node_reference_id and not same_node_target:
                queue_state = "pending"
                reason = self.same_node_wait_reason(task)
            elif ready_slots > 0:
                ready_ids = [
                    str(item["allocation_id"])
                    for item in capacity.get("allocations", [])
                    if item.get("state") in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
                ]
                if same_node_target:
                    reason = (
                        f"ready to co-locate with task {same_node_reference_id} "
                        f"on node {same_node_target['node_name']}: allocations {','.join(ready_ids)}"
                    )
                else:
                    reason = f"ready to attach to allocation{'s' if len(ready_ids) != 1 else ''} {','.join(ready_ids)}"
            elif pending_slots > 0:
                pending_ids = [
                    str(item["allocation_id"])
                    for item in capacity.get("allocations", [])
                    if item.get("state") == AllocationStatus.PENDING.value
                ]
                if same_node_target:
                    reason = (
                        f"waiting for pending same-node pool on {same_node_target['node_name']}: "
                        f"allocations {','.join(pending_ids)}"
                    )
                elif self.task_requires_gpu(task):
                    gpu_count = int(task.get("gpus") or 0)
                    gpu_model = normalize_gpu_model(str(task.get("gpu_model") or "")) or "GPU"
                    reason = f"waiting for pending {gpu_count} {gpu_model} GPU pool: allocations {','.join(pending_ids)}"
                else:
                    pool_size = int(task.get("cpus") or 0)
                    reason = f"waiting for pending {pool_size} CPU pool: allocations {','.join(pending_ids)}"
            else:
                if same_node_target:
                    queue_state = "pending"
                    reason = (
                        f"waiting for capacity on node {same_node_target['node_name']} "
                        f"with task {same_node_reference_id}"
                    )
                    limit_reason = ""
                else:
                    limit_reason = self.account_limit_reason_for_allocation(task, allocation_rows=allocation_rows)
                if limit_reason:
                    queue_state = "blocked"
                    reason = limit_reason
                else:
                    worker_limit_reason = self.fea_worker_limit_reason_for_task(
                        task,
                        allocation_rows=allocation_rows,
                        active_task_allocation_ids=active_task_allocation_ids,
                        active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                    )
                    resource_pool = self.demand_resource_pool_for_task(task)
                    shape_block_reason = self.allocation_shape_block_reason_for_task(task)
                    if worker_limit_reason:
                        queue_state = "blocked"
                        reason = worker_limit_reason
                    elif resource_pool and self.allocation_pool_in_backoff(resource_pool):
                        queue_state = "blocked"
                        reason = f"allocation backoff active for {resource_pool}"
                    elif shape_block_reason:
                        queue_state = "blocked"
                        reason = shape_block_reason
                    elif self.task_requires_gpu(task):
                        queue_state = "opening"
                        reason = "no ready GPU pool fits; opening demand pools"
                    else:
                        queue_state = "opening"
                        reason = f"no single ready pool has {int(task.get('cpus') or 0)} free CPUs; opening demand pools"

        return {
            "ready_fit_slots": ready_slots,
            "pending_fit_slots": pending_slots,
            "inflight_fit_slots": inflight_slots,
            "queue_state": queue_state,
            "queue_reason": reason,
            "preferred_node_relaxed": preferred_node_relaxed,
        }

    def fea_worker_limit_reason_for_task(
        self,
        task: dict,
        allocation_rows: list[dict] | None = None,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> str:
        if not self.task_is_fea_bursty(task):
            return ""
        max_workers = int(task.get("max_workers_per_node") or 0)
        if max_workers <= 0:
            return ""
        if active_task_allocation_ids is None or active_exclusive_allocation_ids is None:
            active_task_allocation_ids, active_exclusive_allocation_ids = self.active_task_allocation_sets()
        rows = allocation_rows if allocation_rows is not None else self.db.list_allocations(limit=500)
        if any(str(allocation.get("node_name") or "") for allocation in rows) and not any(
            "_node_worker_count" in allocation for allocation in rows
        ):
            self.annotate_fea_node_worker_counts(rows)
        capped_nodes: dict[str, int] = {}
        for effective_task, _relaxed in self.effective_task_variants(task):
            uncapped_task = {**effective_task, "max_workers_per_node": 0}
            for allocation in rows:
                if allocation["state"] not in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}:
                    continue
                if not self.allocation_accepts_new_tasks(allocation):
                    continue
                if not allocation.get("slurm_job_id"):
                    continue
                if not self.allocation_can_run_task(
                    allocation,
                    uncapped_task,
                    include_pending=False,
                    active_task_allocation_ids=active_task_allocation_ids,
                    active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                ):
                    continue
                node_name = str(allocation.get("node_name") or "").strip()
                worker_count = self.allocation_worker_count_for_task(allocation, effective_task)
                if node_name:
                    worker_count += self.reserved_fea_slots_for_node(rows, node_name)
                effective_limit = self.fea_effective_worker_limit(allocation, effective_task, worker_count, max_workers)
                if worker_count < effective_limit:
                    return ""
                label = node_name or f"allocation {allocation['id']}"
                capped_nodes[label] = max(capped_nodes.get(label, 0), worker_count)
        if not capped_nodes:
            return ""
        capped = ", ".join(
            f"{name} {count}/{max_workers}" for name, count in sorted(capped_nodes.items())
        )
        return f"FEA max_workers_per_node reached: {capped}"

    def demand_resource_pool_for_task(self, task: dict) -> str:
        if self.task_requires_gpu(task):
            model = self.choose_gpu_model_for_task(task) or self.choose_gpu_model_for_prewarm()
            return f"gpu:{model}" if model else ""
        return "cpu"

    def account_limit_reason_for_allocation(self, task: dict, allocation_rows: list[dict] | None = None) -> str:
        cached_snapshots = self.cached_snapshots()
        if not cached_snapshots:
            return ""
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in cached_snapshots}
        open_by_account: dict[str, int] = {}
        pending_by_account: dict[str, int] = {}
        for allocation in allocation_rows if allocation_rows is not None else self.db.list_allocations(limit=500):
            if allocation["state"] in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
                AllocationStatus.CLOSING.value,
            }:
                open_by_account[allocation["account_name"]] = open_by_account.get(allocation["account_name"], 0) + 1
            if allocation["state"] == AllocationStatus.PENDING.value:
                pending_by_account[allocation["account_name"]] = pending_by_account.get(allocation["account_name"], 0) + 1
        requested_accounts = self.requested_accounts(str(task.get("account_name") or ""))
        eligible = [
            account
            for account in self.accounts
            if (not requested_accounts or account.name in requested_accounts)
            and self.account_supports(
                account,
                str(task.get("required_capability") or ""),
                str(task.get("env_profile") or ""),
            )
            and snapshots_by_name.get(account.name)
        ]
        if not eligible:
            return "no configured account supports task requirements"
        blocked: list[str] = []
        for account in eligible:
            snapshot = snapshots_by_name[account.name]
            max_total = max(0, account.max_total_jobs - self.allocation_reserved_job_slots)
            current_total = max(snapshot.running + snapshot.pending, open_by_account.get(account.name, 0))
            current_pending = max(snapshot.pending, pending_by_account.get(account.name, 0))
            if current_total >= max_total or current_pending >= account.max_pending_jobs:
                blocked.append(account.name)
                continue
            return ""
        if len(blocked) == 1:
            return f"account {blocked[0]} job limit reached"
        return f"account job limit reached: {','.join(blocked)}"

    def allocation_shape_block_reason_for_task(self, task: dict) -> str:
        if self.same_node_as_task_id(task):
            return ""
        if self.task_requires_gpu(task):
            return ""
        requested_cpus = int(task.get("cpus") or 0)
        if requested_cpus <= 0:
            return ""
        if self.choose_allocation_shape(
            resource_pool="cpu",
            requested_cpus=requested_cpus,
            require_fea_eligible_node=self.task_is_fea_bursty(task),
        ):
            return ""

        target_partition = self.allocation_partition
        fitting_nodes_by_partition: dict[str, set[str]] = {}
        inventory_rows = self.db.list_node_inventory()
        if inventory_rows:
            for row in inventory_rows:
                partition = str(row.get("partition") or "")
                if target_partition != "auto" and not self.partition_spec_allows(target_partition, partition):
                    continue
                gpu_count = int(row.get("gpu_count") or 0)
                node_is_gpu_partition = partition.startswith("gpu") or gpu_count > 0
                if target_partition == "auto" and node_is_gpu_partition and not self.cpu_pool_allow_gpu_partitions:
                    continue
                reserve = self.gpu_cpu_reserve if node_is_gpu_partition else 0
                if max(0, int(row.get("cpus") or 0) - reserve) >= requested_cpus:
                    fitting_nodes_by_partition.setdefault(partition, set()).add(str(row.get("node_name") or ""))
        else:
            for row in self.db.list_pestat_nodes():
                partition = str(row.get("partition") or "")
                if target_partition != "auto" and not self.partition_spec_allows(target_partition, partition):
                    continue
                node_is_gpu_partition = partition.startswith("gpu")
                if target_partition == "auto" and node_is_gpu_partition and not self.cpu_pool_allow_gpu_partitions:
                    continue
                reserve = self.gpu_cpu_reserve if node_is_gpu_partition else 0
                if max(0, int(row.get("cpu_total") or 0) - reserve) >= requested_cpus:
                    fitting_nodes_by_partition.setdefault(partition, set()).add(str(row.get("hostname") or ""))

        fitting_nodes_by_partition = {
            partition: {node_name for node_name in node_names if node_name}
            for partition, node_names in fitting_nodes_by_partition.items()
            if any(node_names)
        }
        if not fitting_nodes_by_partition:
            return f"cannot open {requested_cpus} CPU pool: no partition has a single node with {requested_cpus} CPUs"

        limited_nodes: list[tuple[str, str]] = []
        total_fitting_nodes = 0
        for partition, node_names in sorted(fitting_nodes_by_partition.items()):
            for node_name in sorted(node_names):
                total_fitting_nodes += 1
                if self.cpu_partition_allocation_limit_reached(partition, node_name):
                    limited_nodes.append((partition, node_name))
        if limited_nodes and len(limited_nodes) == total_fitting_nodes:
            labels = []
            for partition, node_name in limited_nodes[:6]:
                limit = self.cpu_allocation_node_limit(partition)
                live_count = self.live_allocation_count_for_partition_node(partition, node_name, resource_pool="cpu")
                labels.append(f"{partition}/{node_name} {live_count}/{limit}")
            if len(limited_nodes) > len(labels):
                labels.append(f"+{len(limited_nodes) - len(labels)} more")
            return (
                f"cannot open {requested_cpus} CPU pool: "
                f"CPU allocation limit reached for {', '.join(labels)}"
            )
        return ""

    def fit_slots_for_allocation(self, allocation: dict, task: dict, reservation_allocations: list[dict] | None = None) -> int:
        if self.task_is_fea_bursty(task):
            if allocation.get("state") != AllocationStatus.PENDING.value and not self.fea_allocation_accepts_task(allocation):
                return 0
            slots = self.fea_max_attach_per_loop
            if (
                allocation.get("state") == AllocationStatus.PENDING.value
                and (allocation.get("resource_pool") or "cpu") == "cpu"
                and self.fea_node_requested_cpu_factor > 0
            ):
                cpu_cap = int(int(allocation.get("total_cpus") or 0) * self.fea_node_requested_cpu_factor)
                slots = min(slots, cpu_cap // max(1, int(task.get("cpus") or 1)))
            reserved_slots = int(allocation.get("_reserved_fea_slots") or 0)
            max_workers = int(task.get("max_workers_per_node") or 0)
            node_name = str(allocation.get("node_name") or "")
            if node_name:
                # Always bound by the node-level dynamic limit (load/memory
                # headroom, young-worker footprint, node CPU cap) — tasks
                # without max_workers_per_node used to bypass it entirely,
                # which both dogpiled nodes and made capacity look infinite
                # to the demand scale-out.
                node_workers = self.allocation_worker_count_for_task(allocation, task)
                node_reserved_slots = self.reserved_fea_slots_for_node(reservation_allocations, node_name)
                dynamic_limit = self.fea_effective_worker_limit(allocation, task, node_workers, max_workers)
                slots = min(slots, max(0, dynamic_limit - node_workers - node_reserved_slots))
                reserved_slots = 0
            elif max_workers > 0:
                slots = min(slots, max(0, max_workers - self.allocation_worker_count(int(allocation["id"]))))
            if self.task_requires_gpu(task):
                gpu_slots = int(allocation.get("free_gpus") or 0) // max(1, int(task.get("gpus") or 1))
                slots = min(slots, gpu_slots)
            return max(0, slots - reserved_slots)
        memory_slots = int(allocation.get("free_memory_mb") or 0) // max(1, int(task.get("memory_mb") or 1))
        if self.task_requires_gpu(task):
            gpu_slots = int(allocation.get("free_gpus") or 0) // max(1, int(task.get("gpus") or 1))
            if int(task.get("cpus") or 0) <= 4 and gpu_slots > 0:
                cpu_slots = gpu_slots
            else:
                cpu_slots = int(allocation.get("free_cpus") or 0) // max(1, int(task.get("cpus") or 1))
            return max(0, min(memory_slots, gpu_slots, cpu_slots))
        if self.task_can_overlap_same_node_allocation(
            allocation,
            task,
            include_pending=allocation.get("state") == AllocationStatus.PENDING.value,
        ):
            return 1
        cpu_slots = self.borrowable_cpus(allocation) // max(1, int(task.get("cpus") or 1))
        return max(0, min(memory_slots, cpu_slots))

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
            for effective_task, _relaxed in self.effective_task_variants(task):
                if self.allocation_can_run_task(allocation, effective_task, include_pending=True):
                    return True
        return False

    def task_requires_gpu(self, task: dict) -> bool:
        return int(task.get("gpus") or 0) > 0

    def task_scheduling_profile(self, task: dict) -> str:
        return normalize_scheduling_profile(str(task.get("scheduling_profile") or ""))

    def task_is_fea_bursty(self, task: dict) -> bool:
        return self.task_scheduling_profile(task) == SchedulingProfile.FEA_BURSTY.value

    def task_can_relax_preferred_node(self, task: dict) -> bool:
        return (
            self.fea_node_name_policy == "preferred"
            and self.task_is_fea_bursty(task)
            and bool(str(task.get("node_name") or "").strip())
            and not int(task.get("same_node_as_task_id") or 0)
            and not self.task_requires_gpu(task)
            and not int(task.get("exclusive_node") or 0)
        )

    def relaxed_preferred_node_task(self, task: dict) -> dict:
        if not self.task_can_relax_preferred_node(task):
            return task
        relaxed = dict(task)
        relaxed["requested_node_name"] = str(task.get("node_name") or "")
        relaxed["node_name"] = ""
        return relaxed

    def effective_task_variants(self, task: dict) -> list[tuple[dict, bool]]:
        variants: list[tuple[dict, bool]] = [(task, False)]
        if self.task_can_relax_preferred_node(task):
            variants.append((self.relaxed_preferred_node_task(task), True))
        return variants

    def pestat_stale_after_seconds(self) -> float:
        candidates = [120.0, float(max(1, self.poll_interval_seconds) * 4)]
        if self.cluster_refresh_interval_seconds > 0:
            candidates.append(float(self.cluster_refresh_interval_seconds) * 2)
        return max(candidates)

    def pestat_node_for_allocation(self, allocation: dict, max_age_seconds: float | None = None) -> dict | None:
        node_name = str(allocation.get("node_name") or "").strip()
        if not node_name:
            return None
        age_limit = self.pestat_stale_after_seconds() if max_age_seconds is None else max_age_seconds
        if self._tick_started_at is None:
            rows_by_hostname = {
                str(row.get("hostname") or ""): row
                for row in self.db.list_pestat_nodes()
            }
        else:
            cached = self._pestat_nodes_cache
            if cached is None or cached[0] != self._tick_seq:
                cached = (
                    self._tick_seq,
                    {
                        str(row.get("hostname") or ""): row
                        for row in self.db.list_pestat_nodes()
                    },
                )
                self._pestat_nodes_cache = cached
            rows_by_hostname = cached[1]
        row = rows_by_hostname.get(node_name)
        if not row:
            return None
        observed_at = self._timestamp(row.get("observed_at"))
        if not observed_at:
            return None
        if (self._now() - observed_at).total_seconds() > age_limit:
            return None
        return row

    def _record_attach_delta(self, allocation: dict, task: dict) -> None:
        node_name = str(allocation.get("node_name") or "").strip()
        if not node_name:
            return
        self._tick_attach_workers_by_node[node_name] = self._tick_attach_workers_by_node.get(node_name, 0) + 1
        # New ATTACHING rows change both the pressure and young-footprint views.
        self._fea_pressures_cache = None
        self._fea_alloc_pressures_cache = None
        self._fea_footprint_cache = None

    def fea_stale_node_recently_ok(self, allocation: dict) -> bool:
        """Fresh pestat is missing; look at the last (up to 3x stale) row. If
        the node looked healthy then, allow a trickle instead of freezing all
        FEA attach cluster-wide on one failed refresh."""
        row = self.pestat_node_for_allocation(allocation, max_age_seconds=3 * self.pestat_stale_after_seconds())
        if not row:
            return False
        total = int(row.get("memory_mb") or 0)
        if total <= 0:
            return False
        free_percent = (int(row.get("free_memory_mb") or 0) / total) * 100.0
        if free_percent < self.fea_soft_memory_free_percent:
            return False
        cpu_total = max(1, int(row.get("cpu_total") or 1))
        return float(row.get("cpu_load") or 0.0) <= cpu_total * self.fea_load_target

    def fea_memory_free_percent(self, allocation: dict) -> float | None:
        node = self.pestat_node_for_allocation(allocation)
        if not node:
            return None
        total = int(node.get("memory_mb") or 0)
        if total <= 0:
            return None
        return max(0.0, (int(node.get("free_memory_mb") or 0) / total) * 100.0)

    def fea_memory_pressure_state(self, allocation: dict) -> str:
        percent = self.fea_memory_free_percent(allocation)
        if percent is None:
            return "soft_blocked"
        if percent < self.fea_hard_memory_free_percent:
            return "hard_pressure"
        if percent < self.fea_soft_memory_free_percent:
            return "soft_blocked"
        return "ok"

    def fea_node_load_ok(self, allocation: dict) -> bool:
        node = self.pestat_node_for_allocation(allocation)
        if not node:
            return False
        cpu_total = max(1, int(node.get("cpu_total") or 1))
        return float(node.get("cpu_load") or 0.0) <= cpu_total * self.fea_load_target

    def fea_allocation_accepts_task(self, allocation: dict) -> bool:
        if self.pestat_node_for_allocation(allocation) is None:
            return self.fea_stale_node_recently_ok(allocation) and not self.fea_allocation_sustained_overloaded(
                allocation
            )
        return (
            self.fea_memory_pressure_state(allocation) == "ok"
            and self.fea_node_load_ok(allocation)
            and not self.fea_allocation_sustained_overloaded(allocation)
        )

    def fea_owned_node_pressures(self) -> dict[str, dict[str, int]]:
        cached = self._fea_pressures_cache
        if cached is not None and cached[0] == self._tick_seq:
            return cached[1]
        pressures = self._compute_fea_owned_node_pressures()
        self._fea_pressures_cache = (self._tick_seq, pressures)
        return pressures

    def _compute_fea_owned_node_pressures(self) -> dict[str, dict[str, int]]:
        allocation_node_by_id: dict[int, str] = {}
        owned_cpus_by_node: dict[str, int] = {}
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            node_name = str(allocation.get("node_name") or "")
            if not node_name:
                continue
            allocation_id = int(allocation["id"])
            allocation_node_by_id[allocation_id] = node_name
            if (allocation.get("resource_pool") or "cpu") == "cpu":
                owned_cpus_by_node[node_name] = owned_cpus_by_node.get(node_name, 0) + int(
                    allocation.get("total_cpus") or 0
                )

        requested_cpus_by_node: dict[str, int] = {}
        workers_by_node: dict[str, int] = {}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            node_name = allocation_node_by_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            workers_by_node[node_name] = workers_by_node.get(node_name, 0) + 1
            requested_cpus_by_node[node_name] = requested_cpus_by_node.get(node_name, 0) + int(
                task.get("cpus") or 0
            )

        pressures: dict[str, dict[str, int]] = {}
        for node_name, workers in workers_by_node.items():
            pressures[node_name] = {
                "workers": workers,
                "requested_cpus": requested_cpus_by_node.get(node_name, 0),
                "owned_cpus": owned_cpus_by_node.get(node_name, 0),
            }
        return pressures

    def fea_allocation_pressures(self) -> dict[int, dict[str, int]]:
        cached = self._fea_alloc_pressures_cache
        if cached is not None and cached[0] == self._tick_seq:
            return cached[1]
        pressures = self._compute_fea_allocation_pressures()
        self._fea_alloc_pressures_cache = (self._tick_seq, pressures)
        return pressures

    def _compute_fea_allocation_pressures(self) -> dict[int, dict[str, int]]:
        # FEA cap is per Slurm allocation (job): each cpu allocation reserves its
        # own cores and tasks attach to one allocation via srun --jobid. Keying
        # per node would inflate the cap when several cpu2 allocations share a
        # node (n allocations -> owned = n*64), letting FEA overshoot a single
        # allocation's reservation.
        owned_by_alloc: dict[int, int] = {}
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            if (allocation.get("resource_pool") or "cpu") != "cpu":
                continue
            owned_by_alloc[int(allocation["id"])] = int(allocation.get("total_cpus") or 0)
        requested_by_alloc: dict[int, int] = {}
        workers_by_alloc: dict[int, int] = {}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            alloc_id = int(task.get("allocation_id") or 0)
            if alloc_id not in owned_by_alloc:
                continue
            workers_by_alloc[alloc_id] = workers_by_alloc.get(alloc_id, 0) + 1
            requested_by_alloc[alloc_id] = requested_by_alloc.get(alloc_id, 0) + int(task.get("cpus") or 0)
        return {
            alloc_id: {
                "workers": workers_by_alloc.get(alloc_id, 0),
                "requested_cpus": requested_by_alloc.get(alloc_id, 0),
                "owned_cpus": owned,
            }
            for alloc_id, owned in owned_by_alloc.items()
        }

    def fea_owned_node_pressure_overloaded(self, pressure: dict[str, int]) -> bool:
        if self.fea_overload_scale_out_load_factor <= 0:
            return False
        owned_cpus = int(pressure.get("owned_cpus") or 0)
        if owned_cpus <= 0:
            return False
        return int(pressure.get("requested_cpus") or 0) > owned_cpus * self.fea_overload_scale_out_load_factor

    def update_fea_overload_state(self) -> None:
        if self.fea_overload_scale_out_load_factor <= 0 or self.fea_overload_scale_out_seconds <= 0:
            self._fea_overload_since_by_node.clear()
            self._fea_overload_scaled_nodes.clear()
            return
        pressures = self.fea_owned_node_pressures()
        now = time.monotonic()
        for node_name, pressure in pressures.items():
            if self.fea_owned_node_pressure_overloaded(pressure):
                self._fea_overload_since_by_node.setdefault(node_name, now)
            else:
                self._fea_overload_since_by_node.pop(node_name, None)
                self._fea_overload_scaled_nodes.discard(node_name)
        for node_name in list(self._fea_overload_since_by_node):
            if node_name not in pressures:
                self._fea_overload_since_by_node.pop(node_name, None)
                self._fea_overload_scaled_nodes.discard(node_name)

    def fea_node_sustained_overloaded(self, node_name: str) -> bool:
        since = self._fea_overload_since_by_node.get(node_name)
        if since is None:
            return False
        return (time.monotonic() - since) >= self.fea_overload_scale_out_seconds

    def fea_allocation_sustained_overloaded(self, allocation: dict) -> bool:
        node_name = str(allocation.get("node_name") or "")
        return bool(node_name) and self.fea_node_sustained_overloaded(node_name)

    def queued_fea_tasks(self) -> list[dict]:
        return sorted(
            [
                task
                for task in self.db.list_tasks(limit=5000)
                if task["status"] == TaskStatus.QUEUED.value
                and self.task_is_fea_bursty(task)
                and not int(task.get("exclusive_node") or 0)
                and not self.task_requires_gpu(task)
                and not self.same_node_as_task_id(task)
            ],
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        )

    def scale_out_for_fea_overload(self) -> bool:
        queued = self.queued_fea_tasks()
        if not queued or self.allocation_pool_in_backoff("cpu"):
            return False
        self.update_fea_overload_state()
        overloaded_nodes = [
            node_name
            for node_name in sorted(self._fea_overload_since_by_node)
            if self.fea_node_sustained_overloaded(node_name)
            and node_name not in self._fea_overload_scaled_nodes
        ]
        if not overloaded_nodes:
            return False
        task = queued[0]
        node_name = overloaded_nodes[0]
        pressure = self.fea_owned_node_pressures().get(node_name, {})
        pressure_text = ""
        if pressure:
            pressure_text = (
                f" owned requested CPU {int(pressure.get('requested_cpus') or 0)}/"
                f"{int(pressure.get('owned_cpus') or 0)}"
            )
        allocation = self.open_allocation_record(
            f"queued FEA overload scale-out {node_name}{pressure_text}",
            resource_pool="cpu",
            preferred_accounts=self.warm_pool_preferred_accounts,
            required_capability=str(task.get("required_capability") or ""),
            env_profile=str(task.get("env_profile") or ""),
            account_name=str(task.get("account_name") or ""),
            require_fea_eligible_node=True,
        )
        if not allocation:
            return False
        self._fea_overload_scaled_nodes.add(node_name)
        return True

    def fea_immature_footprint(self, node_name: str) -> dict[str, float]:
        """Declared cpus/memory of FEA workers younger than the maturity window
        on this node. FEA consumes its resources late (meshing refines over
        time), so observed pestat free capacity overstates what is really
        available while young workers are still growing into their footprint."""
        empty = {"cpus": 0.0, "memory_mb": 0.0}
        if self.fea_footprint_maturity_seconds <= 0 or not node_name:
            return empty
        cached = self._fea_footprint_cache
        if cached is not None and cached[0] == self._tick_seq:
            return cached[1].get(node_name, empty)
        by_node = self._compute_fea_immature_footprints()
        self._fea_footprint_cache = (self._tick_seq, by_node)
        return by_node.get(node_name, empty)

    def _compute_fea_immature_footprints(self) -> dict[str, dict[str, float]]:
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
        }
        node_by_allocation_id = {
            int(allocation["id"]): str(allocation.get("node_name") or "")
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"] in live_states
        }
        now = self._now()
        out: dict[str, dict[str, float]] = {}
        for task in self.db.list_tasks(limit=5000):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            node_name = node_by_allocation_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            started = self._timestamp(
                task.get("attached_at") or task.get("started_at") or task.get("created_at")
            )
            # Unknown start time counts as young: overcounting is the safe side.
            if started is not None and (now - started).total_seconds() >= self.fea_footprint_maturity_seconds:
                continue
            entry = out.setdefault(node_name, {"cpus": 0.0, "memory_mb": 0.0})
            entry["cpus"] += float(task.get("cpus") or 0)
            entry["memory_mb"] += float(task.get("memory_mb") or 0)
        return out

    def fea_dynamic_extra_slots(self, allocation: dict, task: dict) -> int:
        if allocation.get("state") == AllocationStatus.PENDING.value:
            return self.fea_max_attach_per_loop
        node_name = str(allocation.get("node_name") or "").strip()
        node_tick_attaches = self._tick_attach_workers_by_node.get(node_name, 0) if node_name else 0
        if node_tick_attaches >= self.fea_max_attach_per_node_per_loop:
            return 0
        node = self.pestat_node_for_allocation(allocation)
        if not node:
            # Stale pestat: conservative single slot when the node was healthy
            # at the last observation, instead of freezing attach entirely.
            return 1 if self.fea_allocation_accepts_task(allocation) else 0
        if not self.fea_allocation_accepts_task(allocation):
            return 0
        # Discount workers still growing into their declared footprint: FEA
        # consumes CPU/RAM late (mesh refinement), so observed free capacity
        # overstates what is really available.
        footprint = self.fea_immature_footprint(node_name)
        memory_total = max(1, int(node.get("memory_mb") or 1))
        memory_free = max(0, int(node.get("free_memory_mb") or 0) - int(footprint.get("memory_mb") or 0))
        soft_floor = int(memory_total * (self.fea_soft_memory_free_percent / 100.0))
        memory_budget = max(0, memory_free - soft_floor)
        memory_slots = memory_budget // max(1, int(task.get("memory_mb") or 1))

        cpu_total = max(1, int(node.get("cpu_total") or 1))
        cpu_load = max(0.0, float(node.get("cpu_load") or 0.0)) + float(footprint.get("cpus") or 0.0)
        load_budget = max(0.0, (cpu_total * self.fea_load_target) - cpu_load)
        cpu_slots = int(load_budget // max(1, int(task.get("cpus") or 1)))

        return max(
            0,
            min(
                memory_slots,
                cpu_slots,
                self.fea_max_attach_per_loop,
                self.fea_max_attach_per_node_per_loop - node_tick_attaches,
            ),
        )

    def fea_effective_worker_limit(
        self,
        allocation: dict,
        task: dict,
        current_workers: int,
        base_limit: int,
    ) -> int:
        if base_limit <= 0:
            limit = current_workers + self.fea_dynamic_extra_slots(allocation, task)
        else:
            limit = max(base_limit, current_workers + self.fea_dynamic_extra_slots(allocation, task))
        cap_remaining = self.fea_node_cpu_cap_remaining(allocation, task)
        if cap_remaining is not None:
            limit = min(limit, current_workers + cap_remaining)
        return limit

    def _node_cpu_total(self, node_name: str) -> int:
        if not node_name:
            return 0
        row = self.pestat_node_for_allocation({"node_name": node_name}, max_age_seconds=float("inf"))
        if row and int(row.get("cpu_total") or 0) > 0:
            return int(row.get("cpu_total") or 0)
        for inventory_row in self.db.list_node_inventory():
            if str(inventory_row.get("node_name") or "") == node_name:
                return int(inventory_row.get("cpus") or 0)
        return 0

    def _task_allocation_job_id(self, task: dict) -> str:
        """Slurm job id of the task's allocation, so cancel can srun onto the
        compute node to reap daemonized solver grandchildren."""
        alloc_id = int(task.get("allocation_id") or 0)
        if alloc_id <= 0:
            return ""
        allocation = self.db.get_allocation(alloc_id)
        return str(allocation.get("slurm_job_id") or "") if allocation else ""

    @staticmethod
    def _orphan_process_sweep_shell(name_patterns: list[str], live_task_ids: list[str], min_age_seconds: int) -> str:
        """On-node shell: for each solver process (own user), kill it if its
        SLURM_SCHED_TASK_ID marker is not among the live task ids; if it carries
        no marker, kill only when it has no python ancestor and is older than
        min_age (ancestry fallback for markerless pre-fix orphans)."""
        pats = " ".join(shlex.quote(p) for p in name_patterns)
        live = " ".join(shlex.quote(str(t)) for t in live_task_ids)
        minage = max(0, int(min_age_seconds))
        return (
            f'live=" {live} "; minage={minage}; killed=0; me=$(id -u); '
            f'for pat in {pats}; do '
            'for pid in $(pgrep -x -u "$me" "$pat" 2>/dev/null); do '
            '[ -e /proc/$pid/environ ] || continue; '
            'tid=$(tr "\\0" "\\n" < /proc/$pid/environ 2>/dev/null | sed -n "s/^SLURM_SCHED_TASK_ID=//p" | head -1); '
            'if [ -n "$tid" ]; then '
            'case "$live" in *" $tid "*) continue ;; esac; '
            'kill -KILL "$pid" 2>/dev/null && killed=$((killed+1)); '
            'else '
            'age=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d " "); '
            '[ -n "$age" ] && [ "$age" -ge "$minage" ] || continue; '
            'p=$pid; haspy=0; '
            'while [ "${p:-0}" -gt 1 ]; do '
            'c=$(ps -o comm= -p "$p" 2>/dev/null); '
            'case "$c" in *python*) haspy=1; break ;; esac; '
            'p=$(ps -o ppid= -p "$p" 2>/dev/null | tr -d " "); [ -n "$p" ] || break; '
            'done; '
            '[ "$haspy" = 0 ] && { kill -KILL "$pid" 2>/dev/null && killed=$((killed+1)); }; '
            'fi; '
            'done; done; echo "orphan_killed=$killed"'
        )

    def sweep_orphan_processes_if_due(self) -> None:
        """Reap daemonized solver grandchildren (ansysedt/3dedy) that left their
        task's process group and survive on a node. Runs on each live
        allocation's node via `srun --overlap`, sparing any process whose
        SLURM_SCHED_TASK_ID marks a still-active task."""
        if not self.orphan_process_sweep_enabled:
            return
        now = time.time()
        if now - self._last_orphan_process_sweep_at < self.orphan_process_sweep_interval_seconds:
            return
        self._last_orphan_process_sweep_at = now
        patterns = [p.strip() for p in self.orphan_process_name_patterns if p and p.strip()]
        if not patterns:
            return
        live_ids = sorted(
            {
                str(task["id"])
                for task in self.db.list_tasks_with_active(limit=5000)
                if task.get("status") in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
            }
        )
        live_states = {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value, AllocationStatus.DRAINING.value}
        command = self._orphan_process_sweep_shell(patterns, live_ids, self.orphan_process_min_age_seconds)
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in live_states:
                continue
            node = str(allocation.get("node_name") or "").strip()
            job_id = str(allocation.get("slurm_job_id") or "").strip()
            account = self.account_by_name(str(allocation.get("account_name") or ""))
            if not node or not job_id or not account:
                continue
            srun = f"srun --jobid={shlex.quote(job_id)} --overlap bash -lc {shlex.quote(command)}"
            try:
                with SSHSession(account, default_timeout=90) as ssh:
                    result = ssh.run(srun, timeout=80)
            except Exception as exc:
                LOGGER.warning("orphan process sweep failed on %s/%s: %s", account.name, node, exc)
                continue
            last = (result.stdout or "").strip().splitlines()[-1:] or [""]
            if last[0].startswith("orphan_killed=") and last[0] != "orphan_killed=0":
                LOGGER.info("orphan process sweep on %s/%s: %s", account.name, node, last[0])

    def enforce_fea_node_cpu_cap(self) -> None:
        """Retroactive side of the per-allocation FEA CPU cap: allocations that
        accumulated more FEA-requested CPUs than their reserved cores * factor
        are drained newest-worker-first, a few per tick, by requeueing the
        workers so the work reruns elsewhere."""
        if self.fea_node_requested_cpu_factor <= 0:
            return
        pressures = self.fea_allocation_pressures()
        if not pressures:
            return
        live_states = {
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
        }
        allocation_by_id = {
            int(allocation["id"]): allocation
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"] in live_states
        }
        tasks_by_alloc: dict[int, list[dict]] = {}
        for task in self.db.list_tasks(limit=5000):
            # RUNNING only: draining a task whose attach is still in flight
            # races the background attach thread.
            if task["status"] != TaskStatus.RUNNING.value:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            alloc_id = int(task.get("allocation_id") or 0)
            if alloc_id in allocation_by_id:
                tasks_by_alloc.setdefault(alloc_id, []).append(task)
        rebalanced = False
        for alloc_id, pressure in pressures.items():
            requested = int(pressure.get("requested_cpus") or 0)
            # Cap on the CPUs THIS allocation reserved (total_cpus), not the
            # node's physical cores nor the sum across allocations on the node:
            # tasks attach per allocation, so each allocation must stay within
            # its own 64-core reservation regardless of node co-tenancy.
            owned = int(pressure.get("owned_cpus") or 0)
            if owned <= 0:
                continue
            cap = owned * self.fea_node_requested_cpu_factor
            if requested <= cap:
                continue
            allocation = allocation_by_id.get(alloc_id, {})
            node_name = str(allocation.get("node_name") or "")
            victims = sorted(
                tasks_by_alloc.get(alloc_id, []),
                key=lambda task: (
                    task.get("attached_at") or task.get("started_at") or task.get("created_at") or "",
                    int(task.get("id") or 0),
                ),
                reverse=True,
            )
            drained = 0
            for task in victims:
                if requested <= cap or drained >= self.fea_max_attach_per_node_per_loop:
                    break
                account = self.account_by_name(str(task.get("account_name") or ""))
                if account:
                    try:
                        self._client(account).cancel_task(task, self._task_allocation_job_id(task))
                    except Exception as exc:
                        LOGGER.warning("failed to cancel FEA task %s for cap rebalance: %s", task["id"], exc)
                self.requeue_task_for_rebalance(
                    task, f"allocation {alloc_id} (node {node_name}) over FEA CPU cap ({requested}/{cap:.0f})"
                )
                requested -= int(task.get("cpus") or 0)
                drained += 1
                rebalanced = True
            if drained:
                LOGGER.info(
                    "rebalanced %d FEA workers off allocation %d on %s (requested CPUs now %d, cap %.0f)",
                    drained,
                    alloc_id,
                    node_name,
                    requested,
                    cap,
                )
        if rebalanced:
            self.recalculate_allocation_capacity()

    def on_task_terminal(self, task: dict, state: str = "terminal") -> None:
        """Run the task's declared cleanup on EVERY terminal path (completed,
        failed, cancelled, timed out, allocation lost) — shell-level cleanup
        inside the task command cannot cover cancel/kill because nothing after
        the killed process runs. Only the scheduler sees all exits."""
        globs = [
            g.strip()
            for g in str(task.get("cleanup_globs") or "").split(",")
            if g.strip() and self._workspace_prune_glob_ok(g)
        ]
        if not globs:
            return
        account = self.account_by_name(str(task.get("account_name") or ""))
        if not account:
            return
        threading.Thread(
            target=self._cleanup_task_workdir,
            args=(dict(task), account, globs, state),
            name=f"task-cleanup-{task.get('id')}",
            daemon=True,
        ).start()

    def _cleanup_task_workdir(self, task: dict, account: AccountConfig, globs: list[str], state: str) -> None:
        try:
            resolved = resolve_task_placeholders(task, account)
            cwd = str(resolved.get("remote_cwd") or "").strip()
            normalized = self._normalize_remote_path(cwd)
            if not cwd or normalized in {"", ".", "/", "~"}:
                return
            name_expr = " -o ".join(f"-name {shlex.quote(g)}" for g in globs)
            command = (
                f"find {shell_path(cwd)} -mindepth 1 -maxdepth 1 \\( {name_expr} \\) -prune "
                "-exec rm -rf {} + 2>/dev/null; true"
            )
            with SSHSession(account, default_timeout=600) as ssh:
                ssh.run(command)
            self.record_event(
                "task_cleanup",
                f"cleaned {', '.join(globs)} in {cwd} after task {task.get('name') or task.get('id')} ended ({state})",
                entity_type="task",
                entity_id=task.get("id") or "",
                account_name=account.name,
            )
        except Exception as exc:
            LOGGER.warning("terminal cleanup failed for task %s: %s", task.get("id"), exc)

    def requeue_task_for_rebalance(self, task: dict, reason: str) -> None:
        """Requeue without touching attempt_count: the worker was placed by a
        policy the scheduler has since corrected, not by its own failure."""
        self._fea_pressures_cache = None
        self._fea_alloc_pressures_cache = None
        self._fea_footprint_cache = None
        self.record_event(
            "task_requeued",
            f"task {task.get('name') or task['id']} requeued: {reason}",
            entity_type="task",
            entity_id=task["id"],
            account_name=str(task.get("account_name") or ""),
        )
        self.db.update_task(
            task["id"],
            status=TaskStatus.QUEUED.value,
            allocation_id=None,
            remote_dir="",
            stdout_path="",
            stderr_path="",
            exit_code_path="",
            wrapper_pid="",
            failure_message="",
            attached_at=None,
            started_at=None,
        )

    def fea_node_cpu_cap_remaining(self, allocation: dict, task: dict) -> int | None:
        """How many more workers of this task fit under the per-ALLOCATION cap:
        FEA-requested CPUs in this Slurm allocation <= its reserved cores
        (total_cpus) * fea_node_requested_cpu_factor. Per-allocation, not
        per-node, so several cpu2 allocations sharing a node each stay bounded to
        their own reservation. None = no cap applicable."""
        if self.fea_node_requested_cpu_factor <= 0:
            return None
        if allocation.get("state") == AllocationStatus.PENDING.value:
            return None
        alloc_id = int(allocation.get("id") or 0)
        owned = int(allocation.get("total_cpus") or 0)
        if alloc_id <= 0 or owned <= 0:
            return None
        # fea_allocation_pressures reads live DB rows (invalidated on every
        # attach), so it already includes this tick's attaches.
        pressure = self.fea_allocation_pressures().get(alloc_id, {})
        requested = int(pressure.get("requested_cpus") or 0)
        budget = owned * self.fea_node_requested_cpu_factor - requested
        return max(0, int(budget // max(1, int(task.get("cpus") or 1))))

    def handle_fea_memory_pressure(self) -> None:
        reclaimed = False
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            if self.fea_memory_pressure_state(allocation) != "hard_pressure":
                continue
            task = self.newest_running_fea_task(int(allocation["id"]))
            if not task:
                continue
            account = self.account_by_name(str(task.get("account_name") or allocation.get("account_name") or ""))
            if account:
                try:
                    self._client(account).cancel_task(task, self._task_allocation_job_id(task))
                except Exception as exc:
                    LOGGER.warning("failed to cancel FEA task %s under memory pressure: %s", task["id"], exc)
            self.requeue_pressure_killed_task(task)
            reclaimed = True
        if reclaimed:
            self.recalculate_allocation_capacity()

    def requeue_pressure_killed_task(self, task: dict) -> None:
        """A pressure kill discards the worker, not the work: requeue so the
        simulation reruns elsewhere, up to the attempt cap."""
        attempts = int(task.get("attempt_count") or 0) + 1
        if attempts >= self.fea_pressure_max_attempts:
            self.db.update_task(
                task["id"],
                status=TaskStatus.FAILED.value,
                attempt_count=attempts,
                failure_message=f"memory pressure hard limit after {attempts} attempts",
                finished_at="CURRENT_TIMESTAMP",
            )
            self.on_task_terminal(task, "failed")
            return
        LOGGER.info(
            "requeueing FEA task %s after memory-pressure kill (attempt %d/%d)",
            task["id"],
            attempts,
            self.fea_pressure_max_attempts,
        )
        self._fea_pressures_cache = None
        self._fea_alloc_pressures_cache = None
        self.record_event(
            "task_requeued",
            f"task {task.get('name') or task['id']} requeued after memory-pressure kill (attempt {attempts}/{self.fea_pressure_max_attempts})",
            entity_type="task",
            entity_id=task["id"],
            account_name=str(task.get("account_name") or ""),
        )
        self.db.update_task(
            task["id"],
            status=TaskStatus.QUEUED.value,
            attempt_count=attempts,
            allocation_id=None,
            remote_dir="",
            stdout_path="",
            stderr_path="",
            exit_code_path="",
            wrapper_pid="",
            failure_message="",
            attached_at=None,
            started_at=None,
        )

    def newest_running_fea_task(self, allocation_id: int) -> dict | None:
        candidates = [
            task
            for task in self.db.list_tasks(limit=5000)
            if int(task.get("allocation_id") or 0) == allocation_id
            # RUNNING only: killing a task whose attach is still in flight
            # races the background attach thread.
            and task["status"] == TaskStatus.RUNNING.value
            and self.task_is_fea_bursty(task)
        ]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda task: (
                task.get("attached_at") or task.get("started_at") or task.get("created_at") or "",
                int(task.get("id") or 0),
            ),
        )

    def requested_accounts(self, account_name: str) -> list[str]:
        return [part.strip() for part in re.split(r"[\s,;/|]+", account_name or "") if part.strip()]

    def same_node_as_task_id(self, task: dict) -> int:
        return max(0, int(task.get("same_node_as_task_id") or task.get("same_node_as") or 0))

    def same_node_target_for_task(self, task: dict) -> dict | None:
        reference_id = self.same_node_as_task_id(task)
        if reference_id <= 0:
            return None
        reference = self.db.get_task(reference_id)
        if not reference:
            return None
        if reference.get("status") not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
            return None
        allocation_id = int(reference.get("allocation_id") or 0)
        if allocation_id <= 0:
            return None
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            return None
        if allocation.get("state") in {AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value}:
            return None
        node_name = str(allocation.get("node_name") or "").strip()
        if not node_name:
            return None
        return {
            "task_id": reference_id,
            "allocation_id": int(allocation["id"]),
            "account_name": allocation.get("account_name") or reference.get("account_name") or "",
            "partition": allocation.get("partition") or "",
            "node_name": node_name,
            "task_status": reference.get("status") or "",
            "allocation_state": allocation.get("state") or "",
        }

    def same_node_wait_reason(self, task: dict) -> str:
        reference_id = self.same_node_as_task_id(task)
        if reference_id <= 0:
            return ""
        reference = self.db.get_task(reference_id)
        if not reference:
            return f"same_node_as task {reference_id} not found"
        if reference.get("status") not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
            return f"same_node_as task {reference_id} is not running"
        allocation_id = int(reference.get("allocation_id") or 0)
        if allocation_id <= 0:
            return f"waiting for same_node_as task {reference_id} to attach to a node"
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            return f"same_node_as task {reference_id} allocation {allocation_id} not found"
        if allocation.get("state") in {AllocationStatus.CLOSED.value, AllocationStatus.FAILED.value}:
            return f"same_node_as task {reference_id} allocation {allocation_id} is {allocation.get('state')}"
        if not str(allocation.get("node_name") or "").strip():
            return f"waiting for same_node_as task {reference_id} node name"
        return ""

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

    def active_task_allocation_sets(self) -> tuple[set[int], set[int]]:
        active_allocation_ids: set[int] = set()
        active_exclusive_allocation_ids: set[int] = set()
        for task in self.db.list_tasks(limit=5000):
            if task.get("status") not in {
                TaskStatus.ATTACHING.value,
                TaskStatus.RUNNING.value,
            }:
                continue
            allocation_id = int(task.get("allocation_id") or 0)
            if not allocation_id:
                continue
            active_allocation_ids.add(allocation_id)
            if int(task.get("exclusive_node") or 0):
                active_exclusive_allocation_ids.add(allocation_id)
        return active_allocation_ids, active_exclusive_allocation_ids

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

    def allocation_matches_task_constraints(
        self,
        allocation: dict,
        task: dict,
        include_pending: bool,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> bool:
        requested_accounts = self.requested_accounts(str(task.get("account_name") or ""))
        if requested_accounts and allocation.get("account_name") not in requested_accounts:
            return False
        reference_id = self.same_node_as_task_id(task)
        if reference_id:
            target = self.same_node_target_for_task(task)
            if not target:
                return False
            if int(allocation.get("id") or 0) != int(target.get("allocation_id") or 0):
                return False
        account = self.account_by_name(str(allocation.get("account_name") or ""))
        if not self.account_supports(
            account,
            str(task.get("required_capability") or ""),
            str(task.get("env_profile") or ""),
        ):
            return False
        if self.task_is_fea_bursty(task) and account and self.account_storage_blocked(account, for_fea=True):
            return False
        max_workers = int(task.get("max_workers_per_node") or 0)
        if max_workers > 0 and not include_pending:
            worker_count = self.allocation_worker_count_for_task(allocation, task)
            if self.task_is_fea_bursty(task):
                if worker_count >= self.fea_effective_worker_limit(allocation, task, worker_count, max_workers):
                    return False
            elif worker_count >= max_workers:
                return False
        if int(task.get("exclusive_node") or 0):
            has_active_task = (
                int(allocation["id"]) in active_task_allocation_ids
                if active_task_allocation_ids is not None
                else self.allocation_has_active_task(int(allocation["id"]))
            )
            if not include_pending and has_active_task:
                return False
            if int(allocation.get("free_cpus") or 0) != int(allocation.get("total_cpus") or 0):
                return False
            if int(allocation.get("free_memory_mb") or 0) != int(allocation.get("total_memory_mb") or 0):
                return False
            if int(allocation.get("free_gpus") or 0) != int(allocation.get("total_gpus") or 0):
                return False
        elif not include_pending:
            has_active_exclusive_task = (
                int(allocation["id"]) in active_exclusive_allocation_ids
                if active_exclusive_allocation_ids is not None
                else self.allocation_has_active_exclusive_task(int(allocation["id"]))
            )
            if has_active_exclusive_task:
                return False
        if (task.get("partition") or "auto") not in {"", "auto"} and allocation.get("partition") != task.get("partition"):
            return False
        if task.get("node_name") and allocation.get("node_name") != task.get("node_name"):
            return False
        return True

    def allocation_gpu_matches_task(self, allocation: dict, task: dict) -> bool:
        if self.task_requires_gpu(task):
            task_models = gpu_model_candidates(str(task.get("gpu_model") or ""))
            allocation_model = normalize_gpu_model(str(allocation.get("gpu_model") or ""))
            if task_models and allocation_model not in task_models:
                return False
            if int(allocation.get("free_gpus") or 0) < int(task.get("gpus") or 0):
                return False
        return True

    def task_can_overlap_same_node_allocation(self, allocation: dict, task: dict, include_pending: bool = False) -> bool:
        if include_pending:
            return False
        if not self.same_node_as_task_id(task):
            return False
        if self.task_requires_gpu(task) or int(task.get("exclusive_node") or 0):
            return False
        if int(task.get("cpus") or 0) > 4:
            return False
        target = self.same_node_target_for_task(task)
        if not target:
            return False
        return int(allocation.get("id") or 0) == int(target.get("allocation_id") or 0)

    def allocation_can_run_task(
        self,
        allocation: dict,
        task: dict,
        include_pending: bool,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> bool:
        if not self.allocation_matches_task_constraints(
            allocation,
            task,
            include_pending,
            active_task_allocation_ids=active_task_allocation_ids,
            active_exclusive_allocation_ids=active_exclusive_allocation_ids,
        ):
            return False
        if self.task_is_fea_bursty(task):
            if not self.allocation_gpu_matches_task(allocation, task):
                return False
            if include_pending:
                return True
            return self.fea_allocation_accepts_task(allocation)
        if self.task_can_overlap_same_node_allocation(allocation, task, include_pending=include_pending):
            return True
        if int(allocation["free_memory_mb"]) < int(task["memory_mb"]):
            return False
        if self.task_requires_gpu(task):
            if not self.allocation_gpu_matches_task(allocation, task):
                return False
            return int(allocation["free_cpus"]) >= int(task["cpus"])
        if int(allocation.get("total_gpus") or 0) > 0:
            return self.borrowable_cpus(allocation) >= int(task["cpus"])
        return int(allocation["free_cpus"]) >= int(task["cpus"])

    def maintain_allocation_pool(self) -> None:
        self.prewarm_gpu_for_minimum()
        self.prewarm_cpu_for_minimum()
        # Retire stale demand pools before opening replacements. Opening first
        # let a new reservation change the current-fit shape, so scale-in
        # could cancel the pool created a few lines earlier in the same tick.
        self.scale_in_idle_allocations()
        self.prewarm_for_demand()

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
        self.close_undersized_gpu_warm_allocations()
        opened_preferred = self.ensure_preferred_gpu_queue()
        if opened_preferred:
            return
        live_allocations = self.live_gpu_allocations()
        satisfied_count = sum(
            1 for allocation in live_allocations if self.gpu_warm_allocation_satisfies_minimum(allocation)
        )
        if satisfied_count >= self.gpu_prewarm_min_warm_allocations:
            return
        if len(live_allocations) >= self.gpu_prewarm_max_warm_allocations:
            return
        models_at_goal = self.gpu_warm_models_at_goal(live_allocations)
        model = self.choose_gpu_model_for_fallback(models_at_goal)
        if not model:
            return
        if not self.gpu_warm_stagger_allows_open(model, live_allocations):
            return
        resource_pool = f"gpu:{model}"
        if self.allocation_pool_in_backoff(resource_pool):
            return
        self.open_allocation(
            f"fallback GPU warm pool {model}",
            resource_pool=resource_pool,
            gpu_model=model,
            gpus=0,
            preferred_accounts=self.gpu_warm_pool_preferred_accounts or self.warm_pool_preferred_accounts,
            account_name=self.preferred_gpu_warm_account_constraint(),
            requested_cpus=self.gpu_prewarm_target_cpus(),
            requested_memory_mb=self.gpu_prewarm_memory_mb(),
        )

    def ensure_preferred_gpu_queue(self) -> bool:
        live_allocations = self.live_gpu_allocations()
        if len(live_allocations) >= self.gpu_prewarm_max_warm_allocations:
            return False
        satisfied_counts = self.satisfied_gpu_warm_counts(live_allocations)
        capacity_by_model = {item["gpu_model"]: item for item in self.gpu_capacity_summary()}
        target_gpus = max(1, int(self.gpu_prewarm_gpus_per_allocation or 1))
        for model in self.gpu_prewarm_preferred_models:
            if not model or satisfied_counts.get(model, 0) >= self.gpu_prewarm_min_warm_allocations:
                continue
            if not self.gpu_warm_stagger_allows_open(model, live_allocations):
                continue
            model_capacity = capacity_by_model.get(model, {})
            if int(model_capacity.get("cluster_total_gpus") or 0) < target_gpus:
                continue
            resource_pool = f"gpu:{model}"
            if self.allocation_pool_in_backoff(resource_pool):
                continue
            if not self.open_allocation(
                f"minimum GPU warm pool {model}",
                resource_pool=resource_pool,
                gpu_model=model,
                gpus=0,
                preferred_accounts=self.gpu_warm_pool_preferred_accounts or self.warm_pool_preferred_accounts,
                account_name=self.preferred_gpu_warm_account_constraint(),
                requested_cpus=self.gpu_prewarm_target_cpus(),
                requested_memory_mb=self.gpu_prewarm_memory_mb(),
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
        if self.scale_out_for_fea_overload():
            return
        queued_tasks = self.queued_demand_tasks()
        opened, blocked = self.open_fit_aware_demand_allocations(queued_tasks)
        if opened or blocked:
            return
        self.prewarm_for_high_utilization()

    def queued_demand_tasks(self) -> list[dict]:
        return sorted(
            [
                task
                for task in self.db.list_tasks(limit=5000)
                if task["status"] == TaskStatus.QUEUED.value and not int(task.get("exclusive_node") or 0)
            ],
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        )

    def open_fit_aware_demand_allocations(self, queued_tasks: list[dict]) -> tuple[int, bool]:
        if not queued_tasks:
            return 0, False
        remaining_allocations = [
            dict(allocation)
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
        ]
        self.annotate_fea_node_worker_counts(remaining_allocations)
        opened = 0
        blocked = False
        for task in queued_tasks:
            if self.reserve_inflight_capacity_for_task(remaining_allocations, task):
                continue
            if opened >= self.allocation_max_new_per_loop:
                blocked = True
                continue
            allocation = self.open_allocation_for_task_record(task)
            if not allocation:
                blocked = True
                continue
            opened += 1
            remaining_allocations.append(dict(allocation))
            self.reserve_inflight_capacity_for_task(remaining_allocations, task)
        return opened, blocked

    def queued_task_allocation_reservations(self, queued_tasks: list[dict] | None = None) -> dict[int, list[int]]:
        tasks = queued_tasks if queued_tasks is not None else self.queued_demand_tasks()
        remaining_allocations = [
            dict(allocation)
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
        ]
        self.annotate_fea_node_worker_counts(remaining_allocations)
        reservations: dict[int, list[int]] = {}
        for task in tasks:
            allocation = self.reserve_inflight_capacity_for_task(remaining_allocations, task)
            if not allocation:
                continue
            reservations.setdefault(int(allocation["id"]), []).append(int(task["id"]))
        return reservations

    def prewarm_for_high_utilization(self) -> None:
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
        remaining_allocations = [
            dict(allocation)
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
        ]
        for task in queued_tasks:
            if int(task.get("exclusive_node") or 0):
                continue
            allocation = self.reserve_inflight_capacity_for_task(remaining_allocations, task)
            if not allocation:
                return task
        return None

    def reserve_inflight_capacity_for_task(self, allocations: list[dict], task: dict) -> dict | None:
        candidates = []
        effective_task = task
        for candidate_task, _relaxed in self.effective_task_variants(task):
            candidates = []
            for allocation in allocations:
                if not self.allocation_can_run_task(allocation, candidate_task, include_pending=True):
                    continue
                if self.fit_slots_for_allocation(allocation, candidate_task, allocations) <= 0:
                    continue
                candidates.append(allocation)
            if candidates:
                effective_task = candidate_task
                break
        if not candidates:
            return None
        allocation = max(
            candidates,
            key=lambda item: (
                self.allocation_model_score(item),
                int(item.get("free_gpus") or 0),
                self.borrowable_cpus(item) if not self.task_requires_gpu(task) else int(item.get("free_cpus") or 0),
                int(item.get("free_memory_mb") or 0),
            ),
        )
        if self.task_is_fea_bursty(effective_task):
            allocation["_reserved_fea_slots"] = int(allocation.get("_reserved_fea_slots") or 0) + 1
            if self.task_requires_gpu(effective_task):
                allocation["free_gpus"] = max(0, int(allocation.get("free_gpus") or 0) - int(effective_task.get("gpus") or 0))
            return allocation
        allocation["free_memory_mb"] = max(0, int(allocation.get("free_memory_mb") or 0) - int(effective_task.get("memory_mb") or 0))
        allocation["free_cpus"] = max(0, int(allocation.get("free_cpus") or 0) - int(effective_task.get("cpus") or 0))
        if self.task_requires_gpu(effective_task):
            allocation["free_gpus"] = max(0, int(allocation.get("free_gpus") or 0) - int(effective_task.get("gpus") or 0))
        return allocation

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
        return self.open_allocation_for_task_record(task) is not None

    def open_allocation_for_task_record(self, task: dict) -> dict | None:
        if self.same_node_as_task_id(task):
            return None
        if self.task_requires_gpu(task):
            model = self.choose_gpu_model_for_task(task) or self.choose_gpu_model_for_prewarm()
            resource_pool = f"gpu:{model}" if model else ""
            if not model or self.allocation_pool_in_backoff(resource_pool):
                return None
            return self.open_allocation_record(
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
                require_fea_eligible_node=self.task_is_fea_bursty(task),
            )
        if self.allocation_pool_in_backoff("cpu"):
            return None
        exclusive_node = bool(task.get("exclusive_node"))
        return self.open_allocation_record(
            "queued CPU demand",
            resource_pool="cpu",
            exclusive_node=exclusive_node,
            required_capability=str(task.get("required_capability") or ""),
            env_profile=str(task.get("env_profile") or ""),
            account_name=str(task.get("account_name") or ""),
            requested_cpus=int(task.get("cpus") or 0) if exclusive_node or not self.task_is_fea_bursty(task) else 0,
            requested_memory_mb=int(task.get("memory_mb") or 0) if exclusive_node else 0,
            require_fea_eligible_node=self.task_is_fea_bursty(task),
        )

    def scale_in_idle_allocations(self) -> None:
        self.scale_in_unneeded_demand_allocations()
        self.enforce_cpu_partition_allocation_limits()
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

    def enforce_cpu_partition_allocation_limits(self) -> None:
        if not self.cpu_partition_allocation_limits:
            return
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        state_rank = {
            AllocationStatus.PENDING.value: 0,
            AllocationStatus.WARM.value: 1,
            AllocationStatus.ACTIVE.value: 2,
            AllocationStatus.DRAINING.value: 3,
            AllocationStatus.CLOSING.value: 4,
        }
        for partition, limit in self.cpu_partition_allocation_limits.items():
            live_by_node: dict[str, list[dict]] = {}
            for allocation in self.db.list_allocations_with_live(limit=0, live_limit=10000):
                if allocation["state"] not in live_states:
                    continue
                if (allocation.get("resource_pool") or "cpu") != "cpu":
                    continue
                if allocation.get("partition") != partition:
                    continue
                node_name = str(allocation.get("node_name") or "")
                if not node_name:
                    continue
                live_by_node.setdefault(node_name, []).append(allocation)
            for node_name, live in live_by_node.items():
                excess = len(live) - int(limit)
                if excess <= 0:
                    continue
                closable = [
                    allocation
                    for allocation in live
                    if not self.active_task_ids_for_allocation(int(allocation["id"]))
                    and allocation["state"] not in {AllocationStatus.DRAINING.value, AllocationStatus.CLOSING.value}
                ]
                closable.sort(
                    key=lambda allocation: (
                        state_rank.get(str(allocation.get("state") or ""), 9),
                        allocation.get("created_at") or "",
                    )
                )
                for allocation in closable[:excess]:
                    self.close_allocation(allocation, f"{partition} node {node_name} CPU allocation limit {limit}")

    def scale_in_unneeded_demand_allocations(self) -> None:
        queued_tasks = sorted(
            [task for task in self.db.list_tasks(limit=5000) if task["status"] == TaskStatus.QUEUED.value],
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        )
        reservations = self.queued_task_allocation_reservations(queued_tasks)
        reserved_allocation_ids = set(reservations)
        queued_tasks_by_id = {int(task["id"]): task for task in queued_tasks}
        desired_cpu_shape = self.choose_allocation_shape(resource_pool="cpu")
        desired_cpu_pool_cpus = int(desired_cpu_shape.get("cpus") or 0) if desired_cpu_shape else 0
        demand_allocations = [
            allocation
            for allocation in self.db.list_allocations(limit=500)
            if allocation["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value}
            and (
                str(allocation.get("drain_reason") or "").startswith("queued ")
                or (
                    allocation["state"] == AllocationStatus.PENDING.value
                    and not str(allocation.get("slurm_job_id") or "")
                    and not str(allocation.get("drain_reason") or "")
                )
            )
        ]
        demand_allocations.sort(key=lambda item: int(item.get("id") or 0))
        for allocation in demand_allocations:
            if (
                (allocation.get("resource_pool") or "cpu") == "cpu"
                and allocation["state"] == AllocationStatus.PENDING.value
                and "QOSMaxCpuPerNode" in str(allocation.get("pending_reason") or "")
            ):
                self.backoff_rejected_allocation_shape(allocation)
                self.close_allocation(allocation, "CPU demand allocation exceeds QOS CPU-per-node limit")
                continue
            allocation_desired_cpu_pool_cpus = desired_cpu_pool_cpus
            reserved_task_cpus = [
                int(queued_tasks_by_id[task_id].get("cpus") or 0)
                for task_id in reservations.get(int(allocation["id"]), [])
                if task_id in queued_tasks_by_id
            ]
            if reserved_task_cpus and (allocation.get("resource_pool") or "cpu") == "cpu":
                reserved_shape = self.choose_allocation_shape(
                    resource_pool="cpu",
                    requested_cpus=max(reserved_task_cpus),
                )
                allocation_desired_cpu_pool_cpus = int(reserved_shape.get("cpus") or 0) if reserved_shape else 0
                if self.pinned_gpu_cpu_demand_allocation_covers_queue(allocation, reserved_allocation_ids):
                    continue
                if self.cpu_demand_allocation_superseded_by_shape(allocation, reserved_shape):
                    self.close_allocation(
                        allocation,
                        f"CPU demand allocation superseded by current-fit partition {reserved_shape.get('partition')}",
                    )
                    continue
            elif self.pinned_gpu_cpu_demand_allocation_covers_queue(allocation, reserved_allocation_ids):
                continue
            elif self.cpu_demand_allocation_superseded_by_shape(allocation, desired_cpu_shape):
                self.close_allocation(
                    allocation,
                    f"CPU demand allocation superseded by current-fit partition {desired_cpu_shape.get('partition')}",
                )
                continue
            if (
                allocation_desired_cpu_pool_cpus
                and (allocation.get("resource_pool") or "cpu") == "cpu"
                and not int(allocation.get("exclusive_node") or 0)
                and not self.pinned_gpu_cpu_demand_allocation_covers_queue(allocation, reserved_allocation_ids)
                and int(allocation.get("total_cpus") or 0) < allocation_desired_cpu_pool_cpus
            ):
                self.close_allocation(allocation, "undersized CPU demand allocation after pool sizing policy change")
                continue
            if int(allocation["id"]) in reserved_allocation_ids:
                continue
            if queued_tasks and self.pending_demand_allocation_in_shape_grace(allocation):
                continue
            if self.warm_demand_allocation_in_attach_grace(allocation, queued_tasks):
                continue
            self.close_allocation(allocation, "demand allocation no longer needed")

    def pending_demand_allocation_in_shape_grace(self, allocation: dict) -> bool:
        if allocation.get("state") != AllocationStatus.PENDING.value:
            return False
        if not str(allocation.get("drain_reason") or "").startswith("queued "):
            return False
        created_at = self._timestamp(allocation.get("created_at"))
        if created_at is None:
            return False
        grace_seconds = max(1, self.poll_interval_seconds * 2)
        return (self._now() - created_at).total_seconds() < grace_seconds

    def warm_demand_allocation_in_attach_grace(self, allocation: dict, queued_tasks: list[dict]) -> bool:
        if allocation.get("state") != AllocationStatus.WARM.value:
            return False
        if not str(allocation.get("drain_reason") or "").startswith("queued "):
            return False
        started_at = self._timestamp(allocation.get("started_at"))
        if started_at is None:
            return False
        grace_seconds = max(1, self.poll_interval_seconds * 2)
        if (self._now() - started_at).total_seconds() >= grace_seconds:
            return False
        return any(
            self.allocation_can_run_task(allocation, effective_task, include_pending=True)
            for task in queued_tasks
            for effective_task, _relaxed in self.effective_task_variants(task)
        )

    def pinned_gpu_cpu_demand_allocation_covers_queue(self, allocation: dict, reserved_allocation_ids: set[int]) -> bool:
        if int(allocation.get("id") or 0) not in reserved_allocation_ids:
            return False
        if (allocation.get("resource_pool") or "cpu") != "cpu":
            return False
        if allocation.get("state") != AllocationStatus.PENDING.value:
            return False
        if not str(allocation.get("node_name") or ""):
            return False
        partition = str(allocation.get("partition") or "")
        return partition.startswith("gpu")

    def cpu_demand_allocation_superseded_by_shape(self, allocation: dict, desired_shape: dict | None) -> bool:
        if not desired_shape:
            return False
        if (allocation.get("resource_pool") or "cpu") != "cpu":
            return False
        if allocation.get("state") != AllocationStatus.PENDING.value:
            return False
        if int(allocation.get("exclusive_node") or 0):
            return False
        if not str(allocation.get("drain_reason") or "").startswith("queued "):
            return False
        if self.pending_demand_allocation_in_shape_grace(allocation):
            return False
        allocation_partitions = set(self.partition_spec_names(str(allocation.get("partition") or "")))
        desired_partitions = set(self.partition_spec_names(str(desired_shape.get("partition") or "")))
        if not allocation_partitions or not desired_partitions:
            return False
        return allocation_partitions.isdisjoint(desired_partitions)

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
        require_fea_eligible_node: bool = False,
    ) -> bool:
        return self.open_allocation_record(
            reason=reason,
            resource_pool=resource_pool,
            gpu_model=gpu_model,
            gpus=gpus,
            exclusive_node=exclusive_node,
            preferred_accounts=preferred_accounts,
            required_capability=required_capability,
            env_profile=env_profile,
            account_name=account_name,
            requested_cpus=requested_cpus,
            requested_memory_mb=requested_memory_mb,
            require_fea_eligible_node=require_fea_eligible_node,
        ) is not None

    def open_allocation_record(
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
        require_fea_eligible_node: bool = False,
    ) -> dict | None:
        account = self.choose_account_for_allocation(
            preferred_accounts=preferred_accounts,
            required_capability=required_capability,
            env_profile=env_profile,
            account_name=account_name,
            require_fea_storage_headroom=require_fea_eligible_node,
        )
        if not account:
            return None
        shape = self.choose_allocation_shape(
            resource_pool=resource_pool,
            gpu_model=gpu_model,
            gpus=gpus,
            exclusive_node=exclusive_node,
            requested_cpus=requested_cpus,
            requested_memory_mb=requested_memory_mb,
            require_fea_eligible_node=require_fea_eligible_node,
        )
        if not shape:
            return None
        allocation_id = self.db.create_allocation(
            account_name=account.name,
            partition=shape["partition"],
            node_name=shape["node_name"] if (resource_pool or "cpu") == "cpu" else "",
            total_cpus=shape["cpus"],
            total_memory_mb=shape["memory_mb"],
            total_gpus=shape["gpus"],
            gpu_model=shape["gpu_model"],
            resource_pool=resource_pool,
            exclusive_node=shape["exclusive_node"],
        )
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            return None
        try:
            time_limit = self.gpu_prewarm_time_limit if resource_pool.startswith("gpu:") else self.allocation_time_limit
            result = self._client(account).submit_allocation(allocation, time_limit)
        except Exception as exc:
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.FAILED.value,
                failure_message=f"{reason}: {exc}",
                closed_at="CURRENT_TIMESTAMP",
            )
            return None
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            submitted_at="CURRENT_TIMESTAMP",
            drain_reason=reason,
            **result,
        )
        self.record_event(
            "allocation_opened",
            f"{reason} (pool {resource_pool}, slurm job {result.get('slurm_job_id')})",
            entity_type="allocation",
            entity_id=allocation_id,
            account_name=account.name,
        )
        return self.db.get_allocation(allocation_id)

    def choose_account_for_allocation(
        self,
        preferred_accounts: list[str] | None = None,
        required_capability: str = "",
        env_profile: str = "",
        account_name: str = "",
        require_fea_storage_headroom: bool = False,
    ) -> AccountConfig | None:
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in self.snapshots()}
        open_by_account: dict[str, int] = {}
        pending_by_account: dict[str, int] = {}
        for allocation in self.db.list_allocations(limit=500):
            if allocation["state"] in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
                AllocationStatus.CLOSING.value,
            }:
                open_by_account[allocation["account_name"]] = open_by_account.get(allocation["account_name"], 0) + 1
            if allocation["state"] == AllocationStatus.PENDING.value:
                pending_by_account[allocation["account_name"]] = pending_by_account.get(allocation["account_name"], 0) + 1
        candidates = []
        requested_accounts = self.requested_accounts(account_name)
        for account in self.accounts:
            if requested_accounts and account.name not in requested_accounts:
                continue
            if not self.account_supports(account, required_capability, env_profile):
                continue
            if require_fea_storage_headroom and self.account_storage_blocked(account, for_fea=True):
                continue
            snapshot = snapshots_by_name.get(account.name)
            if not snapshot:
                continue
            max_total = max(0, account.max_total_jobs - self.allocation_reserved_job_slots)
            local_open = open_by_account.get(account.name, 0)
            local_pending = pending_by_account.get(account.name, 0)
            current_total = max(snapshot.running + snapshot.pending, local_open)
            current_pending = max(snapshot.pending, local_pending)
            if current_total >= max_total:
                continue
            if current_pending >= account.max_pending_jobs:
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

    def gpu_prewarm_memory_mb(self) -> int:
        return self._memory_mb(self.gpu_prewarm_memory)

    def gpu_prewarm_target_cpus(self) -> int:
        return max(0, int(self.gpu_prewarm_cpus_per_allocation or 0))

    def gpu_warm_allocation_policy_mismatch_reason(self, allocation: dict) -> str:
        if normalize_gpu_model(str(allocation.get("gpu_model") or "")) not in set(self.gpu_prewarm_preferred_models):
            return ""
        target_gpus = max(1, int(self.gpu_prewarm_gpus_per_allocation or 1))
        state = str(allocation.get("state") or "")
        if state == AllocationStatus.PENDING.value and str(allocation.get("node_name") or "").strip():
            return f"node pin policy change ({allocation.get('node_name')} -> partition-only)"
        total_gpus = int(allocation.get("total_gpus") or 0)
        if state in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value} and total_gpus != target_gpus:
            return f"GPU count policy change ({total_gpus} != {target_gpus} GPUs)"
        target_cpus = self.gpu_prewarm_target_cpus()
        total_cpus = int(allocation.get("total_cpus") or 0)
        if target_cpus > 0 and total_cpus != target_cpus:
            return f"CPU count policy change ({total_cpus} != {target_cpus} CPUs)"
        required_memory_mb = self.gpu_prewarm_memory_mb()
        total_memory_mb = int(allocation.get("total_memory_mb") or 0)
        if required_memory_mb > 0 and total_memory_mb < required_memory_mb:
            return f"memory policy change ({total_memory_mb} < {required_memory_mb} MB)"
        if state == AllocationStatus.PENDING.value and not str(allocation.get("node_name") or ""):
            preferred_partitions = self.preferred_full_gpu_partitions(
                normalize_gpu_model(str(allocation.get("gpu_model") or "")),
                target_gpus,
                allow_multi=True,
                require_current_fit=False,
            )
            if len(preferred_partitions) > 1:
                desired_partition = ",".join(preferred_partitions)
                if self.partition_spec_names(str(allocation.get("partition") or "")) != preferred_partitions:
                    return f"partition policy change ({allocation.get('partition') or ''} -> {desired_partition})"
        return ""

    def gpu_warm_allocation_is_undersized(self, allocation: dict) -> bool:
        return bool(self.gpu_warm_allocation_policy_mismatch_reason(allocation))

    def close_undersized_gpu_warm_allocations(self) -> None:
        for allocation in self.live_gpu_allocations():
            if allocation.get("state") not in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value}:
                continue
            if "warm pool" not in str(allocation.get("drain_reason") or "").lower():
                continue
            mismatch_reason = self.gpu_warm_allocation_policy_mismatch_reason(allocation)
            if not mismatch_reason:
                continue
            self.close_allocation(
                allocation,
                f"undersized GPU warm allocation after {mismatch_reason}",
            )

    def gpu_warm_allocation_satisfies_minimum(self, allocation: dict) -> bool:
        if normalize_gpu_model(str(allocation.get("gpu_model") or "")) not in set(self.gpu_prewarm_preferred_models):
            return False
        if self.gpu_warm_allocation_is_undersized(allocation):
            return False
        target_gpus = max(1, int(self.gpu_prewarm_gpus_per_allocation or 1))
        if int(allocation.get("total_gpus") or 0) != target_gpus:
            return False
        state = allocation.get("state")
        if state == AllocationStatus.PENDING.value:
            return True
        if state in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}:
            return int(allocation.get("free_gpus") or 0) >= target_gpus
        return False

    def satisfied_gpu_warm_models(self, live_allocations: list[dict] | None = None) -> set[str]:
        allocations = live_allocations if live_allocations is not None else self.live_gpu_allocations()
        return {
            normalize_gpu_model(str(allocation.get("gpu_model") or ""))
            for allocation in allocations
            if self.gpu_warm_allocation_satisfies_minimum(allocation)
        }

    def satisfied_gpu_warm_counts(self, live_allocations: list[dict] | None = None) -> dict[str, int]:
        allocations = live_allocations if live_allocations is not None else self.live_gpu_allocations()
        counts: dict[str, int] = {}
        for allocation in allocations:
            if not self.gpu_warm_allocation_satisfies_minimum(allocation):
                continue
            model = normalize_gpu_model(str(allocation.get("gpu_model") or ""))
            counts[model] = counts.get(model, 0) + 1
        return counts

    def gpu_warm_models_at_goal(self, live_allocations: list[dict] | None = None) -> set[str]:
        return {
            model
            for model, count in self.satisfied_gpu_warm_counts(live_allocations).items()
            if count >= self.gpu_prewarm_min_warm_allocations
        }

    def gpu_warm_stagger_allows_open(self, gpu_model: str, live_allocations: list[dict] | None = None) -> bool:
        if self.gpu_prewarm_stagger_seconds <= 0:
            return True
        model = normalize_gpu_model(gpu_model)
        allocations = live_allocations if live_allocations is not None else self.live_gpu_allocations()
        matching = [
            allocation
            for allocation in allocations
            if self.gpu_warm_allocation_satisfies_minimum(allocation)
            and normalize_gpu_model(str(allocation.get("gpu_model") or "")) == model
        ]
        if not matching:
            return True
        timestamps = [
            timestamp
            for timestamp in (
                self._timestamp(
                    allocation.get("submitted_at")
                    or allocation.get("started_at")
                    or allocation.get("created_at")
                )
                for allocation in matching
            )
            if timestamp
        ]
        newest = max(timestamps, default=None)
        if not newest:
            return True
        return (self._now() - newest).total_seconds() >= self.gpu_prewarm_stagger_seconds

    def choose_gpu_model_for_prewarm(self) -> str:
        capacity = self.gpu_capacity_summary()
        by_model = {item["gpu_model"]: item for item in capacity}
        live_count = len(self.live_gpu_allocations())
        if live_count >= self.gpu_prewarm_max_warm_allocations:
            return ""
        for model in self.gpu_prewarm_preferred_models:
            item = by_model.get(model)
            if item and int(item["cluster_free_gpus"]) >= self.gpu_prewarm_min_gpus_per_allocation:
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
        preferred_models = set(self.gpu_prewarm_preferred_models)
        target_gpus = max(1, int(self.gpu_prewarm_gpus_per_allocation or 1))
        candidates = [
            item
            for item in capacity
            if normalize_gpu_model(str(item.get("gpu_model") or "")) in preferred_models
            and normalize_gpu_model(str(item.get("gpu_model") or "")) not in excluded_models
            and int(item.get("cluster_total_gpus") or 0) >= target_gpus
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

    def minimum_cpus_for_gpu_allocation(self, gpu_model: str, gpus: int) -> int:
        if normalize_gpu_model(gpu_model) == "a6000":
            return max(0, int(gpus or 0) * 4)
        return 0

    @staticmethod
    def partition_spec_names(partition_spec: str) -> list[str]:
        return [item.strip() for item in str(partition_spec or "").split(",") if item.strip()]

    @classmethod
    def partition_spec_allows(cls, partition_spec: str, partition: str) -> bool:
        names = cls.partition_spec_names(partition_spec)
        return not names or str(partition or "") in names

    @staticmethod
    def partition_sort_key(partition: str) -> tuple:
        match = re.match(r"^([A-Za-z_-]+)(\d+)$", str(partition or ""))
        if not match:
            return (str(partition or ""), -1)
        return (match.group(1), int(match.group(2)))

    def preferred_full_gpu_partitions(
        self,
        gpu_model: str,
        requested_gpus: int,
        allow_multi: bool = False,
        require_current_fit: bool = True,
    ) -> list[str]:
        target_models = gpu_model_candidates(gpu_model)
        if "a6000" not in target_models:
            return []
        if int(requested_gpus or 0) < 4 and not allow_multi:
            return []
        pestat_by_node = {
            row["hostname"]: PestatNode(
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
        }
        minimum_cpus = self.minimum_cpus_for_gpu_allocation("a6000", requested_gpus)

        def current_sched_free_cpus(row: dict) -> int:
            pestat = pestat_by_node.get(str(row.get("node_name") or ""))
            if not pestat:
                return int(row.get("cpus") or 0)
            if pestat.state not in {"idle", "mix"}:
                return 0
            return pestat.sched_free_cpus

        rows = [
            row
            for row in self.db.list_node_inventory()
            if normalize_gpu_model(str(row.get("gpu_model") or "")) in target_models
            and int(row.get("gpu_count") or 0) >= int(requested_gpus or 0)
            and int(row.get("cpus") or 0) >= minimum_cpus
            and (
                not require_current_fit
                or (
                    max(0, int(row.get("gpu_count") or 0) - int(row.get("gpu_used_count") or 0)) >= int(requested_gpus or 0)
                    and current_sched_free_cpus(row) >= minimum_cpus
                )
            )
        ]
        ranked = partition_rank(rows, needs_gpu=True)
        partitions = [str(item["partition"]) for item in ranked]
        if not allow_multi:
            return partitions[:1]
        return sorted(partitions, key=self.partition_sort_key)

    def cpu_pool_spread(self, cpus: int, memory_mb: int, requested_cpus: int = 0) -> tuple[list[str], int, int]:
        """Partitions (best CPU profile first) whose nodes can eventually serve
        a CPU pool, plus a CPU/memory request every listed partition can grant.
        Total node capacity is used, not current free capacity — the point of a
        spread submission is to wait in several queues at once, so the pool is
        sized down to what the smallest listed node type can offer."""
        floor = max(1, int(requested_cpus or 0), int(cpus) // 2)
        best_by_partition: dict[str, dict] = {}
        for row in self.db.list_node_inventory():
            partition = str(row.get("partition") or "")
            if not partition or self.is_single_job_partition(partition):
                continue
            if str(row.get("state") or "").lower() not in {"idle", "mix", "mixed"}:
                continue
            is_gpu = partition.startswith("gpu") or int(row.get("gpu_count") or 0) > 0
            if is_gpu and not self.cpu_pool_allow_gpu_partitions:
                continue
            reserve = self.gpu_cpu_reserve if is_gpu else 0
            capacity = int(row.get("cpus") or 0) - reserve
            if capacity < floor:
                continue
            entry = best_by_partition.setdefault(
                partition, {"cpu_score": 0, "capacity": 0, "memory_mb": 0, "is_cpu_only": not is_gpu}
            )
            entry["cpu_score"] = max(entry["cpu_score"], int(row.get("cpu_score") or 0))
            entry["capacity"] = max(entry["capacity"], capacity)
            entry["memory_mb"] = max(entry["memory_mb"], int(row.get("memory_mb") or 0))
        if not best_by_partition:
            return [], cpus, memory_mb
        spread_cpus = max(floor, min(int(cpus), min(entry["capacity"] for entry in best_by_partition.values())))
        # Ask only for memory the smallest listed node type can grant, so no
        # partition in the list is unable to start the job.
        grantable_memory = min(entry["memory_mb"] for entry in best_by_partition.values())
        spread_memory_mb = max(1024, min(memory_mb, int(grantable_memory * 0.9)))
        ordered = sorted(
            best_by_partition.items(),
            key=lambda item: (item[1]["cpu_score"], item[1]["is_cpu_only"], item[1]["capacity"]),
            reverse=True,
        )
        return [partition for partition, _entry in ordered], spread_cpus, spread_memory_mb

    def choose_allocation_shape(
        self,
        resource_pool: str = "cpu",
        gpu_model: str = "",
        gpus: int = 0,
        exclusive_node: bool = False,
        requested_cpus: int = 0,
        requested_memory_mb: int = 0,
        require_fea_eligible_node: bool = False,
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
        wants_shared_cpu_pool = not wants_gpu and not exclusive_node
        dynamic_warm_gpu_count = wants_gpu and resource_pool.startswith("gpu:") and int(gpus or 0) <= 0
        target_gpu_count = max(1, int(gpus or self.gpu_prewarm_gpus_per_allocation))
        minimum_gpu_count = max(1, self.gpu_prewarm_min_gpus_per_allocation) if dynamic_warm_gpu_count else max(1, int(gpus or 1))
        target_models = gpu_model_candidates(gpu_model)
        target_partition = self.gpu_prewarm_partition if wants_gpu else self.allocation_partition
        preferred_full_gpu_partitions = (
            self.preferred_full_gpu_partitions(
                ",".join(target_models) if target_models else gpu_model,
                target_gpu_count,
                allow_multi=dynamic_warm_gpu_count,
                require_current_fit=not dynamic_warm_gpu_count,
            )
            if wants_gpu and target_partition == "auto"
            else []
        )
        preferred_full_gpu_partition_set = set(preferred_full_gpu_partitions)
        occupied_by_partition: dict[str, set[str]] = {}
        reserved_nodes = self.reserved_allocation_nodes()
        for node in nodes:
            node_free_cpus_for_shape = node.sched_free_cpus if wants_gpu else node.effective_free_cpus
            if node.state not in {"idle", "mix"} or node_free_cpus_for_shape <= 0:
                continue
            if require_fea_eligible_node and not self.fea_allocation_accepts_task({"node_name": node.hostname}):
                continue
            if not wants_gpu and self.cpu_partition_allocation_limit_reached(node.partition, node.hostname):
                continue
            if preferred_full_gpu_partition_set and node.partition not in preferred_full_gpu_partition_set:
                continue
            if self.is_single_job_partition(node.partition):
                if exclusive_node and not wants_gpu and self.partition_has_live_allocation(node.partition, resource_pool="cpu"):
                    continue
                if not wants_shared_cpu_pool:
                    occupied = occupied_by_partition.setdefault(
                        node.partition,
                        self.occupied_single_job_nodes(node.partition, include_queued_jobs=True),
                    )
                    if node.hostname in occupied:
                        continue
                if exclusive_node and int(node.cpu_used) > 0:
                    continue
            inventory = inventory_by_node.get(node.hostname, {})
            node_gpu_count = int(inventory.get("gpu_count") or 0)
            node_gpu_used = int(inventory.get("gpu_used_count") or 0)
            node_gpu_model = normalize_gpu_model(str(inventory.get("gpu_model") or ""))
            node_is_gpu_partition = node.partition.startswith("gpu") or node_gpu_count > 0
            pins_to_node = dynamic_warm_gpu_count or (wants_shared_cpu_pool and node_is_gpu_partition)
            if pins_to_node and node.hostname in reserved_nodes:
                continue
            if pins_to_node and self.allocation_node_in_backoff(resource_pool, node.hostname):
                continue
            if wants_gpu:
                if target_partition != "auto" and not self.partition_spec_allows(target_partition, node.partition):
                    continue
                if target_models and node_gpu_model not in target_models:
                    continue
                if node_gpu_count <= 0:
                    continue
                if max(0, node_gpu_count - node_gpu_used) < minimum_gpu_count:
                    continue
            else:
                if target_partition != "auto" and not self.partition_spec_allows(target_partition, node.partition):
                    continue
                if target_partition == "auto" and node_gpu_count > 0 and not self.cpu_pool_allow_gpu_partitions:
                    continue
            if target_partition != "auto" and not self.partition_spec_allows(target_partition, node.partition):
                continue
            gpu_free = max(0, node_gpu_count - node_gpu_used)
            requested_gpus = min(target_gpu_count, gpu_free) if wants_gpu else 0
            leaves_unclaimed_gpus = wants_gpu and gpu_free > requested_gpus
            reserve = self.gpu_cpu_reserve if node.partition.startswith("gpu") and (not wants_gpu or leaves_unclaimed_gpus) else 0
            available_cpus = node_free_cpus_for_shape - reserve
            if wants_gpu and available_cpus <= 0 and node_free_cpus_for_shape > 0:
                available_cpus = node_free_cpus_for_shape
            if available_cpus <= 0:
                continue
            gpu_cpu_floor = 0 if dynamic_warm_gpu_count and int(requested_cpus or 0) > 0 else (
                self.minimum_cpus_for_gpu_allocation(node_gpu_model, requested_gpus) if wants_gpu else 0
            )
            minimum_cpus = max(
                int(requested_cpus or 0),
                gpu_cpu_floor,
            )
            if minimum_cpus and available_cpus < minimum_cpus:
                continue
            if wants_shared_cpu_pool:
                cpu_capacity = max(1, int(node.cpu_total) - reserve)
                requested_cpu_floor = int(requested_cpus or 0)
                if requested_cpu_floor and cpu_capacity < requested_cpu_floor:
                    continue
                if node_is_gpu_partition:
                    minimum_pool_cpus = max(1, requested_cpu_floor)
                    if available_cpus < minimum_pool_cpus:
                        continue
                    cpus = available_cpus
                else:
                    target_pool_cpus = min(max(1, int(self.allocation_cpus or cpu_capacity)), cpu_capacity)
                    minimum_pool_cpus = max(requested_cpu_floor, target_pool_cpus)
                    if available_cpus < minimum_pool_cpus:
                        continue
                    cpus = minimum_pool_cpus
            elif wants_gpu or exclusive_node:
                cpus = minimum_cpus or self.allocation_cpus or available_cpus
            else:
                cpus = self.allocation_cpus or available_cpus
            cpus = max(1, min(cpus, available_cpus))
            if requested_memory_mb and node.free_memory_mb < requested_memory_mb:
                continue
            memory_mb = requested_memory_mb or self._memory_mb(self.allocation_memory) or node.free_memory_mb
            memory_mb = max(1024, min(memory_mb, node.free_memory_mb))
            if cpus > 0 and memory_mb > 0:
                cpu_profile = CPU_PROFILES_BY_PARTITION.get(node.partition, {})
                cpu_score = int(inventory.get("cpu_score") or cpu_profile.get("cpu_score") or 0)
                score = GPU_PRIORITY.get(node_gpu_model, 0) if wants_gpu else cpu_score
                candidates.append((node, cpus, memory_mb, node_gpu_model, gpu_free, score, cpu_score, node_is_gpu_partition))
        if candidates:
            fit_count_by_partition: dict[str, int] = {}
            for candidate in candidates:
                fit_count_by_partition[candidate[0].partition] = fit_count_by_partition.get(candidate[0].partition, 0) + 1
            if wants_gpu:
                candidates.sort(
                    key=lambda item: (
                        fit_count_by_partition.get(item[0].partition, 0),
                        item[4],
                        item[1],
                        item[2],
                        item[5],
                        item[0].sched_free_cpus,
                    ),
                    reverse=True,
                )
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
                candidates.sort(
                    key=lambda item: (
                        1 if not item[7] else 0,
                        fit_count_by_partition.get(item[0].partition, 0) if item[7] else item[6],
                        item[0].effective_free_cpus,
                        item[1],
                        item[2],
                    ),
                    reverse=True,
                )
            for candidate in candidates:
                node, cpus, memory_mb, chosen_gpu_model, gpu_free, _score, _cpu_score, _node_is_gpu_partition = candidate
                chosen_gpus = min(target_gpu_count, gpu_free) if wants_gpu else 0
                node_name = node.hostname
                if (
                    not wants_gpu
                    and not self.is_single_job_partition(node.partition)
                    and not _node_is_gpu_partition
                    and not require_fea_eligible_node
                ):
                    node_name = ""
                single_partition_shape = {
                    "partition": node.partition,
                    "node_name": node_name,
                    "cpus": cpus,
                    "memory_mb": memory_mb,
                    "gpus": chosen_gpus,
                    "gpu_model": chosen_gpu_model if wants_gpu else "",
                    "exclusive_node": exclusive_node,
                }
                shape = single_partition_shape
                used_partition_spread = False
                if (
                    wants_shared_cpu_pool
                    and _node_is_gpu_partition
                    and self.cpu_pool_partition_spread
                    and target_partition == "auto"
                    and not require_fea_eligible_node
                ):
                    spread, spread_cpus, spread_memory_mb = self.cpu_pool_spread(
                        cpus, memory_mb, requested_cpus=int(requested_cpus or 0)
                    )
                    if len(spread) > 1:
                        # Every listed partition can eventually serve this shape,
                        # so submit unpinned and let Slurm start it where room opens.
                        shape = {
                            "partition": ",".join(spread),
                            "node_name": "",
                            "cpus": spread_cpus,
                            "memory_mb": spread_memory_mb,
                            "gpus": 0,
                            "gpu_model": "",
                            "exclusive_node": exclusive_node,
                        }
                        used_partition_spread = True
                if self.allocation_shape_in_backoff(resource_pool, shape):
                    if used_partition_spread and not self.allocation_shape_in_backoff(
                        resource_pool, single_partition_shape
                    ):
                        return single_partition_shape
                    continue
                return shape
            return None
        if require_fea_eligible_node and nodes:
            return None
        if target_partition != "auto" and self.is_single_job_partition(target_partition):
            return None
        partition_request = {
            "gpus": target_gpu_count if wants_gpu else 0,
            "gpu_model": ",".join(target_models) if target_models else gpu_model,
        }
        preferred_full_gpu_partition = ",".join(preferred_full_gpu_partitions)
        partition = target_partition if target_partition != "auto" else preferred_full_gpu_partition or self.choose_partition(partition_request)
        if not partition:
            return None
        if not wants_gpu:
            partition_names = self.partition_spec_names(partition)
            if partition_names and all(self.cpu_partition_allocation_partition_saturated(item, nodes) for item in partition_names):
                return None
        if self.is_single_job_partition(partition):
            return None
        fallback_cpu_limit = 0
        fallback_cpu_capacity = 0
        fallback_memory_capacity = 0
        fallback_partition_can_fit_gpus = not wants_gpu
        if wants_gpu:
            requested_gpu_count = target_gpu_count
            for node in nodes:
                if not self.partition_spec_allows(partition, node.partition):
                    continue
                inventory = inventory_by_node.get(node.hostname, {})
                node_gpu_model = normalize_gpu_model(str(inventory.get("gpu_model") or ""))
                if target_models and node_gpu_model and node_gpu_model not in target_models:
                    continue
                node_gpu_count = int(inventory.get("gpu_count") or 0)
                node_gpu_used = int(inventory.get("gpu_used_count") or 0)
                if node_gpu_count > 0 and node_gpu_count < requested_gpu_count:
                    continue
                fallback_partition_can_fit_gpus = True
                reserve = self.gpu_cpu_reserve if node_gpu_count > requested_gpu_count else 0
                fallback_cpu_capacity = max(fallback_cpu_capacity, max(1, int(node.cpu_total) - reserve))
                fallback_memory_capacity = max(fallback_memory_capacity, int(node.memory_mb))
                if node_gpu_count > 0 and max(0, node_gpu_count - node_gpu_used) < requested_gpu_count:
                    continue
                fallback_cpu_limit = max(fallback_cpu_limit, max(1, int(node.sched_free_cpus) - reserve))
            if not fallback_partition_can_fit_gpus:
                return None
        else:
            for node in nodes:
                if not self.partition_spec_allows(partition, node.partition):
                    continue
                inventory = inventory_by_node.get(node.hostname, {})
                node_gpu_count = int(inventory.get("gpu_count") or 0)
                node_is_gpu_partition = node.partition.startswith("gpu") or node_gpu_count > 0
                reserve = self.gpu_cpu_reserve if node_is_gpu_partition else 0
                fallback_cpu_capacity = max(fallback_cpu_capacity, max(1, int(node.cpu_total) - reserve))
                fallback_memory_capacity = max(fallback_memory_capacity, int(node.memory_mb))
                fallback_cpu_limit = max(fallback_cpu_limit, max(1, int(node.effective_free_cpus) - reserve))
        fallback_gpu_model = (target_models[0] if target_models else "") if wants_gpu else ""
        fallback_gpu_cpu_floor = 0 if dynamic_warm_gpu_count and int(requested_cpus or 0) > 0 else (
            self.minimum_cpus_for_gpu_allocation(fallback_gpu_model, target_gpu_count) if wants_gpu else 0
        )
        fallback_minimum_cpus = max(
            int(requested_cpus or 0),
            fallback_gpu_cpu_floor,
        )
        if wants_shared_cpu_pool and fallback_cpu_capacity:
            fallback_target_cpus = min(
                fallback_cpu_capacity,
                max(1, int(self.allocation_cpus or fallback_cpu_capacity)),
            )
            fallback_cpus = max(fallback_target_cpus, fallback_minimum_cpus or 0)
        else:
            fallback_cpus = fallback_minimum_cpus or self.allocation_cpus or 4
        if fallback_cpu_capacity and fallback_minimum_cpus and fallback_cpu_capacity < fallback_minimum_cpus:
            return None
        if requested_memory_mb and fallback_memory_capacity and fallback_memory_capacity < requested_memory_mb:
            return None
        if fallback_cpu_limit and not wants_shared_cpu_pool:
            if not fallback_minimum_cpus or fallback_cpu_limit >= fallback_minimum_cpus:
                fallback_cpus = min(fallback_cpus, fallback_cpu_limit)
        fallback_shape = {
            "partition": partition,
            "node_name": "",
            "cpus": fallback_cpus,
            "memory_mb": requested_memory_mb or self._memory_mb(self.allocation_memory) or fallback_memory_capacity or 16384,
            "gpus": target_gpu_count if wants_gpu else 0,
            "gpu_model": (target_models[0] if target_models else "") if wants_gpu else "",
            "exclusive_node": exclusive_node,
        }
        if self.allocation_shape_in_backoff(resource_pool, fallback_shape):
            return None
        return fallback_shape

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

        def empty_summary(model: str) -> dict:
            return {
                "gpu_model": model,
                "cluster_total_gpus": 0,
                "cluster_used_gpus": 0,
                "cluster_free_gpus": 0,
                "scheduler_owned_gpus": 0,
                "scheduler_free_gpus": 0,
                "single_node_max_free_gpus": 0,
                "single_node_max_free_cpus": 0,
                "single_node_max_free_memory_mb": 0,
                "single_node_max_free_gpu_node": "",
                "single_node_max_free_gpu_partition": "",
                "nodes": 0,
                "available_nodes": 0,
                "pending_gpu_tasks": 0,
                "pending_gpu_jobs": 0,
                "score": GPU_PRIORITY.get(model, 0),
            }

        pestat_by_node = {
            row["hostname"]: PestatNode(
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
        }
        for row in self.db.list_node_inventory():
            model = normalize_gpu_model(str(row.get("gpu_model") or ""))
            if not model:
                continue
            total = int(row.get("gpu_count") or 0)
            used = min(total, max(0, int(row.get("gpu_used_count") or 0)))
            state = str(row.get("state") or "").lower()
            available_state = state in {"idle", "mix", "mixed"}
            item = summaries.setdefault(model, empty_summary(model))
            item["nodes"] += 1
            item["cluster_total_gpus"] += total
            item["cluster_used_gpus"] += used
            if available_state:
                free_gpus = max(0, total - used)
                item["available_nodes"] += 1
                item["cluster_free_gpus"] += free_gpus
                node_name = str(row.get("node_name") or "")
                pestat = pestat_by_node.get(node_name)
                scheduler_usable = pestat.state in {"idle", "mix"} and pestat.sched_free_cpus > 0 if pestat else available_state
                if free_gpus > 0 and scheduler_usable:
                    free_cpus = max(0, int(pestat.sched_free_cpus if pestat else int(row.get("cpus") or 0)))
                    free_memory_mb = max(0, int(pestat.free_memory_mb if pestat else int(row.get("memory_mb") or 0)))
                    if (
                        free_gpus > int(item.get("single_node_max_free_gpus") or 0)
                        or (
                            free_gpus == int(item.get("single_node_max_free_gpus") or 0)
                            and free_cpus > int(item.get("single_node_max_free_cpus") or 0)
                        )
                    ):
                        item["single_node_max_free_gpus"] = free_gpus
                        item["single_node_max_free_cpus"] = free_cpus
                        item["single_node_max_free_memory_mb"] = free_memory_mb
                        item["single_node_max_free_gpu_node"] = node_name
                        item["single_node_max_free_gpu_partition"] = str(row.get("partition") or "")
        for allocation in self.db.list_allocations_with_live(limit=500):
            if allocation["state"] not in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }:
                continue
            model = normalize_gpu_model(str(allocation.get("gpu_model") or ""))
            if not model:
                continue
            item = summaries.setdefault(model, empty_summary(model))
            item["scheduler_owned_gpus"] += int(allocation.get("total_gpus") or 0)
            item["scheduler_free_gpus"] += int(allocation.get("free_gpus") or 0)
        for task in self.db.list_tasks(limit=5000):
            if task["status"] != TaskStatus.QUEUED.value or int(task.get("gpus") or 0) <= 0:
                continue
            model = normalize_gpu_model(str(task.get("gpu_model") or "")) or "unspecified"
            item = summaries.setdefault(model, empty_summary(model))
            item["pending_gpu_tasks"] += int(task.get("gpus") or 0)
        for job in self.db.list_jobs(limit=5000):
            if job["status"] != JobStatus.QUEUED.value or int(job.get("gpus") or 0) <= 0:
                continue
            model = normalize_gpu_model(str(job.get("gpu_model") or "")) or "unspecified"
            item = summaries.setdefault(model, empty_summary(model))
            item["pending_gpu_jobs"] += int(job.get("gpus") or 0)
        return sorted(summaries.values(), key=lambda item: (item["score"], item["cluster_free_gpus"]), reverse=True)

    def snapshots(self) -> list[AccountSnapshot]:
        now = time.time()
        if self._snapshot_cache and now - self._snapshot_cache[0] < self.poll_interval_seconds:
            return self._snapshot_cache[1]
        accounts_by_name = {account.name: account for account in self.accounts}

        def probe(account_name: str, _items: list) -> AccountSnapshot:
            account = accounts_by_name[account_name]
            client = self._client(account)
            storage_used = self.cached_storage(account, client, now)
            return client.snapshot(storage_used_gb=storage_used)

        outcomes = self._fan_out_by_account({account.name: [] for account in self.accounts}, probe)
        snapshots = []
        for account in self.accounts:
            outcome = outcomes.get(account.name)
            if isinstance(outcome, Exception) or outcome is None:
                LOGGER.warning("failed to refresh account snapshot for %s: %s", account.name, outcome)
                continue
            snapshots.append(outcome)
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

    def cached_storage_quota(
        self, account: AccountConfig, client: SlurmAccountClient, now: float
    ) -> StorageQuotaProbe:
        cached = self._storage_quota_cache.get(account.name)
        if cached and now - cached[0] < self._storage_quota_refresh_interval_seconds:
            return cached[1]
        probe_method = getattr(client, "storage_quota_probe", None)
        if not callable(probe_method):
            value = StorageQuotaProbe(filesystem_type="unsupported")
        else:
            try:
                value = probe_method()
            except Exception as exc:
                # FEA placement treats a real probe failure as blocked. Do not
                # retain an old successful reading and silently fail open.
                value = StorageQuotaProbe(filesystem_type="", error=str(exc) or type(exc).__name__)
        self._storage_quota_cache[account.name] = (now, value)
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
        job = self.apply_dynamic_env_profile(job, account)
        try:
            result = self._client(account).submit(job)
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
        if requested_models and rows:
            rows = [row for row in rows if normalize_gpu_model(str(row.get("gpu_model") or "")) in requested_models]
            if not rows:
                return ""
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
        by_account: dict[str, list[dict]] = {}
        for job in self.db.list_jobs(limit=500):
            if job["status"] not in {JobStatus.SUBMITTED.value, JobStatus.RUNNING.value}:
                continue
            if not job["account_name"] or not job["slurm_job_id"]:
                continue
            if job["account_name"] not in accounts_by_name:
                continue
            by_account.setdefault(job["account_name"], []).append(job)
        outcomes = self._fan_out_by_account(
            by_account,
            lambda account_name, jobs: self._job_states(
                self._client(accounts_by_name[account_name]),
                [str(job["slurm_job_id"]) for job in jobs],
            ),
        )
        for account_name, outcome in outcomes.items():
            if isinstance(outcome, Exception):
                LOGGER.warning(
                    "failed to refresh %d jobs on %s: %s", len(by_account[account_name]), account_name, outcome
                )
                self._mark_account_failed_this_tick(account_name)
                continue
            for job in by_account[account_name]:
                info = outcome.get(str(job["slurm_job_id"]))
                if info is None:
                    continue
                updates = {"status": info.status.value}
                if info.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}:
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
        self._client(account).cancel(job["slurm_job_id"])
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
                self._client(account).cancel_task(task, self._task_allocation_job_id(task))
            except Exception as exc:
                LOGGER.warning("failed to cancel task %s remotely: %s", task_id, exc)
        self.db.update_task(task_id, status=TaskStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
        self.on_task_terminal(task, "cancelled")
        if task.get("allocation_id"):
            self.recalculate_allocation_capacity()

    def request_cancel_task(self, task_id: int, expected_statuses: set[str] | None = None) -> dict:
        cancellable = {
            TaskStatus.QUEUED.value,
            TaskStatus.ATTACHING.value,
            TaskStatus.RUNNING.value,
        }
        wanted_statuses = set(expected_statuses) if expected_statuses is not None else cancellable
        for _ in range(4):
            task = self.db.get_task(task_id)
            if not task:
                raise ValueError("task not found")
            previous_status = str(task["status"])
            if previous_status not in cancellable or previous_status not in wanted_statuses:
                return {
                    "ok": True,
                    "cancelled": False,
                    "id": task_id,
                    "previous_status": previous_status,
                    "status": previous_status,
                    "reason": "status_mismatch",
                }
            # Compare against the exact row observed above. If queued became
            # running meanwhile, a queued-only caller must not kill it. A
            # broader caller retries from a fresh row so remote identifiers
            # match the state whose cancellation it wins.
            if not self.db.update_task_if_status(
                task_id,
                [previous_status],
                status=TaskStatus.CANCELLED.value,
                finished_at="CURRENT_TIMESTAMP",
            ):
                continue
            self.on_task_terminal(task, "cancelled")
            if task.get("allocation_id"):
                self.recalculate_allocation_capacity()
            if previous_status in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                thread = threading.Thread(target=self._cancel_task_remote_best_effort, args=(task,), daemon=True)
                thread.start()
            return {
                "ok": True,
                "cancelled": True,
                "id": task_id,
                "previous_status": previous_status,
                "status": TaskStatus.CANCELLED.value,
            }
        task = self.db.get_task(task_id)
        if not task:
            raise ValueError("task not found")
        current_status = str(task["status"])
        return {
            "ok": True,
            "cancelled": False,
            "id": task_id,
            "previous_status": current_status,
            "status": current_status,
            "reason": "concurrent_transition",
        }

    def _cancel_task_remote_best_effort(self, task: dict) -> None:
        account = self.account_by_name(str(task.get("account_name") or ""))
        if not account:
            return
        try:
            self._client(account).cancel_task(task, self._task_allocation_job_id(task))
        except Exception as exc:
            LOGGER.warning("failed to cancel task %s remotely: %s", task.get("id"), exc)

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
            result = self.request_cancel_task(int(task["id"]), expected_statuses=wanted_statuses)
            if result["cancelled"]:
                cancelled.append(int(task["id"]))
        return sorted(cancelled)
