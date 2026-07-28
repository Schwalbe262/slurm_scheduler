from __future__ import annotations

import fnmatch
import json
import logging
import os
import posixpath
import re
import shlex
import sys
import threading
import time
import traceback
import uuid
from bisect import bisect_right
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait as wait_futures
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .config import AccountConfig
from .db import Database
from .inventory import CPU_PROFILES_BY_PARTITION, GPU_PRIORITY, gpu_model_candidates, normalize_gpu_model, parse_scontrol_nodes, parse_sinfo_nodes, partition_rank
from .models import (
    AedtBackend,
    AccountSnapshot,
    AllocationStatus,
    JobStatus,
    NodeNamePolicy,
    SchedulingProfile,
    TaskStatus,
    normalize_aedt_backend,
    normalize_node_name_policy,
    normalize_scheduling_profile,
)
from .pestat import PestatNode, parse_pestat
from .retention import (
    WORKSPACE_PRUNE_PROTECTION_MARKER,
    WORKSPACE_PRUNE_PROTECTION_MAX_BYTES,
    parse_workspace_prune_protection_manifest,
    workspace_prune_protection_marker_paths,
)
from .slurm import (
    AllocationSubmissionNotCreated,
    JobStateInfo,
    RemoteExecutionError,
    SSHSession,
    SlurmAccountClient,
    StorageQuotaProbe,
    TaskProbe,
    resolve_task_placeholders,
    shell_path,
    workspace_runs_dir,
)

LOGGER = logging.getLogger(__name__)
ClientFactory = Callable[[AccountConfig], SlurmAccountClient]
FEA_PROJECT_CURSOR_SETTING = "fea_project_last_claim_by_priority"
FEA_HARD_PRESSURE_RECLAIM_SAMPLE_SETTING_PREFIX = (
    "observation_claim:fea_memory_pressure_reclaim:"
)
FEA_HARD_PRESSURE_EPISODE_COOLDOWN_SECONDS = 300
TASK_COUNT_SAMPLE_INTERVAL_SECONDS = 60
TERMINAL_AEDT_WORKSPACE_SWEEP_INTERVAL_SECONDS = 60
TERMINAL_AEDT_WORKSPACE_RETRY_SECONDS = 60
TERMINAL_AEDT_WORKSPACE_CLAIM_STALE_SECONDS = 600
TERMINAL_AEDT_WORKSPACE_SCAN_LIMIT = 1000
TERMINAL_AEDT_WORKSPACE_SUBMIT_LIMIT = 128
TERMINAL_AEDT_WORKSPACE_DELETED_MARKER = "SLURM_AEDT_WORKSPACE_DELETED"
TERMINAL_AEDT_WORKSPACE_ABSENT_MARKER = "SLURM_AEDT_WORKSPACE_ABSENT"
ALLOCATION_SUBMISSION_RESERVED = "scheduler allocation submission reserved"
ALLOCATION_SUBMISSION_IN_PROGRESS = "scheduler allocation submission in progress"
ALLOCATION_SUBMISSION_RECOVERY_GRACE_SECONDS = 120
STRICT_NODE_CANCELLATION_PENDING_PREFIX = (
    "strict node placement cancellation pending: "
)
_STDERR_FAILURE_SIGNAL_RE = re.compile(
    r"(?:^|[\s\[])(?:error|critical|fatal)(?=[:\]\s-])"
    r"|(?:^|\s)(?:[A-Za-z_][\w.]*?(?:Error|Exception)):",
    re.IGNORECASE,
)
_STDERR_WORKLOAD_FAILURE_SIGNAL_RE = re.compile(
    r"^(?:error|critical|fatal)(?=[:\s-])"
    r"|^(?:[A-Za-z_][\w.]*?(?:Error|Exception)):",
    re.IGNORECASE,
)
_RESULT_JSON_PREFIX = "RESULT_JSON "
_RESULT_FAILURE_KEYS = (
    "failure_message",
    "failure_reason",
    "error",
    "error_message",
)
_RESULT_FAILURE_KEY_SUFFIXES = (
    "_failure_reason",
    "_error_message",
)


def _stderr_failure_signal_score(line: str) -> int:
    if _STDERR_WORKLOAD_FAILURE_SIGNAL_RE.search(line):
        return 2
    if _STDERR_FAILURE_SIGNAL_RE.search(line):
        return 1
    return 0

# Only fields that can change queued-task placement or fit belong in the
# reservation certificate.  Allocation/task heartbeat writers update
# ``updated_at`` and ``last_active_at`` continuously; including those
# operational timestamps made every long queue scan look stale even when its
# capacity inputs had not changed.
_RESERVATION_ALLOCATION_FIT_FIELDS = (
    "state",
    "account_name",
    "partition",
    "node_name",
    "total_cpus",
    "free_cpus",
    "total_memory_mb",
    "free_memory_mb",
    "total_gpus",
    "free_gpus",
    "gpu_model",
    "resource_pool",
    "exclusive_node",
    "drain_reason",
)
_RESERVATION_ACTIVE_TASK_FIT_FIELDS = (
    "status",
    "allocation_id",
    "cpus",
    "memory_mb",
    "gpus",
    "scheduling_profile",
    "aedt_backend",
    "project",
    "exclusive_node",
)
TASK_TIMEOUT_CANCEL_MAX_PER_TICK = 8


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


@dataclass
class _QueuedTaskAllocationReservationStep:
    task_id: int
    allocation_id: int | None
    effective_task: dict | None


@dataclass
class _QueuedTaskAllocationReservationPlan:
    """Read-only certificate produced by one fit-aware queue scan.

    The scale-in path needs the allocation -> task reservations, while the
    immediately following scale-out path only needs to know whether every
    standalone demand task already has in-flight capacity.  Keeping the
    certificate local to ``maintain_allocation_pool`` avoids a second identical
    O(queued tasks * allocations) scan without carrying stale scheduling state
    across ticks.
    """

    reservations: dict[int, list[int]]
    reserved_task_ids: frozenset[int]
    task_signatures_by_id: dict[int, str]
    planned_demand_task_ids: tuple[int, ...]
    allocation_signature: tuple[tuple[Any, ...], ...]
    fit_state_signature: tuple[Any, ...]
    steps: tuple[_QueuedTaskAllocationReservationStep, ...]
    changed_while_built: tuple[str, ...]
    reusable: bool


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
        fea_baseline_max_attach_per_loop: int = 64,
        fea_node_name_policy: str = "preferred",
        fea_overload_scale_out_load_factor: float = 2.0,
        fea_overload_scale_out_seconds: int = 300,
        fea_pressure_max_attempts: int = 3,
        fea_max_attach_per_node_per_loop: int = 12,
        fea_node_requested_cpu_factor: float = 1.0,
        fea_footprint_maturity_seconds: int = 900,
        fea_cpu_footprint_maturity_seconds: int = 120,
        fea_alloc_util_enabled: bool = True,
        fea_alloc_util_target: float = 0.85,
        fea_alloc_util_sample_interval_seconds: int = 60,
        fea_shared_memory_estimate_fraction: float = 0.25,
        fea_shared_memory_min_estimate_mb: int = 8192,
        fea_adaptive_memory_relax_enabled: bool = True,
        fea_adaptive_memory_window_seconds: int = 3600,
        fea_adaptive_memory_min_coverage_seconds: int = 2700,
        fea_adaptive_memory_margin_percent: float = 5.0,
        fea_adaptive_memory_max_attach_per_tick: int = 1,
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
        aedt_pool_terminal_workspace_root: str = "/gpfs/tmp_cpu2/aedt_pool",
        aedt_storage_reservation_per_project_gb: float = 4.0,
        aedt_storage_reservation_maturity_seconds: int = 900,
        license_monitor_enabled: bool = False,
        license_monitor_account: str = "",
        license_monitor_lmutil_path: str = "",
        license_monitor_license_server: str = "",
        license_monitor_interval_seconds: int = 60,
        license_monitor_watch_features: list[str] | None = None,
        license_monitor_display: dict[str, str] | None = None,
        license_admission_enabled: bool = False,
        license_admission_snapshot_max_age_seconds: int = 120,
        license_admission_settlement_seconds: int = 300,
        license_admission_reserve_by_feature: dict[str, int] | None = None,
        license_admission_persistent_cost_by_project: dict[str, dict[str, int]] | None = None,
        license_admission_unknown_fea_project_policy: str = "block",
        cleanup_db_row_ttl_seconds: int = 1209600,
        cleanup_event_ttl_seconds: int = 604800,
        watchdog_enabled: bool = True,
        watchdog_stall_seconds: int = 0,
        ssh_parallelism: int = 4,
        license_admission_reserve_exempt_projects: list[str] | None = None,
        standalone_aedt_max_running_by_project: dict[str, int] | None = None,
        standalone_aedt_running_lanes: dict[str, dict[str, Any]] | None = None,
    ):
        self.db = db
        self.accounts = accounts
        self.poll_interval_seconds = poll_interval_seconds
        self.client_factory = client_factory
        self._aedt_backend_admission_checker: Callable[[dict], tuple[bool, str]] | None = None
        self._aedt_backend_task_preparer: Callable[[dict], tuple[bool, str]] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_cache: tuple[float, list[AccountSnapshot]] | None = None
        self._storage_cache: dict[str, tuple[float, float | None]] = {}
        self._storage_quota_cache: dict[str, tuple[float, StorageQuotaProbe]] = {}
        self._storage_refresh_interval_seconds = max(900, poll_interval_seconds * 20)
        self._storage_quota_refresh_interval_seconds = max(10, min(30, poll_interval_seconds))
        self.cluster_refresh_interval_seconds = cluster_refresh_interval_seconds
        self._last_cluster_refresh_at = 0.0
        self._last_task_count_sample_at = 0.0
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
        self.fea_baseline_max_attach_per_loop = max(
            1, int(fea_baseline_max_attach_per_loop)
        )
        self._fea_last_attach_baseline = False
        node_policy = (fea_node_name_policy or "preferred").strip().lower()
        self.fea_node_name_policy = node_policy if node_policy in {"preferred", "strict"} else "preferred"
        self.fea_overload_scale_out_load_factor = max(0.0, float(fea_overload_scale_out_load_factor))
        self.fea_overload_scale_out_seconds = max(0, int(fea_overload_scale_out_seconds))
        self.fea_pressure_max_attempts = max(1, int(fea_pressure_max_attempts))
        self.fea_max_attach_per_node_per_loop = max(1, int(fea_max_attach_per_node_per_loop))
        # FEA may ramp from the physical-core baseline, but never beyond 2x
        # the CPUs owned by the allocation.
        self.fea_node_requested_cpu_factor = min(2.0, max(1.0, float(fea_node_requested_cpu_factor)))
        self.fea_footprint_maturity_seconds = max(0, int(fea_footprint_maturity_seconds))
        self.fea_cpu_footprint_maturity_seconds = max(0, int(fea_cpu_footprint_maturity_seconds))
        self.fea_shared_memory_estimate_fraction = min(
            1.0, max(0.0, float(fea_shared_memory_estimate_fraction))
        )
        self.fea_shared_memory_min_estimate_mb = max(1, int(fea_shared_memory_min_estimate_mb))
        self.fea_adaptive_memory_relax_enabled = bool(fea_adaptive_memory_relax_enabled)
        self.fea_adaptive_memory_window_seconds = max(300, int(fea_adaptive_memory_window_seconds))
        self.fea_adaptive_memory_min_coverage_seconds = max(
            0, int(fea_adaptive_memory_min_coverage_seconds)
        )
        self.fea_adaptive_memory_margin_percent = max(0.0, float(fea_adaptive_memory_margin_percent))
        self.fea_adaptive_memory_max_attach_per_tick = max(
            1, int(fea_adaptive_memory_max_attach_per_tick)
        )
        standalone_caps = standalone_aedt_max_running_by_project or {}
        if not isinstance(standalone_caps, dict):
            raise ValueError(
                "standalone_aedt_max_running_by_project must be a mapping"
            )
        self.standalone_aedt_max_running_by_project: dict[str, int] = {}
        for raw_project, raw_limit in standalone_caps.items():
            project = str(raw_project).strip()
            if (
                not project
                or isinstance(raw_limit, bool)
                or not isinstance(raw_limit, int)
                or raw_limit <= 0
            ):
                raise ValueError(
                    "standalone_aedt_max_running_by_project entries require "
                    "a non-empty project and a positive integer limit"
                )
            self.standalone_aedt_max_running_by_project[project] = raw_limit
        standalone_lanes = standalone_aedt_running_lanes or {}
        if not isinstance(standalone_lanes, dict):
            raise ValueError("standalone_aedt_running_lanes must be a mapping")
        self.standalone_aedt_running_lanes: list[dict[str, Any]] = []
        lane_scopes: list[tuple[str, str, str, str]] = []
        for raw_lane_name, raw_lane in standalone_lanes.items():
            lane_name = str(raw_lane_name).strip()
            if not lane_name or not isinstance(raw_lane, dict):
                raise ValueError(
                    "standalone_aedt_running_lanes entries require a non-empty "
                    "lane name and a mapping"
                )
            project = str(raw_lane.get("project") or "").strip()
            name_prefix = str(raw_lane.get("name_prefix") or "").strip()
            backend = str(raw_lane.get("aedt_backend") or "standalone").strip().lower()
            raw_limit = raw_lane.get("max_running")
            if not project or not name_prefix or backend != AedtBackend.STANDALONE.value:
                raise ValueError(
                    f"standalone AEDT lane {lane_name!r} requires exact project, "
                    "name_prefix, and aedt_backend: standalone"
                )
            if (
                isinstance(raw_limit, bool)
                or not isinstance(raw_limit, int)
                or raw_limit <= 0
            ):
                raise ValueError(
                    f"standalone AEDT lane {lane_name!r} requires a positive "
                    "integer max_running"
                )
            for other_name, other_project, other_backend, other_prefix in lane_scopes:
                if (
                    project == other_project
                    and backend == other_backend
                    and (
                        name_prefix.startswith(other_prefix)
                        or other_prefix.startswith(name_prefix)
                    )
                ):
                    raise ValueError(
                        f"standalone AEDT lanes {other_name!r} and {lane_name!r} "
                        "have overlapping project/backend/name_prefix scopes"
                    )
            lane_scopes.append((lane_name, project, backend, name_prefix))
            self.standalone_aedt_running_lanes.append(
                {
                    "name": lane_name,
                    "project": project,
                    "name_prefix": name_prefix,
                    "aedt_backend": backend,
                    "max_running": raw_limit,
                }
            )
        # hostname -> deque[(monotonic, free_memory_mb)]: rolling pestat
        # observations backing the adaptive memory relax. In-memory only, so a
        # web-worker restart re-arms the coverage requirement before relaxing.
        self._node_memory_history: dict[str, deque[tuple[float, int]]] = {}
        # hostname -> deque[monotonic]: attaches admitted while the node's
        # memory admission came from the adaptive relax (spends its budget).
        self._adaptive_memory_attaches: dict[str, deque[float]] = {}
        self._tick_adaptive_memory_nodes: set[str] = set()
        self._fea_footprint_cache: tuple[int, dict[str, dict[str, float]]] | None = None
        self.fea_alloc_util_enabled = fea_alloc_util_enabled
        self.fea_alloc_util_target = min(1.0, max(0.1, float(fea_alloc_util_target)))
        self.fea_alloc_util_sample_interval_seconds = max(30, int(fea_alloc_util_sample_interval_seconds))
        # slurm_job_id -> (monotonic_time, {step_id: cumulative cpu seconds})
        self._alloc_cpu_samples: dict[str, tuple[float, dict[str, float]]] = {}
        # allocation_id -> {"busy_cores": float, "at": monotonic_time}
        self._alloc_cpu_util: dict[int, dict[str, float]] = {}
        self._last_alloc_util_at = 0.0
        self._tick_attach_workers_by_node: dict[str, int] = {}
        self._fea_worker_counts_cache: tuple[int, dict[str, int]] | None = None
        self._fea_pressures_cache: tuple[int, dict[str, dict[str, int]]] | None = None
        self._fea_alloc_pressures_cache: tuple[int, dict[int, dict[str, int]]] | None = None
        self._fea_node_resources_cache: tuple[int, dict[str, dict[str, int]]] | None = None
        self._pestat_nodes_cache: tuple[int, dict[str, dict]] | None = None
        self._fea_overload_since_by_node: dict[str, float] = {}
        self._fea_overload_scaled_nodes: set[str] = set()
        self.task_refresh_max_per_tick = max(1, int(task_refresh_max_per_tick))
        self._non_fea_task_refresh_cursor_id = 0
        self._fea_task_refresh_cursor_id = 0
        self._prefer_fea_for_single_task_refresh = False
        self._fea_project_last_claim = self.load_fea_project_claim_cursor()
        # Candidate selection and the queued -> attaching claim can also be
        # entered from a web request.  Serialize the final admission check so
        # two callers cannot both consume the same last FEA node-fit slot.
        self._task_assignment_lock = threading.RLock()
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
        self.aedt_pool_terminal_workspace_root = (
            posixpath.normpath(str(aedt_pool_terminal_workspace_root or "").strip())
            or "/gpfs/tmp_cpu2/aedt_pool"
        )
        self.aedt_storage_reservation_per_project_gb = max(
            0.0, float(aedt_storage_reservation_per_project_gb)
        )
        self.aedt_storage_reservation_maturity_seconds = max(
            0, int(aedt_storage_reservation_maturity_seconds)
        )
        # Candidate placement can evaluate hundreds of allocations for each
        # queued task.  Reuse the derived ledger briefly for those advisory
        # checks; the two mutating admission points always force a fresh read.
        self._aedt_storage_growth_cache: dict[
            str, tuple[float, dict[str, dict[str, int]]]
        ] = {}
        self._aedt_storage_growth_cache_seconds = 1.0
        self._storage_guard_warned_at: dict[str, float] = {}
        self.license_monitor_enabled = license_monitor_enabled
        self.license_monitor_account = license_monitor_account
        self.license_monitor_lmutil_path = license_monitor_lmutil_path
        self.license_monitor_license_server = license_monitor_license_server
        self.license_monitor_interval_seconds = max(60, int(license_monitor_interval_seconds))
        self.license_monitor_watch_features = list(license_monitor_watch_features or [])
        self.license_monitor_display = dict(license_monitor_display or {})
        self.license_admission_enabled = bool(license_admission_enabled)
        self.license_admission_snapshot_max_age_seconds = max(
            1, int(license_admission_snapshot_max_age_seconds)
        )
        self.license_admission_settlement_seconds = max(
            0, int(license_admission_settlement_seconds)
        )
        reserve_source = license_admission_reserve_by_feature or {}
        self.license_admission_reserve_by_feature = {
            str(feature): max(0, int(reserve))
            for feature, reserve in reserve_source.items()
            if str(feature).strip()
        }
        cost_source = license_admission_persistent_cost_by_project or {}
        self.license_admission_persistent_cost_by_project = {
            str(project): {
                str(feature): max(0, int(cost))
                for feature, cost in dict(costs or {}).items()
                if str(feature).strip() and int(cost) > 0
            }
            for project, costs in cost_source.items()
            if str(project).strip()
        }
        self.license_admission_reserve_exempt_projects = {
            str(project).strip()
            for project in (license_admission_reserve_exempt_projects or [])
            if str(project).strip()
        }
        unknown_policy = str(license_admission_unknown_fea_project_policy or "block").strip().lower()
        self.license_admission_unknown_fea_project_policy = (
            unknown_policy if unknown_policy in {"allow", "block"} else "block"
        )
        self._license_usage: dict = {}
        self._last_license_refresh_at = 0.0
        self._license_refresh_inflight = False
        self._license_snapshot_completed_monotonic: float | None = None
        self._license_snapshot_query_started_at: datetime | None = None
        self._license_last_successful_checked_at = ""
        self._license_admission_claims: dict[str, dict] = {}
        self._license_last_blocked_reason = ""
        self._reconstruct_license_admission_claims()
        self.reconcile_on_start = reconcile_on_start
        self._needs_reconcile = False
        self.backup_enabled = backup_enabled
        self.backup_interval_seconds = max(3600, int(backup_interval_seconds))
        self.backup_keep = max(1, int(backup_keep))
        self.backup_dir = backup_dir
        self._last_backup_at = 0.0
        self._backup_lock = threading.Lock()
        self._backup_inflight = False
        self._backup_thread: threading.Thread | None = None
        self._last_tick_completed_monotonic: float | None = None
        self._last_tick_completed_at: str = ""
        self._last_tick_duration: float | None = None
        self._last_tick_stage_seconds: dict[str, float] = {}
        self._last_demand_reservation_plan: dict[str, Any] = {}
        self._allocation_submission_recovery: dict[str, Any] = {
            "blocked": False,
            "blockers": [],
        }
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
        self._active_profile_sets_cache: tuple[set[int], set[int]] | None = None
        self.ssh_parallelism = max(1, int(ssh_parallelism))
        self._ssh_executor = ThreadPoolExecutor(
            max_workers=self.ssh_parallelism, thread_name_prefix="ssh-fanout"
        )
        # A timed-out task can spend up to a minute in the best-effort
        # node-side reap after its login-side wrapper is killed.  Do not hold
        # the scheduler tick on that remote cleanup.  The task deliberately
        # remains RUNNING (and therefore keeps its allocation capacity
        # reserved) until its future completes, so a replacement cannot be
        # admitted on top of a solver that has not been reaped yet.
        self._timed_out_task_cancel_executor = ThreadPoolExecutor(
            max_workers=self.ssh_parallelism,
            thread_name_prefix="timed-out-task-cancel",
        )
        self._timed_out_task_cancellations: dict[int, tuple[dict, Any]] = {}
        self._terminal_aedt_workspace_cleanup_executor = ThreadPoolExecutor(
            max_workers=max(1, min(4, len(self.accounts))),
            thread_name_prefix="aedt-workspace-cleanup",
        )
        self._terminal_aedt_workspace_cleanup_lock = threading.Lock()
        self._terminal_aedt_workspace_cleanup_inflight: set[int] = set()
        self._last_terminal_aedt_workspace_sweep_at = 0.0

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
        self.retire_legacy_unprofiled_pending_demand_allocations()
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
        # Intentionally use an unbounded join: neither SQLite backup_to() nor
        # a stalled shared-filesystem scan is safely cancellable. A bounded
        # join would let a restarted scheduler overlap the old generation and
        # possibly treat a half-written file as its cadence marker. The trade-
        # off is that graceful stop can take as long as the in-flight I/O.
        with self._backup_lock:
            backup_thread = self._backup_thread
        if backup_thread and backup_thread is not threading.current_thread():
            backup_thread.join()
        self._ssh_executor.shutdown(wait=False, cancel_futures=True)
        self._timed_out_task_cancel_executor.shutdown(
            wait=False, cancel_futures=True
        )
        self._terminal_aedt_workspace_cleanup_executor.shutdown(
            wait=False, cancel_futures=True
        )

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
            "demand_reservation_plan": dict(self._last_demand_reservation_plan),
            "allocation_submission_recovery": {
                "blocked": bool(
                    self._allocation_submission_recovery.get("blocked")
                ),
                "blockers": list(
                    self._allocation_submission_recovery.get("blockers") or []
                ),
            },
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
            attach_token = str(task.get("attach_token") or "")
            launch_started_at = str(task.get("launch_started_at") or "")
            if attach_token and not launch_started_at:
                recovered = self.db.update_task_if_attach_claim(
                    task["id"],
                    attach_token,
                    require_launch_not_started=True,
                    status=TaskStatus.QUEUED.value,
                    allocation_id=None,
                    account_name=self.task_requested_account_name(task),
                    remote_dir="",
                    stdout_path="",
                    stderr_path="",
                    exit_code_path="",
                    wrapper_pid="",
                    attach_token="",
                    launch_started_at=None,
                    failure_message="",
                    attached_at=None,
                    started_at=None,
                )
                if not recovered:
                    continue
                LOGGER.warning("recovering task %s reserved before remote launch; requeueing", task["id"])
                self.record_event(
                    "recovered",
                    "task reserved before remote launch requeued",
                    entity_type="task",
                    entity_id=task["id"],
                )
                self._license_release_prelaunch_claim(task)
                continue
            if not str(task.get("exit_code_path") or ""):
                # Once launch_started_at is durable the remote nohup may have
                # succeeded even though its paths/PID were never acknowledged.
                # Legacy rows have no attach_token, so they are ambiguous too.
                # Keeping the claim is the fail-safe: a timeout/manual cancel
                # can reap it by task marker, while requeue could run it twice.
                LOGGER.warning(
                    "preserving task %s attaching claim with unacknowledged remote launch",
                    task["id"],
                )
                self.record_event(
                    "recovery_held",
                    "task attach launch may have started; claim preserved to prevent duplicate execution",
                    entity_type="task",
                    entity_id=task["id"],
                    account_name=str(task.get("account_name") or ""),
                )
        self.recover_allocation_submission_claims()
        self._reconstruct_license_admission_claims()

    def recover_allocation_submission_claims(self) -> None:
        """Resolve allocation rows left around the remote sbatch boundary.

        A reserved claim proves sbatch was never entered and is safe to fail
        for deterministic retry.  An in-progress claim is reconciled by the
        allocation-specific Slurm job name; ambiguous outcomes remain visible
        and capacity-holding rather than risking a duplicate pool job.
        """
        blockers: list[dict[str, Any]] = []
        candidates = [
            allocation
            for allocation in self.db.list_allocations_with_live(
                limit=0, live_limit=10000
            )
            if allocation["state"] == AllocationStatus.PENDING.value
            and not str(allocation.get("slurm_job_id") or "")
            and str(allocation.get("pending_reason") or "")
            in {
                ALLOCATION_SUBMISSION_RESERVED,
                ALLOCATION_SUBMISSION_IN_PROGRESS,
            }
        ]
        for allocation in sorted(candidates, key=lambda item: int(item["id"])):
            allocation_id = int(allocation["id"])
            phase = str(allocation.get("pending_reason") or "")
            account_name = str(allocation.get("account_name") or "")
            if phase == ALLOCATION_SUBMISSION_RESERVED:
                recovered = self.db.update_allocation_if_submission_claim(
                    allocation_id,
                    ALLOCATION_SUBMISSION_RESERVED,
                    state=AllocationStatus.FAILED.value,
                    pending_reason="",
                    failure_message=(
                        "scheduler restarted before allocation sbatch began; "
                        "safe to retry"
                    ),
                    closed_at="CURRENT_TIMESTAMP",
                )
                if recovered:
                    self.record_event(
                        "allocation_submission_recovered",
                        "reserved allocation failed before remote submission; safe to retry",
                        entity_type="allocation",
                        entity_id=allocation_id,
                        account_name=account_name,
                    )
                continue
            account = self.account_by_name(account_name)
            matches: list[dict[str, str]] | None = None
            query_error = ""
            if account is None:
                query_error = f"account {account_name or '<blank>'} not configured"
            else:
                try:
                    finder = getattr(
                        self._client(account), "find_allocation_submissions", None
                    )
                    if not callable(finder):
                        raise RuntimeError(
                            "client does not support exact allocation submission recovery"
                        )
                    matches = finder(allocation)
                except Exception as exc:
                    query_error = str(exc) or type(exc).__name__
            if query_error:
                message = f"allocation submission recovery query failed: {query_error}"
                if message != str(allocation.get("failure_message") or ""):
                    self.db.update_allocation(
                        allocation_id, failure_message=message
                    )
                    self.record_event(
                        "allocation_submission_recovery_held",
                        message,
                        entity_type="allocation",
                        entity_id=allocation_id,
                        account_name=account_name,
                    )
                blockers.append(
                    {
                        "allocation_id": allocation_id,
                        "account_name": account_name,
                        "reason": message,
                    }
                )
                continue
            exact_matches = list(matches or [])
            if len(exact_matches) == 1:
                slurm_job_id = str(
                    exact_matches[0].get("slurm_job_id") or ""
                ).strip()
                if slurm_job_id:
                    adopted = self.db.update_allocation_if_submission_claim(
                        allocation_id,
                        ALLOCATION_SUBMISSION_IN_PROGRESS,
                        state=AllocationStatus.PENDING.value,
                        slurm_job_id=slurm_job_id,
                        pending_reason="",
                        failure_message="",
                        stdout_path=str(allocation.get("stdout_path") or "").replace(
                            "%j", slurm_job_id
                        ),
                        stderr_path=str(allocation.get("stderr_path") or "").replace(
                            "%j", slurm_job_id
                        ),
                    )
                    if adopted:
                        self.record_event(
                            "allocation_submission_adopted",
                            f"adopted exact Slurm job {slurm_job_id} after scheduler restart",
                            entity_type="allocation",
                            entity_id=allocation_id,
                            account_name=account_name,
                        )
                    continue
            if len(exact_matches) > 1:
                job_ids = sorted(
                    str(match.get("slurm_job_id") or "")
                    for match in exact_matches
                )
                message = (
                    "allocation submission recovery ambiguous; exact job name "
                    f"matched {', '.join(job_ids)}"
                )
                if message != str(allocation.get("failure_message") or ""):
                    self.db.update_allocation(
                        allocation_id, failure_message=message
                    )
                    self.record_event(
                        "allocation_submission_recovery_held",
                        message,
                        entity_type="allocation",
                        entity_id=allocation_id,
                        account_name=account_name,
                    )
                blockers.append(
                    {
                        "allocation_id": allocation_id,
                        "account_name": account_name,
                        "reason": message,
                    }
                )
                continue
            started_at = self._timestamp(
                allocation.get("submitted_at") or allocation.get("updated_at")
            )
            age = (
                (self._now() - started_at).total_seconds()
                if started_at is not None
                else float(ALLOCATION_SUBMISSION_RECOVERY_GRACE_SECONDS)
            )
            if age >= ALLOCATION_SUBMISSION_RECOVERY_GRACE_SECONDS:
                recovered = self.db.update_allocation_if_submission_claim(
                    allocation_id,
                    ALLOCATION_SUBMISSION_IN_PROGRESS,
                    state=AllocationStatus.FAILED.value,
                    pending_reason="",
                    failure_message=(
                        "no exact Slurm allocation job appeared within "
                        f"{ALLOCATION_SUBMISSION_RECOVERY_GRACE_SECONDS}s; safe to retry"
                    ),
                    closed_at="CURRENT_TIMESTAMP",
                )
                if recovered:
                    self.record_event(
                        "allocation_submission_recovered",
                        "in-progress allocation had no exact Slurm job after recovery grace; safe to retry",
                        entity_type="allocation",
                        entity_id=allocation_id,
                        account_name=account_name,
                    )
                continue
            remaining = max(
                1,
                int(ALLOCATION_SUBMISSION_RECOVERY_GRACE_SECONDS - age),
            )
            message = (
                "awaiting exact Slurm allocation job during recovery grace "
                f"({remaining}s remaining)"
            )
            self.db.update_allocation(allocation_id, failure_message=message)
            blockers.append(
                {
                    "allocation_id": allocation_id,
                    "account_name": account_name,
                    "reason": message,
                }
            )
        self._allocation_submission_recovery = {
            "blocked": bool(blockers),
            "blockers": blockers,
        }

    def reconcile_slurm_state(self) -> None:
        """Cancel scheduler-created pool allocation jobs the DB no longer
        tracks, while protecting exact restart-recovery claims. Legacy
        ``pool`` and exact ``pool-<allocation id>`` names are scheduler-owned.
        """
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        }
        live_allocations = [
            allocation
            for allocation in self.db.list_allocations_with_live(limit=1000)
            if allocation["state"] in live_states
        ]
        known_ids = {
            str(allocation.get("slurm_job_id"))
            for allocation in live_allocations
            if allocation.get("slurm_job_id")
        }
        protected_job_names = {
            (
                str(allocation.get("account_name") or ""),
                f"pool-{int(allocation['id'])}",
            )
            for allocation in live_allocations
            if allocation["state"] == AllocationStatus.PENDING.value
            and not allocation.get("slurm_job_id")
            and allocation.get("pending_reason")
            == ALLOCATION_SUBMISSION_IN_PROGRESS
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
                    normalized_name = job_name.strip()
                    if not sep or not (
                        normalized_name == "pool"
                        or re.fullmatch(r"pool-\d+", normalized_name)
                    ):
                        continue
                    if (account.name, normalized_name) in protected_job_names:
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
            return seq
        LOGGER.critical(
            "scheduler tick %s still stalled after transport close (%.0fs); exiting for supervisor restart",
            seq,
            stalled_for,
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
        LOGGER.critical(
            "scheduler thread stack database_path=%s sqlite_journal_mode=%s:\n%s",
            self.db.path,
            self.db.journal_mode,
            stack,
        )

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
        # Account environment overlays are scheduling metadata.  A large FEA
        # fit pass calls account_supports() once per task/allocation pair; on a
        # network-backed SQLite database, reopening and re-reading the same
        # rows thousands of times can dominate the whole tick.  Keep one
        # coherent, scheduler-thread-local view for this tick.  Web/control
        # threads do not inherit this thread-local cache, so overlay updates
        # remain immediately visible outside the in-progress scheduling pass.
        self._tick_local.account_env_overlay_capabilities = {}
        try:
            yield cache
        finally:
            self._tick_local.cache = None
            self._tick_local.account_env_overlay_capabilities = None
            with self._tick_caches_lock:
                self._tick_caches.discard(cache)
            cache.close_all()

    def tick(self) -> None:
        self._tick_seq += 1
        self._tick_started_at = time.monotonic()
        self._tick_attach_workers_by_node.clear()
        self._tick_adaptive_memory_nodes.clear()
        self._active_profile_sets_cache = None
        stage_seconds: dict[str, float] = {}

        def run_stage(name: str, operation: Callable[[], Any]) -> Any:
            stage_started_at = time.monotonic()
            try:
                return operation()
            finally:
                stage_seconds[name] = time.monotonic() - stage_started_at

        try:
            run_stage("task_count_sample", self.sample_task_counts_if_due)
            with self._tick_client_cache():
                if self._needs_reconcile:
                    self._needs_reconcile = False
                    run_stage("reconcile_slurm_state", self.reconcile_slurm_state)
                run_stage("fail_stale_same_node_tasks", self.fail_stale_same_node_tasks)
                run_stage(
                    "fail_stale_requested_allocation_tasks",
                    self.fail_stale_requested_allocation_tasks,
                )
                run_stage("enforce_cpu_partition_limits", self.enforce_cpu_partition_allocation_limits)
                # Admission must be able to recover even when a later account
                # lifecycle/storage stage fails.  This method only launches a
                # background refresh and does not block the tick.
                run_stage("license_refresh", self.refresh_license_usage_if_due)
                run_stage("update_fea_overload_before", self.update_fea_overload_state)
                run_stage("alloc_utilization", self.refresh_allocation_utilization_if_due)
                ready_non_fea_ids = run_stage(
                    "plan_ready_non_fea",
                    self.ready_non_fea_assignment_candidate_ids,
                )
                run_stage("assign_ready_same_node", self.assign_ready_same_node_tasks)
                run_stage(
                    "assign_ready_gpu",
                    lambda: self.assign_ready_gpu_tasks(ready_non_fea_ids),
                )
                run_stage(
                    "assign_ready_standard",
                    lambda: self.assign_ready_standard_tasks(ready_non_fea_ids),
                )
                run_stage("assign_ready_fea", lambda: self.assign_ready_fea_tasks(background=True))
                run_stage("refresh_cluster_state", self.refresh_cluster_state_if_due)
                run_stage("refresh_allocations", self.refresh_allocations)
                run_stage("refresh_tasks", self.refresh_tasks)
                run_stage(
                    "aedt_workspace_cleanup",
                    self.cleanup_terminal_aedt_workspaces_if_due,
                )
                run_stage("allocation_lifecycle", self.apply_allocation_lifecycle)
                run_stage("fea_memory_pressure", self.handle_fea_memory_pressure)
                run_stage("fea_cpu_cap", self.enforce_fea_node_cpu_cap)
                run_stage("update_fea_overload_after", self.update_fea_overload_state)
                refreshed_non_fea_ids = run_stage(
                    "plan_refreshed_non_fea",
                    self.ready_non_fea_assignment_candidate_ids,
                )
                run_stage(
                    "assign_queued_standard",
                    lambda: self.assign_queued_tasks(
                        include_fea=False,
                        eligible_task_ids=refreshed_non_fea_ids,
                    ),
                )
                run_stage("maintain_allocation_pool", self.maintain_allocation_pool)
                run_stage("refresh_submitted_jobs", self.refresh_submitted_jobs)
                run_stage("submit_next_job", self.submit_next_queued_job)
                run_stage("cleanup", self.cleanup_remote_artifacts_if_due)
                run_stage("orphan_process_sweep", self.sweep_orphan_processes_if_due)
                run_stage("backup", self.backup_database_if_due)
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

    def sample_task_counts_if_due(self, now: float | None = None) -> bool:
        """Persist the dashboard task population on the scheduler tick."""
        sampled_at = time.time() if now is None else float(now)
        if (
            self._last_task_count_sample_at
            and sampled_at - self._last_task_count_sample_at
            < TASK_COUNT_SAMPLE_INTERVAL_SECONDS
        ):
            return False
        try:
            summary = self.db.task_activity_summary()
            self.db.record_task_count_sample(
                sampled_at=sampled_at,
                total_active=summary["total"],
                running=summary["running"],
                queued=summary["queued"],
                attaching=summary["attaching"],
            )
        except Exception:
            LOGGER.warning("failed to record task-count sample", exc_info=True)
            return False
        self._last_task_count_sample_at = sampled_at
        return True

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
                f"find {shlex.quote(workspace)} -mindepth 1 "
                f"\\( {name_expr} \\) -prune -print 2>/dev/null"
            )
            try:
                with SSHSession(account, default_timeout=600) as ssh:
                    result = ssh.run(list_command)
                    if result.exit_code != 0:
                        continue
                    candidates: list[str] = []
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
                        if normalized not in candidates:
                            candidates.append(normalized)
                    if not candidates:
                        continue
                    deleted: list[str] = []
                    protected: list[tuple[str, str]] = []
                    recent: list[str] = []
                    failed: list[str] = []
                    for index in range(0, len(candidates), 20):
                        chunk = candidates[index : index + 20]
                        delete_result = ssh.run(
                            self._workspace_prune_delete_command(
                                workspace, chunk, minutes
                            ),
                            timeout=600,
                        )
                        if delete_result.exit_code != 0:
                            failed.extend(chunk)
                            continue
                        reported: set[str] = set()
                        for raw_line in delete_result.stdout.splitlines():
                            fields = raw_line.split("\t")
                            if len(fields) < 2:
                                continue
                            status, reported_path = fields[0], fields[1]
                            normalized_path = self._normalize_remote_path(
                                reported_path
                            )
                            if (
                                normalized_path not in chunk
                                or normalized_path in reported
                            ):
                                continue
                            reported.add(normalized_path)
                            if status == "D" and len(fields) == 2:
                                deleted.append(normalized_path)
                            elif status == "P" and len(fields) == 3:
                                try:
                                    expected_markers = (
                                        workspace_prune_protection_marker_paths(
                                            workspace, normalized_path
                                        )
                                    )
                                except ValueError:
                                    expected_markers = ()
                                marker_path = (
                                    fields[2]
                                    if fields[2] in expected_markers
                                    else "__unsafe_path__"
                                )
                                protected.append(
                                    (normalized_path, marker_path)
                                )
                            elif status in {"R", "M"} and len(fields) == 2:
                                recent.append(normalized_path)
                            else:
                                failed.append(normalized_path)
                        failed.extend(path for path in chunk if path not in reported)
                    marker_validity: dict[str, tuple[bool, str]] = {}
                    for _artifact, marker_path in protected:
                        if marker_path in marker_validity:
                            continue
                        marker_validity[marker_path] = (
                            self._validate_remote_workspace_prune_marker(
                                ssh, marker_path
                            )
                        )
            except Exception as exc:
                LOGGER.warning("workspace prune failed on %s: %s", account.name, exc)
                continue
            invalid_markers = [
                marker
                for marker, (valid, _reason) in marker_validity.items()
                if not valid
            ]
            for marker in invalid_markers:
                LOGGER.warning(
                    "workspace prune preserved marker %s on %s but its "
                    "manifest is invalid: %s",
                    marker,
                    account.name,
                    marker_validity[marker][1],
                )
            LOGGER.info(
                "workspace prune removed %d, preserved %d, recent/missing %d, "
                "and failed closed on %d artifacts on %s",
                len(deleted),
                len(protected),
                len(recent),
                len(failed),
                account.name,
            )
            self.record_event(
                "workspace_prune",
                f"removed {len(deleted)} disposable artifacts matching "
                f"{', '.join(globs)}; preserved {len(protected)} via "
                f"{WORKSPACE_PRUNE_PROTECTION_MARKER} "
                f"({len(invalid_markers)} invalid manifest(s), still "
                f"fail-closed); skipped {len(recent)} recent/missing; "
                f"failed closed on {len(failed)}",
                entity_type="account",
                entity_id=account.name,
                account_name=account.name,
            )

    @staticmethod
    def _workspace_prune_delete_command(
        workspace: str,
        candidates: list[str],
        minutes: int,
    ) -> str:
        """Build a marker-aware deletion command for already-vetted paths.

        Marker existence, including a broken symlink or malformed JSON file,
        protects the candidate. The marker check is repeated after the mtime
        scan so a manifest added during a large-tree scan still wins before
        ``rm``.
        """

        commands: list[str] = []
        for candidate in candidates:
            marker_paths = workspace_prune_protection_marker_paths(
                workspace, candidate
            )
            marker_args = " ".join(
                shlex.quote(path) for path in marker_paths
            )
            marker_check = (
                "protected_marker=; "
                f"for marker_path in {marker_args}; do "
                'if [ -e "$marker_path" ] || [ -L "$marker_path" ]; then '
                'protected_marker="$marker_path"; break; fi; done'
            )
            quoted_candidate = shlex.quote(candidate)
            commands.append(
                f"target={quoted_candidate}; "
                f"{marker_check}; "
                'if [ -n "$protected_marker" ]; then '
                'printf "P\\t%s\\t%s\\n" "$target" "$protected_marker"; '
                'elif [ ! -e "$target" ] && [ ! -L "$target" ]; then '
                'printf "M\\t%s\\n" "$target"; '
                "else "
                f'recent=$(find "$target" -mmin -{int(minutes)} '
                "-print -quit 2>/dev/null); find_status=$?; "
                'if [ "$find_status" -ne 0 ]; then '
                'printf "E\\t%s\\n" "$target"; '
                'elif [ -n "$recent" ]; then '
                'printf "R\\t%s\\n" "$target"; '
                f"else {marker_check}; "
                'if [ -n "$protected_marker" ]; then '
                'printf "P\\t%s\\t%s\\n" "$target" "$protected_marker"; '
                'elif rm -rf -- "$target"; then '
                'printf "D\\t%s\\n" "$target"; '
                'else printf "E\\t%s\\n" "$target"; fi; fi; fi'
            )
        return "; ".join(commands)

    @staticmethod
    def _validate_remote_workspace_prune_marker(
        ssh: SSHSession, marker_path: str
    ) -> tuple[bool, str]:
        if marker_path == "__unsafe_path__":
            return False, "unsafe marker ancestry"
        quoted = shlex.quote(marker_path)
        try:
            result = ssh.run(
                f"marker={quoted}; "
                'if [ -L "$marker" ] || [ ! -f "$marker" ]; then '
                "exit 2; fi; "
                'size=$(wc -c < "$marker") || exit 3; '
                f'if [ "$size" -gt '
                f"{WORKSPACE_PRUNE_PROTECTION_MAX_BYTES} ]; "
                "then exit 4; fi; "
                'cat -- "$marker"',
                timeout=30,
            )
        except Exception as exc:
            return False, f"marker read raised {type(exc).__name__}: {exc}"
        if result.exit_code != 0:
            return (
                False,
                f"marker read failed with exit code {result.exit_code}",
            )
        try:
            parse_workspace_prune_protection_manifest(result.stdout)
        except ValueError as exc:
            return False, str(exc)
        return True, ""

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

    def _license_costs_for_task(self, task: dict) -> tuple[dict[str, int], str]:
        """Return persistent feature costs, or a fail-closed profile error.

        Only features held for the whole task belong here.  Stage-specific
        Maxwell/Icepak solver features are intentionally not treated as
        persistent leases without a runner stage heartbeat.
        """
        if not self.license_admission_enabled:
            return {}, ""
        project = str(task.get("project") or "").strip()
        configured = self.license_admission_persistent_cost_by_project.get(project)
        if configured:
            costs = dict(configured)
            if self.task_aedt_backend(task) == AedtBackend.POOLED.value:
                costs = {
                    feature: count
                    for feature, count in costs.items()
                    if feature.casefold() != "electronics_desktop"
                }
            return costs, ""
        if self.task_is_fea_bursty(task) and self.license_admission_unknown_fea_project_policy == "block":
            return {}, f"unconfigured license admission profile for FEA project {project or '<empty>'}"
        return {}, ""

    @staticmethod
    def _license_claim_key(task: dict) -> str:
        token = str(task.get("attach_token") or "").strip()
        if token:
            return token
        return "legacy:{id}:{stamp}".format(
            id=int(task.get("id") or 0),
            stamp=task.get("attached_at") or task.get("started_at") or task.get("created_at") or "unknown",
        )

    def _license_claim_from_task(self, task: dict, costs: dict[str, int]) -> dict:
        status = str(task.get("status") or TaskStatus.ATTACHING.value)
        return {
            "task_id": int(task.get("id") or 0),
            "project": str(task.get("project") or ""),
            "costs": dict(costs),
            "state": "running" if status == TaskStatus.RUNNING.value else "attaching",
            "claimed_at": self._timestamp(
                task.get("attached_at") or task.get("created_at")
            ) or self._now(),
            "launch_started_at": self._timestamp(task.get("launch_started_at")),
            "running_at": self._timestamp(task.get("started_at")),
        }

    def _reconstruct_license_admission_claims(self) -> None:
        """Rebuild reservations that a process restart cannot safely forget."""
        if not self.license_admission_enabled:
            return
        active = self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        )
        with self._task_assignment_lock:
            for task in active:
                costs, profile_error = self._license_costs_for_task(task)
                if profile_error or not costs:
                    continue
                key = self._license_claim_key(task)
                if key not in self._license_admission_claims:
                    self._license_admission_claims[key] = self._license_claim_from_task(task, costs)

    def _license_record_claim_locked(self, task: dict, attach_token: str) -> None:
        costs, profile_error = self._license_costs_for_task(task)
        if profile_error or not costs:
            return
        now = self._now()
        self._license_admission_claims[attach_token] = {
            "task_id": int(task.get("id") or 0),
            "project": str(task.get("project") or ""),
            "costs": dict(costs),
            "state": "attaching",
            "claimed_at": now,
            "launch_started_at": None,
            "running_at": None,
        }

    def _license_claims_for_task_locked(self, task: dict) -> list[tuple[str, dict]]:
        token = str(task.get("attach_token") or "").strip()
        if token and token in self._license_admission_claims:
            return [(token, self._license_admission_claims[token])]
        task_id = int(task.get("id") or 0)
        return [
            (key, claim)
            for key, claim in self._license_admission_claims.items()
            if int(claim.get("task_id") or 0) == task_id
        ]

    def _license_mark_launch_started(self, task: dict) -> None:
        if not self.license_admission_enabled:
            return
        with self._task_assignment_lock:
            for _key, claim in self._license_claims_for_task_locked(task):
                if claim.get("launch_started_at") is None:
                    claim["launch_started_at"] = self._now()

    def _license_mark_running(self, task: dict) -> None:
        if not self.license_admission_enabled:
            return
        with self._task_assignment_lock:
            for _key, claim in self._license_claims_for_task_locked(task):
                claim["state"] = "running"
                if claim.get("running_at") is None:
                    claim["running_at"] = self._now()

    def _license_mark_terminal(self, task: dict) -> None:
        """Release only a provably unlaunched claim; launched claims settle."""
        if not self.license_admission_enabled:
            return
        with self._task_assignment_lock:
            for key, claim in list(self._license_claims_for_task_locked(task)):
                if claim.get("launch_started_at") is None and claim.get("running_at") is None:
                    self._license_admission_claims.pop(key, None)
                else:
                    claim["state"] = "terminal"

    def _license_release_prelaunch_claim(self, task: dict) -> None:
        if not self.license_admission_enabled:
            return
        with self._task_assignment_lock:
            for key, claim in list(self._license_claims_for_task_locked(task)):
                if claim.get("launch_started_at") is None and claim.get("running_at") is None:
                    self._license_admission_claims.pop(key, None)

    def _license_reconcile_claims_locked(self, query_started_at: datetime) -> None:
        settlement = timedelta(seconds=self.license_admission_settlement_seconds)
        for key, claim in list(self._license_admission_claims.items()):
            if claim.get("state") == "attaching":
                continue
            anchor = claim.get("running_at") or claim.get("launch_started_at")
            if anchor is None:
                continue
            # A capture that began before the launch (or before its settlement
            # window elapsed) cannot prove that this checkout was represented.
            if query_started_at >= anchor + settlement:
                self._license_admission_claims.pop(key, None)

    def _license_unsettled_costs_locked(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for claim in self._license_admission_claims.values():
            for feature, cost in dict(claim.get("costs") or {}).items():
                out[feature] = out.get(feature, 0) + int(cost)
        return out

    def _license_configured_features(self) -> set[str]:
        features = set(self.license_admission_reserve_by_feature)
        for costs in self.license_admission_persistent_cost_by_project.values():
            features.update(costs)
        return features

    def _license_admission_diagnostics_locked(
        self,
        candidate_costs: dict[str, int] | None = None,
        exempt_reserve: bool = False,
    ) -> dict:
        candidate_costs = dict(candidate_costs or {})
        required_features = self._license_configured_features() | set(candidate_costs)
        unsettled = self._license_unsettled_costs_locked()
        diagnostics = {
            "enabled": self.license_admission_enabled,
            "snapshot_valid": False,
            "snapshot_age_seconds": None,
            "snapshot_max_age_seconds": self.license_admission_snapshot_max_age_seconds,
            "settlement_seconds": self.license_admission_settlement_seconds,
            "reserve_exempt": bool(exempt_reserve),
            "reserve_by_feature": dict(self.license_admission_reserve_by_feature),
            "persistent_cost_by_project": {
                project: dict(costs)
                for project, costs in self.license_admission_persistent_cost_by_project.items()
            },
            "unknown_fea_project_policy": self.license_admission_unknown_fea_project_policy,
            "unsettled_claims": len(self._license_admission_claims),
            "unsettled_by_feature": unsettled,
            "features": {},
            "blocked_reason": "",
            "last_blocked_reason": self._license_last_blocked_reason,
            "last_successful_checked_at": self._license_last_successful_checked_at,
        }
        if not self.license_admission_enabled:
            return diagnostics
        if not self.license_monitor_enabled:
            diagnostics["blocked_reason"] = "license monitor is disabled"
            return diagnostics
        usage = self._license_usage
        if not usage:
            diagnostics["blocked_reason"] = "no successful license snapshot"
            return diagnostics
        if str(usage.get("error") or ""):
            diagnostics["blocked_reason"] = f"license monitor error: {usage['error']}"
            return diagnostics
        if usage.get("server_up") is not True:
            diagnostics["blocked_reason"] = "license server is not confirmed up"
            return diagnostics
        if self._license_snapshot_completed_monotonic is None:
            diagnostics["blocked_reason"] = "no successful license snapshot"
            return diagnostics
        age = max(0.0, time.monotonic() - self._license_snapshot_completed_monotonic)
        diagnostics["snapshot_age_seconds"] = round(age, 3)
        # The snapshot refreshes inside the tick, so heavy ticks stretch its
        # age past any fixed limit; scale the tolerance with observed tick
        # time like the pestat gate does, instead of fail-closing admission.
        max_age = float(self.license_admission_snapshot_max_age_seconds)
        if self._last_tick_duration is not None:
            max_age = max(
                max_age,
                self.license_monitor_interval_seconds + 2.0 * self._last_tick_duration,
            )
        diagnostics["snapshot_max_age_seconds"] = round(max_age, 1)
        if age > max_age:
            diagnostics["blocked_reason"] = (
                f"license snapshot is stale: {age:.1f}s > {max_age:.0f}s"
            )
            return diagnostics
        raw_features = usage.get("features")
        if not isinstance(raw_features, list):
            diagnostics["blocked_reason"] = "license snapshot has no feature list"
            return diagnostics
        by_name = {
            str(item.get("feature") or ""): item
            for item in raw_features
            if isinstance(item, dict) and str(item.get("feature") or "")
        }
        capacity_reason = ""
        for feature in sorted(required_features):
            item = by_name.get(feature)
            if item is None:
                diagnostics["blocked_reason"] = f"license feature missing from snapshot: {feature}"
                return diagnostics
            try:
                total = int(item.get("total"))
                used = int(item.get("used"))
            except (TypeError, ValueError):
                diagnostics["blocked_reason"] = f"invalid license counts for {feature}"
                return diagnostics
            if total <= 0 or used < 0 or used > total:
                diagnostics["blocked_reason"] = (
                    f"invalid license counts for {feature}: used={used}, total={total}"
                )
                return diagnostics
            reserve = int(self.license_admission_reserve_by_feature.get(feature, 0))
            effective_used = used + int(unsettled.get(feature, 0))
            capacity = total if exempt_reserve else max(0, total - reserve)
            headroom = capacity - effective_used
            diagnostics["features"][feature] = {
                "used": used,
                "total": total,
                "reserve": reserve,
                "unsettled": int(unsettled.get(feature, 0)),
                "effective_used": effective_used,
                "admit_capacity": capacity,
                "admit_headroom": headroom,
                "candidate_cost": int(candidate_costs.get(feature, 0)),
            }
            cost = int(candidate_costs.get(feature, 0))
            if effective_used + cost > capacity and not capacity_reason:
                capacity_reason = (
                    f"license capacity exhausted for {feature}: used={used}, "
                    f"unsettled={int(unsettled.get(feature, 0))}, candidate={cost}, "
                    f"reserve={reserve}, total={total}"
                )
        diagnostics["snapshot_valid"] = True
        diagnostics["blocked_reason"] = capacity_reason
        return diagnostics

    def _license_task_admitted_locked(self, task: dict) -> tuple[bool, str]:
        if not self.license_admission_enabled:
            return True, ""
        costs, profile_error = self._license_costs_for_task(task)
        if profile_error:
            self._license_last_blocked_reason = profile_error
            return False, profile_error
        if not costs:
            return True, ""
        project = str(task.get("project") or "").strip()
        diagnostics = self._license_admission_diagnostics_locked(
            costs,
            exempt_reserve=project in self.license_admission_reserve_exempt_projects,
        )
        reason = str(diagnostics.get("blocked_reason") or "")
        if reason:
            self._license_last_blocked_reason = reason
            return False, reason
        self._license_last_blocked_reason = ""
        return True, ""

    def aedt_pool_warm_spare_admission(self, requested_sessions: int) -> tuple[int, str]:
        """Return how many new AEDT processes current license headroom permits."""
        requested = max(0, int(requested_sessions))
        if requested <= 0:
            return requested, ""
        if not self.license_admission_enabled:
            reason = "license admission is disabled"
            self._license_last_blocked_reason = reason
            return 0, reason
        host_task = {
            "project": "_aedt_pool_hosts",
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "aedt_backend": AedtBackend.STANDALONE.value,
        }
        exempt_reserve = (
            host_task["project"] in self.license_admission_reserve_exempt_projects
        )
        with self._task_assignment_lock:
            costs, profile_error = self._license_costs_for_task(host_task)
            if profile_error:
                self._license_last_blocked_reason = profile_error
                return 0, profile_error
            desktop_cost = sum(
                int(cost)
                for feature, cost in costs.items()
                if str(feature).casefold() == "electronics_desktop"
            )
            if desktop_cost <= 0:
                reason = (
                    "license admission profile for _aedt_pool_hosts must declare "
                    "electronics_desktop headroom"
                )
                self._license_last_blocked_reason = reason
                return 0, reason
            diagnostics = self._license_admission_diagnostics_locked(
                exempt_reserve=exempt_reserve
            )
            reason = str(diagnostics.get("blocked_reason") or "")
            if reason:
                self._license_last_blocked_reason = reason
                return 0, reason
            allowed = requested
            for feature, cost in costs.items():
                per_session = int(cost)
                if per_session <= 0:
                    continue
                feature_status = dict(diagnostics.get("features") or {}).get(feature) or {}
                allowed = min(
                    allowed,
                    max(0, int(feature_status.get("admit_headroom") or 0)) // per_session,
                )
            if allowed < requested:
                requested_costs = {
                    feature: int(cost) * requested
                    for feature, cost in costs.items()
                }
                requested_diagnostics = self._license_admission_diagnostics_locked(
                    requested_costs,
                    exempt_reserve=exempt_reserve,
                )
                reason = str(requested_diagnostics.get("blocked_reason") or "") or (
                    f"license admission headroom permits {allowed} of "
                    f"{requested} warm-spare AEDT sessions"
                )
                self._license_last_blocked_reason = reason
                return allowed, reason
            self._license_last_blocked_reason = ""
            return allowed, ""

    def _publish_successful_license_snapshot(
        self, payload: dict, query_started_at: datetime
    ) -> None:
        with self._task_assignment_lock:
            self._license_usage = payload
            self._license_snapshot_completed_monotonic = time.monotonic()
            self._license_snapshot_query_started_at = query_started_at
            self._license_last_successful_checked_at = str(payload.get("checked_at") or "")
            self._license_reconcile_claims_locked(query_started_at)

    def license_usage(self) -> dict:
        with self._task_assignment_lock:
            payload = dict(self._license_usage)
            if isinstance(payload.get("features"), list):
                payload["features"] = [dict(item) for item in payload["features"]]
            if isinstance(payload.get("in_use"), list):
                payload["in_use"] = [dict(item) for item in payload["in_use"]]
            if isinstance(payload.get("display"), list):
                payload["display"] = [dict(item) for item in payload["display"]]
            payload["admission"] = self._license_admission_diagnostics_locked()
            return payload

    def allocation_utilizations(self) -> dict[int, dict[str, float]]:
        """allocation_id -> measured busy cores / utilization inside the
        allocation's own reserved cores (co-tenant load excluded)."""
        out: dict[int, dict[str, float]] = {}
        now = time.monotonic()
        max_age = self._alloc_util_max_age_seconds()
        for allocation_id, info in self._alloc_cpu_util.items():
            if now - info["at"] > max_age:
                continue
            out[allocation_id] = {
                "busy_cores": round(info["busy_cores"], 2),
                "total_cpus": info.get("total_cpus", 0),
                "util_percent": round(
                    min(100.0, info["busy_cores"] / max(1.0, info.get("total_cpus") or 1.0) * 100.0), 1
                ),
            }
        return out

    def node_fill_diagnosis(self, node_name: str, task: dict) -> dict:
        """Everything the node detail page needs to explain why FEA is or is
        not filling this node further, for a representative task shape."""
        live_states = {
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
        }
        allocations = [
            allocation
            for allocation in self.db.list_allocations_with_live(limit=500)
            if str(allocation.get("node_name") or "") == node_name and allocation["state"] in live_states
        ]
        utils = self.allocation_utilizations()
        per_allocation = []
        for allocation in allocations:
            trace: dict = {}
            slots = self.fea_dynamic_extra_slots(allocation, task, trace=trace)
            cap_remaining = self.fea_node_cpu_cap_remaining(allocation, task)
            if cap_remaining is not None and cap_remaining < slots:
                trace["blocked_by"] = trace.get("blocked_by") or "node CPU cap (total requested FEA CPUs)"
            per_allocation.append(
                {
                    "allocation": allocation,
                    "fit_slots": min(slots, cap_remaining) if cap_remaining is not None else slots,
                    "raw_slots": slots,
                    "node_cpu_cap_remaining": cap_remaining,
                    "utilization": utils.get(int(allocation["id"])),
                    "trace": trace,
                }
            )
        pressure = self.fea_owned_node_pressures().get(node_name, {})
        return {
            "node_name": node_name,
            "task_shape": {key: task.get(key) for key in ("cpus", "memory_mb", "scheduling_profile")},
            "allocations": per_allocation,
            "footprint": self.fea_immature_footprint(node_name),
            "footprint_maturity_seconds": self.fea_footprint_maturity_seconds,
            "node_requested_fea_cpus": int(pressure.get("requested_cpus") or 0),
            "node_cpu_total": self._node_cpu_total(node_name),
            "node_cpu_cap_factor": self.fea_node_requested_cpu_factor,
            "alloc_util_target": self.fea_alloc_util_target,
            "soft_memory_free_percent": self.fea_soft_memory_free_percent,
            "hard_memory_free_percent": self.fea_hard_memory_free_percent,
            "adaptive_memory_relax_enabled": self.fea_adaptive_memory_relax_enabled,
            "adaptive_memory_window_seconds": self.fea_adaptive_memory_window_seconds,
            "adaptive_memory_margin_percent": self.fea_adaptive_memory_margin_percent,
            "adaptive_memory_max_attach_per_tick": self.fea_adaptive_memory_max_attach_per_tick,
        }

    def refresh_allocation_utilization_if_due(self) -> None:
        """Sample each pool job's per-step CPU time via gate-side sstat and
        difference consecutive samples into an allocation-local utilization.
        Node-wide loadavg mixes co-tenant activity; scheduling decisions should
        track how busy OUR reserved cores are."""
        if not self.fea_alloc_util_enabled:
            return
        now = time.time()
        if now - self._last_alloc_util_at < self.fea_alloc_util_sample_interval_seconds:
            return
        self._last_alloc_util_at = now
        accounts_by_name = {account.name: account for account in self.accounts}
        live_states = {
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
        }
        by_account: dict[str, list[dict]] = {}
        live_job_ids: set[str] = set()
        for allocation in self.db.list_allocations_with_live(limit=500):
            if allocation["state"] not in live_states or not allocation.get("slurm_job_id"):
                continue
            if allocation["account_name"] not in accounts_by_name:
                continue
            live_job_ids.add(str(allocation["slurm_job_id"]))
            by_account.setdefault(allocation["account_name"], []).append(allocation)
        # Forget samples of allocations that went away.
        for job_id in list(self._alloc_cpu_samples):
            if job_id not in live_job_ids:
                self._alloc_cpu_samples.pop(job_id, None)
        outcomes = self._fan_out_by_account(
            by_account,
            lambda account_name, allocations: self._probe_allocation_cpu(
                accounts_by_name[account_name], allocations
            ),
        )
        for account_name, outcome in outcomes.items():
            if isinstance(outcome, Exception):
                LOGGER.warning("allocation utilization probe failed on %s: %s", account_name, outcome)
                continue
            self._alloc_cpu_util.update(outcome)
        live_ids = {int(allocation["id"]) for allocations in by_account.values() for allocation in allocations}
        for allocation_id in list(self._alloc_cpu_util):
            if allocation_id not in live_ids:
                self._alloc_cpu_util.pop(allocation_id, None)

    def _probe_allocation_cpu(
        self, account: AccountConfig, allocations: list[dict]
    ) -> dict[int, dict[str, float]]:
        client = self._client(account)
        probe = getattr(client, "pool_step_cpu_seconds", None)
        if not callable(probe):
            return {}
        job_ids = [str(allocation["slurm_job_id"]) for allocation in allocations]
        steps_by_job = probe(job_ids)
        sampled_at = time.monotonic()
        out: dict[int, dict[str, float]] = {}
        for allocation in allocations:
            job_id = str(allocation["slurm_job_id"])
            current = steps_by_job.get(job_id, {})
            previous = self._alloc_cpu_samples.get(job_id)
            self._alloc_cpu_samples[job_id] = (sampled_at, current)
            if not previous:
                continue
            previous_at, previous_steps = previous
            elapsed = sampled_at - previous_at
            if elapsed < 5:
                continue
            delta = sum(
                max(0.0, seconds - previous_steps.get(step_id, 0.0))
                for step_id, seconds in current.items()
            )
            out[int(allocation["id"])] = {
                "busy_cores": delta / elapsed,
                "total_cpus": float(allocation.get("total_cpus") or 0),
                "at": sampled_at,
            }
        return out

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
        query_started_at = self._now()
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
            if not features:
                with self._task_assignment_lock:
                    if self._license_usage.get("features"):
                        previous = dict(self._license_usage)
                        previous["error"] = "lmstat returned no feature block; showing the last good snapshot"
                        self._license_usage = previous
                        return
            server_up = "license server UP" in result.stdout
            error = ""
            if result.exit_code != 0:
                error = (
                    result.stdout.strip().splitlines()[-1][:200]
                    if result.stdout.strip()
                    else f"lmstat exited with status {result.exit_code}"
                )
            elif not server_up:
                error = result.stdout.strip().splitlines()[-1][:200] if result.stdout.strip() else "no lmstat output"
            elif not features:
                error = "lmstat returned no feature block"
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
            payload = {
                "checked_at": self._now().isoformat(),
                "query_started_at": query_started_at.isoformat(),
                "server": self.license_monitor_license_server,
                "server_up": server_up,
                "features": features,
                "in_use": [item for item in features if item["used"] > 0],
                "display": display,
                "error": error,
            }
            if result.exit_code == 0 and server_up and features and not error:
                self._publish_successful_license_snapshot(payload, query_started_at)
            else:
                with self._task_assignment_lock:
                    self._license_usage = payload
        except Exception as exc:
            with self._task_assignment_lock:
                self._license_usage = {
                    "checked_at": self._now().isoformat(),
                    "query_started_at": query_started_at.isoformat(),
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
        with self._backup_lock:
            if self._stop.is_set() or self._backup_inflight:
                return
            now = time.time()
            if (
                self._last_backup_at != 0.0
                and now - self._last_backup_at < self.backup_interval_seconds
            ):
                return
            self._backup_inflight = True
            backup_thread = threading.Thread(
                target=self._backup_database_if_due_worker,
                args=(now,),
                name="database-backup",
                daemon=True,
            )
            self._backup_thread = backup_thread
            try:
                # Start while holding the lifecycle lock so stop() can never
                # observe and join a Thread object that has not started yet.
                backup_thread.start()
            except Exception as exc:
                self._backup_inflight = False
                if self._backup_thread is backup_thread:
                    self._backup_thread = None
                LOGGER.warning("failed to start database backup: %s", exc)

    def _backup_database_if_due_worker(self, now: float) -> None:
        try:
            if self._last_backup_at == 0.0:
                # Survive restarts: resume the cadence from the newest backup
                # file. This scan is intentionally off the scheduler tick
                # because backup_dir can be a slow shared filesystem.
                try:
                    mtimes = [
                        os.path.getmtime(os.path.join(self.backup_dir, entry))
                        for entry in os.listdir(self.backup_dir)
                        if entry.startswith("slurm_scheduler-")
                        and entry.endswith(".db")
                    ]
                    if mtimes:
                        self._last_backup_at = max(mtimes)
                except OSError:
                    pass
            if now - self._last_backup_at < self.backup_interval_seconds:
                return
            # Preserve the existing cadence on failure: one attempted backup
            # consumes this interval instead of retrying on every tick.
            self._last_backup_at = now
            stamp = self._now().strftime("%Y%m%d-%H%M%S")
            backup_path = os.path.join(
                self.backup_dir, f"slurm_scheduler-{stamp}.db"
            )
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
                    if entry.startswith("slurm_scheduler-")
                    and entry.endswith(".db")
                )
                for stale in backups[: -self.backup_keep]:
                    os.unlink(os.path.join(self.backup_dir, stale))
            except OSError as exc:
                LOGGER.warning("failed to rotate database backups: %s", exc)
        finally:
            with self._backup_lock:
                self._backup_inflight = False
                if self._backup_thread is threading.current_thread():
                    self._backup_thread = None

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
                        pestat_nodes = parse_pestat(pestat_result.stdout)
                        self.db.replace_pestat_nodes(pestat_nodes)
                        self._pestat_nodes_cache = None
                        self._record_node_memory_history(pestat_nodes)
                return
            except Exception as exc:
                LOGGER.warning("failed to refresh cluster state through %s: %s", account.name, exc)

    def account_by_name(self, name: str) -> AccountConfig | None:
        return next((item for item in self.accounts if item.name == name), None)

    def aedt_session_processes_absent(
        self, session: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """Prove that both recorded owner PIDs are absent on the exact node.

        This is intentionally a read-only ``srun --overlap`` probe.  An SSH,
        Slurm, allocation-identity, or output ambiguity raises instead of
        treating the process as dead; no allocation or sibling task is ever
        cancelled by this check.
        """

        allocation_id = int(session.get("allocation_id") or 0)
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            raise RuntimeError("session allocation no longer exists")
        account_name = str(allocation.get("account_name") or "").strip()
        session_account = str(session.get("account_name") or "").strip()
        if session_account and session_account != account_name:
            raise RuntimeError("session account does not match its allocation")
        account = self.account_by_name(account_name)
        if not account:
            raise RuntimeError("session allocation account is unavailable")

        allocation_job = str(allocation.get("slurm_job_id") or "").strip()
        session_job = str(session.get("host_slurm_job_id") or "").strip()
        if not allocation_job or (session_job and session_job != allocation_job):
            raise RuntimeError("session Slurm job identity does not match its allocation")
        allocation_node = str(allocation.get("node_name") or "").strip()
        session_node = str(
            session.get("actual_node_name") or session.get("node_name") or ""
        ).strip()
        if (
            not allocation_node
            or not session_node
            or allocation_node.split(".", 1)[0]
            != session_node.split(".", 1)[0]
        ):
            raise RuntimeError("session node identity does not match its allocation")

        host_pid = str(session.get("host_process_id") or "").strip()
        aedt_pid = str(session.get("process_id") or "").strip()
        if not re.fullmatch(r"[1-9][0-9]*", host_pid) or not re.fullmatch(
            r"[1-9][0-9]*", aedt_pid
        ):
            raise RuntimeError("session process identity is incomplete")
        process_ids = list(dict.fromkeys((host_pid, aedt_pid)))
        pid_words = " ".join(process_ids)
        probe_script = (
            "present=''; "
            f"for pid in {pid_words}; do "
            "if ps -p \"$pid\" >/dev/null 2>&1; then "
            "present=\"$present $pid\"; fi; done; "
            "if [ -n \"$present\" ]; then "
            "printf 'PRESENT%s\\n' \"$present\"; exit 3; fi; "
            "echo ABSENT"
        )
        command = (
            f"srun --jobid={shlex.quote(allocation_job)} --overlap "
            "--nodes=1 --ntasks=1 --cpus-per-task=1 "
            f"--nodelist={shlex.quote(allocation_node)} "
            f"bash -lc {shlex.quote(probe_script)}"
        )
        try:
            with SSHSession(account, default_timeout=45) as ssh:
                result = ssh.run(command, timeout=40)
        except Exception as exc:
            raise RuntimeError(f"remote PID absence probe failed: {exc}") from exc

        lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
        evidence: dict[str, Any] = {
            "allocation_id": allocation_id,
            "slurm_job_id": allocation_job,
            "node_name": allocation_node,
            "checked_process_ids": process_ids,
        }
        if result.exit_code == 0 and lines == ["ABSENT"]:
            evidence["status"] = "absent"
            return True, evidence
        if result.exit_code == 3 and len(lines) == 1 and lines[0].startswith("PRESENT "):
            present = lines[0].split()[1:]
            if present and set(present).issubset(set(process_ids)):
                evidence["status"] = "present"
                evidence["present_process_ids"] = present
                return False, evidence
        raise RuntimeError(
            "remote PID absence probe returned an inconclusive result "
            f"(exit={int(result.exit_code)})"
        )

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
        tick_cache = getattr(
            self._tick_local,
            "account_env_overlay_capabilities",
            None,
        )
        overlay_sets = tick_cache.get(account.name) if tick_cache is not None else None
        if overlay_sets is None:
            overlays = self.db.list_account_env_overlays(account.name)
            overlay_sets = (
                frozenset(str(item.get("capability") or "") for item in overlays),
                frozenset(str(item.get("env_profile") or "") for item in overlays),
            )
            if tick_cache is not None:
                tick_cache[account.name] = overlay_sets
        overlay_capabilities, overlay_profiles = overlay_sets
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
                    # A CLOSING allocation's scancel was already issued; when
                    # Slurm no longer reports the job at all (fell out of
                    # squeue and the sacct window), nothing will ever converge
                    # it and the row pins the node ledger forever (observed
                    # 26h+ stuck 'closing' rows). Treat job-vanished as closed.
                    if allocation["state"] == AllocationStatus.CLOSING.value:
                        self.db.update_allocation(
                            allocation["id"],
                            state=AllocationStatus.CLOSED.value,
                            closed_at="CURRENT_TIMESTAMP",
                        )
                        self.record_event(
                            "allocation_closed",
                            f"slurm job {allocation.get('slurm_job_id')} vanished while closing",
                            entity_type="allocation",
                            entity_id=allocation["id"],
                        )
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
        elif status == JobStatus.SUBMITTED and allocation["state"] == AllocationStatus.CLOSING.value:
            # The batched probe reports jobs missing from BOTH squeue and the
            # sacct window as SUBMITTED (see SlurmClient.job_states). A CLOSING
            # allocation already had its scancel issued, so "Slurm has no
            # record" means the job is gone for good; without this branch the
            # row stays 'closing' forever and pins the node ledger (observed
            # 26h+ stuck rows blocking pool scale-out).
            self.db.update_allocation(
                allocation["id"],
                state=AllocationStatus.CLOSED.value,
                closed_at="CURRENT_TIMESTAMP",
            )
            self.record_event(
                "allocation_closed",
                f"slurm job {allocation.get('slurm_job_id')} vanished while closing",
                entity_type="allocation",
                entity_id=allocation["id"],
                account_name=str(allocation.get("account_name") or ""),
            )
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
        self._drain_timed_out_task_cancellations()
        accounts_by_name = {account.name: account for account in self.accounts}
        active_candidates = [
            task
            for task in self.db.list_tasks_by_statuses(
                [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
            )
            if task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        ]
        candidates = []
        for task in active_candidates:
            if self.strict_node_cancellation_is_pending(task):
                account = accounts_by_name.get(str(task.get("account_name") or ""))
                if account:
                    self.retry_strict_node_cancellation(task, account)
                continue
            candidates.append(task)
        # Timeout enforcement is a local deadline decision and must not depend
        # on the bounded remote-probe sample.  Previously a large, long-running
        # standard population consumed that whole sample before any FEA task
        # was considered, so expired FEA tasks could occupy slots forever.
        expired = [task for task in candidates if self.task_timed_out(task)]
        expired.sort(
            key=lambda task: (
                self._timestamp(
                    task.get("started_at")
                    or task.get("attached_at")
                    or task.get("created_at")
                )
                or self._now(),
                int(task.get("id") or 0),
            )
        )
        timeout_cancel_limit = min(
            TASK_TIMEOUT_CANCEL_MAX_PER_TICK,
            self.task_refresh_max_per_tick if max_tasks is None else max(1, int(max_tasks)),
        )
        for task in expired[:timeout_cancel_limit]:
            self._schedule_timed_out_task_cancellation(task)
        expired_ids = {int(task.get("id") or 0) for task in expired}
        refreshable = [
            task
            for task in candidates
            if int(task.get("id") or 0) not in expired_ids
            and not (
                task["status"] == TaskStatus.ATTACHING.value
                and not task.get("exit_code_path")
            )
        ]
        by_account: dict[str, list[dict]] = {}
        for task in self.tasks_to_refresh(refreshable, max_tasks=max_tasks):
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
        # Fake/fast clients commonly finish before the probe fan-out returns;
        # a single bounded wait preserves prompt terminalization without ever
        # making a real remote reap part of the tick's critical path.
        self._drain_timed_out_task_cancellations(wait_seconds=0.1)
        # Assignment may have populated the tick-local node worker snapshot.
        # Refresh can terminalize tasks or change allocation/node membership,
        # so the later demand-planning stage must rebuild from the new rows.
        self._fea_worker_counts_cache = None
        self.recalculate_allocation_capacity()

    def _apply_task_probe(self, task: dict, probe: TaskProbe, client) -> None:
        status = probe.status
        if status == JobStatus.RUNNING:
            if task["status"] != TaskStatus.RUNNING.value:
                self.db.update_task(task["id"], status=TaskStatus.RUNNING.value, started_at="CURRENT_TIMESTAMP")
                self._license_mark_running(task)
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
                failure_message = (
                    task.get("failure_message")
                    or self.task_result_failure_message(task, client)
                    or self.task_stderr_failure_message(task, client)
                )
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
        fea = sorted(
            [task for task in tasks if self.task_is_fea_bursty(task)],
            key=lambda item: int(item.get("id") or 0),
        )

        def rotate(items: list[dict], cursor: int) -> list[dict]:
            after = [task for task in items if int(task.get("id") or 0) > cursor]
            before = [task for task in items if int(task.get("id") or 0) <= cursor]
            return after + before

        non_fea = rotate(non_fea, self._non_fea_task_refresh_cursor_id)
        fea = rotate(fea, self._fea_task_refresh_cursor_id)
        if not non_fea:
            non_fea_selected: list[dict] = []
            fea_selected = fea[:limit]
        elif not fea:
            non_fea_selected = non_fea[:limit]
            fea_selected = []
        elif limit == 1:
            # Alternate the one available slot across classes.
            if self._prefer_fea_for_single_task_refresh:
                non_fea_selected = []
                fea_selected = fea[:1]
            else:
                non_fea_selected = non_fea[:1]
                fea_selected = []
            self._prefer_fea_for_single_task_refresh = (
                not self._prefer_fea_for_single_task_refresh
            )
        else:
            # Reserve at least a quarter of the bounded probe budget for FEA.
            # Unused quota immediately spills to the other class.
            fea_quota = max(1, limit // 4)
            non_fea_quota = limit - fea_quota
            non_fea_selected = non_fea[:non_fea_quota]
            fea_selected = fea[:fea_quota]
            remaining = limit - len(non_fea_selected) - len(fea_selected)
            if remaining > 0:
                non_fea_extra = non_fea[len(non_fea_selected) : len(non_fea_selected) + remaining]
                non_fea_selected.extend(non_fea_extra)
                remaining -= len(non_fea_extra)
            if remaining > 0:
                fea_selected.extend(fea[len(fea_selected) : len(fea_selected) + remaining])

        if non_fea_selected:
            self._non_fea_task_refresh_cursor_id = int(
                non_fea_selected[-1].get("id") or 0
            )
        if fea_selected:
            self._fea_task_refresh_cursor_id = int(fea_selected[-1].get("id") or 0)
        return non_fea_selected + fea_selected

    def task_result_failure_message(
        self, task: dict, client: SlurmAccountClient
    ) -> str:
        stdout_path = task.get("stdout_path") or ""
        if not stdout_path:
            return ""
        try:
            try:
                text = client.read_text_file(stdout_path, tail_lines=200)
            except TypeError:
                # Lightweight test/adapter clients may only accept ``path``.
                text = client.read_text_file(stdout_path)
        except Exception:
            return ""
        for line in reversed(text.splitlines()):
            marker = line.find(_RESULT_JSON_PREFIX)
            if marker < 0:
                continue
            try:
                payload = json.loads(line[marker + len(_RESULT_JSON_PREFIX) :])
            except (TypeError, ValueError):
                continue
            if not isinstance(payload, dict):
                continue
            ordered_keys = list(_RESULT_FAILURE_KEYS)
            ordered_keys.extend(
                str(key)
                for key in payload
                if str(key) not in _RESULT_FAILURE_KEYS
                and str(key).lower().endswith(_RESULT_FAILURE_KEY_SUFFIXES)
            )
            reasons: list[str] = []
            for key in ordered_keys:
                value = payload.get(key)
                if value is None or value is False:
                    continue
                rendered = str(value).strip()
                if not rendered:
                    continue
                reasons.append(f"{key}={rendered}")
            if reasons:
                return ("RESULT_JSON: " + "; ".join(reasons))[:2000]
        return ""

    def task_stderr_failure_message(self, task: dict, client: SlurmAccountClient) -> str:
        stderr_path = task.get("stderr_path") or ""
        if not stderr_path:
            return ""
        try:
            text = client.read_text_file(stderr_path)
        except Exception:
            return ""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        failure_signals = [
            (_stderr_failure_signal_score(line), index, line)
            for index, line in enumerate(lines)
            if _stderr_failure_signal_score(line) > 0
        ]
        if failure_signals:
            # Workload logger/exception lines outrank generic launcher errors
            # such as ``srun: error: ... exit code 1``; among equal-strength
            # signals, retain the latest line.
            return max(failure_signals, key=lambda item: (item[0], item[1]))[2]
        return "\n".join(lines[:3])

    def task_timed_out(self, task: dict) -> bool:
        timeout = int(task.get("timeout_seconds") or 0)
        if timeout <= 0:
            return False
        started = self._timestamp(task.get("started_at") or task.get("attached_at") or task.get("created_at"))
        if not started:
            return False
        return (self._now() - started).total_seconds() >= timeout

    def _cancel_timed_out_task_remote(
        self,
        account: AccountConfig,
        task: dict,
        allocation_job_id: str,
    ) -> None:
        self.client_factory(account).cancel_task(task, allocation_job_id)

    def _schedule_timed_out_task_cancellation(self, task: dict) -> bool:
        task_id = int(task.get("id") or 0)
        if task_id <= 0 or task_id in self._timed_out_task_cancellations:
            return False
        account = self.account_by_name(str(task.get("account_name") or ""))
        if not account:
            self._finalize_timed_out_task(task)
            return True
        allocation_job_id = self._task_allocation_job_id(task)
        future = self._timed_out_task_cancel_executor.submit(
            self._cancel_timed_out_task_remote,
            account,
            dict(task),
            allocation_job_id,
        )
        self._timed_out_task_cancellations[task_id] = (dict(task), future)
        return True

    def _drain_timed_out_task_cancellations(
        self, wait_seconds: float = 0.0
    ) -> int:
        if wait_seconds > 0 and self._timed_out_task_cancellations:
            wait_futures(
                [item[1] for item in self._timed_out_task_cancellations.values()],
                timeout=max(0.0, float(wait_seconds)),
            )
        finalized = 0
        for task_id, (task, future) in list(
            self._timed_out_task_cancellations.items()
        ):
            if not future.done():
                continue
            self._timed_out_task_cancellations.pop(task_id, None)
            try:
                future.result()
            except Exception as exc:
                LOGGER.warning(
                    "failed to cancel timed out task %s remotely: %s",
                    task_id,
                    exc,
                )
            if self._finalize_timed_out_task(task):
                finalized += 1
        return finalized

    def _finalize_timed_out_task(self, task: dict) -> bool:
        updated = self.db.update_task_if_status(
            int(task["id"]),
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
            status=TaskStatus.FAILED.value,
            failure_message=f"task timed out after {int(task.get('timeout_seconds') or 0)}s",
            exit_code=124,
            finished_at="CURRENT_TIMESTAMP",
        )
        if not updated:
            return False
        self.on_task_terminal(task, "timed out")
        self.close_allocation_after_exclusive_task(task)
        return True

    def cancel_timed_out_task(self, task: dict) -> None:
        """Compatibility entry point; remote cleanup is intentionally async."""
        self._schedule_timed_out_task_cancellation(task)
        self._drain_timed_out_task_cancellations(wait_seconds=0.1)

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

    def recalculate_allocation_capacity(
        self, allocation_ids: set[int] | None = None
    ) -> None:
        if allocation_ids is None:
            tasks = self.db.list_tasks_by_statuses(
                [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
                limit=5000,
            )
            allocation_rows = self.db.list_allocations_with_live(limit=500)
        else:
            scoped_ids = sorted({int(item) for item in allocation_ids if int(item) > 0})
            # The hot attach path nearly always updates one allocation. Use
            # the indexed allocation_id lookup instead of rescanning every
            # active task and every allocation after each accepted claim.
            tasks = [
                task
                for allocation_id in scoped_ids
                for task in self.db.list_live_task_claims_for_allocation(
                    allocation_id
                )
            ]
            allocation_rows = [
                allocation
                for allocation_id in scoped_ids
                if (allocation := self.db.get_allocation(allocation_id))
                is not None
            ]
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
        capacity_rows: list[dict[str, Any]] = []
        for allocation in allocation_rows:
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
            capacity_rows.append(
                {
                    "id": int(allocation["id"]),
                    "state": state,
                    "free_cpus": free_cpus,
                    "free_memory_mb": free_mem,
                    "free_gpus": free_gpus,
                    "active_tasks": int(stats["active_tasks"]),
                }
            )
        self.db.update_allocation_capacities(capacity_rows)

    def apply_allocation_lifecycle(self) -> None:
        allocations = self.db.list_allocations_with_live(limit=500)
        protected_pending_fea_ids = (
            self.pending_fea_cpu_demand_timeout_protected_allocation_ids(
                allocations
            )
        )
        for allocation in allocations:
            if allocation["state"] == AllocationStatus.PENDING.value:
                self.expire_pending_allocation_if_stale(
                    allocation,
                    protected_pending_fea_ids=protected_pending_fea_ids,
                )
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
                self.close_allocation(allocation, "force timeout", force=True)

    def pending_fea_cpu_demand_timeout_protected_allocation_ids(
        self,
        allocations: list[dict],
    ) -> set[int]:
        """Return stale Priority waits with a current queued FEA CPU claim."""

        if self.allocation_pending_timeout_seconds <= 0:
            return set()
        now = self._now()
        candidates = {
            int(allocation["id"])
            for allocation in allocations
            if self.stale_priority_fea_cpu_demand_allocation(allocation, now)
        }
        if not candidates:
            return set()

        queued_tasks = self.queued_tasks_for_allocation_reservations()
        if not queued_tasks:
            return set()
        plan = self.queued_task_allocation_reservation_plan(queued_tasks)
        if not plan.reusable:
            # A concurrent allocation/task/pressure change means this pass
            # cannot prove that the old PENDING row still owns demand.
            return set()
        protected: set[int] = set()
        for allocation_id, task_ids in plan.reservations.items():
            if int(allocation_id) not in candidates:
                continue
            current_allocation = self.db.get_allocation(int(allocation_id))
            if not current_allocation or not self.stale_priority_fea_cpu_demand_allocation(
                current_allocation,
                self._now(),
            ):
                continue
            for task_id in task_ids:
                task = self.db.get_task(int(task_id))
                if (
                    not task
                    or plan.task_signatures_by_id.get(int(task_id))
                    != self._reservation_record_signature(task)
                    or task["status"] != TaskStatus.QUEUED.value
                    or not self.task_aedt_backend_admitted(task)
                    or self.task_aedt_backend(task)
                    != AedtBackend.STANDALONE.value
                    or not self.task_is_fea_bursty(task)
                    or self.task_requires_gpu(task)
                ):
                    continue
                protected.add(int(allocation_id))
                break
        return protected

    def stale_priority_fea_cpu_demand_allocation(
        self,
        allocation: dict,
        now: datetime,
    ) -> bool:
        if (
            allocation.get("state") != AllocationStatus.PENDING.value
            or str(allocation.get("resource_pool") or "cpu") != "cpu"
            or self.allocation_demand_profile(allocation) != "fea"
            or not self.pending_reason_is_priority(
                str(allocation.get("pending_reason") or "")
            )
        ):
            return False
        submitted_at = self._timestamp(
            allocation.get("submitted_at") or allocation.get("created_at")
        )
        return bool(
            submitted_at is not None
            and (now - submitted_at).total_seconds()
            >= self.allocation_pending_timeout_seconds
        )

    def expire_pending_allocation_if_stale(
        self,
        allocation: dict,
        *,
        protected_pending_fea_ids: set[int] | None = None,
    ) -> None:
        if self.allocation_pending_timeout_seconds <= 0:
            return
        if self.allocation_is_dedicated_aedt_pool(allocation):
            # AEDT demand allocations are durable, owner-managed capacity.
            # Generic close is intentionally refused for these allocations;
            # applying the generic timeout first would therefore refresh the
            # global CPU backoff every scheduler tick while leaving the Slurm
            # request alive. That permanently suppresses unrelated accounts
            # and replacement AEDT capacity. Keep the request queued and let
            # the AEDT reconciler/Slurm own its lifecycle.
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
        if self.pending_allocation_timeout_exempt(
            allocation,
            reason,
            protected_pending_fea_ids=protected_pending_fea_ids,
        ):
            return
        normalized_reason = (reason or "").strip().lower()
        if pool != "cpu" or "priority" not in normalized_reason:
            self._allocation_backoff_until_by_pool[pool] = time.monotonic() + max(
                0, self.allocation_pending_backoff_seconds
            )
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

    @staticmethod
    def pending_reason_is_priority(reason: str) -> bool:
        return (reason or "").strip().lower() in {"priority", "(priority)"}

    def pending_allocation_timeout_exempt(
        self,
        allocation: dict,
        reason: str,
        *,
        protected_pending_fea_ids: set[int] | None = None,
    ) -> bool:
        if (
            self.pending_reason_is_priority(reason)
            and str(allocation.get("resource_pool") or "cpu") == "cpu"
            and self.allocation_demand_profile(allocation) == "fea"
            and int(allocation.get("id") or 0)
            in (protected_pending_fea_ids or ())
        ):
            return True
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
            for task in self.db.list_tasks_by_statuses(
                [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
            )
            if task.get("allocation_id") == allocation_id
            and task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        )

    def fail_running_tasks(self, allocation_id: int, message: str) -> None:
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
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
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.QUEUED.value, TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
            limit=5000,
        ):
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

    def fail_stale_requested_allocation_tasks(self) -> None:
        """Terminalize queued exact pins whose requested parent cannot recover."""
        terminal_states = {
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
            AllocationStatus.CLOSED.value,
            AllocationStatus.FAILED.value,
        }
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.QUEUED.value],
            limit=5000,
        ):
            requested_id = self.task_requested_allocation_id(task)
            if requested_id <= 0:
                continue
            allocation = self.db.get_allocation(requested_id)
            if allocation and allocation.get("state") not in terminal_states:
                continue
            if (
                self.task_aedt_backend(task) == AedtBackend.POOLED.value
                and self.db.task_has_auto_aedt_reservation(int(task["id"]))
            ):
                # Automatic pins are renewable scheduler state, not operator
                # intent.  The side-effecting pre-admission hook will fail the
                # stale reservation and either repin a healthy session or keep
                # this task queued and unpinned.
                continue
            state = allocation.get("state") if allocation else "not found"
            self.db.update_task(
                task["id"],
                status=TaskStatus.FAILED.value,
                failure_message=f"requested allocation {requested_id} is {state}",
                finished_at="CURRENT_TIMESTAMP",
            )
            self.on_task_terminal(task, "failed")

    def close_allocation(
        self,
        allocation: dict,
        reason: str,
        *,
        force: bool = False,
        allow_aedt_pool_owner: bool = False,
    ) -> bool:
        allocation_id = int(allocation["id"])
        # Placement and shutdown share one lock. Without it, the same-node web
        # fast path could reserve a task after the final claim check but before
        # scancel. Keep the lock through cancellation so a closing allocation
        # can never acquire a new owner.
        with self._task_assignment_lock:
            current = self.db.get_allocation(allocation_id)
            if not current or current["state"] in {
                AllocationStatus.CLOSED.value,
                AllocationStatus.FAILED.value,
                AllocationStatus.CLOSING.value,
            }:
                return False
            if (
                not force
                and not allow_aedt_pool_owner
                and str(current.get("drain_reason") or "").startswith("AEDT pool")
            ):
                LOGGER.warning(
                    "refusing generic close of dedicated AEDT pool allocation %s (%s)",
                    allocation_id,
                    reason,
                )
                return False
            if not force and self.db.allocation_has_aedt_pool_claim(allocation_id):
                LOGGER.warning(
                    "refusing to close allocation %s (%s); counted AEDT session claim exists",
                    allocation_id,
                    reason,
                )
                return False
            live_claims = self.db.list_live_task_claims_for_allocation(allocation_id)
            if live_claims and not force:
                # A WARM row with a live claim is stale.  Reconcile it
                # conservatively and leave the Slurm parent untouched.  An
                # ATTACHING row also covers a recovery-held launch whose
                # remote outcome is ambiguous.
                if current["state"] == AllocationStatus.WARM.value:
                    self.db.update_allocation(
                        allocation_id,
                        state=AllocationStatus.ACTIVE.value,
                        last_active_at="CURRENT_TIMESTAMP",
                    )
                LOGGER.warning(
                    "refusing to close allocation %s (%s); live task claims: %s",
                    allocation_id,
                    reason,
                    ", ".join(str(task["id"]) for task in live_claims),
                )
                return False
            account = next((item for item in self.accounts if item.name == current["account_name"]), None)
            client = self._client(account) if account and current.get("slurm_job_id") else None
            if client is not None and not force:
                try:
                    live_steps = client.live_allocation_task_steps(current["slurm_job_id"])
                except Exception as exc:
                    LOGGER.warning(
                        "refusing to close allocation %s (%s); live-step probe failed: %s",
                        allocation_id,
                        reason,
                        exc,
                    )
                    self.db.update_allocation(
                        allocation_id,
                        failure_message=f"allocation close safety probe failed: {exc}",
                    )
                    return False
                if live_steps:
                    if current["state"] == AllocationStatus.WARM.value:
                        self.db.update_allocation(
                            allocation_id,
                            state=AllocationStatus.ACTIVE.value,
                            last_active_at="CURRENT_TIMESTAMP",
                        )
                    LOGGER.warning(
                        "refusing to close allocation %s (%s); live Slurm task steps: %s",
                        allocation_id,
                        reason,
                        ", ".join(live_steps),
                    )
                    return False
            previous_state = current["state"]
            self.db.update_allocation(allocation_id, state=AllocationStatus.CLOSING.value, drain_reason=reason)
            if client is not None:
                try:
                    client.cancel(current["slurm_job_id"])
                except Exception as exc:
                    self.db.update_allocation(
                        allocation_id,
                        state=previous_state,
                        failure_message=str(exc),
                    )
                    return False
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.CLOSED.value,
                failure_message="",
                closed_at="CURRENT_TIMESTAMP",
            )
            self.record_event(
                "allocation_closed",
                reason,
                entity_type="allocation",
                entity_id=allocation_id,
                account_name=str(current.get("account_name") or ""),
            )
            return True

    def close_empty_aedt_pool_allocation(
        self,
        allocation_id: int,
        *,
        expected_state: str = "",
    ) -> bool:
        """Pool-runtime-only lifecycle path for a dedicated empty allocation.

        ``expected_state`` lets a caller that observed a pending request fail
        closed if the allocation became live before the exact-owner close.
        """
        allocation = self.db.get_allocation(int(allocation_id))
        if not allocation:
            return False
        if expected_state and str(allocation.get("state") or "") != expected_state:
            return False
        if not str(allocation.get("drain_reason") or "").startswith("AEDT pool"):
            return False
        if self.db.allocation_has_aedt_pool_claim(int(allocation_id)):
            return False
        if self.db.list_live_task_claims_for_allocation(int(allocation_id)):
            return False
        return self.close_allocation(
            allocation,
            "AEDT pool empty",
            allow_aedt_pool_owner=True,
        )

    def active_task_ids_for_allocation(self, allocation_id: int) -> list[int]:
        return [
            int(task["id"])
            for task in self.db.list_live_task_claims_for_allocation(allocation_id)
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
        self.close_allocation(allocation, "manual close", force=force)
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
            for allocation in self.db.list_allocations_with_live(limit=1000)
            if allocation.get("state") in live_states and str(allocation.get("node_name") or "")
        }

    def ready_non_fea_assignment_candidate_ids(self) -> frozenset[int]:
        """Return the queued non-FEA tasks that fit the current ready pool.

        The exact assignment path still reloads and revalidates every returned
        task under ``_task_assignment_lock``.  This is only a conservative
        in-memory prefilter that prevents two serial scheduler stages from
        reopening the network-backed database for every task/allocation pair
        when no compatible ready allocation exists.  Pending allocations are
        deliberately excluded: they are useful to demand planning, but cannot
        accept a task yet.

        A stale positive is harmless because ``assign_queued_task`` fails
        closed.  A concurrent capacity increase can defer a task until the
        refreshed plan later in the same tick or the next tick; it can never
        launch a task without the normal admission checks.
        """

        queued = self.db.list_tasks(
            limit=5000,
            statuses=[TaskStatus.QUEUED.value],
        )
        gpu_tasks = sorted(
            [
                task
                for task in queued
                if task["status"] == TaskStatus.QUEUED.value
                and not self.same_node_as_task_id(task)
                and not self.task_is_fea_bursty(task)
                and self.task_requires_gpu(task)
            ],
            key=lambda item: (
                -int(item.get("priority") or 0),
                -int(item.get("gpus") or 0),
                int(item["id"]),
            ),
        )
        standard_tasks = sorted(
            [
                task
                for task in queued
                if task["status"] == TaskStatus.QUEUED.value
                and not self.same_node_as_task_id(task)
                and not self.task_is_fea_bursty(task)
                and not self.task_requires_gpu(task)
            ],
            key=lambda item: (
                -int(item.get("priority") or 0),
                -int(item.get("cpus") or 0),
                int(item["id"]),
            ),
        )
        if not gpu_tasks and not standard_tasks:
            return frozenset()

        remaining_allocations = [
            dict(allocation)
            for allocation in self.db.list_allocations_with_live(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
        ]
        if not remaining_allocations:
            return frozenset()

        # ``include_pending=False`` must honor node worker and exclusivity
        # limits.  Load those facts once for the whole plan instead of making
        # one full active-task query for every queued-task/allocation pair.
        active_tasks = [
            task
            for task in self.db.list_tasks_by_statuses(
                [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
                limit=5000,
            )
            if task.get("status")
            in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        ]
        worker_counts: dict[int, int] = {}
        active_task_allocation_ids: set[int] = set()
        active_exclusive_allocation_ids: set[int] = set()
        for active_task in active_tasks:
            allocation_id = int(active_task.get("allocation_id") or 0)
            if allocation_id <= 0:
                continue
            worker_counts[allocation_id] = worker_counts.get(allocation_id, 0) + 1
            active_task_allocation_ids.add(allocation_id)
            if int(active_task.get("exclusive_node") or 0):
                active_exclusive_allocation_ids.add(allocation_id)
        for allocation in remaining_allocations:
            allocation["_allocation_worker_count"] = worker_counts.get(
                int(allocation["id"]),
                0,
            )

        eligible: set[int] = set()
        # Match the real tick order: GPU tasks are offered before ordinary
        # standard tasks.  Each successful simulated reservation decrements
        # the copied allocation capacity, so the set is an upper bound on the
        # exact claims that can succeed in the following assignment stages.
        for task in [*gpu_tasks, *standard_tasks]:
            if self.reserve_inflight_capacity_for_task(
                remaining_allocations,
                task,
                include_pending=False,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            ):
                eligible.add(int(task["id"]))
        return frozenset(eligible)

    def assign_queued_tasks(
        self,
        include_fea: bool = True,
        eligible_task_ids: frozenset[int] | set[int] | None = None,
    ) -> None:
        queued_tasks = sorted(
            [
                task
                for task in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if task["status"] == TaskStatus.QUEUED.value
            ],
            key=lambda item: (
                0 if self.task_requires_gpu(item) else 1,
                -int(item.get("priority") or 0),
                int(item["id"]),
            ),
        )
        fea_attached_this_loop = 0
        for task in queued_tasks:
            if (
                eligible_task_ids is not None
                and int(task["id"]) not in eligible_task_ids
            ):
                continue
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
                for item in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if item["status"] == TaskStatus.QUEUED.value and self.same_node_as_task_id(item)
            ],
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        ):
            self.assign_queued_task(task)

    def assign_ready_standard_tasks(
        self,
        eligible_task_ids: frozenset[int] | set[int] | None = None,
    ) -> None:
        attached = 0
        for task in sorted(
            [
                item
                for item in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if item["status"] == TaskStatus.QUEUED.value
                and not self.same_node_as_task_id(item)
                and not self.task_is_fea_bursty(item)
                and not self.task_requires_gpu(item)
                and (
                    eligible_task_ids is None
                    or int(item["id"]) in eligible_task_ids
                )
            ],
            key=lambda item: (-int(item.get("priority") or 0), -int(item.get("cpus") or 0), int(item["id"])),
        ):
            if attached >= self.allocation_max_new_per_loop:
                return
            if self.assign_queued_task(task):
                attached += 1

    def assign_ready_gpu_tasks(
        self,
        eligible_task_ids: frozenset[int] | set[int] | None = None,
    ) -> None:
        attached = 0
        for task in sorted(
            [
                item
                for item in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if item["status"] == TaskStatus.QUEUED.value
                and not self.same_node_as_task_id(item)
                and not self.task_is_fea_bursty(item)
                and self.task_requires_gpu(item)
                and (
                    eligible_task_ids is None
                    or int(item["id"]) in eligible_task_ids
                )
            ],
            key=lambda item: (-int(item.get("priority") or 0), -int(item.get("gpus") or 0), int(item["id"])),
        ):
            if attached >= self.allocation_max_new_per_loop:
                return
            if self.assign_queued_task(task):
                attached += 1

    @staticmethod
    def project_fair_queue_order(
        tasks: list[dict],
        last_claim_by_priority: dict[int, str] | None = None,
    ) -> list[dict]:
        """Round-robin equal-priority work across projects, FIFO within each project."""
        grouped: dict[int, dict[str, deque[dict]]] = {}
        for task in sorted(
            tasks,
            key=lambda item: (-int(item.get("priority") or 0), int(item["id"])),
        ):
            priority = int(task.get("priority") or 0)
            project = str(task.get("project") or "")
            grouped.setdefault(priority, {}).setdefault(project, deque()).append(task)

        ordered = []
        for priority in sorted(grouped, reverse=True):
            queues = grouped[priority]
            projects = sorted(queues)
            if last_claim_by_priority is not None \
                    and priority in last_claim_by_priority:
                last_claim = last_claim_by_priority[priority]
                start = bisect_right(projects, last_claim)
                projects = projects[start:] + projects[:start]
            while projects:
                remaining = []
                for project in projects:
                    queue = queues[project]
                    ordered.append(queue.popleft())
                    if queue:
                        remaining.append(project)
                projects = remaining
        return ordered

    def load_fea_project_claim_cursor(self) -> dict[int, str]:
        raw = self.db.get_setting(FEA_PROJECT_CURSOR_SETTING)
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("cursor payload is not an object")
            return {
                int(priority): str(project)
                for priority, project in payload.items()
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            LOGGER.warning("ignoring invalid persisted FEA project cursor")
            return {}

    def persist_fea_project_claim(self, task: dict) -> None:
        priority = int(task.get("priority") or 0)
        project = str(task.get("project") or "")
        self._fea_project_last_claim[priority] = project
        self.db.set_setting(
            FEA_PROJECT_CURSOR_SETTING,
            json.dumps(
                {str(key): value for key, value in self._fea_project_last_claim.items()},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    def assign_ready_fea_tasks(self, background: bool = False) -> None:
        # Baseline attaches (allocations still below their 1x solver budget)
        # draw from their own per-tick budget, separate from the global
        # overcommit cap, so a slow overcommit trickle cannot delay baseline
        # recovery across many underfilled allocations.
        attached_overcommit = 0
        attached_baseline = 0
        queued_fea_tasks = self.project_fair_queue_order(
            [
                item
                for item in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if item["status"] == TaskStatus.QUEUED.value
                and self.task_is_fea_bursty(item)
            ],
            self._fea_project_last_claim,
        )
        eligible_storage_accounts = self.fea_storage_accounts_for_tasks(
            queued_fea_tasks
        )
        for task in queued_fea_tasks:
            if attached_baseline >= self.fea_baseline_max_attach_per_loop:
                return
            # Quota observations expire during a large 400-worker refill.
            # Refresh every stale account together so the assignment loop does
            # not pay one serial SSH round trip when it first reaches each
            # account after the 30-second boundary.
            self.prefetch_fea_storage_quotas(eligible_storage_accounts)
            baseline_only = attached_overcommit >= self.fea_max_attach_per_loop
            self._fea_last_attach_baseline = False
            if self.assign_queued_task(
                task, background=background, fea_baseline_only=baseline_only
            ):
                if self._fea_last_attach_baseline:
                    attached_baseline += 1
                else:
                    attached_overcommit += 1
                # This is an accepted DB attach claim, not proof that the remote
                # worker later started. Rotating on claims also prevents a
                # repeatedly failing project from monopolizing scarce slots.
                self.persist_fea_project_claim(task)

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

    def _aedt_storage_growth_reservation(
        self,
        account: AccountConfig,
        observation_at: float,
        additional_future_projects: int = 0,
        *,
        refresh_reservations: bool = False,
    ) -> tuple[int, float, dict[str, int]]:
        per_project_gb = self.aedt_storage_reservation_per_project_gb
        additional = max(0, int(additional_future_projects or 0))
        if per_project_gb <= 0:
            return 0, 0.0, {}
        observed = datetime.fromtimestamp(float(observation_at), tz=timezone.utc)
        cutoff = observed - timedelta(
            seconds=self.aedt_storage_reservation_maturity_seconds
        )
        cutoff_text = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        now = time.monotonic()
        cached = self._aedt_storage_growth_cache.get(cutoff_text)
        if (
            not refresh_reservations
            and cached
            and now - cached[0] < self._aedt_storage_growth_cache_seconds
        ):
            reservations = cached[1]
        else:
            reservations = self.db.aedt_storage_growth_reservations_by_account(
                cutoff_text
            )
            if len(self._aedt_storage_growth_cache) >= 32:
                self._aedt_storage_growth_cache.clear()
            self._aedt_storage_growth_cache[cutoff_text] = (now, reservations)
        detail = dict(reservations.get(account.name) or {})
        projects = max(0, int(detail.get("total_projects") or 0)) + additional
        if additional:
            detail["prospective_projects"] = additional
        detail["total_projects"] = projects
        return projects, projects * per_project_gb, detail

    def _storage_headroom_blocked(
        self,
        account: AccountConfig,
        *,
        observed_free_gb: float,
        observation_at: float,
        additional_future_projects: int,
        label: str,
        quota_context: str = "",
        refresh_reservations: bool = False,
    ) -> bool:
        projects, reserved_gb, reservation_detail = (
            self._aedt_storage_growth_reservation(
                account,
                observation_at,
                additional_future_projects,
                refresh_reservations=refresh_reservations,
            )
        )
        effective_free_gb = float(observed_free_gb) - reserved_gb
        if effective_free_gb >= self.storage_guard_min_free_gb:
            return False
        reservation_context = ""
        if projects:
            reservation_context = (
                f"; {reserved_gb:.1f} GB shadow-reserved for {projects} "
                "starting/active pooled project(s)"
            )
            starting = int(reservation_detail.get("starting_project_slots") or 0)
            ready = int(reservation_detail.get("ready_project_slots") or 0)
            busy = int(reservation_detail.get("busy_project_slots") or 0)
            detached = int(
                reservation_detail.get("detached_active_projects") or 0
            )
            attaching = int(reservation_detail.get("attaching_projects") or 0)
            running = int(reservation_detail.get("running_projects") or 0)
            young = int(reservation_detail.get("young_running_projects") or 0)
            mature = int(
                reservation_detail.get("mature_running_projects") or 0
            )
            prospective = int(reservation_detail.get("prospective_projects") or 0)
            reservation_context += (
                f" [session slots starting {starting}, ready {ready}, "
                f"busy {busy}; detached active {detached}; "
                f"attaching {attaching}, "
                f"running {running} (young {young}, mature {mature}), "
                f"prospective {prospective}]"
            )
        detail = (
            f"{label}: {observed_free_gb:.1f} GB observed free"
            f"{reservation_context}; {effective_free_gb:.1f} GB effective free "
            f"is below the {self.storage_guard_min_free_gb:g} GB threshold"
        )
        if quota_context:
            detail += f"; {quota_context}"
        self._warn_storage_guard(account, detail)
        return True

    def account_storage_guard_status(
        self,
        account: AccountConfig,
        *,
        for_fea: bool = False,
        additional_future_projects: int = 0,
        refresh_reservations: bool = False,
    ) -> tuple[bool, bool]:
        """Return ``(blocked, confirmed_pressure)`` for storage admission.

        A failed or unavailable GPFS probe fails closed for new work but is
        not confirmed pressure: callers may hold admission, but must not drain
        a healthy Desktop on that evidence alone. ``confirmed_pressure`` is
        true only when a valid quota/usage observation plus projected active
        AEDT growth is actually below the configured floor.

        Cached quota probes are adjusted by a DB-derived AEDT shadow
        reservation. Callers performing the final queued -> attaching claim
        pass one prospective project so concurrent callers cannot all spend
        the same cached free-space reading.
        """
        if self.storage_guard_min_free_gb <= 0:
            return False, False
        if for_fea:
            try:
                probe = self.cached_storage_quota(
                    account, self._client(account), time.time()
                )
            except AccountUnavailableThisTick as exc:
                self._warn_storage_guard(
                    account,
                    f"FEA work held: account unavailable this tick ({exc})",
                )
                return True, False
            if probe.error:
                detail = f"FEA work held: storage quota probe failed ({probe.error[:200]})"
                self._warn_storage_guard(account, detail)
                return True, False
            if probe.is_gpfs:
                if probe.quota is None:
                    self._warn_storage_guard(
                        account,
                        "FEA work held: GPFS quota status is unavailable",
                    )
                    return True, False
                free_gb = probe.quota.free_gb
                if free_gb is None:
                    return False, False
                observation_at = self._storage_quota_cache.get(
                    account.name, (time.time(), probe)
                )[0]
                blocked = self._storage_headroom_blocked(
                    account,
                    observed_free_gb=free_gb,
                    observation_at=observation_at,
                    additional_future_projects=additional_future_projects,
                    label=(
                        f"FEA work held: GPFS {probe.quota.quota_type.lower()} "
                        "block quota for fileset "
                        f"{probe.fileset_name or probe.quota.fileset_name or 'unknown'}"
                    ),
                    quota_context=(
                        f"used+in_doubt {probe.quota.effective_used_gb:.1f} / "
                        f"{probe.quota.block_limit_gb:.1f} GB"
                    ),
                    refresh_reservations=refresh_reservations,
                )
                return blocked, blocked
        if not account.storage_quota_gb:
            return False, False
        cached = self._storage_cache.get(account.name)
        used = cached[1] if cached else None
        if used is None:
            return False, False
        free_gb = float(account.storage_quota_gb) - float(used)
        observation_at = cached[0] if cached else time.time()
        blocked = self._storage_headroom_blocked(
            account,
            observed_free_gb=free_gb,
            observation_at=observation_at,
            additional_future_projects=additional_future_projects,
            label="attaches held",
            refresh_reservations=refresh_reservations,
        )
        return blocked, blocked

    def account_storage_blocked(
        self,
        account: AccountConfig,
        *,
        for_fea: bool = False,
        additional_future_projects: int = 0,
        refresh_reservations: bool = False,
    ) -> bool:
        """Hold new work when storage is low or its FEA quota probe failed."""

        blocked, _confirmed_pressure = self.account_storage_guard_status(
            account,
            for_fea=for_fea,
            additional_future_projects=additional_future_projects,
            refresh_reservations=refresh_reservations,
        )
        return blocked

    def fea_storage_accounts_for_tasks(
        self, tasks: list[dict]
    ) -> list[AccountConfig]:
        """Accounts that may receive at least one queued FEA task."""
        if self.storage_guard_min_free_gb <= 0 or not tasks:
            return []
        eligible: list[AccountConfig] = []
        for account in self.accounts:
            for task in tasks:
                requested = self.requested_accounts(
                    self.task_requested_account_name(task)
                )
                if requested and account.name not in requested:
                    continue
                if self.account_supports(
                    account,
                    str(task.get("required_capability") or ""),
                    str(task.get("env_profile") or ""),
                ):
                    eligible.append(account)
                    break
        return eligible

    def prefetch_fea_storage_quotas(
        self, accounts: list[AccountConfig]
    ) -> None:
        """Refresh stale FEA quota probes concurrently by account.

        The ordinary lazy guard remains authoritative. This helper only fills
        the same short-lived cache before a large assignment/planning loop so
        reaching several accounts after cache expiry costs one parallel SSH
        round trip rather than one serial round trip per account.
        """
        if self.storage_guard_min_free_gb <= 0 or not accounts:
            return
        now = time.time()
        stale = [
            account
            for account in accounts
            if not self._storage_quota_cache.get(account.name)
            or now - self._storage_quota_cache[account.name][0]
            >= self._storage_quota_refresh_interval_seconds
        ]
        if not stale:
            return
        accounts_by_name = {account.name: account for account in stale}

        def probe(account_name: str, _items: list) -> StorageQuotaProbe:
            account = accounts_by_name[account_name]
            client = self._client(account)
            probe_method = getattr(client, "storage_quota_probe", None)
            if not callable(probe_method):
                return StorageQuotaProbe(filesystem_type="unsupported")
            try:
                return probe_method()
            except Exception as exc:
                return StorageQuotaProbe(
                    filesystem_type="",
                    error=str(exc) or type(exc).__name__,
                )

        outcomes = self._fan_out_by_account(
            {account.name: [] for account in stale}, probe
        )
        observed_at = time.time()
        for account in stale:
            outcome = outcomes.get(account.name)
            if isinstance(outcome, StorageQuotaProbe):
                value = outcome
            elif isinstance(outcome, Exception):
                value = StorageQuotaProbe(
                    filesystem_type="",
                    error=str(outcome) or type(outcome).__name__,
                )
            else:
                value = StorageQuotaProbe(
                    filesystem_type="",
                    error="storage quota prefetch returned no result",
                )
            self._storage_quota_cache[account.name] = (observed_at, value)

    def project_active_cap_reason(self, task: dict) -> str:
        project_name = str(task.get("project") or "").strip()
        if not project_name:
            return ""
        project = self.db.get_project_by_name(project_name)
        limit = max(0, int((project or {}).get("max_active_tasks") or 0))
        if limit <= 0:
            return ""
        active = self.db.count_tasks_by_project(
            project_name,
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
        )
        if active < limit:
            return ""
        return f"project active cap reached for {project_name}: {active}/{limit} attaching+running"

    def standalone_aedt_running_cap_details(self, task: dict) -> dict[str, Any]:
        """Return the exact standalone-AEDT cap scope for ``task``.

        Active includes ATTACHING because the Desktop launch has already been
        reserved at that boundary.  Queued tasks remain logical workload
        capacity and do not consume this physical-AEDT limit.  An exact lane
        match takes precedence over the broad per-project compatibility cap.
        """

        project_name = str(task.get("project") or "").strip()
        if (
            not project_name
            or self.task_is_fea_infra(task)
            or not self.task_is_fea_bursty(task)
            or self.task_aedt_backend(task) != AedtBackend.STANDALONE.value
        ):
            return {"active": 0, "limit": 0, "lane": "", "name_prefix": ""}
        task_name = str(task.get("name") or "")
        for lane in self.standalone_aedt_running_lanes:
            if (
                project_name == lane["project"]
                and self.task_aedt_backend(task) == lane["aedt_backend"]
                and task_name.startswith(str(lane["name_prefix"]))
            ):
                active = self.db.count_active_standalone_fea_tasks_by_scope(
                    project_name,
                    name_prefix=str(lane["name_prefix"]),
                )
                return {
                    "active": active,
                    "limit": int(lane["max_running"]),
                    "lane": str(lane["name"]),
                    "name_prefix": str(lane["name_prefix"]),
                }
        limit = int(
            self.standalone_aedt_max_running_by_project.get(project_name, 0)
        )
        if limit <= 0:
            return {"active": 0, "limit": 0, "lane": "", "name_prefix": ""}
        active = self.db.count_active_standalone_fea_tasks_by_project(
            project_name
        )
        return {"active": active, "limit": limit, "lane": "", "name_prefix": ""}

    def standalone_aedt_running_cap_status(self, task: dict) -> tuple[int, int]:
        """Return ``(active, limit)`` for compatibility with existing callers."""

        details = self.standalone_aedt_running_cap_details(task)
        return int(details["active"]), int(details["limit"])

    def standalone_aedt_running_cap_reason(self, task: dict) -> str:
        details = self.standalone_aedt_running_cap_details(task)
        active = int(details["active"])
        limit = int(details["limit"])
        if limit <= 0 or active < limit:
            return ""
        project_name = str(task.get("project") or "").strip()
        lane = str(details.get("lane") or "")
        lane_context = f" lane {lane}" if lane else ""
        return (
            f"standalone AEDT running cap reached for {project_name}{lane_context}: "
            f"{active}/{limit} attaching+running standalone FEA"
        )

    def assign_queued_task(
        self, task: dict, background: bool = False, fea_baseline_only: bool = False
    ) -> bool:
        if task.get("status") != TaskStatus.QUEUED.value:
            return False
        if self.aedt_backend_block_reason(task):
            return False
        # Normal assignment is serialized by the scheduler loop. The existing
        # same-node web fast path can also call this method; strict enforcement
        # across concurrent callers requires one transactional check/transition.
        if self.project_active_cap_reason(task):
            return False
        if self.standalone_aedt_running_cap_reason(task):
            return False
        if not self.prepare_aedt_backend_task(task):
            return False
        # Pooled pre-admission atomically writes the exact allocation/node pin
        # and the real four-core project request.  Never select from the stale
        # thin-client snapshot supplied by the caller.
        task = self.db.get_task(int(task["id"])) or task
        if task.get("status") != TaskStatus.QUEUED.value:
            return False
        allocation = self.best_allocation_for_task(task, fea_baseline_only=fea_baseline_only)
        if not allocation:
            return False
        account = next((item for item in self.accounts if item.name == allocation["account_name"]), None)
        if not account:
            return False
        if self.account_storage_blocked(account, for_fea=self.task_is_fea_bursty(task)):
            return False
        task = self.reserve_task_on_allocation(task, allocation, account)
        if not task:
            return False
        if background:
            self.start_background_task_attach(task, allocation, account)
            return True
        return self.finish_reserved_task_attach(task, allocation, account)

    def reserve_task_on_allocation(self, task: dict, allocation: dict, account: AccountConfig) -> dict | None:
        with self._task_assignment_lock:
            # The allocation returned by best_allocation_for_task is only a
            # snapshot.  Reload both rows under the assignment lock and, for
            # bursty FEA, recompute dynamic node fit immediately before the
            # optimistic DB claim.  _record_attach_delta invalidates the
            # footprint/pressure caches before the next waiter enters.
            current_task = self.db.get_task(int(task["id"]))
            current_allocation = self.db.get_allocation(int(allocation["id"]))
            if not current_task or current_task.get("status") != TaskStatus.QUEUED.value:
                return None
            if not current_allocation:
                return None
            task = current_task
            allocation = current_allocation
            allocation_account_name = str(allocation.get("account_name") or "")
            if str(account.name) != allocation_account_name:
                account = self.account_by_name(allocation_account_name)
                if account is None:
                    return None
            if self.aedt_backend_block_reason(task):
                return None
            # This is the final, serialized standalone-AEDT admission point.
            # The first claimant becomes ATTACHING before the assignment lock
            # is released, so the next concurrent caller observes the spent
            # project seat rather than racing past the configured boundary.
            if self.standalone_aedt_running_cap_reason(task):
                return None
            if self.allocation_profile_conflicts(allocation, task, refresh=True):
                return None
            if allocation.get("state") not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }:
                return None
            if not allocation.get("slurm_job_id") or not self.allocation_accepts_new_tasks(allocation):
                return None
            active_task_allocation_ids, active_exclusive_allocation_ids = (
                self.active_task_allocation_sets()
            )
            validation_task = task
            if (
                self.task_can_relax_preferred_node(task)
                and str(allocation.get("node_name") or "")
                != str(task.get("node_name") or "")
            ):
                validation_task = self.relaxed_preferred_node_task(task)
            if not self.allocation_can_run_task(
                allocation,
                validation_task,
                include_pending=False,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            ):
                return None
            if self.task_is_fea_bursty(task):
                # The reloaded row does not carry the selection snapshot's
                # transient annotation. Rebuild the node count once for this
                # final transactional claim instead of letting fit_slots do a
                # separate implicit global scan from each helper path.
                self.annotate_fea_node_worker_counts([allocation])
                if self.fit_slots_for_allocation(allocation, task) <= 0:
                    return None

            # This is the serialized, final quota admission point for every
            # FEA task, including standalone pressure-requeues. Selection may
            # have used the sub-second storage-shadow cache; refresh the
            # DB-derived reservations and include this prospective project
            # before changing the row to ATTACHING. Pooled session hosts
            # already reserve their complete project capacity, so they need
            # the refresh but not another prospective unit.
            if (
                self.task_is_fea_bursty(task)
                and self.account_storage_blocked(
                    account,
                    for_fea=True,
                    additional_future_projects=(
                        0 if self.task_is_fea_infra(task) else 1
                    ),
                    refresh_reservations=True,
                )
            ):
                return None

            admitted, admission_reason = self._license_task_admitted_locked(task)
            if not admitted:
                LOGGER.debug("holding task %s for license admission: %s", task["id"], admission_reason)
                return None
            task = self.apply_dynamic_env_profile(task, account)
            attach_token = uuid.uuid4().hex
            claim_fields = dict(
                status=TaskStatus.ATTACHING.value,
                allocation_id=allocation["id"],
                account_name=allocation["account_name"],
                remote_dir="",
                stdout_path="",
                stderr_path="",
                exit_code_path="",
                wrapper_pid="",
                attach_token=attach_token,
                launch_started_at=None,
                failure_message="",
                attached_at="CURRENT_TIMESTAMP",
                started_at=None,
                finished_at=None,
                exit_code=None,
            )
            if self.task_aedt_backend(task) == AedtBackend.POOLED.value:
                try:
                    heartbeat_timeout = max(
                        30,
                        int(
                            self.db.get_setting(
                                "aedt_pool_session_heartbeat_timeout_seconds"
                            )
                            or 600
                        ),
                    )
                except (TypeError, ValueError):
                    heartbeat_timeout = 600
                now_dt = datetime.now(timezone.utc)
                claimed = self.db.update_pooled_task_if_reserved_capacity(
                    int(task["id"]),
                    int(allocation["id"]),
                    heartbeat_cutoff=(
                        now_dt - timedelta(seconds=heartbeat_timeout)
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    now=now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    project_cpus=self.db.aedt_pool_project_cpus(),
                    **claim_fields,
                )
            else:
                claimed = self.db.update_task_if_status(
                    task["id"],
                    [TaskStatus.QUEUED.value],
                    **claim_fields,
                )
            if not claimed:
                return None
            self._license_record_claim_locked(task, attach_token)
            self._record_attach_delta(allocation, task)
            self.recalculate_allocation_capacity({int(allocation["id"])})
            return {
                **task,
                "status": TaskStatus.ATTACHING.value,
                "allocation_id": allocation["id"],
                "account_name": allocation["account_name"],
                "attach_token": attach_token,
                "launch_started_at": None,
            }

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

    def strict_node_attach_readback_error(
        self,
        task: dict,
        allocation: dict,
        *,
        expected_allocation_id: int,
        client: Any,
    ) -> str:
        """Validate an explicit strict placement at the last remote boundary."""

        if not self.task_has_strict_node_contract(task):
            return ""
        expected_node = self.strict_task_node_name(task)
        if not expected_node:
            return "strict node placement has no requested node"
        assigned_allocation_id = int(task.get("allocation_id") or 0)
        actual_allocation_id = int(allocation.get("id") or 0)
        if (
            assigned_allocation_id != expected_allocation_id
            or actual_allocation_id != expected_allocation_id
        ):
            return (
                "strict node placement allocation mismatch "
                f"(expected {expected_allocation_id}, task {assigned_allocation_id}, "
                f"readback {actual_allocation_id})"
            )
        allocation_node = str(allocation.get("node_name") or "").strip()
        if allocation_node != expected_node:
            return (
                "strict node placement DB readback mismatch "
                f"(requested {expected_node}, allocation on {allocation_node or 'unknown'})"
            )
        slurm_job_id = str(allocation.get("slurm_job_id") or "").strip()
        if not slurm_job_id:
            return "strict node placement allocation has no Slurm job id"
        try:
            slurm_node = str(client.allocation_node_name(slurm_job_id) or "").strip()
        except Exception as exc:
            return f"strict node placement Slurm readback failed: {exc}"
        if slurm_node != expected_node:
            return (
                "strict node placement Slurm readback mismatch "
                f"(requested {expected_node}, job {slurm_job_id} on "
                f"{slurm_node or 'unknown'})"
            )
        return ""

    def strict_node_cancellation_is_pending(self, task: dict) -> bool:
        return bool(
            str(task.get("status") or "") == TaskStatus.ATTACHING.value
            and self.task_has_strict_node_contract(task)
            and str(task.get("failure_message") or "").startswith(
                STRICT_NODE_CANCELLATION_PENDING_PREFIX
            )
        )

    def retry_strict_node_cancellation(
        self,
        task: dict,
        account: AccountConfig,
    ) -> bool:
        """Retry a durable post-launch rejection until the worker is stopped.

        A task whose exact-node readback changed after launch must not be
        declared terminal while cancellation itself is unconfirmed. Keeping it
        ATTACHING preserves ownership and makes the retry survive restarts
        instead of releasing capacity around a possible orphan.
        """

        if not self.strict_node_cancellation_is_pending(task):
            return False
        allocation_id = int(task.get("allocation_id") or 0)
        allocation = (
            self.db.get_allocation(allocation_id) if allocation_id else None
        )
        allocation_job_id = str(
            (allocation or {}).get("slurm_job_id")
            or self._task_allocation_job_id(task)
            or ""
        )
        try:
            self._client(account).cancel_task(task, allocation_job_id)
        except Exception as exc:
            message = str(task.get("failure_message") or "")
            detail = message.split("; last cancel error:", 1)[0]
            self.db.update_task_if_attach_claim(
                int(task["id"]),
                str(task.get("attach_token") or ""),
                failure_message=f"{detail}; last cancel error: {exc}",
            )
            LOGGER.warning(
                "strict-placement cancellation remains pending for task %s: %s",
                task["id"],
                exc,
            )
            return False
        pending_message = str(task.get("failure_message") or "")
        placement_error = pending_message.removeprefix(
            STRICT_NODE_CANCELLATION_PENDING_PREFIX
        ).split("; last cancel error:", 1)[0]
        if self.db.update_task_if_attach_claim(
            int(task["id"]),
            str(task.get("attach_token") or ""),
            status=TaskStatus.FAILED.value,
            failure_message=placement_error,
            finished_at="CURRENT_TIMESTAMP",
        ):
            self.on_task_terminal(task, "strict placement rejected")
        self.recalculate_allocation_capacity(
            {allocation_id} if allocation_id else None
        )
        return True

    def finish_reserved_task_attach(self, task: dict, allocation: dict, account: AccountConfig) -> bool:
        """Complete a reserved attach. All task updates are conditional on the
        exact attach token still owning the ATTACHING row. Persisting the launch
        boundary before the remote side effect lets startup distinguish a safe,
        unlaunched reservation from an ambiguous launch that must not be rerun."""
        attach_token = str(task.get("attach_token") or "")
        expected_allocation_id = int(allocation.get("id") or 0)
        current_task = self.db.get_task(int(task["id"])) or task
        current_allocation = (
            self.db.get_allocation(expected_allocation_id)
            if expected_allocation_id
            else None
        )
        client = self._client(account)
        strict_error = (
            self.strict_node_attach_readback_error(
                current_task,
                current_allocation or {},
                expected_allocation_id=expected_allocation_id,
                client=client,
            )
            if self.task_has_strict_node_contract(current_task)
            else ""
        )
        if strict_error:
            if self.db.update_task_if_attach_claim(
                task["id"],
                attach_token,
                status=TaskStatus.FAILED.value,
                failure_message=strict_error,
                finished_at="CURRENT_TIMESTAMP",
            ):
                self.on_task_terminal(current_task, "strict placement rejected")
            self.recalculate_allocation_capacity(
                {expected_allocation_id} if expected_allocation_id else None
            )
            return False
        if not self.db.update_task_if_attach_claim(
            task["id"],
            attach_token,
            require_launch_not_started=True,
            launch_started_at="CURRENT_TIMESTAMP",
        ):
            LOGGER.info("attach claim for task %s is no longer active; skipping remote launch", task["id"])
            self._license_release_prelaunch_claim(task)
            self.recalculate_allocation_capacity({int(allocation["id"])})
            return False
        self._license_mark_launch_started(task)
        try:
            result = client.attach_task(task, current_allocation or allocation)
        except RemoteExecutionError as exc:
            if self.db.update_task_if_attach_claim(
                task["id"],
                attach_token,
                status=TaskStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
                **exc.result_fields,
            ):
                self.on_task_terminal(task, "attach failed")
            else:
                LOGGER.info("attach failure for task %s ignored; task already transitioned", task["id"])
            self.recalculate_allocation_capacity({int(allocation["id"])})
            return False
        except Exception as exc:
            if self.db.update_task_if_attach_claim(
                task["id"],
                attach_token,
                status=TaskStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
            ):
                self.on_task_terminal(task, "attach failed")
            else:
                LOGGER.info("attach failure for task %s ignored; task already transitioned", task["id"])
            self.recalculate_allocation_capacity({int(allocation["id"])})
            return False
        current_task = self.db.get_task(int(task["id"])) or current_task
        current_allocation = (
            self.db.get_allocation(expected_allocation_id)
            if expected_allocation_id
            else None
        )
        strict_error = (
            self.strict_node_attach_readback_error(
                current_task,
                current_allocation or {},
                expected_allocation_id=expected_allocation_id,
                client=client,
            )
            if self.task_has_strict_node_contract(current_task)
            else ""
        )
        if strict_error:
            cancel_error = ""
            try:
                client.cancel_task(
                    {**current_task, **result},
                    str((current_allocation or allocation).get("slurm_job_id") or ""),
                )
            except Exception as exc:
                cancel_error = str(exc) or type(exc).__name__
                LOGGER.warning(
                    "failed to cancel strict-placement mismatch for task %s: %s",
                    task["id"],
                    exc,
                )
            if cancel_error:
                # Do not release an ambiguous remote worker by declaring its
                # task terminal. ATTACHING + launch_started_at is the durable
                # recovery hold, and refresh_tasks retries the cancellation.
                self.db.update_task_if_attach_claim(
                    task["id"],
                    attach_token,
                    failure_message=(
                        f"{STRICT_NODE_CANCELLATION_PENDING_PREFIX}{strict_error}; "
                        f"last cancel error: {cancel_error}"
                    ),
                    **result,
                )
                self.recalculate_allocation_capacity(
                    {expected_allocation_id} if expected_allocation_id else None
                )
                return False
            if self.db.update_task_if_attach_claim(
                task["id"],
                attach_token,
                status=TaskStatus.FAILED.value,
                failure_message=strict_error,
                finished_at="CURRENT_TIMESTAMP",
                **result,
            ):
                self.on_task_terminal(current_task, "strict placement rejected")
            self.recalculate_allocation_capacity(
                {expected_allocation_id} if expected_allocation_id else None
            )
            return False
        if not self.db.update_task_if_attach_claim(
            task["id"],
            attach_token,
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
            self.recalculate_allocation_capacity({int(allocation["id"])})
            return False
        self._license_mark_running(task)
        return True

    def best_allocation_for_task(self, task: dict, fea_baseline_only: bool = False) -> dict | None:
        exact = self.best_allocation_for_effective_task(task, fea_baseline_only=fea_baseline_only)
        if exact or not self.task_can_relax_preferred_node(task):
            return exact
        return self.best_allocation_for_effective_task(
            self.relaxed_preferred_node_task(task), fea_baseline_only=fea_baseline_only
        )

    def best_allocation_for_effective_task(
        self, task: dict, fea_baseline_only: bool = False
    ) -> dict | None:
        cpu_candidates = []
        gpu_candidates = []
        is_fea = self.task_is_fea_bursty(task)
        active_task_allocation_ids, active_exclusive_allocation_ids = self.active_task_allocation_sets()
        allocation_rows = self.db.list_allocations_with_live(limit=500)
        if is_fea:
            # fit_slots_for_allocation needs the physical-node worker count.
            # Annotate the whole candidate set once. Without this, every
            # allocation independently rebuilt the same global task/node map,
            # turning a 64-task refill into thousands of identical DB scans.
            self.annotate_fea_node_worker_counts(allocation_rows)
        for allocation in allocation_rows:
            if allocation["state"] not in {AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}:
                continue
            if not self.allocation_accepts_new_tasks(allocation):
                continue
            if not allocation.get("slurm_job_id"):
                continue
            if is_fea:
                account = self.account_by_name(str(allocation.get("account_name") or ""))
                if not account or self.account_storage_blocked(account, for_fea=True):
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
            if is_fea:
                cap_remaining = self.fea_node_cpu_cap_remaining(allocation, task)
                if cap_remaining is not None and cap_remaining <= 0:
                    continue
                fit_slots = self.fit_slots_for_allocation(allocation, task)
                if fit_slots <= 0:
                    continue
                allocation["_task_fit_slots"] = fit_slots
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
            # A healthy instantaneous pestat row is necessary but not
            # sufficient: fit_slots also discounts young workers' declared
            # footprint and the per-tick attach ledger.  A zero-fit candidate
            # must never remain eligible merely because it sorts last/best.
            candidates = [
                item
                for item in candidates
                if self.fit_slots_for_allocation(item, task) > 0
            ]
            if not candidates:
                return None
            # Baseline-first: every allocation is entitled to one solver per
            # requested-CPU share of its own reservation (1x = owned_cpus /
            # task cpus) before any allocation is overcommitted. Ordering by
            # physical-node worker count treated a 32-core and a 64-core
            # allocation as equals, starving large under-baseline allocations
            # while small ones ran past 1x.
            under_baseline = [
                item for item in candidates if self.fea_baseline_ratio(item) < 1.0
            ]
            if under_baseline:
                # Under license scarcity flip from spread-first (fill the most
                # underfilled allocation) to fill-first (top up the allocation
                # closest to baseline) so scarce licenses concentrate into
                # fully usable allocations instead of spreading thinly.
                headroom = self.fea_license_admit_headroom()
                fill_first = headroom is not None and headroom < len(under_baseline)
                direction = -1.0 if fill_first else 1.0
                chosen = min(
                    under_baseline,
                    key=lambda item: (
                        direction * self.fea_baseline_ratio(item),
                        -int(item.get("_task_fit_slots") or 0),
                        -(self.fea_memory_free_percent(item) or 0.0),
                        -int(item.get("free_memory_mb") or 0),
                    ),
                )
                self._fea_last_attach_baseline = True
                return chosen
            if fea_baseline_only:
                return None
            self._fea_last_attach_baseline = False
            return max(
                candidates,
                key=lambda item: (
                    -self.allocation_worker_count_for_task(item, task),
                    int(item.get("_task_fit_slots") or 0),
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
            for task in self.db.list_tasks_by_statuses(
                [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
            )
            if int(task.get("allocation_id") or 0) == allocation_id
            and task["status"] in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        )

    def node_worker_counts(self) -> dict[str, int]:
        allocation_node_by_id = {
            int(allocation["id"]): str(allocation.get("node_name") or "")
            for allocation in self.db.list_allocations_with_live(limit=500)
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
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            node_name = allocation_node_by_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            counts[node_name] = counts.get(node_name, 0) + 1
        return counts

    def node_fea_worker_counts(self) -> dict[str, int]:
        cached = self._fea_worker_counts_cache
        scheduler_tick_thread = getattr(self._tick_local, "cache", None) is not None
        if (
            scheduler_tick_thread
            and cached is not None
            and cached[0] == self._tick_seq
        ):
            return cached[1]
        allocation_node_by_id = {
            int(allocation["id"]): str(allocation.get("node_name") or "")
            for allocation in self.db.list_allocations_with_live(limit=500)
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
        project_cpus = self.db.aedt_pool_project_cpus()
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            if self.task_is_fea_infra(task):
                continue
            if (
                self.task_aedt_backend(task) == AedtBackend.POOLED.value
                and int(task.get("cpus") or 0) < project_cpus
            ):
                continue
            node_name = allocation_node_by_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            counts[node_name] = counts.get(node_name, 0) + 1
        for allocation_id, usage in self.db.aedt_project_pressure_by_allocation().items():
            node_name = allocation_node_by_id.get(int(allocation_id))
            if not node_name:
                continue
            counts[node_name] = counts.get(node_name, 0) + int(
                usage.get("workers") or 0
            )
        if scheduler_tick_thread:
            self._fea_worker_counts_cache = (self._tick_seq, counts)
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
        if "_allocation_worker_count" in allocation:
            return int(allocation.get("_allocation_worker_count") or 0)
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
            allocation_rows = self.db.list_allocations_with_live(limit=500)
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
        for allocation in allocation_rows if allocation_rows is not None else self.db.list_allocations_with_live(limit=500):
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
        requested = self.requested_accounts(self.task_requested_account_name(task))
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
                "scheduling_profile", "aedt_backend", "required_capability", "env_profile", "account_name",
                "requested_allocation_id",
            )},
            "queue_state": diagnostics.get("queue_state"),
            "queue_reason": diagnostics.get("queue_reason"),
            "capacity": capacity,
            "accounts": accounts,
            "allocations": allocations,
        }

    def allocation_rejection_reasons(self, allocation: dict, task: dict) -> list[str]:
        reasons: list[str] = []
        requested_allocation_id = self.task_requested_allocation_id(task)
        if requested_allocation_id and int(allocation.get("id") or 0) != requested_allocation_id:
            return [f"task requests allocation {requested_allocation_id}"]
        if (
            self.task_is_fea_bursty(task)
            and not self.task_requires_gpu(task)
            and self.allocation_has_gpu_pool(allocation)
        ):
            return ["CPU-only FEA cannot claim a GPU allocation pool"]
        if self.allocation_profile_conflicts(allocation, task):
            wanted = self.task_allocation_profile(task)
            demand_profile = self.allocation_demand_profile(allocation)
            if demand_profile and demand_profile != wanted:
                return [
                    f"CPU allocation is reserved for queued {demand_profile} demand"
                ]
            occupied = "standard" if wanted == "fea" else "fea"
            return [f"CPU allocation is occupied by active {occupied} tasks"]
        if self.task_is_fea_bursty(task):
            if allocation.get("state") != AllocationStatus.PENDING.value:
                node = self.pestat_node_for_allocation(allocation)
                if node is None:
                    reasons.append("pestat stale or missing; shared-memory FEA admission is fail-closed")
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
        # License admission blocks trump fit capacity: a task the admission
        # gate refuses would otherwise report a misleading "ready" state.
        if str(task.get("status") or "queued") == TaskStatus.QUEUED.value:
            backend_reason = self.aedt_backend_block_reason(task)
            if backend_reason:
                return {
                    "queue_state": "blocked",
                    "queue_reason": f"AEDT backend: {backend_reason}",
                    "preferred_node_relaxed": False,
                }
            _costs, admission_error = self._license_costs_for_task(task)
            if admission_error:
                return {
                    "queue_state": "blocked",
                    "queue_reason": (
                        f"license admission: {admission_error} — set the task's project field "
                        "or add the project to license_monitor.admission.persistent_cost_by_project"
                    ),
                    "preferred_node_relaxed": False,
                }
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
        requested_allocation_id = self.task_requested_allocation_id(task)
        requested_allocation = (
            self.db.get_allocation(requested_allocation_id)
            if requested_allocation_id
            else None
        )

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

        project_cap_reason = self.project_active_cap_reason(task)
        if project_cap_reason:
            queue_state = "blocked"
            reason = project_cap_reason

        standalone_aedt_details = self.standalone_aedt_running_cap_details(task)
        standalone_aedt_active = int(standalone_aedt_details["active"])
        standalone_aedt_limit = int(standalone_aedt_details["limit"])
        if (
            standalone_aedt_limit > 0
            and standalone_aedt_active >= standalone_aedt_limit
        ):
            queue_state = "blocked"
            reason = self.standalone_aedt_running_cap_reason(task)

        if not reason and requested_allocation_id:
            if not requested_allocation:
                queue_state = "blocked"
                reason = f"requested allocation {requested_allocation_id} not found"
            elif requested_allocation.get("state") not in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }:
                queue_state = "blocked"
                reason = (
                    f"requested allocation {requested_allocation_id} is "
                    f"{requested_allocation.get('state') or 'unavailable'}"
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

        diagnostics = {
            "ready_fit_slots": ready_slots,
            "pending_fit_slots": pending_slots,
            "inflight_fit_slots": inflight_slots,
            "queue_state": queue_state,
            "queue_reason": reason,
            "preferred_node_relaxed": preferred_node_relaxed,
        }
        if standalone_aedt_limit > 0:
            diagnostics.update(
                {
                    "standalone_aedt_active": standalone_aedt_active,
                    "standalone_aedt_max_running": standalone_aedt_limit,
                    "standalone_aedt_available": max(
                        0, standalone_aedt_limit - standalone_aedt_active
                    ),
                }
            )
            if standalone_aedt_details.get("lane"):
                diagnostics.update(
                    {
                        "standalone_aedt_lane": standalone_aedt_details["lane"],
                        "standalone_aedt_name_prefix": standalone_aedt_details[
                            "name_prefix"
                        ],
                    }
                )
        return diagnostics

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
        rows = allocation_rows if allocation_rows is not None else self.db.list_allocations_with_live(limit=500)
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
        for allocation in allocation_rows if allocation_rows is not None else self.db.list_allocations_with_live(limit=500):
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
        requested_accounts = self.requested_accounts(self.task_requested_account_name(task))
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
        if self.same_node_as_task_id(task) or self.task_requested_allocation_id(task):
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
            requested_node_name=self.strict_task_node_name(task),
            requested_partition=(
                str(task.get("partition") or "auto")
                if self.task_has_strict_node_contract(task)
                else "auto"
            ),
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
            if self.task_is_fea_infra(task):
                # The AEDT pool has already pinned this host to an exact
                # dedicated allocation after _allocation_session_capacity
                # reserved the aggregate CPU and memory for every session.
                # Solver load/footprint gates apply to project workers, not to
                # the control-plane task that starts their Desktop.
                return 1
            if self.task_uses_reserved_aedt_pool_capacity(allocation, task):
                # Exact pooled admission has already reserved one of the three
                # project slots on a healthy Desktop in this allocation.  The
                # Slurm allocation itself owns the matching 4-CPU/32-GiB
                # footprint as part of the complete 13-CPU/96-GiB session
                # shape.  Reapplying the generic FEA load/ramp gate here double
                # counts that reservation and can strand ready Desktop slots.
                # The final transactional claim still revalidates the exact
                # session generation, profile, heartbeat, allocation and
                # per-allocation density before any remote launch.
                return 1
            if allocation.get("state") != AllocationStatus.PENDING.value and not self.fea_allocation_accepts_task(allocation):
                return 0
            slots = self.fea_max_attach_per_loop
            if (
                allocation.get("state") == AllocationStatus.PENDING.value
                and (allocation.get("resource_pool") or "cpu") == "cpu"
                and self.fea_node_requested_cpu_factor > 0
            ):
                # A new FEA pool starts at one worker-core budget per owned
                # core.  The configured factor is the later load-driven
                # ceiling, not an instruction to fill the overcommit margin
                # before we have observed CPU/RAM headroom.
                startup_factor = min(1.0, self.fea_node_requested_cpu_factor)
                cpu_cap = int(int(allocation.get("total_cpus") or 0) * startup_factor)
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
                # max_workers_per_node is a soft baseline for bursty FEA, not
                # permission to bypass measured shared-memory capacity. The
                # dynamic result fills the allocation-owned 1x CPU baseline
                # first, then limits only the overcommit region to +2/tick.
                measured_ramp_slots = self.fea_dynamic_extra_slots(allocation, task)
                slots = min(slots, measured_ramp_slots)
                reserved_slots = 0
            elif max_workers > 0:
                slots = min(slots, max(0, max_workers - self.allocation_worker_count(int(allocation["id"]))))
            if self.task_requires_gpu(task):
                gpu_slots = int(allocation.get("free_gpus") or 0) // max(1, int(task.get("gpus") or 1))
                slots = min(slots, gpu_slots)
            if self.task_aedt_backend(task) == AedtBackend.POOLED.value:
                project_cpus = self.db.aedt_pool_project_cpus()
                density_cap = int(allocation.get("total_cpus") or 0) // project_cpus
                active = self.db.active_pooled_aedt_tasks_by_allocation().get(
                    int(allocation.get("id") or 0), 0
                )
                reserved = int(
                    allocation.get("_reserved_pooled_aedt_client_slots") or 0
                )
                slots = min(slots, max(0, density_cap - active - reserved))
            node_shadow_slots = self.fea_node_shadow_cap_remaining(
                allocation,
                task,
                reservation_allocations=reservation_allocations,
            )
            if node_shadow_slots is not None:
                slots = min(slots, node_shadow_slots)
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
        for allocation in self.db.list_allocations_with_live(limit=500):
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

    def set_aedt_backend_admission_checker(
        self, checker: Callable[[dict], tuple[bool, str]] | None
    ) -> None:
        self._aedt_backend_admission_checker = checker

    def set_aedt_backend_task_preparer(
        self, preparer: Callable[[dict], tuple[bool, str]] | None
    ) -> None:
        """Install the side-effecting pooled pre-admission hook.

        Unlike the pure admission checker used by dashboards and planning,
        this hook is called only from the actual queued-task assignment path.
        """

        self._aedt_backend_task_preparer = preparer

    def prepare_aedt_backend_task(self, task: dict) -> bool:
        if self.task_aedt_backend(task) == AedtBackend.STANDALONE.value:
            return True
        preparer = self._aedt_backend_task_preparer
        if preparer is None:
            return False
        try:
            prepared, reason = preparer(task)
        except Exception:
            LOGGER.exception("AEDT pooled backend pre-admission failed")
            return False
        if not prepared:
            LOGGER.debug(
                "holding pooled task %s before launch: %s",
                task.get("id"),
                str(reason or "no healthy AEDT session slot is ready"),
            )
        return bool(prepared)

    def task_aedt_backend(self, task: dict) -> str:
        return normalize_aedt_backend(str(task.get("aedt_backend") or ""))

    def aedt_backend_block_reason(self, task: dict) -> str:
        if self.task_aedt_backend(task) == AedtBackend.STANDALONE.value:
            return ""
        checker = self._aedt_backend_admission_checker
        if checker is None:
            return "AEDT pooled backend admission is not configured"
        try:
            allowed, reason = checker(task)
        except Exception as exc:
            LOGGER.exception("AEDT pooled backend admission check failed")
            return f"AEDT pooled backend admission check failed: {exc}"
        return "" if allowed else (str(reason).strip() or "AEDT pooled backend is unavailable")

    def task_aedt_backend_admitted(self, task: dict) -> bool:
        return not self.aedt_backend_block_reason(task)

    def task_is_fea_bursty(self, task: dict) -> bool:
        return self.task_scheduling_profile(task) == SchedulingProfile.FEA_BURSTY.value

    def task_is_fea_infra(self, task: dict) -> bool:
        """AEDT pool session hosts ride the FEA profile but are not solver
        workers; they are excluded from the per-allocation solver baseline and
        requested-CPU accounting so a 64-core reservation keeps its full
        floor(64/4)=16 solver budget."""
        return str(task.get("project") or "").strip() == "_aedt_pool_hosts"

    @staticmethod
    def allocation_is_dedicated_aedt_pool(allocation: dict) -> bool:
        """Return whether an allocation is logically owned by the AEDT pool.

        Several 64-CPU allocations may share one 256-CPU physical node, but an
        individual allocation is shared only by one or more Desktop hosts and
        their exact-reserved pooled clients.  Unrelated scheduler work must use
        another allocation even when it lands on the same physical node.
        """

        return str(allocation.get("drain_reason") or "").startswith("AEDT pool")

    def task_can_share_dedicated_aedt_pool(
        self, allocation: dict, task: dict
    ) -> bool:
        """Admit only pool-owned tasks to a dedicated AEDT allocation."""

        allocation_id = int(allocation.get("id") or 0)
        if allocation_id <= 0:
            return False
        if self.task_requested_allocation_id(task) != allocation_id:
            return False
        if self.task_is_fea_infra(task):
            return True
        task_id = int(task.get("id") or 0)
        return bool(
            task_id > 0
            and self.task_aedt_backend(task) == AedtBackend.POOLED.value
            and self.db.task_has_auto_aedt_reservation(task_id)
        )

    def task_uses_reserved_aedt_pool_capacity(
        self, allocation: dict, task: dict
    ) -> bool:
        """Whether an exact pooled client consumes pre-reserved pool capacity.

        This is deliberately narrower than merely using the pooled backend: an
        automatic exact-session reservation, its requested allocation pin, and
        a dedicated AEDT allocation must all agree.  The database's atomic
        queued-to-attaching claim performs the authoritative live validation.
        """

        return bool(
            self.allocation_is_dedicated_aedt_pool(allocation)
            and self.task_can_share_dedicated_aedt_pool(allocation, task)
        )

    def explicit_task_node_name_policy(self, task: dict) -> str:
        """Return only the durable per-task override, never the global default."""

        try:
            return normalize_node_name_policy(
                str(task.get("node_name_policy") or "")
            )
        except ValueError:
            # Database/API writers validate this field.  A hand-edited or
            # corrupt row must not accidentally gain preferred-node fallback.
            return NodeNamePolicy.STRICT.value

    def task_node_name_policy(self, task: dict) -> str:
        """Return the effective node policy without changing legacy behavior."""

        explicit = self.explicit_task_node_name_policy(task)
        if explicit:
            return explicit
        if self.task_is_fea_bursty(task):
            return self.fea_node_name_policy
        # Historically node_name is a hard constraint outside fea_bursty.
        return (
            NodeNamePolicy.STRICT.value
            if str(task.get("node_name") or "").strip()
            else ""
        )

    def task_has_strict_node_contract(self, task: dict) -> bool:
        """Whether the API caller explicitly opted into fail-closed placement."""

        return bool(
            self.explicit_task_node_name_policy(task)
            == NodeNamePolicy.STRICT.value
        )

    def strict_task_node_name(self, task: dict) -> str:
        if not self.task_has_strict_node_contract(task):
            return ""
        return str(task.get("node_name") or "").strip()

    def task_can_relax_preferred_node(self, task: dict) -> bool:
        return (
            self.task_node_name_policy(task) == NodeNamePolicy.PREFERRED.value
            and self.task_is_fea_bursty(task)
            and bool(str(task.get("node_name") or "").strip())
            and not int(task.get("same_node_as_task_id") or 0)
            and not self.task_requested_allocation_id(task)
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
        if self._last_tick_duration is not None:
            # The refresh runs inside the tick, so a heavy tick stretches the
            # gap between observations past any fixed threshold. Scale the
            # tolerance with the observed tick time instead of fail-closing
            # every admission decision whenever ticks run long.
            candidates.append(
                float(self.cluster_refresh_interval_seconds) + 2.0 * self._last_tick_duration
            )
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
        self._active_profile_sets_cache = None
        node_name = str(allocation.get("node_name") or "").strip()
        if not node_name:
            return
        self._tick_attach_workers_by_node[node_name] = self._tick_attach_workers_by_node.get(node_name, 0) + 1
        if node_name in self._tick_adaptive_memory_nodes:
            # Memory admission on this node currently comes from the adaptive
            # relax; every attach spends its observed-slack budget.
            ledger = self._adaptive_memory_attaches.setdefault(node_name, deque())
            now = time.monotonic()
            ledger.append(now)
            horizon = now - self.fea_adaptive_memory_window_seconds
            while ledger and ledger[0] < horizon:
                ledger.popleft()
        # New ATTACHING rows change both the pressure and young-footprint views.
        self._fea_worker_counts_cache = None
        self._fea_pressures_cache = None
        self._fea_alloc_pressures_cache = None
        self._fea_node_resources_cache = None
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

    def fea_memory_pressure_observation(
        self, allocation: dict
    ) -> dict[str, Any] | None:
        """Return a fresh node-memory sample and its hard-pressure state."""

        row = self.pestat_node_for_allocation(allocation)
        if not row:
            return None
        total = int(row.get("memory_mb") or 0)
        if total <= 0:
            return None
        free = int(row.get("free_memory_mb") or 0)
        observed_at = str(row.get("observed_at") or "").strip()
        if not observed_at:
            return None
        return {
            "observed_at": observed_at,
            "total_memory_mb": total,
            "free_memory_mb": free,
            "hard_pressure": (free / total) * 100.0
            < self.fea_hard_memory_free_percent,
        }

    def _alloc_util_max_age_seconds(self) -> float:
        # Samples land once per tick, and heavy ticks stretch well past the
        # nominal interval; utilization drifts slowly, so a sample within ten
        # minutes still beats falling back to co-tenant node loadavg.
        return max(3.0 * self.fea_alloc_util_sample_interval_seconds, 600.0)

    def _alloc_util_fresh(self, allocation: dict) -> dict | None:
        """Fresh allocation-local CPU sample (sstat step deltas), or None."""
        util_info = self._alloc_cpu_util.get(int(allocation.get("id") or 0))
        if (
            self.fea_alloc_util_enabled
            and util_info
            and time.monotonic() - util_info["at"] <= self._alloc_util_max_age_seconds()
        ):
            return util_info
        return None

    def fea_node_load_ok(self, allocation: dict) -> bool:
        node = self.pestat_node_for_allocation(allocation)
        if not node:
            return False
        cpu_total = max(1, int(node.get("cpu_total") or 1))
        node_load = float(node.get("cpu_load") or 0.0)
        if self._alloc_util_fresh(allocation) is not None:
            # Slurm reserved these cores for us; co-tenants overcommitting the
            # rest of the node must not veto admission. How much we add is
            # governed by the allocation-local sstat budget in
            # fea_dynamic_extra_slots. Keep only an extreme node-thrash stop.
            return node_load <= cpu_total * 2.0
        return node_load <= cpu_total * self.fea_load_target

    def fea_allocation_accepts_task(self, allocation: dict) -> bool:
        if self.pestat_node_for_allocation(allocation) is None:
            # Shared-memory FEA has no per-step cgroup ceiling. Never admit a
            # new worker from stale/missing node metrics, even if the previous
            # sample was healthy.
            return False
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
        for allocation in self.db.list_allocations_with_live(limit=500):
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
            owned_cpus = int(allocation.get("total_cpus") or 0)
            if owned_cpus > 0:
                owned_cpus_by_node[node_name] = owned_cpus_by_node.get(node_name, 0) + int(
                    owned_cpus
                )

        requested_cpus_by_node: dict[str, int] = {}
        workers_by_node: dict[str, int] = {}
        project_cpus = self.db.aedt_pool_project_cpus()
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            if self.task_is_fea_infra(task):
                continue
            if (
                self.task_aedt_backend(task) == AedtBackend.POOLED.value
                and int(task.get("cpus") or 0) < project_cpus
            ):
                continue
            node_name = allocation_node_by_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            workers_by_node[node_name] = workers_by_node.get(node_name, 0) + 1
            requested_cpus_by_node[node_name] = requested_cpus_by_node.get(node_name, 0) + int(
                task.get("cpus") or 0
            )

        # A pooled AEDT client's task is deliberately thin (normally one CPU),
        # but its Maxwell/Icepak work executes inside the long-lived Desktop on
        # the session's allocation.  Charge that host allocation once per
        # accepted/live project lease instead of charging the 12-CPU session
        # infrastructure task, which would undercount a full three-project
        # session and overcount an idle one.
        for allocation_id, usage in self.db.aedt_project_pressure_by_allocation().items():
            node_name = allocation_node_by_id.get(int(allocation_id))
            if not node_name:
                continue
            workers_by_node[node_name] = workers_by_node.get(node_name, 0) + int(
                usage.get("workers") or 0
            )
            requested_cpus_by_node[node_name] = (
                requested_cpus_by_node.get(node_name, 0)
                + int(usage.get("shadow_cpus") or 0)
            )

        pressures: dict[str, dict[str, int]] = {}
        for node_name in set(workers_by_node) | set(requested_cpus_by_node):
            pressures[node_name] = {
                "workers": workers_by_node.get(node_name, 0),
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
        # FEA cap is per Slurm allocation (job): every CPU or GPU allocation
        # reserves its own CPUs and tasks attach via srun --jobid. Keying
        # per node would inflate the cap when several cpu2 allocations share a
        # node (n allocations -> owned = n*64), letting FEA overshoot a single
        # allocation's reservation.
        owned_by_alloc: dict[int, int] = {}
        for allocation in self.db.list_allocations_with_live(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            owned = int(allocation.get("total_cpus") or 0)
            if owned <= 0:
                continue
            owned_by_alloc[int(allocation["id"])] = owned
        requested_by_alloc: dict[int, int] = {}
        workers_by_alloc: dict[int, int] = {}
        project_cpus = self.db.aedt_pool_project_cpus()
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            if self.task_is_fea_infra(task):
                continue
            if (
                self.task_aedt_backend(task) == AedtBackend.POOLED.value
                and int(task.get("cpus") or 0) < project_cpus
            ):
                continue
            alloc_id = int(task.get("allocation_id") or 0)
            if alloc_id not in owned_by_alloc:
                continue
            workers_by_alloc[alloc_id] = workers_by_alloc.get(alloc_id, 0) + 1
            requested_by_alloc[alloc_id] = requested_by_alloc.get(alloc_id, 0) + int(task.get("cpus") or 0)
        for alloc_id, usage in self.db.aedt_project_pressure_by_allocation().items():
            if int(alloc_id) not in owned_by_alloc:
                continue
            workers_by_alloc[int(alloc_id)] = (
                workers_by_alloc.get(int(alloc_id), 0)
                + int(usage.get("workers") or 0)
            )
            requested_by_alloc[int(alloc_id)] = (
                requested_by_alloc.get(int(alloc_id), 0)
                + int(usage.get("shadow_cpus") or 0)
            )
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
                for task in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if task["status"] == TaskStatus.QUEUED.value
                and self.task_is_fea_bursty(task)
                and not int(task.get("exclusive_node") or 0)
                and not self.task_requires_gpu(task)
                and not self.same_node_as_task_id(task)
                and self.task_aedt_backend_admitted(task)
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
            account_name=self.task_requested_account_name(task),
            require_fea_eligible_node=True,
        )
        if not allocation:
            return False
        self._fea_overload_scaled_nodes.add(node_name)
        return True

    def fea_shared_memory_estimate_mb(self, task: dict) -> int:
        """Conservative growth estimate for one worker in the shared pool.

        ``task.memory_mb`` remains its peak/safety metadata. Reserving that
        whole peak for every overlapping young FEA worker defeated bursty
        scheduling, so admission combines live pestat Freemem with a bounded
        fraction of the peak. The node soft/hard floors remain authoritative.
        """
        peak_mb = max(1, int(task.get("memory_mb") or 1))
        fractional_mb = int((peak_mb * self.fea_shared_memory_estimate_fraction) + 0.999999)
        return min(peak_mb, max(self.fea_shared_memory_min_estimate_mb, fractional_mb))

    def fea_immature_footprint(self, node_name: str) -> dict[str, float]:
        """Estimated not-yet-mature footprint of young FEA workers.

        Memory stays reserved through the late-stage growth window. Declared
        CPU is reserved only for a short observation window; after that fresh
        load samples already include the worker.
        """
        empty = {"cpus": 0.0, "memory_mb": 0.0}
        if (
            self.fea_footprint_maturity_seconds <= 0
            and self.fea_cpu_footprint_maturity_seconds <= 0
        ) or not node_name:
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
            for allocation in self.db.list_allocations_with_live(limit=500)
            if allocation["state"] in live_states
        }
        now = self._now()
        out: dict[str, dict[str, float]] = {}
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
            if task["status"] not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            if (
                self.task_aedt_backend(task) == AedtBackend.POOLED.value
                and int(task.get("cpus") or 0)
                < self.db.aedt_pool_project_cpus()
            ):
                continue
            node_name = node_by_allocation_id.get(int(task.get("allocation_id") or 0))
            if not node_name:
                continue
            started = self._timestamp(
                task.get("attached_at") or task.get("started_at") or task.get("created_at")
            )
            # Unknown start time counts as young: overcounting is the safe side.
            age_seconds = None if started is None else max(0.0, (now - started).total_seconds())
            cpu_young = self.fea_cpu_footprint_maturity_seconds > 0 and (
                age_seconds is None or age_seconds < self.fea_cpu_footprint_maturity_seconds
            )
            memory_young = self.fea_footprint_maturity_seconds > 0 and (
                age_seconds is None or age_seconds < self.fea_footprint_maturity_seconds
            )
            if not cpu_young and not memory_young:
                continue
            entry = out.setdefault(node_name, {"cpus": 0.0, "memory_mb": 0.0})
            if cpu_young:
                entry["cpus"] += float(task.get("cpus") or 0)
            if memory_young:
                entry["memory_mb"] += float(self.fea_shared_memory_estimate_mb(task))
        return out

    def _record_node_memory_history(self, nodes: list) -> None:
        """Rolling per-node pestat free-memory samples; the evidence base for
        the adaptive memory relax (fea_adaptive_memory_*)."""
        now = time.monotonic()
        horizon = now - self.fea_adaptive_memory_window_seconds
        for node in nodes:
            hostname = str(getattr(node, "hostname", "") or "")
            if not hostname:
                continue
            history = self._node_memory_history.setdefault(hostname, deque())
            history.append((now, int(getattr(node, "free_memory_mb", 0) or 0)))
        for hostname in list(self._node_memory_history):
            history = self._node_memory_history[hostname]
            while history and history[0][0] < horizon:
                history.popleft()
            if not history:
                del self._node_memory_history[hostname]

    def fea_adaptive_memory_slots(
        self,
        node_name: str,
        memory_total: int,
        soft_floor: int,
        memory_estimate_mb: int,
        node_tick_attaches: int,
        trace: dict | None = None,
    ) -> int:
        """Evidence-based relax for a zero memory budget. The young-worker
        reservation is a pessimistic prediction; when a full observation
        window shows the node's worst free memory held a margin above the
        soft floor anyway, admit a slow trickle. Every admitted worker
        eventually shows up in pestat free memory, so the window minimum
        walks down toward the floor and the relax shuts itself off."""
        record = trace.update if trace is not None else (lambda _values: None)
        if not self.fea_adaptive_memory_relax_enabled:
            return 0
        history = self._node_memory_history.get(node_name)
        if not history:
            record({"adaptive_memory_state": "no observation history yet"})
            return 0
        now = time.monotonic()
        coverage = now - history[0][0]
        if coverage < self.fea_adaptive_memory_min_coverage_seconds:
            record(
                {
                    "adaptive_memory_state": "insufficient coverage",
                    "adaptive_memory_coverage_seconds": int(coverage),
                }
            )
            return 0
        frees = [free for _, free in history]
        min_free = min(frees)
        avg_free = sum(frees) / len(frees)
        margin_mb = int(memory_total * (self.fea_adaptive_memory_margin_percent / 100.0))
        record(
            {
                "adaptive_memory_coverage_seconds": int(coverage),
                "adaptive_memory_min_free_mb": int(min_free),
                "adaptive_memory_avg_free_mb": int(avg_free),
                "adaptive_memory_margin_mb": margin_mb,
            }
        )
        if min_free < soft_floor + margin_mb or avg_free < soft_floor + 2 * margin_mb:
            record({"adaptive_memory_state": "observed history too close to the soft floor"})
            return 0
        budget_mb = min_free - soft_floor - margin_mb
        total_slots = budget_mb // max(1, int(memory_estimate_mb))
        ledger = self._adaptive_memory_attaches.get(node_name)
        horizon = now - self.fea_adaptive_memory_window_seconds
        spent = sum(1 for at in ledger if at >= horizon) if ledger else 0
        available = max(0, int(total_slots) - spent)
        slots = max(
            0,
            min(available, self.fea_adaptive_memory_max_attach_per_tick - node_tick_attaches),
        )
        record(
            {
                "adaptive_memory_state": "granting" if slots > 0 else "budget spent this window",
                "adaptive_memory_budget_mb": int(budget_mb),
                "adaptive_memory_window_slots": int(total_slots),
                "adaptive_memory_spent_slots": int(spent),
                "adaptive_memory_slots": int(slots),
            }
        )
        return slots

    def fea_dynamic_extra_slots(self, allocation: dict, task: dict, trace: dict | None = None) -> int:
        """FEA attach budget for one allocation. When `trace` is supplied it is
        filled with every intermediate so the node detail page can explain why
        the number is what it is — keep the trace writes in lockstep with the
        actual computation."""
        record = trace.update if trace is not None else (lambda _values: None)
        if allocation.get("state") == AllocationStatus.PENDING.value:
            record({"blocked_by": "", "note": "pending allocation: counts as future capacity"})
            return self.fea_max_attach_per_loop
        node_name = str(allocation.get("node_name") or "").strip()
        node_tick_attaches = self._tick_attach_workers_by_node.get(node_name, 0) if node_name else 0
        record(
            {
                "tick_attaches": node_tick_attaches,
                "baseline_tick_attach_cap": self.fea_max_attach_per_node_per_loop,
                "overcommit_tick_attach_cap": 2,
            }
        )
        node = self.pestat_node_for_allocation(allocation)
        if not node:
            # No fresh MemAvailable/Freemem observation means no safe shared-
            # pool admission decision.
            record({"blocked_by": "pestat stale or missing; shared-memory admission is fail-closed"})
            return 0
        if not self.fea_allocation_accepts_task(allocation):
            record(
                {
                    "blocked_by": "allocation not accepting FEA tasks",
                    "memory_pressure_state": self.fea_memory_pressure_state(allocation),
                    "node_load_ok": self.fea_node_load_ok(allocation),
                    "sustained_overload": self.fea_allocation_sustained_overloaded(allocation),
                }
            )
            return 0
        # Pestat Freemem is the observed shared-pool authority. Discount a
        # conservative growth estimate for workers still maturing because FEA
        # consumes CPU/RAM late during mesh refinement.
        footprint = self.fea_immature_footprint(node_name)
        memory_total = max(1, int(node.get("memory_mb") or 1))
        memory_free = max(0, int(node.get("free_memory_mb") or 0) - int(footprint.get("memory_mb") or 0))
        soft_floor = int(memory_total * (self.fea_soft_memory_free_percent / 100.0))
        memory_budget = max(0, memory_free - soft_floor)
        memory_estimate_mb = self.fea_shared_memory_estimate_mb(task)
        memory_slots = memory_budget // memory_estimate_mb
        if memory_slots <= 0:
            # Prediction says no room; check whether a full hour of observed
            # free-memory history contradicts it before giving up.
            adaptive_slots = self.fea_adaptive_memory_slots(
                node_name,
                memory_total,
                soft_floor,
                memory_estimate_mb,
                node_tick_attaches,
                trace,
            )
            if adaptive_slots > 0:
                memory_slots = adaptive_slots
                self._tick_adaptive_memory_nodes.add(node_name)
        record(
            {
                "footprint_young_cpus": round(float(footprint.get("cpus") or 0.0), 1),
                "footprint_young_memory_mb": int(footprint.get("memory_mb") or 0),
                "memory_free_mb": int(node.get("free_memory_mb") or 0),
                "memory_soft_floor_mb": soft_floor,
                "memory_budget_mb": memory_budget,
                "memory_estimate_per_worker_mb": memory_estimate_mb,
                "memory_slots": int(memory_slots),
            }
        )

        # The first stage fills CPUs actually owned by this Slurm allocation
        # up to 1x requested CPU. It is not overcommit, so the +2 ramp applies
        # only after this per-allocation baseline is full. Fresh memory data
        # and the soft floor still gate every baseline attach.
        baseline_slots = self.fea_allocation_baseline_slots_remaining(allocation, task)
        if baseline_slots > 0:
            baseline_slots_left = self.fea_max_attach_per_node_per_loop - node_tick_attaches
            slots = max(
                0,
                min(
                    memory_slots,
                    baseline_slots,
                    self.fea_max_attach_per_loop,
                    baseline_slots_left,
                ),
            )
            limiter = "memory budget" if memory_slots <= baseline_slots else "1x CPU baseline"
            if slots == baseline_slots_left and slots < min(memory_slots, baseline_slots):
                limiter = "baseline per-tick node attach cap"
            record(
                {
                    "baseline_slots_remaining": baseline_slots,
                    "slots": slots,
                    "limiter": limiter,
                    "cpu_mode": "allocation-owned 1x baseline",
                }
            )
            return slots

        util_info = self._alloc_util_fresh(allocation)
        if util_info is not None:
            # Allocation-local control: how busy are OUR reserved cores (sstat
            # step CPU deltas), independent of co-tenant load on the node. Keep
            # utilization at the configured target by budgeting the difference.
            alloc_cpus = max(1, int(allocation.get("total_cpus") or 1))
            # The fresh sstat delta already measures these workers. Adding the
            # young declared CPUs again double-counted them and blocked all
            # overcommit for the one-hour RAM maturity window.
            busy_cores = float(util_info["busy_cores"])
            load_budget = max(0.0, (alloc_cpus * self.fea_alloc_util_target) - busy_cores)
            record(
                {
                    "cpu_mode": "allocation-local (sstat)",
                    "measured_busy_cores": round(float(util_info["busy_cores"]), 2),
                    "cpu_target_cores": round(alloc_cpus * self.fea_alloc_util_target, 1),
                    "cpu_budget_cores": round(load_budget, 1),
                }
            )
        else:
            cpu_total = max(1, int(node.get("cpu_total") or 1))
            cpu_load = max(0.0, float(node.get("cpu_load") or 0.0)) + float(footprint.get("cpus") or 0.0)
            load_budget = max(0.0, (cpu_total * self.fea_load_target) - cpu_load)
            record(
                {
                    "cpu_mode": "node loadavg fallback (no fresh sstat sample)",
                    "node_load": round(float(node.get("cpu_load") or 0.0), 2),
                    "cpu_target_cores": round(cpu_total * self.fea_load_target, 1),
                    "cpu_budget_cores": round(load_budget, 1),
                }
            )
        cpu_slots = int(load_budget // max(1, int(task.get("cpus") or 1)))

        # Even when both CPU and memory are idle, observe the next samples
        # before adding more than two workers to one physical node per tick.
        ramp_slots_left = 2 - node_tick_attaches
        slots = max(
            0,
            min(
                memory_slots,
                cpu_slots,
                self.fea_max_attach_per_loop,
                ramp_slots_left,
            ),
        )
        limiter = "memory budget" if memory_slots <= cpu_slots else "cpu budget"
        if slots == self.fea_max_attach_per_loop:
            limiter = "per-loop attach cap"
        elif slots == ramp_slots_left and slots < min(memory_slots, cpu_slots):
            limiter = "overcommit +2 per-tick node attach cap"
        record({"cpu_slots": int(cpu_slots), "slots": int(slots), "limiter": limiter if slots >= 0 else ""})
        return slots

    def fea_allocation_baseline_slots_remaining(self, allocation: dict, task: dict) -> int:
        """Workers still fitting inside this allocation's owned 1x CPU pool."""
        if allocation.get("state") == AllocationStatus.PENDING.value:
            owned = int(allocation.get("total_cpus") or 0)
            requested = 0
        else:
            alloc_id = int(allocation.get("id") or 0)
            # FEA intentionally ignores hard free_cpus bookkeeping; the 1x
            # baseline is the CPU pool owned by the parent allocation.
            owned = int(allocation.get("total_cpus") or 0)
            pressure = self.fea_allocation_pressures().get(alloc_id, {}) if alloc_id > 0 else {}
            requested = int(pressure.get("requested_cpus") or 0)
        budget = max(0, owned - requested)
        return budget // max(1, int(task.get("cpus") or 1))

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
        # The sweep scans every process owned by one Unix account on one node,
        # regardless of which Slurm allocation supplied the overlapping srun.
        # Running it once per allocation repeated the exact same scan whenever
        # an account had co-located allocations.  Group those allocations and
        # use the remaining jobs only as fallbacks if the preferred job has
        # already vanished.
        grouped: dict[tuple[str, str], list[dict]] = {}
        state_rank = {
            AllocationStatus.ACTIVE.value: 0,
            AllocationStatus.WARM.value: 1,
            AllocationStatus.DRAINING.value: 2,
        }
        for allocation in self.db.list_allocations_with_live(limit=500):
            if allocation["state"] not in live_states:
                continue
            node = str(allocation.get("node_name") or "").strip()
            job_id = str(allocation.get("slurm_job_id") or "").strip()
            account = self.account_by_name(str(allocation.get("account_name") or ""))
            if not node or not job_id or not account:
                continue
            grouped.setdefault((account.name, node), []).append(allocation)

        for allocations in grouped.values():
            allocations.sort(
                key=lambda item: (
                    state_rank.get(str(item.get("state") or ""), 99),
                    -int(item.get("id") or 0),
                )
            )

        futures = {
            key: self._ssh_executor.submit(
                self._sweep_orphan_process_group,
                self.account_by_name(key[0]),
                key[1],
                allocations,
                command,
            )
            for key, allocations in grouped.items()
        }
        for (account_name, node), future in futures.items():
            try:
                result = future.result()
            except Exception as exc:
                LOGGER.warning(
                    "orphan process sweep failed on %s/%s: %s",
                    account_name,
                    node,
                    exc,
                )
                continue
            last = (result.stdout or "").strip().splitlines()[-1:] or [""]
            if last[0].startswith("orphan_killed=") and last[0] != "orphan_killed=0":
                LOGGER.info(
                    "orphan process sweep on %s/%s: %s",
                    account_name,
                    node,
                    last[0],
                )

    @staticmethod
    def _sweep_orphan_process_group(
        account: AccountConfig | None,
        node: str,
        allocations: list[dict],
        command: str,
    ) -> Any:
        if not account:
            raise RuntimeError("orphan sweep account is not configured")
        errors: list[str] = []
        for allocation in allocations:
            job_id = str(allocation.get("slurm_job_id") or "").strip()
            if not job_id:
                continue
            srun = (
                f"srun --jobid={shlex.quote(job_id)} --overlap "
                f"bash -lc {shlex.quote(command)}"
            )
            try:
                with SSHSession(account, default_timeout=90) as ssh:
                    result = ssh.run(srun, timeout=80)
            except Exception as exc:
                errors.append(f"job {job_id}: {exc}")
                continue
            if result.exit_code == 0:
                return result
            detail = result.stderr.strip() or result.stdout.strip() or "remote exit"
            errors.append(f"job {job_id}: exit {result.exit_code}: {detail[:200]}")
        raise RuntimeError(
            "; ".join(errors)
            or f"no usable live allocation for {account.name}/{node}"
        )

    def enforce_fea_node_cpu_cap(self) -> None:
        """Retroactive side of the per-allocation FEA CPU cap: allocations that
        accumulated more FEA-requested CPUs than their reserved cores * factor
        are drained newest-worker-first, a few per tick, by requeueing the
        workers so the work reruns elsewhere."""
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
            for allocation in self.db.list_allocations_with_live(limit=500)
            if allocation["state"] in live_states
        }
        tasks_by_alloc: dict[int, list[dict]] = {}
        for task in self.db.list_tasks_by_statuses([TaskStatus.RUNNING.value], limit=5000):
            # RUNNING only: draining a task whose attach is still in flight
            # races the background attach thread.
            if task["status"] != TaskStatus.RUNNING.value:
                continue
            if not self.task_is_fea_bursty(task):
                continue
            if self.task_aedt_backend(task) == AedtBackend.POOLED.value:
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

    @staticmethod
    def _terminal_aedt_workspace_timestamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _terminal_aedt_workspace_cutoffs(self) -> tuple[str, str, str]:
        now = self._now()
        return (
            self._terminal_aedt_workspace_timestamp(now),
            self._terminal_aedt_workspace_timestamp(
                now
                - timedelta(seconds=TERMINAL_AEDT_WORKSPACE_RETRY_SECONDS)
            ),
            self._terminal_aedt_workspace_timestamp(
                now
                - timedelta(
                    seconds=TERMINAL_AEDT_WORKSPACE_CLAIM_STALE_SECONDS
                )
            ),
        )

    def _validated_terminal_aedt_workspace_path(
        self, task_id: int, workspace_path: str
    ) -> str:
        """Accept only the canonical AEDT scratch leaf owned by a task."""

        if int(task_id or 0) <= 0:
            raise ValueError("AEDT workspace cleanup task identity is unavailable")
        raw = str(workspace_path or "").strip()
        if not raw or "\x00" in raw or not raw.startswith("/"):
            raise ValueError("AEDT workspace cleanup path must be absolute")
        normalized = posixpath.normpath(raw)
        root = self.aedt_pool_terminal_workspace_root
        expected_leaf = f"aedt-{int(task_id)}"
        expected = posixpath.join(root, expected_leaf)
        if raw != normalized:
            raise ValueError(
                "AEDT workspace cleanup path is not canonical: "
                f"{workspace_path!r}"
            )
        if (
            normalized != expected
            or posixpath.dirname(normalized) != root
            or posixpath.basename(normalized) != expected_leaf
        ):
            raise ValueError(
                "AEDT workspace cleanup path is not the exact task leaf: "
                f"expected={expected!r}, actual={workspace_path!r}"
            )
        return normalized

    def _terminal_aedt_workspace_remove_command(
        self, task_id: int, workspace_path: str
    ) -> str:
        """Build a fail-closed, same-account removal of one exact GPFS leaf."""

        root = self.aedt_pool_terminal_workspace_root
        leaf = f"aedt-{int(task_id)}"
        script = "\n".join(
            [
                "set -u",
                "fail() { code=$1; shift; printf '%s\\n' \"$*\" >&2; exit \"$code\"; }",
                f"root={shlex.quote(root)}",
                f"target={shlex.quote(workspace_path)}",
                f"leaf={shlex.quote(leaf)}",
                "[ -d \"$root\" ] || fail 70 'AEDT workspace root is unavailable'",
                "[ ! -L \"$root\" ] || fail 71 'AEDT workspace root is a symlink'",
                "root_real=$(readlink -f -- \"$root\") || fail 72 'cannot canonicalize AEDT workspace root'",
                "[ \"$root_real\" = \"$root\" ] || fail 73 'AEDT workspace root is not canonical'",
                "if [ ! -e \"$target\" ] && [ ! -L \"$target\" ]; then",
                f"  printf '%s\\n' {shlex.quote(TERMINAL_AEDT_WORKSPACE_ABSENT_MARKER)}",
                "  exit 0",
                "fi",
                "[ -d \"$target\" ] || fail 74 'AEDT workspace target is not a directory'",
                "[ ! -L \"$target\" ] || fail 75 'AEDT workspace target is a symlink'",
                "target_real=$(readlink -f -- \"$target\") || fail 76 'cannot canonicalize AEDT workspace target'",
                "[ \"$target_real\" = \"$target\" ] || fail 77 'AEDT workspace target is not canonical'",
                "[ \"$(dirname -- \"$target_real\")\" = \"$root_real\" ] || fail 78 'AEDT workspace target escaped its root'",
                "[ \"$(basename -- \"$target_real\")\" = \"$leaf\" ] || fail 79 'AEDT workspace target leaf changed'",
                "uid=$(id -u) || fail 80 'cannot determine remote account uid'",
                "owner=$(stat -c %u -- \"$target\") || fail 81 'cannot stat AEDT workspace target'",
                "[ \"$owner\" = \"$uid\" ] || fail 82 'AEDT workspace target belongs to another account'",
                "bad_link=$(find -P \"$target\" -xdev -type l -print -quit 2>/dev/null) || fail 83 'cannot attest AEDT workspace symlinks'",
                "[ -z \"$bad_link\" ] || fail 84 'AEDT workspace contains a symlink'",
                "bad_owner=$(find -P \"$target\" -xdev ! -uid \"$uid\" -print -quit 2>/dev/null) || fail 85 'cannot attest AEDT workspace ownership'",
                "[ -z \"$bad_owner\" ] || fail 86 'AEDT workspace contains cross-account content'",
                "rm -rf --one-file-system -- \"$target\" || fail 87 'exact AEDT workspace removal failed'",
                "if [ -e \"$target\" ] || [ -L \"$target\" ]; then fail 88 'exact AEDT workspace still exists'; fi",
                f"printf '%s\\n' {shlex.quote(TERMINAL_AEDT_WORKSPACE_DELETED_MARKER)}",
            ]
        )
        return "sh -c " + shlex.quote(script)

    def _finish_terminal_aedt_workspace_failure(
        self,
        *,
        task_id: int,
        workspace_path: str,
        account_name: str,
        error: str,
    ) -> None:
        finished_at, _retry_before, _stale_before = (
            self._terminal_aedt_workspace_cutoffs()
        )
        self.db.finish_terminal_aedt_workspace_cleanup(
            task_id,
            workspace_path,
            state="failed",
            finished_at=finished_at,
            error=error,
        )
        self.record_event(
            "aedt_workspace_cleanup_failed",
            f"exact workspace cleanup failed for task {task_id}: {error[:500]}",
            entity_type="task",
            entity_id=task_id,
            account_name=account_name,
        )

    def _cleanup_terminal_aedt_workspace_task(
        self, task_id: int, trigger_state: str
    ) -> None:
        now, retry_before, stale_before = self._terminal_aedt_workspace_cutoffs()
        candidates = self.db.list_terminal_aedt_workspace_cleanup_candidates(
            task_id=int(task_id),
            retry_before=retry_before,
            stale_claim_before=stale_before,
            limit=100,
        )
        for candidate in candidates:
            workspace_path = str(candidate.get("workspace_path") or "")
            account_name = str(candidate.get("account_name") or "").strip()
            try:
                exact_path = self._validated_terminal_aedt_workspace_path(
                    int(task_id), workspace_path
                )
            except ValueError as exc:
                self.db.reject_terminal_aedt_workspace_cleanup(
                    int(task_id),
                    workspace_path,
                    rejected_at=now,
                    error=str(exc),
                )
                self.record_event(
                    "aedt_workspace_cleanup_rejected",
                    f"refused workspace cleanup for task {task_id}: {exc}",
                    entity_type="task",
                    entity_id=task_id,
                    account_name=account_name,
                )
                continue
            if not self.db.claim_terminal_aedt_workspace_cleanup(
                int(task_id),
                exact_path,
                retry_before=retry_before,
                stale_claim_before=stale_before,
                claimed_at=now,
            ):
                continue
            account = self.account_by_name(account_name)
            if not account:
                self._finish_terminal_aedt_workspace_failure(
                    task_id=int(task_id),
                    workspace_path=exact_path,
                    account_name=account_name,
                    error="task account is not configured",
                )
                continue
            command = self._terminal_aedt_workspace_remove_command(
                int(task_id), exact_path
            )
            try:
                with SSHSession(account, default_timeout=1800) as ssh:
                    result = ssh.run(command, timeout=1800)
            except Exception as exc:
                self._finish_terminal_aedt_workspace_failure(
                    task_id=int(task_id),
                    workspace_path=exact_path,
                    account_name=account.name,
                    error=f"remote cleanup error: {exc}",
                )
                continue
            stdout_lines = {
                line.strip() for line in result.stdout.splitlines() if line.strip()
            }
            if (
                result.exit_code == 0
                and TERMINAL_AEDT_WORKSPACE_DELETED_MARKER in stdout_lines
            ):
                cleanup_state = "deleted"
            elif (
                result.exit_code == 0
                and TERMINAL_AEDT_WORKSPACE_ABSENT_MARKER in stdout_lines
            ):
                cleanup_state = "absent"
            else:
                detail = (
                    result.stderr.strip()
                    or result.stdout.strip()
                    or "remote cleanup returned no attestation marker"
                )
                self._finish_terminal_aedt_workspace_failure(
                    task_id=int(task_id),
                    workspace_path=exact_path,
                    account_name=account.name,
                    error=f"remote exit {result.exit_code}: {detail[:1000]}",
                )
                continue
            finished_at, _retry_before, _stale_before = (
                self._terminal_aedt_workspace_cutoffs()
            )
            if not self.db.finish_terminal_aedt_workspace_cleanup(
                int(task_id),
                exact_path,
                state=cleanup_state,
                finished_at=finished_at,
            ):
                self.record_event(
                    "aedt_workspace_cleanup_failed",
                    f"workspace {cleanup_state} remotely but cleanup claim changed for task {task_id}",
                    entity_type="task",
                    entity_id=task_id,
                    account_name=account.name,
                )
                continue
            LOGGER.info(
                "terminal AEDT workspace %s for task %s on %s",
                cleanup_state,
                task_id,
                account.name,
            )
            self.record_event(
                "aedt_workspace_cleanup",
                f"exact scratch {exact_path} {cleanup_state} after task "
                f"{task_id} ended ({trigger_state})",
                entity_type="task",
                entity_id=task_id,
                account_name=account.name,
            )

    def _terminal_aedt_workspace_cleanup_done(
        self, task_id: int, future: Any
    ) -> None:
        with self._terminal_aedt_workspace_cleanup_lock:
            self._terminal_aedt_workspace_cleanup_inflight.discard(int(task_id))
        try:
            future.result()
        except Exception:
            LOGGER.exception(
                "terminal AEDT workspace cleanup worker crashed for task %s",
                task_id,
            )

    def _schedule_terminal_aedt_workspace_cleanup(
        self, task_id: int, trigger_state: str
    ) -> bool:
        normalized_task_id = int(task_id or 0)
        if not self.cleanup_enabled or normalized_task_id <= 0:
            return False
        with self._terminal_aedt_workspace_cleanup_lock:
            if normalized_task_id in self._terminal_aedt_workspace_cleanup_inflight:
                return False
            self._terminal_aedt_workspace_cleanup_inflight.add(normalized_task_id)
        try:
            future = self._terminal_aedt_workspace_cleanup_executor.submit(
                self._cleanup_terminal_aedt_workspace_task,
                normalized_task_id,
                str(trigger_state or "terminal"),
            )
        except RuntimeError:
            with self._terminal_aedt_workspace_cleanup_lock:
                self._terminal_aedt_workspace_cleanup_inflight.discard(
                    normalized_task_id
                )
            return False
        future.add_done_callback(
            lambda completed, exact_task_id=normalized_task_id: (
                self._terminal_aedt_workspace_cleanup_done(
                    exact_task_id, completed
                )
            )
        )
        return True

    def cleanup_terminal_aedt_workspaces_if_due(self) -> None:
        """Retry pending/failed exact cleanup without blocking a scheduler tick."""

        if not self.cleanup_enabled:
            return
        now_monotonic = time.monotonic()
        if (
            now_monotonic - self._last_terminal_aedt_workspace_sweep_at
            < TERMINAL_AEDT_WORKSPACE_SWEEP_INTERVAL_SECONDS
        ):
            return
        self._last_terminal_aedt_workspace_sweep_at = now_monotonic
        _now, retry_before, stale_before = self._terminal_aedt_workspace_cutoffs()
        candidates = self.db.list_terminal_aedt_workspace_cleanup_candidates(
            retry_before=retry_before,
            stale_claim_before=stale_before,
            limit=TERMINAL_AEDT_WORKSPACE_SCAN_LIMIT,
        )
        submitted = 0
        for candidate in candidates:
            if submitted >= TERMINAL_AEDT_WORKSPACE_SUBMIT_LIMIT:
                break
            if self._schedule_terminal_aedt_workspace_cleanup(
                int(candidate.get("task_id") or 0), "periodic retry"
            ):
                submitted += 1

    def on_task_terminal(self, task: dict, state: str = "terminal") -> None:
        """Run the task's declared cleanup on EVERY terminal path (completed,
        failed, cancelled, timed out, allocation lost) — shell-level cleanup
        inside the task command cannot cover cancel/kill because nothing after
        the killed process runs. Only the scheduler sees all exits."""
        self._license_mark_terminal(task)
        if str(task.get("aedt_backend") or "").strip().lower() == AedtBackend.POOLED.value:
            self._schedule_terminal_aedt_workspace_cleanup(
                int(task.get("id") or 0), state
            )
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
        self._fea_worker_counts_cache = None
        self._fea_pressures_cache = None
        self._fea_alloc_pressures_cache = None
        self._fea_node_resources_cache = None
        self._fea_footprint_cache = None
        self._active_profile_sets_cache = None
        self.record_event(
            "task_requeued",
            f"task {task.get('name') or task['id']} requeued: {reason}",
            entity_type="task",
            entity_id=task["id"],
            account_name=str(task.get("account_name") or ""),
        )
        self._license_mark_terminal(task)
        self.db.update_task(
            task["id"],
            status=TaskStatus.QUEUED.value,
            allocation_id=None,
            account_name=self.task_requested_account_name(task),
            remote_dir="",
            stdout_path="",
            stderr_path="",
            exit_code_path="",
            wrapper_pid="",
            attach_token="",
            launch_started_at=None,
            failure_message="",
            attached_at=None,
            started_at=None,
        )

    def fea_baseline_ratio(self, allocation: dict) -> float:
        """Solver-requested CPUs over owned CPUs for one allocation, with
        infrastructure tasks excluded. Below 1.0 the allocation has not yet
        received its 1x baseline of solvers (16 four-core solvers on a
        64-core reservation)."""
        pressure = self.fea_allocation_pressures().get(int(allocation.get("id") or 0), {})
        owned = int(pressure.get("owned_cpus") or allocation.get("total_cpus") or 0)
        if owned <= 0:
            return 1.0
        return float(int(pressure.get("requested_cpus") or 0)) / float(owned)

    def fea_license_admit_headroom(self) -> int | None:
        """electronics_desktop admit headroom from license admission, or None
        when admission is disabled or no usable snapshot exists."""
        if not self.license_admission_enabled:
            return None
        with self._task_assignment_lock:
            diagnostics = self._license_admission_diagnostics_locked()
        status = dict(diagnostics.get("features") or {}).get("electronics_desktop")
        if not isinstance(status, dict):
            return None
        try:
            return int(status.get("admit_headroom"))
        except (TypeError, ValueError):
            return None

    def fea_node_cpu_cap_remaining(self, allocation: dict, task: dict) -> int | None:
        """How many more workers of this task fit under the per-ALLOCATION cap:
        FEA-requested CPUs in this Slurm allocation <= its reserved cores
        (total_cpus) * fea_node_requested_cpu_factor. Explicit strict
        same-node requests are exact-allocation contracts, so they use the
        allocation's owned 1x CPUs instead of the general burst allowance.
        Per-allocation, not per-node, so several cpu2 allocations sharing a
        node each stay bounded to their own reservation. None = no cap
        applicable."""
        if self.task_is_fea_infra(task) or self.task_uses_reserved_aedt_pool_capacity(
            allocation, task
        ):
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
        capacity_factor = (
            1.0
            if self.task_has_strict_node_contract(task)
            and self.same_node_as_task_id(task)
            else self.fea_node_requested_cpu_factor
        )
        budget = owned * capacity_factor - requested
        return max(0, int(budget // max(1, int(task.get("cpus") or 1))))

    def fea_node_resource_pressures(self) -> dict[str, dict[str, int]]:
        """Full declared FEA resources grouped by physical hostname.

        Allocation-local ``free_cpus`` and ``free_memory_mb`` intentionally do
        not debit bursty FEA workers because measured load and memory govern
        their overcommit. When multiple account allocations land on one host,
        however, each allocation must not treat the same physical CPU/RAM as a
        private pool. This node-global shadow supplies that safety bound.
        """
        cached = self._fea_node_resources_cache
        if cached is not None and cached[0] == self._tick_seq:
            return cached[1]

        live_states = {
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
        }
        allocation_node_by_id: dict[int, str] = {}
        resources: dict[str, dict[str, int]] = {}
        for live_allocation in self.db.list_allocations_with_live(limit=500):
            if live_allocation["state"] not in live_states:
                continue
            node_name = str(live_allocation.get("node_name") or "").strip()
            if not node_name:
                continue
            allocation_node_by_id[int(live_allocation["id"])] = node_name
            entry = resources.setdefault(
                node_name,
                {
                    "allocation_count": 0,
                    "owned_cpus": 0,
                    "owned_memory_mb": 0,
                    "requested_cpus": 0,
                    "requested_memory_mb": 0,
                },
            )
            entry["allocation_count"] += 1
            entry["owned_cpus"] += max(
                0, int(live_allocation.get("total_cpus") or 0)
            )
            entry["owned_memory_mb"] += max(
                0, int(live_allocation.get("total_memory_mb") or 0)
            )

        project_cpus = self.db.aedt_pool_project_cpus()
        for active_task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
            if active_task["status"] not in {
                TaskStatus.ATTACHING.value,
                TaskStatus.RUNNING.value,
            }:
                continue
            if not self.task_is_fea_bursty(active_task):
                continue
            if self.task_is_fea_infra(active_task):
                continue
            # Thin pooled clients consume the capacity already reserved by
            # their exact AEDT session shape, so charging the client row again
            # would double count it. Ordinary standalone FEA is charged fully.
            if (
                self.task_aedt_backend(active_task) == AedtBackend.POOLED.value
                and int(active_task.get("cpus") or 0) < project_cpus
            ):
                continue
            node_name = allocation_node_by_id.get(
                int(active_task.get("allocation_id") or 0)
            )
            if not node_name:
                continue
            entry = resources[node_name]
            entry["requested_cpus"] += max(
                0, int(active_task.get("cpus") or 0)
            )
            entry["requested_memory_mb"] += max(
                0, int(active_task.get("memory_mb") or 0)
            )

        self._fea_node_resources_cache = (self._tick_seq, resources)
        return resources

    def _node_memory_total(self, node_name: str) -> int:
        if not node_name:
            return 0
        row = self.pestat_node_for_allocation(
            {"node_name": node_name}, max_age_seconds=float("inf")
        )
        if row and int(row.get("memory_mb") or 0) > 0:
            return int(row.get("memory_mb") or 0)
        for inventory_row in self.db.list_node_inventory():
            if str(inventory_row.get("node_name") or "") == node_name:
                return int(inventory_row.get("memory_mb") or 0)
        return 0

    def fea_node_shadow_cap_remaining(
        self,
        allocation: dict,
        task: dict,
        reservation_allocations: list[dict] | None = None,
    ) -> int | None:
        """Slots remaining under the physical-node declared CPU/RAM shadow.

        The additional cap applies only to co-located allocations. A single
        allocation retains the existing measured bursty policy, while two or
        more account allocations cannot each claim the same physical resource
        as if it were private.
        """
        if self.task_is_fea_infra(task) or self.task_uses_reserved_aedt_pool_capacity(
            allocation, task
        ):
            return None
        if allocation.get("state") == AllocationStatus.PENDING.value:
            return None
        node_name = str(allocation.get("node_name") or "").strip()
        if not node_name:
            return None
        pressure = self.fea_node_resource_pressures().get(node_name)
        if not pressure or int(pressure.get("allocation_count") or 0) < 2:
            return None

        reserved_slots = self.reserved_fea_slots_for_node(
            reservation_allocations, node_name
        )
        task_cpus = max(1, int(task.get("cpus") or 1))
        task_memory_mb = max(1, int(task.get("memory_mb") or 1))
        requested_cpus = int(pressure.get("requested_cpus") or 0) + (
            reserved_slots * task_cpus
        )
        requested_memory_mb = int(
            pressure.get("requested_memory_mb") or 0
        ) + (reserved_slots * task_memory_mb)

        owned_cpus = max(0, int(pressure.get("owned_cpus") or 0))
        physical_cpus = self._node_cpu_total(node_name)
        if physical_cpus > 0:
            owned_cpus = min(owned_cpus, physical_cpus)
        cpu_capacity = int(owned_cpus * self.fea_node_requested_cpu_factor)

        owned_memory_mb = max(
            0, int(pressure.get("owned_memory_mb") or 0)
        )
        physical_memory_mb = self._node_memory_total(node_name)
        if physical_memory_mb > 0:
            owned_memory_mb = min(owned_memory_mb, physical_memory_mb)

        if cpu_capacity <= 0 or owned_memory_mb <= 0:
            return 0
        cpu_slots = max(0, (cpu_capacity - requested_cpus) // task_cpus)
        memory_slots = max(
            0, (owned_memory_mb - requested_memory_mb) // task_memory_mb
        )
        return min(cpu_slots, memory_slots)

    def handle_fea_memory_pressure(self) -> None:
        reclaimed = False
        pressured_allocations_by_node: dict[str, dict[str, Any]] = {}
        for allocation in self.db.list_allocations_with_live(limit=500):
            if allocation["state"] not in {
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
            }:
                continue
            observation = self.fea_memory_pressure_observation(allocation)
            if not observation:
                continue
            node_name = str(allocation.get("node_name") or "")
            if not node_name:
                continue
            pressure = pressured_allocations_by_node.setdefault(
                node_name,
                {**observation, "allocations": []},
            )
            if str(observation["observed_at"]) > str(
                pressure.get("observed_at") or ""
            ):
                pressure.update(observation)
            pressure["allocations"].append(allocation)

        # Memory pressure is measured by pestat for the whole node.  Several
        # scheduler allocations can share that node, so reclaiming once per
        # allocation or once per tick would kill a burst of otherwise
        # recoverable simulations from one stale pressure episode. Reclaim
        # only the newest standalone FEA worker once per durable node episode.
        for node_name, pressure in pressured_allocations_by_node.items():
            setting_key = (
                f"{FEA_HARD_PRESSURE_RECLAIM_SAMPLE_SETTING_PREFIX}{node_name}"
            )
            if not bool(pressure.get("hard_pressure")):
                self.db.claim_fea_pressure_episode(
                    setting_key,
                    observed_at=str(pressure.get("observed_at") or ""),
                    total_memory_mb=int(pressure.get("total_memory_mb") or 0),
                    free_memory_mb=int(pressure.get("free_memory_mb") or 0),
                    hard_pressure=False,
                    cooldown_seconds=FEA_HARD_PRESSURE_EPISODE_COOLDOWN_SECONDS,
                )
                continue
            allocations = list(pressure["allocations"])
            allocation_by_id = {
                int(allocation["id"]): allocation for allocation in allocations
            }
            candidates = [
                task
                for allocation_id in allocation_by_id
                if (task := self.newest_running_fea_task(allocation_id)) is not None
            ]
            if not candidates:
                continue
            task = max(
                candidates,
                key=lambda candidate: (
                    candidate.get("attached_at")
                    or candidate.get("started_at")
                    or candidate.get("created_at")
                    or "",
                    int(candidate.get("id") or 0),
                ),
            )
            allocation = allocation_by_id[int(task.get("allocation_id") or 0)]
            observed_at = str(pressure.get("observed_at") or "")
            if not self.db.claim_fea_pressure_episode(
                setting_key,
                observed_at=observed_at,
                total_memory_mb=int(pressure.get("total_memory_mb") or 0),
                free_memory_mb=int(pressure.get("free_memory_mb") or 0),
                hard_pressure=True,
                cooldown_seconds=FEA_HARD_PRESSURE_EPISODE_COOLDOWN_SECONDS,
            ):
                LOGGER.info(
                    "skipping repeated FEA memory-pressure reclaim on %s "
                    "for pressure episode sample %s (%s/%s MB free)",
                    node_name,
                    observed_at,
                    int(pressure.get("free_memory_mb") or 0),
                    int(pressure.get("total_memory_mb") or 0),
                )
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
        self._fea_worker_counts_cache = None
        self._fea_pressures_cache = None
        self._fea_alloc_pressures_cache = None
        self._fea_node_resources_cache = None
        self.record_event(
            "task_requeued",
            f"task {task.get('name') or task['id']} requeued after memory-pressure kill (attempt {attempts}/{self.fea_pressure_max_attempts})",
            entity_type="task",
            entity_id=task["id"],
            account_name=str(task.get("account_name") or ""),
        )
        self._license_mark_terminal(task)
        self.db.update_task(
            task["id"],
            status=TaskStatus.QUEUED.value,
            attempt_count=attempts,
            allocation_id=None,
            account_name=self.task_requested_account_name(task),
            remote_dir="",
            stdout_path="",
            stderr_path="",
            exit_code_path="",
            wrapper_pid="",
            attach_token="",
            launch_started_at=None,
            failure_message="",
            attached_at=None,
            started_at=None,
        )

    def newest_running_fea_task(self, allocation_id: int) -> dict | None:
        candidates = [
            task
            for task in self.db.list_tasks_by_statuses([TaskStatus.RUNNING.value], limit=5000)
            if int(task.get("allocation_id") or 0) == allocation_id
            # RUNNING only: killing a task whose attach is still in flight
            # races the background attach thread.
            and task["status"] == TaskStatus.RUNNING.value
            and self.task_is_fea_bursty(task)
            # A pooled session host owns one shared Desktop and every solver
            # process for its attached projects.  Treating it as an ordinary
            # disposable FEA worker turns one node-level pressure sample into a
            # three-project cascade, and the per-allocation loop can kill every
            # Desktop on the same physical node in one tick.  Slurm already
            # accounts the host's reserved CPU/RAM; pool draining/recycling is
            # the only safe way to retire it.
            and not self.task_is_fea_infra(task)
            and self.task_aedt_backend(task) != AedtBackend.POOLED.value
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

    @staticmethod
    def task_requested_account_name(task: dict) -> str:
        """Return the requested placement constraint, not the last assignment.

        NULL means a legacy row whose original intent is unknown, so preserve
        its existing account_name. New rows store an explicit empty string for
        unpinned work and a non-empty value for deliberate account pins.
        """
        if "requested_account_name" in task and task.get("requested_account_name") is not None:
            return str(task.get("requested_account_name") or "")
        return str(task.get("account_name") or "")

    @staticmethod
    def task_requested_allocation_id(task: dict) -> int:
        """Return an exact allocation placement request, distinct from assignment."""
        return max(0, int(task.get("requested_allocation_id") or 0))

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
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
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
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
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

    def active_profile_allocation_sets(self, *, refresh: bool = False) -> tuple[set[int], set[int]]:
        """Allocations claimed by ordinary FEA or standard tasks.

        ATTACHING rows are reservations and count immediately. Same-node
        placement remains available only within the same scheduling profile;
        every task therefore contributes its actual FEA/standard profile.
        """
        if not refresh and self._active_profile_sets_cache is not None:
            return self._active_profile_sets_cache
        fea_allocation_ids: set[int] = set()
        standard_allocation_ids: set[int] = set()
        for active_task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
            limit=5000,
        ):
            allocation_id = int(active_task.get("allocation_id") or 0)
            profile = self.task_allocation_profile(active_task)
            if allocation_id <= 0 or not profile:
                continue
            if profile == "fea":
                fea_allocation_ids.add(allocation_id)
            else:
                standard_allocation_ids.add(allocation_id)
        result = (fea_allocation_ids, standard_allocation_ids)
        self._active_profile_sets_cache = result
        return result

    def task_allocation_profile(self, task: dict) -> str:
        return "fea" if self.task_is_fea_bursty(task) else "standard"

    @staticmethod
    def allocation_demand_profile(allocation: dict) -> str:
        """Return the durable profile reserved by an unclaimed demand pool.

        ``drain_reason`` already persists why a demand allocation was opened,
        survives PENDING -> WARM/ACTIVE, and is included in reservation-plan
        signatures. Encoding only the exceptional FEA lane keeps historical
        and generic standard rows backward-compatible.
        """
        reason = str(allocation.get("drain_reason") or "")
        return "fea" if reason.startswith("queued FEA ") else ""

    def retire_legacy_unprofiled_pending_demand_allocations(self) -> int:
        """One-time restart migration for pre-profile demand requests.

        Historical demand rows do not say whether FEA or standard work opened
        them.  A pending allocation cannot host a live step yet, so retiring
        only claimless legacy rows is lossless: queued tasks remain queued and
        the fixed planner recreates exact, profile-tagged FEA capacity.  Both
        direct and requested allocation claims are protected by an unbounded
        database query.  The durable setting prevents later restarts from
        resetting the queue age of newly created standard demand pools.
        """
        setting = "legacy_demand_profile_retired_v1"
        if self.db.get_setting(setting) == "1":
            return 0
        retired = 0
        all_processed = True
        for allocation in self.db.list_legacy_unprofiled_pending_demand_allocations():
            if self.db.list_nonterminal_task_claims_for_allocation(
                int(allocation["id"])
            ):
                # Retry on a later restart after the protected claim becomes
                # terminal; do not permanently bless an unprofiled survivor.
                all_processed = False
                continue
            if self.close_allocation(
                allocation,
                "legacy unprofiled pending demand retired",
            ):
                retired += 1
            else:
                all_processed = False
        if all_processed:
            self.db.set_setting(setting, "1")
        return retired

    @staticmethod
    def allocation_has_gpu_pool(allocation: dict) -> bool:
        return (
            int(allocation.get("total_gpus") or 0) > 0
            or str(allocation.get("resource_pool") or "").startswith("gpu:")
        )

    def allocation_profile_conflicts(
        self,
        allocation: dict,
        task: dict,
        *,
        refresh: bool = False,
    ) -> bool:
        """Prevent shared-memory FEA and per-step-memory standard mixing.

        Isolation applies to CPU and GPU allocations because FEA omits
        per-step --mem in both. Same-node placement can overlap only tasks
        belonging to the allocation's existing profile.
        """
        profile = self.task_allocation_profile(task)
        if not profile:
            return False
        reserved_profile = str(allocation.get("_reserved_scheduling_profile") or "")
        if reserved_profile and reserved_profile != profile:
            return True
        demand_profile = self.allocation_demand_profile(allocation)
        if demand_profile and demand_profile != profile:
            return True
        fea_allocation_ids, standard_allocation_ids = self.active_profile_allocation_sets(refresh=refresh)
        allocation_id = int(allocation.get("id") or 0)
        if profile == "fea":
            return allocation_id in standard_allocation_ids
        return allocation_id in fea_allocation_ids

    def allocation_has_active_task(self, allocation_id: int) -> bool:
        for task in self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value], limit=5000
        ):
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
        if self.allocation_is_dedicated_aedt_pool(allocation):
            if not self.task_can_share_dedicated_aedt_pool(allocation, task):
                return False
        elif bool(int(allocation.get("exclusive_node") or 0)) != bool(
            int(task.get("exclusive_node") or 0)
        ):
            return False
        requested_allocation_id = self.task_requested_allocation_id(task)
        if requested_allocation_id and int(allocation.get("id") or 0) != requested_allocation_id:
            return False
        requested_accounts = self.requested_accounts(self.task_requested_account_name(task))
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
        if (
            self.task_is_fea_bursty(task)
            and not self.task_requires_gpu(task)
            and self.allocation_has_gpu_pool(allocation)
        ):
            return False
        if self.allocation_profile_conflicts(allocation, task):
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
        task_partition = str(task.get("partition") or "auto")
        if (
            task_partition not in {"", "auto"}
            and not self.partition_spec_allows(
                task_partition,
                str(allocation.get("partition") or ""),
            )
        ):
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
            if self.task_is_fea_infra(task):
                # Exact AEDT host placement is capacity-controlled by the
                # pool.  Do not let transient solver load, stale pestat, or a
                # full project CPU baseline reject its infrastructure launch.
                return True
            if self.task_uses_reserved_aedt_pool_capacity(allocation, task):
                # The exact Desktop/session reservation and complete Slurm
                # session footprint are the capacity authority for this
                # client. Generic FEA node pressure is already represented by
                # the allocation reservation and must not be counted twice.
                return True
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
        # Startup reconciliation deliberately holds ambiguous remote submits.
        # Revisit those claims every tick so query failures and the zero-match
        # grace are bounded without risking a duplicate allocation job.
        self.recover_allocation_submission_claims()
        self.prewarm_gpu_for_minimum()
        self.prewarm_cpu_for_minimum()
        # Retire stale demand pools before opening replacements. Opening first
        # let a new reservation change the current-fit shape, so scale-in
        # could cancel the pool created a few lines earlier in the same tick.
        queued_tasks = self.queued_tasks_for_allocation_reservations()
        reservation_plan = self.queued_task_allocation_reservation_plan(queued_tasks)
        self.scale_in_idle_allocations(
            reservation_plan=reservation_plan,
            queued_tasks=queued_tasks,
        )
        self.prewarm_for_demand(reservation_plan=reservation_plan)

    def prewarm_cpu_for_minimum(self) -> None:
        live_count = sum(
            1
            for allocation in self.db.list_allocations_with_live(limit=500)
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

    def prewarm_for_demand(
        self,
        reservation_plan: _QueuedTaskAllocationReservationPlan | None = None,
    ) -> None:
        if self.prewarm_exclusive_demand():
            return
        if self.scale_out_for_fea_overload():
            return
        queued_tasks = self.queued_demand_tasks()
        opened, blocked = self.open_fit_aware_demand_allocations(
            queued_tasks,
            reservation_plan=reservation_plan,
        )
        if opened or blocked:
            return
        self.prewarm_for_high_utilization()

    def queued_demand_tasks(self) -> list[dict]:
        return sorted(
            [
                task
                for task in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if self.task_is_queued_demand(task)
            ],
            key=self.queued_task_order_key,
        )

    def task_is_queued_demand(self, task: dict) -> bool:
        return bool(
            task["status"] == TaskStatus.QUEUED.value
            and not int(task.get("exclusive_node") or 0)
            and self.task_aedt_backend_admitted(task)
            and self.task_aedt_backend(task) == AedtBackend.STANDALONE.value
        )

    @staticmethod
    def queued_task_order_key(task: dict) -> tuple[int, int]:
        return (-int(task.get("priority") or 0), int(task["id"]))

    def open_fit_aware_demand_allocations(
        self,
        queued_tasks: list[dict],
        reservation_plan: _QueuedTaskAllocationReservationPlan | None = None,
    ) -> tuple[int, bool]:
        if not queued_tasks:
            self._last_demand_reservation_plan = {
                "tick": self._tick_seq,
                "mode": "empty",
                "queued_tasks": 0,
                "replayed_tasks": 0,
                "scanned_tasks": 0,
                "reason": "",
            }
            return 0, False
        remaining_allocations: list[dict] | None = None
        start_index = 0
        replay_reason = "no_plan"
        if reservation_plan is not None:
            remaining_allocations, start_index, replay_reason = self.replay_reservation_plan_prefix(
                reservation_plan,
                queued_tasks,
            )
        if remaining_allocations is None:
            remaining_allocations = self.current_reservation_allocations()
            self.annotate_fea_node_worker_counts(remaining_allocations)
            start_index = 0
        reserved_submissions: list[dict] = []
        blocked = False
        self.prefetch_fea_storage_quotas(
            self.fea_storage_accounts_for_tasks(queued_tasks[start_index:])
        )
        # Fix account limits, allocation shapes, and queue capacity in durable
        # PENDING rows before any worker may enter remote sbatch. Sequential
        # planning means each later choice observes every earlier reservation.
        with self._task_assignment_lock:
            for task in queued_tasks[start_index:]:
                if self.reserve_inflight_capacity_for_task(
                    remaining_allocations, task
                ):
                    continue
                if (
                    len(reserved_submissions)
                    >= self.allocation_max_new_per_loop
                ):
                    blocked = True
                    continue
                allocation = self.open_allocation_for_task_record(
                    task, submit=False
                )
                if not allocation:
                    blocked = True
                    continue
                reserved_submissions.append(dict(allocation))
                remaining_allocations.append(dict(allocation))
                self.reserve_inflight_capacity_for_task(
                    remaining_allocations, task
                )
        successful_ids = self.submit_reserved_allocation_records(
            reserved_submissions
        )
        opened = len(successful_ids)
        if opened != len(reserved_submissions):
            blocked = True
        self._last_demand_reservation_plan = {
            "tick": self._tick_seq,
            "mode": (
                "complete"
                if start_index == len(queued_tasks)
                else "prefix"
                if start_index > 0
                else "fallback"
            ),
            "queued_tasks": len(queued_tasks),
            "plan_reserved_tasks": len(reservation_plan.reserved_task_ids)
            if reservation_plan is not None
            else 0,
            "replayed_tasks": start_index,
            "scanned_tasks": len(queued_tasks) - start_index,
            "reason": replay_reason,
            "opened_allocations": opened,
            "reserved_allocations": len(reserved_submissions),
            "blocked": blocked,
        }
        return opened, blocked

    @staticmethod
    def _reservation_record_signature(record: dict) -> str:
        return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _reservation_records_signature(
        records: list[dict],
        fields: tuple[str, ...],
    ) -> tuple[tuple[Any, ...], ...]:
        return tuple(
            (
                int(record.get("id") or 0),
                *(record.get(field) for field in fields),
            )
            for record in sorted(records, key=lambda item: int(item.get("id") or 0))
        )

    def _reservation_allocation_signature(
        self, allocations: list[dict]
    ) -> tuple[tuple[Any, ...], ...]:
        return self._reservation_records_signature(
            allocations,
            _RESERVATION_ALLOCATION_FIT_FIELDS,
        )

    @staticmethod
    def _reservation_record_signature_changes(
        before: tuple[tuple[Any, ...], ...],
        after: tuple[tuple[Any, ...], ...],
        fields: tuple[str, ...],
        prefix: str,
    ) -> list[str]:
        before_by_id = {int(item[0]): item[1:] for item in before}
        after_by_id = {int(item[0]): item[1:] for item in after}
        changes: list[str] = []
        if before_by_id.keys() != after_by_id.keys():
            changes.append(f"{prefix}.membership")
        common_ids = before_by_id.keys() & after_by_id.keys()
        for index, field in enumerate(fields):
            if any(
                before_by_id[record_id][index] != after_by_id[record_id][index]
                for record_id in common_ids
            ):
                changes.append(f"{prefix}.{field}")
        return changes

    def current_reservation_allocations(self) -> list[dict]:
        return [
            dict(allocation)
            for allocation in self.db.list_allocations_with_live(limit=500)
            if allocation["state"]
            in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
            }
        ]

    def _reservation_fit_state_signature(self) -> tuple[Any, ...]:
        active_tasks = self.db.list_tasks_by_statuses(
            [TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value],
            limit=5000,
        )
        active_signature = self._reservation_records_signature(
            active_tasks,
            _RESERVATION_ACTIVE_TASK_FIT_FIELDS,
        )
        aedt_pressure_signature = tuple(
            (
                int(allocation_id),
                int(pressure.get("workers") or 0),
                int(pressure.get("shadow_cpus") or 0),
                int(pressure.get("live_projects") or 0),
            )
            for allocation_id, pressure in sorted(
                self.db.aedt_project_pressure_by_allocation().items()
            )
        )
        return (
            active_signature,
            aedt_pressure_signature,
            tuple(sorted(self._tick_attach_workers_by_node.items())),
            self.db.aedt_pool_project_cpus(),
        )

    def _reservation_fit_state_changes(
        self,
        before: tuple[Any, ...],
        after: tuple[Any, ...],
    ) -> list[str]:
        changes = self._reservation_record_signature_changes(
            before[0],
            after[0],
            _RESERVATION_ACTIVE_TASK_FIT_FIELDS,
            "active_task",
        )
        if before[1] != after[1]:
            changes.append("aedt_pressure")
        if before[2] != after[2]:
            changes.append("tick_attach_workers")
        if before[3] != after[3]:
            changes.append("aedt_project_cpus")
        return changes

    def queued_tasks_for_allocation_reservations(self) -> list[dict]:
        return sorted(
            [
                task
                for task in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if task["status"] == TaskStatus.QUEUED.value
            ],
            key=self.queued_task_order_key,
        )

    def queued_task_allocation_reservation_plan(
        self,
        queued_tasks: list[dict] | None = None,
    ) -> _QueuedTaskAllocationReservationPlan:
        tasks = queued_tasks if queued_tasks is not None else self.queued_demand_tasks()
        fit_state_before = self._reservation_fit_state_signature()
        remaining_allocations = self.current_reservation_allocations()
        allocation_signature = self._reservation_allocation_signature(remaining_allocations)
        self.annotate_fea_node_worker_counts(remaining_allocations)
        reservations: dict[int, list[int]] = {}
        steps: list[_QueuedTaskAllocationReservationStep] = []
        for task in tasks:
            allocation = self.reserve_inflight_capacity_for_task(
                remaining_allocations,
                task,
                reservation_steps=steps,
            )
            if not allocation:
                continue
            reservations.setdefault(int(allocation["id"]), []).append(int(task["id"]))
        current_allocations = self.current_reservation_allocations()
        current_allocation_signature = self._reservation_allocation_signature(
            current_allocations
        )
        fit_state_after = self._reservation_fit_state_signature()
        changed_while_built = tuple(
            self._reservation_record_signature_changes(
                allocation_signature,
                current_allocation_signature,
                _RESERVATION_ALLOCATION_FIT_FIELDS,
                "allocation",
            )
            + self._reservation_fit_state_changes(fit_state_before, fit_state_after)
        )
        task_signatures_by_id = {
            int(task["id"]): self._reservation_record_signature(task)
            for task in tasks
        }
        planned_demand_task_ids = tuple(
            int(task["id"])
            for task in tasks
            if self.task_is_queued_demand(task)
        )
        return _QueuedTaskAllocationReservationPlan(
            reservations=reservations,
            reserved_task_ids=frozenset(
                task_id
                for task_ids in reservations.values()
                for task_id in task_ids
            ),
            task_signatures_by_id=task_signatures_by_id,
            planned_demand_task_ids=planned_demand_task_ids,
            allocation_signature=allocation_signature,
            fit_state_signature=fit_state_before,
            steps=tuple(steps),
            changed_while_built=changed_while_built,
            reusable=not changed_while_built,
        )

    def reservation_plan_replay_state(
        self,
        plan: _QueuedTaskAllocationReservationPlan,
        queued_tasks: list[dict],
    ) -> tuple[int | None, str]:
        if not plan.reusable:
            details = ",".join(plan.changed_while_built)
            return None, f"plan_changed_while_built:{details}"
        order_keys = [self.queued_task_order_key(task) for task in queued_tasks]
        if any(left >= right for left, right in zip(order_keys, order_keys[1:])):
            return None, "queued_task_order_changed"
        planned_ids = plan.planned_demand_task_ids
        if len(queued_tasks) < len(planned_ids):
            return None, "planned_queued_task_removed"
        for index, task_id in enumerate(planned_ids):
            task = queued_tasks[index]
            if int(task["id"]) != task_id:
                if int(task["id"]) in plan.task_signatures_by_id:
                    return None, "queued_task_order_changed"
                return None, "queued_task_inserted_before_plan_prefix"
            signature = plan.task_signatures_by_id[task_id]
            if signature != self._reservation_record_signature(task):
                return None, "queued_task_changed"
        appended_tasks = queued_tasks[len(planned_ids) :]
        if any(
            int(task["id"]) in plan.task_signatures_by_id
            for task in appended_tasks
        ):
            return None, "queued_task_order_changed"
        if planned_ids and appended_tasks:
            planned_last_key = self.queued_task_order_key(
                queued_tasks[len(planned_ids) - 1]
            )
            if any(
                self.queued_task_order_key(task) <= planned_last_key
                for task in appended_tasks
            ):
                return None, "queued_task_inserted_before_plan_prefix"
        current_allocations = self.current_reservation_allocations()
        if plan.allocation_signature != self._reservation_allocation_signature(current_allocations):
            return None, "allocation_state_changed"
        if plan.fit_state_signature != self._reservation_fit_state_signature():
            return None, "fit_state_changed"
        return (
            len(planned_ids),
            "queued_tail_appended" if appended_tasks else "",
        )

    def reservation_plan_state_matches(
        self,
        plan: _QueuedTaskAllocationReservationPlan,
        queued_tasks: list[dict],
    ) -> tuple[bool, str]:
        replayed_tasks, reason = self.reservation_plan_replay_state(
            plan,
            queued_tasks,
        )
        return replayed_tasks is not None, "" if replayed_tasks is not None else reason

    def replay_reservation_plan_prefix(
        self,
        plan: _QueuedTaskAllocationReservationPlan,
        queued_tasks: list[dict],
    ) -> tuple[list[dict] | None, int, str]:
        # Task assignment and allocation close both use this lock.  Hold it
        # through validation and the cheap direct replay so the certificate
        # cannot be invalidated between its fingerprint check and application.
        with self._task_assignment_lock:
            certified_prefix_length, reason = self.reservation_plan_replay_state(
                plan,
                queued_tasks,
            )
            if certified_prefix_length is None:
                return None, 0, reason
            remaining_allocations = self.current_reservation_allocations()
            self.annotate_fea_node_worker_counts(remaining_allocations)
            allocations_by_id = {
                int(allocation["id"]): allocation
                for allocation in remaining_allocations
            }
            queued_ids = {int(task["id"]) for task in queued_tasks}
            queued_index = 0
            stop_reason = ""
            for step in plan.steps:
                if queued_index >= certified_prefix_length:
                    break
                expected_task_id = int(queued_tasks[queued_index]["id"])
                if step.task_id != expected_task_id:
                    if step.task_id in queued_ids:
                        return None, 0, "queued_task_order_changed"
                    if step.allocation_id is not None:
                        stop_reason = "non_demand_reservation"
                        break
                    continue
                if step.allocation_id is None or step.effective_task is None:
                    stop_reason = "unreserved_task"
                    break
                allocation = allocations_by_id.get(step.allocation_id)
                if allocation is None:
                    return None, 0, "reserved_allocation_missing"
                self.apply_inflight_capacity_reservation(allocation, step.effective_task)
                queued_index += 1
            if not stop_reason and queued_index != certified_prefix_length:
                return None, 0, "reservation_trace_incomplete"
            # Pool leases can change through the control-plane web thread,
            # outside the scheduler assignment lock. Recheck the certificate
            # after replay so that such a concurrent change falls back rather
            # than using a stale prefix.
            if plan.allocation_signature != self._reservation_allocation_signature(
                self.current_reservation_allocations()
            ):
                return None, 0, "allocation_state_changed_during_replay"
            if plan.fit_state_signature != self._reservation_fit_state_signature():
                return None, 0, "fit_state_changed_during_replay"
            return (
                remaining_allocations,
                queued_index,
                stop_reason or reason or "all_tasks_reserved",
            )

    def reservation_plan_covers_tasks(
        self,
        plan: _QueuedTaskAllocationReservationPlan,
        queued_tasks: list[dict],
    ) -> bool:
        allocations, replayed_tasks, _reason = self.replay_reservation_plan_prefix(
            plan,
            queued_tasks,
        )
        return allocations is not None and replayed_tasks == len(queued_tasks)

    def queued_task_allocation_reservations(
        self,
        queued_tasks: list[dict] | None = None,
    ) -> dict[int, list[int]]:
        return self.queued_task_allocation_reservation_plan(queued_tasks).reservations

    def prewarm_for_high_utilization(self) -> None:
        allocations = [
            item
            for item in self.db.list_allocations_with_live(limit=500)
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
            [
                task
                for task in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if task["status"] == TaskStatus.QUEUED.value
                and self.task_aedt_backend_admitted(task)
                and self.task_aedt_backend(task) == AedtBackend.STANDALONE.value
            ],
            key=lambda item: int(item["id"]),
        )
        remaining_allocations = [
            dict(allocation)
            for allocation in self.db.list_allocations_with_live(limit=500)
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

    def reserve_inflight_capacity_for_task(
        self,
        allocations: list[dict],
        task: dict,
        *,
        reservation_steps: list[_QueuedTaskAllocationReservationStep] | None = None,
        include_pending: bool = True,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
    ) -> dict | None:
        candidates = []
        effective_task = task
        for candidate_task, _relaxed in self.effective_task_variants(task):
            candidates = []
            for allocation in allocations:
                if not self.allocation_can_run_task(
                    allocation,
                    candidate_task,
                    include_pending=include_pending,
                    active_task_allocation_ids=active_task_allocation_ids,
                    active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                ):
                    continue
                if self.fit_slots_for_allocation(allocation, candidate_task, allocations) <= 0:
                    continue
                candidates.append(allocation)
            if candidates:
                effective_task = candidate_task
                break
        if not candidates and include_pending:
            # A strict-node FEA demand pool is opened on the only node the
            # caller permits.  Once Slurm starts it, transient node pressure
            # can make the ordinary ready-fit calculation return zero.  Do
            # not then forget the reservation and close/reopen the same pool
            # every two ticks: retain one structurally fitting, storage-safe
            # pool while the exact queued task waits for the unchanged final
            # memory/load/storage attach gates.
            for candidate_task, _relaxed in self.effective_task_variants(task):
                candidates = [
                    allocation
                    for allocation in allocations
                    if self.warm_strict_fea_demand_pool_can_wait_for_task(
                        allocation,
                        candidate_task,
                        allocations,
                    )
                ]
                if candidates:
                    effective_task = candidate_task
                    break
        if not candidates:
            if reservation_steps is not None:
                reservation_steps.append(
                    _QueuedTaskAllocationReservationStep(
                        task_id=int(task["id"]),
                        allocation_id=None,
                        effective_task=None,
                    )
                )
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
        if reservation_steps is not None:
            reservation_steps.append(
                _QueuedTaskAllocationReservationStep(
                    task_id=int(task["id"]),
                    allocation_id=int(allocation["id"]),
                    effective_task=dict(effective_task),
                )
            )
        self.apply_inflight_capacity_reservation(allocation, effective_task)
        if not include_pending and active_task_allocation_ids is not None:
            allocation_id = int(allocation["id"])
            active_task_allocation_ids.add(allocation_id)
            if (
                active_exclusive_allocation_ids is not None
                and int(effective_task.get("exclusive_node") or 0)
            ):
                active_exclusive_allocation_ids.add(allocation_id)
        return allocation

    def apply_inflight_capacity_reservation(self, allocation: dict, effective_task: dict) -> None:
        allocation_profile = self.task_allocation_profile(effective_task)
        if allocation_profile:
            allocation["_reserved_scheduling_profile"] = allocation_profile
        if self.task_is_fea_bursty(effective_task):
            allocation["_reserved_fea_slots"] = int(allocation.get("_reserved_fea_slots") or 0) + 1
            if self.task_aedt_backend(effective_task) == AedtBackend.POOLED.value:
                allocation["_reserved_pooled_aedt_client_slots"] = int(
                    allocation.get("_reserved_pooled_aedt_client_slots") or 0
                ) + 1
            if self.task_requires_gpu(effective_task):
                allocation["free_gpus"] = max(0, int(allocation.get("free_gpus") or 0) - int(effective_task.get("gpus") or 0))
            return
        allocation["free_memory_mb"] = max(0, int(allocation.get("free_memory_mb") or 0) - int(effective_task.get("memory_mb") or 0))
        allocation["free_cpus"] = max(0, int(allocation.get("free_cpus") or 0) - int(effective_task.get("cpus") or 0))
        if "_allocation_worker_count" in allocation:
            allocation["_allocation_worker_count"] = int(
                allocation.get("_allocation_worker_count") or 0
            ) + 1
        if self.task_requires_gpu(effective_task):
            allocation["free_gpus"] = max(0, int(allocation.get("free_gpus") or 0) - int(effective_task.get("gpus") or 0))

    def warm_strict_fea_demand_pool_can_wait_for_task(
        self,
        allocation: dict,
        task: dict,
        reservation_allocations: list[dict] | None = None,
    ) -> bool:
        """Whether a warm strict FEA demand pool remains a durable reservation.

        This is intentionally a planning-only structural check.  It must not
        make a pressure-blocked pool attachable; ``assign_queued_task`` still
        evaluates the real warm row and performs the serialized final storage
        admission before changing the task claim.
        """

        if allocation.get("state") != AllocationStatus.WARM.value:
            return False
        if self.allocation_demand_profile(allocation) != "fea":
            return False
        if not self.task_is_fea_bursty(task):
            return False
        if not self.task_has_strict_node_contract(task):
            return False
        requested_node = self.strict_task_node_name(task)
        if not requested_node or str(allocation.get("node_name") or "") != requested_node:
            return False
        if not str(allocation.get("slurm_job_id") or ""):
            return False
        account = self.account_by_name(str(allocation.get("account_name") or ""))
        if account is None or self.account_storage_blocked(
            account,
            for_fea=True,
            additional_future_projects=1,
        ):
            return False
        pending_view = dict(allocation)
        pending_view["state"] = AllocationStatus.PENDING.value
        if not self.allocation_can_run_task(
            pending_view,
            task,
            include_pending=True,
        ):
            return False
        return (
            self.fit_slots_for_allocation(
                pending_view,
                task,
                reservation_allocations,
            )
            > 0
        )

    def prewarm_exclusive_demand(self) -> bool:
        queued_tasks = sorted(
            [
                task
                for task in self.db.list_tasks(
                    limit=5000, statuses=[TaskStatus.QUEUED.value]
                )
                if task["status"] == TaskStatus.QUEUED.value
                and int(task.get("exclusive_node") or 0)
                and self.task_aedt_backend_admitted(task)
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
        for allocation in self.db.list_allocations_with_live(limit=500):
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

    def open_allocation_for_task_record(
        self, task: dict, *, submit: bool = True
    ) -> dict | None:
        if self.same_node_as_task_id(task) or self.task_requested_allocation_id(task):
            return None
        strict_node_name = self.strict_task_node_name(task)
        strict_partition = (
            str(task.get("partition") or "auto")
            if strict_node_name
            else "auto"
        )
        if self.task_requires_gpu(task):
            model = self.choose_gpu_model_for_task(task) or self.choose_gpu_model_for_prewarm()
            resource_pool = f"gpu:{model}" if model else ""
            if not model or self.allocation_pool_in_backoff(resource_pool):
                return None
            return self.open_allocation_record(
                (
                    f"queued FEA GPU demand {model}"
                    if self.task_is_fea_bursty(task)
                    else f"queued GPU demand {model}"
                ),
                resource_pool=resource_pool,
                gpu_model=model,
                gpus=max(1, int(task.get("gpus") or self.gpu_prewarm_gpus_per_allocation)),
                exclusive_node=bool(task.get("exclusive_node")),
                required_capability=str(task.get("required_capability") or ""),
                env_profile=str(task.get("env_profile") or ""),
                account_name=self.task_requested_account_name(task),
                requested_cpus=int(task.get("cpus") or 0),
                requested_memory_mb=int(task.get("memory_mb") or 0),
                require_fea_eligible_node=self.task_is_fea_bursty(task),
                requested_node_name=strict_node_name,
                requested_partition=strict_partition,
                submit=submit,
            )
        if self.allocation_pool_in_backoff("cpu"):
            return None
        exclusive_node = bool(task.get("exclusive_node"))
        return self.open_allocation_record(
            (
                "queued FEA CPU demand"
                if self.task_is_fea_bursty(task)
                else "queued CPU demand"
            ),
            resource_pool="cpu",
            exclusive_node=exclusive_node,
            required_capability=str(task.get("required_capability") or ""),
            env_profile=str(task.get("env_profile") or ""),
            account_name=self.task_requested_account_name(task),
            requested_cpus=int(task.get("cpus") or 0) if exclusive_node or not self.task_is_fea_bursty(task) else 0,
            requested_memory_mb=int(task.get("memory_mb") or 0) if exclusive_node else 0,
            require_fea_eligible_node=self.task_is_fea_bursty(task),
            requested_node_name=strict_node_name,
            requested_partition=strict_partition,
            submit=submit,
        )

    def scale_in_idle_allocations(
        self,
        reservation_plan: _QueuedTaskAllocationReservationPlan | None = None,
        queued_tasks: list[dict] | None = None,
    ) -> None:
        tasks = (
            queued_tasks
            if queued_tasks is not None
            else self.queued_tasks_for_allocation_reservations()
        )
        plan = (
            reservation_plan
            if reservation_plan is not None
            else self.queued_task_allocation_reservation_plan(tasks)
        )
        protected_allocation_ids = set(plan.reservations)
        self.scale_in_unneeded_demand_allocations(
            reservation_plan=plan,
            queued_tasks=tasks,
        )
        self.enforce_cpu_partition_allocation_limits(
            protected_allocation_ids=protected_allocation_ids,
        )
        warm_allocations = [
            item
            for item in self.db.list_allocations_with_live(limit=500)
            if item["state"] == AllocationStatus.WARM.value
        ]
        self.scale_in_pool(
            [item for item in warm_allocations if (item.get("resource_pool") or "cpu") == "cpu"],
            self.min_warm_allocations,
            self.allocation_scale_in_idle_seconds,
            protected_allocation_ids=protected_allocation_ids,
        )
        self.scale_in_pool(
            [item for item in warm_allocations if (item.get("resource_pool") or "cpu").startswith("gpu:")],
            self.gpu_prewarm_min_warm_allocations if self.gpu_prewarm_enabled else 0,
            self.allocation_scale_in_idle_seconds,
            protected_allocation_ids=protected_allocation_ids,
        )

    def scale_in_pool(
        self,
        warm_allocations: list[dict],
        minimum: int,
        idle_seconds: int,
        *,
        protected_allocation_ids: set[int] | None = None,
    ) -> None:
        protected_ids = protected_allocation_ids or set()
        excess = len(warm_allocations) - minimum
        if excess <= 0:
            return
        closable_allocations = [
            allocation
            for allocation in warm_allocations
            if int(allocation.get("id") or 0) not in protected_ids
        ]
        closable_allocations.sort(
            key=lambda item: item.get("last_active_at")
            or item.get("started_at")
            or item.get("created_at")
            or ""
        )
        for allocation in closable_allocations[:excess]:
            last_active = self._timestamp(allocation.get("last_active_at") or allocation.get("started_at") or allocation.get("created_at"))
            if not last_active:
                continue
            if (self._now() - last_active).total_seconds() >= idle_seconds:
                self.close_allocation(allocation, "idle scale-in")

    def enforce_cpu_partition_allocation_limits(
        self,
        *,
        protected_allocation_ids: set[int] | None = None,
    ) -> None:
        if not self.cpu_partition_allocation_limits:
            return
        protected_ids = protected_allocation_ids or set()
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
                    if int(allocation.get("id") or 0) not in protected_ids
                    if not self.active_task_ids_for_allocation(int(allocation["id"]))
                    and allocation["state"] not in {AllocationStatus.DRAINING.value, AllocationStatus.CLOSING.value}
                    # AEDT pools deliberately use up to floor(node CPUs / pool
                    # CPUs) pinned allocations.  The generic cpu2 count limit
                    # must not collapse four exact 64-CPU pools back to two.
                    and not self.allocation_is_dedicated_aedt_pool(allocation)
                    # A durable FEA demand lane is retired by the reservation-
                    # aware scale-in pass.  The generic per-node limiter also
                    # runs independently early in a tick, so it must never
                    # destroy that lane before its queued task can attach.
                    and self.allocation_demand_profile(allocation) != "fea"
                ]
                closable.sort(
                    key=lambda allocation: (
                        state_rank.get(str(allocation.get("state") or ""), 9),
                        allocation.get("created_at") or "",
                    )
                )
                for allocation in closable[:excess]:
                    self.close_allocation(allocation, f"{partition} node {node_name} CPU allocation limit {limit}")

    def scale_in_unneeded_demand_allocations(
        self,
        reservation_plan: _QueuedTaskAllocationReservationPlan | None = None,
        queued_tasks: list[dict] | None = None,
    ) -> None:
        tasks = (
            queued_tasks
            if queued_tasks is not None
            else self.queued_tasks_for_allocation_reservations()
        )
        reservations = (
            reservation_plan.reservations
            if reservation_plan is not None
            else self.queued_task_allocation_reservations(tasks)
        )
        reserved_allocation_ids = set(reservations)
        queued_tasks_by_id = {int(task["id"]): task for task in tasks}
        desired_cpu_shape = self.choose_allocation_shape(resource_pool="cpu")
        desired_cpu_pool_cpus = int(desired_cpu_shape.get("cpus") or 0) if desired_cpu_shape else 0
        demand_allocations = [
            allocation
            for allocation in self.db.list_allocations_with_live(limit=500)
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
            allocation_is_reserved = int(allocation["id"]) in reserved_allocation_ids
            if (
                (allocation.get("resource_pool") or "cpu") == "cpu"
                and allocation["state"] == AllocationStatus.PENDING.value
                and "QOSMaxCpuPerNode" in str(allocation.get("pending_reason") or "")
            ):
                self.backoff_rejected_allocation_shape(allocation)
                self.close_allocation(allocation, "CPU demand allocation exceeds QOS CPU-per-node limit")
                continue
            # A profiled FEA reservation is the capacity authority.  Dynamic
            # pestat preferences can flip between cpu1/cpu2 on every long tick;
            # replacing its already reserved request merely resets Slurm queue
            # age (and can alternate forever).  Also preserve structurally
            # valid AssocMaxJobsLimit waits.  Generic unprofiled reservations
            # still follow the shape-replacement policy below.  The structural
            # QOS rejection above remains fatal because it can never start.
            pending_reason = str(allocation.get("pending_reason") or "").lower()
            preserve_waiting_reservation = "assocmaxjobslimit" in pending_reason
            if allocation_is_reserved and (
                self.allocation_demand_profile(allocation) == "fea"
                or preserve_waiting_reservation
            ):
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
                if self.cpu_demand_allocation_superseded_by_shape(
                    allocation, reserved_shape
                ):
                    self.close_allocation(
                        allocation,
                        f"CPU demand allocation superseded by current-fit partition {reserved_shape.get('partition')}",
                    )
                    continue
            elif self.pinned_gpu_cpu_demand_allocation_covers_queue(allocation, reserved_allocation_ids):
                continue
            elif self.cpu_demand_allocation_superseded_by_shape(
                allocation, desired_cpu_shape
            ):
                self.close_allocation(
                    allocation,
                    f"CPU demand allocation superseded by current-fit partition {desired_cpu_shape.get('partition')}",
                )
                continue
            if (
                allocation_desired_cpu_pool_cpus
                and (allocation.get("resource_pool") or "cpu") == "cpu"
                and allocation["state"] == AllocationStatus.PENDING.value
                and not int(allocation.get("exclusive_node") or 0)
                and not self.pinned_gpu_cpu_demand_allocation_covers_queue(allocation, reserved_allocation_ids)
                and int(allocation.get("total_cpus") or 0) < allocation_desired_cpu_pool_cpus
            ):
                self.close_allocation(allocation, "undersized CPU demand allocation after pool sizing policy change")
                continue
            if allocation_is_reserved:
                continue
            if tasks and self.pending_demand_allocation_in_shape_grace(allocation):
                continue
            if self.warm_demand_allocation_in_attach_grace(allocation, tasks):
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
        cpu_only_nodes: bool = False,
        aedt_pool_node_sharing: bool = False,
        aedt_pool_max_sessions: int = 0,
        requested_node_name: str = "",
        requested_partition: str = "auto",
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
            cpu_only_nodes=cpu_only_nodes,
            aedt_pool_node_sharing=aedt_pool_node_sharing,
            aedt_pool_max_sessions=aedt_pool_max_sessions,
            requested_node_name=requested_node_name,
            requested_partition=requested_partition,
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
        cpu_only_nodes: bool = False,
        aedt_pool_node_sharing: bool = False,
        aedt_pool_max_sessions: int = 0,
        requested_node_name: str = "",
        requested_partition: str = "auto",
        submit: bool = True,
    ) -> dict | None:
        account = self.choose_account_for_allocation(
            preferred_accounts=preferred_accounts,
            required_capability=required_capability,
            env_profile=env_profile,
            account_name=account_name,
            require_fea_storage_headroom=require_fea_eligible_node,
            # A standalone demand allocation is useful only if its account
            # can safely admit the first project.  Reserve that project's
            # storage growth now instead of selecting a near-floor account
            # that the final queued -> attaching guard must immediately
            # reject.
            storage_additional_future_projects=(
                1 if require_fea_eligible_node else 0
            ),
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
            cpu_only_nodes=cpu_only_nodes,
            aedt_pool_node_sharing=aedt_pool_node_sharing,
            aedt_pool_max_sessions=aedt_pool_max_sessions,
            requested_node_name=requested_node_name,
            requested_partition=requested_partition,
        )
        if not shape:
            return None
        stamp = int(time.time())
        remote_dir = posixpath.join(
            workspace_runs_dir(account.remote_workspace, stamp),
            f"allocation-reserved-{uuid.uuid4().hex}-{stamp}",
        )
        allocation_id = self.db.create_allocation(
            account_name=account.name,
            partition=shape["partition"],
            node_name=(
                shape["node_name"]
                if (resource_pool or "cpu") == "cpu" or requested_node_name
                else ""
            ),
            total_cpus=shape["cpus"],
            total_memory_mb=shape["memory_mb"],
            total_gpus=shape["gpus"],
            gpu_model=shape["gpu_model"],
            resource_pool=resource_pool,
            exclusive_node=shape["exclusive_node"],
            remote_dir=remote_dir,
            stdout_path=posixpath.join(remote_dir, "allocation-%j.out"),
            stderr_path=posixpath.join(remote_dir, "allocation-%j.err"),
            drain_reason=reason,
            pending_reason=ALLOCATION_SUBMISSION_RESERVED,
        )
        allocation = self.db.get_allocation(allocation_id)
        if not allocation:
            return None
        if not submit:
            return allocation
        successful_ids = self.submit_reserved_allocation_records([allocation])
        if allocation_id not in successful_ids:
            return None
        return self.db.get_allocation(allocation_id)

    def submit_reserved_allocation_records(
        self, allocations: list[dict]
    ) -> set[int]:
        """Submit a durable allocation plan with one serial worker per account."""
        if not allocations:
            return set()
        accounts_by_name = {account.name: account for account in self.accounts}
        outcomes_by_id: dict[int, dict[str, Any]] = {}
        by_account: dict[str, list[dict]] = {}
        # Submission-phase claims are also fixed in global plan order on the
        # scheduler thread. Workers below perform remote I/O only, preventing
        # concurrent SQLite writers from becoming the next tick bottleneck.
        for allocation in allocations:
            allocation_id = int(allocation["id"])
            account_name = str(allocation.get("account_name") or "")
            if account_name not in accounts_by_name:
                outcomes_by_id[allocation_id] = {
                    "allocation_id": allocation_id,
                    "claimed": False,
                    "definitely_not_created": True,
                    "error": RuntimeError(
                        f"allocation account {account_name!r} is not configured"
                    ),
                }
                continue
            claimed = self.db.update_allocation_if_submission_claim(
                allocation_id,
                ALLOCATION_SUBMISSION_RESERVED,
                pending_reason=ALLOCATION_SUBMISSION_IN_PROGRESS,
                submitted_at="CURRENT_TIMESTAMP",
                failure_message="",
            )
            if not claimed:
                outcomes_by_id[allocation_id] = {
                    "allocation_id": allocation_id,
                    "claimed": False,
                }
                continue
            current = self.db.get_allocation(allocation_id)
            if current is None:
                outcomes_by_id[allocation_id] = {
                    "allocation_id": allocation_id,
                    "claimed": True,
                    "error": RuntimeError(
                        "claimed allocation disappeared before remote submission"
                    ),
                }
                continue
            by_account.setdefault(account_name, []).append(current)

        def submit_account(
            account_name: str, account_allocations: list[dict]
        ) -> list[dict[str, Any]]:
            account = accounts_by_name.get(account_name)
            if account is None:
                raise RuntimeError(f"allocation account {account_name!r} is not configured")
            # One client (and therefore one SSH transport) is owned by this
            # account worker.  Its loop is the serialization boundary.
            try:
                client = self._client(account)
            except Exception as exc:
                return [
                    {
                        "allocation_id": int(allocation["id"]),
                        "claimed": True,
                        "definitely_not_created": True,
                        "error": exc,
                    }
                    for allocation in account_allocations
                ]
            outcomes: list[dict[str, Any]] = []
            for allocation in account_allocations:
                allocation_id = int(allocation["id"])
                time_limit = (
                    self.gpu_prewarm_time_limit
                    if str(allocation.get("resource_pool") or "").startswith("gpu:")
                    else self.allocation_time_limit
                )
                try:
                    result = client.submit_allocation(allocation, time_limit)
                except Exception as exc:
                    outcomes.append(
                        {
                            "allocation_id": allocation_id,
                            "claimed": True,
                            "definitely_not_created": isinstance(
                                exc, AllocationSubmissionNotCreated
                            ),
                            "error": exc,
                        }
                    )
                    continue
                outcomes.append(
                    {
                        "allocation_id": allocation_id,
                        "claimed": True,
                        "result": result,
                    }
                )
            return outcomes

        account_outcomes = self._fan_out_by_account(by_account, submit_account)
        for account_name, outcome in account_outcomes.items():
            if isinstance(outcome, Exception):
                for allocation in by_account.get(account_name, []):
                    outcomes_by_id[int(allocation["id"])] = {
                        "allocation_id": int(allocation["id"]),
                        "claimed": True,
                        # Executor budget expiry can leave the worker inside
                        # sbatch. Preserve the claim for exact reconciliation.
                        "definitely_not_created": False,
                        "error": outcome,
                    }
                continue
            for item in outcome:
                outcomes_by_id[int(item["allocation_id"])] = item

        successful_ids: set[int] = set()
        for allocation in allocations:
            allocation_id = int(allocation["id"])
            outcome = outcomes_by_id.get(allocation_id)
            if not outcome:
                continue
            error = outcome.get("error")
            if error is not None:
                expected_phase = (
                    ALLOCATION_SUBMISSION_IN_PROGRESS
                    if outcome.get("claimed")
                    else ALLOCATION_SUBMISSION_RESERVED
                )
                if (
                    outcome.get("claimed")
                    and not outcome.get("definitely_not_created")
                ):
                    # The SSH result was lost after the sbatch boundary. Keep
                    # the exact claim live; per-tick recovery will adopt the
                    # deterministic pool-<id> job or fail it after grace.
                    message = (
                        "allocation sbatch outcome ambiguous; awaiting exact "
                        f"Slurm reconciliation: {error}"
                    )
                    self.db.update_allocation_if_submission_claim(
                        allocation_id,
                        ALLOCATION_SUBMISSION_IN_PROGRESS,
                        failure_message=message,
                    )
                    self.record_event(
                        "allocation_submission_recovery_held",
                        message,
                        entity_type="allocation",
                        entity_id=allocation_id,
                        account_name=str(allocation.get("account_name") or ""),
                    )
                    blockers = list(
                        self._allocation_submission_recovery.get("blockers")
                        or []
                    )
                    blockers = [
                        blocker
                        for blocker in blockers
                        if int(blocker.get("allocation_id") or 0)
                        != allocation_id
                    ]
                    blockers.append(
                        {
                            "allocation_id": allocation_id,
                            "account_name": str(
                                allocation.get("account_name") or ""
                            ),
                            "reason": message,
                        }
                    )
                    self._allocation_submission_recovery = {
                        "blocked": True,
                        "blockers": blockers,
                    }
                    continue
                self.db.update_allocation_if_submission_claim(
                    allocation_id,
                    expected_phase,
                    state=AllocationStatus.FAILED.value,
                    pending_reason="",
                    failure_message=(
                        f"{allocation.get('drain_reason') or 'allocation submission'}: {error}"
                    ),
                    closed_at="CURRENT_TIMESTAMP",
                )
                self.record_event(
                    "allocation_submission_failed",
                    str(error) or type(error).__name__,
                    entity_type="allocation",
                    entity_id=allocation_id,
                    account_name=str(allocation.get("account_name") or ""),
                )
                continue
            result = outcome.get("result")
            if not outcome.get("claimed") or not isinstance(result, dict):
                continue
            acknowledged = self.db.update_allocation_if_submission_claim(
                allocation_id,
                ALLOCATION_SUBMISSION_IN_PROGRESS,
                state=AllocationStatus.PENDING.value,
                pending_reason="",
                failure_message="",
                **result,
            )
            if not acknowledged:
                # A concurrent close won the DB claim after sbatch returned.
                # Reap the now-unowned remote job instead of leaking it.
                slurm_job_id = str(result.get("slurm_job_id") or "")
                account = accounts_by_name.get(
                    str(allocation.get("account_name") or "")
                )
                if account is not None and slurm_job_id:
                    try:
                        self._client(account).cancel(slurm_job_id)
                    except Exception:
                        LOGGER.exception(
                            "failed to reap unacknowledged allocation job %s",
                            slurm_job_id,
                        )
                continue
            successful_ids.add(allocation_id)
            self.record_event(
                "allocation_opened",
                (
                    f"{allocation.get('drain_reason') or 'allocation opened'} "
                    f"(pool {allocation.get('resource_pool') or 'cpu'}, "
                    f"slurm job {result.get('slurm_job_id')})"
                ),
                entity_type="allocation",
                entity_id=allocation_id,
                account_name=str(allocation.get("account_name") or ""),
            )
        return successful_ids

    def choose_account_for_allocation(
        self,
        preferred_accounts: list[str] | None = None,
        required_capability: str = "",
        env_profile: str = "",
        account_name: str = "",
        require_fea_storage_headroom: bool = False,
        storage_additional_future_projects: int = 0,
    ) -> AccountConfig | None:
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in self.snapshots()}
        submitted_open_by_account: dict[str, int] = {}
        submitted_pending_by_account: dict[str, int] = {}
        unsubmitted_open_by_account: dict[str, int] = {}
        unsubmitted_pending_by_account: dict[str, int] = {}
        for allocation in self.db.list_allocations_with_live(limit=500):
            account_name_for_allocation = str(allocation.get("account_name") or "")
            submitted = bool(str(allocation.get("slurm_job_id") or ""))
            open_counts = (
                submitted_open_by_account
                if submitted
                else unsubmitted_open_by_account
            )
            pending_counts = (
                submitted_pending_by_account
                if submitted
                else unsubmitted_pending_by_account
            )
            if allocation["state"] in {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
                AllocationStatus.CLOSING.value,
            }:
                open_counts[account_name_for_allocation] = (
                    open_counts.get(account_name_for_allocation, 0) + 1
                )
            if allocation["state"] == AllocationStatus.PENDING.value:
                pending_counts[account_name_for_allocation] = (
                    pending_counts.get(account_name_for_allocation, 0) + 1
                )
        candidates: list[tuple[AccountConfig, int, int, int, bool]] = []
        requested_accounts = self.requested_accounts(account_name)
        for account in self.accounts:
            if requested_accounts and account.name not in requested_accounts:
                continue
            if not self.account_supports(account, required_capability, env_profile):
                continue
            if require_fea_storage_headroom and self.account_storage_blocked(
                account,
                for_fea=True,
                additional_future_projects=max(
                    0, int(storage_additional_future_projects or 0)
                ),
            ):
                continue
            snapshot = snapshots_by_name.get(account.name)
            if not snapshot:
                continue
            max_total = max(0, account.max_total_jobs - self.allocation_reserved_job_slots)
            submitted_open = submitted_open_by_account.get(account.name, 0)
            submitted_pending = submitted_pending_by_account.get(account.name, 0)
            unsubmitted_open = unsubmitted_open_by_account.get(account.name, 0)
            unsubmitted_pending = unsubmitted_pending_by_account.get(account.name, 0)
            # Submitted local rows should already be represented in the Slurm
            # snapshot, so max() avoids double counting them.  A submit=False
            # reservation has no Slurm job id and therefore must be debited on
            # top of that snapshot immediately within this planning pass.
            current_total = (
                max(snapshot.running + snapshot.pending, submitted_open)
                + unsubmitted_open
            )
            current_pending = (
                max(snapshot.pending, submitted_pending) + unsubmitted_pending
            )
            if current_total >= max_total:
                continue
            if current_pending >= account.max_pending_jobs:
                continue
            candidates.append(
                (
                    account,
                    current_total,
                    snapshot.running,
                    current_pending,
                    snapshot.running
                    < min(account.max_running_jobs, snapshot.max_running),
                )
            )
        if not candidates:
            return None
        ordered_preferences = preferred_accounts or self.requested_accounts(account_name)
        preferred_index = {name: index for index, name in enumerate(ordered_preferences)}
        return min(
            candidates,
            key=lambda candidate: (
                # A job submitted through an account that has already reached
                # MaxJobs can only wait in AssocMaxJobsLimit.  Prefer any
                # compatible account with immediate running headroom before
                # balancing queued demand across saturated accounts.
                0 if candidate[4] else 1,
                0 if candidate[0].name in preferred_index else 1,
                preferred_index.get(candidate[0].name, len(preferred_index)),
                # Include durable reservations made earlier in this planning
                # pass. Snapshot.score alone is stale until the next Slurm
                # poll and concentrates an entire batch on one account,
                # defeating cross-account submission fan-out.
                candidate[1],
                candidate[2],
                candidate[3],
                candidate[0].name,
            ),
        )[0]

    def live_gpu_allocations(self) -> list[dict]:
        return [
            allocation
            for allocation in self.db.list_allocations_with_live(limit=500)
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
        cpu_only_nodes: bool = False,
        aedt_pool_node_sharing: bool = False,
        aedt_pool_max_sessions: int = 0,
        requested_node_name: str = "",
        requested_partition: str = "auto",
    ) -> dict | None:
        requested_node_name = str(requested_node_name or "").strip()
        requested_partition = str(requested_partition or "auto").strip() or "auto"
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
        # For shared AEDT pools these two request fields describe one complete
        # Desktop session, not merely generic allocation floors.  Keeping both
        # footprints explicit prevents a 13-CPU session floor from expanding
        # into a 64-CPU/768-GiB allocation that can only host four sessions.
        aedt_session_cpus = int(requested_cpus or 0)
        aedt_session_memory_mb = int(requested_memory_mb or 0)
        aedt_session_limit = max(0, int(aedt_pool_max_sessions or 0))
        if aedt_pool_node_sharing and (
            aedt_session_cpus <= 0 or aedt_session_memory_mb <= 0
        ):
            return None
        aedt_reserved_cpus_by_node: dict[str, int] = {}
        aedt_pending_cpus_by_node: dict[str, int] = {}
        aedt_reserved_memory_by_node: dict[str, int] = {}
        aedt_pending_memory_by_node: dict[str, int] = {}
        if aedt_pool_node_sharing:
            live_states = {
                AllocationStatus.PENDING.value,
                AllocationStatus.WARM.value,
                AllocationStatus.ACTIVE.value,
                AllocationStatus.DRAINING.value,
                AllocationStatus.CLOSING.value,
            }
            for allocation in self.db.list_allocations_with_live(
                limit=0, live_limit=10000
            ):
                if allocation.get("state") not in live_states:
                    continue
                node_name = str(allocation.get("node_name") or "")
                if not node_name:
                    continue
                aedt_reserved_cpus_by_node[node_name] = (
                    aedt_reserved_cpus_by_node.get(node_name, 0)
                    + max(0, int(allocation.get("total_cpus") or 0))
                )
                aedt_reserved_memory_by_node[node_name] = (
                    aedt_reserved_memory_by_node.get(node_name, 0)
                    + max(0, int(allocation.get("total_memory_mb") or 0))
                )
                if allocation.get("state") == AllocationStatus.PENDING.value:
                    aedt_pending_cpus_by_node[node_name] = (
                        aedt_pending_cpus_by_node.get(node_name, 0)
                        + max(0, int(allocation.get("total_cpus") or 0))
                    )
                    aedt_pending_memory_by_node[node_name] = (
                        aedt_pending_memory_by_node.get(node_name, 0)
                        + max(0, int(allocation.get("total_memory_mb") or 0))
                    )
        dynamic_warm_gpu_count = wants_gpu and resource_pool.startswith("gpu:") and int(gpus or 0) <= 0
        target_gpu_count = max(1, int(gpus or self.gpu_prewarm_gpus_per_allocation))
        minimum_gpu_count = max(1, self.gpu_prewarm_min_gpus_per_allocation) if dynamic_warm_gpu_count else max(1, int(gpus or 1))
        target_models = gpu_model_candidates(gpu_model)
        target_partition = (
            requested_partition
            if requested_partition not in {"", "auto"}
            else self.gpu_prewarm_partition
            if wants_gpu
            else self.allocation_partition
        )
        preferred_full_gpu_partitions = (
            self.preferred_full_gpu_partitions(
                ",".join(target_models) if target_models else gpu_model,
                target_gpu_count,
                allow_multi=dynamic_warm_gpu_count,
                require_current_fit=not dynamic_warm_gpu_count,
            )
            if wants_gpu
            and target_partition == "auto"
            and not requested_node_name
            else []
        )
        preferred_full_gpu_partition_set = set(preferred_full_gpu_partitions)
        occupied_by_partition: dict[str, set[str]] = {}
        reserved_nodes = self.reserved_allocation_nodes()
        for node in nodes:
            if requested_node_name and node.hostname != requested_node_name:
                continue
            node_free_cpus_for_shape = node.sched_free_cpus if wants_gpu else node.effective_free_cpus
            node_free_memory_for_shape = max(0, int(node.free_memory_mb))
            if aedt_pool_node_sharing:
                # pestat already reflects running Slurm jobs, while the DB also
                # includes pinned pending allocations not visible there yet.
                # Taking the minimum avoids both stale over-placement and double
                # subtraction, and naturally caps 256/64 at four allocations.
                aggregate_free = max(
                    0,
                    int(node.cpu_total)
                    - aedt_reserved_cpus_by_node.get(node.hostname, 0),
                )
                node_free_cpus_for_shape = min(
                    max(
                        0,
                        node_free_cpus_for_shape
                        - aedt_pending_cpus_by_node.get(node.hostname, 0),
                    ),
                    aggregate_free,
                )
                # Running jobs are already reflected in pestat's free memory,
                # while pinned pending jobs are not.  The aggregate reservation
                # bound also protects against actual free memory lagging behind
                # Slurm's committed memory accounting.
                aggregate_free_memory = max(
                    0,
                    int(node.memory_mb)
                    - aedt_reserved_memory_by_node.get(node.hostname, 0),
                )
                node_free_memory_for_shape = min(
                    max(
                        0,
                        node_free_memory_for_shape
                        - aedt_pending_memory_by_node.get(node.hostname, 0),
                    ),
                    aggregate_free_memory,
                )
            if node.state not in {"idle", "mix"} or node_free_cpus_for_shape <= 0:
                continue
            if require_fea_eligible_node and not self.fea_allocation_accepts_task({"node_name": node.hostname}):
                continue
            if (
                not wants_gpu
                and not aedt_pool_node_sharing
                and self.cpu_partition_allocation_limit_reached(
                    node.partition, node.hostname
                )
            ):
                continue
            if preferred_full_gpu_partition_set and node.partition not in preferred_full_gpu_partition_set:
                continue
            if exclusive_node and (node.state != "idle" or int(node.cpu_used) > 0):
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
            inventory = inventory_by_node.get(node.hostname, {})
            node_gpu_count = int(inventory.get("gpu_count") or 0)
            node_gpu_used = int(inventory.get("gpu_used_count") or 0)
            node_gpu_model = normalize_gpu_model(str(inventory.get("gpu_model") or ""))
            node_is_gpu_partition = node.partition.startswith("gpu") or node_gpu_count > 0
            pins_to_node = (
                bool(requested_node_name and wants_gpu)
                or dynamic_warm_gpu_count
                or (wants_shared_cpu_pool and node_is_gpu_partition)
            )
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
                if node_gpu_count > 0 and (
                    cpu_only_nodes
                    or (
                        target_partition == "auto"
                        and not self.cpu_pool_allow_gpu_partitions
                    )
                ):
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
                if aedt_pool_node_sharing:
                    target_cpu_budget = min(
                        cpu_capacity,
                        max(
                            aedt_session_cpus,
                            int(self.allocation_cpus or cpu_capacity),
                        ),
                    )
                    session_count = min(
                        target_cpu_budget // aedt_session_cpus,
                        available_cpus // aedt_session_cpus,
                        node_free_memory_for_shape // aedt_session_memory_mb,
                    )
                    if aedt_session_limit:
                        session_count = min(session_count, aedt_session_limit)
                    if session_count <= 0:
                        continue
                    cpus = session_count * aedt_session_cpus
                    memory_mb = session_count * aedt_session_memory_mb
                elif node_is_gpu_partition:
                    minimum_pool_cpus = max(1, requested_cpu_floor)
                    if available_cpus < minimum_pool_cpus:
                        continue
                    cpus = available_cpus
                else:
                    target_pool_cpus = min(max(1, int(self.allocation_cpus or cpu_capacity)), cpu_capacity)
                    minimum_pool_cpus = max(
                        requested_cpu_floor, target_pool_cpus
                    )
                    if available_cpus < minimum_pool_cpus:
                        continue
                    cpus = minimum_pool_cpus
            elif wants_gpu or exclusive_node:
                cpus = minimum_cpus or self.allocation_cpus or available_cpus
            else:
                cpus = self.allocation_cpus or available_cpus
            cpus = max(1, min(cpus, available_cpus))
            if not aedt_pool_node_sharing:
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
                    not requested_node_name
                    and not wants_gpu
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
                    not requested_node_name
                    and wants_shared_cpu_pool
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
                # AEDT pool demand is durable and already bounded by the
                # reconciler's exact session deficit plus each account's
                # Slurm pending-job ceiling.  A generic partition backoff from
                # one old pending allocation must not suppress every other
                # account's AEDT request; Slurm remains the placement authority.
                if (
                    not aedt_pool_node_sharing
                    and self.allocation_shape_in_backoff(resource_pool, shape)
                ):
                    if used_partition_spread and not self.allocation_shape_in_backoff(
                        resource_pool, single_partition_shape
                    ):
                        return single_partition_shape
                    continue
                return shape
            return None
        if requested_node_name:
            # A strict per-task request is fail-closed.  Inventory staleness,
            # pressure, backoff, or insufficient capacity must leave the task
            # queued; a partition-only fallback would violate the contract.
            return None
        if require_fea_eligible_node and nodes and not aedt_pool_node_sharing:
            return None
        if (
            target_partition != "auto"
            and self.is_single_job_partition(target_partition)
            and not aedt_pool_node_sharing
        ):
            return None
        partition_request = {
            "gpus": target_gpu_count if wants_gpu else 0,
            "gpu_model": ",".join(target_models) if target_models else gpu_model,
        }
        preferred_full_gpu_partition = ",".join(preferred_full_gpu_partitions)
        partition = target_partition if target_partition != "auto" else preferred_full_gpu_partition or self.choose_partition(partition_request)
        if not partition:
            return None
        if not wants_gpu and not aedt_pool_node_sharing:
            partition_names = self.partition_spec_names(partition)
            if partition_names and all(self.cpu_partition_allocation_partition_saturated(item, nodes) for item in partition_names):
                return None
        if self.is_single_job_partition(partition) and not aedt_pool_node_sharing:
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
        if aedt_pool_node_sharing:
            target_cpu_budget = max(
                aedt_session_cpus,
                int(self.allocation_cpus or fallback_cpu_capacity or aedt_session_cpus),
            )
            if fallback_cpu_capacity:
                target_cpu_budget = min(target_cpu_budget, fallback_cpu_capacity)
            session_count = target_cpu_budget // aedt_session_cpus
            if aedt_session_limit:
                session_count = min(session_count, aedt_session_limit)
            if fallback_memory_capacity:
                session_count = min(
                    session_count,
                    fallback_memory_capacity // aedt_session_memory_mb,
                )
            if session_count <= 0:
                return None
            fallback_cpus = session_count * aedt_session_cpus
            fallback_memory_mb = session_count * aedt_session_memory_mb
        elif wants_shared_cpu_pool and fallback_cpu_capacity:
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
            "memory_mb": (
                fallback_memory_mb
                if aedt_pool_node_sharing
                else requested_memory_mb
                or self._memory_mb(self.allocation_memory)
                or fallback_memory_capacity
                or 16384
            ),
            "gpus": target_gpu_count if wants_gpu else 0,
            "gpu_model": (target_models[0] if target_models else "") if wants_gpu else "",
            "exclusive_node": exclusive_node,
        }
        if (
            not aedt_pool_node_sharing
            and self.allocation_shape_in_backoff(resource_pool, fallback_shape)
        ):
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
        for task in self.db.list_tasks(
            limit=5000, statuses=[TaskStatus.QUEUED.value]
        ):
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
