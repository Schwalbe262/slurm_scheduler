from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import secrets
import shlex
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .db import Database
from .models import SchedulingProfile, TaskCreate, TaskStatus
from .aedt_session_host import (
    EXPECTED_AEDT_VERSION,
    SUPPORTED_DSO_PROFILE,
    SUPPORTED_DSO_PROFILES,
    canonical_expected_session_profile,
)
from .aedt_automation_lock import automation_lock_path


LOGGER = logging.getLogger(__name__)

# The hard cap represents capacity that can become assignable without replacing
# the Desktop.  Draining/unhealthy rows remain live lifecycle records, but they
# cannot satisfy capacity or consume the global replacement-start budget.
SESSION_HARD_CAP_STATES = ("starting", "ready", "busy")
SESSION_VISIBLE_STATES = (*SESSION_HARD_CAP_STATES, "draining", "unhealthy")
SESSION_HISTORY_STATES = ("failed", "closed")
SESSION_HISTORY_LIMIT = 30
SESSION_ASSIGNABLE_STATES = ("ready", "busy")
LEASE_LIVE_STATES = (
    "queued",
    "offered",
    "leased",  # protocol-v1 compatibility
    "attaching",
    "active",
    "releasing",
)
LEASE_SLOT_STATES = ("offered", "leased", "attaching", "active", "releasing")
LEASE_TERMINAL_STATES = ("released", "failed", "cancelled", "expired")
TASK_TERMINAL_STATES = (
    TaskStatus.COMPLETED.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
)
DEAD_SESSION_IDENTITY_FIELDS = (
    "generation",
    "allocation_id",
    "host_id",
    "host_task_id",
    "host_process_id",
    "process_id",
)
SESSION_START_ACK_TIMEOUT_MESSAGE = "session start acknowledgement timed out"
SESSION_HEARTBEAT_TIMEOUT_MESSAGE = "session heartbeat expired"
FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON = (
    "AEDT pool faulted Desktop allocation recycle"
)
UNHEALTHY_ALLOCATION_RECYCLE_REASON = (
    "AEDT pool unhealthy/quarantined session allocation recycle"
)
ALLOCATION_AGE_ROTATION_REASON = "AEDT pool allocation age rotation"
DEFAULT_HOST_LAUNCH_STAGGER_SECONDS = 15
HOST_LAUNCH_STAGGER_ENV = "AEDT_POOL_HOST_LAUNCH_STAGGER_SECONDS"
HEARTBEAT_PERSIST_MAX_SECONDS = 30
RECONCILE_PLACEMENT_BATCH_SIZE = 32

# Families for which a rolling build may enable concurrent native solves.  The
# emergency ``serial`` mode ignores this allowlist; ``validated_parallel``
# restores it without requiring another code rollout.
PARALLEL_SAFE_NATIVE_SOLVE_FAMILIES = frozenset({"mft_validated_async"})
NATIVE_SOLVE_MODE_ENV = "SLURM_AEDT_POOL_NATIVE_SOLVE_MODE"
NATIVE_SOLVE_MODE_SERIAL = "serial"
NATIVE_SOLVE_MODE_VALIDATED_PARALLEL = "validated_parallel"
NATIVE_SOLVE_MODES = frozenset(
    {NATIVE_SOLVE_MODE_SERIAL, NATIVE_SOLVE_MODE_VALIDATED_PARALLEL}
)


AEDT_POOL_SCHEMA = """
CREATE TABLE IF NOT EXISTS aedt_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key TEXT NOT NULL UNIQUE,
    allocation_id INTEGER NOT NULL DEFAULT 0,
    account_name TEXT NOT NULL DEFAULT '',
    node_name TEXT NOT NULL DEFAULT '',
    host_id TEXT NOT NULL DEFAULT '',
    host_task_id INTEGER NOT NULL DEFAULT 0,
    host_process_id TEXT NOT NULL DEFAULT '',
    host_slurm_job_id TEXT NOT NULL DEFAULT '',
    actual_node_name TEXT NOT NULL DEFAULT '',
    start_claimed_at TEXT,
    endpoint TEXT NOT NULL DEFAULT '',
    process_id TEXT NOT NULL DEFAULT '',
    session_profile TEXT NOT NULL DEFAULT '',
    artifact_dir TEXT NOT NULL DEFAULT '',
    host_stdout_path TEXT NOT NULL DEFAULT '',
    host_stderr_path TEXT NOT NULL DEFAULT '',
    error_log_path TEXT NOT NULL DEFAULT '',
    journal_path TEXT NOT NULL DEFAULT '',
    native_snapshot_path TEXT NOT NULL DEFAULT '',
    runtime_metadata_json TEXT NOT NULL DEFAULT '{}',
    last_fault_evidence_json TEXT NOT NULL DEFAULT '{}',
    last_fault_at TEXT,
    host_token_hash TEXT NOT NULL DEFAULT '',
    slots_total INTEGER NOT NULL DEFAULT 2,
    state TEXT NOT NULL DEFAULT 'starting',
    generation INTEGER NOT NULL DEFAULT 1,
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    last_heartbeat_at TEXT,
    reuse_blocked_at TEXT,
    idle_since TEXT,
    drain_requested_at TEXT,
    quarantine_until TEXT,
    quarantine_reason TEXT NOT NULL DEFAULT '',
    solve_batch_sealed_at TEXT,
    solve_batch_generation INTEGER NOT NULL DEFAULT 0,
    closed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_aedt_sessions_state ON aedt_sessions(state);
CREATE INDEX IF NOT EXISTS idx_aedt_sessions_allocation ON aedt_sessions(allocation_id);
CREATE INDEX IF NOT EXISTS idx_aedt_sessions_node ON aedt_sessions(node_name);

CREATE TABLE IF NOT EXISTS aedt_project_leases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_key TEXT NOT NULL UNIQUE,
    project_name TEXT NOT NULL,
    placement_group TEXT,
    workload_family TEXT NOT NULL DEFAULT '',
    session_profile TEXT NOT NULL DEFAULT '',
    project_namespace TEXT NOT NULL DEFAULT '',
    isolation_policy TEXT NOT NULL DEFAULT 'family',
    workspace_path TEXT NOT NULL DEFAULT '',
    protocol_version INTEGER NOT NULL DEFAULT 1,
    task_id INTEGER NOT NULL DEFAULT 0,
    requested_allocation_id INTEGER NOT NULL DEFAULT 0,
    requested_node_name TEXT NOT NULL DEFAULT '',
    requested_session_id INTEGER NOT NULL DEFAULT 0,
    requested_session_generation INTEGER NOT NULL DEFAULT 0,
    exact_session_reservation_id INTEGER NOT NULL DEFAULT 0,
    exclusive_session INTEGER NOT NULL DEFAULT 0,
    session_id INTEGER,
    slot_index INTEGER,
    state TEXT NOT NULL DEFAULT 'queued',
    client_token_hash TEXT NOT NULL,
    failure_message TEXT NOT NULL DEFAULT '',
    requested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    offered_at TEXT,
    offer_expires_at TEXT,
    accepted_at TEXT,
    activated_at TEXT,
    solve_permit_at TEXT,
    solve_permit_generation INTEGER NOT NULL DEFAULT 0,
    native_pipeline_completed_at TEXT,
    native_pipeline_session_id INTEGER NOT NULL DEFAULT 0,
    native_pipeline_generation INTEGER NOT NULL DEFAULT 0,
    client_deadline_at TEXT,
    acquired_at TEXT,
    last_heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,
    release_requested_at TEXT,
    finished_at TEXT,
    fault_phase TEXT NOT NULL DEFAULT '',
    fault_kind TEXT NOT NULL DEFAULT '',
    fault_evidence_json TEXT NOT NULL DEFAULT '{}',
    mixed_canary_admission_id INTEGER NOT NULL DEFAULT 0,
    mixed_canary_session_id INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_aedt_leases_state ON aedt_project_leases(state);
CREATE INDEX IF NOT EXISTS idx_aedt_leases_session ON aedt_project_leases(session_id);
CREATE INDEX IF NOT EXISTS idx_aedt_leases_task ON aedt_project_leases(task_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aedt_leases_live_slot
ON aedt_project_leases(session_id, slot_index)
WHERE session_id IS NOT NULL
  AND state IN ('offered', 'leased', 'attaching', 'active', 'releasing');
CREATE INDEX IF NOT EXISTS idx_aedt_leases_project_name
ON aedt_project_leases(project_name, state);

CREATE TABLE IF NOT EXISTS aedt_exact_session_reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reservation_key TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    session_id INTEGER NOT NULL,
    session_generation INTEGER NOT NULL,
    session_profile TEXT NOT NULL,
    workload_family TEXT NOT NULL DEFAULT '',
    isolation_policy TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'reserved',
    lease_id INTEGER,
    failure_message TEXT NOT NULL DEFAULT '',
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    claimed_at TEXT,
    consumed_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(reservation_key, task_id)
);

CREATE INDEX IF NOT EXISTS idx_aedt_exact_reservations_session
ON aedt_exact_session_reservations(session_id, state);
CREATE INDEX IF NOT EXISTS idx_aedt_exact_reservations_key
ON aedt_exact_session_reservations(reservation_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aedt_exact_reservations_live_task
ON aedt_exact_session_reservations(task_id)
WHERE state IN ('reserved', 'claimed', 'consumed');
CREATE UNIQUE INDEX IF NOT EXISTS idx_aedt_exact_reservations_lease
ON aedt_exact_session_reservations(lease_id)
WHERE lease_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS aedt_pool_validations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL,
    baseline_desktops INTEGER NOT NULL DEFAULT 0,
    pooled_desktops INTEGER NOT NULL DEFAULT 0,
    baseline_projects INTEGER NOT NULL DEFAULT 0,
    pooled_projects INTEGER NOT NULL DEFAULT 0,
    runtime_ratio REAL NOT NULL DEFAULT 0,
    desktop_license_delta INTEGER NOT NULL DEFAULT 0,
    output_parity_passed INTEGER NOT NULL DEFAULT 0,
    solver_isolation_passed INTEGER NOT NULL DEFAULT 0,
    cancellation_isolation_passed INTEGER NOT NULL DEFAULT 0,
    crash_recovery_passed INTEGER NOT NULL DEFAULT 0,
    timeout_fault_injection_passed INTEGER NOT NULL DEFAULT 0,
    sibling_completion_passed INTEGER NOT NULL DEFAULT 0,
    sibling_terminal_output_passed INTEGER NOT NULL DEFAULT 0,
    sibling_data_rows_passed INTEGER NOT NULL DEFAULT 0,
    sibling_field_solution_passed INTEGER NOT NULL DEFAULT 0,
    fault_checkout_released_after_recycle_passed INTEGER NOT NULL DEFAULT 0,
    faulted_desktop_not_reused_passed INTEGER NOT NULL DEFAULT 0,
    mixed_mft_ipmsm_isolation_passed INTEGER NOT NULL DEFAULT 0,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_aedt_pool_validations_status ON aedt_pool_validations(status);

CREATE TABLE IF NOT EXISTS aedt_mixed_canary_admissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    placement_group TEXT NOT NULL UNIQUE,
    session_profile TEXT NOT NULL,
    expected_mft_projects INTEGER NOT NULL,
    expected_ipmsm_projects INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'open',
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    filled_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_aedt_mixed_canary_live_session
ON aedt_mixed_canary_admissions(session_id)
WHERE state IN ('open', 'filled');

CREATE TABLE IF NOT EXISTS aedt_mixed_canary_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admission_id INTEGER NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    workload_family TEXT NOT NULL,
    project_namespace TEXT NOT NULL,
    lease_id INTEGER,
    admitted_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_aedt_mixed_canary_slots_admission
ON aedt_mixed_canary_slots(admission_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aedt_mixed_canary_slots_lease
ON aedt_mixed_canary_slots(lease_id)
WHERE lease_id IS NOT NULL;
"""


DEFAULT_SETTINGS: dict[str, str] = {
    # The requested 250/500 topology is staged but deliberately disabled.
    "aedt_pool_enabled": "0",
    "aedt_pool_adapter_ready": "0",
    "aedt_pool_max_sessions": "250",
    "aedt_pool_min_idle_sessions": "3",
    "aedt_pool_target_projects": "500",
    "aedt_pool_projects_per_session": "2",
    "aedt_pool_project_cpus": "4",
    # Maxwell plus Icepak can coexist in one project.  Reserve a conservative
    # 32 GiB per project (64 GiB for the validated 1:2 host topology).
    "aedt_pool_project_memory_mb": "32768",
    "aedt_pool_node_cpu_factor": "1.0",
    # Both defaults exceed the node/client six-minute retry budget so a
    # five-minute control-plane outage cannot expire otherwise-live work.
    "aedt_pool_lease_ttl_seconds": "600",
    "aedt_pool_queued_stale_seconds": "90",
    "aedt_pool_offer_ack_seconds": "60",
    "aedt_pool_admission_deadline_seconds": "600",
    "aedt_pool_session_heartbeat_timeout_seconds": "600",
    "aedt_pool_unhealthy_recycle_grace_seconds": "180",
    "aedt_pool_session_start_timeout_seconds": "600",
    "aedt_pool_idle_ttl_seconds": "3600",
    "aedt_pool_allocation_max_age_seconds": "158400",
    "aedt_pool_scale_step_nodes": "4",
    "aedt_pool_required_capability": "",
    "aedt_pool_env_profile": "",
    "aedt_pool_account_name": "",
    # Node-visible HTTP base URL published by the optional control-plane
    # relay.  This is deliberately separate from the scheduler process's
    # static launch configuration: relay supervision may withdraw or replace
    # the URL while the web worker remains alive.
    "aedt_pool_control_plane_url": "",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sql_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_utc_time(value: str) -> datetime:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("timestamp is required")
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _heartbeat_persist_interval_seconds(
    timeout_seconds: int, identity: int
) -> int:
    """Bound and spread durable heartbeats well inside their expiry window."""

    ceiling = max(
        1,
        min(
            HEARTBEAT_PERSIST_MAX_SECONDS,
            max(1, int(timeout_seconds) // 3),
        ),
    )
    spread = min(6, ceiling - 1)
    return ceiling - (int(identity) % (spread + 1) if spread else 0)


def _heartbeat_is_fresh(
    value: str, now: datetime, interval_seconds: int
) -> bool:
    try:
        elapsed = (now - _parse_utc_time(value)).total_seconds()
    except ValueError:
        return False
    return 0 <= elapsed < max(1, int(interval_seconds))


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _derive_placement_group(project_name: str) -> str:
    normalized = project_name.strip().lower()
    if "ipmsm" in normalized:
        return "ipmsm"
    if normalized.startswith(("mft", "simulation")):
        return "mft"
    token_end = 0
    while token_end < len(normalized) and normalized[token_end].isalnum():
        token_end += 1
    return normalized[:token_end] or normalized


def canonical_workload_family(value: str, project_name: str) -> str:
    """Canonicalize namespace labels to the thin client's AEDT family key."""

    explicit = str(value or "").strip().lower()
    if explicit:
        return explicit
    normalized = str(project_name or "").strip().lower()
    if "pyaedt_motor" in normalized:
        return "ipmsm"
    return _derive_placement_group(normalized)


def _canonical_session_profile(value: Any) -> str:
    """Return a stable compatibility key for Desktop-global settings."""

    if value is None:
        return ""
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if not isinstance(value, str):
        raise ValueError("session_profile must be a string or object")
    return value.strip()


def _normalized_family(value: str, project_name: str) -> str:
    return canonical_workload_family(value, project_name)


def _short_node_name(value: str) -> str:
    return str(value or "").strip().lower().split(".", 1)[0]


def _bool_setting(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class AedtPoolConfig:
    enabled: bool
    adapter_ready: bool
    validation_passed: bool
    max_sessions: int
    min_idle_sessions: int
    target_projects: int
    projects_per_session: int
    project_cpus: int
    project_memory_mb: int
    node_cpu_factor: float
    lease_ttl_seconds: int
    queued_stale_seconds: int
    offer_ack_seconds: int
    admission_deadline_seconds: int
    session_heartbeat_timeout_seconds: int
    unhealthy_recycle_grace_seconds: int
    session_start_timeout_seconds: int
    idle_ttl_seconds: int
    allocation_max_age_seconds: int
    scale_step_nodes: int
    required_capability: str
    env_profile: str
    account_name: str
    control_plane_url: str

    @property
    def operational(self) -> bool:
        return self.enabled and self.adapter_ready and self.validation_passed


class AedtPoolService:
    """Durable control plane for pooled AEDT sessions.

    The service never imports PyAEDT and never opens or kills Electronics
    Desktop.  A session-host process is the sole owner of an AEDT process.  A
    project client owns only its lease and asks the host to close that project.
    This separation is intentional: an attach client must not kill a shared
    Desktop when its own solve is cancelled.
    """

    def __init__(
        self,
        db: Database,
        *,
        bootstrap_token: str = "",
        lease_client_token: str = "",
        now: Callable[[], datetime] = _utcnow,
        native_solve_mode: str | None = None,
    ) -> None:
        self.db = db
        self.bootstrap_token = bootstrap_token
        self.lease_client_token = str(lease_client_token or "").strip()
        self._now = now
        configured_native_solve_mode = str(
            native_solve_mode
            if native_solve_mode is not None
            else os.environ.get(NATIVE_SOLVE_MODE_ENV, NATIVE_SOLVE_MODE_SERIAL)
        ).strip().lower()
        if configured_native_solve_mode not in NATIVE_SOLVE_MODES:
            raise ValueError(
                f"{NATIVE_SOLVE_MODE_ENV} must be one of "
                f"{', '.join(sorted(NATIVE_SOLVE_MODES))}"
            )
        self.native_solve_mode = configured_native_solve_mode
        self._parallel_safe_native_solve_families = (
            PARALLEL_SAFE_NATIVE_SOLVE_FAMILIES
            if configured_native_solve_mode
            == NATIVE_SOLVE_MODE_VALIDATED_PARALLEL
            else frozenset()
        )
        self._lock = threading.RLock()
        # Reconcile intentionally holds _lock across one control-plane pass.
        # Config cache reads are independent and must not queue every HTTP
        # heartbeat behind that potentially long maintenance transaction.
        self._config_lock = threading.RLock()
        self._warm_spare_admission_checker: Callable[[int], tuple[int, str]] | None = None
        self._dead_session_process_checker: (
            Callable[[dict[str, Any]], tuple[bool, dict[str, Any]]] | None
        ) = None
        self._task_account_selector: Callable[[dict[str, Any]], str] | None = None
        self._config_cache: AedtPoolConfig | None = None
        self._config_cache_until = 0.0
        self._placement_cursor = 0

    def set_task_account_selector(
        self, selector: Callable[[dict[str, Any]], str] | None
    ) -> None:
        """Install the scheduler's read-only account choice for pooled demand."""

        self._task_account_selector = selector

    def init(self) -> None:
        with self.db.connect() as conn:
            conn.executescript(AEDT_POOL_SCHEMA)
            # Additive migration for databases created by an early pilot.
            session_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(aedt_sessions)").fetchall()
            }
            for name, ddl in {
                "start_claimed_at": "TEXT",
                "quarantine_until": "TEXT",
                "quarantine_reason": "TEXT NOT NULL DEFAULT ''",
                "host_task_id": "INTEGER NOT NULL DEFAULT 0",
                "host_process_id": "TEXT NOT NULL DEFAULT ''",
                "host_slurm_job_id": "TEXT NOT NULL DEFAULT ''",
                "actual_node_name": "TEXT NOT NULL DEFAULT ''",
                "session_profile": "TEXT NOT NULL DEFAULT ''",
                "artifact_dir": "TEXT NOT NULL DEFAULT ''",
                "host_stdout_path": "TEXT NOT NULL DEFAULT ''",
                "host_stderr_path": "TEXT NOT NULL DEFAULT ''",
                "error_log_path": "TEXT NOT NULL DEFAULT ''",
                "journal_path": "TEXT NOT NULL DEFAULT ''",
                "native_snapshot_path": "TEXT NOT NULL DEFAULT ''",
                "runtime_metadata_json": "TEXT NOT NULL DEFAULT '{}'",
                "last_fault_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
                "last_fault_at": "TEXT",
                "solve_batch_sealed_at": "TEXT",
                "solve_batch_generation": "INTEGER NOT NULL DEFAULT 0",
                "reuse_blocked_at": "TEXT",
            }.items():
                if name not in session_columns:
                    conn.execute(f"ALTER TABLE aedt_sessions ADD COLUMN {name} {ddl}")
            lease_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(aedt_project_leases)"
                ).fetchall()
            }
            for name, ddl in {
                "exclusive_session": "INTEGER NOT NULL DEFAULT 0",
                "placement_group": "TEXT",
                "workload_family": "TEXT NOT NULL DEFAULT ''",
                "session_profile": "TEXT NOT NULL DEFAULT ''",
                "project_namespace": "TEXT NOT NULL DEFAULT ''",
                "isolation_policy": "TEXT NOT NULL DEFAULT 'family'",
                "workspace_path": "TEXT NOT NULL DEFAULT ''",
                "protocol_version": "INTEGER NOT NULL DEFAULT 1",
                "offered_at": "TEXT",
                "offer_expires_at": "TEXT",
                "accepted_at": "TEXT",
                "activated_at": "TEXT",
                "solve_permit_at": "TEXT",
                "solve_permit_generation": "INTEGER NOT NULL DEFAULT 0",
                "native_pipeline_completed_at": "TEXT",
                "native_pipeline_session_id": "INTEGER NOT NULL DEFAULT 0",
                "native_pipeline_generation": "INTEGER NOT NULL DEFAULT 0",
                "client_deadline_at": "TEXT",
                "fault_phase": "TEXT NOT NULL DEFAULT ''",
                "fault_kind": "TEXT NOT NULL DEFAULT ''",
                "fault_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
                "mixed_canary_admission_id": "INTEGER NOT NULL DEFAULT 0",
                "mixed_canary_session_id": "INTEGER NOT NULL DEFAULT 0",
                "requested_session_id": "INTEGER NOT NULL DEFAULT 0",
                "requested_session_generation": "INTEGER NOT NULL DEFAULT 0",
                "exact_session_reservation_id": "INTEGER NOT NULL DEFAULT 0",
            }.items():
                if name not in lease_columns:
                    conn.execute(
                        f"ALTER TABLE aedt_project_leases ADD COLUMN {name} {ddl}"
                    )
            exact_reservation_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(aedt_exact_session_reservations)"
                ).fetchall()
            }
            for name, ddl in {
                "failure_message": "TEXT NOT NULL DEFAULT ''",
                "workload_family": "TEXT NOT NULL DEFAULT ''",
                "isolation_policy": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if name not in exact_reservation_columns:
                    conn.execute(
                        "ALTER TABLE aedt_exact_session_reservations "
                        f"ADD COLUMN {name} {ddl}"
                    )
            legacy_leases = conn.execute(
                """
                SELECT id, project_name FROM aedt_project_leases
                WHERE placement_group IS NULL OR TRIM(placement_group) = ''
                """
            ).fetchall()
            conn.executemany(
                """
                UPDATE aedt_project_leases
                SET placement_group = ?, workload_family = CASE
                    WHEN TRIM(COALESCE(workload_family, '')) = '' THEN ?
                    ELSE workload_family END
                WHERE id = ?
                """,
                (
                    (
                        _derive_placement_group(str(row["project_name"])),
                        _derive_placement_group(str(row["project_name"])),
                        int(row["id"]),
                    )
                    for row in legacy_leases
                ),
            )
            conn.execute("DROP INDEX IF EXISTS idx_aedt_leases_live_slot")
            conn.execute(
                """
                CREATE UNIQUE INDEX idx_aedt_leases_live_slot
                ON aedt_project_leases(session_id, slot_index)
                WHERE session_id IS NOT NULL
                  AND state IN (
                      'offered','leased','attaching','active','releasing'
                  )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_aedt_leases_project_name "
                "ON aedt_project_leases(project_name, state)"
            )
            # A deploy may land while a host is already closing a project.
            # Backfill the reuse barrier before placement can see that slot.
            conn.execute(
                """
                UPDATE aedt_sessions
                SET reuse_blocked_at = COALESCE(
                        (
                            SELECT MAX(COALESCE(l.release_requested_at, l.finished_at))
                            FROM aedt_project_leases l
                            WHERE l.session_id = aedt_sessions.id
                              AND (
                                  l.state = 'releasing'
                                  OR (
                                      l.state IN ('released','failed','cancelled','expired')
                                      AND l.finished_at > COALESCE(
                                          aedt_sessions.last_heartbeat_at, ''
                                      )
                                  )
                              )
                        ),
                        CURRENT_TIMESTAMP
                    ),
                    updated_at = CURRENT_TIMESTAMP
                WHERE reuse_blocked_at IS NULL
                  AND EXISTS (
                      SELECT 1 FROM aedt_project_leases l
                      WHERE l.session_id = aedt_sessions.id
                        AND (
                            l.state = 'releasing'
                            OR (
                                l.state IN ('released','failed','cancelled','expired')
                                AND l.finished_at > COALESCE(
                                    aedt_sessions.last_heartbeat_at, ''
                                )
                            )
                        )
                  )
                """
            )
            validation_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(aedt_pool_validations)").fetchall()
            }
            for name in (
                "timeout_fault_injection_passed",
                "sibling_completion_passed",
                "sibling_terminal_output_passed",
                "sibling_data_rows_passed",
                "sibling_field_solution_passed",
                "fault_checkout_released_after_recycle_passed",
                "faulted_desktop_not_reused_passed",
                "mixed_mft_ipmsm_isolation_passed",
            ):
                if name not in validation_columns:
                    conn.execute(
                        f"ALTER TABLE aedt_pool_validations ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0"
                    )
        self._invalidate_config()

    def _setting(self, key: str) -> str:
        value = self.db.get_setting(key)
        return DEFAULT_SETTINGS[key] if value is None else value

    def _invalidate_config(self) -> None:
        with self._config_lock:
            self._config_cache = None
            self._config_cache_until = 0.0

    def set_warm_spare_admission_checker(
        self,
        checker: Callable[[int], tuple[int, str]] | None,
    ) -> None:
        """Install the scheduler's fail-closed license headroom check."""
        self._warm_spare_admission_checker = checker

    def set_dead_session_process_checker(
        self,
        checker: Callable[
            [dict[str, Any]], tuple[bool, dict[str, Any]]
        ]
        | None,
    ) -> None:
        """Install the scheduler-owned, fail-closed remote PID probe."""

        self._dead_session_process_checker = checker

    def latest_validation(self) -> dict[str, Any] | None:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM aedt_pool_validations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            item = dict(row)
            try:
                item["evidence"] = json.loads(item.pop("evidence_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                item["evidence"] = {}
            return item

    def config(self) -> AedtPoolConfig:
        cache_now = time.monotonic()
        with self._config_lock:
            if self._config_cache is not None and cache_now < self._config_cache_until:
                return self._config_cache
            with self.db.connect() as conn:
                settings = dict(DEFAULT_SETTINGS)
                settings.update(
                    {
                        str(row["key"]): str(row["value"])
                        for row in conn.execute(
                            "SELECT key, value FROM scheduler_settings WHERE key LIKE 'aedt_pool_%'"
                        ).fetchall()
                    }
                )
                validation = conn.execute(
                    "SELECT status FROM aedt_pool_validations ORDER BY id DESC LIMIT 1"
                ).fetchone()

            def setting(key: str) -> str:
                return settings[key]

            resolved = AedtPoolConfig(
            enabled=_bool_setting(setting("aedt_pool_enabled")),
            adapter_ready=_bool_setting(setting("aedt_pool_adapter_ready")),
            validation_passed=bool(validation and validation["status"] == "passed"),
            max_sessions=max(0, int(setting("aedt_pool_max_sessions"))),
            min_idle_sessions=max(0, int(setting("aedt_pool_min_idle_sessions"))),
            target_projects=max(0, int(setting("aedt_pool_target_projects"))),
            projects_per_session=max(1, int(setting("aedt_pool_projects_per_session"))),
            project_cpus=max(1, int(setting("aedt_pool_project_cpus"))),
            project_memory_mb=max(1024, int(setting("aedt_pool_project_memory_mb"))),
            node_cpu_factor=max(1.0, min(2.0, float(setting("aedt_pool_node_cpu_factor")))),
            lease_ttl_seconds=max(30, int(setting("aedt_pool_lease_ttl_seconds"))),
            queued_stale_seconds=max(
                30, int(setting("aedt_pool_queued_stale_seconds"))
            ),
            offer_ack_seconds=max(
                15, int(setting("aedt_pool_offer_ack_seconds"))
            ),
            admission_deadline_seconds=max(
                60, int(setting("aedt_pool_admission_deadline_seconds"))
            ),
            session_heartbeat_timeout_seconds=max(
                30, int(setting("aedt_pool_session_heartbeat_timeout_seconds"))
            ),
            unhealthy_recycle_grace_seconds=max(
                0, int(setting("aedt_pool_unhealthy_recycle_grace_seconds"))
            ),
            session_start_timeout_seconds=max(
                60, int(setting("aedt_pool_session_start_timeout_seconds"))
            ),
            idle_ttl_seconds=max(0, int(setting("aedt_pool_idle_ttl_seconds"))),
            allocation_max_age_seconds=max(
                0, int(setting("aedt_pool_allocation_max_age_seconds"))
            ),
            scale_step_nodes=max(1, int(setting("aedt_pool_scale_step_nodes"))),
            required_capability=setting("aedt_pool_required_capability").strip(),
            env_profile=setting("aedt_pool_env_profile").strip(),
            account_name=setting("aedt_pool_account_name").strip(),
            control_plane_url=setting("aedt_pool_control_plane_url").strip().rstrip("/"),
            )
            self._config_cache = resolved
            self._config_cache_until = cache_now + 1.0
            return resolved

    def set_operator_limit(self, max_sessions: int) -> AedtPoolConfig:
        """Compatibility wrapper for the original one-field operator API."""
        return self.set_operator_limits(max_sessions=max_sessions)

    def set_operator_limits(
        self,
        *,
        max_sessions: int | None = None,
        min_idle_sessions: int | None = None,
        target_projects: int | None = None,
        projects_per_session: int | None = None,
    ) -> AedtPoolConfig:
        current = self.config()
        requested_max = current.max_sessions if max_sessions is None else max_sessions
        requested_min_idle = (
            min(current.min_idle_sessions, requested_max)
            if min_idle_sessions is None
            else min_idle_sessions
        )
        requested_slots = (
            current.projects_per_session
            if projects_per_session is None else projects_per_session
        )
        if type(requested_max) is not int or not 0 <= requested_max <= 550:
            raise ValueError("max_aedt_sessions must be an integer between 0 and 550")
        if type(requested_min_idle) is not int or not 0 <= requested_min_idle <= 550:
            raise ValueError("min_idle_aedt_sessions must be an integer between 0 and 550")
        if requested_min_idle > requested_max:
            raise ValueError("min_idle_aedt_sessions cannot exceed max_aedt_sessions")
        if type(requested_slots) is not int or not 1 <= requested_slots <= 3:
            raise ValueError(
                "projects_per_aedt must be an integer between 1 and 3; "
                "3 was operator-accepted on 2026-07-14 after the original "
                "validated 1:2 contract"
            )
        if target_projects is None:
            requested_target = requested_max * requested_slots
        else:
            requested_target = target_projects
        if type(requested_target) is not int or not 0 <= requested_target <= 1650:
            raise ValueError(
                "target_project_concurrency must be an integer between 0 and 1650"
            )
        physical_ceiling = requested_max * requested_slots
        if requested_target > physical_ceiling:
            raise ValueError(
                "target_project_concurrency cannot exceed "
                "max_aedt_sessions * projects_per_aedt"
            )
        with self._lock:
            if requested_slots != current.projects_per_session:
                with self.db.connect() as conn:
                    counted = int(
                        conn.execute(
                            "SELECT COUNT(*) FROM aedt_sessions "
                            "WHERE state IN ('starting','ready','busy','draining','unhealthy')"
                        ).fetchone()[0]
                    )
                if current.enabled or counted:
                    raise ValueError(
                        "disable and fully drain the pool before changing projects_per_aedt"
                    )
            with self.db.connect() as conn:
                for key, value in (
                    ("aedt_pool_max_sessions", requested_max),
                    ("aedt_pool_min_idle_sessions", requested_min_idle),
                    ("aedt_pool_target_projects", requested_target),
                    ("aedt_pool_projects_per_session", requested_slots),
                ):
                    conn.execute(
                        "INSERT INTO scheduler_settings(key, value, updated_at) "
                        "VALUES(?, ?, CURRENT_TIMESTAMP) "
                        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                        "updated_at = CURRENT_TIMESTAMP",
                        (key, str(value)),
                    )
        self._invalidate_config()
        return self.config()

    def set_operator_timeouts(
        self,
        *,
        lease_ttl_seconds: int | None = None,
        session_heartbeat_timeout_seconds: int | None = None,
        unhealthy_recycle_grace_seconds: int | None = None,
        idle_ttl_seconds: int | None = None,
        allocation_max_age_seconds: int | None = None,
    ) -> AedtPoolConfig:
        """Operator knob for liveness windows.

        Client/host heartbeats land as SQLite writes and serialize behind the
        scheduler tick's own write transactions; a heavy campaign tick can
        stall them for minutes.  The expiry windows must ride out the longest
        realistic tick, otherwise a busy scheduler kills its own pool.
        """
        current = self.config()
        requested_ttl = (
            current.lease_ttl_seconds
            if lease_ttl_seconds is None
            else lease_ttl_seconds
        )
        requested_session = (
            current.session_heartbeat_timeout_seconds
            if session_heartbeat_timeout_seconds is None
            else session_heartbeat_timeout_seconds
        )
        requested_recycle_grace = (
            current.unhealthy_recycle_grace_seconds
            if unhealthy_recycle_grace_seconds is None
            else unhealthy_recycle_grace_seconds
        )
        requested_idle_ttl = (
            current.idle_ttl_seconds
            if idle_ttl_seconds is None
            else idle_ttl_seconds
        )
        requested_allocation_max_age = (
            current.allocation_max_age_seconds
            if allocation_max_age_seconds is None
            else allocation_max_age_seconds
        )
        if type(requested_ttl) is not int or not 60 <= requested_ttl <= 3600:
            raise ValueError(
                "lease_ttl_seconds must be an integer between 60 and 3600"
            )
        if type(requested_session) is not int or not 60 <= requested_session <= 3600:
            raise ValueError(
                "session_heartbeat_timeout_seconds must be an integer "
                "between 60 and 3600"
            )
        if (
            type(requested_recycle_grace) is not int
            or not 0 <= requested_recycle_grace <= 3600
        ):
            raise ValueError(
                "unhealthy_recycle_grace_seconds must be an integer "
                "between 0 and 3600"
            )
        if (
            type(requested_idle_ttl) is not int
            or not 60 <= requested_idle_ttl <= 86400
        ):
            raise ValueError(
                "idle_ttl_seconds must be an integer between 60 and 86400"
            )
        if (
            type(requested_allocation_max_age) is not int
            or not 0 <= requested_allocation_max_age <= 172800
        ):
            raise ValueError(
                "allocation_max_age_seconds must be an integer between 0 and 172800"
            )
        with self.db.connect() as conn:
            for key, value in (
                ("aedt_pool_lease_ttl_seconds", requested_ttl),
                (
                    "aedt_pool_session_heartbeat_timeout_seconds",
                    requested_session,
                ),
                (
                    "aedt_pool_unhealthy_recycle_grace_seconds",
                    requested_recycle_grace,
                ),
                ("aedt_pool_idle_ttl_seconds", requested_idle_ttl),
                (
                    "aedt_pool_allocation_max_age_seconds",
                    requested_allocation_max_age,
                ),
            ):
                conn.execute(
                    "INSERT INTO scheduler_settings(key, value, updated_at) "
                    "VALUES(?, ?, CURRENT_TIMESTAMP) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                    "updated_at = CURRENT_TIMESTAMP",
                    (key, str(value)),
                )
        self._invalidate_config()
        return self.config()

    def set_adapter_ready(self, ready: bool) -> AedtPoolConfig:
        """Deployment hook, intentionally not exposed as an operator UI toggle."""
        if ready and not self.bootstrap_token:
            raise ValueError("a non-empty session-host bootstrap token is required")
        was_enabled = self.config().enabled
        self.db.set_setting("aedt_pool_adapter_ready", "1" if ready else "0")
        self._invalidate_config()
        if not ready and was_enabled:
            self._request_all_sessions_drain("session-host adapter disabled")
        return self.config()

    def set_control_plane_url(self, url: str) -> AedtPoolConfig:
        """Publish (or withdraw) the node-visible control-plane base URL.

        Relay supervision owns this deployment setting.  Keeping it durable
        lets the AEDT runtime resolve the current URL on every reconciliation
        pass instead of capturing a possibly stale address at process start.
        An empty value intentionally unpublishes the endpoint.
        """
        normalized = str(url or "").strip().rstrip("/")
        self.db.set_setting("aedt_pool_control_plane_url", normalized)
        self._invalidate_config()
        return self.config()

    def set_enabled(self, enabled: bool) -> AedtPoolConfig:
        current = self.config()
        if enabled and not current.validation_passed:
            raise ValueError("1-AEDT:2-project validation has not passed")
        if enabled and not current.adapter_ready:
            raise ValueError("AEDT session-host adapter is not ready")
        self.db.set_setting("aedt_pool_enabled", "1" if enabled else "0")
        self._invalidate_config()
        if not enabled:
            self._request_all_sessions_drain("operator disabled AEDT pool")
        return self.config()

    def _request_all_sessions_drain(self, reason: str) -> None:
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            # Starting is left claimable: a host may already have launched AEDT
            # between claim and register. register_session observes disabled
            # state and registers it directly as draining.
            self._request_sessions_drain(conn, reason, now)

    @staticmethod
    def _request_sessions_drain(
        conn: Any,
        reason: str,
        now: str,
        *,
        allocation_ids: list[int] | None = None,
    ) -> None:
        if allocation_ids == []:
            return
        allocation_filter = ""
        parameters: list[Any] = [
            reason,
            now,
            now,
            SESSION_HEARTBEAT_TIMEOUT_MESSAGE,
        ]
        if allocation_ids is not None:
            placeholders = ",".join("?" for _ in allocation_ids)
            allocation_filter = f" AND allocation_id IN ({placeholders})"
            parameters.extend(allocation_ids)
        conn.execute(
            f"""
            UPDATE aedt_sessions
            SET state = 'draining', failure_message = ?,
                drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
            WHERE (
                state IN ('ready','busy')
                OR (state = 'unhealthy' AND failure_message = ?
                    AND quarantine_reason = '')
            ){allocation_filter}
            """,
            parameters,
        )

    def record_validation(self, evidence: dict[str, Any]) -> dict[str, Any]:
        """Evaluate the mandatory 2x standalone vs 1x pooled A/B contract."""
        required_bools = (
            "output_parity_passed",
            "cancellation_isolation_passed",
            "crash_recovery_passed",
            "timeout_fault_injection_passed",
            "sibling_completion_passed",
            "sibling_terminal_output_passed",
            "sibling_data_rows_passed",
            "sibling_field_solution_passed",
            "fault_checkout_released_after_recycle_passed",
            "faulted_desktop_not_reused_passed",
        )
        baseline_desktops = int(evidence.get("baseline_desktops") or 0)
        pooled_desktops = int(evidence.get("pooled_desktops") or 0)
        baseline_projects = int(evidence.get("baseline_projects") or 0)
        pooled_projects = int(evidence.get("pooled_projects") or 0)
        runtime_ratio = float(evidence.get("runtime_ratio") or 0.0)
        desktop_license_delta = int(evidence.get("desktop_license_delta") or 0)
        checks = {
            key: type(evidence.get(key)) is bool and bool(evidence.get(key))
            for key in required_bools
        }
        mixed_isolation_passed = (
            type(evidence.get("mixed_mft_ipmsm_isolation_passed")) is bool
            and bool(evidence.get("mixed_mft_ipmsm_isolation_passed"))
        )
        failures: list[str] = []
        if (baseline_desktops, baseline_projects) != (2, 2):
            failures.append("baseline must be 2 Desktops running 2 projects")
        if (pooled_desktops, pooled_projects) != (1, 2):
            failures.append("pooled treatment must be 1 Desktop running 2 projects")
        if not 0 < runtime_ratio <= 1.20:
            failures.append("pooled runtime must be positive and no more than 1.20x baseline")
        if desktop_license_delta > -1:
            failures.append("pooled treatment must reduce Desktop checkout by at least one")
        for key, passed in checks.items():
            if not passed:
                failures.append(key)
        # Evidence paths/identifiers make an accidental UI click insufficient
        # to approve a production topology.
        if not str(evidence.get("baseline_artifact") or "").strip():
            failures.append("baseline_artifact")
        if not str(evidence.get("pooled_artifact") or "").strip():
            failures.append("pooled_artifact")
        if not str(evidence.get("license_artifact") or "").strip():
            failures.append("license_artifact")
        status = "passed" if not failures else "failed"
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO aedt_pool_validations (
                    status, baseline_desktops, pooled_desktops,
                    baseline_projects, pooled_projects, runtime_ratio,
                    desktop_license_delta, output_parity_passed,
                    solver_isolation_passed, cancellation_isolation_passed,
                    crash_recovery_passed, timeout_fault_injection_passed,
                    sibling_completion_passed,
                    sibling_terminal_output_passed,
                    sibling_data_rows_passed,
                    sibling_field_solution_passed,
                    fault_checkout_released_after_recycle_passed,
                    faulted_desktop_not_reused_passed,
                    mixed_mft_ipmsm_isolation_passed,
                    evidence_json, failure_message,
                    finished_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    status,
                    baseline_desktops,
                    pooled_desktops,
                    baseline_projects,
                    pooled_projects,
                    runtime_ratio,
                    desktop_license_delta,
                    int(checks["output_parity_passed"]),
                    # Retain the old column as a compatibility alias.  It is
                    # true only when the timeout fault test proved the sibling
                    # completed; it is not an architectural assumption.
                    int(checks["timeout_fault_injection_passed"] and checks["sibling_completion_passed"]),
                    int(checks["cancellation_isolation_passed"]),
                    int(checks["crash_recovery_passed"]),
                    int(checks["timeout_fault_injection_passed"]),
                    int(checks["sibling_completion_passed"]),
                    int(checks["sibling_terminal_output_passed"]),
                    int(checks["sibling_data_rows_passed"]),
                    int(checks["sibling_field_solution_passed"]),
                    int(checks["fault_checkout_released_after_recycle_passed"]),
                    int(checks["faulted_desktop_not_reused_passed"]),
                    int(mixed_isolation_passed),
                    json.dumps(evidence, ensure_ascii=False, separators=(",", ":")),
                    "; ".join(failures),
                    now,
                    now,
                ),
            )
            validation_id = int(cursor.lastrowid)
        self._invalidate_config()
        return self.get_validation(validation_id)

    def get_validation(self, validation_id: int) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM aedt_pool_validations WHERE id = ?", (int(validation_id),)
            ).fetchone()
            if not row:
                raise KeyError(validation_id)
            item = dict(row)
            item["evidence"] = json.loads(item.pop("evidence_json") or "{}")
            return item

    def create_exact_session_reservation(
        self,
        *,
        reservation_key: str,
        session_id: int,
        session_generation: int,
        session_profile: Any,
        task_ids: list[int],
        ttl_seconds: int = 1800,
    ) -> dict[str, Any]:
        """Atomically reserve exact slots for one operator-authorized task cohort.

        The bootstrap-authenticated reservation is the placement authority.  A
        lease client's requested_session_id is only an assertion against this
        server-side record and can never create an exact-session pin itself.
        """

        normalized_key = str(reservation_key or "").strip()
        if not normalized_key:
            raise ValueError("reservation_key is required")
        if type(session_id) is not int or session_id <= 0:
            raise ValueError("session_id must be a positive integer")
        if type(session_generation) is not int or session_generation <= 0:
            raise ValueError("session_generation must be a positive integer")
        if type(ttl_seconds) is not int or not 60 <= ttl_seconds <= 3600:
            raise ValueError("ttl_seconds must be an integer between 60 and 3600")
        if not isinstance(task_ids, list) or not task_ids:
            raise ValueError("task_ids must be a non-empty list")
        if any(type(task_id) is not int or task_id <= 0 for task_id in task_ids):
            raise ValueError("task_ids must contain positive integers")
        normalized_task_ids = sorted(set(task_ids))
        if len(normalized_task_ids) != len(task_ids):
            raise ValueError("task_ids must be unique")
        normalized_profile = canonical_expected_session_profile(session_profile)

        now_dt = self._now()
        now = _sql_time(now_dt)
        expires = _sql_time(now_dt + timedelta(seconds=ttl_seconds))
        target_projects = int(self.config().target_projects)
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._refresh_exact_session_reservations(conn, now)

            existing_rows = conn.execute(
                """
                SELECT * FROM aedt_exact_session_reservations
                WHERE reservation_key = ? ORDER BY task_id
                """,
                (normalized_key,),
            ).fetchall()
            if existing_rows:
                existing_task_ids = [int(row["task_id"]) for row in existing_rows]
                identical = bool(
                    existing_task_ids == normalized_task_ids
                    and all(int(row["session_id"]) == session_id for row in existing_rows)
                    and all(
                        int(row["session_generation"]) == session_generation
                        for row in existing_rows
                    )
                    and all(
                        str(row["session_profile"]) == normalized_profile
                        for row in existing_rows
                    )
                )
                if not identical:
                    raise ValueError(
                        "reservation_key already exists with different immutable payload"
                    )
                return self._exact_session_reservation_from_rows(existing_rows)

            admitted_projects = self._admitted_project_count(conn)
            if (
                target_projects <= 0
                or admitted_projects + len(normalized_task_ids) > target_projects
            ):
                raise ValueError(
                    "exact-session reservation exceeds target project concurrency"
                )

            session = conn.execute(
                """
                SELECT s.*, a.state AS allocation_state
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.id = ?
                """,
                (session_id,),
            ).fetchone()
            if not session:
                raise ValueError("requested AEDT session does not exist")
            if str(session["state"]) not in SESSION_ASSIGNABLE_STATES:
                raise ValueError("requested AEDT session is not assignable")
            if str(session["allocation_state"]) not in {"warm", "active"}:
                raise ValueError("requested AEDT session allocation is not active")
            if int(session["generation"] or 0) != session_generation:
                raise ValueError("requested AEDT session generation does not match")
            if str(session["session_profile"] or "") != normalized_profile:
                raise ValueError("requested AEDT session profile does not match")
            if session["solve_batch_sealed_at"] or session["drain_requested_at"]:
                raise ValueError("requested AEDT session is sealed or draining")
            mixed_canary = conn.execute(
                """
                SELECT 1 FROM aedt_mixed_canary_admissions
                WHERE session_id = ? AND state IN ('open','filled','aborting')
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if mixed_canary:
                raise ValueError("requested AEDT session is reserved for a mixed canary")

            placeholders = ",".join("?" for _ in normalized_task_ids)
            tasks = conn.execute(
                f"SELECT id, status, aedt_backend FROM tasks WHERE id IN ({placeholders})",
                tuple(normalized_task_ids),
            ).fetchall()
            if len(tasks) != len(normalized_task_ids):
                raise ValueError("one or more reservation task_ids do not exist")
            for task in tasks:
                if str(task["status"]) in TASK_TERMINAL_STATES:
                    raise ValueError("exact-session reservation task is terminal")
                if str(task["aedt_backend"] or "").strip().lower() != "pooled":
                    raise ValueError("exact-session reservation task must use pooled AEDT")
            live_task_reservation = conn.execute(
                f"""
                SELECT task_id FROM aedt_exact_session_reservations
                WHERE task_id IN ({placeholders})
                  AND state IN ('reserved','claimed','consumed')
                LIMIT 1
                """,
                tuple(normalized_task_ids),
            ).fetchone()
            if live_task_reservation:
                raise ValueError("task already has a live exact-session reservation")
            live_task_lease = conn.execute(
                f"""
                SELECT task_id FROM aedt_project_leases
                WHERE task_id IN ({placeholders})
                  AND state IN (
                      'queued','offered','leased','attaching','active','releasing'
                  )
                LIMIT 1
                """,
                tuple(normalized_task_ids),
            ).fetchone()
            if live_task_lease:
                raise ValueError("task already has a live AEDT project lease")

            occupied = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM aedt_project_leases
                    WHERE session_id = ?
                      AND state IN ('offered','leased','attaching','active','releasing')
                    """,
                    (session_id,),
                ).fetchone()["count"]
                or 0
            )
            held = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS count FROM aedt_exact_session_reservations
                    WHERE session_id = ? AND state IN ('reserved','claimed')
                    """,
                    (session_id,),
                ).fetchone()["count"]
                or 0
            )
            free_slots = int(session["slots_total"] or 0) - occupied - held
            if len(normalized_task_ids) > free_slots:
                raise ValueError(
                    "requested AEDT session does not have enough unreserved free slots"
                )

            conn.executemany(
                """
                INSERT INTO aedt_exact_session_reservations (
                    reservation_key, task_id, session_id, session_generation,
                    session_profile, state, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'reserved', ?, ?, ?)
                """,
                (
                    (
                        normalized_key,
                        task_id,
                        session_id,
                        session_generation,
                        normalized_profile,
                        expires,
                        now,
                        now,
                    )
                    for task_id in normalized_task_ids
                ),
            )
        return self.get_exact_session_reservation(normalized_key)

    @staticmethod
    def _auto_reservation_fits_session(
        *,
        workload_family: str,
        isolation_policy: str,
        session_profile: str,
        occupants: list[Any],
        reservations: list[Any],
    ) -> bool:
        """Apply the protocol-v2 sharing contract before launching a worker.

        Operator-created reservations predate family metadata.  They remain
        authoritative, but an automatic admission must not guess that an
        unknown reserved peer is compatible with it.
        """

        if isolation_policy == "exclusive" and (occupants or reservations):
            return False
        for occupant in occupants:
            if int(occupant["protocol_version"] or 1) < 2:
                return False
            if str(occupant["session_profile"] or "") != session_profile:
                return False
            occupant_policy = str(occupant["isolation_policy"] or "family")
            if bool(occupant["exclusive_session"]) or occupant_policy == "exclusive":
                return False
            if isolation_policy == "family":
                if (
                    occupant_policy != "family"
                    or str(occupant["workload_family"] or "") != workload_family
                ):
                    return False
            elif occupant_policy != "shared_if_compatible":
                return False
        for reservation in reservations:
            reserved_family = str(reservation["workload_family"] or "")
            reserved_policy = str(reservation["isolation_policy"] or "")
            if not reserved_family or not reserved_policy:
                return False
            if reserved_policy == "exclusive":
                return False
            if isolation_policy == "family":
                if (
                    reserved_policy != "family"
                    or reserved_family != workload_family
                ):
                    return False
            elif reserved_policy != "shared_if_compatible":
                return False
        return True

    @staticmethod
    def _admitted_project_count(
        conn: Any,
        *,
        exclude_task_id: int = 0,
        exclude_reservation_id: int = 0,
        exclude_lease_id: int = 0,
    ) -> int:
        """Count atomic slot admissions, de-duplicating task reservations."""

        return int(
            conn.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT CASE
                               WHEN l.task_id > 0 THEN 'task:' || l.task_id
                               ELSE 'lease:' || l.id
                           END AS admission_key
                    FROM aedt_project_leases l
                    WHERE l.state IN (
                        'offered','leased','attaching','active','releasing'
                    )
                      AND (? = 0 OR l.id != ?)
                      AND (? = 0 OR l.task_id != ?)
                    UNION
                    SELECT 'task:' || r.task_id AS admission_key
                    FROM aedt_exact_session_reservations r
                    WHERE r.state IN ('reserved','claimed')
                      AND (? = 0 OR r.id != ?)
                      AND (? = 0 OR r.task_id != ?)
                )
                """,
                (
                    int(exclude_lease_id),
                    int(exclude_lease_id),
                    int(exclude_task_id),
                    int(exclude_task_id),
                    int(exclude_reservation_id),
                    int(exclude_reservation_id),
                    int(exclude_task_id),
                    int(exclude_task_id),
                ),
            ).fetchone()[0]
        )

    def prepare_pooled_task_session(
        self,
        *,
        task_id: int,
        session_profile: Any,
        workload_family: str,
        isolation_policy: str,
        eligible_allocation_ids: list[int] | None = None,
        ttl_seconds: int = 1800,
    ) -> dict[str, Any] | None:
        """Reserve one healthy Desktop slot before a pooled task can launch.

        The reservation and the task's allocation/node pin are committed in
        the same SQLite writer transaction.  A caller that receives ``None``
        must leave the task queued; it may never launch a generic thin client
        and wait for a Desktop after the fact.
        """

        if type(task_id) is not int or task_id <= 0:
            raise ValueError("task_id must be a positive integer")
        if type(ttl_seconds) is not int or not 60 <= ttl_seconds <= 3600:
            raise ValueError("ttl_seconds must be an integer between 60 and 3600")
        normalized_profile = canonical_expected_session_profile(session_profile)
        normalized_family = str(workload_family or "").strip().lower()
        if not normalized_family:
            raise ValueError("workload_family is required for pooled pre-admission")
        normalized_policy = str(isolation_policy or "family").strip().lower()
        if normalized_policy not in {"family", "shared_if_compatible", "exclusive"}:
            raise ValueError("invalid pooled isolation_policy")
        allowed_allocations = sorted(
            {
                int(allocation_id)
                for allocation_id in (eligible_allocation_ids or [])
                if int(allocation_id) > 0
            }
        )
        if eligible_allocation_ids is not None and not allowed_allocations:
            return None

        config = self.config()
        if not config.operational:
            return None
        if normalized_policy == "shared_if_compatible":
            mixed_validation = self.latest_validation()
            if not (
                mixed_validation
                and mixed_validation.get("status") == "passed"
                and bool(
                    mixed_validation.get("mixed_mft_ipmsm_isolation_passed")
                )
            ):
                # Otherwise the worker would launch with an exact reservation
                # and request_lease would require an incompatible canary slot.
                return None
        now_dt = self._now()
        now = _sql_time(now_dt)
        heartbeat_cutoff = _sql_time(
            now_dt - timedelta(seconds=config.session_heartbeat_timeout_seconds)
        )
        expires = _sql_time(now_dt + timedelta(seconds=ttl_seconds))
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._refresh_exact_session_reservations(conn, now)
            task = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if not task or str(task["status"]) != TaskStatus.QUEUED.value:
                return None
            if str(task["aedt_backend"] or "").strip().lower() != "pooled":
                raise ValueError("pooled pre-admission requires a pooled task")
            if config.target_projects <= 0 or self._admitted_project_count(
                conn, exclude_task_id=task_id
            ) >= int(config.target_projects):
                return None
            requested_account_value = (
                task["requested_account_name"]
                if "requested_account_name" in task.keys()
                and task["requested_account_name"] is not None
                else task["account_name"]
            )
            requested_accounts = {
                part.strip()
                for part in re.split(
                    r"[\s,;/|]+", str(requested_account_value or "")
                )
                if part.strip()
            }

            latest = conn.execute(
                """
                SELECT r.*, s.allocation_id, s.node_name,
                       s.state AS session_state, s.generation AS live_generation,
                       s.session_profile AS live_profile,
                       s.last_heartbeat_at, s.reuse_blocked_at,
                       s.solve_batch_sealed_at, s.drain_requested_at,
                       a.state AS allocation_state,
                       a.account_name AS allocation_account_name
                FROM aedt_exact_session_reservations r
                LEFT JOIN aedt_sessions s ON s.id = r.session_id
                LEFT JOIN allocations a ON a.id = s.allocation_id
                WHERE r.task_id = ?
                ORDER BY r.id DESC LIMIT 1
                """,
                (task_id,),
            ).fetchone()
            if latest and str(latest["state"]) in {"reserved", "claimed"}:
                metadata_matches = bool(
                    not str(latest["workload_family"] or "")
                    or (
                        str(latest["workload_family"]) == normalized_family
                        and str(latest["isolation_policy"] or "")
                        == normalized_policy
                    )
                )
                target_is_usable = bool(
                    str(latest["session_state"] or "") in SESSION_ASSIGNABLE_STATES
                    and str(latest["allocation_state"] or "") in {"warm", "active"}
                    and int(latest["live_generation"] or 0)
                    == int(latest["session_generation"] or 0)
                    and str(latest["live_profile"] or "")
                    == str(latest["session_profile"] or "")
                    and str(latest["last_heartbeat_at"] or "") >= heartbeat_cutoff
                    and not latest["reuse_blocked_at"]
                    and not latest["solve_batch_sealed_at"]
                    and not latest["drain_requested_at"]
                    and (
                        not allowed_allocations
                        or int(latest["allocation_id"] or 0) in allowed_allocations
                    )
                    and (
                        not requested_accounts
                        or str(latest["allocation_account_name"] or "")
                        in requested_accounts
                    )
                    and metadata_matches
                )
                if target_is_usable:
                    conn.execute(
                        """
                        UPDATE tasks
                        SET requested_allocation_id = ?, node_name = ?, cpus = ?,
                            updated_at = ?
                        WHERE id = ? AND status = 'queued'
                        """,
                        (
                            int(latest["allocation_id"]),
                            str(latest["node_name"] or ""),
                            config.project_cpus,
                            now,
                            task_id,
                        ),
                    )
                    return dict(latest)
            if latest and not str(latest["reservation_key"] or "").startswith(
                "aedt-auto:"
            ):
                # A bootstrap/operator pin is fail-closed.  Automatic placement
                # must never silently replace it with a different Desktop.
                return None
            if latest and str(latest["state"]) in {"reserved", "claimed"}:
                self._fail_exact_session_reservation_cohort(
                    conn,
                    reservation_key=str(latest["reservation_key"]),
                    now=now,
                    failure_message="automatic AEDT reservation target became unavailable",
                )

            allocation_predicate = ""
            params: list[Any] = [heartbeat_cutoff, now, normalized_profile]
            if allowed_allocations:
                placeholders = ",".join("?" for _ in allowed_allocations)
                allocation_predicate = f" AND s.allocation_id IN ({placeholders})"
                params.extend(allowed_allocations)
            account_predicate = ""
            if requested_accounts:
                placeholders = ",".join("?" for _ in requested_accounts)
                account_predicate = f" AND a.account_name IN ({placeholders})"
                params.extend(sorted(requested_accounts))
            candidates = conn.execute(
                f"""
                SELECT s.*,
                       (SELECT COUNT(*) FROM aedt_project_leases l
                        WHERE l.session_id = s.id
                          AND l.state IN ('offered','leased','attaching','active','releasing'))
                           AS used_slots,
                       (SELECT COUNT(*) FROM aedt_exact_session_reservations r
                        WHERE r.session_id = s.id
                          AND r.state IN ('reserved','claimed')) AS held_slots
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state IN ('ready','busy')
                  AND a.state IN ('warm','active')
                  AND s.last_heartbeat_at >= ?
                  AND (s.quarantine_until IS NULL OR s.quarantine_until <= ?)
                  AND s.session_profile = ?
                  AND TRIM(COALESCE(s.endpoint, '')) != ''
                  AND TRIM(COALESCE(s.process_id, '')) != ''
                  AND s.reuse_blocked_at IS NULL
                  AND s.solve_batch_sealed_at IS NULL
                  AND s.drain_requested_at IS NULL
                  {allocation_predicate}
                  {account_predicate}
                ORDER BY (used_slots + held_slots) DESC,
                         COALESCE(s.idle_since, s.created_at) ASC, s.id ASC
                """,
                tuple(params),
            ).fetchall()
            selected = None
            for session in candidates:
                if int(session["used_slots"] or 0) + int(
                    session["held_slots"] or 0
                ) >= int(session["slots_total"] or 0):
                    continue
                occupants = conn.execute(
                    """
                    SELECT * FROM aedt_project_leases
                    WHERE session_id = ?
                      AND state IN ('offered','leased','attaching','active','releasing')
                    ORDER BY id
                    """,
                    (int(session["id"]),),
                ).fetchall()
                reservations = conn.execute(
                    """
                    SELECT * FROM aedt_exact_session_reservations
                    WHERE session_id = ? AND state IN ('reserved','claimed')
                    ORDER BY id
                    """,
                    (int(session["id"]),),
                ).fetchall()
                if self._auto_reservation_fits_session(
                    workload_family=normalized_family,
                    isolation_policy=normalized_policy,
                    session_profile=normalized_profile,
                    occupants=list(occupants),
                    reservations=list(reservations),
                ):
                    selected = session
                    break
            if selected is None:
                conn.execute(
                    """
                    UPDATE tasks SET requested_allocation_id = 0, node_name = '',
                        updated_at = ?
                    WHERE id = ? AND status = 'queued'
                    """,
                    (now, task_id),
                )
                return None

            # Every automatic slot reserved against one still-unsealed Desktop
            # generation belongs to the same admission cohort.  A per-task key
            # made a terminal/expired sibling invisible to leases that had
            # already become ACTIVE: the remaining lease could later seal an
            # underfilled batch as if that sibling had never existed.  Reuse a
            # shared key (and its first authoritative expiry) until the session
            # seals.  Once all leases release, the session is unsealed and all
            # old reservation rows are terminal, so the next wave receives a
            # fresh key.
            auto_cohort = conn.execute(
                """
                SELECT reservation_key, MIN(expires_at) AS expires_at
                FROM aedt_exact_session_reservations
                WHERE session_id = ? AND session_generation = ?
                  AND reservation_key LIKE 'aedt-auto:%'
                  AND state IN ('reserved','claimed','consumed')
                GROUP BY reservation_key
                ORDER BY MIN(id) ASC
                LIMIT 1
                """,
                (int(selected["id"]), int(selected["generation"])),
            ).fetchone()
            if auto_cohort:
                reservation_key = str(auto_cohort["reservation_key"])
                expires = str(auto_cohort["expires_at"])
            else:
                reservation_key = (
                    f"aedt-auto:{int(selected['id'])}:"
                    f"{int(selected['generation'])}:{secrets.token_hex(8)}"
                )
            cursor = conn.execute(
                """
                INSERT INTO aedt_exact_session_reservations (
                    reservation_key, task_id, session_id, session_generation,
                    session_profile, workload_family, isolation_policy,
                    state, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?, ?)
                """,
                (
                    reservation_key,
                    task_id,
                    int(selected["id"]),
                    int(selected["generation"]),
                    normalized_profile,
                    normalized_family,
                    normalized_policy,
                    expires,
                    now,
                    now,
                ),
            )
            reservation_id = int(cursor.lastrowid)
            pinned = conn.execute(
                """
                UPDATE tasks
                SET requested_allocation_id = ?, node_name = ?, cpus = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'queued'
                """,
                (
                    int(selected["allocation_id"]),
                    str(selected["node_name"] or ""),
                    config.project_cpus,
                    now,
                    task_id,
                ),
            )
            if pinned.rowcount != 1:
                raise RuntimeError("pooled task changed while reserving its AEDT slot")
            return {
                "id": reservation_id,
                "reservation_key": reservation_key,
                "task_id": task_id,
                "session_id": int(selected["id"]),
                "session_generation": int(selected["generation"]),
                "session_profile": normalized_profile,
                "workload_family": normalized_family,
                "isolation_policy": normalized_policy,
                "allocation_id": int(selected["allocation_id"]),
                "node_name": str(selected["node_name"] or ""),
                "state": "reserved",
                "expires_at": expires,
            }

    @staticmethod
    def _exact_session_reservation_from_rows(rows: list[Any]) -> dict[str, Any]:
        first = rows[0]
        return {
            "reservation_key": str(first["reservation_key"]),
            "session_id": int(first["session_id"]),
            "session_generation": int(first["session_generation"]),
            "session_profile": str(first["session_profile"]),
            "expires_at": str(first["expires_at"]),
            "slots": [dict(row) for row in rows],
        }

    def get_exact_session_reservation(self, reservation_key: str) -> dict[str, Any]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM aedt_exact_session_reservations
                WHERE reservation_key = ? ORDER BY task_id
                """,
                (str(reservation_key or "").strip(),),
            ).fetchall()
            if not rows:
                raise KeyError(reservation_key)
            return self._exact_session_reservation_from_rows(rows)

    @staticmethod
    def _fail_exact_session_reservation_cohort(
        conn: Any,
        *,
        reservation_key: str,
        now: str,
        failure_message: str,
        settle_project_owners: bool = False,
    ) -> None:
        """Fail one exact cohort without permitting an unpinned fallback."""

        normalized_reason = str(failure_message or "").strip() or (
            "exact-session reservation target became unavailable"
        )
        if settle_project_owners:
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = 'releasing', failure_message = ?,
                    release_requested_at = COALESCE(release_requested_at, ?),
                    updated_at = ?
                WHERE exact_session_reservation_id IN (
                    SELECT id FROM aedt_exact_session_reservations
                    WHERE reservation_key = ?
                )
                  AND state IN ('attaching','active','releasing')
                """,
                (normalized_reason, now, now, reservation_key),
            )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                    updated_at = ?
                WHERE id IN (
                    SELECT DISTINCT l.session_id
                    FROM aedt_project_leases l
                    JOIN aedt_exact_session_reservations r
                      ON r.id = l.exact_session_reservation_id
                    WHERE r.reservation_key = ?
                      AND l.state = 'releasing'
                      AND l.session_id IS NOT NULL
                )
                  AND state NOT IN ('closed','failed')
                """,
                (now, now, reservation_key),
            )
            terminal_states = ("queued", "offered", "leased")
        else:
            terminal_states = (
                "queued",
                "offered",
                "leased",
                "attaching",
                "active",
                "releasing",
            )
        placeholders = ",".join("?" for _ in terminal_states)
        conn.execute(
            f"""
            UPDATE aedt_project_leases
            SET state = 'failed', failure_message = ?, finished_at = ?, updated_at = ?
            WHERE exact_session_reservation_id IN (
                SELECT id FROM aedt_exact_session_reservations
                WHERE reservation_key = ?
            )
              AND state IN ({placeholders})
            """,
            (normalized_reason, now, now, reservation_key, *terminal_states),
        )
        conn.execute(
            """
            UPDATE aedt_exact_session_reservations
            SET state = 'failed', failure_message = ?, finished_at = ?, updated_at = ?
            WHERE reservation_key = ?
              AND state IN ('reserved','claimed','consumed')
            """,
            (normalized_reason, now, now, reservation_key),
        )

    @staticmethod
    def _expire_exact_session_reservation_cohort(
        conn: Any,
        *,
        reservation_key: str,
        now: str,
    ) -> None:
        """Expire every unsealed member at the cohort's shared deadline."""

        reason = "exact-session reservation cohort expired before solve permit"
        conn.execute(
            """
            UPDATE aedt_project_leases
            SET state = 'releasing', failure_message = ?,
                release_requested_at = COALESCE(release_requested_at, ?),
                updated_at = ?
            WHERE exact_session_reservation_id IN (
                SELECT id FROM aedt_exact_session_reservations
                WHERE reservation_key = ?
            )
              AND state IN ('attaching','active','releasing')
              AND TRIM(COALESCE(solve_permit_at, '')) = ''
            """,
            (reason, now, now, reservation_key),
        )
        conn.execute(
            """
            UPDATE aedt_sessions
            SET reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                updated_at = ?
            WHERE id IN (
                SELECT DISTINCT l.session_id
                FROM aedt_project_leases l
                JOIN aedt_exact_session_reservations r
                  ON r.id = l.exact_session_reservation_id
                WHERE r.reservation_key = ?
                  AND l.state = 'releasing'
                  AND l.session_id IS NOT NULL
            )
              AND state NOT IN ('closed','failed')
            """,
            (now, now, reservation_key),
        )
        conn.execute(
            """
            UPDATE aedt_project_leases
            SET state = 'expired', failure_message = ?, finished_at = ?, updated_at = ?
            WHERE exact_session_reservation_id IN (
                SELECT id FROM aedt_exact_session_reservations
                WHERE reservation_key = ?
            )
              AND state IN ('queued','offered','leased')
              AND TRIM(COALESCE(solve_permit_at, '')) = ''
            """,
            (reason, now, now, reservation_key),
        )
        conn.execute(
            """
            UPDATE aedt_exact_session_reservations
            SET state = 'expired', failure_message = ?, finished_at = ?, updated_at = ?
            WHERE reservation_key = ?
              AND state IN ('reserved','claimed','consumed')
            """,
            (reason, now, now, reservation_key),
        )

    @staticmethod
    def _fail_exact_session_reservations_for_target(
        conn: Any,
        *,
        session_id: int,
        now: str,
        failure_message: str,
    ) -> None:
        rows = conn.execute(
            """
            SELECT DISTINCT r.reservation_key
            FROM aedt_exact_session_reservations r
            LEFT JOIN aedt_project_leases l ON l.id = r.lease_id
            WHERE r.session_id = ?
              AND (
                  r.state IN ('reserved','claimed','consumed')
                  OR l.state IN (
                      'queued','offered','leased','attaching','active','releasing'
                  )
              )
            ORDER BY r.reservation_key
            """,
            (int(session_id),),
        ).fetchall()
        for row in rows:
            AedtPoolService._fail_exact_session_reservation_cohort(
                conn,
                reservation_key=str(row["reservation_key"]),
                now=now,
                failure_message=failure_message,
            )

    @staticmethod
    def _refresh_exact_session_reservations(conn: Any, now: str) -> None:
        """Release capacity after task/lease terminal state and expire admissions."""

        terminal_siblings = conn.execute(
            """
            SELECT r.reservation_key, r.task_id, t.status
            FROM aedt_exact_session_reservations r
            JOIN tasks t ON t.id = r.task_id
            LEFT JOIN aedt_sessions s ON s.id = r.session_id
            WHERE r.state IN ('reserved','claimed','consumed')
              AND t.status IN ('completed','failed','cancelled')
              AND (s.id IS NULL OR s.solve_batch_sealed_at IS NULL)
              AND EXISTS (
                  SELECT 1
                  FROM aedt_exact_session_reservations sibling
                  WHERE sibling.reservation_key = r.reservation_key
                    AND sibling.id != r.id
                    AND sibling.state IN ('reserved','claimed','consumed')
              )
            ORDER BY r.id
            """
        ).fetchall()
        failed_keys: set[str] = set()
        for row in terminal_siblings:
            reservation_key = str(row["reservation_key"])
            if reservation_key in failed_keys:
                continue
            failed_keys.add(reservation_key)
            AedtPoolService._fail_exact_session_reservation_cohort(
                conn,
                reservation_key=reservation_key,
                now=now,
                failure_message=(
                    "exact-session reservation sibling task "
                    f"{int(row['task_id'])} became {str(row['status'])} "
                    "before solve permit"
                ),
                settle_project_owners=True,
            )

        expired_cohorts = conn.execute(
            """
            SELECT DISTINCT r.reservation_key
            FROM aedt_exact_session_reservations r
            LEFT JOIN aedt_sessions s ON s.id = r.session_id
            WHERE r.state IN ('reserved','claimed','consumed')
              AND r.expires_at <= ?
              AND (s.id IS NULL OR s.solve_batch_sealed_at IS NULL)
            ORDER BY r.reservation_key
            """,
            (now,),
        ).fetchall()
        for row in expired_cohorts:
            AedtPoolService._expire_exact_session_reservation_cohort(
                conn,
                reservation_key=str(row["reservation_key"]),
                now=now,
            )

        conn.execute(
            """
            UPDATE aedt_exact_session_reservations
            SET state = 'released', finished_at = ?, updated_at = ?
            WHERE state IN ('reserved','claimed','consumed')
              AND EXISTS (
                  SELECT 1 FROM tasks t
                  WHERE t.id = aedt_exact_session_reservations.task_id
                    AND t.status IN ('completed','failed','cancelled')
              )
            """,
            (now, now),
        )
        conn.execute(
            """
            UPDATE aedt_exact_session_reservations
            SET state = 'released', finished_at = ?, updated_at = ?
            WHERE state IN ('claimed','consumed')
              AND lease_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM aedt_project_leases l
                  WHERE l.id = aedt_exact_session_reservations.lease_id
                    AND l.state IN ('released','failed','cancelled','expired')
              )
            """,
            (now, now),
        )
        invalid_targets = conn.execute(
            """
            SELECT DISTINCT r.reservation_key
            FROM aedt_exact_session_reservations r
            LEFT JOIN aedt_sessions s ON s.id = r.session_id
            LEFT JOIN allocations a ON a.id = s.allocation_id
            WHERE r.state IN ('reserved','claimed','consumed')
              AND (
                  s.id IS NULL
                  OR s.generation != r.session_generation
                  OR s.session_profile != r.session_profile
                  OR s.state NOT IN ('ready','busy')
                  OR s.drain_requested_at IS NOT NULL
                  OR (
                      a.state NOT IN ('warm','active')
                      AND NOT (
                          -- A parent allocation can be marked draining by a
                          -- concurrent scheduler reconciliation even though
                          -- its Desktop and already-attached projects remain
                          -- alive.  Allocation state gates new placement; it
                          -- must not revoke an ACTIVE owner that is waiting
                          -- for the next serialized solve generation.
                          s.solve_batch_sealed_at IS NOT NULL
                          AND r.state = 'consumed'
                          AND TRIM(COALESCE(s.process_id, '')) != ''
                          AND TRIM(COALESCE(s.host_id, '')) != ''
                          AND TRIM(COALESCE(s.host_token_hash, '')) != ''
                          AND EXISTS (
                              SELECT 1
                              FROM aedt_project_leases active_owner
                              WHERE active_owner.id = r.lease_id
                                AND active_owner.exact_session_reservation_id = r.id
                                AND active_owner.session_id = r.session_id
                                AND active_owner.state = 'active'
                          )
                      )
                  )
                  OR (
                      s.solve_batch_sealed_at IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM aedt_exact_session_reservations sealed_pending
                          LEFT JOIN aedt_project_leases sealed_lease
                            ON sealed_lease.id = sealed_pending.lease_id
                          WHERE sealed_pending.reservation_key = r.reservation_key
                            AND sealed_pending.state IN (
                                'reserved','claimed','consumed'
                            )
                            AND (
                                sealed_pending.state IN ('reserved','claimed')
                                OR sealed_lease.id IS NULL
                                OR (
                                    TRIM(COALESCE(
                                        sealed_lease.solve_permit_at, ''
                                    )) = ''
                                    AND sealed_lease.state != 'active'
                                )
                            )
                      )
                  )
              )
              AND EXISTS (
                  SELECT 1
                  FROM aedt_exact_session_reservations pending
                  LEFT JOIN aedt_project_leases pending_lease
                    ON pending_lease.id = pending.lease_id
                  WHERE pending.reservation_key = r.reservation_key
                    AND pending.state IN ('reserved','claimed','consumed')
                    AND (
                        pending.state IN ('reserved','claimed')
                        OR pending_lease.id IS NULL
                        OR TRIM(COALESCE(pending_lease.solve_permit_at, '')) = ''
                    )
              )
            """
        ).fetchall()
        for row in invalid_targets:
            AedtPoolService._fail_exact_session_reservation_cohort(
                conn,
                reservation_key=str(row["reservation_key"]),
                now=now,
                failure_message=(
                    "exact-session reservation target became unavailable "
                    "before solve permit"
                ),
            )
    @staticmethod
    def _authorize_exact_session_reservation(
        conn: Any,
        *,
        task_id: int,
        requested_session_id: int,
        session_profile: str,
        workload_family: str,
        isolation_policy: str,
        request_key: str,
        token_hash: str,
        now: str,
        heartbeat_cutoff: str,
    ) -> dict[str, Any] | None:
        assertion = int(requested_session_id or 0)
        if task_id <= 0:
            if assertion:
                raise ValueError(
                    "requested_session_id requires a bootstrap-issued task reservation"
                )
            return None
        reservation = conn.execute(
            """
            SELECT * FROM aedt_exact_session_reservations
            WHERE task_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (task_id,),
        ).fetchone()
        if not reservation:
            if assertion:
                raise ValueError(
                    "requested_session_id has no bootstrap-issued task reservation"
                )
            return None
        if assertion and assertion != int(reservation["session_id"]):
            raise ValueError("requested_session_id does not match task reservation")
        if str(reservation["session_profile"]) != session_profile:
            raise ValueError("lease session profile does not match task reservation")
        if str(reservation["workload_family"] or "") and (
            str(reservation["workload_family"]) != workload_family
            or str(reservation["isolation_policy"] or "") != isolation_policy
        ):
            raise ValueError("lease workload isolation does not match task reservation")
        existing_lease_id = int(reservation["lease_id"] or 0)
        if existing_lease_id:
            existing = conn.execute(
                """
                SELECT request_key, client_token_hash
                FROM aedt_project_leases WHERE id = ?
                """,
                (existing_lease_id,),
            ).fetchone()
            replay = bool(
                existing
                and str(existing["request_key"]) == request_key
                and secrets.compare_digest(str(existing["client_token_hash"]), token_hash)
            )
            if replay:
                return dict(reservation)
            if str(reservation["state"]) in {"claimed", "consumed"}:
                raise ValueError("exact-session task reservation is already claimed")
        if str(reservation["state"]) != "reserved":
            failure_message = str(reservation["failure_message"] or "").strip()
            raise ValueError(
                failure_message
                or (
                    "task exact-session reservation is no longer active; "
                    "unpinned fallback is forbidden"
                )
            )
        if str(reservation["expires_at"]) <= now:
            raise ValueError("exact-session reservation expired")
        session = conn.execute(
            """
            SELECT s.*, a.state AS allocation_state
            FROM aedt_sessions s
            LEFT JOIN allocations a ON a.id = s.allocation_id
            WHERE s.id = ?
            """,
            (int(reservation["session_id"]),),
        ).fetchone()
        if not session or str(session["state"]) not in SESSION_ASSIGNABLE_STATES:
            raise ValueError("reserved AEDT session is not assignable")
        if int(session["generation"] or 0) != int(reservation["session_generation"]):
            raise ValueError("reserved AEDT session generation drifted")
        if str(session["session_profile"] or "") != str(reservation["session_profile"]):
            raise ValueError("reserved AEDT session profile drifted")
        if session["solve_batch_sealed_at"] or session["drain_requested_at"]:
            raise ValueError("reserved AEDT session is sealed or draining")
        if str(session["allocation_state"] or "") not in {"warm", "active"}:
            raise ValueError("reserved AEDT session allocation is unavailable")
        if str(session["last_heartbeat_at"] or "") < heartbeat_cutoff:
            raise ValueError("reserved AEDT session heartbeat is stale")
        return dict(reservation)

    def create_mixed_canary_admission(
        self,
        *,
        session_id: int,
        mft_projects: int = 2,
        ipmsm_projects: int = 1,
        ttl_seconds: int = 1800,
    ) -> dict[str, Any]:
        """Reserve one empty 3-slot session for one operator-authorized mixed canary.

        The returned dedupe keys are capabilities bound to scheduler task rows.
        Lease clients receive no bootstrap credential and cannot create another
        authorized task with an already-live dedupe key.
        """

        for value, name in (
            (session_id, "session_id"),
            (mft_projects, "mft_projects"),
            (ipmsm_projects, "ipmsm_projects"),
            (ttl_seconds, "ttl_seconds"),
        ):
            if type(value) is not int:
                raise ValueError(f"{name} must be an integer")
        if session_id <= 0:
            raise ValueError("session_id must be positive")
        if mft_projects < 1 or ipmsm_projects < 1:
            raise ValueError("mixed canary requires at least one MFT and one IPMSM project")
        if mft_projects + ipmsm_projects != 3:
            raise ValueError("mixed canary must reserve exactly three projects")
        if not 60 <= ttl_seconds <= 3600:
            raise ValueError("ttl_seconds must be between 60 and 3600")
        latest = self.latest_validation()
        if latest and bool(latest.get("mixed_mft_ipmsm_isolation_passed")):
            raise ValueError("mixed MFT/IPMSM isolation has already passed validation")

        now_dt = self._now()
        now = _sql_time(now_dt)
        expires = _sql_time(now_dt + timedelta(seconds=ttl_seconds))
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._refresh_mixed_canary_admissions(conn, now)
            session = conn.execute(
                "SELECT * FROM aedt_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if not session:
                raise ValueError("mixed canary session does not exist")
            if str(session["state"]) != "ready":
                raise ValueError("mixed canary session must be ready")
            if int(session["slots_total"] or 0) != 3:
                raise ValueError("mixed canary session must have exactly three slots")
            if session["solve_batch_sealed_at"]:
                raise ValueError("mixed canary session solve batch is already sealed")
            if session["drain_requested_at"]:
                raise ValueError("mixed canary session is draining")
            session_profile = canonical_expected_session_profile(
                str(session["session_profile"] or "")
            )
            occupant = conn.execute(
                """
                SELECT 1 FROM aedt_project_leases
                WHERE session_id = ?
                  AND state IN ('offered','leased','attaching','active','releasing')
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if occupant:
                raise ValueError("mixed canary session must be empty")
            active = conn.execute(
                """
                SELECT 1 FROM aedt_mixed_canary_admissions
                WHERE session_id = ? AND state IN ('open','filled')
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if active:
                raise ValueError("mixed canary session already has an active admission")
            exact_reservation = conn.execute(
                """
                SELECT 1 FROM aedt_exact_session_reservations
                WHERE session_id = ? AND state IN ('reserved','claimed')
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            if exact_reservation:
                raise ValueError("mixed canary session has exact-session reservations")
            placement_group = f"mixed-canary-{session_id}-{secrets.token_hex(8)}"
            cursor = conn.execute(
                """
                INSERT INTO aedt_mixed_canary_admissions (
                    session_id, placement_group, session_profile,
                    expected_mft_projects, expected_ipmsm_projects,
                    state, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?)
                """,
                (
                    session_id,
                    placement_group,
                    session_profile,
                    mft_projects,
                    ipmsm_projects,
                    expires,
                    now,
                    now,
                ),
            )
            admission_id = int(cursor.lastrowid)
            slots: list[tuple[str, str, str]] = []
            for family, namespace, count in (
                ("mft", "mft", mft_projects),
                ("ipmsm", "pyaedt_motor", ipmsm_projects),
            ):
                for index in range(count):
                    dedupe_key = (
                        f"aedt-mixed-canary:{admission_id}:{family}:{index}:"
                        f"{secrets.token_urlsafe(18)}"
                    )
                    slots.append((dedupe_key, family, namespace))
            conn.executemany(
                """
                INSERT INTO aedt_mixed_canary_slots (
                    admission_id, dedupe_key, workload_family,
                    project_namespace, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (admission_id, dedupe_key, family, namespace, now, now)
                    for dedupe_key, family, namespace in slots
                ),
            )
        return self.get_mixed_canary_admission(admission_id)

    def get_mixed_canary_admission(self, admission_id: int) -> dict[str, Any]:
        with self.db.connect() as conn:
            admission = conn.execute(
                "SELECT * FROM aedt_mixed_canary_admissions WHERE id = ?",
                (int(admission_id),),
            ).fetchone()
            if not admission:
                raise KeyError(admission_id)
            item = dict(admission)
            item["slots"] = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT id, dedupe_key, workload_family, project_namespace,
                           lease_id, admitted_at
                    FROM aedt_mixed_canary_slots
                    WHERE admission_id = ? ORDER BY id
                    """,
                    (int(admission_id),),
                ).fetchall()
            ]
            return item

    def _refresh_mixed_canary_admissions(self, conn: Any, now: str) -> None:
        """Abort an incomplete one-shot canary without stranding its session.

        The admission TTL bounds the time to assemble and activate the exact
        three-project batch; it is not a solve-runtime deadline.  Once a solve
        permit exists, normal sibling completion semantics own the session.
        Before that point, an expired admission or a consumed slot that can no
        longer participate makes the exact 2+1 experiment impossible.

        Project-owning leases use the normal two-phase host release.  Queued
        and merely offered leases can be cancelled immediately.  The session
        remains reserved while any release is outstanding and becomes reusable
        after the host confirms that every attached project is closed.
        """

        abort_candidates = conn.execute(
            """
            SELECT ca.id, ca.session_id
            FROM aedt_mixed_canary_admissions ca
            WHERE ca.state IN ('open','filled')
              AND NOT EXISTS (
                  SELECT 1
                  FROM aedt_mixed_canary_slots started_slot
                  JOIN aedt_project_leases started_lease
                    ON started_lease.id = started_slot.lease_id
                  WHERE started_slot.admission_id = ca.id
                    AND TRIM(COALESCE(started_lease.solve_permit_at, '')) != ''
              )
              AND (
                  ca.expires_at <= ?
                  OR EXISTS (
                      SELECT 1
                      FROM aedt_mixed_canary_slots broken_slot
                      JOIN aedt_project_leases broken_lease
                        ON broken_lease.id = broken_slot.lease_id
                      WHERE broken_slot.admission_id = ca.id
                        AND broken_lease.state IN (
                            'releasing','released','failed','cancelled','expired'
                        )
                  )
              )
            ORDER BY ca.id ASC
            """,
            (now,),
        ).fetchall()
        abort_reason = "mixed canary admission aborted before exact batch activation"
        for candidate in abort_candidates:
            admission_id = int(candidate["id"])
            session_id = int(candidate["session_id"])
            conn.execute(
                """
                UPDATE aedt_mixed_canary_admissions
                SET state = 'aborting', updated_at = ?
                WHERE id = ? AND state IN ('open','filled')
                """,
                (now, admission_id),
            )
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = 'cancelled', session_id = NULL, slot_index = NULL,
                    failure_message = ?, finished_at = ?, updated_at = ?
                WHERE id IN (
                    SELECT lease_id FROM aedt_mixed_canary_slots
                    WHERE admission_id = ? AND lease_id IS NOT NULL
                )
                  AND state IN ('queued','offered')
                """,
                (abort_reason, now, now, admission_id),
            )
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = 'releasing', failure_message = ?,
                    release_requested_at = COALESCE(release_requested_at, ?),
                    updated_at = ?
                WHERE id IN (
                    SELECT lease_id FROM aedt_mixed_canary_slots
                    WHERE admission_id = ? AND lease_id IS NOT NULL
                )
                  AND state IN ('leased','attaching','active')
                """,
                (abort_reason, now, now, admission_id),
            )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                    updated_at = ?
                WHERE id = ? AND state NOT IN ('closed','failed')
                """,
                (now, now, session_id),
            )
            self._refresh_session_state(conn, session_id, now)

        conn.execute(
            """
            UPDATE aedt_mixed_canary_admissions
            SET state = 'aborted', finished_at = ?, updated_at = ?
            WHERE state = 'aborting'
              AND NOT EXISTS (
                  SELECT 1
                  FROM aedt_mixed_canary_slots cs
                  JOIN aedt_project_leases l ON l.id = cs.lease_id
                  WHERE cs.admission_id = aedt_mixed_canary_admissions.id
                    AND l.state IN (
                        'queued','offered','leased','attaching','active','releasing'
                    )
              )
            """,
            (now, now),
        )
        conn.execute(
            """
            UPDATE aedt_mixed_canary_admissions
            SET state = 'closed', finished_at = ?, updated_at = ?
            WHERE state = 'filled'
              AND NOT EXISTS (
                  SELECT 1
                  FROM aedt_mixed_canary_slots cs
                  JOIN aedt_project_leases l ON l.id = cs.lease_id
                  WHERE cs.admission_id = aedt_mixed_canary_admissions.id
                    AND l.state IN (
                        'queued','offered','leased','attaching','active','releasing'
                    )
              )
            """,
            (now, now),
        )

    @staticmethod
    def _mixed_canary_batch_is_complete(
        conn: Any,
        session_id: int,
    ) -> bool:
        """Return false while a reserved mixed session lacks its exact batch."""

        admission = conn.execute(
            """
            SELECT ca.id, ca.state,
                   ca.expected_mft_projects + ca.expected_ipmsm_projects
                       AS expected_projects,
                   COUNT(cs.id) AS reserved_projects,
                   SUM(
                       CASE
                           WHEN l.session_id = ca.session_id
                            AND l.state = 'active'
                           THEN 1 ELSE 0
                       END
                   ) AS active_projects
            FROM aedt_mixed_canary_admissions ca
            LEFT JOIN aedt_mixed_canary_slots cs ON cs.admission_id = ca.id
            LEFT JOIN aedt_project_leases l ON l.id = cs.lease_id
            WHERE ca.session_id = ?
              AND ca.state IN ('open','filled','aborting')
            GROUP BY ca.id
            ORDER BY ca.id DESC
            LIMIT 1
            """,
            (int(session_id),),
        ).fetchone()
        if not admission:
            return True
        expected = int(admission["expected_projects"] or 0)
        return bool(
            str(admission["state"]) == "filled"
            and expected == 3
            and int(admission["reserved_projects"] or 0) == expected
            and int(admission["active_projects"] or 0) == expected
        )

    @staticmethod
    def _exact_session_reservations_are_active(
        conn: Any,
        session_id: int,
    ) -> bool:
        """Prevent underfilled sealing while exact reserved peers are pending."""

        rows = conn.execute(
            """
            SELECT r.state, r.session_id AS reserved_session_id,
                   l.session_id AS lease_session_id, l.state AS lease_state
            FROM aedt_exact_session_reservations r
            LEFT JOIN aedt_project_leases l ON l.id = r.lease_id
            WHERE r.session_id = ?
              AND r.state IN ('reserved','claimed','consumed')
            ORDER BY r.id
            """,
            (int(session_id),),
        ).fetchall()
        if not rows:
            return True
        return all(
            str(row["state"]) == "consumed"
            and int(row["lease_session_id"] or 0)
            == int(row["reserved_session_id"])
            and str(row["lease_state"] or "") == "active"
            for row in rows
        )

    @staticmethod
    def _authorize_mixed_canary_lease(
        conn: Any,
        *,
        task_id: int,
        request_key: str,
        token_hash: str,
        workload_family: str,
        project_namespace: str,
        session_profile: str,
        protocol_version: int,
        now: str,
    ) -> dict[str, Any]:
        if task_id <= 0:
            raise ValueError(
                "shared_if_compatible requires passed mixed MFT/IPMSM isolation "
                "validation or a bootstrap-issued canary task"
            )
        task = conn.execute(
            "SELECT id, dedupe_key, status, aedt_backend FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        if not task or str(task["status"]) in {
            TaskStatus.COMPLETED.value,
            TaskStatus.FAILED.value,
            TaskStatus.CANCELLED.value,
        }:
            raise ValueError("mixed canary task is missing or terminal")
        if str(task["aedt_backend"] or "") != "pooled":
            raise ValueError("mixed canary task must declare aedt_backend=pooled")
        if protocol_version != 2:
            raise ValueError("mixed canary task requires protocol_version=2")
        slot = conn.execute(
            """
            SELECT cs.*, ca.session_id AS canary_session_id,
                   ca.placement_group, ca.session_profile AS canary_session_profile,
                   ca.state AS admission_state, ca.expires_at AS admission_expires_at
            FROM aedt_mixed_canary_slots cs
            JOIN aedt_mixed_canary_admissions ca ON ca.id = cs.admission_id
            WHERE cs.dedupe_key = ?
            """,
            (str(task["dedupe_key"] or ""),),
        ).fetchone()
        if not slot:
            raise ValueError(
                "shared_if_compatible requires passed mixed MFT/IPMSM isolation "
                "validation or a bootstrap-issued canary task"
            )
        if str(slot["admission_state"]) not in {"open", "filled"}:
            raise ValueError("mixed canary admission is no longer active")
        if str(slot["workload_family"]) != workload_family:
            raise ValueError("mixed canary workload_family does not match its reserved slot")
        if str(slot["project_namespace"]) != project_namespace:
            raise ValueError("mixed canary project_namespace does not match its reserved slot")
        if str(slot["canary_session_profile"]) != session_profile:
            raise ValueError("mixed canary session profile does not match its reservation")
        session = conn.execute(
            "SELECT * FROM aedt_sessions WHERE id = ?",
            (int(slot["canary_session_id"]),),
        ).fetchone()
        if not session or str(session["state"]) not in {"ready", "busy"}:
            raise ValueError("mixed canary session is not ready")
        if int(session["slots_total"] or 0) != 3:
            raise ValueError("mixed canary session no longer has exactly three slots")
        if session["solve_batch_sealed_at"] or session["drain_requested_at"]:
            raise ValueError("mixed canary session is sealed or draining")
        if str(session["session_profile"] or "") != session_profile:
            raise ValueError("mixed canary host session profile drifted")
        existing_lease_id = int(slot["lease_id"] or 0)
        if existing_lease_id:
            existing = conn.execute(
                "SELECT request_key, client_token_hash FROM aedt_project_leases WHERE id = ?",
                (existing_lease_id,),
            ).fetchone()
            if not (
                existing
                and str(existing["request_key"]) == request_key
                and secrets.compare_digest(
                    str(existing["client_token_hash"]), token_hash
                )
            ):
                raise ValueError("mixed canary task slot has already been consumed")
        elif str(slot["admission_expires_at"]) <= now:
            raise ValueError("mixed canary admission expired")
        return dict(slot)

    def request_lease(
        self,
        *,
        request_key: str,
        project_name: str,
        placement_group: str | None = None,
        workload_family: str = "",
        session_profile: Any = "",
        project_namespace: str = "",
        isolation_policy: str = "family",
        workspace_path: str = "",
        protocol_version: int = 1,
        client_deadline_at: str = "",
        admission_timeout_seconds: int | None = None,
        client_token: str = "",
        task_id: int = 0,
        allocation_id: int = 0,
        node_name: str = "",
        requested_session_id: int = 0,
        exclusive_session: bool = False,
    ) -> tuple[dict[str, Any], str]:
        request_key = request_key.strip()
        project_name = project_name.strip()
        if not request_key:
            raise ValueError("request_key is required")
        if not project_name:
            raise ValueError("project_name is required")
        if placement_group is None:
            resolved_placement_group = _derive_placement_group(project_name)
        else:
            if not isinstance(placement_group, str):
                raise ValueError("placement_group must be a string")
            resolved_placement_group = placement_group.strip()
            if not resolved_placement_group:
                raise ValueError("placement_group must not be empty")
        if type(exclusive_session) is not bool:
            raise ValueError("exclusive_session must be a boolean")
        if type(requested_session_id) is not int or requested_session_id < 0:
            raise ValueError("requested_session_id must be a non-negative integer")
        if type(protocol_version) is not int or protocol_version not in {1, 2}:
            raise ValueError("protocol_version must be 1 or 2")
        resolved_family = _normalized_family(workload_family, project_name)
        resolved_profile = _canonical_session_profile(session_profile)
        normalized_namespace = str(project_namespace or "").strip()
        normalized_workspace = str(workspace_path or "").strip()
        if protocol_version >= 2:
            if not str(workload_family or "").strip():
                raise ValueError("protocol-v2 workload_family is required")
            if not normalized_workspace:
                raise ValueError("protocol-v2 workspace_path is required")
            resolved_profile = canonical_expected_session_profile(session_profile)
        normalized_isolation = str(isolation_policy or "family").strip().lower()
        if normalized_isolation not in {"family", "shared_if_compatible", "exclusive"}:
            raise ValueError(
                "isolation_policy must be family, shared_if_compatible, or exclusive"
            )
        if normalized_isolation == "exclusive":
            exclusive_session = True
        mixed_canary_required = False
        if normalized_isolation == "shared_if_compatible":
            mixed_validation = self.latest_validation()
            if not (
                mixed_validation
                and mixed_validation.get("status") == "passed"
                and bool(
                    mixed_validation.get("mixed_mft_ipmsm_isolation_passed")
                )
            ):
                mixed_canary_required = True
        token = str(client_token or "").strip() or secrets.token_urlsafe(32)
        if len(token) < 16:
            raise ValueError("client_token must contain at least 16 characters")
        config = self.config()
        now_dt = self._now()
        now = _sql_time(now_dt)
        expires = _sql_time(now_dt + timedelta(seconds=config.lease_ttl_seconds))
        explicit_deadline = str(client_deadline_at or "").strip()
        if explicit_deadline and admission_timeout_seconds is not None:
            raise ValueError(
                "provide client_deadline_at or admission_timeout_seconds, not both"
            )
        if admission_timeout_seconds is None:
            admission_timeout = config.admission_deadline_seconds
        else:
            if (
                type(admission_timeout_seconds) is not int
                or not 30 <= admission_timeout_seconds <= 3600
            ):
                raise ValueError(
                    "admission_timeout_seconds must be an integer between 30 and 3600"
                )
            admission_timeout = admission_timeout_seconds
        deadline = (
            _parse_utc_time(explicit_deadline)
            if explicit_deadline
            else now_dt + timedelta(seconds=admission_timeout)
        )
        if deadline <= now_dt:
            raise ValueError("client_deadline_at must be in the future")
        requested_task_id = max(0, int(task_id))
        resolved_allocation_id = max(0, int(allocation_id))
        resolved_node_name = node_name.strip()
        if not resolved_allocation_id:
            # node_name is commonly reported as worker provenance.  A node-only
            # value must never constrain placement in the central AEDT pool.
            resolved_node_name = ""
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._refresh_mixed_canary_admissions(conn, now)
            self._refresh_exact_session_reservations(conn, now)
            # task_id is provenance.  Central-pool requests remain unpinned
            # unless the caller explicitly supplies allocation_id.  When it
            # does, derive and verify the affinity from the scheduler task.
            if requested_task_id and resolved_allocation_id:
                task = conn.execute(
                    "SELECT allocation_id FROM tasks WHERE id = ?",
                    (requested_task_id,),
                ).fetchone()
                # Public callers are required to provide a real task by the API.
                # Direct service calls without a matching task are retained for
                # trusted/internal compatibility (including old control scripts).
                if task:
                    task_allocation_id = int(task["allocation_id"] or 0)
                    if not task_allocation_id:
                        raise ValueError("task has no active allocation for affinity")
                    else:
                        if (
                            resolved_allocation_id
                            and resolved_allocation_id != task_allocation_id
                        ):
                            raise ValueError(
                                "requested allocation does not belong to task_id"
                            )
                        task_allocation = conn.execute(
                            "SELECT node_name FROM allocations WHERE id = ?",
                            (task_allocation_id,),
                        ).fetchone()
                        task_node_name = (
                            str(task_allocation["node_name"] or "")
                            if task_allocation
                            else ""
                        )
                        if (
                            resolved_node_name
                            and _short_node_name(resolved_node_name)
                            != _short_node_name(task_node_name)
                        ):
                            raise ValueError("requested node does not belong to task_id")
                        resolved_allocation_id = task_allocation_id
                        resolved_node_name = task_node_name
            token_hash = _token_hash(token)
            exact_reservation = self._authorize_exact_session_reservation(
                conn,
                task_id=requested_task_id,
                requested_session_id=requested_session_id,
                session_profile=resolved_profile,
                workload_family=resolved_family,
                isolation_policy=normalized_isolation,
                request_key=request_key,
                token_hash=token_hash,
                now=now,
                heartbeat_cutoff=_sql_time(
                    now_dt
                    - timedelta(seconds=config.session_heartbeat_timeout_seconds)
                ),
            )
            exact_reservation_id = int(
                (exact_reservation or {}).get("id") or 0
            )
            exact_session_id = int(
                (exact_reservation or {}).get("session_id") or 0
            )
            exact_session_generation = int(
                (exact_reservation or {}).get("session_generation") or 0
            )
            mixed_canary: dict[str, Any] | None = None
            mixed_canary_admission_id = 0
            mixed_canary_session_id = 0
            if mixed_canary_required:
                mixed_canary = self._authorize_mixed_canary_lease(
                    conn,
                    task_id=requested_task_id,
                    request_key=request_key,
                    token_hash=token_hash,
                    workload_family=resolved_family,
                    project_namespace=normalized_namespace,
                    session_profile=resolved_profile,
                    protocol_version=protocol_version,
                    now=now,
                )
                mixed_canary_admission_id = int(mixed_canary["admission_id"])
                mixed_canary_session_id = int(mixed_canary["canary_session_id"])
                # The operator reservation, not the untrusted lease payload,
                # owns both the placement group and exact host session.
                resolved_placement_group = str(mixed_canary["placement_group"])
                if exact_reservation is not None:
                    raise ValueError(
                        "exact-session reservation cannot be combined with mixed canary admission"
                    )
            existing = conn.execute(
                """
                SELECT l.*, t.status AS scheduler_task_status
                FROM aedt_project_leases l
                LEFT JOIN tasks t ON t.id = l.task_id
                WHERE l.request_key = ?
                """,
                (request_key,),
            ).fetchone()
            lease_id = 0
            if existing and secrets.compare_digest(
                str(existing["client_token_hash"]), token_hash
            ):
                # A create response can be lost.  The original token makes the
                # retry exactly idempotent, including if the lease has since
                # reached a terminal state.
                lease_id = int(existing["id"])
            elif existing:
                existing_state = str(existing["state"])
                task_terminal = str(existing["scheduler_task_status"] or "") in {
                    TaskStatus.COMPLETED.value,
                    TaskStatus.FAILED.value,
                    TaskStatus.CANCELLED.value,
                }
                queued_abandoned = existing_state == "queued" and (
                    str(existing["expires_at"] or "") <= now
                    or str(existing["client_deadline_at"] or "") <= now
                    or task_terminal
                )
                offered_abandoned = existing_state == "offered" and (
                    str(existing["offer_expires_at"] or "") <= now or task_terminal
                )
                replaceable = (
                    existing_state in LEASE_TERMINAL_STATES
                    or queued_abandoned
                    or offered_abandoned
                )
                if not replaceable:
                    raise ValueError(
                        "request_key is owned by a live lease; reuse the original "
                        "client token or wait for terminal cleanup"
                    )
                old_lease_id = int(existing["id"])
                old_session_id = int(existing["session_id"] or 0)
                archived_key = f"{request_key}#superseded:{old_lease_id}"
                if existing_state in LEASE_TERMINAL_STATES:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET request_key = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (archived_key, now, old_lease_id),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET request_key = ?, state = 'expired', session_id = NULL,
                            slot_index = NULL,
                            failure_message = 'durable request was abandoned and replayed',
                            finished_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (archived_key, now, now, old_lease_id),
                    )
                    if old_session_id:
                        self._refresh_session_state(conn, old_session_id, now)

            if not lease_id:
                duplicate_project = conn.execute(
                    """
                    SELECT id FROM aedt_project_leases
                    WHERE project_name = ?
                      AND state IN (
                          'queued','offered','leased','attaching','active','releasing'
                      )
                    LIMIT 1
                    """,
                    (project_name,),
                ).fetchone()
                if duplicate_project and protocol_version >= 2:
                    raise ValueError("project_name is already owned by a live lease")
                cursor = conn.execute(
                    """
                    INSERT INTO aedt_project_leases (
                        request_key, project_name, placement_group,
                        workload_family, session_profile, project_namespace,
                        isolation_policy, workspace_path, protocol_version, task_id,
                        requested_allocation_id, requested_node_name,
                        requested_session_id, requested_session_generation,
                        exact_session_reservation_id,
                        mixed_canary_admission_id, mixed_canary_session_id,
                        exclusive_session, state, client_token_hash,
                        last_heartbeat_at, expires_at, client_deadline_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              ?, ?, ?, 'queued', ?, ?, ?, ?)
                    """,
                    (
                        request_key,
                        project_name,
                        resolved_placement_group,
                        resolved_family,
                        resolved_profile,
                        normalized_namespace,
                        normalized_isolation,
                        normalized_workspace,
                        protocol_version,
                        requested_task_id,
                        resolved_allocation_id,
                        resolved_node_name,
                        exact_session_id,
                        exact_session_generation,
                        exact_reservation_id,
                        mixed_canary_admission_id,
                        mixed_canary_session_id,
                        int(exclusive_session),
                        token_hash,
                        now,
                        expires,
                        _sql_time(deadline),
                    ),
                )
                lease_id = int(cursor.lastrowid)
                if exact_reservation is not None:
                    claimed = conn.execute(
                        """
                        UPDATE aedt_exact_session_reservations
                        SET state = 'claimed', lease_id = ?, claimed_at = ?, updated_at = ?
                        WHERE id = ? AND state = 'reserved' AND lease_id IS NULL
                        """,
                        (lease_id, now, now, exact_reservation_id),
                    )
                    if claimed.rowcount != 1:
                        raise ValueError(
                            "exact-session task reservation was consumed concurrently"
                        )
                if mixed_canary is not None:
                    claimed = conn.execute(
                        """
                        UPDATE aedt_mixed_canary_slots
                        SET lease_id = ?, admitted_at = ?, updated_at = ?
                        WHERE id = ? AND lease_id IS NULL
                        """,
                        (lease_id, now, now, int(mixed_canary["id"])),
                    )
                    if claimed.rowcount != 1:
                        raise ValueError("mixed canary task slot was consumed concurrently")
                    remaining = conn.execute(
                        """
                        SELECT COUNT(*) AS count FROM aedt_mixed_canary_slots
                        WHERE admission_id = ? AND lease_id IS NULL
                        """,
                        (mixed_canary_admission_id,),
                    ).fetchone()
                    if int(remaining["count"] or 0) == 0:
                        conn.execute(
                            """
                            UPDATE aedt_mixed_canary_admissions
                            SET state = 'filled', filled_at = ?, updated_at = ?
                            WHERE id = ? AND state = 'open'
                            """,
                            (now, now, mixed_canary_admission_id),
                        )
            if config.operational:
                self._place_queued_leases(
                    conn,
                    now,
                    lease_ids=(lease_id,),
                    config=config,
                    refresh_reservations=False,
                )
        return self.get_lease(lease_id), token

    def _authorize_lease(self, lease_id: int, token: str) -> dict[str, Any]:
        lease = self.get_lease(lease_id)
        if not secrets.compare_digest(str(lease["client_token_hash"]), _token_hash(token)):
            raise PermissionError("invalid lease token")
        return lease

    def get_lease(self, lease_id: int, *, include_secret_hash: bool = True) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT l.*, s.session_key, s.endpoint, s.node_name AS session_node_name,
                       s.allocation_id AS session_allocation_id,
                       s.process_id AS session_process_id,
                       s.artifact_dir AS session_artifact_dir,
                       s.slots_total AS session_slots_total,
                       s.generation AS session_generation,
                       s.solve_batch_sealed_at,
                       (SELECT COUNT(*) FROM aedt_project_leases live
                        WHERE live.session_id = l.session_id
                          AND live.state IN (
                              'offered','leased','attaching','active','releasing'
                          )) AS session_live_lease_count,
                       (SELECT COUNT(*) FROM aedt_project_leases active
                        WHERE active.session_id = l.session_id
                          AND active.state = 'active') AS session_active_lease_count
                FROM aedt_project_leases l
                LEFT JOIN aedt_sessions s ON s.id = l.session_id
                WHERE l.id = ?
                """,
                (int(lease_id),),
            ).fetchone()
            if not row:
                raise KeyError(lease_id)
            item = dict(row)
            item["automation_lock_path"] = automation_lock_path(
                str(item.get("session_artifact_dir") or "")
            )
            item["expected_aedt_version"] = (
                EXPECTED_AEDT_VERSION
                if str(item.get("session_profile") or "")
                else ""
            )
            item["solve_permit_granted"] = bool(
                str(item.get("solve_permit_at") or "")
            )
            item["solve_permit_required"] = bool(
                int(item.get("protocol_version") or 1) >= 2
                and int(item.get("session_id") or 0)
                and str(item.get("state") or "") == "active"
                and not item["solve_permit_granted"]
            )
            item["exact_session_reservation_key"] = ""
            item["exact_session_reservation_state"] = ""
            item["exact_session_reservation_expires_at"] = ""
            item["exact_session_reservation_failure_message"] = ""
            item["exact_session_cohort_state"] = ""
            item["exact_session_cohort_deadline_at"] = ""
            item["exact_session_cohort_expected_count"] = 0
            item["exact_session_cohort_active_count"] = 0
            item["exact_session_cohort_pending_count"] = 0
            item["exact_session_cohort_failure_message"] = ""
            reservation_id = int(
                item.get("exact_session_reservation_id") or 0
            )
            if reservation_id:
                reservation = conn.execute(
                    """
                    SELECT * FROM aedt_exact_session_reservations
                    WHERE id = ?
                    """,
                    (reservation_id,),
                ).fetchone()
                if reservation:
                    reservation_key = str(reservation["reservation_key"])
                    item["exact_session_reservation_key"] = reservation_key
                    item["exact_session_reservation_state"] = str(
                        reservation["state"] or ""
                    )
                    item["exact_session_reservation_expires_at"] = str(
                        reservation["expires_at"] or ""
                    )
                    item["exact_session_reservation_failure_message"] = str(
                        reservation["failure_message"] or ""
                    )
                    cohort = conn.execute(
                        """
                        SELECT r.state, r.expires_at, r.failure_message,
                               l.state AS lease_state,
                               l.solve_permit_at AS lease_solve_permit_at
                        FROM aedt_exact_session_reservations r
                        LEFT JOIN aedt_project_leases l ON l.id = r.lease_id
                        WHERE r.reservation_key = ?
                        ORDER BY r.id
                        """,
                        (reservation_key,),
                    ).fetchall()
                    active_count = sum(
                        1
                        for row in cohort
                        if str(row["state"] or "") == "consumed"
                        and str(row["lease_state"] or "") == "active"
                    )
                    pending_count = sum(
                        1
                        for row in cohort
                        if str(row["state"] or "")
                        in {"reserved", "claimed", "consumed"}
                        and not (
                            str(row["state"] or "") == "consumed"
                            and str(row["lease_state"] or "") == "active"
                        )
                    )
                    cohort_states = {
                        str(row["state"] or "") for row in cohort
                    }
                    terminal_state = next(
                        (
                            state
                            for state in ("failed", "expired")
                            if state in cohort_states
                        ),
                        "",
                    )
                    if terminal_state:
                        cohort_state = terminal_state
                    elif (
                        "released" in cohort_states
                        and not str(item.get("solve_batch_sealed_at") or "")
                        and any(
                            state in cohort_states
                            for state in {"reserved", "claimed", "consumed"}
                        )
                    ):
                        cohort_state = "broken"
                    elif str(item.get("solve_batch_sealed_at") or ""):
                        cohort_state = "sealed"
                    elif pending_count:
                        cohort_state = "waiting"
                    elif active_count:
                        cohort_state = "active"
                    else:
                        cohort_state = "released"
                    deadlines = [
                        str(row["expires_at"] or "")
                        for row in cohort
                        if str(row["state"] or "")
                        in {"reserved", "claimed", "consumed"}
                        and str(row["expires_at"] or "")
                    ]
                    failure_messages = [
                        str(row["failure_message"] or "").strip()
                        for row in cohort
                        if str(row["failure_message"] or "").strip()
                    ]
                    item["exact_session_cohort_state"] = cohort_state
                    item["exact_session_cohort_deadline_at"] = (
                        min(deadlines) if deadlines else ""
                    )
                    item["exact_session_cohort_expected_count"] = len(cohort)
                    item["exact_session_cohort_active_count"] = active_count
                    item["exact_session_cohort_pending_count"] = pending_count
                    item["exact_session_cohort_failure_message"] = (
                        failure_messages[0] if failure_messages else ""
                    )
            session_id = int(item.get("session_id") or 0)
            solve_generation = int(item.get("solve_permit_generation") or 0)
            native_completed = bool(
                session_id > 0
                and solve_generation > 0
                and str(item.get("native_pipeline_completed_at") or "")
                and int(item.get("native_pipeline_session_id") or 0) == session_id
                and int(item.get("native_pipeline_generation") or 0)
                == solve_generation
            )
            expected_count = 0
            completed_count = 0
            broken_count = 0
            if session_id > 0 and solve_generation > 0:
                cohort = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS expected_count,
                        SUM(CASE
                            WHEN native_pipeline_completed_at IS NOT NULL
                             AND native_pipeline_session_id = ?
                             AND native_pipeline_generation = ?
                            THEN 1 ELSE 0 END
                        ) AS completed_count,
                        SUM(CASE
                            WHEN NOT (
                                native_pipeline_completed_at IS NOT NULL
                                AND native_pipeline_session_id = ?
                                AND native_pipeline_generation = ?
                            ) AND state != 'active'
                            THEN 1 ELSE 0 END
                        ) AS broken_count
                    FROM aedt_project_leases
                    WHERE session_id = ? AND solve_permit_generation = ?
                      AND solve_permit_at IS NOT NULL
                    """,
                    (
                        session_id,
                        solve_generation,
                        session_id,
                        solve_generation,
                        session_id,
                        solve_generation,
                    ),
                ).fetchone()
                expected_count = int(cohort["expected_count"] or 0)
                completed_count = int(cohort["completed_count"] or 0)
                broken_count = int(cohort["broken_count"] or 0)
            barrier_granted = bool(
                native_completed
                and expected_count > 0
                and completed_count == expected_count
            )
            item["native_pipeline_completed"] = native_completed
            item["native_pipeline_expected_count"] = expected_count
            item["native_pipeline_completed_count"] = completed_count
            item["native_pipeline_barrier_granted"] = barrier_granted
            item["native_pipeline_barrier_broken"] = bool(
                not barrier_granted and broken_count > 0
            )
            if not include_secret_hash:
                item.pop("client_token_hash", None)
            return item

    def lease_status(self, lease_id: int, token: str) -> dict[str, Any]:
        self._authorize_lease(lease_id, token)
        item = self.get_lease(lease_id, include_secret_hash=False)
        item["legacy_state"] = (
            "leased" if item.get("state") in {"offered", "attaching"} else item.get("state")
        )
        return item

    def heartbeat_lease(
        self, lease_id: int, token: str, *, phase: str = ""
    ) -> dict[str, Any]:
        normalized_phase = str(phase or "").strip().lower()
        if normalized_phase and normalized_phase not in {
            "waiting",
            "attaching",
            "solving",
            "postprocess",
            "releasing",
        }:
            raise ValueError("invalid lease heartbeat phase")
        config = self.config()
        now = self._now()
        now_sql = _sql_time(now)
        token_hash = _token_hash(token)
        # Most pooled clients heartbeat far more often than the durable lease
        # timeout requires.  Authenticate and validate from a read snapshot,
        # then coalesce unchanged accepted-lease heartbeats so a large ramp
        # does not turn every poll into a serialized SQLite writer.
        with self.db.connect() as conn:
            candidate_row = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
        if not candidate_row:
            raise KeyError(lease_id)
        candidate = dict(candidate_row)
        if not secrets.compare_digest(
            str(candidate["client_token_hash"]), token_hash
        ):
            raise PermissionError("invalid lease token")
        candidate_state = str(candidate["state"])
        if candidate_state not in LEASE_LIVE_STATES:
            raise ValueError(f"lease is {candidate_state}")
        phase_unchanged = not normalized_phase or normalized_phase == str(
            candidate.get("fault_phase") or ""
        )
        persist_interval = _heartbeat_persist_interval_seconds(
            config.lease_ttl_seconds, int(lease_id)
        )
        if (
            candidate_state in {"attaching", "active", "releasing"}
            and phase_unchanged
            and _heartbeat_is_fresh(
                str(candidate.get("last_heartbeat_at") or ""),
                now,
                persist_interval,
            )
        ):
            current = self.get_lease(lease_id, include_secret_hash=False)
            current_state = str(current.get("state") or "")
            if current_state not in LEASE_LIVE_STATES:
                raise ValueError(f"lease is {current_state}")
            return current
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            if not row:
                raise KeyError(lease_id)
            if not secrets.compare_digest(str(row["client_token_hash"]), token_hash):
                raise PermissionError("invalid lease token")
            current_state = str(row["state"])
            if current_state not in LEASE_LIVE_STATES:
                raise ValueError(f"lease is {current_state}")
            cursor = conn.execute(
                """
                UPDATE aedt_project_leases SET
                    state = CASE
                        WHEN protocol_version < 2 AND state = 'leased'
                        THEN 'active' ELSE state END,
                    last_heartbeat_at = ?, expires_at = ?,
                    offer_expires_at = CASE
                        WHEN protocol_version >= 2 AND state = 'offered'
                        THEN ? ELSE offer_expires_at END,
                    fault_phase = CASE WHEN ? = '' THEN fault_phase ELSE ? END,
                    updated_at = ?
                WHERE id = ? AND state = ? AND client_token_hash = ?
                """,
                (
                    now_sql,
                    _sql_time(now + timedelta(seconds=config.lease_ttl_seconds)),
                    _sql_time(now + timedelta(seconds=config.offer_ack_seconds)),
                    normalized_phase,
                    normalized_phase,
                    now_sql,
                    int(lease_id),
                    current_state,
                    token_hash,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("lease state changed while heartbeating")
            if current_state == "queued" and config.operational:
                self._place_queued_leases(
                    conn, now_sql, lease_ids=(int(lease_id),), config=config
                )
        return self.get_lease(lease_id, include_secret_hash=False)

    def accept_lease(self, lease_id: int, token: str) -> dict[str, Any]:
        token_hash = _token_hash(token)
        now_dt = self._now()
        now = _sql_time(now_dt)
        config = self.config()
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if not secrets.compare_digest(
                str(lease["client_token_hash"]), token_hash
            ):
                raise PermissionError("invalid lease token")
            if int(lease["protocol_version"] or 1) < 2:
                if str(lease["state"]) in {"leased", "active"}:
                    return self.get_lease(lease_id, include_secret_hash=False)
                raise ValueError(f"lease is {lease['state']}")
            if str(lease["state"]) in {"attaching", "active"}:
                return self.get_lease(lease_id, include_secret_hash=False)
            if str(lease["state"]) != "offered":
                raise ValueError(f"lease is {lease['state']}")

            session = conn.execute(
                """
                SELECT s.*, a.state AS allocation_state
                FROM aedt_sessions s
                LEFT JOIN allocations a ON a.id = s.allocation_id
                WHERE s.id = ?
                """,
                (int(lease["session_id"] or 0),),
            ).fetchone()
            exact_reservation_id = int(
                lease["exact_session_reservation_id"] or 0
            )
            exact_reservation = (
                conn.execute(
                    """
                    SELECT * FROM aedt_exact_session_reservations
                    WHERE id = ?
                    """,
                    (exact_reservation_id,),
                ).fetchone()
                if exact_reservation_id
                else None
            )
            heartbeat_cutoff = _sql_time(
                now_dt
                - timedelta(seconds=config.session_heartbeat_timeout_seconds)
            )
            offer_live = str(lease["offer_expires_at"] or "") > now
            requested_session_id = int(lease["requested_session_id"] or 0)
            requested_generation = int(
                lease["requested_session_generation"] or 0
            )
            exact_target_invalid = bool(
                exact_reservation_id
                and (
                    not exact_reservation
                    or str(exact_reservation["state"] or "") != "consumed"
                    or int(exact_reservation["lease_id"] or 0) != int(lease_id)
                    or int(exact_reservation["session_id"] or 0)
                    != int(lease["session_id"] or 0)
                    or int(exact_reservation["session_generation"] or 0)
                    != requested_generation
                    or str(exact_reservation["session_profile"] or "")
                    != str(lease["session_profile"] or "")
                    or (
                        str(exact_reservation["workload_family"] or "")
                        and (
                            str(exact_reservation["workload_family"] or "")
                            != str(lease["workload_family"] or "")
                            or str(exact_reservation["isolation_policy"] or "")
                            != str(lease["isolation_policy"] or "")
                        )
                    )
                )
            )
            hard_target_invalid = bool(
                exact_target_invalid
                or not session
                or str(session["state"] or "") not in SESSION_ASSIGNABLE_STATES
                or str(session["allocation_state"] or "") not in {"warm", "active"}
                or session["solve_batch_sealed_at"]
                or session["drain_requested_at"]
                or (
                    requested_session_id
                    and int(session["id"]) != requested_session_id
                )
                or (
                    requested_generation
                    and int(session["generation"] or 0) != requested_generation
                )
                or str(session["session_profile"] or "")
                != str(lease["session_profile"] or "")
                or str(session["last_heartbeat_at"] or "") < heartbeat_cutoff
                or not str(session["endpoint"] or "").strip()
                or not str(session["process_id"] or "").strip()
            )
            transient_invalid = bool(
                not offer_live
                or (
                    session
                    and (
                        session["reuse_blocked_at"]
                        or conn.execute(
                            """
                            SELECT 1 FROM aedt_project_leases
                            WHERE session_id = ? AND state = 'releasing'
                            LIMIT 1
                            """,
                            (int(session["id"]),),
                        ).fetchone()
                    )
                )
            )
            if hard_target_invalid or transient_invalid:
                if exact_reservation_id and hard_target_invalid:
                    failure_message = (
                        "exact-session reservation target failed "
                        "accept-time ownership/liveness validation"
                    )
                    if exact_reservation:
                        self._fail_exact_session_reservation_cohort(
                            conn,
                            reservation_key=str(
                                exact_reservation["reservation_key"]
                            ),
                            now=now,
                            failure_message=failure_message,
                        )
                    # A missing or already-terminal reservation is outside the
                    # cohort updater's active-state predicate.  The offered
                    # lease itself must still fail closed instead of attaching.
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'failed', failure_message = ?,
                            finished_at = ?, updated_at = ?
                        WHERE id = ? AND state = 'offered'
                        """,
                        (failure_message, now, now, int(lease_id)),
                    )
                    if lease["session_id"]:
                        self._refresh_session_state(
                            conn, int(lease["session_id"]), now
                        )
                else:
                    old_session_id = int(lease["session_id"] or 0)
                    cursor = conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'queued', session_id = NULL, slot_index = NULL,
                            acquired_at = NULL, offered_at = NULL,
                            offer_expires_at = NULL, accepted_at = NULL,
                            activated_at = NULL, solve_permit_at = NULL,
                            solve_permit_generation = 0,
                            failure_message = ?, last_heartbeat_at = ?,
                            expires_at = ?, updated_at = ?
                        WHERE id = ? AND state = 'offered'
                        """,
                        (
                            "session reuse/liveness changed before offer acceptance",
                            now,
                            _sql_time(
                                now_dt
                                + timedelta(seconds=config.lease_ttl_seconds)
                            ),
                            now,
                            int(lease_id),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ValueError("lease changed while revoking stale offer")
                    if exact_reservation_id:
                        restored = conn.execute(
                            """
                            UPDATE aedt_exact_session_reservations
                            SET state = 'claimed', consumed_at = NULL, updated_at = ?
                            WHERE id = ? AND lease_id = ? AND state = 'consumed'
                            """,
                            (now, exact_reservation_id, int(lease_id)),
                        )
                        if restored.rowcount != 1:
                            raise RuntimeError(
                                "exact reservation changed while revoking stale offer"
                            )
                    if old_session_id:
                        self._refresh_session_state(conn, old_session_id, now)
            else:
                cursor = conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'attaching', accepted_at = ?,
                        last_heartbeat_at = ?, expires_at = ?, updated_at = ?
                    WHERE id = ? AND state = 'offered' AND offer_expires_at > ?
                    """,
                    (
                        now,
                        now,
                        _sql_time(
                            now_dt + timedelta(seconds=config.lease_ttl_seconds)
                        ),
                        now,
                        int(lease_id),
                        now,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ValueError("lease offer expired or was already claimed")
        return self.get_lease(lease_id, include_secret_hash=False)

    def activate_lease(self, lease_id: int, token: str) -> dict[str, Any]:
        lease = self._authorize_lease(lease_id, token)
        if lease["state"] == "active":
            return self.get_lease(lease_id, include_secret_hash=False)
        if int(lease.get("protocol_version") or 1) < 2:
            if lease["state"] != "leased":
                raise ValueError(f"lease is {lease['state']}")
        elif lease["state"] != "attaching":
            raise ValueError(f"lease is {lease['state']}")
        now_dt = self._now()
        now = _sql_time(now_dt)
        config = self.config()
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = 'active', activated_at = COALESCE(activated_at, ?),
                    last_heartbeat_at = ?, expires_at = ?, updated_at = ?
                WHERE id = ? AND state = ?
                """,
                (
                    now,
                    now,
                    _sql_time(now_dt + timedelta(seconds=config.lease_ttl_seconds)),
                    now,
                    int(lease_id),
                    str(lease["state"]),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("lease state changed while activating")
            self._grant_solve_batch_if_ready(
                conn,
                int(lease.get("session_id") or 0),
                now,
                allow_underfilled=False,
            )
        return self.get_lease(lease_id, include_secret_hash=False)

    def _grant_solve_batch_if_ready(
        self,
        conn: Any,
        session_id: int,
        now: str,
        *,
        allow_underfilled: bool,
    ) -> bool:
        """Atomically seal one Desktop and grant its next safe solve wave.

        Every family currently runs one native pipeline at a time.  Remaining
        leases wait in ``active`` without a permit until the predecessor has
        both marked its native pipeline complete and released its project.
        This prevents a long blocking Analyze call from invalidating a sibling
        client's gRPC handle while retaining parallelism across Desktop
        sessions.
        """

        if session_id <= 0:
            return False
        self._refresh_mixed_canary_admissions(conn, now)
        self._refresh_exact_session_reservations(conn, now)
        session = conn.execute(
            "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
        ).fetchone()
        if not session or str(session["state"]) not in {"ready", "busy"}:
            return False
        occupants = conn.execute(
            """
            SELECT id, state, exclusive_session, workload_family,
                   solve_permit_at, solve_permit_generation,
                   native_pipeline_completed_at,
                   native_pipeline_session_id, native_pipeline_generation
            FROM aedt_project_leases
            WHERE session_id = ?
              AND state IN ('offered','leased','attaching','active','releasing')
            ORDER BY id ASC
            """,
            (int(session_id),),
        ).fetchall()
        sealed = bool(str(session["solve_batch_sealed_at"] or ""))

        if not sealed:
            if not self._mixed_canary_batch_is_complete(conn, int(session_id)):
                # A one-shot mixed admission is evidence only if all three
                # exact reserved projects are ACTIVE before the first wave.
                return False
            if not self._exact_session_reservations_are_active(
                conn, int(session_id)
            ):
                # Do not seal while a bootstrap-reserved peer still owns a
                # slot on this exact session.
                return False
            if not occupants or any(
                str(row["state"]) != "active" for row in occupants
            ):
                return False
            full = len(occupants) >= int(session["slots_total"] or 1)
            exclusive = any(bool(row["exclusive_session"]) for row in occupants)
            if not (full or exclusive or allow_underfilled):
                return False
            conn.execute(
                """
                UPDATE aedt_sessions
                SET solve_batch_sealed_at = ?,
                    solve_batch_generation = solve_batch_generation + 1,
                    updated_at = ?
                WHERE id = ? AND solve_batch_sealed_at IS NULL
                """,
                (now, now, int(session_id)),
            )
        else:
            # A waiting client polls request_solve_permit while the preceding
            # wave is active/releasing.  Never let that poll widen the sealed
            # cohort.  Only a fully attested, successfully released generation
            # can authorize the next family wave.
            generation = int(session["solve_batch_generation"] or 0)
            granted = conn.execute(
                """
                SELECT state, native_pipeline_completed_at,
                       native_pipeline_session_id, native_pipeline_generation
                FROM aedt_project_leases
                WHERE session_id = ? AND solve_permit_generation = ?
                  AND solve_permit_at IS NOT NULL
                ORDER BY id
                """,
                (int(session_id), generation),
            ).fetchall()
            if not granted:
                return False
            if any(str(row["state"]) in LEASE_LIVE_STATES for row in granted):
                return False
            predecessor_ok = all(
                str(row["state"]) == "released"
                and bool(str(row["native_pipeline_completed_at"] or ""))
                and int(row["native_pipeline_session_id"] or 0)
                == int(session_id)
                and int(row["native_pipeline_generation"] or 0) == generation
                for row in granted
            )
            if not predecessor_ok:
                failure = (
                    "sealed native solve predecessor wave failed; "
                    "refusing mixed-family continuation"
                )
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'draining',
                        failure_message = ?,
                        reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                        drain_requested_at = COALESCE(drain_requested_at, ?),
                        updated_at = ?
                    WHERE id = ? AND state IN ('ready','busy')
                    """,
                    (
                        failure,
                        now,
                        now,
                        now,
                        int(session_id),
                    ),
                )
                # Waiting members already own projects, so use the normal
                # two-phase host close instead of stranding an ACTIVE lease on
                # a draining Desktop.  The client observes ``releasing`` and
                # fails closed; the host ACK then frees every reserved slot.
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'releasing', failure_message = ?,
                        release_requested_at = COALESCE(release_requested_at, ?),
                        updated_at = ?
                    WHERE session_id = ? AND state = 'active'
                      AND solve_permit_at IS NULL
                    """,
                    (failure, now, now, int(session_id)),
                )
                return False
            waiting = [
                row
                for row in occupants
                if str(row["state"]) == "active"
                and not str(row["solve_permit_at"] or "")
            ]
            if not waiting:
                return False
            conn.execute(
                """
                UPDATE aedt_sessions
                SET solve_batch_generation = solve_batch_generation + 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, int(session_id)),
            )

        waiting = [
            row
            for row in occupants
            if str(row["state"]) == "active"
            and not str(row["solve_permit_at"] or "")
        ]
        if not waiting:
            return False
        families: dict[str, list[Any]] = {}
        for row in waiting:
            family = str(row["workload_family"] or "").strip().lower()
            families.setdefault(family, []).append(row)
        # Keep mixed cohorts deterministic: finish MFT predecessors before the
        # motor family, but issue only one native permit unless a family is
        # explicitly restored to the proven-parallel allowlist above.
        selected_family = (
            "mft" if "mft" in families else sorted(families)[0]
        )
        selected = families[selected_family]
        if selected_family not in self._parallel_safe_native_solve_families:
            selected = selected[:1]

        generation = int(
            conn.execute(
                "SELECT solve_batch_generation FROM aedt_sessions WHERE id = ?",
                (int(session_id),),
            ).fetchone()[0]
        )
        lease_ids = [int(row["id"]) for row in selected]
        placeholders = ",".join("?" for _ in lease_ids)
        conn.execute(
            f"""
            UPDATE aedt_project_leases
            SET solve_permit_at = COALESCE(solve_permit_at, ?),
                solve_permit_generation = ?, updated_at = ?
            WHERE session_id = ? AND state = 'active'
              AND solve_permit_at IS NULL
              AND id IN ({placeholders})
            """,
            (now, generation, now, int(session_id), *lease_ids),
        )
        return True

    def request_solve_permit(
        self,
        lease_id: int,
        token: str,
        *,
        seal_underfilled: bool = False,
    ) -> dict[str, Any]:
        """Seal an underfilled active batch after the bounded client wait."""

        if type(seal_underfilled) is not bool:
            raise ValueError("seal_underfilled must be a boolean")
        token_hash = _token_hash(token)
        now_dt = self._now()
        now = _sql_time(now_dt)
        config = self.config()
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if not secrets.compare_digest(
                str(lease["client_token_hash"]), token_hash
            ):
                raise PermissionError("invalid lease token")
            if str(lease["state"]) != "active":
                raise ValueError(f"lease is {lease['state']}")
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET last_heartbeat_at = ?, expires_at = ?,
                    fault_phase = 'waiting', updated_at = ?
                WHERE id = ? AND state = 'active' AND client_token_hash = ?
                """,
                (
                    now,
                    _sql_time(now_dt + timedelta(seconds=config.lease_ttl_seconds)),
                    now,
                    int(lease_id),
                    token_hash,
                ),
            )
            self._grant_solve_batch_if_ready(
                conn,
                int(lease["session_id"] or 0),
                now,
                allow_underfilled=seal_underfilled,
            )
        return self.get_lease(lease_id, include_secret_hash=False)

    def complete_native_pipeline(
        self,
        lease_id: int,
        token: str,
        *,
        solve_permit_generation: int,
    ) -> dict[str, Any]:
        """Mark one sealed-batch member safe for Desktop-global postprocess.

        A project reaches this point only after its final blocking native
        ``Analyze`` and terminal attestation have both completed.  The marker
        is scoped to the server-authorized session and solve generation, so a
        delayed client from a recycled/requeued lease cannot satisfy a later
        cohort.  Callers must wait until every exact cohort member has marked
        completion before entering long Desktop-global extraction.
        """

        if type(solve_permit_generation) is not int \
                or solve_permit_generation <= 0:
            raise ValueError("solve_permit_generation must be a positive integer")
        token_hash = _token_hash(token)
        now_dt = self._now()
        now = _sql_time(now_dt)
        config = self.config()
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if not secrets.compare_digest(
                str(lease["client_token_hash"]), token_hash
            ):
                raise PermissionError("invalid lease token")
            if str(lease["state"]) != "active":
                raise ValueError(f"lease is {lease['state']}")
            session_id = int(lease["session_id"] or 0)
            authorized_generation = int(
                lease["solve_permit_generation"] or 0
            )
            if not str(lease["solve_permit_at"] or "") \
                    or session_id <= 0 or authorized_generation <= 0:
                raise ValueError("lease has no sealed native solve permit")
            if solve_permit_generation != authorized_generation:
                raise ValueError(
                    "native pipeline generation mismatch: "
                    f"expected={authorized_generation}, "
                    f"actual={solve_permit_generation}"
                )
            cohort_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM aedt_project_leases
                    WHERE session_id = ? AND solve_permit_generation = ?
                      AND solve_permit_at IS NOT NULL
                    """,
                    (session_id, authorized_generation),
                ).fetchone()[0]
            )
            if cohort_count <= 0:
                raise ValueError("sealed native solve cohort is unavailable")
            cursor = conn.execute(
                """
                UPDATE aedt_project_leases
                SET native_pipeline_completed_at = CASE
                        WHEN native_pipeline_session_id = ?
                         AND native_pipeline_generation = ?
                         AND native_pipeline_completed_at IS NOT NULL
                        THEN native_pipeline_completed_at ELSE ? END,
                    native_pipeline_session_id = ?,
                    native_pipeline_generation = ?,
                    fault_phase = 'postprocess',
                    last_heartbeat_at = ?, expires_at = ?, updated_at = ?
                WHERE id = ? AND state = 'active'
                  AND client_token_hash = ?
                  AND session_id = ? AND solve_permit_generation = ?
                """,
                (
                    session_id,
                    authorized_generation,
                    now,
                    session_id,
                    authorized_generation,
                    now,
                    _sql_time(
                        now_dt + timedelta(seconds=config.lease_ttl_seconds)
                    ),
                    now,
                    int(lease_id),
                    token_hash,
                    session_id,
                    authorized_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(
                    "lease changed while completing its native pipeline"
                )
        return self.get_lease(lease_id, include_secret_hash=False)

    def bind_lease_project_name(
        self, lease_id: int, token: str, project_name: str
    ) -> dict[str, Any]:
        project_name = project_name.strip()
        if not project_name:
            raise ValueError("project_name is required")
        token_hash = _token_hash(token)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if not secrets.compare_digest(
                str(lease["client_token_hash"]), token_hash
            ):
                raise PermissionError("invalid lease token")
            current_state = str(lease["state"])
            if current_state not in {
                "queued", "offered", "leased", "attaching", "active"
            }:
                raise ValueError(f"lease is {current_state}")
            if int(lease["protocol_version"] or 1) >= 2:
                duplicate = conn.execute(
                    """
                    SELECT 1 FROM aedt_project_leases
                    WHERE id != ? AND project_name = ?
                      AND state IN (
                          'queued','offered','leased','attaching','active','releasing'
                      )
                    LIMIT 1
                    """,
                    (int(lease_id), project_name),
                ).fetchone()
                if duplicate:
                    raise ValueError("project_name is already owned by a live lease")
            cursor = conn.execute(
                """
                UPDATE aedt_project_leases
                SET project_name = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND state = ? AND client_token_hash = ?
                """,
                (project_name, int(lease_id), current_state, token_hash),
            )
            if cursor.rowcount != 1:
                raise ValueError("lease state changed while binding project name")
        return self.get_lease(lease_id, include_secret_hash=False)

    def cancel_lease(
        self,
        lease_id: int,
        token: str,
        *,
        reason: str = "client cancelled lease",
    ) -> dict[str, Any]:
        now = _sql_time(self._now())
        token_hash = _token_hash(token)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if not secrets.compare_digest(
                str(lease["client_token_hash"]), token_hash
            ):
                raise PermissionError("invalid lease token")
            current_state = str(lease["state"])
            if current_state not in LEASE_TERMINAL_STATES:
                owns_project = current_state in {
                    "attaching", "active", "releasing"
                }
                if (
                    current_state == "leased"
                    and int(lease["protocol_version"] or 1) < 2
                ):
                    owns_project = True
                if owns_project and lease["session_id"]:
                    # Two phase release: only the session host can confirm that
                    # the project is closed and make this slot reusable.
                    cursor = conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'releasing', failure_message = ?,
                            release_requested_at = ?, updated_at = ?
                        WHERE id = ? AND state = ? AND client_token_hash = ?
                        """,
                        (
                            reason.strip(), now, now, int(lease_id), current_state,
                            token_hash,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ValueError("lease state changed while cancelling")
                    conn.execute(
                        """
                        UPDATE aedt_sessions
                        SET reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                            updated_at = ?
                        WHERE id = ? AND state NOT IN ('closed','failed')
                        """,
                        (now, now, int(lease["session_id"])),
                    )
                else:
                    cursor = conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'cancelled', session_id = NULL, slot_index = NULL,
                            failure_message = ?, finished_at = ?, updated_at = ?
                        WHERE id = ? AND state = ? AND client_token_hash = ?
                        """,
                        (
                            reason.strip(), now, now, int(lease_id), current_state,
                            token_hash,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ValueError("lease state changed while cancelling")
                    if lease["session_id"]:
                        self._refresh_session_state(
                            conn, int(lease["session_id"]), now
                        )
        return self.get_lease(lease_id, include_secret_hash=False)

    def release_lease(self, lease_id: int, token: str) -> dict[str, Any]:
        """Protocol-v1 compatibility alias for cancel/release."""

        return self.cancel_lease(lease_id, token, reason="client released lease")

    def report_project_fault(
        self,
        lease_id: int,
        token: str,
        *,
        fault_kind: str,
        phase: str = "",
        evidence: dict[str, Any] | None = None,
        sibling_grace_seconds: int = 900,
        failure_message: str = "",
    ) -> dict[str, Any]:
        """Report a project-local error without pretending solver cancellation is local.

        Pre-solve/script errors use the normal two-phase project close.  Once a
        solver may be running, AEDT's public stop call is session-wide, so the
        session is quarantined immediately (no new leases), a healthy sibling
        gets a bounded grace period, and only then may the host globally stop
        and recycle the Desktop.
        """
        lease = self._authorize_lease(lease_id, token)
        normalized = fault_kind.strip().lower()
        allowed = {
            "admission_timeout",
            "attach_failed",
            "project_create_failed",
            "pre_solve",
            "script_error",
            "solver_timeout",
            "aedt_transport_death",
            "aedt_death",  # protocol-v1 compatibility
        }
        if normalized not in allowed:
            raise ValueError(f"fault_kind must be one of: {', '.join(sorted(allowed))}")
        normalized_phase = str(phase or "").strip().lower()
        evidence_json = json.dumps(
            evidence or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET fault_phase = ?, fault_kind = ?, fault_evidence_json = ?,
                    failure_message = CASE WHEN ? = '' THEN failure_message ELSE ? END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    normalized_phase,
                    normalized,
                    evidence_json,
                    failure_message.strip(),
                    failure_message.strip(),
                    int(lease_id),
                ),
            )
        if normalized in {
            "admission_timeout",
            "attach_failed",
            "project_create_failed",
            "pre_solve",
            "script_error",
        }:
            return self.cancel_lease(
                lease_id,
                token,
                reason=failure_message.strip() or normalized.replace("_", " "),
            )
        session_id = int(lease.get("session_id") or 0)
        if not session_id:
            return self.cancel_lease(lease_id, token, reason=normalized)
        if normalized == "aedt_transport_death":
            # A client cannot authoritatively kill or quarantine a shared
            # Desktop.  Stop new placement briefly; the next authenticated
            # host heartbeat proves it is alive and recovers the session.
            message = failure_message.strip() or "project client lost AEDT transport"
            now = _sql_time(self._now())
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'unhealthy', failure_message = ?,
                        drain_requested_at = NULL, updated_at = ?
                    WHERE id = ? AND state IN ('ready','busy')
                    """,
                    (f"client transport suspect: {message}", now, session_id),
                )
            return self.cancel_lease(lease_id, token, reason=message)
        if normalized == "aedt_death":
            session = self.get_session(session_id)
            # Only a host token can normally declare a Desktop dead.  A client
            # report merely quarantines it until host heartbeat/recovery logic
            # confirms the failure.
            message = failure_message.strip() or "project client lost AEDT/gRPC connection"
            now = _sql_time(self._now())
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE aedt_sessions SET state = 'unhealthy', failure_message = ?,
                        quarantine_reason = 'aedt_death_reported',
                        drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                    WHERE id = ? AND state IN ('ready','busy','draining')
                    """,
                    (message, now, now, int(session["id"])),
                )
                conn.execute(
                    """
                    UPDATE allocations SET state = 'draining',
                        drain_reason = 'AEDT pool reported AEDT/gRPC death',
                        drain_at = COALESCE(drain_at, ?), updated_at = ?
                    WHERE id = ? AND state IN ('warm','active','draining')
                    """,
                    (now, now, int(session.get("allocation_id") or 0)),
                )
            return self.get_lease(lease_id, include_secret_hash=False)

        now_dt = self._now()
        now = _sql_time(now_dt)
        quarantine_until = _sql_time(
            now_dt + timedelta(seconds=max(60, min(3600, int(sibling_grace_seconds))))
        )
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = 'releasing', failure_message = ?,
                    release_requested_at = ?, updated_at = ?
                WHERE id = ? AND state IN ('leased','attaching','active')
                """,
                (failure_message.strip() or "solver timeout", now, now, int(lease_id)),
            )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'draining', quarantine_reason = 'solver_timeout',
                    quarantine_until = ?, failure_message = ?,
                    reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE id = ? AND state IN ('ready','busy','draining')
                """,
                (
                    quarantine_until,
                    failure_message.strip() or "solver timeout; sibling grace active",
                    now,
                    now,
                    now,
                    session_id,
                ),
            )
            conn.execute(
                """
                UPDATE allocations
                SET state = 'draining',
                    drain_reason = 'AEDT pool solver fault quarantine',
                    drain_at = COALESCE(drain_at, ?), updated_at = ?
                WHERE id = ? AND state IN ('warm','active','draining')
                """,
                (now, now, int(lease.get("session_allocation_id") or 0)),
            )
        return self.get_lease(lease_id, include_secret_hash=False)

    def report_session_fault(
        self,
        session_id: int,
        token: str,
        *,
        kind: str,
        failure_message: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Record a process-level fault only an authenticated host can confirm."""

        session = self._authorize_session(session_id, token)
        normalized = str(kind or "").strip().lower()
        if normalized not in {"confirmed_aedt_death", "native_probe_suspect"}:
            raise ValueError(
                "session fault kind must be confirmed_aedt_death or native_probe_suspect"
            )
        now = _sql_time(self._now())
        metadata = json.dumps(
            evidence or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        native_snapshot_path = str(
            (evidence or {}).get("native_snapshot_path") or ""
        ).strip()
        message = failure_message.strip() or "session host confirmed AEDT process death"
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if normalized == "native_probe_suspect":
                # A native probe can time out while Analyze owns AEDT's
                # scripting thread.  Keep that session heartbeat fresh while
                # an accepted client still owns native work.  Conversely, an
                # idle proxy that is permanently wedged must not keep itself
                # alive forever merely by reporting the same suspect probe.
                #
                # ``offered`` is only a revocable slot reservation; no client
                # may touch AEDT until it is accepted.  ``queued`` has no
                # session ownership at all.  An expired owner is likewise not
                # evidence that a long solve is still supervised by a live
                # client.
                live_native_owner = conn.execute(
                    """
                    SELECT 1 FROM aedt_project_leases
                    WHERE session_id = ?
                      AND state IN ('leased','attaching','active','releasing')
                      AND expires_at >= ?
                    LIMIT 1
                    """,
                    (int(session_id), now),
                ).fetchone()
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'unhealthy', quarantine_reason = '',
                        failure_message = ?, last_fault_evidence_json = ?,
                        last_fault_at = ?,
                        native_snapshot_path = CASE WHEN ? = ''
                            THEN native_snapshot_path ELSE ? END,
                        drain_requested_at = CASE WHEN ?
                            THEN NULL ELSE COALESCE(drain_requested_at, ?) END,
                        last_heartbeat_at = CASE WHEN ?
                            THEN ? ELSE last_heartbeat_at END,
                        updated_at = ?
                    WHERE id = ? AND state IN ('ready','busy','unhealthy')
                    """,
                    (
                        f"host native liveness suspect: {message}",
                        metadata,
                        now,
                        native_snapshot_path,
                        native_snapshot_path,
                        int(bool(live_native_owner)),
                        now,
                        int(bool(live_native_owner)),
                        now,
                        now,
                        int(session_id),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'unhealthy', quarantine_reason = ?,
                        failure_message = ?, last_fault_evidence_json = ?,
                        last_fault_at = ?,
                        native_snapshot_path = CASE WHEN ? = ''
                            THEN native_snapshot_path ELSE ? END,
                        drain_requested_at = COALESCE(drain_requested_at, ?),
                        updated_at = ?
                    WHERE id = ? AND state IN ('ready','busy','draining','unhealthy')
                    """,
                    (
                        normalized,
                        message,
                        metadata,
                        now,
                        native_snapshot_path,
                        native_snapshot_path,
                        now,
                        now,
                        int(session_id),
                    ),
                )
                conn.execute(
                    """
                    UPDATE allocations
                    SET state = 'draining', drain_reason = ?,
                        drain_at = COALESCE(drain_at, ?), updated_at = ?
                    WHERE id = ? AND state IN ('warm','active','draining')
                    """,
                    (
                        FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON,
                        now,
                        now,
                        int(session.get("allocation_id") or 0),
                    ),
                )
        return self.get_session(session_id, include_secret_hash=False)

    def authorize_bootstrap(self, token: str) -> None:
        """Authorize a control-plane mutation with the shared bootstrap secret."""
        if not self.bootstrap_token or not secrets.compare_digest(self.bootstrap_token, token):
            raise PermissionError("invalid session-host bootstrap token")

    def authorize_lease_client(self, token: str) -> None:
        """Authorize lease creation without operator or host authority."""

        if not self.lease_client_token:
            raise RuntimeError("AEDT lease client credential is not configured")
        if not secrets.compare_digest(
            self.lease_client_token, str(token or "")
        ):
            raise PermissionError("invalid AEDT lease client credential")

    def _authorize_bootstrap(self, token: str) -> None:
        """Backward-compatible internal alias for bootstrap authorization."""
        self.authorize_bootstrap(token)

    def claim_start(
        self,
        *,
        allocation_id: int,
        node_name: str,
        host_id: str,
        actual_node_name: str = "",
        slurm_job_id: str = "",
        host_process_id: str = "",
        bootstrap_token: str,
        session_id: int = 0,
    ) -> dict[str, Any] | None:
        self._authorize_bootstrap(bootstrap_token)
        normalized_host_id = host_id.strip()
        if not normalized_host_id:
            raise ValueError("host_id is required")
        normalized_node_name = node_name.strip()
        normalized_actual_node = actual_node_name.strip()
        normalized_slurm_job_id = slurm_job_id.strip()
        normalized_host_process_id = host_process_id.strip()
        requested_session_id = max(0, int(session_id or 0))
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if requested_session_id:
                row = conn.execute(
                    """
                    SELECT * FROM aedt_sessions
                    WHERE id = ? AND state = 'starting'
                      AND allocation_id = ? AND node_name = ?
                      AND (host_id = '' OR host_id = ?)
                    """,
                    (
                        requested_session_id,
                        int(allocation_id),
                        normalized_node_name,
                        normalized_host_id,
                    ),
                ).fetchone()
            else:
                # Backward-compatible untargeted hosts first recover their own
                # claim after an ambiguous HTTP response, then take the oldest
                # unclaimed row on the allocation.
                row = conn.execute(
                    """
                    SELECT * FROM aedt_sessions
                    WHERE state = 'starting'
                      AND allocation_id = ? AND node_name = ?
                      AND (host_id = '' OR host_id = ?)
                    ORDER BY CASE WHEN host_id = ? THEN 0 ELSE 1 END, id ASC
                    LIMIT 1
                    """,
                    (
                        int(allocation_id),
                        normalized_node_name,
                        normalized_host_id,
                        normalized_host_id,
                    ),
                ).fetchone()
            if not row:
                return None
            allocation = conn.execute(
                "SELECT * FROM allocations WHERE id = ?",
                (int(row["allocation_id"] or 0),),
            ).fetchone()
            if not allocation:
                raise ValueError("session allocation no longer exists")
            if int(row["allocation_id"] or 0) != int(allocation_id):
                raise ValueError("host claimed a different allocation")
            expected_node = str(row["node_name"] or allocation["node_name"] or "")
            if (
                normalized_actual_node
                and _short_node_name(normalized_actual_node)
                != _short_node_name(expected_node)
            ):
                raise ValueError(
                    f"host is running on {normalized_actual_node}, expected {expected_node}"
                )
            expected_slurm_job = str(allocation["slurm_job_id"] or "").strip()
            if (
                normalized_slurm_job_id
                and expected_slurm_job
                and normalized_slurm_job_id != expected_slurm_job
            ):
                raise ValueError(
                    f"host Slurm job {normalized_slurm_job_id} does not own allocation "
                    f"{int(allocation_id)} ({expected_slurm_job})"
                )
            host_task_id = int(row["host_task_id"] or 0)
            if host_task_id:
                task = conn.execute(
                    "SELECT * FROM tasks WHERE id = ?", (host_task_id,)
                ).fetchone()
                if not task:
                    raise ValueError("session host task no longer exists")
                if (
                    int(task["requested_allocation_id"] or 0) != int(allocation_id)
                    or int(task["allocation_id"] or 0) != int(allocation_id)
                    or str(task["status"])
                    not in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
                ):
                    raise ValueError("session host task is not pinned to its allocation")
            cursor = conn.execute(
                """
                UPDATE aedt_sessions
                SET host_id = ?, start_claimed_at = COALESCE(start_claimed_at, ?),
                    actual_node_name = ?, host_slurm_job_id = ?, host_process_id = ?,
                    updated_at = ?
                WHERE id = ? AND state = 'starting'
                  AND (host_id = '' OR host_id = ?)
                """,
                (
                    normalized_host_id,
                    now,
                    normalized_actual_node or expected_node,
                    normalized_slurm_job_id or expected_slurm_job,
                    normalized_host_process_id,
                    now,
                    int(row["id"]),
                    normalized_host_id,
                ),
            )
            if cursor.rowcount != 1:
                return None
            return dict(
                conn.execute(
                    "SELECT * FROM aedt_sessions WHERE id = ?", (int(row["id"]),)
                ).fetchone()
            )

    def register_session(
        self,
        *,
        session_id: int,
        host_id: str,
        endpoint: str,
        process_id: str,
        artifact_dir: str = "",
        error_log_path: str = "",
        journal_path: str = "",
        runtime_metadata: dict[str, Any] | None = None,
        session_profile: Any = "",
        bootstrap_token: str,
        host_token: str = "",
    ) -> tuple[dict[str, Any], str]:
        self._authorize_bootstrap(bootstrap_token)
        normalized_host_id = host_id.strip()
        if not normalized_host_id:
            raise ValueError("host_id is required")
        normalized_endpoint = endpoint.strip()
        normalized_process_id = process_id.strip()
        normalized_artifact_dir = artifact_dir.strip()
        normalized_error_log_path = error_log_path.strip()
        normalized_journal_path = journal_path.strip()
        runtime_metadata_json = json.dumps(
            runtime_metadata or {},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        normalized_session_profile = canonical_expected_session_profile(session_profile)
        presented_host_token = host_token.strip()
        issued_host_token = presented_host_token or secrets.token_urlsafe(32)
        now = _sql_time(self._now())
        register_state = "ready" if self.config().enabled else "draining"
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)).fetchone()
            if not row:
                raise KeyError(session_id)
            existing_profile = str(row["session_profile"] or "")
            if existing_profile and existing_profile != normalized_session_profile:
                raise ValueError("session host profile does not match admitted lease profile")
            owns_claim = str(row["host_id"] or "") == normalized_host_id
            same_registration = bool(
                owns_claim
                and presented_host_token
                and row["host_token_hash"]
                and secrets.compare_digest(
                    str(row["host_token_hash"]), _token_hash(presented_host_token)
                )
                and str(row["endpoint"] or "") == normalized_endpoint
                and str(row["process_id"] or "") == normalized_process_id
            )
            if same_registration and row["state"] in {
                "ready",
                "busy",
                "draining",
                "unhealthy",
            }:
                # The first response may have been lost after commit.  Echo the
                # token the host already knows instead of rotating it and
                # invalidating the live process's future heartbeats.
                issued_host_token = presented_host_token
            else:
                recovered_start_timeout = bool(
                    owns_claim
                    and row["state"] == "unhealthy"
                    and str(row["failure_message"] or "")
                    == SESSION_START_ACK_TIMEOUT_MESSAGE
                    and not row["started_at"]
                    and not row["host_token_hash"]
                )
                if row["state"] != "starting" and not recovered_start_timeout:
                    raise ValueError("session start claim is not owned by this host")
                if not owns_claim:
                    raise ValueError("session start claim is not owned by this host")
                if recovered_start_timeout:
                    allocation = conn.execute(
                        "SELECT state, drain_reason FROM allocations WHERE id = ?",
                        (int(row["allocation_id"] or 0),),
                    ).fetchone()
                    if not allocation or allocation["state"] not in {
                        "warm",
                        "active",
                        "draining",
                    }:
                        raise ValueError("session allocation is no longer available")
                    if (
                        allocation["state"] == "draining"
                        and str(allocation["drain_reason"] or "")
                        != UNHEALTHY_ALLOCATION_RECYCLE_REASON
                    ):
                        # The rightful late owner still needs a token so it can
                        # close its Desktop cleanly, but it must not cancel an
                        # unrelated age/operator/scheduler drain.
                        register_state = "draining"
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = ?, endpoint = ?, process_id = ?,
                        artifact_dir = ?, error_log_path = ?, journal_path = ?,
                        runtime_metadata_json = ?,
                        session_profile = CASE
                            WHEN ? = '' THEN session_profile ELSE ? END,
                        host_token_hash = ?, started_at = ?, last_heartbeat_at = ?,
                        idle_since = ?, failure_message = '', drain_requested_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        register_state,
                        normalized_endpoint,
                        normalized_process_id,
                        normalized_artifact_dir,
                        normalized_error_log_path,
                        normalized_journal_path,
                        runtime_metadata_json,
                        normalized_session_profile,
                        normalized_session_profile,
                        _token_hash(issued_host_token),
                        now,
                        now,
                        now,
                        now,
                        int(session_id),
                    ),
                )
                if recovered_start_timeout and register_state == "ready":
                    remaining_unhealthy = conn.execute(
                        """
                        SELECT 1 FROM aedt_sessions
                        WHERE allocation_id = ?
                          AND (state = 'unhealthy' OR quarantine_reason != '')
                        LIMIT 1
                        """,
                        (int(row["allocation_id"] or 0),),
                    ).fetchone()
                    if not remaining_unhealthy:
                        conn.execute(
                            """
                            UPDATE allocations
                            SET state = 'active',
                                drain_reason = 'AEDT pool project demand',
                                drain_at = NULL, failure_message = '', updated_at = ?
                            WHERE id = ? AND state = 'draining' AND drain_reason = ?
                            """,
                            (
                                now,
                                int(row["allocation_id"] or 0),
                                UNHEALTHY_ALLOCATION_RECYCLE_REASON,
                            ),
                        )
        self.reconcile(execute=True)
        return self.get_session(session_id, include_secret_hash=False), issued_host_token

    def fail_session_start(
        self,
        *,
        session_id: int,
        host_id: str,
        bootstrap_token: str,
        failure_message: str,
        artifact_dir: str = "",
        error_log_path: str = "",
        journal_path: str = "",
        runtime_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._authorize_bootstrap(bootstrap_token)
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'failed', failure_message = ?, artifact_dir = ?,
                    error_log_path = ?, journal_path = ?, runtime_metadata_json = ?,
                    closed_at = ?, updated_at = ?
                WHERE id = ? AND state = 'starting' AND host_id = ?
                """,
                (
                    failure_message.strip() or "AEDT session start failed",
                    str(artifact_dir or "").strip(),
                    str(error_log_path or "").strip(),
                    str(journal_path or "").strip(),
                    json.dumps(
                        runtime_metadata or {},
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    now,
                    now,
                    int(session_id),
                    host_id.strip(),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("session start claim is not owned by this host")
        return self.get_session(session_id, include_secret_hash=False)

    def bind_session_host_task(self, session_id: int, task_id: int) -> None:
        """Bind the sole host task before it can race the global scheduler."""

        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
            ).fetchone()
            task = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (int(task_id),)
            ).fetchone()
            if not session or not task:
                raise ValueError("session or host task no longer exists")
            allocation_id = int(session["allocation_id"] or 0)
            if int(task["requested_allocation_id"] or 0) != allocation_id:
                raise ValueError("session host task is not allocation-pinned")
            cursor = conn.execute(
                """
                UPDATE aedt_sessions SET host_task_id = ?,
                    host_stdout_path = ?, host_stderr_path = ?, updated_at = ?
                WHERE id = ? AND state = 'starting'
                  AND (host_task_id = 0 OR host_task_id = ?)
                """,
                (
                    int(task_id),
                    str(task["stdout_path"] or ""),
                    str(task["stderr_path"] or ""),
                    _sql_time(self._now()),
                    int(session_id),
                    int(task_id),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("session host task is already owned")

    def get_session(self, session_id: int, *, include_secret_hash: bool = True) -> dict[str, Any]:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)).fetchone()
            if not row:
                raise KeyError(session_id)
            item = dict(row)
            if not include_secret_hash:
                item.pop("host_token_hash", None)
            return item

    def _authorize_session(self, session_id: int, token: str) -> dict[str, Any]:
        session = self.get_session(session_id)
        if not session.get("host_token_hash") or not secrets.compare_digest(
            str(session["host_token_hash"]), _token_hash(token)
        ):
            raise PermissionError("invalid session token")
        return session

    @staticmethod
    def _validate_session_heartbeat(
        session: dict[str, Any],
        token_hash: str,
        *,
        liveness_confirmed: bool,
        process_id: str,
        port: int,
        native_probe: str,
    ) -> None:
        if not session.get("host_token_hash") or not secrets.compare_digest(
            str(session["host_token_hash"]), token_hash
        ):
            raise PermissionError("invalid session token")
        if session["state"] not in {"ready", "busy", "draining", "unhealthy"}:
            raise ValueError(f"session is {session['state']}")
        if not liveness_confirmed:
            raise ValueError("host heartbeat requires fresh Desktop liveness proof")
        if process_id and str(process_id).strip() != str(
            session["process_id"] or ""
        ):
            raise ValueError(
                "heartbeat Desktop process_id does not match registration"
            )
        if port:
            try:
                registered_port = int(
                    str(session["endpoint"] or "").rsplit(":", 1)[1]
                )
            except (IndexError, ValueError):
                registered_port = 0
            if int(port) != registered_port:
                raise ValueError("heartbeat Desktop port does not match registration")
        if native_probe and str(native_probe) != "GetVersion":
            raise ValueError("unsupported Desktop native liveness probe")

    def heartbeat_session(
        self,
        session_id: int,
        token: str,
        *,
        liveness_confirmed: bool = True,
        process_id: str = "",
        port: int = 0,
        native_probe: str = "",
    ) -> dict[str, Any]:
        now_dt = self._now()
        now = _sql_time(now_dt)
        token_hash = _token_hash(token)
        candidate = self.get_session(session_id)
        self._validate_session_heartbeat(
            candidate,
            token_hash,
            liveness_confirmed=liveness_confirmed,
            process_id=process_id,
            port=port,
            native_probe=native_probe,
        )
        config = self.config()
        persist_interval = _heartbeat_persist_interval_seconds(
            config.session_heartbeat_timeout_seconds, int(session_id)
        )
        if (
            candidate["state"] in {"ready", "busy"}
            and not str(candidate.get("failure_message") or "")
            and not str(candidate.get("quarantine_reason") or "")
            and not candidate.get("reuse_blocked_at")
            and _heartbeat_is_fresh(
                str(candidate.get("last_heartbeat_at") or ""),
                now_dt,
                persist_interval,
            )
        ):
            candidate.pop("host_token_hash", None)
            return candidate
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
            ).fetchone()
            if not row:
                raise KeyError(session_id)
            session = dict(row)
            self._validate_session_heartbeat(
                session,
                token_hash,
                liveness_confirmed=liveness_confirmed,
                process_id=process_id,
                port=port,
                native_probe=native_probe,
            )
            setting_rows = {
                str(item["key"]): str(item["value"])
                for item in conn.execute(
                    """
                    SELECT key, value FROM scheduler_settings
                    WHERE key IN ('aedt_pool_enabled', 'aedt_pool_adapter_ready')
                    """
                ).fetchall()
            }
            validation = conn.execute(
                "SELECT status FROM aedt_pool_validations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            pool_operational = bool(
                _bool_setting(
                    setting_rows.get(
                        "aedt_pool_enabled", DEFAULT_SETTINGS["aedt_pool_enabled"]
                    )
                )
                and _bool_setting(
                    setting_rows.get(
                        "aedt_pool_adapter_ready",
                        DEFAULT_SETTINGS["aedt_pool_adapter_ready"],
                    )
                )
                and validation
                and validation["status"] == "passed"
            )
            barrier_cleared = False
            if session.get("reuse_blocked_at"):
                cleared = conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET reuse_blocked_at = NULL, last_heartbeat_at = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND reuse_blocked_at IS NOT NULL
                      AND reuse_blocked_at < ?
                      AND state IN ('ready','busy')
                      AND failure_message = ''
                      AND quarantine_reason = ''
                      AND NOT EXISTS (
                          SELECT 1 FROM aedt_project_leases l
                          WHERE l.session_id = aedt_sessions.id
                            AND l.state = 'releasing'
                      )
                    """,
                    (now, now, int(session_id), now),
                )
                barrier_cleared = cleared.rowcount == 1
            recovered = False
            if (
                pool_operational
                and session["state"] == "unhealthy"
                and (
                    str(session.get("failure_message") or "")
                    == SESSION_HEARTBEAT_TIMEOUT_MESSAGE
                    or str(session.get("failure_message") or "").startswith(
                        "client transport suspect:"
                    )
                    or str(session.get("failure_message") or "").startswith(
                        "host native liveness suspect:"
                    )
                )
                and not str(session.get("quarantine_reason") or "")
            ):
                allocation = conn.execute(
                    "SELECT state, drain_reason FROM allocations WHERE id = ?",
                    (int(session.get("allocation_id") or 0),),
                ).fetchone()
                recoverable_allocation = bool(
                    allocation
                    and (
                        allocation["state"] in {"warm", "active"}
                        or (
                            allocation["state"] == "draining"
                            and str(allocation["drain_reason"] or "")
                            == UNHEALTHY_ALLOCATION_RECYCLE_REASON
                        )
                    )
                )
                if recoverable_allocation:
                    live = int(
                        conn.execute(
                            """
                            SELECT COUNT(*) FROM aedt_project_leases
                            WHERE session_id = ?
                              AND state IN ('offered','leased','attaching','active','releasing')
                            """,
                            (int(session_id),),
                        ).fetchone()[0]
                    )
                    cursor = conn.execute(
                        """
                        UPDATE aedt_sessions
                        SET state = ?, failure_message = '', drain_requested_at = NULL,
                            last_heartbeat_at = ?, idle_since = ?, updated_at = ?
                        WHERE id = ? AND state = 'unhealthy'
                          AND (
                              failure_message = ?
                              OR failure_message LIKE 'client transport suspect:%'
                              OR failure_message LIKE 'host native liveness suspect:%'
                          )
                          AND quarantine_reason = ''
                        """,
                        (
                            "busy" if live else "ready",
                            now,
                            None if live else now,
                            now,
                            int(session_id),
                            SESSION_HEARTBEAT_TIMEOUT_MESSAGE,
                        ),
                    )
                    recovered = cursor.rowcount == 1
                if recovered:
                    # A valid host token proves this was a control-plane gap,
                    # not a dead host.  Cancel only the recycle drain created
                    # for that gap, and only after every sibling has recovered.
                    remaining_unhealthy = conn.execute(
                        """
                        SELECT 1 FROM aedt_sessions
                        WHERE allocation_id = ?
                          AND (state = 'unhealthy' OR quarantine_reason != '')
                        LIMIT 1
                        """,
                        (int(session.get("allocation_id") or 0),),
                    ).fetchone()
                    if not remaining_unhealthy:
                        conn.execute(
                            """
                            UPDATE allocations
                            SET state = 'active',
                                drain_reason = 'AEDT pool project demand',
                                drain_at = NULL, failure_message = '', updated_at = ?
                            WHERE id = ? AND state = 'draining' AND drain_reason = ?
                            """,
                            (
                                now,
                                int(session.get("allocation_id") or 0),
                                UNHEALTHY_ALLOCATION_RECYCLE_REASON,
                            ),
                        )
            if not recovered:
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET last_heartbeat_at = ?, updated_at = ? WHERE id = ?
                    """,
                    (now, now, int(session_id)),
                )
            if barrier_cleared and pool_operational:
                self._place_queued_leases(conn, now, config=config)
        return self.get_session(session_id, include_secret_hash=False)

    def session_commands(self, session_id: int, token: str) -> dict[str, Any]:
        session = self._authorize_session(session_id, token)
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, request_key, project_name, task_id, slot_index,
                       state, failure_message, project_namespace,
                       workspace_path, protocol_version
                FROM aedt_project_leases
                WHERE session_id = ? AND state = 'releasing'
                ORDER BY id ASC
                """,
                (int(session_id),),
            ).fetchall()
            sibling_live = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM aedt_project_leases
                    WHERE session_id = ? AND state IN ('offered','leased','attaching','active')
                    """,
                    (int(session_id),),
                ).fetchone()[0]
            )
        deadline = str(session.get("quarantine_until") or "")
        grace_expired = bool(deadline and deadline <= _sql_time(self._now()))
        quarantined = bool(session.get("quarantine_reason"))
        global_stop_allowed = bool(
            session["state"] in {"draining", "unhealthy"}
            and quarantined
            and (sibling_live == 0 or grace_expired)
        )
        close_projects = []
        deferred_projects = []
        for row in rows:
            item = dict(row)
            failure_text = str(item.get("failure_message") or "").lower()
            is_timeout_owner = "timeout" in failure_text or "heartbeat expired" in failure_text
            # A killed solver PID can leave both its license checkout and
            # Desktop AreThereSimulationsRunning state stuck.  Never turn that
            # lease into released/failed via project-close ACK and never reuse
            # this Desktop.  Keep A in releasing until the whole quarantined
            # Desktop is recycled; close_session then requeues A.
            if quarantined and is_timeout_owner:
                deferred_projects.append(item)
            else:
                close_projects.append(item)
        return {
            "close_projects": close_projects,
            "deferred_projects": deferred_projects,
            "drain": session["state"] in {"draining", "unhealthy"},
            "quarantine_reason": str(session.get("quarantine_reason") or ""),
            "sibling_live_count": sibling_live,
            "sibling_grace_deadline": deadline or None,
            "global_stop_allowed": global_stop_allowed,
            "recycle_after_global_stop": global_stop_allowed,
        }

    def complete_release(
        self,
        session_id: int,
        token: str,
        lease_id: int,
        *,
        success: bool,
        failure_message: str = "",
    ) -> dict[str, Any]:
        self._authorize_session(session_id, token)
        now = _sql_time(self._now())
        config = self.config()
        expected_state = "released" if success else "failed"
        normalized_failure = "" if success else failure_message.strip()
        replayed = False
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ? AND session_id = ?",
                (int(lease_id), int(session_id)),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if lease["state"] != "releasing":
                replayed = bool(
                    lease["state"] == expected_state
                    and str(lease["failure_message"] or "") == normalized_failure
                )
                if not replayed:
                    raise ValueError(f"lease is {lease['state']}")
            if not replayed:
                cursor = conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = ?, failure_message = ?, finished_at = ?, updated_at = ?
                    WHERE id = ? AND session_id = ? AND state = 'releasing'
                    """,
                    (
                        expected_state,
                        normalized_failure,
                        now,
                        now,
                        int(lease_id),
                        int(session_id),
                    ),
                )
                if cursor.rowcount != 1:
                    raise ValueError("lease changed while completing release")
                # The ACK is the release barrier origin.  A heartbeat that was
                # sent before cleanup completed must not clear a timestamp left
                # over from cancel_lease while waiting for this writer lock.
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET reuse_blocked_at = ?, updated_at = ?
                    WHERE id = ? AND state NOT IN ('closed','failed')
                    """,
                    (now, now, int(session_id)),
                )
                if not success:
                    conn.execute(
                        """
                        UPDATE aedt_sessions
                        SET state = 'unhealthy', drain_requested_at = ?,
                            quarantine_until = ?,
                            quarantine_reason = ?, failure_message = ?,
                            updated_at = ?
                        WHERE id = ? AND state NOT IN ('closed','failed')
                        """,
                        (
                            now,
                            now,
                            "project release cleanup failed",
                            normalized_failure
                            or "session host could not confirm project cleanup",
                            now,
                            int(session_id),
                        ),
                    )
                self._refresh_session_state(conn, int(session_id), now)
                self._refresh_exact_session_reservations(conn, now)
                if config.operational:
                    self._place_queued_leases(conn, now, config=config)
        return self.get_lease(lease_id, include_secret_hash=False)

    def _validate_dead_session_reap_candidate(
        self,
        conn: Any,
        session_id: int,
        expected_identity: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
        ).fetchone()
        if not row:
            raise KeyError(session_id)
        session = dict(row)
        if str(session["state"]) not in {"unhealthy", "draining"}:
            raise ValueError(
                "dead-session reap requires an unhealthy or draining session"
            )

        normalized_expected = {
            "generation": int(expected_identity.get("generation") or 0),
            "allocation_id": int(expected_identity.get("allocation_id") or 0),
            "host_id": str(expected_identity.get("host_id") or "").strip(),
            "host_task_id": int(expected_identity.get("host_task_id") or 0),
            "host_process_id": str(
                expected_identity.get("host_process_id") or ""
            ).strip(),
            "process_id": str(expected_identity.get("process_id") or "").strip(),
        }
        if any(
            not normalized_expected[field]
            for field in DEAD_SESSION_IDENTITY_FIELDS
        ):
            raise ValueError("complete expected session identity is required")
        for field in DEAD_SESSION_IDENTITY_FIELDS:
            actual = session.get(field)
            if field in {"generation", "allocation_id", "host_task_id"}:
                actual = int(actual or 0)
            else:
                actual = str(actual or "").strip()
            if actual != normalized_expected[field]:
                raise ValueError(f"session {field} does not match expected identity")

        live_placeholders = ",".join("?" for _ in LEASE_LIVE_STATES)
        live = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM aedt_project_leases
                WHERE session_id = ? AND state IN ({live_placeholders})
                """,
                (int(session_id), *LEASE_LIVE_STATES),
            ).fetchone()[0]
        )
        if live:
            raise ValueError("dead-session reap requires zero live project leases")

        task = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (int(session["host_task_id"]),)
        ).fetchone()
        if not task:
            raise ValueError("session host task no longer exists")
        host_task = dict(task)
        if str(host_task.get("status") or "") not in TASK_TERMINAL_STATES:
            raise ValueError("session host task is not terminal")
        allocation_id = int(session["allocation_id"] or 0)
        if (
            int(host_task.get("requested_allocation_id") or 0) != allocation_id
            or int(host_task.get("allocation_id") or 0) != allocation_id
        ):
            raise ValueError("session host task identity does not match its allocation")
        return session, host_task

    def reap_dead_session(
        self,
        session_id: int,
        *,
        expected_identity: dict[str, Any],
    ) -> dict[str, Any]:
        """Close one proven-dead empty session without touching its allocation.

        The remote PID check deliberately runs outside SQLite's write
        transaction.  The second validation is a compare-and-swap barrier: a
        heartbeat, lease, task transition, or session identity change that
        races the probe makes the operation fail closed.
        """

        checker = self._dead_session_process_checker
        if checker is None:
            raise RuntimeError("dead-session process identity checker is unavailable")
        with self.db.connect() as conn:
            candidate, _host_task = self._validate_dead_session_reap_candidate(
                conn, int(session_id), expected_identity
            )
        snapshot = {
            key: candidate.get(key)
            for key in (
                "id",
                "state",
                "generation",
                "allocation_id",
                "account_name",
                "node_name",
                "host_id",
                "host_task_id",
                "host_process_id",
                "host_slurm_job_id",
                "actual_node_name",
                "endpoint",
                "process_id",
                "last_heartbeat_at",
                "updated_at",
            )
        }
        try:
            processes_absent, probe_evidence = checker(dict(candidate))
        except Exception as exc:
            raise RuntimeError(f"dead-session process identity probe failed: {exc}") from exc
        if processes_absent is not True:
            raise ValueError("session host or AEDT process identity is still present")
        evidence = dict(probe_evidence or {})

        now = _sql_time(self._now())
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current, host_task = self._validate_dead_session_reap_candidate(
                conn, int(session_id), expected_identity
            )
            for field, value in snapshot.items():
                if current.get(field) != value:
                    raise ValueError(
                        f"session changed during process identity probe ({field})"
                    )
            cursor = conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'closed', closed_at = ?, updated_at = ?
                WHERE id = ? AND state = ? AND updated_at = ?
                """,
                (
                    now,
                    now,
                    int(session_id),
                    str(snapshot["state"]),
                    str(snapshot["updated_at"]),
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("session changed during process identity probe")
            audit = {
                "action": "verified_dead_session_reap",
                "allocation_id": int(current["allocation_id"] or 0),
                "generation": int(current["generation"] or 0),
                "host_id": str(current["host_id"] or ""),
                "host_task_id": int(current["host_task_id"] or 0),
                "host_task_status": str(host_task.get("status") or ""),
                "host_process_id": str(current["host_process_id"] or ""),
                "process_id": str(current["process_id"] or ""),
                "process_probe": evidence,
            }
            conn.execute(
                """
                INSERT INTO scheduler_events(
                    kind, message, entity_type, entity_id, account_name
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "aedt_session_reaped",
                    json.dumps(
                        audit,
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "aedt_session",
                    str(int(session_id)),
                    str(current["account_name"] or ""),
                ),
            )

        # Replenish pool capacity only after the verified close commits.
        plan = self.reconcile(execute=True)
        return {
            "session": self.get_session(session_id, include_secret_hash=False),
            "process_probe": evidence,
            "plan": plan,
        }

    def close_session(
        self,
        session_id: int,
        token: str,
        *,
        success: bool,
        failure_message: str = "",
        requeue_siblings: bool = True,
    ) -> dict[str, Any]:
        self._authorize_session(session_id, token)
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session_row = conn.execute(
                "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
            ).fetchone()
            if not session_row:
                raise KeyError(session_id)
            live = conn.execute(
                """
                SELECT COUNT(*) FROM aedt_project_leases
                WHERE session_id = ? AND state IN ('offered','leased','attaching','active','releasing')
                """,
                (int(session_id),),
            ).fetchone()[0]
            if live and success:
                raise ValueError("session still has live project leases")
            exact_failure_message = (
                f"exact-session reservation target {int(session_id)} "
                + (
                    f"failed: {failure_message.strip()}"
                    if failure_message.strip()
                    else (
                        "closed before cohort completion"
                        if success
                        else "failed before cohort completion"
                    )
                )
            )
            self._fail_exact_session_reservations_for_target(
                conn,
                session_id=int(session_id),
                now=now,
                failure_message=exact_failure_message,
            )
            if live:
                if not success and requeue_siblings:
                    expires = _sql_time(
                        self._now() + timedelta(seconds=self.config().lease_ttl_seconds)
                    )
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'queued', session_id = NULL, slot_index = NULL,
                            requested_allocation_id = 0, requested_node_name = '',
                            acquired_at = NULL, release_requested_at = NULL,
                            solve_permit_at = NULL,
                            solve_permit_generation = 0,
                            failure_message = ?, last_heartbeat_at = ?, expires_at = ?,
                            updated_at = ?
                        WHERE session_id = ? AND state IN ('offered','leased','attaching','active','releasing')
                          AND exact_session_reservation_id = 0
                        """,
                        (
                            failure_message.strip() or "AEDT session died; lease requeued",
                            now,
                            expires,
                            now,
                            int(session_id),
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'failed', failure_message = ?, finished_at = ?, updated_at = ?
                        WHERE session_id = ? AND state IN ('offered','leased','attaching','active','releasing')
                        """,
                        (failure_message.strip() or "AEDT session failed", now, now, int(session_id)),
                    )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = ?, failure_message = ?, closed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    "closed" if success else "failed",
                    "" if success else failure_message.strip(),
                    now,
                    now,
                    int(session_id),
                ),
            )
            if not success and int(session_row["allocation_id"] or 0):
                # A process-level AEDT fault taints the whole node allocation
                # for this generation.  Do not start a replacement Desktop on
                # it: drain the allocation, let the host srun exit, then the
                # runtime closes it and places requeued leases on fresh capacity.
                conn.execute(
                    """
                    UPDATE allocations
                    SET state = 'draining',
                        drain_reason = ?,
                        drain_at = COALESCE(drain_at, ?), updated_at = ?
                    WHERE id = ? AND state IN ('warm','active','draining')
                    """,
                    (
                        FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON,
                        now,
                        now,
                        int(session_row["allocation_id"]),
                    ),
                )
        return self.get_session(session_id, include_secret_hash=False)

    def _refresh_session_state(self, conn: Any, session_id: int, now: str) -> None:
        session = conn.execute("SELECT * FROM aedt_sessions WHERE id = ?", (session_id,)).fetchone()
        if not session or session["state"] in {"draining", "unhealthy", "closed", "failed"}:
            return
        live = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM aedt_project_leases
                WHERE session_id = ? AND state IN ('offered','leased','attaching','active','releasing')
                """,
                (session_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            UPDATE aedt_sessions
            SET state = ?, idle_since = ?,
                solve_batch_sealed_at = CASE
                    WHEN ? = 0 THEN NULL ELSE solve_batch_sealed_at END,
                updated_at = ?
            WHERE id = ?
            """,
            (
                "busy" if live else "ready",
                None if live else now,
                live,
                now,
                session_id,
            ),
        )

    def _dedicated_allocations(
        self, states: set[str], *, conn: Any | None = None
    ) -> list[dict[str, Any]]:
        # Dedicated allocation ownership prevents this opt-in pool from
        # silently placing AEDT hosts inside an unrelated production campaign.
        if conn is None:
            rows = self.db.list_allocations_with_live(limit=1000, live_limit=10000)
        elif states:
            ordered_states = sorted(states)
            placeholders = ",".join("?" for _ in ordered_states)
            rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT * FROM allocations WHERE state IN ({placeholders}) "
                    "ORDER BY id DESC LIMIT 10000",
                    tuple(ordered_states),
                ).fetchall()
            ]
        else:
            rows = []
        return [
            row
            for row in rows
            if row.get("state") in states
            and str(row.get("drain_reason") or "").startswith("AEDT pool")
        ]

    def _eligible_allocations(self, *, conn: Any | None = None) -> list[dict[str, Any]]:
        return [
            row
            for row in self._dedicated_allocations(
                {"warm", "active"}, conn=conn
            )
            if str(row.get("node_name") or "").strip()
            # A recycling/rotation reason can briefly coexist with an active
            # allocation while state transitions are reconciled.  Never put a
            # replacement Desktop into that allocation during the gap.
            and str(row.get("drain_reason") or "")
            == "AEDT pool project demand"
        ]

    @staticmethod
    def _allocation_session_capacity(
        allocation: dict[str, Any],
        config: AedtPoolConfig,
        *,
        current_sessions: int = 0,
    ) -> int:
        """Bound host count by both Slurm CPU and memory reservations.

        `current_sessions + floor(free/resource)` handles both already-reserved
        hosts and newly planned `starting` rows whose task reservation has not
        happened yet, without double counting either case.
        """

        slots = config.projects_per_session
        # One control CPU owns the Desktop process; every admitted project then
        # contributes its normal project CPU request on this same allocation.
        session_cpus = max(1, 1 + config.project_cpus * slots)
        session_memory_mb = max(1024, config.project_memory_mb * slots)
        total_cpus = max(0, int(allocation.get("total_cpus") or 0))
        total_memory_mb = max(0, int(allocation.get("total_memory_mb") or 0))
        free_cpus = max(
            0,
            int(
                allocation.get("free_cpus")
                if allocation.get("free_cpus") is not None
                else total_cpus
            ),
        )
        free_memory_mb = max(
            0,
            int(
                allocation.get("free_memory_mb")
                if allocation.get("free_memory_mb") is not None
                else total_memory_mb
            ),
        )
        current = max(0, int(current_sessions))
        cpu_capacity = min(
            total_cpus // session_cpus,
            current + free_cpus // session_cpus,
        )
        memory_capacity = min(
            total_memory_mb // session_memory_mb,
            current + free_memory_mb // session_memory_mb,
        )
        return max(0, min(cpu_capacity, memory_capacity))

    @staticmethod
    def _task_account_fingerprint(task: dict[str, Any]) -> tuple[str, ...]:
        """Identity of the task routing fields consumed by account selection."""

        return tuple(
            str(task.get(field) or "")
            for field in (
                "name",
                "project",
                "requested_account_name",
                "task_account_name",
                "required_capability",
                "env_profile",
            )
        )

    def _preselect_task_accounts(
        self,
    ) -> dict[int, tuple[tuple[str, ...], str]]:
        """Resolve scheduler-owned account choices outside a DB transaction.

        The application callback may refresh Slurm snapshots and storage quota
        state over SSH. It must never run while reconcile owns SQLite's writer
        lock. First copy the bounded task inputs under a read connection, close
        that connection, and only then invoke the callback. ``_plan`` compares
        the routing fingerprint again so a task edit racing the probe falls back
        to its explicit/fallback account instead of using a stale selection.
        """

        selector = self._task_account_selector
        if selector is None:
            return {}
        with self.db.connect() as conn:
            tasks = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT DISTINCT t.id AS task_id, t.name, t.project,
                           t.requested_account_name,
                           t.account_name AS task_account_name,
                           t.required_capability, t.env_profile
                    FROM aedt_project_leases l
                    JOIN tasks t ON t.id = l.task_id
                    LEFT JOIN aedt_sessions s ON s.id = l.session_id
                    LEFT JOIN allocations sa ON sa.id = s.allocation_id
                    LEFT JOIN allocations ra ON ra.id = l.requested_allocation_id
                    WHERE l.state IN (
                        'queued','offered','leased','attaching',
                        'active','releasing'
                    )
                      AND TRIM(COALESCE(sa.account_name, ra.account_name, '')) = ''
                    UNION
                    SELECT DISTINCT t.id AS task_id, t.name, t.project,
                           t.requested_account_name,
                           t.account_name AS task_account_name,
                           t.required_capability, t.env_profile
                    FROM tasks t
                    LEFT JOIN aedt_exact_session_reservations r
                      ON r.task_id = t.id AND r.state IN ('reserved','claimed')
                    LEFT JOIN aedt_sessions s ON s.id = r.session_id
                    LEFT JOIN allocations a ON a.id = s.allocation_id
                    WHERE t.status = 'queued'
                      AND LOWER(TRIM(COALESCE(t.aedt_backend, ''))) = 'pooled'
                      AND NOT EXISTS (
                          SELECT 1 FROM aedt_project_leases l
                          WHERE l.task_id = t.id
                            AND l.state IN (
                                'queued','offered','leased','attaching',
                                'active','releasing'
                            )
                      )
                      AND TRIM(COALESCE(a.account_name, '')) = ''
                    ORDER BY task_id
                    """
                ).fetchall()
            ]

        selections: dict[int, tuple[tuple[str, ...], str]] = {}
        for task in tasks:
            task_id = int(task.get("task_id") or 0)
            if task_id <= 0:
                continue
            requested_value = task.get("requested_account_name")
            if requested_value is None:
                requested_value = task.get("task_account_name")
            requested = [
                item.strip()
                for item in re.split(r"[\s,;/|]+", str(requested_value or ""))
                if item.strip()
            ]
            if len(requested) == 1:
                continue
            try:
                selected = str(selector(dict(task)) or "").strip()
            except Exception:
                LOGGER.exception(
                    "AEDT pooled demand account selection failed for task %s",
                    task_id,
                )
                continue
            if selected:
                selections[task_id] = (
                    self._task_account_fingerprint(task),
                    selected,
                )
        return selections

    def _plan(
        self,
        conn: Any,
        config: AedtPoolConfig,
        *,
        task_account_selections: dict[
            int, tuple[tuple[str, ...], str]
        ] | None = None,
        warm_spare_admission_result: tuple[int, str] | None = None,
        excluded_start_accounts: set[str] | None = None,
        start_block_reasons: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        excluded_start_accounts = {
            str(account or "").strip()
            for account in (excluded_start_accounts or set())
            if str(account or "").strip()
        }
        start_block_reasons = {
            str(account or "").strip(): str(reason or "").strip()
            for account, reason in (start_block_reasons or {}).items()
            if str(account or "").strip()
        }
        state_counts = {
            str(row["state"]): int(row["count"])
            for row in conn.execute(
                "SELECT state, COUNT(*) AS count FROM aedt_sessions GROUP BY state"
            ).fetchall()
        }
        lease_counts = {
            str(row["state"]): int(row["count"])
            for row in conn.execute(
                "SELECT state, COUNT(*) AS count FROM aedt_project_leases GROUP BY state"
            ).fetchall()
        }
        hard_count = sum(state_counts.get(state, 0) for state in SESSION_HARD_CAP_STATES)
        session_heartbeat_cutoff = _sql_time(
            self._now()
            - timedelta(seconds=config.session_heartbeat_timeout_seconds)
        )
        assignable_ready_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state = 'ready'
                  AND a.state IN ('warm','active')
                  AND s.solve_batch_sealed_at IS NULL
                  AND s.drain_requested_at IS NULL
                  AND s.reuse_blocked_at IS NULL
                  AND s.last_heartbeat_at >= ?
                """,
                (session_heartbeat_cutoff,),
            ).fetchone()[0]
        )
        busy_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state = 'busy'
                  AND a.state IN ('warm','active','draining')
                """
            ).fetchone()[0]
        )
        active_session_count = assignable_ready_count + busy_count
        usable_or_starting = active_session_count + state_counts.get("starting", 0)
        # A Desktop can remain healthy while its parent Slurm allocation is
        # already draining.  It is not assignable (placement correctly joins
        # allocations in warm/active), so it must not satisfy the idle-spare
        # target.  Count it as unavailable until the host drains and a
        # replacement starts on an assignable allocation.
        idle_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state = 'ready'
                  AND a.state IN ('warm','active')
                  AND s.solve_batch_sealed_at IS NULL
                  AND s.drain_requested_at IS NULL
                  AND s.reuse_blocked_at IS NULL
                  AND s.last_heartbeat_at >= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM aedt_exact_session_reservations r
                      WHERE r.session_id = s.id
                        AND r.state IN ('reserved','claimed')
                  )
                """,
                (session_heartbeat_cutoff,),
            ).fetchone()[0]
        )
        unassignable_ready_count = max(
            0, state_counts.get("ready", 0) - assignable_ready_count
        )
        unavailable_busy_count = max(
            0, state_counts.get("busy", 0) - busy_count
        )
        unavailable_count = (
            state_counts.get("draining", 0)
            + state_counts.get("unhealthy", 0)
            + unassignable_ready_count
            + unavailable_busy_count
        )
        fallback_account_row = conn.execute(
            """
            SELECT account_name FROM allocations
            WHERE state IN ('pending','warm','active','draining')
              AND TRIM(COALESCE(account_name, '')) != ''
            ORDER BY id LIMIT 1
            """
        ).fetchone()
        fallback_account = str(
            config.account_name
            or (fallback_account_row["account_name"] if fallback_account_row else "")
            or ""
        )
        resolved_task_accounts = task_account_selections or {}

        def planned_account(row: Any) -> str:
            reserved_account = str(row["reserved_account"] or "").strip()
            if reserved_account:
                return reserved_account
            task = dict(row)
            requested_value = task.get("requested_account_name")
            if requested_value is None:
                requested_value = task.get("task_account_name")
            requested = [
                item.strip()
                for item in re.split(r"[\s,;/|]+", str(requested_value or ""))
                if item.strip()
            ]
            if len(requested) == 1:
                return requested[0]
            task_id = int(task.get("task_id") or task.get("id") or 0)
            resolved = resolved_task_accounts.get(task_id)
            if resolved and resolved[0] == self._task_account_fingerprint(task):
                selected = str(resolved[1] or "").strip()
                if selected:
                    return selected
            return requested[0] if requested else fallback_account

        live_project_rows = conn.execute(
            """
            SELECT l.id AS lease_id, l.task_id, l.exclusive_session,
                   l.placement_group, l.workload_family, l.isolation_policy,
                   COALESCE(sa.account_name, ra.account_name, '') AS reserved_account,
                   t.name, t.project, t.requested_account_name,
                   t.account_name AS task_account_name,
                   t.required_capability, t.env_profile
            FROM aedt_project_leases l
            LEFT JOIN aedt_sessions s ON s.id = l.session_id
            LEFT JOIN allocations sa ON sa.id = s.allocation_id
            LEFT JOIN allocations ra ON ra.id = l.requested_allocation_id
            LEFT JOIN tasks t ON t.id = l.task_id
            WHERE l.state IN (
                'queued','offered','leased','attaching','active','releasing'
            )
            ORDER BY l.id
            """
        ).fetchall()
        live_projects = len(live_project_rows)
        queued_backlog_rows = conn.execute(
            """
            SELECT t.id AS task_id, t.name, t.project,
                   t.requested_account_name,
                   t.account_name AS task_account_name,
                   t.required_capability, t.env_profile,
                   COALESCE(r.workload_family, '') AS reserved_family,
                   COALESCE(r.isolation_policy, '') AS reserved_policy,
                   COALESCE(a.account_name, '') AS reserved_account
            FROM tasks t
            LEFT JOIN aedt_exact_session_reservations r
              ON r.task_id = t.id AND r.state IN ('reserved','claimed')
            LEFT JOIN aedt_sessions s ON s.id = r.session_id
            LEFT JOIN allocations a ON a.id = s.allocation_id
            WHERE t.status = 'queued'
              AND LOWER(TRIM(COALESCE(t.aedt_backend, ''))) = 'pooled'
              AND NOT EXISTS (
                  SELECT 1 FROM aedt_project_leases l
                  WHERE l.task_id = t.id
                    AND l.state IN (
                        'queued','offered','leased','attaching','active','releasing'
                    )
              )
            ORDER BY t.id
            """
        ).fetchall()
        queued_pooled_task_backlog = len(queued_backlog_rows)
        demand_entries: list[tuple[str, str, bool]] = []
        for row in live_project_rows:
            family = str(
                row["workload_family"] or row["placement_group"] or ""
            ).strip().lower()
            if not family:
                family = f"__legacy_lease_{int(row['lease_id'])}"
            demand_entries.append(
                (
                    planned_account(row),
                    family,
                    bool(row["exclusive_session"])
                    or str(row["isolation_policy"] or "") == "exclusive",
                )
            )
        for row in queued_backlog_rows:
            family = str(row["reserved_family"] or "").strip().lower()
            if not family:
                family = canonical_workload_family(
                    "", str(row["project"] or row["name"] or "")
                )
            demand_entries.append(
                (
                    planned_account(row),
                    family,
                    str(row["reserved_policy"] or "") == "exclusive",
                )
            )
        demand_entries = demand_entries[: config.target_projects]
        desired_projects = len(demand_entries)
        desired_exclusive = sum(1 for _account, _family, exclusive in demand_entries if exclusive)
        shared_counts: dict[tuple[str, str], int] = {}
        queued_pooled_task_backlog_by_account: dict[str, int] = {}
        for row in queued_backlog_rows:
            account = planned_account(row)
            queued_pooled_task_backlog_by_account[account] = (
                queued_pooled_task_backlog_by_account.get(account, 0) + 1
            )
        group_demand: dict[tuple[str, str], int] = {}
        exclusive_counts_by_account: dict[str, int] = {}
        for account, family, exclusive in demand_entries:
            if exclusive:
                exclusive_counts_by_account[account] = (
                    exclusive_counts_by_account.get(account, 0) + 1
                )
            else:
                key = (account, family)
                shared_counts[key] = shared_counts.get(key, 0) + 1
        for account, count in exclusive_counts_by_account.items():
            group_demand[(account, "__exclusive__")] = count
        for (account, family), count in sorted(shared_counts.items()):
            group_demand[(account, f"family:{family}")] = math.ceil(
                count / config.projects_per_session
            )

        bound_contracts: dict[tuple[int, str], dict[str, Any]] = {}
        for row in conn.execute(
            """
            SELECT s.id AS session_id, a.account_name,
                   l.workload_family, l.placement_group,
                   l.isolation_policy, l.exclusive_session
            FROM aedt_sessions s
            JOIN allocations a ON a.id = s.allocation_id
            JOIN aedt_project_leases l ON l.session_id = s.id
              AND l.state IN ('offered','leased','attaching','active','releasing')
            WHERE (
                s.state = 'busy'
                AND a.state IN ('warm','active','draining')
            ) OR (
                s.state = 'ready'
                AND a.state IN ('warm','active')
                AND s.solve_batch_sealed_at IS NULL
                AND s.drain_requested_at IS NULL
                AND s.reuse_blocked_at IS NULL
                AND s.last_heartbeat_at >= ?
            )
            """,
            (session_heartbeat_cutoff,),
        ).fetchall():
            key = (int(row["session_id"]), str(row["account_name"] or ""))
            contract = bound_contracts.setdefault(
                key,
                {"families": set(), "policies": set(), "exclusive": False},
            )
            family = str(
                row["workload_family"] or row["placement_group"] or ""
            ).strip().lower()
            if family:
                contract["families"].add(family)
            contract["policies"].add(
                str(row["isolation_policy"] or "family")
            )
            contract["exclusive"] = bool(
                contract["exclusive"] or row["exclusive_session"]
            )
        for row in conn.execute(
            """
            SELECT s.id AS session_id, a.account_name,
                   r.workload_family, r.isolation_policy
            FROM aedt_sessions s
            JOIN allocations a ON a.id = s.allocation_id
            JOIN aedt_exact_session_reservations r ON r.session_id = s.id
              AND r.state IN ('reserved','claimed')
            WHERE s.state = 'ready'
              AND a.state IN ('warm','active')
              AND s.solve_batch_sealed_at IS NULL
              AND s.drain_requested_at IS NULL
              AND s.reuse_blocked_at IS NULL
              AND s.last_heartbeat_at >= ?
            """,
            (session_heartbeat_cutoff,),
        ).fetchall():
            key = (int(row["session_id"]), str(row["account_name"] or ""))
            contract = bound_contracts.setdefault(
                key,
                {"families": set(), "policies": set(), "exclusive": False},
            )
            family = str(row["workload_family"] or "").strip().lower()
            if family:
                contract["families"].add(family)
            policy = str(row["isolation_policy"] or "").strip().lower()
            if policy:
                contract["policies"].add(policy)
            contract["exclusive"] = bool(
                contract["exclusive"] or policy == "exclusive"
            )
        bound_sessions_by_group: dict[tuple[str, str], int] = {}
        bound_sessions_by_account: dict[str, int] = {}
        for (session_id, account), contract in bound_contracts.items():
            if contract["exclusive"]:
                group = "__exclusive__"
            elif (
                contract["policies"] == {"family"}
                and len(contract["families"]) == 1
            ):
                group = f"family:{next(iter(contract['families']))}"
            else:
                group = f"__bound_session_{session_id}"
            key = (account, group)
            bound_sessions_by_group[key] = bound_sessions_by_group.get(key, 0) + 1
            bound_sessions_by_account[account] = (
                bound_sessions_by_account.get(account, 0) + 1
            )
        required_sessions_by_group = {
            key: max(group_demand.get(key, 0), bound_sessions_by_group.get(key, 0))
            for key in set(group_demand) | set(bound_sessions_by_group)
        }
        demand_sessions_by_account: dict[str, int] = {}
        for (account, _group), count in required_sessions_by_group.items():
            demand_sessions_by_account[account] = (
                demand_sessions_by_account.get(account, 0) + count
            )
        remaining_session_ceiling = config.max_sessions
        capped_demand_by_account: dict[str, int] = {}
        for account, count in sorted(demand_sessions_by_account.items()):
            admitted = min(max(0, int(count)), remaining_session_ceiling)
            if admitted:
                capped_demand_by_account[account] = admitted
            remaining_session_ceiling -= admitted
            if remaining_session_ceiling <= 0:
                break
        demand_sessions_by_account = capped_demand_by_account
        demand_sessions = sum(demand_sessions_by_account.values())
        desired_sessions = min(
            config.max_sessions,
            demand_sessions + config.min_idle_sessions,
        )
        active_sessions_by_account = {
            str(row["account_name"] or ""): int(row["count"] or 0)
            for row in conn.execute(
                """
                SELECT account_name, COUNT(*) AS count FROM (
                    SELECT a.account_name, s.id
                    FROM aedt_sessions s
                    JOIN allocations a ON a.id = s.allocation_id
                    WHERE s.state = 'ready'
                      AND a.state IN ('warm','active')
                      AND s.solve_batch_sealed_at IS NULL
                      AND s.drain_requested_at IS NULL
                      AND s.reuse_blocked_at IS NULL
                      AND s.last_heartbeat_at >= ?
                    UNION ALL
                    SELECT a.account_name, s.id
                    FROM aedt_sessions s
                    JOIN allocations a ON a.id = s.allocation_id
                    WHERE s.state = 'busy'
                      AND a.state IN ('warm','active','draining')
                )
                GROUP BY account_name
                """,
                (session_heartbeat_cutoff,),
            ).fetchall()
        }
        starting_sessions_by_account = {
            str(row["account_name"] or ""): int(row["count"] or 0)
            for row in conn.execute(
                """
                SELECT a.account_name, COUNT(*) AS count
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state = 'starting'
                  AND a.state IN ('warm','active')
                GROUP BY a.account_name
                """
            ).fetchall()
        }
        usable_sessions_by_account = {
            account: active_sessions_by_account.get(account, 0)
            + starting_sessions_by_account.get(account, 0)
            for account in set(active_sessions_by_account)
            | set(starting_sessions_by_account)
            | set(demand_sessions_by_account)
        }
        usable_or_starting = sum(usable_sessions_by_account.values())
        unsatisfied_group_sessions_by_account: dict[str, int] = {}
        for (account, group), required in required_sessions_by_group.items():
            unsatisfied_group_sessions_by_account[account] = (
                unsatisfied_group_sessions_by_account.get(account, 0)
                + max(0, required - bound_sessions_by_group.get((account, group), 0))
            )
        flexible_sessions_by_account = {
            account: max(
                0,
                active_sessions_by_account.get(account, 0)
                - bound_sessions_by_account.get(account, 0),
            )
            + starting_sessions_by_account.get(account, 0)
            for account in set(usable_sessions_by_account)
            | set(unsatisfied_group_sessions_by_account)
        }
        demand_start_needed_by_account = {
            account: max(
                0,
                count - flexible_sessions_by_account.get(account, 0),
            )
            for account, count in unsatisfied_group_sessions_by_account.items()
        }
        demand_start_needed_by_account = {
            account: count
            for account, count in demand_start_needed_by_account.items()
            if count > 0
        }
        demand_start_needed = sum(demand_start_needed_by_account.values())
        spare_capacity_after_demand = sum(
            max(
                0,
                flexible_sessions_by_account.get(account, 0)
                - unsatisfied_group_sessions_by_account.get(account, 0),
            )
            for account in flexible_sessions_by_account
        )
        spare_target = max(0, desired_sessions - demand_sessions)
        warm_spare_start_needed = max(
            0, spare_target - spare_capacity_after_demand
        )
        unclaimed_starting_sessions = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM aedt_sessions s
                WHERE s.state = 'starting'
                  AND NOT EXISTS (
                      SELECT 1 FROM tasks t
                      WHERE t.dedupe_key = 'aedt-session-host:' || s.id
                        AND t.status IN ('attaching','running')
                  )
                """
            ).fetchone()[0]
        )
        warm_spare_admission_requested_sessions = (
            unclaimed_starting_sessions
            + demand_start_needed
            + warm_spare_start_needed
        )
        warm_spare_starts_authorized = warm_spare_start_needed
        warm_spare_status_reason = ""
        if warm_spare_start_needed and not config.operational:
            warm_spare_starts_authorized = 0
            warm_spare_status_reason = "AEDT pool is not operational; warm-spare start is gated"
        elif warm_spare_start_needed and self._warm_spare_admission_checker:
            if warm_spare_admission_result is None:
                allowed_total = 0
                warm_spare_status_reason = (
                    "warm-spare license admission was not resolved outside "
                    "the planning transaction"
                )
            else:
                allowed_total, warm_spare_status_reason = warm_spare_admission_result
            warm_spare_starts_authorized = max(
                0,
                min(
                    warm_spare_start_needed,
                    int(allowed_total)
                    - unclaimed_starting_sessions
                    - demand_start_needed,
                ),
            )
            if (
                warm_spare_starts_authorized < warm_spare_start_needed
                and not warm_spare_status_reason
            ):
                warm_spare_status_reason = (
                    "license admission headroom is insufficient for "
                    f"{warm_spare_start_needed} warm-spare session(s)"
                )
        start_needed_by_account = dict(demand_start_needed_by_account)
        if warm_spare_starts_authorized:
            spare_accounts = sorted(demand_sessions_by_account)
            if not spare_accounts:
                spare_accounts = [fallback_account]
            for index in range(warm_spare_starts_authorized):
                spare_account = spare_accounts[index % len(spare_accounts)]
                start_needed_by_account[spare_account] = (
                    start_needed_by_account.get(spare_account, 0) + 1
                )
        start_needed = sum(start_needed_by_account.values())
        warm_spare_deficit = max(0, config.min_idle_sessions - idle_count)
        cap_excess = max(0, usable_or_starting - config.max_sessions)
        demand_excess = max(0, usable_or_starting - desired_sessions)
        idle_ready_by_account = {
            str(row["account_name"] or ""): int(row["count"] or 0)
            for row in conn.execute(
                """
                SELECT a.account_name, COUNT(*) AS count
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state = 'ready'
                  AND a.state IN ('warm','active')
                  AND s.solve_batch_sealed_at IS NULL
                  AND s.drain_requested_at IS NULL
                  AND s.reuse_blocked_at IS NULL
                  AND s.last_heartbeat_at >= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM aedt_exact_session_reservations r
                      WHERE r.session_id = s.id
                        AND r.state IN ('reserved','claimed')
                  )
                GROUP BY a.account_name
                """,
                (session_heartbeat_cutoff,),
            ).fetchall()
        }
        flexible_surplus_by_account = {
            account: min(
                idle_ready_by_account.get(account, 0),
                max(
                    0,
                    flexible_sessions_by_account.get(account, 0)
                    - unsatisfied_group_sessions_by_account.get(account, 0),
                ),
            )
            for account in flexible_sessions_by_account
        }
        spare_to_keep_by_account: dict[str, int] = {}
        remaining_spares_to_keep = spare_target
        keep_order = list(sorted(demand_sessions_by_account)) + [
            account
            for account in sorted(flexible_surplus_by_account)
            if account not in demand_sessions_by_account
        ]
        while remaining_spares_to_keep > 0 and any(
            flexible_surplus_by_account.get(account, 0)
            > spare_to_keep_by_account.get(account, 0)
            for account in keep_order
        ):
            for account in keep_order:
                if (
                    flexible_surplus_by_account.get(account, 0)
                    <= spare_to_keep_by_account.get(account, 0)
                ):
                    continue
                spare_to_keep_by_account[account] = (
                    spare_to_keep_by_account.get(account, 0) + 1
                )
                remaining_spares_to_keep -= 1
                if remaining_spares_to_keep <= 0:
                    break
        rebalance_drains_by_account = {
            account: surplus - spare_to_keep_by_account.get(account, 0)
            for account, surplus in flexible_surplus_by_account.items()
            if surplus - spare_to_keep_by_account.get(account, 0) > 0
        }
        if not demand_start_needed_by_account:
            # Ordinary scale-in still observes idle_ttl. Immediate drains are
            # only for freeing wrong-account capacity needed by queued work.
            rebalance_drains_by_account = {}
        planned_rebalance_drains = sum(rebalance_drains_by_account.values())
        dead_parent_counted_session_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state IN ('starting','ready','busy')
                  AND a.state IN ('closed','failed')
                """
            ).fetchone()[0]
        )
        # Counted rows whose parent allocation is conclusively gone cannot
        # consume usable capacity either. Draining/unhealthy rows are already
        # excluded from hard_count regardless of their parent lifecycle.
        effective_hard_count = max(
            0, hard_count - dead_parent_counted_session_count
        )
        global_start_budget = max(0, config.max_sessions - effective_hard_count)
        blocked_start_needed_by_account = {
            account: count
            for account, count in start_needed_by_account.items()
            if account in excluded_start_accounts and count > 0
        }
        if excluded_start_accounts:
            start_needed_by_account = {
                account: count
                for account, count in start_needed_by_account.items()
                if account not in excluded_start_accounts and count > 0
            }
            start_needed = sum(start_needed_by_account.values())
        if start_needed > global_start_budget:
            remaining = dict(start_needed_by_account)
            limited: dict[str, int] = {}
            order = sorted(remaining)
            while sum(limited.values()) < global_start_budget and any(
                remaining.values()
            ):
                for account in order:
                    if remaining.get(account, 0) <= 0:
                        continue
                    limited[account] = limited.get(account, 0) + 1
                    remaining[account] -= 1
                    if sum(limited.values()) >= global_start_budget:
                        break
            start_needed_by_account = limited
            start_needed = sum(start_needed_by_account.values())
        idle_cutoff = _sql_time(self._now() - timedelta(seconds=config.idle_ttl_seconds))
        idle_drainable = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state = 'ready'
                  AND a.state IN ('warm','active')
                  AND s.solve_batch_sealed_at IS NULL
                  AND s.drain_requested_at IS NULL
                  AND COALESCE(s.idle_since, s.created_at) <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM aedt_exact_session_reservations r
                      WHERE r.session_id = s.id
                        AND r.state IN ('reserved','claimed')
                  )
                """,
                (idle_cutoff,),
            ).fetchone()[0]
        )
        unsafe_allocation_ids = {
            int(row["allocation_id"])
            for row in conn.execute(
                """
                SELECT DISTINCT allocation_id
                FROM aedt_sessions
                WHERE state IN ('starting','ready','busy','draining','unhealthy')
                  AND (
                      state IN ('draining','unhealthy')
                      OR drain_requested_at IS NOT NULL
                      OR TRIM(COALESCE(quarantine_reason, '')) != ''
                  )
                """
            ).fetchall()
        }
        allocations = [
            allocation
            for allocation in self._eligible_allocations(conn=conn)
            if str(allocation.get("account_name") or "")
            not in excluded_start_accounts
            and int(allocation["id"]) not in unsafe_allocation_ids
        ]
        pending_allocations = [
            allocation
            for allocation in self._dedicated_allocations({"pending"}, conn=conn)
            if str(allocation.get("drain_reason") or "")
            == "AEDT pool project demand"
            and str(allocation.get("account_name") or "")
            not in excluded_start_accounts
        ]
        current_by_allocation = {
            int(row["allocation_id"]): int(row["count"])
            for row in conn.execute(
                """
                SELECT allocation_id, COUNT(*) AS count FROM aedt_sessions
                WHERE state IN ('starting','ready','busy','draining','unhealthy')
                GROUP BY allocation_id
                """
            ).fetchall()
        }
        placements: list[dict[str, Any]] = []
        remaining_starts_by_account = dict(start_needed_by_account)
        for allocation in sorted(
            allocations,
            key=lambda row: (current_by_allocation.get(int(row["id"]), 0), int(row["id"])),
        ):
            account_name = str(allocation.get("account_name") or "")
            account_need = remaining_starts_by_account.get(account_name, 0)
            if account_need <= 0:
                continue
            allocation_id = int(allocation["id"])
            current_sessions = current_by_allocation.get(allocation_id, 0)
            capacity = self._allocation_session_capacity(
                allocation, config, current_sessions=current_sessions
            )
            free = max(0, capacity - current_sessions)
            granted = min(free, account_need)
            for _ in range(granted):
                placements.append(
                    {
                        "allocation_id": allocation_id,
                        "account_name": account_name,
                        "node_name": str(allocation.get("node_name") or ""),
                    }
                )
            remaining_starts_by_account[account_name] = account_need - granted
            if not any(remaining_starts_by_account.values()):
                break
        unplaced_by_account = {
            account: count
            for account, count in remaining_starts_by_account.items()
            if count > 0
        }
        unplaced = sum(unplaced_by_account.values())
        # Pending dedicated nodes already consume a Slurm request/account slot.
        # Count their future session capacity so every runtime tick does not
        # request another batch while Slurm is still queueing the first one.
        pending_capacity_by_account: dict[str, int] = {}
        for allocation in pending_allocations:
            account_name = str(allocation.get("account_name") or "")
            pending_capacity_by_account[account_name] = (
                pending_capacity_by_account.get(account_name, 0)
                + self._allocation_session_capacity(allocation, config)
            )
        pending_capacity = sum(pending_capacity_by_account.values())
        unplaced_after_pending_by_account = {
            account: max(
                0,
                count - pending_capacity_by_account.get(account, 0),
            )
            for account, count in unplaced_by_account.items()
        }
        unplaced_after_pending = sum(unplaced_after_pending_by_account.values())
        shape_cpus = max(
            [
                int(row.get("total_cpus") or 0)
                for row in [*allocations, *pending_allocations]
            ]
            + [64]
        )
        shape_memory_mb = max(
            [
                int(row.get("total_memory_mb") or 0)
                for row in [*allocations, *pending_allocations]
            ]
            + [512 * 1024]
        )
        sessions_per_new_node = max(
            1,
            self._allocation_session_capacity(
                {
                    "total_cpus": shape_cpus,
                    "free_cpus": shape_cpus,
                    "total_memory_mb": shape_memory_mb,
                    "free_memory_mb": shape_memory_mb,
                },
                config,
            ),
        )
        node_requests_by_account = {
            account: math.ceil(count / sessions_per_new_node)
            for account, count in unplaced_after_pending_by_account.items()
            if count > 0
        }
        node_requests = sum(node_requests_by_account.values())
        if warm_spare_deficit and not warm_spare_status_reason:
            if demand_sessions + config.min_idle_sessions > config.max_sessions:
                warm_spare_status_reason = (
                    "session ceiling leaves insufficient capacity for the idle-session target"
                )
            elif state_counts.get("starting", 0):
                warm_spare_status_reason = "warm-spare session startup is in progress"
            elif warm_spare_starts_authorized and len(placements) > demand_start_needed:
                warm_spare_status_reason = "warm-spare session start is planned on a live allocation"
            elif warm_spare_starts_authorized and (node_requests or pending_capacity):
                warm_spare_status_reason = "waiting for AEDT pool allocation capacity"
            elif usable_or_starting >= desired_sessions:
                warm_spare_status_reason = (
                    "counted non-idle sessions currently consume the warm-spare capacity"
                )
        return {
            "hard_session_count": hard_count,
            "active_session_count": active_session_count,
            "starting_session_count": state_counts.get("starting", 0),
            "draining_session_count": state_counts.get("draining", 0),
            "unhealthy_session_count": state_counts.get("unhealthy", 0),
            "active_project_capacity": (
                active_session_count * config.projects_per_session
            ),
            "usable_or_starting_session_count": usable_or_starting,
            "desired_sessions": desired_sessions,
            "demand_sessions": demand_sessions,
            "start_needed": start_needed,
            "idle_session_count": idle_count,
            "unassignable_ready_session_count": unassignable_ready_count,
            "unavailable_busy_session_count": unavailable_busy_count,
            "unavailable_session_count": unavailable_count,
            "min_idle_aedt_sessions": config.min_idle_sessions,
            "warm_spare_deficit": warm_spare_deficit,
            "warm_spare_start_needed": warm_spare_start_needed,
            "warm_spare_starts_authorized": warm_spare_starts_authorized,
            "warm_spare_admission_requested_sessions": (
                warm_spare_admission_requested_sessions
            ),
            "unclaimed_starting_sessions": unclaimed_starting_sessions,
            "warm_spare_status_reason": warm_spare_status_reason,
            "drain_needed": max(
                0,
                max(cap_excess, min(demand_excess, idle_drainable))
                - planned_rebalance_drains,
            ),
            "rebalance_drains_by_account": rebalance_drains_by_account,
            "idle_drainable_sessions": idle_drainable,
            "placements": placements,
            "unplaced_sessions": unplaced,
            "unplaced_sessions_by_account": unplaced_by_account,
            "pending_node_session_capacity": pending_capacity,
            "pending_node_session_capacity_by_account": pending_capacity_by_account,
            "node_requests": node_requests,
            "node_requests_by_account": node_requests_by_account,
            "sessions_per_new_node": sessions_per_new_node,
            "state_counts": state_counts,
            "lease_counts": lease_counts,
            "live_projects": live_projects,
            "queued_pooled_task_backlog": queued_pooled_task_backlog,
            "queued_pooled_task_backlog_by_account": queued_pooled_task_backlog_by_account,
            "desired_projects": desired_projects,
            "exclusive_projects": desired_exclusive,
            "demand_sessions_by_account": demand_sessions_by_account,
            "active_sessions_by_account": active_sessions_by_account,
            "starting_sessions_by_account": starting_sessions_by_account,
            "start_needed_by_account": start_needed_by_account,
            "blocked_start_needed": sum(
                blocked_start_needed_by_account.values()
            ),
            "blocked_start_needed_by_account": blocked_start_needed_by_account,
            "start_block_reasons_by_account": {
                account: start_block_reasons.get(
                    account,
                    "AEDT session starts are temporarily blocked for this account",
                )
                for account in sorted(excluded_start_accounts)
            },
        }

    def _precheck_warm_spare_admission(
        self,
        config: AedtPoolConfig,
        *,
        task_account_selections: dict[int, tuple[tuple[str, ...], str]],
        excluded_start_accounts: set[str] | None = None,
        start_block_reasons: dict[str, str] | None = None,
    ) -> tuple[int, str] | None:
        """Run the scheduler admission callback with no DB transaction open."""

        checker = self._warm_spare_admission_checker
        if checker is None or not config.operational:
            return None
        # Compute the exact requested total from a read-only snapshot. The
        # placeholder result prevents _plan from authorizing a speculative
        # warm spare before the external admission callback has run.
        with self.db.connect() as conn:
            preview = self._plan(
                conn,
                config,
                task_account_selections=task_account_selections,
                warm_spare_admission_result=(
                    0,
                    "warm-spare license admission preflight is pending",
                ),
                excluded_start_accounts=excluded_start_accounts,
                start_block_reasons=start_block_reasons,
            )
        requested_sessions = max(
            0,
            int(preview.get("warm_spare_admission_requested_sessions") or 0),
        )
        if requested_sessions <= 0:
            return (0, "")
        try:
            allowed, reason = checker(requested_sessions)
        except Exception as exc:
            LOGGER.exception("AEDT warm-spare license admission failed")
            return (0, f"license admission check failed: {exc}")
        return (max(0, int(allowed)), str(reason or "").strip())

    def dry_run(
        self,
        *,
        excluded_start_accounts: set[str] | None = None,
        start_block_reasons: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        config = self.config()
        task_account_selections = self._preselect_task_accounts()
        warm_spare_admission_result = self._precheck_warm_spare_admission(
            config,
            task_account_selections=task_account_selections,
            excluded_start_accounts=excluded_start_accounts,
            start_block_reasons=start_block_reasons,
        )
        with self.db.connect() as conn:
            return self._plan(
                conn,
                config,
                task_account_selections=task_account_selections,
                warm_spare_admission_result=warm_spare_admission_result,
                excluded_start_accounts=excluded_start_accounts,
                start_block_reasons=start_block_reasons,
            )

    @staticmethod
    def _lease_fits_session(
        lease: Any,
        session: Any,
        occupants: list[Any],
        *,
        reserved_slots_for_others: int = 0,
        pending_reservations: list[Any] | None = None,
    ) -> bool:
        pending_reservations = pending_reservations or []
        if (
            int(session["used_slots"] or 0) + int(reserved_slots_for_others)
            >= int(session["slots_total"])
        ):
            return False
        if "reuse_blocked_at" in session.keys() and session["reuse_blocked_at"]:
            return False
        if any(bool(item["exclusive_session"]) for item in occupants):
            return False
        if bool(lease["exclusive_session"]) and occupants:
            return False

        requested_session_id = int(lease["requested_session_id"] or 0)
        if requested_session_id:
            if int(session["id"]) != requested_session_id:
                return False
            if int(session["generation"] or 0) != int(
                lease["requested_session_generation"] or 0
            ):
                return False

        canary_admission_id = int(lease["mixed_canary_admission_id"] or 0)
        canary_session_id = int(lease["mixed_canary_session_id"] or 0)
        if canary_session_id and int(session["id"]) != canary_session_id:
            return False
        if canary_admission_id:
            if any(
                int(item["mixed_canary_admission_id"] or 0)
                != canary_admission_id
                for item in occupants
            ):
                return False
        elif any(int(item["mixed_canary_admission_id"] or 0) for item in occupants):
            # A normal family/shared request cannot steal a slot reserved by a
            # bootstrap-issued mixed canary.
            return False

        requested_profile = str(lease["session_profile"] or "")
        fixed_profile = str(session["session_profile"] or "")
        protocol_version = int(lease["protocol_version"] or 1)
        if protocol_version >= 2:
            if fixed_profile != requested_profile:
                return False
            if any(
                int(item["protocol_version"] or 1) < 2
                or str(item["session_profile"] or "") != requested_profile
                for item in occupants
            ):
                return False

        if protocol_version < 2:
            if pending_reservations or any(
                int(item["protocol_version"] or 1) >= 2 for item in occupants
            ):
                return False
            group = str(lease["placement_group"] or "")
            return all(str(item["placement_group"] or "") == group for item in occupants)

        policy = str(lease["isolation_policy"] or "family")
        family = str(lease["workload_family"] or "")
        if any(
            not str(item["workload_family"] or "")
            or not str(item["isolation_policy"] or "")
            or str(item["session_profile"] or "") != requested_profile
            for item in pending_reservations
        ):
            return False
        if policy == "family":
            return all(
                str(item["isolation_policy"] or "family") == "family"
                and str(item["workload_family"] or "") == family
                for item in occupants
            ) and all(
                str(item["isolation_policy"] or "") == "family"
                and str(item["workload_family"] or "") == family
                for item in pending_reservations
            )
        if policy == "shared_if_compatible":
            return all(
                int(item["protocol_version"] or 1) >= 2
                and str(item["isolation_policy"] or "family")
                == "shared_if_compatible"
                for item in occupants
            ) and all(
                str(item["isolation_policy"] or "")
                == "shared_if_compatible"
                for item in pending_reservations
            )
        return not occupants and not pending_reservations

    def _place_queued_leases(
        self,
        conn: Any,
        now: str,
        *,
        lease_ids: tuple[int, ...] | None = None,
        config: AedtPoolConfig | None = None,
        refresh_reservations: bool = True,
    ) -> int:
        """Place only admission-ready leases in one short existing transaction."""

        config = config or self.config()
        if not config.operational:
            return 0
        self._refresh_mixed_canary_admissions(conn, now)
        if refresh_reservations:
            self._refresh_exact_session_reservations(conn, now)
        ready_cutoff = _sql_time(
            _parse_utc_time(now) - timedelta(seconds=config.queued_stale_seconds)
        )
        params: tuple[Any, ...] = (ready_cutoff,)
        predicate = ""
        order_by = "ORDER BY id ASC"
        if lease_ids:
            placeholders = ",".join("?" for _ in lease_ids)
            predicate = f" AND id IN ({placeholders})"
            params = (ready_cutoff, *(int(item) for item in lease_ids))
        else:
            # Generic reconcile/register placement is deliberately bounded.
            # Continue after the preceding batch (wrapping to the oldest id)
            # so pinned or incompatible requests at the head cannot starve
            # later compatible leases forever.
            order_by = "ORDER BY CASE WHEN id > ? THEN 0 ELSE 1 END, id ASC"
            params = (ready_cutoff, int(self._placement_cursor))
        placement_limit = (
            max(1, len(lease_ids))
            if lease_ids
            else RECONCILE_PLACEMENT_BATCH_SIZE
        )
        params = (*params, placement_limit)
        queued = conn.execute(
            "SELECT * FROM aedt_project_leases "
            "WHERE state = 'queued' "
            "AND (protocol_version < 2 OR last_heartbeat_at >= ?) "
            f"{predicate} {order_by} LIMIT ?",
            params,
        ).fetchall()
        if not lease_ids:
            self._placement_cursor = int(queued[-1]["id"]) if queued else 0
        placed = 0
        offer_expires = _sql_time(
            self._now() + timedelta(seconds=config.offer_ack_seconds)
        )
        allocation_age_cutoff = (
            _sql_time(
                self._now()
                - timedelta(seconds=config.allocation_max_age_seconds)
            )
            if config.allocation_max_age_seconds
            else ""
        )
        session_heartbeat_cutoff = _sql_time(
            self._now()
            - timedelta(seconds=config.session_heartbeat_timeout_seconds)
        )
        for lease in queued:
            if str(lease["client_deadline_at"] or "") <= now:
                continue
            task_id = int(lease["task_id"] or 0)
            if task_id:
                task = conn.execute(
                    "SELECT status FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if task and str(task["status"]) in {
                    TaskStatus.COMPLETED.value,
                    TaskStatus.FAILED.value,
                    TaskStatus.CANCELLED.value,
                }:
                    continue
            exact_reservation_id = int(
                lease["exact_session_reservation_id"] or 0
            )
            if config.target_projects <= 0 or self._admitted_project_count(
                conn,
                exclude_task_id=task_id,
                exclude_reservation_id=exact_reservation_id,
                exclude_lease_id=int(lease["id"]),
            ) >= int(config.target_projects):
                continue
            sessions = conn.execute(
                """
                SELECT s.*,
                       (SELECT COUNT(*) FROM aedt_project_leases l
                        WHERE l.session_id = s.id
                          AND l.state IN (
                              'offered','leased','attaching','active','releasing'
                          )) AS used_slots
                FROM aedt_sessions s
                JOIN allocations a ON a.id = s.allocation_id
                WHERE s.state IN ('ready','busy')
                  AND s.last_heartbeat_at >= ?
                  AND s.solve_batch_sealed_at IS NULL
                  AND s.reuse_blocked_at IS NULL
                  AND a.state IN ('warm','active')
                  AND (? = '' OR a.created_at > ?)
                  AND (? = 0 OR s.allocation_id = ?)
                  AND (? = '' OR s.node_name = ?)
                  AND (
                      ? = 0
                      OR (s.id = ? AND s.generation = ?)
                  )
                  AND (? = 0 OR s.id = ?)
                  AND (
                      ? > 0 OR NOT EXISTS (
                          SELECT 1 FROM aedt_mixed_canary_admissions ca
                          WHERE ca.session_id = s.id
                            AND ca.state IN ('open','filled','aborting')
                      )
                  )
                  AND (
                      ? < 2
                      OR (? >= 2 AND s.session_profile = ?)
                  )
                ORDER BY used_slots DESC, s.id ASC
                """,
                (
                    session_heartbeat_cutoff,
                    allocation_age_cutoff,
                    allocation_age_cutoff,
                    int(lease["requested_allocation_id"] or 0),
                    int(lease["requested_allocation_id"] or 0),
                    str(lease["requested_node_name"] or ""),
                    str(lease["requested_node_name"] or ""),
                    int(lease["requested_session_id"] or 0),
                    int(lease["requested_session_id"] or 0),
                    int(lease["requested_session_generation"] or 0),
                    int(lease["mixed_canary_session_id"] or 0),
                    int(lease["mixed_canary_session_id"] or 0),
                    int(lease["mixed_canary_session_id"] or 0),
                    int(lease["protocol_version"] or 1),
                    int(lease["protocol_version"] or 1),
                    str(lease["session_profile"] or ""),
                ),
            ).fetchall()
            selected = None
            selected_occupants: list[Any] = []
            for session in sessions:
                occupants = conn.execute(
                    """
                    SELECT * FROM aedt_project_leases
                    WHERE session_id = ?
                      AND state IN (
                          'offered','leased','attaching','active','releasing'
                      )
                    ORDER BY slot_index ASC
                    """,
                    (int(session["id"]),),
                ).fetchall()
                exact_reservation_id = int(
                    lease["exact_session_reservation_id"] or 0
                )
                exact_reservation = None
                if exact_reservation_id:
                    exact_reservation = conn.execute(
                        """
                        SELECT state, lease_id, reservation_key
                        FROM aedt_exact_session_reservations WHERE id = ?
                        """,
                        (exact_reservation_id,),
                    ).fetchone()
                    if not (
                        exact_reservation
                        and str(exact_reservation["state"]) == "claimed"
                        and int(exact_reservation["lease_id"] or 0)
                        == int(lease["id"])
                    ):
                        continue
                pending_reservations = list(
                    conn.execute(
                        """
                        SELECT * FROM aedt_exact_session_reservations
                        WHERE session_id = ?
                          AND state IN ('reserved','claimed')
                          AND (? = 0 OR id != ?)
                        ORDER BY id
                        """,
                        (
                            int(session["id"]),
                            exact_reservation_id,
                            exact_reservation_id,
                        ),
                    ).fetchall()
                )
                reserved_slots_for_others = len(pending_reservations)
                exact_reservation_key = (
                    str(exact_reservation["reservation_key"])
                    if exact_reservation_id and exact_reservation
                    else ""
                )
                # Legacy bootstrap exact cohorts intentionally have no family
                # metadata.  Their own sibling reservations are authoritative
                # pins to the same session, so ignore them only for the
                # compatibility comparison while still reserving every slot.
                # A generic or different-cohort lease continues to fail closed
                # on those unknown reservations.
                compatibility_reservations = [
                    item
                    for item in pending_reservations
                    if not exact_reservation_key
                    or str(item["reservation_key"])
                    != exact_reservation_key
                ]
                if self._lease_fits_session(
                    lease,
                    session,
                    occupants,
                    reserved_slots_for_others=reserved_slots_for_others,
                    pending_reservations=compatibility_reservations,
                ):
                    selected = session
                    selected_occupants = list(occupants)
                    break
            if selected is None:
                continue
            occupied = {int(row["slot_index"]) for row in selected_occupants}
            slot_index = next(
                index
                for index in range(int(selected["slots_total"]))
                if index not in occupied
            )
            next_state = (
                "offered" if int(lease["protocol_version"] or 1) >= 2 else "leased"
            )
            cursor = conn.execute(
                """
                UPDATE aedt_project_leases
                SET session_id = ?, slot_index = ?, state = ?,
                    acquired_at = ?, offered_at = ?, offer_expires_at = ?,
                    updated_at = ?
                WHERE id = ? AND state = 'queued'
                  AND client_deadline_at > ?
                """,
                (
                    int(selected["id"]),
                    slot_index,
                    next_state,
                    now,
                    now,
                    offer_expires,
                    now,
                    int(lease["id"]),
                    now,
                ),
            )
            if cursor.rowcount != 1:
                continue
            exact_reservation_id = int(
                lease["exact_session_reservation_id"] or 0
            )
            if exact_reservation_id:
                consumed = conn.execute(
                    """
                    UPDATE aedt_exact_session_reservations
                    SET state = 'consumed', consumed_at = ?, updated_at = ?
                    WHERE id = ? AND state = 'claimed' AND lease_id = ?
                    """,
                    (
                        now,
                        now,
                        exact_reservation_id,
                        int(lease["id"]),
                    ),
                )
                if consumed.rowcount != 1:
                    raise RuntimeError(
                        "exact-session reservation changed during placement"
                    )
            profile = str(lease["session_profile"] or "")
            if profile and not str(selected["session_profile"] or ""):
                conn.execute(
                    "UPDATE aedt_sessions SET session_profile = ?, updated_at = ? "
                    "WHERE id = ? AND session_profile = ''",
                    (profile, now, int(selected["id"])),
                )
            self._refresh_session_state(conn, int(selected["id"]), now)
            placed += 1
        return placed

    def reconcile(
        self,
        *,
        execute: bool,
        excluded_start_accounts: set[str] | None = None,
        start_block_reasons: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Reap stale ownership, assign leases, and scale only when gated.

        `execute=True` still cannot open a session unless enabled, validated,
        and adapter-ready.  This makes calls from lease/heartbeat endpoints safe
        before the pooled backend is approved.
        """
        config = self.config()
        excluded_start_accounts = {
            str(account or "").strip()
            for account in (excluded_start_accounts or set())
            if str(account or "").strip()
        }
        start_block_reasons = {
            str(account or "").strip(): str(reason or "").strip()
            for account, reason in (start_block_reasons or {}).items()
            if str(account or "").strip()
        }
        # Account selection can refresh remote Slurm/quota snapshots. Resolve
        # it before BEGIN IMMEDIATE so a slow SSH probe cannot block every API,
        # heartbeat, cancellation, and background attach writer.
        task_account_selections = self._preselect_task_accounts()
        warm_spare_admission_result = self._precheck_warm_spare_admission(
            config,
            task_account_selections=task_account_selections,
            excluded_start_accounts=excluded_start_accounts,
            start_block_reasons=start_block_reasons,
        )
        now_dt = self._now()
        now = _sql_time(now_dt)
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._refresh_exact_session_reservations(conn, now)

            if execute and config.operational and excluded_start_accounts:
                # A storage-blocked account cannot launch its host task.  Retire
                # only starts that no host has acquired; otherwise every tick
                # creates another starting row and another doomed host task.
                # ATTACHING/RUNNING hosts are deliberately left alone so a
                # transient quota probe never kills a Desktop already starting.
                for account_name in sorted(excluded_start_accounts):
                    blocked_starts = conn.execute(
                        """
                        SELECT s.id
                        FROM aedt_sessions s
                        JOIN allocations a ON a.id = s.allocation_id
                        WHERE s.state = 'starting'
                          AND s.host_id = ''
                          AND a.account_name = ?
                          AND NOT EXISTS (
                              SELECT 1 FROM tasks active_host
                              WHERE (
                                  active_host.id = s.host_task_id
                                  OR active_host.dedupe_key =
                                     'aedt-session-host:' || s.id
                              )
                                AND active_host.status IN ('attaching','running')
                          )
                        ORDER BY s.id
                        """,
                        (account_name,),
                    ).fetchall()
                    reason = start_block_reasons.get(
                        account_name,
                        "AEDT session start blocked by account storage guard",
                    )
                    for row in blocked_starts:
                        session_id = int(row["id"])
                        conn.execute(
                            """
                            UPDATE tasks
                            SET status = 'cancelled', failure_message = ?,
                                finished_at = ?, updated_at = ?
                            WHERE status = 'queued'
                              AND project = '_aedt_pool_hosts'
                              AND (
                                  id = (
                                      SELECT host_task_id FROM aedt_sessions
                                      WHERE id = ?
                                  )
                                  OR dedupe_key = 'aedt-session-host:' || ?
                              )
                            """,
                            (reason, now, now, session_id, session_id),
                        )
                        conn.execute(
                            """
                            UPDATE aedt_sessions
                            SET state = 'failed', failure_message = ?,
                                closed_at = ?, updated_at = ?
                            WHERE id = ? AND state = 'starting' AND host_id = ''
                              AND NOT EXISTS (
                                  SELECT 1 FROM tasks active_host
                                  WHERE (
                                      active_host.id = aedt_sessions.host_task_id
                                      OR active_host.dedupe_key =
                                         'aedt-session-host:' || aedt_sessions.id
                                  )
                                    AND active_host.status IN ('attaching','running')
                              )
                            """,
                            (reason, now, now, session_id),
                        )

            if execute and config.allocation_max_age_seconds:
                allocation_age_cutoff = _sql_time(
                    now_dt
                    - timedelta(seconds=config.allocation_max_age_seconds)
                )
                conn.execute(
                    """
                    UPDATE allocations
                    SET state = 'draining', drain_reason = ?,
                        drain_at = COALESCE(drain_at, ?), updated_at = ?
                    WHERE state IN ('warm','active')
                      AND drain_reason LIKE 'AEDT pool%'
                      AND created_at <= ?
                    """,
                    (
                        ALLOCATION_AGE_ROTATION_REASON,
                        now,
                        now,
                        allocation_age_cutoff,
                    ),
                )
            if execute:
                rotating_allocation_ids = [
                    int(row["id"])
                    for row in conn.execute(
                        """
                        SELECT id FROM allocations
                        WHERE state = 'draining' AND drain_reason = ?
                        """,
                        (ALLOCATION_AGE_ROTATION_REASON,),
                    ).fetchall()
                ]
                # Keep using the normal session-drain lifecycle on every pass.
                # This also catches a host that registered after its allocation
                # crossed the age boundary in the preceding pass.
                self._request_sessions_drain(
                    conn,
                    ALLOCATION_AGE_ROTATION_REASON,
                    now,
                    allocation_ids=rotating_allocation_ids,
                )

            # A terminal scheduler task cannot still own admission.  Reconcile
            # this before placement so an abandoned request is never offered
            # after its client process has already exited.
            terminal_leases = conn.execute(
                """
                SELECT l.id, l.session_id, l.state
                FROM aedt_project_leases l
                JOIN tasks t ON t.id = l.task_id
                WHERE l.state IN (
                    'queued','offered','leased','attaching','active'
                )
                  AND t.status IN ('completed','failed','cancelled')
                """
            ).fetchall()
            for row in terminal_leases:
                if row["state"] in {"attaching", "active"} and row["session_id"]:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'releasing',
                            failure_message = 'scheduler task became terminal',
                            release_requested_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (now, now, int(row["id"])),
                    )
                    conn.execute(
                        """
                        UPDATE aedt_sessions
                        SET reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                            updated_at = ?
                        WHERE id = ? AND state NOT IN ('closed','failed')
                        """,
                        (now, now, int(row["session_id"])),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'cancelled', session_id = NULL,
                            slot_index = NULL,
                            failure_message = 'scheduler task became terminal',
                            finished_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (now, now, int(row["id"])),
                    )
                    if row["session_id"]:
                        self._refresh_session_state(conn, int(row["session_id"]), now)

            legacy_queued_cutoff = _sql_time(
                now_dt - timedelta(seconds=min(config.lease_ttl_seconds, 300))
            )
            # A v2 offer is a revocable reservation, not terminal ownership.
            # If its short ACK window passes while the relay/client is offline,
            # return the slot to the queue until the durable admission deadline.
            stale_offers = conn.execute(
                """
                SELECT id, session_id FROM aedt_project_leases
                WHERE state = 'offered' AND protocol_version >= 2
                  AND offer_expires_at IS NOT NULL AND offer_expires_at < ?
                  AND client_deadline_at > ? AND expires_at > ?
                """,
                (now, now, now),
            ).fetchall()
            for row in stale_offers:
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'queued', session_id = NULL, slot_index = NULL,
                        offered_at = NULL, offer_expires_at = NULL,
                        acquired_at = NULL, solve_permit_at = NULL,
                        solve_permit_generation = 0,
                        failure_message = 'lease offer requeued after ACK window',
                        updated_at = ?
                    WHERE id = ? AND state = 'offered'
                    """,
                    (now, int(row["id"])),
                )
                conn.execute(
                    """
                    UPDATE aedt_exact_session_reservations
                    SET state = 'claimed', consumed_at = NULL, updated_at = ?
                    WHERE lease_id = ? AND state = 'consumed'
                    """,
                    (now, int(row["id"])),
                )
                if row["session_id"]:
                    self._refresh_session_state(conn, int(row["session_id"]), now)
            expired = conn.execute(
                """
                SELECT id, session_id, state FROM aedt_project_leases
                WHERE state IN ('queued','offered','leased','attaching','active')
                  AND (
                      expires_at < ?
                      OR (
                          state IN ('queued','offered')
                          AND client_deadline_at < ?
                      )
                      OR (
                          state = 'queued'
                          AND last_heartbeat_at IS NOT NULL
                          AND (
                              protocol_version < 2 AND last_heartbeat_at < ?
                          )
                      )
                      OR (
                          state = 'offered'
                          AND offer_expires_at IS NOT NULL
                           AND protocol_version < 2
                           AND offer_expires_at < ?
                      )
                  )
                """,
                (now, now, legacy_queued_cutoff, now),
            ).fetchall()
            for row in expired:
                if row["state"] == "queued":
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'expired', failure_message = 'lease request heartbeat expired',
                            finished_at = ?, updated_at = ? WHERE id = ?
                        """,
                        (now, now, int(row["id"])),
                    )
                    continue

                if row["state"] == "offered":
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'expired', session_id = NULL, slot_index = NULL,
                            failure_message = 'lease offer acknowledgement expired',
                            finished_at = ?, updated_at = ? WHERE id = ?
                        """,
                        (now, now, int(row["id"])),
                    )
                    if row["session_id"]:
                        self._refresh_session_state(conn, int(row["session_id"]), now)
                    continue

                # A lease that never became active cannot own a running solve.
                # Ask the host to close its pending project, but keep the
                # Desktop and every sibling lease serving normally.
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'releasing', failure_message = 'lease heartbeat expired',
                        release_requested_at = ?, updated_at = ? WHERE id = ?
                    """,
                    (now, now, int(row["id"])),
                )
                if row["session_id"]:
                    conn.execute(
                        """
                        UPDATE aedt_sessions
                        SET reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                            updated_at = ?
                        WHERE id = ? AND state NOT IN ('closed','failed')
                        """,
                        (now, now, int(row["session_id"])),
                    )
                if row["state"] != "active" or not row["session_id"]:
                    continue

                # A dead active client may still have a solve executing in
                # AEDT.  Preserve the conservative whole-session quarantine.
                conn.execute(
                    """
                    UPDATE aedt_sessions SET state = 'draining',
                        quarantine_reason = 'lease_heartbeat_expired',
                        quarantine_until = COALESCE(quarantine_until, ?),
                        failure_message = 'project lease heartbeat expired',
                        drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                    WHERE id = ? AND (
                        state IN ('ready','busy')
                        OR (state = 'unhealthy' AND failure_message = ?
                            AND quarantine_reason = '')
                    )
                    """,
                    (
                        _sql_time(now_dt + timedelta(seconds=900)),
                        now,
                        now,
                        int(row["session_id"]),
                        SESSION_HEARTBEAT_TIMEOUT_MESSAGE,
                    ),
                )

            heartbeat_cutoff = _sql_time(
                now_dt - timedelta(seconds=config.session_heartbeat_timeout_seconds)
            )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'unhealthy', failure_message = CASE
                        WHEN state = 'draining'
                        THEN 'session heartbeat expired while already draining'
                        ELSE ?
                    END,
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE state IN ('ready','busy','draining')
                  AND COALESCE(last_heartbeat_at, started_at, created_at) < ?
                """,
                (SESSION_HEARTBEAT_TIMEOUT_MESSAGE, now, now, heartbeat_cutoff),
            )
            # A host that disappeared after receiving a release command can
            # never send complete_release.  Once native session heartbeats are
            # stale and the session is quarantined unhealthy, terminalize the
            # orphaned release so a stopped controller generation cannot leave
            # scheduler-live leases forever.  The session reuse barrier stays
            # set and the Desktop is made non-recoverably quarantined, so a
            # late heartbeat cannot make an unclosed native project reusable.
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'unhealthy',
                    failure_message = 'release acknowledgement lost; Desktop recycle required',
                    quarantine_reason = 'release_ack_lost',
                    reuse_blocked_at = COALESCE(reuse_blocked_at, ?),
                    drain_requested_at = COALESCE(drain_requested_at, ?),
                    updated_at = ?
                WHERE state = 'unhealthy'
                  AND COALESCE(last_heartbeat_at, started_at, created_at) < ?
                  AND EXISTS (
                      SELECT 1 FROM aedt_project_leases l
                      WHERE l.session_id = aedt_sessions.id
                        AND l.state = 'releasing'
                        AND COALESCE(
                            l.release_requested_at, l.updated_at, l.requested_at
                        ) < ?
                  )
                """,
                (now, now, now, heartbeat_cutoff, heartbeat_cutoff),
            )
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = 'failed',
                    failure_message = CASE
                        WHEN TRIM(COALESCE(failure_message, '')) != ''
                        THEN failure_message
                        ELSE 'release acknowledgement lost after session heartbeat expiry'
                    END,
                    finished_at = ?, updated_at = ?
                WHERE state = 'releasing'
                  AND COALESCE(release_requested_at, updated_at, requested_at) < ?
                  AND (
                      session_id IS NULL
                      OR NOT EXISTS (
                          SELECT 1 FROM aedt_sessions s
                          WHERE s.id = aedt_project_leases.session_id
                      )
                      OR EXISTS (
                          SELECT 1 FROM aedt_sessions s
                          WHERE s.id = aedt_project_leases.session_id
                            AND (
                                s.state IN ('closed','failed')
                                OR (
                                    s.state = 'unhealthy'
                                    AND COALESCE(
                                        s.last_heartbeat_at,
                                        s.started_at,
                                        s.created_at
                                    ) < ?
                                )
                            )
                      )
                  )
                """,
                (now, now, heartbeat_cutoff, heartbeat_cutoff),
            )
            start_cutoff = _sql_time(now_dt - timedelta(seconds=config.session_start_timeout_seconds))
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'unhealthy', failure_message = ?,
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE state = 'starting'
                  AND COALESCE(start_claimed_at, created_at) < ?
                """,
                (SESSION_START_ACK_TIMEOUT_MESSAGE, now, now, start_cutoff),
            )
            unhealthy_recycle_cutoff = _sql_time(
                now_dt
                - timedelta(seconds=config.unhealthy_recycle_grace_seconds)
            )
            # Mark the allocation before stale-session reaping changes the
            # session out of the unhealthy state used by this selection.
            conn.execute(
                """
                UPDATE allocations
                SET state = 'draining',
                    drain_reason = CASE
                        WHEN state = 'draining'
                         AND TRIM(COALESCE(drain_reason, '')) NOT IN (
                             '', 'AEDT pool project demand'
                         )
                        THEN drain_reason
                        ELSE ?
                    END,
                    drain_at = COALESCE(drain_at, ?), updated_at = ?
                WHERE id IN (
                    SELECT allocation_id FROM aedt_sessions
                    WHERE quarantine_reason != ''
                       OR (state = 'unhealthy'
                           AND COALESCE(
                               drain_requested_at, last_heartbeat_at,
                               updated_at, created_at
                           ) < ?)
                ) AND state IN ('warm','active','draining')
                """,
                (
                    UNHEALTHY_ALLOCATION_RECYCLE_REASON,
                    now,
                    now,
                    unhealthy_recycle_cutoff,
                ),
            )
            # An unhealthy session whose host has been silent far beyond the
            # heartbeat window can never call close_session (only the live
            # host holds the token).  Without reaping, its counted claim pins
            # the drained allocation open forever, which in turn blocks every
            # new dedicated-node request: the pool deadlocks at zero capacity.
            reap_cutoff = _sql_time(
                now_dt
                - timedelta(
                    seconds=max(900, 5 * config.session_heartbeat_timeout_seconds)
                )
            )
            stale_unhealthy = [
                int(row["id"])
                for row in conn.execute(
                    """
                    SELECT id FROM aedt_sessions
                    WHERE state = 'unhealthy'
                      AND (
                          (
                              (
                                  failure_message LIKE 'host native liveness suspect:%'
                                  OR quarantine_reason = 'confirmed_aedt_death'
                              )
                              AND COALESCE(
                                  drain_requested_at, last_fault_at,
                                  last_heartbeat_at,
                                  started_at, created_at
                              ) < ?
                              AND NOT EXISTS (
                                  SELECT 1 FROM aedt_project_leases owner
                                  WHERE owner.session_id = aedt_sessions.id
                                    AND owner.state IN (
                                        'offered','leased','attaching',
                                        'active','releasing'
                                    )
                              )
                          )
                          OR (
                              COALESCE(
                                  last_heartbeat_at, started_at, created_at
                              ) < ?
                              AND COALESCE(
                                  drain_requested_at, last_heartbeat_at,
                                  updated_at, created_at
                              ) < ?
                          )
                      )
                    """,
                    (
                        unhealthy_recycle_cutoff,
                        reap_cutoff,
                        unhealthy_recycle_cutoff,
                    ),
                ).fetchall()
            ]
            for session_id in stale_unhealthy:
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'failed',
                        failure_message = 'AEDT session host unreachable; session reaped',
                        finished_at = ?, updated_at = ?
                    WHERE session_id = ? AND state IN ('offered','leased','attaching','active','releasing')
                    """,
                    (now, now, session_id),
                )
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'failed',
                        failure_message = CASE
                            WHEN TRIM(COALESCE(failure_message, '')) != ''
                            THEN failure_message
                            ELSE 'session host unreachable; reaped'
                        END,
                        closed_at = ?, updated_at = ?
                    WHERE id = ? AND state = 'unhealthy'
                    """,
                    (now, now, session_id),
                )
            if execute:
                recoverable_allocation_ids = [
                    int(row["id"])
                    for row in conn.execute(
                        """
                        SELECT a.id
                        FROM allocations a
                        WHERE a.state = 'draining'
                          AND a.drain_reason IN (?, ?)
                          AND EXISTS (
                              SELECT 1 FROM aedt_sessions healthy
                              WHERE healthy.allocation_id = a.id
                                AND healthy.state IN ('ready','busy')
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM aedt_sessions unsafe
                              WHERE unsafe.allocation_id = a.id
                                -- Only LIVE sessions make the node unsafe.
                                -- Terminal (failed/closed) siblings keep their
                                -- historical quarantine_reason forever and must
                                -- not pin the allocation in draining (observed:
                                -- 2 healthy ready Desktops idled behind 3
                                -- failed siblings' history while 249 leases
                                -- queued).
                                AND unsafe.state IN (
                                    'starting','ready','busy','draining','unhealthy'
                                )
                                AND (
                                    unsafe.state = 'unhealthy'
                                    OR TRIM(COALESCE(unsafe.quarantine_reason, '')) != ''
                                )
                          )
                        ORDER BY a.id ASC
                        """,
                        (
                            FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON,
                            UNHEALTHY_ALLOCATION_RECYCLE_REASON,
                        ),
                    ).fetchall()
                ]
                for allocation_id in recoverable_allocation_ids:
                    restored = conn.execute(
                        """
                        UPDATE allocations
                        SET state = 'active',
                            drain_reason = 'AEDT pool project demand',
                            drain_at = NULL, failure_message = '', updated_at = ?
                        WHERE id = ? AND state = 'draining'
                          AND drain_reason IN (?, ?)
                        """,
                        (
                            now,
                            allocation_id,
                            FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON,
                            UNHEALTHY_ALLOCATION_RECYCLE_REASON,
                        ),
                    )
                    if restored.rowcount != 1:
                        continue
                    conn.execute(
                        """
                        UPDATE aedt_sessions
                        SET drain_requested_at = NULL, updated_at = ?
                        WHERE allocation_id = ? AND state IN ('ready','busy')
                        """,
                        (now, allocation_id),
                    )
            if execute:
                # Never advertise an idle Desktop whose parent allocation can
                # no longer accept scheduler tasks.  Busy owners are left
                # untouched so in-flight q19/q20 solves finish naturally; the
                # same rule catches them on a later pass after their final
                # project releases and the session becomes ready.
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'draining',
                        drain_requested_at = COALESCE(drain_requested_at, ?),
                        updated_at = ?
                    WHERE state = 'ready'
                      AND allocation_id IN (
                          SELECT id FROM allocations
                          WHERE state NOT IN ('warm','active')
                      )
                    """,
                    (now, now),
                )
            # Admission placement is deliberately short and shares the same
            # helper used by POST /leases.  Full expiry/scale reconciliation
            # stays on this background tick instead of every HTTP request.
            if execute and config.operational:
                self._place_queued_leases(
                    conn,
                    now,
                    config=config,
                    refresh_reservations=False,
                )

            plan = self._plan(
                conn,
                config,
                task_account_selections=task_account_selections,
                warm_spare_admission_result=warm_spare_admission_result,
                excluded_start_accounts=excluded_start_accounts,
                start_block_reasons=start_block_reasons,
            )
            if execute and config.operational:
                for account_name, count in sorted(
                    (plan.get("rebalance_drains_by_account") or {}).items()
                ):
                    candidates = conn.execute(
                        """
                        SELECT s.id
                        FROM aedt_sessions s
                        JOIN allocations a ON a.id = s.allocation_id
                        WHERE s.state = 'ready'
                          AND a.state IN ('warm','active')
                          AND a.account_name = ?
                          AND s.solve_batch_sealed_at IS NULL
                          AND s.drain_requested_at IS NULL
                          AND s.reuse_blocked_at IS NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM aedt_exact_session_reservations r
                              WHERE r.session_id = s.id
                                AND r.state IN ('reserved','claimed')
                          )
                        ORDER BY COALESCE(s.idle_since, s.created_at), s.id
                        LIMIT ?
                        """,
                        (str(account_name), max(0, int(count))),
                    ).fetchall()
                    for session in candidates:
                        conn.execute(
                            """
                            UPDATE aedt_sessions
                            SET state = 'draining',
                                drain_requested_at = COALESCE(drain_requested_at, ?),
                                updated_at = ?
                            WHERE id = ? AND state = 'ready'
                            """,
                            (now, now, int(session["id"])),
                        )
                # Lowering the cap drains naturally.  Busy sessions reject new
                # leases and close only after their host has closed all projects.
                drain_needed = int(plan["drain_needed"])
                if drain_needed:
                    candidates = conn.execute(
                        """
                        SELECT s.*,
                               (SELECT COUNT(*) FROM aedt_project_leases l
                                WHERE l.session_id = s.id
                                  AND l.state IN ('offered','leased','attaching','active','releasing')) AS used_slots
                        FROM aedt_sessions s
                        WHERE s.state IN ('ready','busy')
                          AND NOT EXISTS (
                              SELECT 1 FROM aedt_exact_session_reservations r
                              WHERE r.session_id = s.id
                                AND r.state IN ('reserved','claimed')
                          )
                        ORDER BY used_slots ASC, COALESCE(s.idle_since, s.created_at) ASC, s.id ASC
                        """
                    ).fetchall()
                    for session in candidates[:drain_needed]:
                        conn.execute(
                            """
                            UPDATE aedt_sessions SET state = 'draining',
                                drain_requested_at = ?, updated_at = ? WHERE id = ?
                            """,
                            (now, now, int(session["id"])),
                        )

                for placement in plan["placements"]:
                    conn.execute(
                        """
                        INSERT INTO aedt_sessions (
                            session_key, allocation_id, account_name, node_name,
                            slots_total, state, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'starting', ?, ?)
                        """,
                        (
                            f"aedt-{secrets.token_hex(12)}",
                            int(placement["allocation_id"]),
                            placement["account_name"],
                            placement["node_name"],
                            config.projects_per_session,
                            now,
                            now,
                        ),
                    )
                plan = self._plan(
                    conn,
                    config,
                    task_account_selections=task_account_selections,
                    warm_spare_admission_result=warm_spare_admission_result,
                    excluded_start_accounts=excluded_start_accounts,
                    start_block_reasons=start_block_reasons,
                )
            plan["operational"] = config.operational
            plan["executed"] = bool(execute and config.operational)
            return plan

    def summary(self) -> dict[str, Any]:
        config = self.config()
        plan = self.dry_run()
        latest = self.latest_validation()
        live_state_placeholders = ", ".join("?" for _ in SESSION_VISIBLE_STATES)
        history_state_placeholders = ", ".join("?" for _ in SESSION_HISTORY_STATES)
        with self.db.connect() as conn:
            sessions = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT s.*,
                           COALESCE(a.account_name, s.account_name) AS allocation_account_name
                    FROM aedt_sessions s
                    LEFT JOIN allocations a ON a.id = s.allocation_id
                    WHERE s.state IN ({live_state_placeholders})
                    ORDER BY s.id DESC
                    """,
                    SESSION_VISIBLE_STATES,
                ).fetchall()
            ]
            session_history = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT s.*,
                           COALESCE(a.account_name, s.account_name) AS allocation_account_name
                    FROM aedt_sessions s
                    LEFT JOIN allocations a ON a.id = s.allocation_id
                    WHERE s.state IN ({history_state_placeholders})
                    ORDER BY COALESCE(s.closed_at, s.updated_at, s.created_at) DESC,
                             s.id DESC
                    LIMIT ?
                    """,
                    (*SESSION_HISTORY_STATES, SESSION_HISTORY_LIMIT),
                ).fetchall()
            ]
            host_tasks = conn.execute(
                """
                SELECT id, name, dedupe_key
                FROM tasks
                WHERE TRIM(COALESCE(project, '')) = '_aedt_pool_hosts'
                  AND (
                      dedupe_key LIKE 'aedt-session-host:%'
                      OR dedupe_key LIKE 'aedt-session-host-%'
                      OR name LIKE 'aedt-session-host-%'
                  )
                ORDER BY id DESC
                """
            ).fetchall()
            attached_leases = conn.execute(
                """
                SELECT id, session_id, task_id, project_name, slot_index, state
                FROM aedt_project_leases
                WHERE session_id IS NOT NULL
                  AND state IN ('offered', 'leased', 'attaching', 'active', 'releasing')
                ORDER BY session_id ASC, slot_index ASC, id ASC
                """
            ).fetchall()
            leases = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT id, request_key, project_name, task_id, session_id,
                           slot_index, state, requested_at, acquired_at,
                           last_heartbeat_at, expires_at, failure_message
                    FROM aedt_project_leases ORDER BY id DESC LIMIT 500
                    """
                ).fetchall()
            ]

        host_task_by_session: dict[int, int] = {}
        for task in host_tasks:
            session_id = 0
            for value in (task["dedupe_key"], task["name"]):
                convention = str(value or "").strip()
                for prefix in ("aedt-session-host:", "aedt-session-host-"):
                    if convention.startswith(prefix):
                        suffix = convention[len(prefix):]
                        if suffix.isdecimal():
                            session_id = int(suffix)
                        break
                if session_id:
                    break
            if session_id:
                host_task_by_session.setdefault(session_id, int(task["id"]))

        attached_leases_by_session: dict[int, list[dict[str, Any]]] = {}
        for lease in attached_leases:
            attached_leases_by_session.setdefault(int(lease["session_id"]), []).append(
                dict(lease)
            )

        for session_collection in (sessions, session_history):
            for session in session_collection:
                session_id = int(session["id"])
                session_attached_leases = attached_leases_by_session.get(session_id, [])
                active_lease_count = len(session_attached_leases)
                session["host_task_id"] = host_task_by_session.get(session_id)
                session["active_lease_count"] = active_lease_count
                session["free_slot_count"] = max(
                    0, int(session.get("slots_total") or 0) - active_lease_count
                )
                session["attached_project_names"] = [
                    str(lease["project_name"] or "")
                    for lease in session_attached_leases
                ]
                session["attached_leases"] = session_attached_leases
        return {
            "config": {
                "enabled": config.enabled,
                "adapter_ready": config.adapter_ready,
                "validation_passed": config.validation_passed,
                "operational": config.operational,
                "max_aedt_sessions": config.max_sessions,
                "min_idle_aedt_sessions": config.min_idle_sessions,
                "target_project_concurrency": config.target_projects,
                "projects_per_aedt": config.projects_per_session,
                "project_cpus": config.project_cpus,
                "project_memory_mb": config.project_memory_mb,
                "session_reserved_cpus": (
                    1 + config.project_cpus * config.projects_per_session
                ),
                "session_reserved_memory_mb": (
                    config.project_memory_mb * config.projects_per_session
                ),
                "native_solve_mode": self.native_solve_mode,
                "parallel_safe_native_solve_families": sorted(
                    self._parallel_safe_native_solve_families
                ),
                "control_plane_url": config.control_plane_url,
                "hard_counted_states": list(SESSION_HARD_CAP_STATES),
            },
            "plan": plan,
            "active_session_count": int(plan.get("active_session_count") or 0),
            "active_project_capacity": int(
                plan.get("active_project_capacity") or 0
            ),
            "starting_session_count": int(
                plan.get("starting_session_count") or 0
            ),
            "draining_session_count": int(
                plan.get("draining_session_count") or 0
            ),
            "unhealthy_session_count": int(
                plan.get("unhealthy_session_count") or 0
            ),
            "latest_validation": latest,
            "sessions": sessions,
            "session_history": session_history,
            "leases": leases,
        }

    def starting_sessions(self) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM aedt_sessions WHERE state = 'starting' ORDER BY id ASC"
                ).fetchall()
            ]

    def fail_unclaimed_session_start(self, session_id: int, reason: str) -> bool:
        """Release a planned row when no node-side host ever acquired it."""
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'failed', failure_message = ?, closed_at = ?, updated_at = ?
                WHERE id = ? AND state = 'starting' AND host_id = ''
                """,
                (
                    reason.strip() or "AEDT session host reservation failed",
                    now,
                    now,
                    int(session_id),
                ),
            )
            return cursor.rowcount == 1

    def allocation_has_counted_session(self, allocation_id: int) -> bool:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM aedt_sessions
                WHERE allocation_id = ?
                  AND state IN ('starting','ready','busy','draining','unhealthy')
                LIMIT 1
                """,
                (int(allocation_id),),
            ).fetchone()
            return bool(row)


class AedtPoolRuntime:
    """Small opt-in reconciler; node creation delegates to Scheduler.

    It cannot launch AEDT itself.  Dedicated node-side session-host agents
    claim `starting` rows and remain the only processes allowed to own/kill
    Desktop.  Until that adapter is configured and validation passes, this
    runtime performs no Slurm or AEDT mutation.
    """

    def __init__(
        self,
        service: AedtPoolService,
        scheduler: Any,
        interval_seconds: int = 30,
        *,
        scheduler_url: str = "",
        host_remote_cwd: str = "",
        host_python: str = "python",
        host_env_setup: str = "",
        host_bootstrap_token_file: str = "",
        host_task_memory_mb: int = 65536,
        host_artifact_root: str = "",
        host_dso_profile: str = "",
        host_session_profile: str = "",
        require_published_control_plane_url: bool = False,
        host_launch_stagger_seconds: int | None = None,
    ) -> None:
        self.service = service
        self.scheduler = scheduler
        self.interval_seconds = max(5, int(interval_seconds))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.scheduler_url = scheduler_url.strip().rstrip("/")
        self.host_remote_cwd = host_remote_cwd.strip()
        self.host_python = host_python.strip() or "python"
        self.host_env_setup = host_env_setup
        self.host_bootstrap_token_file = host_bootstrap_token_file.strip()
        self.host_task_memory_mb = max(1024, int(host_task_memory_mb))
        self.host_artifact_root = str(host_artifact_root or "").strip()
        self.host_dso_profile = str(host_dso_profile or "").strip().lower()
        if (
            self.host_dso_profile
            and self.host_dso_profile not in SUPPORTED_DSO_PROFILES
        ):
            raise ValueError(
                f"unsupported AEDT host DSO profile: {self.host_dso_profile}"
            )
        if self.host_dso_profile == SUPPORTED_DSO_PROFILE and not host_session_profile:
            raise ValueError(
                "canonical AEDT host DSO profile requires host_session_profile"
            )
        self.host_session_profile = (
            canonical_expected_session_profile(host_session_profile)
            if host_session_profile
            else ""
        )
        self.host_aedt_version = (
            EXPECTED_AEDT_VERSION if self.host_session_profile else ""
        )
        self.require_published_control_plane_url = bool(
            require_published_control_plane_url
        )
        configured_stagger: Any = host_launch_stagger_seconds
        if configured_stagger is None:
            configured_stagger = os.environ.get(
                HOST_LAUNCH_STAGGER_ENV, DEFAULT_HOST_LAUNCH_STAGGER_SECONDS
            )
        try:
            self.host_launch_stagger_seconds = max(
                0, min(60, int(configured_stagger))
            )
        except (TypeError, ValueError):
            LOGGER.warning(
                "Ignoring invalid %s=%r; using %ss",
                HOST_LAUNCH_STAGGER_ENV,
                configured_stagger,
                DEFAULT_HOST_LAUNCH_STAGGER_SECONDS,
            )
            self.host_launch_stagger_seconds = DEFAULT_HOST_LAUNCH_STAGGER_SECONDS
        # Treat runtime startup as unpublished.  The first observed relay URL
        # gets the same one-tick settling grace as a later relay recovery.
        self._control_plane_was_published = False
        self._account_request_cursor = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="aedt-pool", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                LOGGER.exception("AEDT pool reconciliation failed")
            self._stop.wait(self.interval_seconds)

    def _storage_start_block_reasons(self) -> dict[str, str]:
        """Return accounts on which a new AEDT host must not be launched."""

        checker = getattr(self.scheduler, "account_storage_blocked", None)
        accounts = list(getattr(self.scheduler, "accounts", ()) or ())
        if not callable(checker) or not accounts:
            return {}
        blocked: dict[str, str] = {}
        for account in accounts:
            account_name = str(getattr(account, "name", "") or "").strip()
            if not account_name:
                continue
            try:
                is_blocked = bool(checker(account, for_fea=True))
            except Exception as exc:
                LOGGER.exception(
                    "AEDT pool storage admission check failed for %s",
                    account_name,
                )
                blocked[account_name] = (
                    "AEDT session start blocked because the account storage "
                    f"admission check failed: {exc}"
                )
                continue
            if is_blocked:
                blocked[account_name] = (
                    "AEDT session start blocked by the account storage guard"
                )
        return blocked

    def tick(self) -> dict[str, Any]:
        config = self.service.config()
        control_plane_url = self._control_plane_url(config)
        control_plane_just_published = bool(
            self.require_published_control_plane_url
            and control_plane_url
            and not self._control_plane_was_published
        )
        if self.require_published_control_plane_url:
            # Update before the unpublished early return so a later publication
            # is recognized as a recovery transition.
            self._control_plane_was_published = bool(control_plane_url)
        if self.require_published_control_plane_url and not control_plane_url:
            # Do not even enter the mutating reconciler while relay
            # supervision has withdrawn the node-visible endpoint.  In
            # particular, this prevents new allocations/sessions from being
            # created when their host processes could not call home.
            plan = self.service.dry_run()
            plan.update(
                {
                    "control_plane_ready": False,
                    "control_plane_error": (
                        "node-visible AEDT control-plane URL is not published"
                    ),
                    "node_allocations_opened": 0,
                    "host_tasks_started": 0,
                    "empty_allocations_closed": 0,
                }
            )
            return plan
        start_block_reasons = (
            self._storage_start_block_reasons() if config.operational else {}
        )
        blocked_start_accounts = set(start_block_reasons)
        plan = self.service.reconcile(
            execute=True,
            excluded_start_accounts=blocked_start_accounts,
            start_block_reasons=start_block_reasons,
        )
        if self.require_published_control_plane_url:
            plan["control_plane_ready"] = True
            plan["control_plane_error"] = ""
        if not config.operational:
            return plan
        request_budget = min(
            int(plan.get("node_requests") or 0), config.scale_step_nodes
        )
        raw_requests_by_account = plan.get("node_requests_by_account") or {
            config.account_name: request_budget
        }
        request_counts = {
            str(account_name or ""): max(0, int(count))
            for account_name, count in raw_requests_by_account.items()
            if int(count) > 0
        }
        account_order = sorted(request_counts)
        if account_order:
            offset = self._account_request_cursor % len(account_order)
            account_order = account_order[offset:] + account_order[:offset]
        requests_by_account: list[str] = []
        while len(requests_by_account) < request_budget and any(
            request_counts.values()
        ):
            for account_name in account_order:
                if request_counts.get(account_name, 0) <= 0:
                    continue
                requests_by_account.append(account_name)
                request_counts[account_name] -= 1
                if len(requests_by_account) >= request_budget:
                    break
        if account_order and requests_by_account:
            self._account_request_cursor = (
                self._account_request_cursor + len(requests_by_account)
            ) % len(account_order)
        opened = 0
        opened_by_account: dict[str, int] = {}
        for account_name in requests_by_account:
            allocation = self.scheduler.open_allocation_record(
                "AEDT pool project demand",
                resource_pool="cpu",
                required_capability=config.required_capability,
                env_profile=config.env_profile,
                account_name=account_name,
                # In AEDT node-sharing mode this is one complete session's CPU
                # footprint.  The scheduler expands it only in whole-session
                # multiples up to its configured allocation target.
                requested_cpus=(
                    1 + config.project_cpus * config.projects_per_session
                ),
                # In AEDT node-sharing mode these are per-session footprints.
                # The scheduler requests an exact whole-session multiple, so a
                # three-project session reserves 3 x project memory alongside
                # its one host CPU plus three project CPU reservations.
                requested_memory_mb=(
                    config.project_memory_mb * config.projects_per_session
                ),
                # cpu2 nodes may host multiple AEDT allocations; aggregate
                # Slurm/DB CPU and memory reservations remain the hard node
                # capacity boundary.
                aedt_pool_node_sharing=True,
                require_fea_eligible_node=True,
                # Desktop hosts are CPU-pool infrastructure.  Generic CPU work
                # may borrow idle GPU nodes, but the long-lived AEDT pool must
                # not consume GPU partitions as a fallback.
                cpu_only_nodes=True,
            )
            if not allocation:
                continue
            opened += 1
            actual_account = str(allocation.get("account_name") or account_name)
            opened_by_account[actual_account] = (
                opened_by_account.get(actual_account, 0) + 1
            )
        plan["node_allocations_opened"] = opened
        plan["node_allocations_opened_by_account"] = opened_by_account
        if control_plane_just_published:
            # Publishing precedes full tunnel readiness.  One normal tick is a
            # small, bounded grace that avoids launching into that half-up gap.
            plan["host_tasks_started"] = 0
        else:
            plan["host_tasks_started"] = self._ensure_session_hosts(
                config,
                control_plane_url=control_plane_url,
                blocked_start_accounts=blocked_start_accounts,
            )
        plan["empty_allocations_closed"] = self._close_empty_dedicated_allocations()
        return plan

    def _control_plane_url(self, config: AedtPoolConfig | None = None) -> str:
        if self.require_published_control_plane_url:
            current = config or self.service.config()
            return current.control_plane_url.strip().rstrip("/")
        return self.scheduler_url

    @property
    def host_launch_configured(self) -> bool:
        return bool(
            self._control_plane_url()
            and self.host_remote_cwd
            and self.host_bootstrap_token_file
        )

    def _host_command(
        self,
        session: dict[str, Any],
        *,
        control_plane_url: str | None = None,
        launch_delay_seconds: int = 0,
    ) -> str:
        scheduler_url = (
            self._control_plane_url()
            if control_plane_url is None
            else control_plane_url.strip().rstrip("/")
        )
        parts = [
            self.host_python,
            "-m",
            "slurm_scheduler.aedt_session_host",
            "--scheduler-url",
            scheduler_url,
            "--allocation-id",
            str(int(session["allocation_id"])),
            "--node-name",
            str(session["node_name"]),
            "--session-id",
            str(int(session["id"])),
            "--bootstrap-token-file",
            self.host_bootstrap_token_file,
        ]
        if self.host_artifact_root:
            parts.extend(["--artifact-root", self.host_artifact_root])
        if self.host_aedt_version:
            parts.extend(["--aedt-version", self.host_aedt_version])
        if self.host_dso_profile:
            parts.extend(["--dso-profile", self.host_dso_profile])
        if self.host_session_profile:
            parts.extend(["--session-profile", self.host_session_profile])
        command = " ".join(shlex.quote(part) for part in parts)
        delay = max(0, int(launch_delay_seconds))
        if delay:
            # Delay in the node-side shell, keeping the host CLI compatible
            # with older cluster checkouts that do not know scheduler changes.
            return f"sleep {delay} && exec {command}"
        return command

    def _ensure_session_hosts(
        self,
        config: AedtPoolConfig,
        *,
        control_plane_url: str | None = None,
        blocked_start_accounts: set[str] | None = None,
    ) -> int:
        scheduler_url = (
            self._control_plane_url(config)
            if control_plane_url is None
            else control_plane_url.strip().rstrip("/")
        )
        if not (
            config.operational
            and scheduler_url
            and self.host_remote_cwd
            and self.host_bootstrap_token_file
        ):
            return 0
        started = 0
        blocked_start_accounts = {
            str(account or "").strip()
            for account in (blocked_start_accounts or set())
            if str(account or "").strip()
        }
        launches_by_allocation: dict[int, int] = {}
        for session in self.service.starting_sessions():
            if str(session.get("host_id") or ""):
                continue
            dedupe_key = f"aedt-session-host:{int(session['id'])}"
            if self.service.db.find_active_task_by_dedupe_key(dedupe_key):
                continue
            allocation_id = int(session["allocation_id"])
            allocation = self.service.db.get_allocation(allocation_id)
            if not allocation:
                self.service.fail_unclaimed_session_start(
                    int(session["id"]),
                    "dedicated AEDT allocation no longer exists",
                )
                continue
            allocation_account = str(allocation.get("account_name") or "")
            if allocation_account in blocked_start_accounts:
                self.service.fail_unclaimed_session_start(
                    int(session["id"]),
                    "AEDT session start blocked by the account storage guard",
                )
                continue
            if (
                allocation.get("state") not in {"warm", "active"}
                or str(allocation.get("drain_reason") or "")
                != "AEDT pool project demand"
            ):
                self.service.fail_unclaimed_session_start(
                    int(session["id"]),
                    "dedicated AEDT allocation is not eligible for a new host",
                )
                continue
            allocation_launch_index = launches_by_allocation.get(allocation_id, 0)
            launch_delay_seconds = (
                allocation_launch_index * self.host_launch_stagger_seconds
            )
            task_id = self.service.db.create_task(
                    # The host is a control task.  Project tasks are launched on
                    # this same allocation at project_cpus each, so charging the
                    # aggregate here would double count solver pressure.
                TaskCreate(
                    name=f"aedt-session-host-{int(session['id'])}",
                    remote_cwd=self.host_remote_cwd,
                    command=self._host_command(
                        session,
                        control_plane_url=scheduler_url,
                        launch_delay_seconds=launch_delay_seconds,
                    ),
                    env_setup=self.host_env_setup,
                    required_capability=config.required_capability,
                    env_profile=config.env_profile,
                    account_name=str(allocation.get("account_name") or ""),
                    cpus=1,
                    memory_mb=max(
                        self.host_task_memory_mb,
                        config.project_memory_mb
                        * max(1, int(session["slots_total"])),
                    ),
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    node_name=str(allocation.get("node_name") or ""),
                    # Keep 0..9 available for ordering ordinary simulation
                    # families.  Session hosts sit just above that band;
                    # reserve large priorities for canaries/recovery work.
                    priority=10,
                    timeout_seconds=0,
                    dedupe_key=dedupe_key,
                    project="_aedt_pool_hosts",
                    entrypoint="slurm_scheduler.aedt_session_host",
                    requested_allocation_id=allocation_id,
                )
            )
            try:
                self.service.bind_session_host_task(int(session["id"]), task_id)
            except ValueError as exc:
                self.service.db.update_task(
                    task_id,
                    status=TaskStatus.FAILED.value,
                    failure_message=str(exc),
                    finished_at="CURRENT_TIMESTAMP",
                )
                self.service.fail_unclaimed_session_start(int(session["id"]), str(exc))
                continue
            task = self.service.db.get_task(task_id)
            account = self.scheduler.account_by_name(str(allocation.get("account_name") or ""))
            reserved = (
                self.scheduler.reserve_task_on_allocation(task, allocation, account)
                if task and account
                else None
            )
            if not reserved or not account:
                # The global scheduler may win the race after create_task.
                # requested_allocation_id makes that safe; recognize the exact
                # reservation instead of failing a host already being started.
                current = self.service.db.get_task(task_id)
                already_exact = bool(
                    current
                    and int(current.get("requested_allocation_id") or 0)
                    == allocation_id
                    and int(current.get("allocation_id") or 0) == allocation_id
                    and str(current.get("status") or "")
                    in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
                )
                if already_exact:
                    launches_by_allocation[allocation_id] = allocation_launch_index + 1
                    started += 1
                    continue
                license_reason = ""
                if bool(getattr(self.scheduler, "license_admission_enabled", False)):
                    _allowed, license_reason = self.scheduler.aedt_pool_warm_spare_admission(1)
                failure_reason = (
                    license_reason
                    or "could not reserve exact dedicated AEDT allocation"
                )
                self.service.db.update_task(
                    task_id,
                    status=TaskStatus.FAILED.value,
                    failure_message=failure_reason,
                    finished_at="CURRENT_TIMESTAMP",
                )
                self.service.fail_unclaimed_session_start(
                    int(session["id"]),
                    failure_reason,
                )
                continue
            self.service.bind_session_host_task(int(session["id"]), task_id)
            self.scheduler.start_background_task_attach(reserved, allocation, account)
            launches_by_allocation[allocation_id] = allocation_launch_index + 1
            started += 1
        return started

    def _close_empty_dedicated_allocations(self) -> int:
        closed = 0
        active_task_states = {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}
        for allocation in self.service._dedicated_allocations({"warm", "active", "draining"}):
            allocation_id = int(allocation["id"])
            if self.service.allocation_has_counted_session(allocation_id):
                continue
            if any(
                int(task.get("allocation_id") or 0) == allocation_id
                and task.get("status") in active_task_states
                for task in self.service.db.list_tasks_by_statuses(list(active_task_states), limit=10000)
            ):
                continue
            if self.scheduler.close_empty_aedt_pool_allocation(allocation_id):
                closed += 1
        return closed
