from __future__ import annotations

import hashlib
import json
import logging
import math
import os
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

SESSION_COUNTED_STATES = ("starting", "ready", "busy", "draining", "unhealthy")
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
    client_deadline_at TEXT,
    acquired_at TEXT,
    last_heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,
    release_requested_at TEXT,
    finished_at TEXT,
    fault_phase TEXT NOT NULL DEFAULT '',
    fault_kind TEXT NOT NULL DEFAULT '',
    fault_evidence_json TEXT NOT NULL DEFAULT '{}',
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
"""


DEFAULT_SETTINGS: dict[str, str] = {
    # The requested 250/500 topology is staged but deliberately disabled.
    "aedt_pool_enabled": "0",
    "aedt_pool_adapter_ready": "0",
    "aedt_pool_max_sessions": "250",
    "aedt_pool_min_idle_sessions": "0",
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
    family = str(value or "").strip().lower()
    return family or _derive_placement_group(project_name)


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
    ) -> None:
        self.db = db
        self.bootstrap_token = bootstrap_token
        self.lease_client_token = str(lease_client_token or "").strip()
        self._now = now
        self._lock = threading.RLock()
        self._warm_spare_admission_checker: Callable[[int], tuple[int, str]] | None = None
        self._config_cache: AedtPoolConfig | None = None
        self._config_cache_until = 0.0

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
                "client_deadline_at": "TEXT",
                "fault_phase": "TEXT NOT NULL DEFAULT ''",
                "fault_kind": "TEXT NOT NULL DEFAULT ''",
                "fault_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
            }.items():
                if name not in lease_columns:
                    conn.execute(
                        f"ALTER TABLE aedt_project_leases ADD COLUMN {name} {ddl}"
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
        with self._lock:
            self._config_cache = None
            self._config_cache_until = 0.0

    def set_warm_spare_admission_checker(
        self,
        checker: Callable[[int], tuple[int, str]] | None,
    ) -> None:
        """Install the scheduler's fail-closed license headroom check."""
        self._warm_spare_admission_checker = checker

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
        with self._lock:
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
            current.min_idle_sessions
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
        if normalized_isolation == "shared_if_compatible":
            mixed_validation = self.latest_validation()
            if not (
                mixed_validation
                and mixed_validation.get("status") == "passed"
                and bool(
                    mixed_validation.get("mixed_mft_ipmsm_isolation_passed")
                )
            ):
                raise ValueError(
                    "shared_if_compatible requires passed mixed MFT/IPMSM "
                    "isolation validation"
                )
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
                        exclusive_session, state, client_token_hash,
                        last_heartbeat_at, expires_at, client_deadline_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              'queued', ?, ?, ?, ?)
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
                        int(exclusive_session),
                        token_hash,
                        now,
                        expires,
                        _sql_time(deadline),
                    ),
                )
                lease_id = int(cursor.lastrowid)
            if config.operational:
                self._place_queued_leases(
                    conn, now, lease_ids=(lease_id,), config=config
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
        lease = self._authorize_lease(lease_id, token)
        if int(lease.get("protocol_version") or 1) < 2:
            if lease["state"] in {"leased", "active"}:
                return self.get_lease(lease_id, include_secret_hash=False)
            raise ValueError(f"lease is {lease['state']}")
        if lease["state"] in {"attaching", "active"}:
            return self.get_lease(lease_id, include_secret_hash=False)
        if lease["state"] != "offered":
            raise ValueError(f"lease is {lease['state']}")
        now_dt = self._now()
        now = _sql_time(now_dt)
        if str(lease.get("offer_expires_at") or "") <= now:
            raise ValueError("lease offer expired")
        config = self.config()
        with self.db.connect() as conn:
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
                    _sql_time(now_dt + timedelta(seconds=config.lease_ttl_seconds)),
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
        """Atomically seal one Desktop batch before any native solve starts."""

        if session_id <= 0:
            return False
        session = conn.execute(
            "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
        ).fetchone()
        if not session or str(session["state"]) not in {"ready", "busy"}:
            return False
        occupants = conn.execute(
            """
            SELECT id, state, exclusive_session FROM aedt_project_leases
            WHERE session_id = ?
              AND state IN ('offered','leased','attaching','active','releasing')
            ORDER BY id ASC
            """,
            (int(session_id),),
        ).fetchall()
        if not occupants or any(str(row["state"]) != "active" for row in occupants):
            return False
        full = len(occupants) >= int(session["slots_total"] or 1)
        exclusive = any(bool(row["exclusive_session"]) for row in occupants)
        if not (full or exclusive or allow_underfilled):
            return False
        if not str(session["solve_batch_sealed_at"] or ""):
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
        generation = int(
            conn.execute(
                "SELECT solve_batch_generation FROM aedt_sessions WHERE id = ?",
                (int(session_id),),
            ).fetchone()[0]
        )
        conn.execute(
            """
            UPDATE aedt_project_leases
            SET solve_permit_at = COALESCE(solve_permit_at, ?),
                solve_permit_generation = ?, updated_at = ?
            WHERE session_id = ? AND state = 'active'
            """,
            (now, generation, now, int(session_id)),
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
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE id = ? AND state IN ('ready','busy','draining')
                """,
                (
                    quarantine_until,
                    failure_message.strip() or "solver timeout; sibling grace active",
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
                conn.execute(
                    """
                    UPDATE aedt_sessions
                    SET state = 'unhealthy', quarantine_reason = '',
                        failure_message = ?, last_fault_evidence_json = ?,
                        last_fault_at = ?,
                        native_snapshot_path = CASE WHEN ? = ''
                            THEN native_snapshot_path ELSE ? END,
                        drain_requested_at = NULL, last_heartbeat_at = ?,
                        updated_at = ?
                    WHERE id = ? AND state IN ('ready','busy','unhealthy')
                    """,
                    (
                        f"host native liveness suspect: {message}",
                        metadata,
                        now,
                        native_snapshot_path,
                        native_snapshot_path,
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
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)
            ).fetchone()
            if not row:
                raise KeyError(session_id)
            session = dict(row)
            if not session.get("host_token_hash") or not secrets.compare_digest(
                str(session["host_token_hash"]), _token_hash(token)
            ):
                raise PermissionError("invalid session token")
            if session["state"] not in {"ready", "busy", "draining", "unhealthy"}:
                raise ValueError(f"session is {session['state']}")
            if not liveness_confirmed:
                raise ValueError("host heartbeat requires fresh Desktop liveness proof")
            if process_id and str(process_id).strip() != str(session["process_id"] or ""):
                raise ValueError("heartbeat Desktop process_id does not match registration")
            if port:
                try:
                    registered_port = int(str(session["endpoint"] or "").rsplit(":", 1)[1])
                except (IndexError, ValueError):
                    registered_port = 0
                if int(port) != registered_port:
                    raise ValueError("heartbeat Desktop port does not match registration")
            if native_probe and str(native_probe) != "GetVersion":
                raise ValueError("unsupported Desktop native liveness probe")
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
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = ?, failure_message = ?, finished_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        expected_state,
                        normalized_failure,
                        now,
                        now,
                        int(lease_id),
                    ),
                )
                self._refresh_session_state(conn, int(session_id), now)
                if config.operational:
                    self._place_queued_leases(conn, now, config=config)
        return self.get_lease(lease_id, include_secret_hash=False)

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
        session_cpus = max(1, config.project_cpus * slots)
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

    def _plan(self, conn: Any, config: AedtPoolConfig) -> dict[str, Any]:
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
        hard_count = sum(state_counts.get(state, 0) for state in SESSION_COUNTED_STATES)
        idle_count = state_counts.get("ready", 0)
        busy_count = state_counts.get("busy", 0)
        unavailable_count = (
            state_counts.get("draining", 0)
            + state_counts.get("unhealthy", 0)
        )
        live_projects = sum(lease_counts.get(state, 0) for state in LEASE_LIVE_STATES)
        desired_projects = min(config.target_projects, live_projects)
        exclusive_projects = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM aedt_project_leases
                WHERE exclusive_session = 1
                  AND state IN ('queued','offered','leased','attaching','active','releasing')
                """
            ).fetchone()[0]
        )
        desired_exclusive = min(exclusive_projects, desired_projects)
        desired_shared = max(0, desired_projects - desired_exclusive)
        shared_group_counts = conn.execute(
            """
            SELECT effective_placement_group, COUNT(*) AS count
            FROM (
                SELECT CASE
                           WHEN placement_group IS NULL
                                OR TRIM(placement_group) = ''
                           THEN '__legacy_lease_' || id
                           ELSE placement_group
                       END AS effective_placement_group
                FROM aedt_project_leases
                WHERE exclusive_session = 0
                  AND state IN ('queued','offered','leased','attaching','active','releasing')
                ORDER BY id ASC
                LIMIT ?
            )
            GROUP BY effective_placement_group
            """,
            (desired_shared,),
        ).fetchall()
        shared_demand_sessions = sum(
            math.ceil(int(row["count"]) / config.projects_per_session)
            for row in shared_group_counts
        )
        demand_sessions = min(
            config.max_sessions,
            desired_exclusive + shared_demand_sessions,
        )
        # A busy session cannot be consolidated immediately even when its
        # sibling slot is empty.  Preserve every such owner, then add whole
        # ready sessions as the warm-spare target.
        demand_sessions = min(config.max_sessions, max(demand_sessions, busy_count))
        desired_sessions = min(
            config.max_sessions,
            demand_sessions + config.min_idle_sessions + unavailable_count,
        )
        start_needed = max(0, desired_sessions - hard_count)
        demand_start_needed = max(0, demand_sessions - hard_count)
        warm_spare_start_needed = max(0, start_needed - demand_start_needed)
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
        warm_spare_starts_authorized = warm_spare_start_needed
        warm_spare_status_reason = ""
        if warm_spare_start_needed and not config.operational:
            warm_spare_starts_authorized = 0
            warm_spare_status_reason = "AEDT pool is not operational; warm-spare start is gated"
        elif warm_spare_start_needed and self._warm_spare_admission_checker:
            try:
                allowed_total, warm_spare_status_reason = self._warm_spare_admission_checker(
                    unclaimed_starting_sessions
                    + demand_start_needed
                    + warm_spare_start_needed
                )
            except Exception as exc:
                LOGGER.exception("AEDT warm-spare license admission failed")
                allowed_total = 0
                warm_spare_status_reason = f"license admission check failed: {exc}"
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
        start_needed = demand_start_needed + warm_spare_starts_authorized
        warm_spare_deficit = max(0, config.min_idle_sessions - idle_count)
        cap_excess = max(0, hard_count - config.max_sessions)
        demand_excess = max(0, hard_count - desired_sessions)
        idle_cutoff = _sql_time(self._now() - timedelta(seconds=config.idle_ttl_seconds))
        idle_drainable = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM aedt_sessions
                WHERE state = 'ready'
                  AND COALESCE(idle_since, created_at) <= ?
                """,
                (idle_cutoff,),
            ).fetchone()[0]
        )
        allocations = self._eligible_allocations(conn=conn)
        pending_allocations = self._dedicated_allocations({"pending"}, conn=conn)
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
        for allocation in sorted(
            allocations,
            key=lambda row: (current_by_allocation.get(int(row["id"]), 0), int(row["id"])),
        ):
            allocation_id = int(allocation["id"])
            current_sessions = current_by_allocation.get(allocation_id, 0)
            capacity = self._allocation_session_capacity(
                allocation, config, current_sessions=current_sessions
            )
            free = max(0, capacity - current_sessions)
            for _ in range(min(free, start_needed - len(placements))):
                placements.append(
                    {
                        "allocation_id": allocation_id,
                        "account_name": str(allocation.get("account_name") or ""),
                        "node_name": str(allocation.get("node_name") or ""),
                    }
                )
            if len(placements) >= start_needed:
                break
        unplaced = max(0, start_needed - len(placements))
        # Pending dedicated nodes already consume a Slurm request/account slot.
        # Count their future session capacity so every runtime tick does not
        # request another batch while Slurm is still queueing the first one.
        pending_capacity = sum(
            self._allocation_session_capacity(allocation, config)
            for allocation in pending_allocations
        )
        unplaced_after_pending = max(0, unplaced - pending_capacity)
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
        node_requests = (
            math.ceil(unplaced_after_pending / sessions_per_new_node)
            if unplaced_after_pending
            else 0
        )
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
            elif hard_count >= desired_sessions:
                warm_spare_status_reason = (
                    "counted non-idle sessions currently consume the warm-spare capacity"
                )
        return {
            "hard_session_count": hard_count,
            "desired_sessions": desired_sessions,
            "demand_sessions": demand_sessions,
            "start_needed": start_needed,
            "idle_session_count": idle_count,
            "unavailable_session_count": unavailable_count,
            "min_idle_aedt_sessions": config.min_idle_sessions,
            "warm_spare_deficit": warm_spare_deficit,
            "warm_spare_start_needed": warm_spare_start_needed,
            "warm_spare_starts_authorized": warm_spare_starts_authorized,
            "unclaimed_starting_sessions": unclaimed_starting_sessions,
            "warm_spare_status_reason": warm_spare_status_reason,
            "drain_needed": max(cap_excess, min(demand_excess, idle_drainable)),
            "idle_drainable_sessions": idle_drainable,
            "placements": placements,
            "unplaced_sessions": unplaced,
            "pending_node_session_capacity": pending_capacity,
            "node_requests": node_requests,
            "sessions_per_new_node": sessions_per_new_node,
            "state_counts": state_counts,
            "lease_counts": lease_counts,
            "live_projects": live_projects,
            "exclusive_projects": exclusive_projects,
        }

    def dry_run(self) -> dict[str, Any]:
        config = self.config()
        with self.db.connect() as conn:
            return self._plan(conn, config)

    @staticmethod
    def _lease_fits_session(
        lease: Any,
        session: Any,
        occupants: list[Any],
    ) -> bool:
        if int(session["used_slots"] or 0) >= int(session["slots_total"]):
            return False
        if any(bool(item["exclusive_session"]) for item in occupants):
            return False
        if bool(lease["exclusive_session"]) and occupants:
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
            if any(int(item["protocol_version"] or 1) >= 2 for item in occupants):
                return False
            group = str(lease["placement_group"] or "")
            return all(str(item["placement_group"] or "") == group for item in occupants)

        policy = str(lease["isolation_policy"] or "family")
        family = str(lease["workload_family"] or "")
        if policy == "family":
            return all(str(item["workload_family"] or "") == family for item in occupants)
        if policy == "shared_if_compatible":
            return all(
                int(item["protocol_version"] or 1) >= 2
                and str(item["isolation_policy"] or "family")
                == "shared_if_compatible"
                for item in occupants
            )
        return not occupants

    def _place_queued_leases(
        self,
        conn: Any,
        now: str,
        *,
        lease_ids: tuple[int, ...] | None = None,
        config: AedtPoolConfig | None = None,
    ) -> int:
        """Place only admission-ready leases in one short existing transaction."""

        config = config or self.config()
        if not config.operational:
            return 0
        ready_cutoff = _sql_time(
            _parse_utc_time(now) - timedelta(seconds=config.queued_stale_seconds)
        )
        params: tuple[Any, ...] = (ready_cutoff,)
        predicate = ""
        if lease_ids:
            placeholders = ",".join("?" for _ in lease_ids)
            predicate = f" AND id IN ({placeholders})"
            params = (ready_cutoff, *(int(item) for item in lease_ids))
        queued = conn.execute(
            "SELECT * FROM aedt_project_leases "
            "WHERE state = 'queued' "
            "AND (protocol_version < 2 OR last_heartbeat_at >= ?) "
            f"{predicate} ORDER BY id ASC",
            params,
        ).fetchall()
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
                  AND s.solve_batch_sealed_at IS NULL
                  AND a.state IN ('warm','active')
                  AND (? = '' OR a.created_at > ?)
                  AND (? = 0 OR s.allocation_id = ?)
                  AND (? = '' OR s.node_name = ?)
                  AND (
                      ? < 2
                      OR (? >= 2 AND s.session_profile = ?)
                  )
                ORDER BY used_slots DESC, s.id ASC
                """,
                (
                    allocation_age_cutoff,
                    allocation_age_cutoff,
                    int(lease["requested_allocation_id"] or 0),
                    int(lease["requested_allocation_id"] or 0),
                    str(lease["requested_node_name"] or ""),
                    str(lease["requested_node_name"] or ""),
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
                if self._lease_fits_session(lease, session, occupants):
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

    def reconcile(self, *, execute: bool) -> dict[str, Any]:
        """Reap stale ownership, assign leases, and scale only when gated.

        `execute=True` still cannot open a session unless enabled, validated,
        and adapter-ready.  This makes calls from lease/heartbeat endpoints safe
        before the pooled backend is approved.
        """
        config = self.config()
        now_dt = self._now()
        now = _sql_time(now_dt)
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")

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
                      AND COALESCE(last_heartbeat_at, started_at, created_at) < ?
                      AND COALESCE(
                          drain_requested_at, last_heartbeat_at,
                          updated_at, created_at
                      ) < ?
                    """,
                    (reap_cutoff, unhealthy_recycle_cutoff),
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
            # Admission placement is deliberately short and shares the same
            # helper used by POST /leases.  Full expiry/scale reconciliation
            # stays on this background tick instead of every HTTP request.
            if execute and config.operational:
                self._place_queued_leases(conn, now, config=config)

            plan = self._plan(conn, config)
            if execute and config.operational:
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
                plan = self._plan(conn, config)
            plan["operational"] = config.operational
            plan["executed"] = bool(execute and config.operational)
            return plan

    def summary(self) -> dict[str, Any]:
        config = self.config()
        plan = self.dry_run()
        latest = self.latest_validation()
        live_state_placeholders = ", ".join("?" for _ in SESSION_COUNTED_STATES)
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
                    SESSION_COUNTED_STATES,
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
                    config.project_cpus * config.projects_per_session
                ),
                "session_reserved_memory_mb": (
                    config.project_memory_mb * config.projects_per_session
                ),
                "control_plane_url": config.control_plane_url,
                "hard_counted_states": list(SESSION_COUNTED_STATES),
            },
            "plan": plan,
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
        plan = self.service.reconcile(execute=True)
        if self.require_published_control_plane_url:
            plan["control_plane_ready"] = True
            plan["control_plane_error"] = ""
        if not config.operational:
            return plan
        requests = min(int(plan.get("node_requests") or 0), config.scale_step_nodes)
        opened = 0
        for _ in range(requests):
            allocation = self.scheduler.open_allocation_record(
                "AEDT pool project demand",
                resource_pool="cpu",
                required_capability=config.required_capability,
                env_profile=config.env_profile,
                account_name=config.account_name,
                # Ask for the scheduler's normal full node shape.  Requesting
                # only 8 CPUs here would accidentally create one-AEDT nodes.
                requested_cpus=max(
                    int(getattr(self.scheduler, "allocation_cpus", 0) or 0),
                    config.project_cpus * config.projects_per_session,
                ),
                require_fea_eligible_node=True,
            )
            if not allocation:
                break
            opened += 1
        plan["node_allocations_opened"] = opened
        if control_plane_just_published:
            # Publishing precedes full tunnel readiness.  One normal tick is a
            # small, bounded grace that avoids launching into that half-up gap.
            plan["host_tasks_started"] = 0
        else:
            plan["host_tasks_started"] = self._ensure_session_hosts(
                config, control_plane_url=control_plane_url
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
        launches_by_allocation: dict[int, int] = {}
        for session in self.service.starting_sessions():
            if str(session.get("host_id") or ""):
                continue
            dedupe_key = f"aedt-session-host:{int(session['id'])}"
            if self.service.db.find_active_task_by_dedupe_key(dedupe_key):
                continue
            allocation_id = int(session["allocation_id"])
            allocation = self.service.db.get_allocation(allocation_id)
            if not allocation or allocation.get("state") not in {"warm", "active"}:
                continue
            allocation_launch_index = launches_by_allocation.get(allocation_id, 0)
            launch_delay_seconds = (
                allocation_launch_index * self.host_launch_stagger_seconds
            )
            task_id = self.service.db.create_task(
                # One task owns the Desktop and every concurrent solver in its
                # session, so Slurm must reserve the aggregate resources.
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
                    cpus=(
                        config.project_cpus * max(1, int(session["slots_total"]))
                    ),
                    memory_mb=max(
                        self.host_task_memory_mb,
                        config.project_memory_mb
                        * max(1, int(session["slots_total"])),
                    ),
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    node_name=str(allocation.get("node_name") or ""),
                    priority=100000,
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
