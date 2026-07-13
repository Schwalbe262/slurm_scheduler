from __future__ import annotations

import hashlib
import json
import logging
import math
import secrets
import shlex
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .db import Database
from .models import SchedulingProfile, TaskCreate, TaskStatus


LOGGER = logging.getLogger(__name__)

SESSION_COUNTED_STATES = ("starting", "ready", "busy", "draining", "unhealthy")
SESSION_ASSIGNABLE_STATES = ("ready", "busy")
LEASE_LIVE_STATES = ("queued", "leased", "active", "releasing")
LEASE_SLOT_STATES = ("leased", "active", "releasing")


AEDT_POOL_SCHEMA = """
CREATE TABLE IF NOT EXISTS aedt_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key TEXT NOT NULL UNIQUE,
    allocation_id INTEGER NOT NULL DEFAULT 0,
    account_name TEXT NOT NULL DEFAULT '',
    node_name TEXT NOT NULL DEFAULT '',
    host_id TEXT NOT NULL DEFAULT '',
    endpoint TEXT NOT NULL DEFAULT '',
    process_id TEXT NOT NULL DEFAULT '',
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
    acquired_at TEXT,
    last_heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TEXT NOT NULL,
    release_requested_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_aedt_leases_state ON aedt_project_leases(state);
CREATE INDEX IF NOT EXISTS idx_aedt_leases_session ON aedt_project_leases(session_id);
CREATE INDEX IF NOT EXISTS idx_aedt_leases_task ON aedt_project_leases(task_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_aedt_leases_live_slot
ON aedt_project_leases(session_id, slot_index)
WHERE session_id IS NOT NULL AND state IN ('leased', 'active', 'releasing');

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
    "aedt_pool_node_cpu_factor": "1.0",
    "aedt_pool_lease_ttl_seconds": "180",
    "aedt_pool_session_heartbeat_timeout_seconds": "120",
    "aedt_pool_session_start_timeout_seconds": "600",
    "aedt_pool_idle_ttl_seconds": "900",
    "aedt_pool_scale_step_nodes": "4",
    "aedt_pool_required_capability": "",
    "aedt_pool_env_profile": "",
    "aedt_pool_account_name": "",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sql_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


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
    node_cpu_factor: float
    lease_ttl_seconds: int
    session_heartbeat_timeout_seconds: int
    session_start_timeout_seconds: int
    idle_ttl_seconds: int
    scale_step_nodes: int
    required_capability: str
    env_profile: str
    account_name: str

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
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        self.db = db
        self.bootstrap_token = bootstrap_token
        self._now = now
        self._lock = threading.RLock()
        self._warm_spare_admission_checker: Callable[[int], tuple[int, str]] | None = None

    def init(self) -> None:
        with self.db.connect() as conn:
            conn.executescript(AEDT_POOL_SCHEMA)
            # Additive migration for databases created by an early pilot.
            session_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(aedt_sessions)").fetchall()
            }
            for name, ddl in {
                "quarantine_until": "TEXT",
                "quarantine_reason": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if name not in session_columns:
                    conn.execute(f"ALTER TABLE aedt_sessions ADD COLUMN {name} {ddl}")
            lease_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(aedt_project_leases)"
                ).fetchall()
            }
            if "exclusive_session" not in lease_columns:
                conn.execute(
                    "ALTER TABLE aedt_project_leases ADD COLUMN "
                    "exclusive_session INTEGER NOT NULL DEFAULT 0"
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
            ):
                if name not in validation_columns:
                    conn.execute(
                        f"ALTER TABLE aedt_pool_validations ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0"
                    )

    def _setting(self, key: str) -> str:
        value = self.db.get_setting(key)
        return DEFAULT_SETTINGS[key] if value is None else value

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
        validation = self.latest_validation()
        return AedtPoolConfig(
            enabled=_bool_setting(self._setting("aedt_pool_enabled")),
            adapter_ready=_bool_setting(self._setting("aedt_pool_adapter_ready")),
            validation_passed=bool(validation and validation.get("status") == "passed"),
            max_sessions=max(0, int(self._setting("aedt_pool_max_sessions"))),
            min_idle_sessions=max(0, int(self._setting("aedt_pool_min_idle_sessions"))),
            target_projects=max(0, int(self._setting("aedt_pool_target_projects"))),
            projects_per_session=max(1, int(self._setting("aedt_pool_projects_per_session"))),
            project_cpus=max(1, int(self._setting("aedt_pool_project_cpus"))),
            node_cpu_factor=max(1.0, min(2.0, float(self._setting("aedt_pool_node_cpu_factor")))),
            lease_ttl_seconds=max(30, int(self._setting("aedt_pool_lease_ttl_seconds"))),
            session_heartbeat_timeout_seconds=max(
                30, int(self._setting("aedt_pool_session_heartbeat_timeout_seconds"))
            ),
            session_start_timeout_seconds=max(
                60, int(self._setting("aedt_pool_session_start_timeout_seconds"))
            ),
            idle_ttl_seconds=max(0, int(self._setting("aedt_pool_idle_ttl_seconds"))),
            scale_step_nodes=max(1, int(self._setting("aedt_pool_scale_step_nodes"))),
            required_capability=self._setting("aedt_pool_required_capability").strip(),
            env_profile=self._setting("aedt_pool_env_profile").strip(),
            account_name=self._setting("aedt_pool_account_name").strip(),
        )

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
        if type(requested_slots) is not int or not 1 <= requested_slots <= 2:
            raise ValueError("projects_per_aedt must be an integer between 1 and 2")
        if target_projects is None:
            requested_target = requested_max * requested_slots
        else:
            requested_target = target_projects
        if type(requested_target) is not int or not 0 <= requested_target <= 1100:
            raise ValueError(
                "target_project_concurrency must be an integer between 0 and 1100"
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
        return self.config()

    def set_adapter_ready(self, ready: bool) -> AedtPoolConfig:
        """Deployment hook, intentionally not exposed as an operator UI toggle."""
        if ready and not self.bootstrap_token:
            raise ValueError("a non-empty session-host bootstrap token is required")
        self.db.set_setting("aedt_pool_adapter_ready", "1" if ready else "0")
        if not ready and self.config().enabled:
            self._request_all_sessions_drain("session-host adapter disabled")
        return self.config()

    def set_enabled(self, enabled: bool) -> AedtPoolConfig:
        current = self.config()
        if enabled and not current.validation_passed:
            raise ValueError("1-AEDT:2-project validation has not passed")
        if enabled and not current.adapter_ready:
            raise ValueError("AEDT session-host adapter is not ready")
        self.db.set_setting("aedt_pool_enabled", "1" if enabled else "0")
        if not enabled:
            self._request_all_sessions_drain("operator disabled AEDT pool")
        return self.config()

    def _request_all_sessions_drain(self, reason: str) -> None:
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            # Starting is left claimable: a host may already have launched AEDT
            # between claim and register. register_session observes disabled
            # state and registers it directly as draining.
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'draining', failure_message = ?,
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE state IN ('ready','busy')
                """,
                (reason, now, now),
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
                    evidence_json, failure_message,
                    finished_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    json.dumps(evidence, ensure_ascii=False, separators=(",", ":")),
                    "; ".join(failures),
                    now,
                    now,
                ),
            )
            validation_id = int(cursor.lastrowid)
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
        if type(exclusive_session) is not bool:
            raise ValueError("exclusive_session must be a boolean")
        token = secrets.token_urlsafe(32)
        config = self.config()
        expires = _sql_time(self._now() + timedelta(seconds=config.lease_ttl_seconds))
        with self.db.connect() as conn:
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO aedt_project_leases (
                        request_key, project_name, task_id,
                        requested_allocation_id, requested_node_name,
                        exclusive_session, state, client_token_hash, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?)
                    """,
                    (
                        request_key,
                        project_name,
                        max(0, int(task_id)),
                        max(0, int(allocation_id)),
                        node_name.strip(),
                        int(exclusive_session),
                        _token_hash(token),
                        expires,
                    ),
                )
            except Exception as exc:
                if "UNIQUE constraint failed" in str(exc):
                    raise ValueError("request_key already exists; reuse the original client token") from exc
                raise
            lease_id = int(cursor.lastrowid)
        self.reconcile(execute=True)
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
                       s.allocation_id AS session_allocation_id
                FROM aedt_project_leases l
                LEFT JOIN aedt_sessions s ON s.id = l.session_id
                WHERE l.id = ?
                """,
                (int(lease_id),),
            ).fetchone()
            if not row:
                raise KeyError(lease_id)
            item = dict(row)
            if not include_secret_hash:
                item.pop("client_token_hash", None)
            return item

    def lease_status(self, lease_id: int, token: str) -> dict[str, Any]:
        self._authorize_lease(lease_id, token)
        return self.get_lease(lease_id, include_secret_hash=False)

    def heartbeat_lease(self, lease_id: int, token: str) -> dict[str, Any]:
        lease = self._authorize_lease(lease_id, token)
        if lease["state"] not in {"queued", "leased", "active", "releasing"}:
            raise ValueError(f"lease is {lease['state']}")
        config = self.config()
        now = self._now()
        next_state = "active" if lease["state"] == "leased" else lease["state"]
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = ?, last_heartbeat_at = ?, expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    next_state,
                    _sql_time(now),
                    _sql_time(now + timedelta(seconds=config.lease_ttl_seconds)),
                    _sql_time(now),
                    int(lease_id),
                ),
            )
        return self.get_lease(lease_id, include_secret_hash=False)

    def bind_lease_project_name(
        self, lease_id: int, token: str, project_name: str
    ) -> dict[str, Any]:
        lease = self._authorize_lease(lease_id, token)
        project_name = project_name.strip()
        if not project_name:
            raise ValueError("project_name is required")
        if lease["state"] not in {"queued", "leased", "active"}:
            raise ValueError(f"lease is {lease['state']}")
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET project_name = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (project_name, int(lease_id)),
            )
        return self.get_lease(lease_id, include_secret_hash=False)

    def release_lease(self, lease_id: int, token: str) -> dict[str, Any]:
        lease = self._authorize_lease(lease_id, token)
        if lease["state"] in {"released", "failed", "cancelled", "expired"}:
            return self.get_lease(lease_id, include_secret_hash=False)
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            if lease.get("session_id"):
                # Two phase release: only the session host can confirm that the
                # project is closed and make this slot reusable.
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'releasing', release_requested_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, int(lease_id)),
                )
            else:
                conn.execute(
                    """
                    UPDATE aedt_project_leases
                    SET state = 'cancelled', finished_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, int(lease_id)),
                )
        self.reconcile(execute=True)
        return self.get_lease(lease_id, include_secret_hash=False)

    def report_project_fault(
        self,
        lease_id: int,
        token: str,
        *,
        fault_kind: str,
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
        if normalized not in {"pre_solve", "script_error", "solver_timeout", "aedt_death"}:
            raise ValueError("fault_kind must be pre_solve, script_error, solver_timeout, or aedt_death")
        if normalized in {"pre_solve", "script_error"}:
            return self.release_lease(lease_id, token)
        session_id = int(lease.get("session_id") or 0)
        if not session_id:
            return self.release_lease(lease_id, token)
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
                WHERE id = ? AND state IN ('leased','active')
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

    def _authorize_bootstrap(self, token: str) -> None:
        if not self.bootstrap_token or not secrets.compare_digest(self.bootstrap_token, token):
            raise PermissionError("invalid session-host bootstrap token")

    def claim_start(
        self,
        *,
        allocation_id: int,
        node_name: str,
        host_id: str,
        bootstrap_token: str,
    ) -> dict[str, Any] | None:
        self._authorize_bootstrap(bootstrap_token)
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM aedt_sessions
                WHERE state = 'starting' AND host_id = ''
                  AND allocation_id = ? AND node_name = ?
                ORDER BY id ASC LIMIT 1
                """,
                (int(allocation_id), node_name.strip()),
            ).fetchone()
            if not row:
                return None
            conn.execute(
                "UPDATE aedt_sessions SET host_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND host_id = ''",
                (host_id.strip(), int(row["id"])),
            )
            return dict(conn.execute("SELECT * FROM aedt_sessions WHERE id = ?", (int(row["id"]),)).fetchone())

    def register_session(
        self,
        *,
        session_id: int,
        host_id: str,
        endpoint: str,
        process_id: str,
        bootstrap_token: str,
    ) -> tuple[dict[str, Any], str]:
        self._authorize_bootstrap(bootstrap_token)
        host_token = secrets.token_urlsafe(32)
        now = _sql_time(self._now())
        register_state = "ready" if self.config().enabled else "draining"
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM aedt_sessions WHERE id = ?", (int(session_id),)).fetchone()
            if not row:
                raise KeyError(session_id)
            if row["state"] != "starting" or str(row["host_id"] or "") != host_id.strip():
                raise ValueError("session start claim is not owned by this host")
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = ?, endpoint = ?, process_id = ?,
                    host_token_hash = ?, started_at = ?, last_heartbeat_at = ?,
                    idle_since = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    register_state,
                    endpoint.strip(),
                    process_id.strip(),
                    _token_hash(host_token),
                    now,
                    now,
                    now,
                    now,
                    int(session_id),
                ),
            )
        self.reconcile(execute=True)
        return self.get_session(session_id, include_secret_hash=False), host_token

    def fail_session_start(
        self,
        *,
        session_id: int,
        host_id: str,
        bootstrap_token: str,
        failure_message: str,
    ) -> dict[str, Any]:
        self._authorize_bootstrap(bootstrap_token)
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'failed', failure_message = ?, closed_at = ?, updated_at = ?
                WHERE id = ? AND state = 'starting' AND host_id = ?
                """,
                (failure_message.strip() or "AEDT session start failed", now, now, int(session_id), host_id.strip()),
            )
            if cursor.rowcount != 1:
                raise ValueError("session start claim is not owned by this host")
        return self.get_session(session_id, include_secret_hash=False)

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

    def heartbeat_session(self, session_id: int, token: str) -> dict[str, Any]:
        session = self._authorize_session(session_id, token)
        if session["state"] not in {"ready", "busy", "draining"}:
            raise ValueError(f"session is {session['state']}")
        now = _sql_time(self._now())
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET last_heartbeat_at = ?, updated_at = ? WHERE id = ?",
                (now, now, int(session_id)),
            )
        return self.get_session(session_id, include_secret_hash=False)

    def session_commands(self, session_id: int, token: str) -> dict[str, Any]:
        session = self._authorize_session(session_id, token)
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, request_key, project_name, task_id, slot_index, state, failure_message
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
                    WHERE session_id = ? AND state IN ('leased','active')
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
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ? AND session_id = ?",
                (int(lease_id), int(session_id)),
            ).fetchone()
            if not lease:
                raise KeyError(lease_id)
            if lease["state"] != "releasing":
                raise ValueError(f"lease is {lease['state']}")
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET state = ?, failure_message = ?, finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    "released" if success else "failed",
                    "" if success else failure_message.strip(),
                    now,
                    now,
                    int(lease_id),
                ),
            )
            self._refresh_session_state(conn, int(session_id), now)
        self.reconcile(execute=True)
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
                WHERE session_id = ? AND state IN ('leased','active','releasing')
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
                            failure_message = ?, last_heartbeat_at = ?, expires_at = ?,
                            updated_at = ?
                        WHERE session_id = ? AND state IN ('leased','active','releasing')
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
                        WHERE session_id = ? AND state IN ('leased','active','releasing')
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
                        drain_reason = 'AEDT pool faulted Desktop allocation recycle',
                        drain_at = COALESCE(drain_at, ?), updated_at = ?
                    WHERE id = ? AND state IN ('warm','active','draining')
                    """,
                    (now, now, int(session_row["allocation_id"])),
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
                WHERE session_id = ? AND state IN ('leased','active','releasing')
                """,
                (session_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            UPDATE aedt_sessions
            SET state = ?, idle_since = ?, updated_at = ?
            WHERE id = ?
            """,
            ("busy" if live else "ready", None if live else now, now, session_id),
        )

    def _dedicated_allocations(self, states: set[str]) -> list[dict[str, Any]]:
        # Dedicated allocation ownership prevents this opt-in pool from
        # silently placing AEDT hosts inside an unrelated production campaign.
        return [
            row
            for row in self.db.list_allocations_with_live(limit=1000, live_limit=10000)
            if row.get("state") in states
            and str(row.get("drain_reason") or "").startswith("AEDT pool")
        ]

    def _eligible_allocations(self) -> list[dict[str, Any]]:
        return [
            row
            for row in self._dedicated_allocations({"warm", "active"})
            if str(row.get("node_name") or "").strip()
        ]

    @staticmethod
    def _allocation_session_capacity(allocation: dict[str, Any], config: AedtPoolConfig) -> int:
        project_slots = math.floor(
            max(0, int(allocation.get("total_cpus") or 0))
            * config.node_cpu_factor
            / config.project_cpus
        )
        return max(1, project_slots // config.projects_per_session)

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
                  AND state IN ('queued','leased','active','releasing')
                """
            ).fetchone()[0]
        )
        desired_exclusive = min(exclusive_projects, desired_projects)
        desired_shared = max(0, desired_projects - desired_exclusive)
        demand_sessions = min(
            config.max_sessions,
            desired_exclusive + (
                math.ceil(desired_shared / config.projects_per_session)
                if desired_shared else 0
            ),
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
        allocations = self._eligible_allocations()
        pending_allocations = self._dedicated_allocations({"pending"})
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
            capacity = self._allocation_session_capacity(allocation, config)
            free = max(0, capacity - current_by_allocation.get(allocation_id, 0))
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
        sessions_per_new_node = max(
            1,
            math.floor(shape_cpus * config.node_cpu_factor / config.project_cpus)
            // config.projects_per_session,
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

            # A dead client may still have a solve executing in AEDT.  Never
            # reuse that slot: request project cleanup and drain the session.
            expired = conn.execute(
                """
                SELECT id, session_id FROM aedt_project_leases
                WHERE state IN ('queued','leased','active') AND expires_at < ?
                """,
                (now,),
            ).fetchall()
            for row in expired:
                if row["session_id"]:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'releasing', failure_message = 'lease heartbeat expired',
                            release_requested_at = ?, updated_at = ? WHERE id = ?
                        """,
                        (now, now, int(row["id"])),
                    )
                    conn.execute(
                        """
                        UPDATE aedt_sessions SET state = 'draining',
                            quarantine_reason = 'lease_heartbeat_expired',
                            quarantine_until = COALESCE(quarantine_until, ?),
                            failure_message = 'project lease heartbeat expired',
                            drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                        WHERE id = ? AND state IN ('ready','busy')
                        """,
                        (
                            _sql_time(now_dt + timedelta(seconds=900)),
                            now,
                            now,
                            int(row["session_id"]),
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET state = 'expired', failure_message = 'lease request heartbeat expired',
                            finished_at = ?, updated_at = ? WHERE id = ?
                        """,
                        (now, now, int(row["id"])),
                    )

            heartbeat_cutoff = _sql_time(
                now_dt - timedelta(seconds=config.session_heartbeat_timeout_seconds)
            )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'unhealthy', failure_message = 'session heartbeat expired',
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE state IN ('ready','busy','draining')
                  AND COALESCE(last_heartbeat_at, started_at, created_at) < ?
                """,
                (now, now, heartbeat_cutoff),
            )
            start_cutoff = _sql_time(now_dt - timedelta(seconds=config.session_start_timeout_seconds))
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'unhealthy', failure_message = 'session start acknowledgement timed out',
                    drain_requested_at = COALESCE(drain_requested_at, ?), updated_at = ?
                WHERE state = 'starting' AND created_at < ?
                """,
                (now, now, start_cutoff),
            )
            conn.execute(
                """
                UPDATE allocations
                SET state = 'draining',
                    drain_reason = 'AEDT pool unhealthy/quarantined session allocation recycle',
                    drain_at = COALESCE(drain_at, ?), updated_at = ?
                WHERE id IN (
                    SELECT allocation_id FROM aedt_sessions
                    WHERE state = 'unhealthy' OR quarantine_reason != ''
                ) AND state IN ('warm','active','draining')
                """,
                (now, now),
            )

            # Assign oldest requests to a same-allocation/same-node session.
            # Empty placement constraints allow the controller to choose any
            # dedicated AEDT allocation; explicit constraints are never relaxed.
            if execute and config.operational:
                queued = conn.execute(
                    "SELECT * FROM aedt_project_leases WHERE state = 'queued' ORDER BY id ASC"
                ).fetchall()
                for lease in queued:
                    sessions = conn.execute(
                        """
                        SELECT s.*,
                               (SELECT COUNT(*) FROM aedt_project_leases l
                                WHERE l.session_id = s.id
                                  AND l.state IN ('leased','active','releasing')) AS used_slots,
                               (SELECT COUNT(*) FROM aedt_project_leases l
                                WHERE l.session_id = s.id
                                  AND l.state IN ('leased','active','releasing')
                                  AND l.exclusive_session = 1) AS exclusive_slots
                        FROM aedt_sessions s
                        WHERE s.state IN ('ready','busy')
                          AND (? = 0 OR s.allocation_id = ?)
                          AND (? = '' OR s.node_name = ?)
                        ORDER BY used_slots ASC, s.id ASC
                        """,
                        (
                            int(lease["requested_allocation_id"] or 0),
                            int(lease["requested_allocation_id"] or 0),
                            str(lease["requested_node_name"] or ""),
                            str(lease["requested_node_name"] or ""),
                        ),
                    ).fetchall()
                    selected = next(
                        (
                            row for row in sessions
                            if int(row["used_slots"] or 0) < int(row["slots_total"])
                            and int(row["exclusive_slots"] or 0) == 0
                            and (
                                not bool(lease["exclusive_session"])
                                or int(row["used_slots"] or 0) == 0
                            )
                        ),
                        None,
                    )
                    if not selected:
                        continue
                    occupied = {
                        int(row[0])
                        for row in conn.execute(
                            """
                            SELECT slot_index FROM aedt_project_leases
                            WHERE session_id = ? AND state IN ('leased','active','releasing')
                            """,
                            (int(selected["id"]),),
                        ).fetchall()
                    }
                    slot_index = next(
                        index for index in range(int(selected["slots_total"])) if index not in occupied
                    )
                    conn.execute(
                        """
                        UPDATE aedt_project_leases
                        SET session_id = ?, slot_index = ?, state = 'leased',
                            acquired_at = ?, updated_at = ?
                        WHERE id = ? AND state = 'queued'
                        """,
                        (int(selected["id"]), slot_index, now, now, int(lease["id"])),
                    )
                    self._refresh_session_state(conn, int(selected["id"]), now)

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
                                  AND l.state IN ('leased','active','releasing')) AS used_slots
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
        with self.db.connect() as conn:
            sessions = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM aedt_sessions ORDER BY id DESC LIMIT 500"
                ).fetchall()
            ]
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
                "hard_counted_states": list(SESSION_COUNTED_STATES),
            },
            "plan": plan,
            "latest_validation": latest,
            "sessions": sessions,
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
        host_task_memory_mb: int = 4096,
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
        plan = self.service.reconcile(execute=True)
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
        plan["host_tasks_started"] = self._ensure_session_hosts(config)
        plan["empty_allocations_closed"] = self._close_empty_dedicated_allocations()
        return plan

    @property
    def host_launch_configured(self) -> bool:
        return bool(
            self.scheduler_url
            and self.host_remote_cwd
            and self.host_bootstrap_token_file
        )

    def _host_command(self, session: dict[str, Any]) -> str:
        parts = [
            self.host_python,
            "-m",
            "slurm_scheduler.aedt_session_host",
            "--scheduler-url",
            self.scheduler_url,
            "--allocation-id",
            str(int(session["allocation_id"])),
            "--node-name",
            str(session["node_name"]),
            "--bootstrap-token-file",
            self.host_bootstrap_token_file,
        ]
        return " ".join(shlex.quote(part) for part in parts)

    def _ensure_session_hosts(self, config: AedtPoolConfig) -> int:
        if not config.operational or not self.host_launch_configured:
            return 0
        started = 0
        for session in self.service.starting_sessions():
            if str(session.get("host_id") or ""):
                continue
            dedupe_key = f"aedt-session-host:{int(session['id'])}"
            if self.service.db.find_active_task_by_dedupe_key(dedupe_key):
                continue
            allocation = self.service.db.get_allocation(int(session["allocation_id"]))
            if not allocation or allocation.get("state") not in {"warm", "active"}:
                continue
            task_id = self.service.db.create_task(
                TaskCreate(
                    name=f"aedt-session-host-{int(session['id'])}",
                    remote_cwd=self.host_remote_cwd,
                    command=self._host_command(session),
                    env_setup=self.host_env_setup,
                    required_capability=config.required_capability,
                    env_profile=config.env_profile,
                    account_name=str(allocation.get("account_name") or ""),
                    cpus=1,
                    memory_mb=self.host_task_memory_mb,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    node_name=str(allocation.get("node_name") or ""),
                    priority=100000,
                    timeout_seconds=0,
                    dedupe_key=dedupe_key,
                    project="_aedt_pool_hosts",
                    entrypoint="slurm_scheduler.aedt_session_host",
                )
            )
            task = self.service.db.get_task(task_id)
            account = self.scheduler.account_by_name(str(allocation.get("account_name") or ""))
            reserved = (
                self.scheduler.reserve_task_on_allocation(task, allocation, account)
                if task and account
                else None
            )
            if not reserved or not account:
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
            self.scheduler.start_background_task_attach(reserved, allocation, account)
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
