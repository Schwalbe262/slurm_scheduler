"""Loopback AEDT pool control planes used by isolated protocol tests."""

from __future__ import annotations

import json
import os
import re
import secrets
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from slurm_scheduler.aedt_automation_lock import automation_lock_path
from slurm_scheduler.aedt_session_host import (
    EXPECTED_AEDT_VERSION,
    EXPECTED_SESSION_PROFILE_JSON,
    canonical_expected_session_profile,
)


TERMINAL_LEASE_STATES = {"released", "failed", "cancelled", "expired"}
SESSION_LIVE_LEASE_STATES = {
    "offered",
    "leased",
    "attaching",
    "active",
    "releasing",
}
SESSION_COMMAND_LIVE_LEASE_STATES = {
    "offered",
    "leased",
    "attaching",
    "active",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ExclusiveControlPlane:
    """Minimal loopback protocol implementation for one exclusive project."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.bootstrap_token = secrets.token_urlsafe(24)
        self.host_token = ""
        self.client_token = ""
        self.session = {
            "id": 1,
            "session_key": "pilot-session-1",
            "generation": 1,
            "state": "starting",
            "host_id": "",
            "endpoint": "",
            "process_id": "",
            "slots_total": 1,
            "session_profile": EXPECTED_SESSION_PROFILE_JSON,
            "artifact_dir": "",
            "error_log_path": "",
            "journal_path": "",
            "runtime_metadata": {},
        }
        self.lease: dict[str, Any] | None = None
        self.project_close_ack = False
        self.closed_ack = False
        self.quarantine_reason = ""
        self.solve_permit_generation = 0
        self.host_heartbeat_count = 0
        self.lease_heartbeat_count = 0
        self.events: list[dict[str, Any]] = []

    def event(self, name: str, **values: Any) -> None:
        self.events.append({"time": _now(), "event": name, **values})

    def force_drain(self, reason: str) -> None:
        """Quarantine the disposable session without touching any other task."""
        with self.lock:
            if self.lease and self.lease["state"] not in TERMINAL_LEASE_STATES:
                self.lease["state"] = "releasing"
                self.lease["failure_message"] = reason
                self.lease["fault_kind"] = "pilot_force_drain"
            self.session["state"] = "draining"
            self.event("pilot_force_drain", reason=reason)

    def _offer_lease(self) -> None:
        if self.lease is None or self.lease["state"] != "queued":
            return
        if (
            self.session["state"] not in {"ready", "busy"}
            or not self.session["endpoint"]
            or not self.session["process_id"]
            or not self.session["artifact_dir"]
        ):
            return
        self.lease.update({
            "state": "offered",
            "endpoint": self.session["endpoint"],
            "session_id": int(self.session["id"]),
            "slot_index": 0,
        })
        self.session["state"] = "busy"
        self.event("lease_offered", lease_id=1)

    def _grant_solve_permit(self) -> None:
        if self.lease is None or self.lease["state"] != "active":
            return
        if self.lease.get("solve_permit_granted"):
            return
        self.solve_permit_generation += 1
        self.lease.update({
            "solve_permit_granted": True,
            "solve_permit_generation": self.solve_permit_generation,
        })
        self.event(
            "solve_permit_granted",
            lease_id=1,
            generation=self.solve_permit_generation,
        )

    def _public_lease(self) -> dict[str, Any]:
        if self.lease is None:
            raise KeyError("lease")
        item = dict(self.lease)
        assigned = int(item.get("session_id") or 0) == int(self.session["id"])
        generation = int(item.get("solve_permit_generation") or 0)
        cohort = (
            [self.lease]
            if generation > 0
            and int(self.lease.get("solve_permit_generation") or 0) == generation
            and bool(self.lease.get("solve_permit_granted"))
            else []
        )
        completed_count = sum(
            bool(lease.get("native_pipeline_completed")) for lease in cohort
        )
        broken_count = sum(
            not bool(lease.get("native_pipeline_completed"))
            and lease["state"] != "active"
            for lease in cohort
        )
        native_completed = bool(item.get("native_pipeline_completed"))
        item.update({
            "legacy_state": (
                "leased"
                if item["state"] in {"offered", "attaching"}
                else item["state"]
            ),
            "session_key": self.session["session_key"] if assigned else "",
            "session_generation": (
                int(self.session["generation"]) if assigned else 0
            ),
            "session_process_id": (
                str(self.session["process_id"]) if assigned else ""
            ),
            "expected_aedt_version": (
                EXPECTED_AEDT_VERSION if assigned else ""
            ),
            "automation_lock_path": (
                automation_lock_path(str(self.session["artifact_dir"]))
                if assigned
                else ""
            ),
            "session_slots_total": int(self.session["slots_total"]),
            "session_live_lease_count": int(
                self.lease["state"] in SESSION_LIVE_LEASE_STATES
            ),
            "session_active_lease_count": int(
                self.lease["state"] == "active"
            ),
            "solve_permit_required": bool(
                self.lease["state"] == "active"
                and not self.lease.get("solve_permit_granted")
            ),
            "native_pipeline_completed": native_completed,
            "native_pipeline_expected_count": len(cohort),
            "native_pipeline_completed_count": completed_count,
            "native_pipeline_barrier_granted": bool(
                native_completed
                and cohort
                and completed_count == len(cohort)
            ),
            "native_pipeline_barrier_broken": bool(
                cohort and completed_count != len(cohort) and broken_count
            ),
        })
        return item

    def dispatch(
        self,
        method: str,
        path: str,
        payload: dict[str, Any],
        headers: Any,
    ) -> tuple[int, dict[str, Any]]:
        with self.lock:
            if method == "POST" and path == "/api/aedt-pool/leases":
                if self.lease is not None:
                    return 409, {"detail": "pilot permits exactly one lease"}
                if payload.get("exclusive_session") is not True:
                    return 422, {"detail": "pilot requires exclusive_session=true"}
                if type(payload.get("protocol_version")) is not int \
                        or int(payload["protocol_version"]) != 2:
                    return 422, {"detail": "pilot requires protocol_version=2"}
                try:
                    session_profile = canonical_expected_session_profile(
                        payload.get("session_profile")
                    )
                except (TypeError, ValueError) as exc:
                    return 422, {"detail": str(exc)}
                workload_family = str(
                    payload.get("workload_family") or ""
                ).strip()
                if not workload_family:
                    return 422, {
                        "detail": "protocol-v2 workload_family is required"
                    }
                workspace_path = str(
                    payload.get("workspace_path") or ""
                ).strip()
                if not workspace_path or not os.path.isabs(workspace_path):
                    return 422, {
                        "detail": (
                            "protocol-v2 workspace_path must be an absolute path"
                        )
                    }
                task_id = int(payload.get("task_id") or 0)
                if task_id <= 0:
                    return 422, {
                        "detail": "protocol-v2 task_id must be positive"
                    }
                isolation_policy = str(
                    payload.get("isolation_policy") or ""
                ).strip().lower()
                if isolation_policy != "exclusive":
                    return 422, {
                        "detail": "1:1 pilot requires isolation_policy=exclusive"
                    }
                self.client_token = str(
                    payload.get("client_token") or secrets.token_urlsafe(32)
                )
                self.lease = {
                    "id": 1,
                    "state": (
                        "offered"
                        if self.session["state"] in {"ready", "busy"}
                        and bool(self.session["endpoint"])
                        else "queued"
                    ),
                    "endpoint": (
                        self.session["endpoint"]
                        if self.session["state"] in {"ready", "busy"}
                        else ""
                    ),
                    "request_key": str(payload.get("request_key") or ""),
                    "project_name": str(payload.get("project_name") or ""),
                    "exclusive_session": 1,
                    "protocol_version": 2,
                    "task_id": task_id,
                    "workload_family": workload_family,
                    "session_profile": session_profile,
                    "project_namespace": str(
                        payload.get("project_namespace") or ""
                    ).strip(),
                    "isolation_policy": isolation_policy,
                    "workspace_path": workspace_path,
                    "requested_session_id": int(
                        payload.get("requested_session_id") or 0
                    ),
                    "requested_session_generation": 0,
                    "session_id": (
                        int(self.session["id"])
                        if self.session["state"] in {"ready", "busy"}
                        and bool(self.session["endpoint"])
                        else 0
                    ),
                    "slot_index": (
                        0
                        if self.session["state"] in {"ready", "busy"}
                        and bool(self.session["endpoint"])
                        else None
                    ),
                    "failure_message": "",
                    "fault_kind": "",
                    "solve_permit_granted": False,
                    "solve_permit_generation": 0,
                    "native_pipeline_completed": False,
                }
                if self.lease["state"] == "offered":
                    self.session["state"] = "busy"
                self.event("lease_created", exclusive_session=True)
                return 200, {
                    "lease": self._public_lease(),
                    "client_token": self.client_token,
                }

            if method == "POST" and path == "/api/aedt-pool/hosts/claim-start":
                if headers.get("X-AEDT-Bootstrap-Token", "") != self.bootstrap_token:
                    return 403, {"detail": "invalid bootstrap token"}
                host_id = str(payload.get("host_id") or "").strip()
                if not host_id:
                    return 422, {"detail": "host_id is required"}
                if int(payload.get("allocation_id") or 0) != 1:
                    return 409, {"detail": "unexpected pilot allocation"}
                if self.session["host_id"] and self.session["host_id"] != host_id:
                    return 409, {"detail": "session already has another host"}
                self.session.update({
                    "host_id": host_id,
                    "node_name": str(payload.get("node_name") or ""),
                    "actual_node_name": str(
                        payload.get("actual_node_name") or ""
                    ),
                    "slurm_job_id": str(payload.get("slurm_job_id") or ""),
                    "host_process_id": str(
                        payload.get("host_process_id") or ""
                    ),
                })
                self.event("host_claimed")
                return 200, {"session": dict(self.session)}

            match = re.fullmatch(r"/api/aedt-pool/sessions/1/(register|start-failed)", path)
            if match and method == "POST":
                if headers.get("X-AEDT-Bootstrap-Token", "") != self.bootstrap_token:
                    return 403, {"detail": "invalid bootstrap token"}
                if match.group(1) == "start-failed":
                    self.session["state"] = "failed"
                    self.event("host_start_failed", message=payload.get("failure_message"))
                    return 200, dict(self.session)
                if (
                    not self.session["host_id"]
                    or str(payload.get("host_id") or "")
                    != self.session["host_id"]
                ):
                    return 409, {"detail": "registration host_id does not own claim"}
                registration_token = str(
                    headers.get("X-AEDT-Host-Token", "") or ""
                ).strip()
                if not registration_token:
                    return 422, {
                        "detail": "registration host token is required"
                    }
                try:
                    session_profile = canonical_expected_session_profile(
                        payload.get("session_profile")
                    )
                except (TypeError, ValueError) as exc:
                    return 422, {"detail": str(exc)}
                endpoint = str(payload.get("endpoint") or "").strip()
                try:
                    _machine, port_text = endpoint.rsplit(":", 1)
                    port = int(port_text)
                except (TypeError, ValueError):
                    port = 0
                process_id = str(payload.get("process_id") or "").strip()
                try:
                    process_id_valid = int(process_id) > 0
                except ValueError:
                    process_id_valid = False
                if not endpoint or port <= 0:
                    return 422, {"detail": "registered endpoint is invalid"}
                if not process_id_valid:
                    return 422, {"detail": "registered process_id is invalid"}
                artifact_dir = str(payload.get("artifact_dir") or "").strip()
                lock_path = automation_lock_path(artifact_dir)
                if (
                    not artifact_dir
                    or not os.path.isabs(artifact_dir)
                    or not Path(artifact_dir).is_dir()
                    or not lock_path
                    or not Path(lock_path).is_file()
                ):
                    return 422, {
                        "detail": "registered host automation artifact is invalid"
                    }
                runtime_metadata = payload.get("runtime_metadata")
                if not isinstance(runtime_metadata, dict):
                    return 422, {"detail": "runtime_metadata must be an object"}
                if (
                    str(runtime_metadata.get("automation_lock_path") or "")
                    != lock_path
                    or str(runtime_metadata.get("session_profile") or "")
                    != session_profile
                ):
                    return 422, {
                        "detail": "runtime metadata does not match registration"
                    }
                if self.session["endpoint"]:
                    replay = bool(
                        self.host_token == registration_token
                        and self.session["endpoint"] == endpoint
                        and self.session["process_id"] == process_id
                        and self.session["artifact_dir"] == artifact_dir
                        and self.session["session_profile"] == session_profile
                    )
                    if not replay:
                        return 409, {
                            "detail": "conflicting session registration replay"
                        }
                    return 200, {
                        "session": dict(self.session),
                        "host_token": self.host_token,
                    }
                self.host_token = registration_token
                self.session.update({
                    "state": "ready",
                    "endpoint": endpoint,
                    "process_id": process_id,
                    "artifact_dir": artifact_dir,
                    "error_log_path": str(
                        payload.get("error_log_path") or ""
                    ),
                    "journal_path": str(payload.get("journal_path") or ""),
                    "session_profile": session_profile,
                    "runtime_metadata": dict(runtime_metadata),
                })
                self._offer_lease()
                self.event(
                    "host_registered",
                    endpoint=self.session["endpoint"],
                    process_id=self.session["process_id"],
                    automation_lock_path=lock_path,
                )
                return 200, {
                    "session": dict(self.session),
                    "host_token": self.host_token,
                }

            if path.startswith("/api/aedt-pool/leases/1"):
                if headers.get("X-AEDT-Lease-Token", "") != self.client_token:
                    return 403, {"detail": "invalid lease token"}
                if self.lease is None:
                    return 404, {"detail": "lease not found"}
                suffix = path.removeprefix("/api/aedt-pool/leases/1")
                if method == "GET" and suffix == "":
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/heartbeat":
                    if self.lease["state"] in TERMINAL_LEASE_STATES:
                        return 409, {"detail": f"lease is {self.lease['state']}"}
                    self.lease_heartbeat_count += 1
                    self._offer_lease()
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/accept":
                    if self.lease["state"] == "offered":
                        self.lease["state"] = "attaching"
                        self.event("lease_accepted", lease_id=1)
                    elif self.lease["state"] not in {"attaching", "active"}:
                        return 409, {
                            "detail": f"lease is {self.lease['state']}"
                        }
                    return 200, self._public_lease()
                if method == "PATCH" and suffix == "/project-name":
                    self.lease["project_name"] = str(
                        payload.get("project_name") or ""
                    )
                    if not self.lease["project_name"].strip():
                        return 422, {"detail": "project_name is required"}
                    self.event(
                        "project_bound",
                        project_name=self.lease["project_name"],
                    )
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/activate":
                    if self.lease["state"] == "attaching":
                        self.lease["state"] = "active"
                        self.event("lease_activated", lease_id=1)
                        self._grant_solve_permit()
                    elif self.lease["state"] != "active":
                        return 409, {
                            "detail": f"lease is {self.lease['state']}"
                        }
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/solve-permit":
                    if self.lease["state"] != "active":
                        return 409, {
                            "detail": f"lease is {self.lease['state']}"
                        }
                    self._grant_solve_permit()
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/native-pipeline-complete":
                    generation = payload.get("solve_permit_generation")
                    if (
                        self.lease["state"] != "active"
                        or not self.lease.get("solve_permit_granted")
                        or type(generation) is not int
                        or int(generation)
                        != int(self.lease.get("solve_permit_generation") or 0)
                    ):
                        return 409, {
                            "detail": "native pipeline generation is not authorized"
                        }
                    self.lease["native_pipeline_completed"] = True
                    self.event(
                        "native_pipeline_completed",
                        lease_id=1,
                        generation=int(generation),
                    )
                    return 200, self._public_lease()
                if method == "POST" and suffix in {"/cancel", "/release"}:
                    if self.lease["state"] in {"attaching", "active", "releasing"}:
                        self.lease["state"] = "releasing"
                        self.lease["failure_message"] = str(
                            payload.get("reason") or "client released lease"
                        )
                    elif self.lease["state"] not in TERMINAL_LEASE_STATES:
                        self.lease["state"] = "cancelled"
                    self.event(
                        "release_requested",
                        route=suffix,
                    )
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/fault":
                    kind = str(payload.get("fault_kind") or "").strip().lower()
                    if not kind:
                        return 422, {"detail": "fault_kind is required"}
                    if self.lease["state"] in {
                        "attaching",
                        "active",
                        "releasing",
                    }:
                        self.lease["state"] = "releasing"
                    else:
                        self.lease["state"] = "cancelled"
                    self.lease["fault_kind"] = kind
                    self.lease["failure_message"] = str(
                        payload.get("failure_message") or kind
                    )
                    if kind in {
                        "solver_timeout",
                        "aedt_death",
                    }:
                        self.quarantine_reason = kind
                        self.session["state"] = "draining"
                    self.event("fault_reported", kind=kind)
                    return 200, self._public_lease()

            if path.startswith("/api/aedt-pool/sessions/1"):
                if headers.get("X-AEDT-Host-Token", "") != self.host_token:
                    return 403, {"detail": "invalid host token"}
                suffix = path.removeprefix("/api/aedt-pool/sessions/1")
                if method == "POST" and suffix == "/heartbeat":
                    endpoint_port = int(
                        str(self.session["endpoint"]).rsplit(":", 1)[1]
                    )
                    if payload.get("liveness_confirmed") is not True:
                        return 409, {"detail": "liveness confirmation is required"}
                    if str(payload.get("process_id") or "") != str(
                        self.session["process_id"]
                    ):
                        return 409, {"detail": "heartbeat process_id mismatch"}
                    if int(payload.get("port") or 0) != endpoint_port:
                        return 409, {"detail": "heartbeat port mismatch"}
                    if str(payload.get("native_probe") or "") not in {
                        "",
                        "GetVersion",
                    }:
                        return 422, {"detail": "unsupported native probe"}
                    self.host_heartbeat_count += 1
                    self.session["last_native_probe_outcome"] = str(
                        payload.get("native_probe_outcome") or ""
                    )
                    return 200, dict(self.session)
                if method == "POST" and suffix == "/fault":
                    kind = str(payload.get("kind") or "").strip().lower()
                    if not kind:
                        return 422, {"detail": "session fault kind is required"}
                    self.quarantine_reason = kind
                    self.session["state"] = "unhealthy"
                    self.session["failure_message"] = str(
                        payload.get("failure_message") or kind
                    )
                    self.event("session_fault_reported", kind=kind)
                    return 200, dict(self.session)
                if method == "GET" and suffix == "/commands":
                    close_projects = []
                    deferred_projects = []
                    if self.lease and self.lease["state"] == "releasing":
                        if self.lease.get("fault_kind") == "solver_timeout":
                            deferred_projects = [self._public_lease()]
                        else:
                            close_projects = [self._public_lease()]
                    sibling_live_count = int(
                        bool(
                            self.lease
                            and self.lease["state"]
                            in SESSION_COMMAND_LIVE_LEASE_STATES
                        )
                    )
                    global_stop = bool(
                        self.quarantine_reason and sibling_live_count == 0
                    )
                    drain = bool(
                        self.project_close_ack
                        or self.session["state"]
                        in {"draining", "failed", "unhealthy"}
                    )
                    return 200, {
                        "close_projects": close_projects,
                        "deferred_projects": deferred_projects,
                        "drain": drain,
                        "quarantine_reason": self.quarantine_reason,
                        "sibling_live_count": sibling_live_count,
                        "global_stop_allowed": global_stop,
                        "recycle_after_global_stop": global_stop,
                    }
                release_match = re.fullmatch(
                    r"/leases/1/release-complete", suffix
                )
                if method == "POST" and release_match:
                    success = payload.get("success") is True
                    self.lease["state"] = "released" if success else "failed"
                    self.project_close_ack = success
                    self.session["state"] = "draining"
                    self.event("project_close_ack", success=success)
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/closed":
                    success = payload.get("success") is True
                    if not success and payload.get("requeue_siblings") is True:
                        if (
                            self.lease
                            and self.lease["state"]
                            not in TERMINAL_LEASE_STATES
                        ):
                            self.lease.update({
                                "state": "queued",
                                "endpoint": "",
                                "session_id": 0,
                                "slot_index": None,
                                "solve_permit_granted": False,
                                "solve_permit_generation": 0,
                                "native_pipeline_completed": False,
                            })
                    self.session["state"] = "closed" if success else "failed"
                    if self.lease and self.lease["state"] not in TERMINAL_LEASE_STATES:
                        self.lease["state"] = "failed"
                    self.closed_ack = True
                    self.event("desktop_closed_ack", success=success)
                    return 200, dict(self.session)

            return 404, {"detail": f"unsupported pilot route: {method} {path}"}

class SharedControlPlane:
    """Minimal bounded-N loopback protocol with project-local release."""

    def __init__(self, max_projects: int = 2) -> None:
        import secrets

        if int(max_projects) < 1:
            raise ValueError("max_projects must be positive")

        self.lock = threading.RLock()
        self.max_projects = int(max_projects)
        self.bootstrap_token = secrets.token_urlsafe(24)
        self.host_token = ""
        self.session = {
            "id": 1,
            "session_key": "shared-pilot-session-1",
            "generation": 1,
            "state": "starting",
            "host_id": "",
            "endpoint": "",
            "process_id": "",
            "slots_total": self.max_projects,
            "session_profile": EXPECTED_SESSION_PROFILE_JSON,
            "artifact_dir": "",
            "error_log_path": "",
            "journal_path": "",
            "runtime_metadata": {},
        }
        self.leases: dict[int, dict[str, Any]] = {}
        self.client_tokens: dict[int, str] = {}
        self.project_close_acks: dict[int, bool] = {}
        self.closed_ack = False
        self.events: list[dict[str, Any]] = []
        self.quarantine_reason = ""
        self.timeout_owner_lease_id = 0
        self.requeued_lease_ids: list[int] = []
        self.rejected_after_quarantine = 0
        self.solve_permit_generation = 0
        self.batch_sealed = False
        self.host_heartbeat_count = 0
        self.lease_heartbeat_counts: dict[int, int] = {}

    def event(self, name: str, **values: Any) -> None:
        self.events.append({"time": _now(), "event": name, **values})

    def force_drain(self, reason: str) -> None:
        with self.lock:
            for lease in self.leases.values():
                if lease["state"] not in TERMINAL_LEASE_STATES:
                    lease["state"] = "releasing"
                    lease["failure_message"] = reason
                    lease["fault_kind"] = "pilot_force_drain"
            self.session["state"] = "draining"
            self.event("pilot_force_drain", reason=reason)

    def abort_pre_solve(self, project_name: str) -> int:
        """Convert one dead/hung client into a safe two-phase project close."""
        with self.lock:
            matches = [
                lease for lease in self.leases.values()
                if lease.get("project_name") == project_name
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    f"expected one lease for abort project {project_name!r}, found {len(matches)}"
                )
            lease = matches[0]
            if lease["state"] not in {"leased", "attaching", "active"}:
                raise RuntimeError(f"abort lease is {lease['state']}")
            lease["state"] = "releasing"
            lease["failure_message"] = "pre_solve client abort injection"
            lease["fault_kind"] = "pre_solve"
            self.event(
                "pre_solve_abort_reported",
                lease_id=lease["id"],
                project_name=project_name,
            )
            return int(lease["id"])

    def report_solver_timeout(self, project_name: str) -> int:
        """Quarantine a live solve without pretending it can be stopped locally."""
        with self.lock:
            matches = [
                lease for lease in self.leases.values()
                if lease.get("project_name") == project_name
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    f"expected one timeout project {project_name!r}, found {len(matches)}"
                )
            lease = matches[0]
            if lease["state"] not in {"leased", "attaching", "active"}:
                raise RuntimeError(f"timeout lease is {lease['state']}")
            lease["state"] = "releasing"
            lease["failure_message"] = "solver timeout injection while solve was active"
            lease["fault_kind"] = "solver_timeout"
            self.timeout_owner_lease_id = int(lease["id"])
            self.quarantine_reason = "solver_timeout"
            self.session["state"] = "draining"
            self.event(
                "solver_timeout_reported",
                lease_id=lease["id"],
                project_name=project_name,
            )
            return int(lease["id"])

    def _offer_lease(self, lease: dict[str, Any]) -> None:
        if lease["state"] != "queued" or self.batch_sealed:
            return
        if (
            self.session["state"] not in {"ready", "busy"}
            or not self.session["endpoint"]
            or not self.session["process_id"]
            or not self.session["artifact_dir"]
        ):
            return
        lease.update({
            "state": "offered",
            "endpoint": self.session["endpoint"],
            "session_id": int(self.session["id"]),
        })
        self.session["state"] = "busy"
        self.event(
            "lease_offered",
            lease_id=int(lease["id"]),
            slot_index=int(lease["slot_index"]),
        )

    def _grant_solve_batch(self, *, allow_underfilled: bool = False) -> None:
        active = [
            lease
            for lease in self.leases.values()
            if lease["state"] == "active"
            and not lease.get("solve_permit_granted")
        ]
        if not active:
            return
        if not allow_underfilled and len(active) != self.max_projects:
            return
        self.solve_permit_generation += 1
        generation = self.solve_permit_generation
        for lease in active:
            lease.update({
                "solve_permit_granted": True,
                "solve_permit_generation": generation,
            })
        self.batch_sealed = True
        self.event(
            "solve_permit_granted",
            lease_ids=[int(lease["id"]) for lease in active],
            generation=generation,
        )

    def _public_lease(self, lease_id: int) -> dict[str, Any]:
        lease = self.leases[lease_id]
        item = dict(lease)
        assigned = int(item.get("session_id") or 0) == int(self.session["id"])
        generation = int(item.get("solve_permit_generation") or 0)
        cohort = [
            candidate
            for candidate in self.leases.values()
            if generation > 0
            and bool(candidate.get("solve_permit_granted"))
            and int(candidate.get("solve_permit_generation") or 0)
            == generation
        ]
        completed_count = sum(
            bool(candidate.get("native_pipeline_completed"))
            for candidate in cohort
        )
        broken_count = sum(
            not bool(candidate.get("native_pipeline_completed"))
            and candidate["state"] != "active"
            for candidate in cohort
        )
        native_completed = bool(item.get("native_pipeline_completed"))
        item.update({
            "legacy_state": (
                "leased"
                if item["state"] in {"offered", "attaching"}
                else item["state"]
            ),
            "session_key": self.session["session_key"] if assigned else "",
            "session_generation": (
                int(self.session["generation"]) if assigned else 0
            ),
            "session_process_id": (
                str(self.session["process_id"]) if assigned else ""
            ),
            "expected_aedt_version": (
                EXPECTED_AEDT_VERSION if assigned else ""
            ),
            "automation_lock_path": (
                automation_lock_path(str(self.session["artifact_dir"]))
                if assigned
                else ""
            ),
            "session_slots_total": int(self.session["slots_total"]),
            "session_live_lease_count": sum(
                candidate["state"] in SESSION_LIVE_LEASE_STATES
                for candidate in self.leases.values()
            ),
            "session_active_lease_count": sum(
                candidate["state"] == "active"
                for candidate in self.leases.values()
            ),
            "solve_permit_required": bool(
                lease["state"] == "active"
                and not lease.get("solve_permit_granted")
            ),
            "native_pipeline_completed": native_completed,
            "native_pipeline_expected_count": len(cohort),
            "native_pipeline_completed_count": completed_count,
            "native_pipeline_barrier_granted": bool(
                native_completed
                and cohort
                and completed_count == len(cohort)
            ),
            "native_pipeline_barrier_broken": bool(
                cohort and completed_count != len(cohort) and broken_count
            ),
        })
        return item

    def _live_siblings(self) -> int:
        return sum(
            lease["state"] in SESSION_COMMAND_LIVE_LEASE_STATES
            for lease in self.leases.values()
        )

    def _all_projects_closed(self) -> bool:
        return (
            len(self.leases) == self.max_projects
            and all(
                lease["state"] in TERMINAL_LEASE_STATES
                for lease in self.leases.values()
            )
        )

    def dispatch(
        self,
        method: str,
        path: str,
        payload: dict[str, Any],
        headers: Any,
    ) -> tuple[int, dict[str, Any]]:
        import secrets

        with self.lock:
            if method == "POST" and path == "/api/aedt-pool/leases":
                if self.quarantine_reason:
                    self.rejected_after_quarantine += 1
                    self.event(
                        "lease_rejected_after_quarantine",
                        project_name=str(payload.get("project_name") or ""),
                    )
                    return 409, {"detail": "pilot Desktop is quarantined"}
                if len(self.leases) >= self.max_projects:
                    return 409, {
                        "detail": f"pilot permits at most {self.max_projects} leases"
                    }
                if payload.get("exclusive_session") is not False:
                    return 422, {"detail": "1:2 pilot requires exclusive_session=false"}
                if type(payload.get("protocol_version")) is not int \
                        or int(payload["protocol_version"]) != 2:
                    return 422, {"detail": "pilot requires protocol_version=2"}
                try:
                    session_profile = canonical_expected_session_profile(
                        payload.get("session_profile")
                    )
                except (TypeError, ValueError) as exc:
                    return 422, {"detail": str(exc)}
                workload_family = str(
                    payload.get("workload_family") or ""
                ).strip()
                if not workload_family:
                    return 422, {
                        "detail": "protocol-v2 workload_family is required"
                    }
                workspace_path = str(
                    payload.get("workspace_path") or ""
                ).strip()
                if not workspace_path or not os.path.isabs(workspace_path):
                    return 422, {
                        "detail": (
                            "protocol-v2 workspace_path must be an absolute path"
                        )
                    }
                task_id = int(payload.get("task_id") or 0)
                if task_id <= 0:
                    return 422, {
                        "detail": "protocol-v2 task_id must be positive"
                    }
                isolation_policy = str(
                    payload.get("isolation_policy") or "family"
                ).strip().lower()
                if isolation_policy not in {
                    "family",
                    "shared_if_compatible",
                }:
                    return 422, {
                        "detail": "shared pilot isolation policy is invalid"
                    }
                for occupant in self.leases.values():
                    if (
                        occupant["session_profile"] != session_profile
                        or occupant["workload_family"] != workload_family
                        or occupant["isolation_policy"] != isolation_policy
                    ):
                        return 409, {
                            "detail": "shared pilot session contract mismatch"
                        }
                lease_id = len(self.leases) + 1
                token = str(
                    payload.get("client_token") or secrets.token_urlsafe(32)
                )
                lease = {
                    "id": lease_id,
                    "state": (
                        "offered"
                        if self.session["state"] in {"ready", "busy"}
                        and bool(self.session["endpoint"])
                        and not self.batch_sealed
                        else "queued"
                    ),
                    "endpoint": (
                        self.session["endpoint"]
                        if self.session["state"] in {"ready", "busy"}
                        and not self.batch_sealed
                        else ""
                    ),
                    "request_key": str(payload.get("request_key") or ""),
                    "project_name": str(payload.get("project_name") or ""),
                    "exclusive_session": 0,
                    "protocol_version": 2,
                    "task_id": task_id,
                    "workload_family": workload_family,
                    "session_profile": session_profile,
                    "project_namespace": str(
                        payload.get("project_namespace") or ""
                    ).strip(),
                    "isolation_policy": isolation_policy,
                    "workspace_path": workspace_path,
                    "requested_session_id": int(
                        payload.get("requested_session_id") or 0
                    ),
                    "requested_session_generation": 0,
                    "session_id": (
                        int(self.session["id"])
                        if self.session["state"] in {"ready", "busy"}
                        and bool(self.session["endpoint"])
                        and not self.batch_sealed
                        else 0
                    ),
                    "slot_index": lease_id - 1,
                    "failure_message": "",
                    "fault_kind": "",
                    "solve_permit_granted": False,
                    "solve_permit_generation": 0,
                    "native_pipeline_completed": False,
                }
                self.leases[lease_id] = lease
                self.client_tokens[lease_id] = token
                self.lease_heartbeat_counts[lease_id] = 0
                if lease["state"] == "offered":
                    self.session["state"] = "busy"
                self.event("lease_created", lease_id=lease_id, slot_index=lease_id - 1)
                return 200, {
                    "lease": self._public_lease(lease_id),
                    "client_token": token,
                }

            if method == "POST" and path == "/api/aedt-pool/hosts/claim-start":
                if headers.get("X-AEDT-Bootstrap-Token", "") != self.bootstrap_token:
                    return 403, {"detail": "invalid bootstrap token"}
                host_id = str(payload.get("host_id") or "").strip()
                if not host_id:
                    return 422, {"detail": "host_id is required"}
                if int(payload.get("allocation_id") or 0) != 1:
                    return 409, {"detail": "unexpected pilot allocation"}
                if self.session["host_id"] and self.session["host_id"] != host_id:
                    return 409, {"detail": "session already has another host"}
                self.session.update({
                    "host_id": host_id,
                    "node_name": str(payload.get("node_name") or ""),
                    "actual_node_name": str(
                        payload.get("actual_node_name") or ""
                    ),
                    "slurm_job_id": str(payload.get("slurm_job_id") or ""),
                    "host_process_id": str(
                        payload.get("host_process_id") or ""
                    ),
                })
                self.event("host_claimed")
                return 200, {"session": dict(self.session)}

            match = re.fullmatch(r"/api/aedt-pool/sessions/1/(register|start-failed)", path)
            if match and method == "POST":
                if headers.get("X-AEDT-Bootstrap-Token", "") != self.bootstrap_token:
                    return 403, {"detail": "invalid bootstrap token"}
                if match.group(1) == "start-failed":
                    self.session["state"] = "failed"
                    self.event("host_start_failed", message=payload.get("failure_message"))
                    return 200, dict(self.session)
                if (
                    not self.session["host_id"]
                    or str(payload.get("host_id") or "")
                    != self.session["host_id"]
                ):
                    return 409, {"detail": "registration host_id does not own claim"}
                registration_token = str(
                    headers.get("X-AEDT-Host-Token", "") or ""
                ).strip()
                if not registration_token:
                    return 422, {
                        "detail": "registration host token is required"
                    }
                try:
                    session_profile = canonical_expected_session_profile(
                        payload.get("session_profile")
                    )
                except (TypeError, ValueError) as exc:
                    return 422, {"detail": str(exc)}
                endpoint = str(payload.get("endpoint") or "").strip()
                try:
                    _machine, port_text = endpoint.rsplit(":", 1)
                    port = int(port_text)
                except (TypeError, ValueError):
                    port = 0
                process_id = str(payload.get("process_id") or "").strip()
                try:
                    process_id_valid = int(process_id) > 0
                except ValueError:
                    process_id_valid = False
                if not endpoint or port <= 0:
                    return 422, {"detail": "registered endpoint is invalid"}
                if not process_id_valid:
                    return 422, {"detail": "registered process_id is invalid"}
                artifact_dir = str(payload.get("artifact_dir") or "").strip()
                lock_path = automation_lock_path(artifact_dir)
                if (
                    not artifact_dir
                    or not os.path.isabs(artifact_dir)
                    or not Path(artifact_dir).is_dir()
                    or not lock_path
                    or not Path(lock_path).is_file()
                ):
                    return 422, {
                        "detail": "registered host automation artifact is invalid"
                    }
                runtime_metadata = payload.get("runtime_metadata")
                if not isinstance(runtime_metadata, dict):
                    return 422, {"detail": "runtime_metadata must be an object"}
                if (
                    str(runtime_metadata.get("automation_lock_path") or "")
                    != lock_path
                    or str(runtime_metadata.get("session_profile") or "")
                    != session_profile
                ):
                    return 422, {
                        "detail": "runtime metadata does not match registration"
                    }
                if self.session["endpoint"]:
                    replay = bool(
                        self.host_token == registration_token
                        and self.session["endpoint"] == endpoint
                        and self.session["process_id"] == process_id
                        and self.session["artifact_dir"] == artifact_dir
                        and self.session["session_profile"] == session_profile
                    )
                    if not replay:
                        return 409, {
                            "detail": "conflicting session registration replay"
                        }
                    return 200, {
                        "session": dict(self.session),
                        "host_token": self.host_token,
                    }
                self.host_token = registration_token
                self.session.update({
                    "state": "ready",
                    "endpoint": endpoint,
                    "process_id": process_id,
                    "artifact_dir": artifact_dir,
                    "error_log_path": str(
                        payload.get("error_log_path") or ""
                    ),
                    "journal_path": str(payload.get("journal_path") or ""),
                    "session_profile": session_profile,
                    "runtime_metadata": dict(runtime_metadata),
                })
                for lease in self.leases.values():
                    self._offer_lease(lease)
                if self.leases:
                    self.session["state"] = "busy"
                self.event(
                    "host_registered",
                    endpoint=self.session["endpoint"],
                    process_id=self.session["process_id"],
                    automation_lock_path=lock_path,
                )
                return 200, {"session": dict(self.session), "host_token": self.host_token}

            lease_match = re.fullmatch(r"/api/aedt-pool/leases/(\d+)(.*)", path)
            if lease_match:
                lease_id = int(lease_match.group(1))
                suffix = lease_match.group(2)
                if lease_id not in self.leases:
                    return 404, {"detail": "lease not found"}
                if headers.get("X-AEDT-Lease-Token", "") != self.client_tokens[lease_id]:
                    return 403, {"detail": "invalid lease token"}
                lease = self.leases[lease_id]
                if method == "GET" and suffix == "":
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/heartbeat":
                    if lease["state"] in TERMINAL_LEASE_STATES:
                        return 409, {"detail": f"lease is {lease['state']}"}
                    self.lease_heartbeat_counts[lease_id] += 1
                    self._offer_lease(lease)
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/accept":
                    if lease["state"] == "offered":
                        lease["state"] = "attaching"
                        self.event("lease_accepted", lease_id=lease_id)
                    elif lease["state"] not in {"attaching", "active"}:
                        return 409, {"detail": f"lease is {lease['state']}"}
                    return 200, self._public_lease(lease_id)
                if method == "PATCH" and suffix == "/project-name":
                    lease["project_name"] = str(payload.get("project_name") or "")
                    if not lease["project_name"].strip():
                        return 422, {"detail": "project_name is required"}
                    self.event(
                        "project_bound",
                        lease_id=lease_id,
                        project_name=lease["project_name"],
                    )
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/activate":
                    if lease["state"] == "attaching":
                        lease["state"] = "active"
                        self.event("lease_activated", lease_id=lease_id)
                        self._grant_solve_batch()
                    elif lease["state"] != "active":
                        return 409, {"detail": f"lease is {lease['state']}"}
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/solve-permit":
                    if lease["state"] != "active":
                        return 409, {"detail": f"lease is {lease['state']}"}
                    seal_underfilled = payload.get("seal_underfilled", False)
                    if type(seal_underfilled) is not bool:
                        return 422, {
                            "detail": "seal_underfilled must be a boolean"
                        }
                    self._grant_solve_batch(
                        allow_underfilled=bool(seal_underfilled)
                    )
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/native-pipeline-complete":
                    generation = payload.get("solve_permit_generation")
                    if (
                        lease["state"] != "active"
                        or not lease.get("solve_permit_granted")
                        or type(generation) is not int
                        or int(generation)
                        != int(lease.get("solve_permit_generation") or 0)
                    ):
                        return 409, {
                            "detail": "native pipeline generation is not authorized"
                        }
                    lease["native_pipeline_completed"] = True
                    self.event(
                        "native_pipeline_completed",
                        lease_id=lease_id,
                        generation=int(generation),
                    )
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix in {"/cancel", "/release"}:
                    if lease["state"] in {"attaching", "active", "releasing"}:
                        lease["state"] = "releasing"
                        lease["failure_message"] = str(
                            payload.get("reason") or "client released lease"
                        )
                    elif lease["state"] not in TERMINAL_LEASE_STATES:
                        lease["state"] = "cancelled"
                    self.event(
                        "release_requested",
                        lease_id=lease_id,
                        route=suffix,
                    )
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/fault":
                    kind = str(
                        payload.get("fault_kind") or ""
                    ).strip().lower()
                    if kind == "solver_timeout":
                        self.report_solver_timeout(str(lease.get("project_name") or ""))
                        return 200, self._public_lease(lease_id)
                    if kind not in {
                        "admission_timeout",
                        "attach_failed",
                        "project_create_failed",
                        "pre_solve",
                        "script_error",
                        "aedt_transport_death",
                    }:
                        return 409, {"detail": "unsupported pilot fault"}
                    if lease["state"] in {
                        "attaching",
                        "active",
                        "releasing",
                    }:
                        lease["state"] = "releasing"
                    else:
                        lease["state"] = "cancelled"
                    lease["fault_kind"] = kind
                    lease["failure_message"] = str(payload.get("failure_message") or kind)
                    self.event("fault_reported", lease_id=lease_id, kind=kind)
                    return 200, self._public_lease(lease_id)

            if path.startswith("/api/aedt-pool/sessions/1"):
                if headers.get("X-AEDT-Host-Token", "") != self.host_token:
                    return 403, {"detail": "invalid host token"}
                suffix = path.removeprefix("/api/aedt-pool/sessions/1")
                if method == "POST" and suffix == "/heartbeat":
                    endpoint_port = int(
                        str(self.session["endpoint"]).rsplit(":", 1)[1]
                    )
                    if payload.get("liveness_confirmed") is not True:
                        return 409, {"detail": "liveness confirmation is required"}
                    if str(payload.get("process_id") or "") != str(
                        self.session["process_id"]
                    ):
                        return 409, {"detail": "heartbeat process_id mismatch"}
                    if int(payload.get("port") or 0) != endpoint_port:
                        return 409, {"detail": "heartbeat port mismatch"}
                    if str(payload.get("native_probe") or "") not in {
                        "",
                        "GetVersion",
                    }:
                        return 422, {"detail": "unsupported native probe"}
                    self.host_heartbeat_count += 1
                    self.session["last_native_probe_outcome"] = str(
                        payload.get("native_probe_outcome") or ""
                    )
                    return 200, dict(self.session)
                if method == "POST" and suffix == "/fault":
                    kind = str(payload.get("kind") or "").strip().lower()
                    if not kind:
                        return 422, {"detail": "session fault kind is required"}
                    self.quarantine_reason = kind
                    self.session["state"] = "unhealthy"
                    self.session["failure_message"] = str(
                        payload.get("failure_message") or kind
                    )
                    self.event("session_fault_reported", kind=kind)
                    return 200, dict(self.session)
                if method == "GET" and suffix == "/commands":
                    close_projects = [
                        self._public_lease(int(lease["id"]))
                        for lease in self.leases.values()
                        if lease["state"] == "releasing"
                        and int(lease["id"]) != self.timeout_owner_lease_id
                    ]
                    deferred_projects = [
                        self._public_lease(int(lease["id"]))
                        for lease in self.leases.values()
                        if lease["state"] == "releasing"
                        and int(lease["id"]) == self.timeout_owner_lease_id
                    ]
                    sibling_live = self._live_siblings()
                    global_stop_allowed = bool(
                        self.quarantine_reason and sibling_live == 0
                    )
                    if global_stop_allowed and not any(
                        event["event"] == "global_stop_allowed"
                        for event in self.events
                    ):
                        self.event("global_stop_allowed")
                    return 200, {
                        "close_projects": close_projects,
                        "deferred_projects": deferred_projects,
                        "drain": bool(self.quarantine_reason) or self._all_projects_closed(),
                        "quarantine_reason": self.quarantine_reason,
                        "sibling_live_count": sibling_live,
                        "global_stop_allowed": global_stop_allowed,
                        "recycle_after_global_stop": global_stop_allowed,
                    }
                release_match = re.fullmatch(r"/leases/(\d+)/release-complete", suffix)
                if method == "POST" and release_match:
                    lease_id = int(release_match.group(1))
                    if lease_id not in self.leases:
                        return 404, {"detail": "lease not found"}
                    success = payload.get("success") is True
                    self.leases[lease_id]["state"] = "released" if success else "failed"
                    self.project_close_acks[lease_id] = success
                    self.event("project_close_ack", lease_id=lease_id, success=success)
                    if self._all_projects_closed():
                        self.session["state"] = "draining"
                    return 200, self._public_lease(lease_id)
                if method == "POST" and suffix == "/closed":
                    success = payload.get("success") is True
                    if not success and payload.get("requeue_siblings") is True:
                        for lease in self.leases.values():
                            if lease["state"] in {
                                "offered",
                                "leased",
                                "attaching",
                                "active",
                                "releasing",
                            }:
                                lease.update({
                                    "state": "queued",
                                    "endpoint": "",
                                    "session_id": 0,
                                    "slot_index": None,
                                    "solve_permit_granted": False,
                                    "solve_permit_generation": 0,
                                    "native_pipeline_completed": False,
                                })
                                self.requeued_lease_ids.append(int(lease["id"]))
                    self.session["state"] = "closed" if success else "failed"
                    self.closed_ack = True
                    self.event("desktop_closed_ack", success=success)
                    return 200, dict(self.session)

            return 404, {"detail": f"unsupported pilot route: {method} {path}"}
class LoopbackHandler(BaseHTTPRequestHandler):
    server_version = "AedtPoolLoopback/1"

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            self._send(400, {"detail": "invalid JSON"})
            return
        status, response = self.server.state.dispatch(  # type: ignore[attr-defined]
            self.command,
            urlparse(self.path).path,
            payload,
            self.headers,
        )
        self._send(status, response)

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _handle
    do_POST = _handle
    do_PATCH = _handle

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def start_control_plane(
    state: ExclusiveControlPlane | SharedControlPlane,
) -> tuple[ThreadingHTTPServer, threading.Thread, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), LoopbackHandler)
    server.state = state  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, thread, f"http://{host}:{port}"
