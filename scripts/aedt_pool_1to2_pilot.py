"""Disposable real 1-AEDT:2-MFT-project attach/isolation pilot.

The entire pilot runs inside one scheduler task and uses a loopback-only
control plane.  It never enables or marks the production AEDT pool ready.
Case ``normal`` requires two concurrent Matrix-only MFT clients to attach to
one Desktop and produce independent valid terminal rows.  Case ``abort``
stops client A while it is intentionally hung before solve, reports a
project-local pre-solve fault, and requires sibling B to remain valid.  The
separately selected ``timeout`` case waits until both Maxwell solver checkouts
are visible, stops only client A (never a solver PID), quarantines the
disposable Desktop, and requires sibling B to produce a terminal result before
the host recycles that Desktop.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.aedt_pool_1to1_pilot import (  # noqa: E402
    PilotHandler,
    SESSION_COMMAND_LIVE_LEASE_STATES,
    SESSION_LIVE_LEASE_STATES,
    _clone_exact,
    _feature_pid_present,
    _lmstat_snapshot,
    _now,
    parse_result_json,
    process_alive,
    start_control_plane,
)
from slurm_scheduler.aedt_automation_lock import (  # noqa: E402
    automation_lock_path,
)
from slurm_scheduler.aedt_session_host import (  # noqa: E402
    AedtSessionHost,
    ControlPlaneClient,
    EXPECTED_AEDT_VERSION,
    EXPECTED_SESSION_PROFILE_JSON,
    SUPPORTED_DSO_PROFILE,
    canonical_expected_session_profile,
)
from slurm_scheduler.aedt_attach_client import AedtPoolHttpClient  # noqa: E402


TERMINAL_LEASE_STATES = {"released", "failed", "cancelled", "expired"}


class SharedPilotControlPlane:
    """Minimal bounded-N loopback protocol with project-local release.

    The isolated 1:2 pilot uses the default of two. Explicit validation
    variants may pass a different bound without duplicating lifecycle logic.
    """

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


def _owned_feature_pid_entries(
    text: str,
    feature: str,
    user: str,
    host: str,
    desktop_pid: int,
) -> list[int]:
    """Return local feature rows owned by the exact pilot Desktop tree."""
    start = re.search(rf"(?m)^Users of {re.escape(feature)}:.*$", text)
    if not start:
        return []
    tail = text[start.end():]
    next_feature = re.search(r"(?m)^Users of [^:]+:.*$", tail)
    section = tail[: next_feature.start()] if next_feature else tail
    host_short = host.casefold().split(".", 1)[0]
    candidates: list[int] = []
    for line in section.splitlines():
        fields = line.split()
        if len(fields) < 4 or fields[0].casefold() != user.casefold():
            continue
        if not any(
            field.casefold().split(".", 1)[0] == host_short
            for field in fields[1:3]
        ):
            continue
        if fields[3].isdigit():
            candidates.append(int(fields[3]))
    try:
        import psutil

        owned = []
        for pid in candidates:
            if pid == desktop_pid:
                owned.append(pid)
                continue
            try:
                if any(parent.pid == desktop_pid for parent in psutil.Process(pid).parents()):
                    owned.append(pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return owned
    except Exception:
        # Fail closed: without process ancestry, do not attribute node-wide
        # checkouts to this pilot.
        return []


def _valid_matrix_result(result: dict[str, Any]) -> list[str]:
    failures = []
    required = {
        "result_valid_em": 1,
        "aedt_backend": "pooled",
        "aedt_exclusive_session": 0,
        "matrix_solve_attempts": 1,
    }
    for key, expected in required.items():
        if result.get(key) != expected:
            failures.append(f"{key}:{result.get(key)!r}!={expected!r}")
    if int(result.get("matrix_solution_queries") or 0) < 1:
        failures.append("matrix_solution_queries<1")
    try:
        if not float(result["Llt"]) > 0:
            failures.append("Llt_not_positive")
    except (KeyError, TypeError, ValueError):
        failures.append("Llt_missing")
    if not str(result.get("project_name") or "").strip():
        failures.append("project_name_missing")
    return failures


def _validate_project_evidence(
    results: dict[str, dict[str, Any]],
    leases: dict[int, dict[str, Any]],
    expected_label_lease_ids: dict[str, int],
) -> list[str]:
    """Tie every successful project result to its own bound lease.

    This is deliberately independent of cohort size so an N-project pilot
    cannot pass by replaying another project's successful terminal output.
    """
    failures: list[str] = []
    if set(results) != set(expected_label_lease_ids):
        failures.append(f"terminal_result_labels={sorted(results)!r}")
    seen_lease_ids: set[int] = set()
    seen_project_names: set[str] = set()
    for label, result in results.items():
        failures.extend(
            f"runner_{label}_{failure}"
            for failure in _valid_matrix_result(result)
        )
        try:
            lease_id = int(result.get("aedt_lease_id") or 0)
        except (TypeError, ValueError):
            lease_id = 0
        if lease_id <= 0 or lease_id not in leases:
            failures.append(f"runner_{label}_lease_id_invalid")
            continue
        if lease_id != expected_label_lease_ids.get(label):
            failures.append(f"runner_{label}_lease_id_mismatch")
        if lease_id in seen_lease_ids:
            failures.append(f"runner_{label}_lease_id_duplicate")
        seen_lease_ids.add(lease_id)
        project_name = str(result.get("project_name") or "").strip()
        if project_name != str(leases[lease_id].get("project_name") or "").strip():
            failures.append(f"runner_{label}_project_lease_mismatch")
        if project_name in seen_project_names:
            failures.append(f"runner_{label}_project_name_duplicate")
        seen_project_names.add(project_name)
    return failures


def _terminate(run: subprocess.Popen[Any]) -> None:
    if run.poll() is not None:
        return
    run.terminate()
    try:
        run.wait(timeout=30)
    except subprocess.TimeoutExpired:
        run.kill()
        run.wait(timeout=30)


def _run_case(
    *,
    case: str,
    output: Path,
    mft_revision: str,
    mft_repo_url: str,
    library: Path,
    scheduler_url: str,
    state: SharedPilotControlPlane,
    timeout_seconds: int,
    lmutil: str,
    license_server: str,
    solver_feature: str,
    desktop_pid: int,
) -> dict[str, Any]:
    case_dir = output / case
    case_dir.mkdir()
    params_path = case_dir / "pilot_params.json"
    params_path.write_text(
        json.dumps({"matrix_on": 1, "loss_on": 0, "thermal_on": 0, "keep_project": 0}),
        encoding="utf-8",
    )
    runners: dict[str, dict[str, Any]] = {}
    task_id_text = str(os.environ.get("SLURM_SCHED_TASK_ID") or "").strip()
    pilot_task_id = (
        int(task_id_text)
        if task_id_text.isdigit() and int(task_id_text) > 0
        else 1
    )
    workspace_root = output / "host-artifacts"
    workspace_root.mkdir(parents=True, exist_ok=True)
    for label in ("A", "B"):
        mft = case_dir / f"MFT_{label}"
        _clone_exact(mft_repo_url, mft_revision, mft)
        workspace = (
            workspace_root
            / f"aedt-{case}-{pilot_task_id}-{label.lower()}"
        )
        workspace.mkdir()
        simulation_link = mft / "simulation"
        if os.path.lexists(simulation_link):
            raise RuntimeError(
                f"pinned MFT clone unexpectedly contains {simulation_link}"
            )
        simulation_link.symlink_to(workspace, target_is_directory=True)
        env = os.environ.copy()
        env.update({
            "MFT_AEDT_BACKEND": "pooled",
            "MFT_AEDT_SHARED_1TO2_PILOT": "1",
            "SLURM_AEDT_SHARED_SESSION": "1",
            "MFT_AEDT_SCHEDULER_URL": scheduler_url,
            "MFT_SLURM_SCHEDULER_ROOT": str(ROOT),
            "MFT_PYAEDT_LIBRARY_ROOT": str(library),
            "MFT_AEDT_LEASE_WAIT_SECONDS": "300",
            "MFT_AEDT_RELEASE_WAIT_SECONDS": "300",
            "MFT_AEDT_PILOT_CLIENT_LABEL": label,
            "MFT_AEDT_PILOT_WORKSPACE": str(workspace.resolve()),
            "SLURM_SCHED_TASK_ID": str(pilot_task_id),
        })
        marker = case_dir / "A_pre_solve_ready.json"
        if case == "abort" and label == "A":
            env.update({
                "MFT_AEDT_PILOT_PRE_SOLVE_READY_FILE": str(marker),
                "MFT_AEDT_PILOT_PRE_SOLVE_HANG_SECONDS": "1800",
            })
        stdout_path = case_dir / f"runner_{label}.stdout.log"
        stderr_path = case_dir / f"runner_{label}.stderr.log"
        stdout_file = stdout_path.open("w", encoding="utf-8")
        stderr_file = stderr_path.open("w", encoding="utf-8")
        run = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "scripts" / "aedt_pool_1to2_runner.py"),
                str(mft / "run_simulation_260706.py"),
                "--fixed",
                "--params",
                str(params_path),
                "--headless",
            ],
            cwd=mft,
            env=env,
            text=True,
            stdout=stdout_file,
            stderr=stderr_file,
        )
        runners[label] = {
            "run": run,
            "mft": mft,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "stdout_file": stdout_file,
            "stderr_file": stderr_file,
        }

    deadline = time.monotonic() + max(1, timeout_seconds)
    injected = False
    aborted_lease_id = None
    license_records = []
    desktop_checkout_seen = False
    solver_peak = 0
    solver_pids: set[int] = set()
    sample_index = 0
    while any(item["run"].poll() is None for item in runners.values()):
        sample_path = case_dir / f"lmstat_during_{sample_index:04d}.txt"
        license_records.append(_lmstat_snapshot(lmutil, license_server, sample_path))
        sample_text = sample_path.read_text(encoding="utf-8", errors="replace")
        desktop_checkout_seen = desktop_checkout_seen or _feature_pid_present(
            sample_text,
            "electronics_desktop",
            getpass.getuser(),
            socket.gethostname(),
            desktop_pid,
        )
        owned_solver = _owned_feature_pid_entries(
            sample_text,
            solver_feature,
            getpass.getuser(),
            socket.gethostname(),
            desktop_pid,
        )
        solver_peak = max(solver_peak, len(owned_solver))
        solver_pids.update(owned_solver)
        sample_index += 1

        marker = case_dir / "A_pre_solve_ready.json"
        if case == "abort" and marker.is_file() and not injected:
            marker_data = json.loads(marker.read_text(encoding="utf-8"))
            _terminate(runners["A"]["run"])
            aborted_lease_id = state.abort_pre_solve(str(marker_data["project_name"]))
            injected = True
        if case == "timeout" and len(owned_solver) >= 2 and not injected:
            project_a = str(state.leases.get(1, {}).get("project_name") or "")
            if not project_a or len(state.leases) != 2:
                raise RuntimeError(
                    "two solver checkouts appeared before both pilot projects were bound"
                )
            # Kill only the disposable Python client.  The owned AEDT and its
            # solver children remain under the session host so this exercises
            # quarantine/grace/recycle without repeating the unsafe direct
            # solver-PID kill experiment.
            _terminate(runners["A"]["run"])
            aborted_lease_id = state.report_solver_timeout(project_a)
            try:
                AedtPoolHttpClient(scheduler_url).request(
                    "POST",
                    "/api/aedt-pool/leases",
                    {
                        "request_key": "timeout-quarantine-probe-C",
                        "project_name": "quarantine-probe-C",
                        "exclusive_session": False,
                    },
                )
            except urllib.error.HTTPError as error:
                if error.code != 409:
                    raise
            else:
                raise RuntimeError("quarantined pilot Desktop admitted project C")
            injected = True
        if time.monotonic() > deadline:
            state.force_drain(f"1:2 {case} pilot timeout")
            for item in runners.values():
                _terminate(item["run"])
            raise TimeoutError(f"1:2 {case} pilot timed out")
        time.sleep(5)

    for item in runners.values():
        item["run"].wait(timeout=60)
        item["stdout_file"].close()
        item["stderr_file"].close()

    results: dict[str, Any] = {}
    failures: list[str] = []
    for label, item in runners.items():
        stdout = item["stdout_path"].read_text(encoding="utf-8", errors="replace")
        returncode = int(item["run"].returncode)
        if case in {"abort", "timeout"} and label == "A":
            if returncode == 0:
                failures.append(f"{case}_A_unexpected_success")
            continue
        if returncode != 0:
            failures.append(f"runner_{label}_exit={returncode}")
            continue
        try:
            result = parse_result_json(stdout)
        except Exception as exc:
            failures.append(f"runner_{label}_terminal:{exc}")
            continue
        results[label] = result

    expected_label_lease_ids = (
        {"B": 2} if case in {"abort", "timeout"} else {"A": 1, "B": 2}
    )
    failures.extend(_validate_project_evidence(
        results, state.leases, expected_label_lease_ids
    ))

    if case == "normal":
        lease_ids = {int(result.get("aedt_lease_id") or 0) for result in results.values()}
        if lease_ids != {1, 2}:
            failures.append(f"normal_lease_ids={sorted(lease_ids)}")
        if solver_peak < 2:
            failures.append(f"maxwell_solver_peak={solver_peak}<2")
    elif case == "abort":
        if not injected or aborted_lease_id is None:
            failures.append("pre_solve_abort_not_injected")
    else:
        if not injected or aborted_lease_id is None:
            failures.append("solver_timeout_not_injected")

    return {
        "case": case,
        "passed": not failures,
        "failures": failures,
        "results": results,
        "runner_returncodes": {
            label: int(item["run"].returncode) for label, item in runners.items()
        },
        "aborted_lease_id": aborted_lease_id,
        "timeout_injected_during_two_solver_checkouts": bool(
            case == "timeout" and injected and solver_peak >= 2
        ),
        "new_lease_rejected_after_quarantine": bool(
            case == "timeout" and state.rejected_after_quarantine > 0
        ),
        "desktop_checkout_seen": desktop_checkout_seen,
        "maxwell_solver_feature": solver_feature,
        "maxwell_solver_peak": solver_peak,
        "solver_pids": sorted(solver_pids),
        "license_records": license_records,
        "mft_roots": {label: str(item["mft"]) for label, item in runners.items()},
    }


def _cleanup_project_workspaces(case_result: dict[str, Any]) -> list[str]:
    failures = []
    roots = case_result.get("mft_roots") or {}
    results = case_result.get("results") or {}
    for label, result in results.items():
        project_name = str(result.get("project_name") or "")
        project_dir = Path(roots[label]) / "simulation" / project_name
        if project_name and project_dir.exists():
            failures.append(f"runner_{label}_workspace_not_cleaned")
    if case_result.get("case") in {"abort", "timeout"}:
        root = Path(roots["A"]) / "simulation"
        if root.exists():
            for project_dir in root.glob("simulation*"):
                resolved = project_dir.resolve()
                if resolved.parent == root.resolve() and resolved.is_dir():
                    shutil.rmtree(resolved)
            if any(root.glob("simulation*")):
                failures.append("abort_A_workspace_cleanup_failed")
    return failures


def _run_host(
    state: SharedPilotControlPlane,
    scheduler_url: str,
    artifact_root: Path,
) -> tuple[AedtSessionHost, threading.Thread, list[int]]:
    host = AedtSessionHost(
        ControlPlaneClient(scheduler_url, bootstrap_token=state.bootstrap_token),
        allocation_id=1,
        node_name=socket.gethostname(),
        heartbeat_seconds=5,
        aedt_version=EXPECTED_AEDT_VERSION,
        artifact_root=str(artifact_root.resolve()),
        dso_profile=SUPPORTED_DSO_PROFILE,
        session_profile=EXPECTED_SESSION_PROFILE_JSON,
    )
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(host.run()), daemon=True)
    thread.start()
    deadline = time.monotonic() + 300
    while not state.session["endpoint"] and time.monotonic() < deadline:
        thread.join(timeout=1)
        if not thread.is_alive():
            failure = next(
                (
                    str(event.get("message") or "")
                    for event in reversed(state.events)
                    if event.get("event") == "host_start_failed"
                ),
                "",
            )
            detail = f": {failure}" if failure else ""
            raise RuntimeError(
                f"session host exited before registration with result {result!r}{detail}"
            )
    if not state.session["endpoint"]:
        raise RuntimeError("session host did not register within 300 seconds")
    return host, thread, result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mft-repo-url", required=True)
    parser.add_argument("--mft-revision", required=True)
    parser.add_argument("--library-repo-url", required=True)
    parser.add_argument("--library-revision", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--lmutil", required=True)
    parser.add_argument("--license-server", required=True)
    parser.add_argument("--solver-license-feature", default="elec_solve_maxwell")
    parser.add_argument("--license-return-wait-seconds", type=int, default=180)
    parser.add_argument(
        "--cases",
        default="normal,abort",
        help="comma-separated subset of normal,abort,timeout",
    )
    args = parser.parse_args(argv)

    selected_cases = [item.strip() for item in args.cases.split(",") if item.strip()]
    invalid_cases = sorted(set(selected_cases) - {"normal", "abort", "timeout"})
    if not selected_cases or invalid_cases or len(selected_cases) != len(set(selected_cases)):
        parser.error(
            "--cases must be a non-empty, duplicate-free subset of normal,abort,timeout"
        )

    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    library = output / "pyaedt_library"
    evidence_path = output / "pilot_evidence.json"
    _clone_exact(args.library_repo_url, args.library_revision, library)
    scheduler_revision = subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
    ).strip()
    all_failures: list[str] = []
    cases = []
    case_states = []
    server = None
    server_thread = None
    host = None
    host_thread = None
    try:
        for case in selected_cases:
            state = SharedPilotControlPlane()
            case_states.append(state)
            server, server_thread, scheduler_url = start_control_plane(state)
            host, host_thread, host_result = _run_host(
                state,
                scheduler_url,
                output / "host-artifacts" / case,
            )
            desktop_pid = int(state.session["process_id"])
            case_result = _run_case(
                case=case,
                output=output,
                mft_revision=args.mft_revision,
                mft_repo_url=args.mft_repo_url,
                library=library,
                scheduler_url=scheduler_url,
                state=state,
                timeout_seconds=args.timeout_seconds,
                lmutil=args.lmutil,
                license_server=args.license_server,
                solver_feature=args.solver_license_feature,
                desktop_pid=desktop_pid,
            )
            if case_result["failures"] and host_thread.is_alive():
                state.force_drain(
                    f"1:2 {case} pilot runner failed; stopping disposable host"
                )
                host.request_stop()
            host_thread.join(
                timeout=60 if case_result["failures"] else 300
            )
            if host_thread.is_alive():
                case_result["failures"].append("session_host_did_not_exit")
                host.request_stop()
                host_thread.join(timeout=40)
            expected_host_result = [2] if case == "timeout" else [0]
            if host_result != expected_host_result:
                case_result["failures"].append(f"session_host_exit={host_result!r}")
            expected_session_state = "failed" if case == "timeout" else "closed"
            if state.session.get("state") != expected_session_state or not state.closed_ack:
                case_result["failures"].append("desktop_close_ack_missing")
            expected_close_acks = {2} if case == "timeout" else {1, 2}
            if (
                set(state.project_close_acks) != expected_close_acks
                or not all(state.project_close_acks.values())
            ):
                case_result["failures"].append(
                    f"project_close_acks={state.project_close_acks!r}"
                )
            if case == "timeout":
                if state.timeout_owner_lease_id in state.project_close_acks:
                    case_result["failures"].append("timeout_owner_locally_closed")
                if state.requeued_lease_ids != [state.timeout_owner_lease_id]:
                    case_result["failures"].append(
                        f"timeout_owner_requeued={state.requeued_lease_ids!r}"
                    )
                event_names = [event["event"] for event in state.events]
                try:
                    sibling_release = max(
                        index for index, event in enumerate(state.events)
                        if event["event"] == "release_requested" and event.get("lease_id") == 2
                    )
                    global_stop = event_names.index("global_stop_allowed")
                    if global_stop <= sibling_release:
                        case_result["failures"].append("global_stop_preceded_sibling_completion")
                except (ValueError, StopIteration):
                    case_result["failures"].append("timeout_recycle_event_order_missing")
            if process_alive(str(state.session.get("process_id") or "")):
                case_result["failures"].append("desktop_process_still_alive")
            case_result["failures"].extend(_cleanup_project_workspaces(case_result))

            desktop_returned = False
            solver_returned = False
            return_deadline = time.monotonic() + max(1, args.license_return_wait_seconds)
            after_index = 0
            while time.monotonic() < return_deadline:
                after_path = output / case / f"lmstat_after_{after_index:04d}.txt"
                case_result["license_records"].append(
                    _lmstat_snapshot(args.lmutil, args.license_server, after_path)
                )
                text = after_path.read_text(encoding="utf-8", errors="replace")
                desktop_present = _feature_pid_present(
                    text,
                    "electronics_desktop",
                    getpass.getuser(),
                    socket.gethostname(),
                    desktop_pid,
                )
                solver_present = any(
                    _feature_pid_present(
                        text,
                        args.solver_license_feature,
                        getpass.getuser(),
                        socket.gethostname(),
                        int(pid),
                    )
                    for pid in case_result["solver_pids"]
                )
                desktop_returned = not desktop_present
                solver_returned = not solver_present
                if desktop_returned and solver_returned:
                    break
                after_index += 1
                time.sleep(5)
            case_result["desktop_checkout_returned"] = desktop_returned
            case_result["maxwell_solver_checkout_returned"] = solver_returned
            if not case_result["desktop_checkout_seen"]:
                case_result["failures"].append("desktop_checkout_not_observed")
            if not desktop_returned:
                case_result["failures"].append("desktop_checkout_not_returned")
            if not solver_returned:
                case_result["failures"].append("maxwell_solver_checkout_not_returned")
            if case == "timeout":
                sibling = (case_result.get("results") or {}).get("B") or {}
                case_result.update({
                    "sibling_terminal_output_passed": bool(sibling),
                    "sibling_data_rows_passed": int(sibling.get("result_valid_em") or 0) == 1,
                    "sibling_field_solution_passed": (
                        int(sibling.get("matrix_solution_queries") or 0) >= 1
                        and float(sibling.get("Llt") or 0.0) > 0.0
                    ),
                    "fault_checkout_released_after_recycle_passed": solver_returned,
                    "faulted_desktop_not_reused_passed": bool(
                        case_result.get("new_lease_rejected_after_quarantine")
                    ),
                })
                for key in (
                    "sibling_terminal_output_passed",
                    "sibling_data_rows_passed",
                    "sibling_field_solution_passed",
                    "fault_checkout_released_after_recycle_passed",
                    "faulted_desktop_not_reused_passed",
                ):
                    if not case_result[key]:
                        case_result["failures"].append(key)
            case_result["passed"] = not case_result["failures"]
            case_result["lease_states"] = {
                str(key): value["state"] for key, value in state.leases.items()
            }
            case_result["project_close_acks"] = state.project_close_acks
            case_result["session"] = state.session
            case_result["events"] = state.events
            cases.append(case_result)
            all_failures.extend(
                f"{case}:{failure}" for failure in case_result["failures"]
            )
            if host_thread.is_alive():
                raise RuntimeError(
                    "session host remained alive after stop request; "
                    "refusing to close its control plane or start another case"
                )
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=10)
            if server_thread.is_alive():
                raise RuntimeError("control-plane server thread did not exit")
            server = None
            server_thread = None
            host = None
            host_thread = None

        evidence = {
            "schema_version": 1,
            "pilot": "aedt_pool_mft_shared_1to2",
            "production_pool_enabled": False,
            "adapter_ready": False,
            "projects_per_aedt_tested": 2,
            "selected_cases": selected_cases,
            "passed": not all_failures,
            "failures": all_failures,
            "mft_revision": args.mft_revision,
            "scheduler_revision": scheduler_revision,
            "library_revision": args.library_revision,
            "cases": cases,
            "completed_at": _now(),
        }
        evidence_path.write_text(
            json.dumps(evidence, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(json.dumps(evidence, indent=2, ensure_ascii=False))
        return 0 if evidence["passed"] else 2
    except Exception as exc:
        all_failures.append(f"{type(exc).__name__}: {exc}")
        evidence_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "pilot": "aedt_pool_mft_shared_1to2",
                    "production_pool_enabled": False,
                    "adapter_ready": False,
                    "passed": False,
                    "failures": all_failures,
                    "mft_revision": args.mft_revision,
                    "scheduler_revision": scheduler_revision,
                    "library_revision": args.library_revision,
                    "cases": cases,
                    "completed_at": _now(),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        raise
    finally:
        if host_thread and host_thread.is_alive() and case_states:
            case_states[-1].force_drain("pilot finalizer requested disposable drain")
            if host is not None:
                host.request_stop()
            host_thread.join(timeout=60)
            if host_thread.is_alive():
                host_thread.join(timeout=40)
        if server is not None and not (
            host_thread is not None and host_thread.is_alive()
        ):
            server.shutdown()
            server.server_close()
            if server_thread is not None:
                server_thread.join(timeout=10)


if __name__ == "__main__":
    raise SystemExit(main())
