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
    _clone_exact,
    _feature_pid_present,
    _lmstat_snapshot,
    _now,
    parse_result_json,
    process_alive,
    start_control_plane,
)
from slurm_scheduler.aedt_session_host import (  # noqa: E402
    AedtSessionHost,
    ControlPlaneClient,
)
from slurm_scheduler.aedt_attach_client import AedtPoolHttpClient  # noqa: E402


TERMINAL_LEASE_STATES = {"released", "failed", "cancelled", "expired"}


class SharedPilotControlPlane:
    """Minimal two-slot loopback protocol with project-local release."""

    def __init__(self) -> None:
        import secrets

        self.lock = threading.RLock()
        self.bootstrap_token = secrets.token_urlsafe(24)
        self.host_token = secrets.token_urlsafe(24)
        self.session = {
            "id": 1,
            "state": "starting",
            "host_id": "",
            "endpoint": "",
            "process_id": "",
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

    def event(self, name: str, **values: Any) -> None:
        self.events.append({"time": _now(), "event": name, **values})

    def force_drain(self, reason: str) -> None:
        with self.lock:
            for lease in self.leases.values():
                if lease["state"] not in TERMINAL_LEASE_STATES:
                    lease["state"] = "releasing"
                    lease["failure_message"] = reason
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
            if lease["state"] not in {"leased", "active"}:
                raise RuntimeError(f"abort lease is {lease['state']}")
            lease["state"] = "releasing"
            lease["failure_message"] = "pre_solve client abort injection"
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
            if lease["state"] not in {"leased", "active"}:
                raise RuntimeError(f"timeout lease is {lease['state']}")
            lease["state"] = "releasing"
            lease["failure_message"] = "solver timeout injection while solve was active"
            self.timeout_owner_lease_id = int(lease["id"])
            self.quarantine_reason = "solver_timeout"
            self.session["state"] = "draining"
            self.event(
                "solver_timeout_reported",
                lease_id=lease["id"],
                project_name=project_name,
            )
            return int(lease["id"])

    def _public_lease(self, lease_id: int) -> dict[str, Any]:
        return dict(self.leases[lease_id])

    def _live_siblings(self) -> int:
        return sum(
            lease["state"] in {"leased", "active"}
            for lease in self.leases.values()
        )

    def _all_projects_closed(self) -> bool:
        return (
            len(self.leases) == 2
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
                if len(self.leases) >= 2:
                    return 409, {"detail": "pilot permits exactly two leases"}
                if payload.get("exclusive_session") is not False:
                    return 422, {"detail": "1:2 pilot requires exclusive_session=false"}
                lease_id = len(self.leases) + 1
                token = secrets.token_urlsafe(24)
                lease = {
                    "id": lease_id,
                    "state": (
                        "leased"
                        if self.session["state"] in {"ready", "busy"}
                        else "queued"
                    ),
                    "endpoint": self.session["endpoint"],
                    "project_name": str(payload.get("project_name") or ""),
                    "exclusive_session": 0,
                    "slot_index": lease_id - 1,
                    "failure_message": "",
                }
                self.leases[lease_id] = lease
                self.client_tokens[lease_id] = token
                self.session["state"] = "busy" if lease["state"] == "leased" else self.session["state"]
                self.event("lease_created", lease_id=lease_id, slot_index=lease_id - 1)
                return 200, {"lease": dict(lease), "client_token": token}

            if method == "POST" and path == "/api/aedt-pool/hosts/claim-start":
                if headers.get("X-AEDT-Bootstrap-Token", "") != self.bootstrap_token:
                    return 403, {"detail": "invalid bootstrap token"}
                self.session["host_id"] = str(payload.get("host_id") or "")
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
                self.session.update({
                    "state": "ready",
                    "endpoint": str(payload.get("endpoint") or ""),
                    "process_id": str(payload.get("process_id") or ""),
                })
                for lease in self.leases.values():
                    if lease["state"] == "queued":
                        lease.update({"state": "leased", "endpoint": self.session["endpoint"]})
                if self.leases:
                    self.session["state"] = "busy"
                self.event(
                    "host_registered",
                    endpoint=self.session["endpoint"],
                    process_id=self.session["process_id"],
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
                    return 200, dict(lease)
                if method == "POST" and suffix == "/heartbeat":
                    if lease["state"] == "queued" and self.session["endpoint"]:
                        lease.update({"state": "leased", "endpoint": self.session["endpoint"]})
                    if lease["state"] == "leased":
                        lease["state"] = "active"
                    return 200, dict(lease)
                if method == "PATCH" and suffix == "/project-name":
                    lease["project_name"] = str(payload.get("project_name") or "")
                    self.event(
                        "project_bound",
                        lease_id=lease_id,
                        project_name=lease["project_name"],
                    )
                    return 200, dict(lease)
                if method == "POST" and suffix == "/release":
                    if lease["state"] not in TERMINAL_LEASE_STATES:
                        lease["state"] = "releasing"
                    self.event("release_requested", lease_id=lease_id)
                    return 200, dict(lease)
                if method == "POST" and suffix == "/fault":
                    kind = str(payload.get("fault_kind") or "")
                    if kind == "solver_timeout":
                        self.report_solver_timeout(str(lease.get("project_name") or ""))
                        return 200, dict(lease)
                    if kind not in {"pre_solve", "script_error"}:
                        return 409, {"detail": "unsupported pilot fault"}
                    lease["state"] = "releasing"
                    lease["failure_message"] = str(payload.get("failure_message") or kind)
                    self.event("fault_reported", lease_id=lease_id, kind=kind)
                    return 200, dict(lease)

            if path.startswith("/api/aedt-pool/sessions/1"):
                if headers.get("X-AEDT-Host-Token", "") != self.host_token:
                    return 403, {"detail": "invalid host token"}
                suffix = path.removeprefix("/api/aedt-pool/sessions/1")
                if method == "POST" and suffix == "/heartbeat":
                    return 200, dict(self.session)
                if method == "GET" and suffix == "/commands":
                    close_projects = [
                        dict(lease) for lease in self.leases.values()
                        if lease["state"] == "releasing"
                        and int(lease["id"]) != self.timeout_owner_lease_id
                    ]
                    deferred_projects = [
                        dict(lease) for lease in self.leases.values()
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
                            if lease["state"] in {"leased", "active", "releasing"}:
                                lease["state"] = "queued"
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
    for label in ("A", "B"):
        mft = case_dir / f"MFT_{label}"
        _clone_exact(mft_repo_url, mft_revision, mft)
        env = os.environ.copy()
        env.update({
            "MFT_AEDT_BACKEND": "pooled",
            "MFT_AEDT_SHARED_1TO2_PILOT": "1",
            "MFT_AEDT_SCHEDULER_URL": scheduler_url,
            "MFT_SLURM_SCHEDULER_ROOT": str(ROOT),
            "MFT_PYAEDT_LIBRARY_ROOT": str(library),
            "MFT_AEDT_LEASE_WAIT_SECONDS": "300",
            "MFT_AEDT_RELEASE_WAIT_SECONDS": "300",
            "MFT_AEDT_PILOT_CLIENT_LABEL": label,
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
                "run_simulation_260706.py",
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
        failures.extend(
            f"runner_{label}_{failure}" for failure in _valid_matrix_result(result)
        )

    if case == "normal":
        lease_ids = {int(result.get("aedt_lease_id") or 0) for result in results.values()}
        project_names = {str(result.get("project_name") or "") for result in results.values()}
        if lease_ids != {1, 2}:
            failures.append(f"normal_lease_ids={sorted(lease_ids)}")
        if len(project_names) != 2:
            failures.append("normal_project_names_not_distinct")
        if solver_peak < 2:
            failures.append(f"maxwell_solver_peak={solver_peak}<2")
    elif case == "abort":
        if not injected or aborted_lease_id is None:
            failures.append("pre_solve_abort_not_injected")
        if set(results) != {"B"}:
            failures.append(f"abort_terminal_result_labels={sorted(results)}")
    else:
        if not injected or aborted_lease_id is None:
            failures.append("solver_timeout_not_injected")
        if set(results) != {"B"}:
            failures.append(f"timeout_terminal_result_labels={sorted(results)}")

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


def _run_host(state: SharedPilotControlPlane, scheduler_url: str) -> tuple[AedtSessionHost, threading.Thread, list[int]]:
    host = AedtSessionHost(
        ControlPlaneClient(scheduler_url, bootstrap_token=state.bootstrap_token),
        allocation_id=1,
        node_name=socket.gethostname(),
        heartbeat_seconds=5,
    )
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(host.run()), daemon=True)
    thread.start()
    deadline = time.monotonic() + 300
    while not state.session["endpoint"] and time.monotonic() < deadline:
        time.sleep(1)
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
    host = None
    host_thread = None
    try:
        for case in selected_cases:
            state = SharedPilotControlPlane()
            case_states.append(state)
            server, _server_thread, scheduler_url = start_control_plane(state)
            host, host_thread, host_result = _run_host(state, scheduler_url)
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
            host_thread.join(timeout=300)
            if host_thread.is_alive():
                case_result["failures"].append("session_host_did_not_exit")
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
            server.shutdown()
            server.server_close()
            server = None
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
            host_thread.join(timeout=60)
        if host_thread and host_thread.is_alive() and host is not None:
            host.request_stop()
            host_thread.join(timeout=40)
        if server is not None:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    raise SystemExit(main())
