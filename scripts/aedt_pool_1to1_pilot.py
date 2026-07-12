"""Disposable real 1-AEDT:1-MFT-project attach pilot.

This does not enable the production pool.  A loopback-only in-process control
plane drives the real scheduler session_host and attach_client in one isolated
Slurm task.  Success requires terminal solver output, an exclusive lease,
project-close ACK, Desktop process exit, license checkout/return evidence, and
workspace cleanup.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slurm_scheduler.aedt_session_host import (  # noqa: E402
    AedtSessionHost,
    ControlPlaneClient,
)


TERMINAL_LEASE_STATES = {"released", "failed", "cancelled", "expired"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class PilotControlPlane:
    """Minimal loopback protocol implementation, intentionally 1:1 only."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.bootstrap_token = secrets.token_urlsafe(24)
        self.host_token = secrets.token_urlsafe(24)
        self.client_token = secrets.token_urlsafe(24)
        self.session = {
            "id": 1,
            "state": "starting",
            "host_id": "",
            "endpoint": "",
            "process_id": "",
        }
        self.lease: dict[str, Any] | None = None
        self.project_close_ack = False
        self.closed_ack = False
        self.events: list[dict[str, Any]] = []

    def event(self, name: str, **values: Any) -> None:
        self.events.append({"time": _now(), "event": name, **values})

    def force_drain(self, reason: str) -> None:
        """Quarantine the disposable session without touching any other task."""
        with self.lock:
            if self.lease and self.lease["state"] not in TERMINAL_LEASE_STATES:
                self.lease["state"] = "releasing"
                self.lease["failure_message"] = reason
            self.session["state"] = "draining"
            self.event("pilot_force_drain", reason=reason)

    def _public_lease(self) -> dict[str, Any]:
        if self.lease is None:
            raise KeyError("lease")
        return dict(self.lease)

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
                self.lease = {
                    "id": 1,
                    "state": (
                        "leased"
                        if self.session["state"] in {"ready", "busy"}
                        else "queued"
                    ),
                    "endpoint": self.session["endpoint"],
                    "project_name": str(payload.get("project_name") or ""),
                    "exclusive_session": 1,
                    "failure_message": "",
                }
                self.event("lease_created", exclusive_session=True)
                return 200, {
                    "lease": self._public_lease(),
                    "client_token": self.client_token,
                }

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
                if self.lease and self.lease["state"] == "queued":
                    self.lease.update({
                        "state": "leased",
                        "endpoint": self.session["endpoint"],
                    })
                    self.session["state"] = "busy"
                self.event(
                    "host_registered",
                    endpoint=self.session["endpoint"],
                    process_id=self.session["process_id"],
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
                    if self.lease["state"] == "queued" and self.session["endpoint"]:
                        self.lease.update({
                            "state": "leased",
                            "endpoint": self.session["endpoint"],
                        })
                    if self.lease["state"] == "leased":
                        self.lease["state"] = "active"
                        self.session["state"] = "busy"
                    return 200, self._public_lease()
                if method == "PATCH" and suffix == "/project-name":
                    self.lease["project_name"] = str(
                        payload.get("project_name") or ""
                    )
                    self.event(
                        "project_bound",
                        project_name=self.lease["project_name"],
                    )
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/release":
                    if self.lease["state"] not in TERMINAL_LEASE_STATES:
                        self.lease["state"] = "releasing"
                    self.event("release_requested")
                    return 200, self._public_lease()
                if method == "POST" and suffix == "/fault":
                    kind = str(payload.get("fault_kind") or "")
                    self.lease["state"] = "releasing"
                    self.lease["failure_message"] = str(
                        payload.get("failure_message") or kind
                    )
                    self.session["state"] = "draining"
                    self.event("fault_reported", kind=kind)
                    return 200, self._public_lease()

            if path.startswith("/api/aedt-pool/sessions/1"):
                if headers.get("X-AEDT-Host-Token", "") != self.host_token:
                    return 403, {"detail": "invalid host token"}
                suffix = path.removeprefix("/api/aedt-pool/sessions/1")
                if method == "POST" and suffix == "/heartbeat":
                    return 200, dict(self.session)
                if method == "GET" and suffix == "/commands":
                    close_projects = []
                    global_stop = False
                    if self.lease and self.lease["state"] == "releasing":
                        faulted = bool(self.lease.get("failure_message"))
                        if faulted:
                            global_stop = True
                        else:
                            close_projects = [self._public_lease()]
                    drain = bool(
                        self.project_close_ack
                        or self.session["state"] in {"draining", "failed"}
                    )
                    return 200, {
                        "close_projects": close_projects,
                        "deferred_projects": [],
                        "drain": drain,
                        "sibling_live_count": 0,
                        "global_stop_allowed": global_stop,
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
                    self.session["state"] = "closed" if success else "failed"
                    if self.lease and self.lease["state"] not in TERMINAL_LEASE_STATES:
                        self.lease["state"] = "failed"
                    self.closed_ack = True
                    self.event("desktop_closed_ack", success=success)
                    return 200, dict(self.session)

            return 404, {"detail": f"unsupported pilot route: {method} {path}"}


class PilotHandler(BaseHTTPRequestHandler):
    server_version = "MftAedtPilot/1"

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
    state: PilotControlPlane,
) -> tuple[ThreadingHTTPServer, threading.Thread, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), PilotHandler)
    server.state = state  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, thread, f"http://{host}:{port}"


def _clone_exact(url: str, revision: str, destination: Path) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("pilot revisions must be full lowercase Git SHAs")
    subprocess.run(
        ["git", "clone", "--no-checkout", url, str(destination)],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(destination), "checkout", "--detach", revision],
        check=True,
    )
    actual = subprocess.check_output(
        ["git", "-C", str(destination), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    if actual != revision:
        raise RuntimeError(f"checkout mismatch: expected={revision}, actual={actual}")
    return actual


def _lmstat_snapshot(
    lmutil: str,
    license_server: str,
    destination: Path,
) -> dict[str, Any]:
    result = subprocess.run(
        [lmutil, "lmstat", "-c", license_server, "-a"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
    )
    destination.write_text(result.stdout, encoding="utf-8")
    return {
        "time": _now(),
        "path": str(destination),
        "returncode": result.returncode,
    }


def _feature_local_pids(
    text: str,
    feature: str,
    user: str,
    host: str,
) -> set[int]:
    if not user.strip() or not host.strip():
        return set()
    start = re.search(
        rf"(?m)^Users of {re.escape(feature)}:.*$", text
    )
    if not start:
        return set()
    tail = text[start.end():]
    next_feature = re.search(r"(?m)^Users of [^:]+:.*$", tail)
    section = tail[: next_feature.start()] if next_feature else tail
    host_short = host.casefold().split(".", 1)[0]
    pids: set[int] = set()
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
            pids.add(int(fields[3]))
    return pids


def _feature_pid_present(
    text: str,
    feature: str,
    user: str,
    host: str,
    pid: int,
) -> bool:
    return pid > 0 and pid in _feature_local_pids(text, feature, user, host)


def parse_result_json(stdout: str) -> dict[str, Any]:
    rows = []
    for line in stdout.splitlines():
        if line.startswith("RESULT_JSON "):
            rows.append(json.loads(line.removeprefix("RESULT_JSON ")))
    if len(rows) != 1:
        raise RuntimeError(f"expected exactly one RESULT_JSON, found {len(rows)}")
    return rows[0]


def process_alive(pid_text: str) -> bool:
    try:
        pid = int(pid_text)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mft-repo-url", required=True)
    parser.add_argument("--mft-revision", required=True)
    parser.add_argument("--library-repo-url", required=True)
    parser.add_argument("--library-revision", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--lmutil", required=True)
    parser.add_argument("--license-server", required=True)
    parser.add_argument("--license-return-wait-seconds", type=int, default=180)
    args = parser.parse_args(argv)

    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = output / "work"
    work.mkdir()
    mft = work / "MFT_1MW_2026"
    library = work / "pyaedt_library"
    evidence_path = output / "pilot_evidence.json"
    state = PilotControlPlane()
    server = None
    host_thread = None
    host: AedtSessionHost | None = None
    host_result: list[int] = []
    license_records: list[dict[str, Any]] = []
    failures: list[str] = []
    try:
        _clone_exact(args.mft_repo_url, args.mft_revision, mft)
        _clone_exact(args.library_repo_url, args.library_revision, library)
        params = {
            "matrix_on": 1,
            "loss_on": 0,
            "thermal_on": 0,
            "keep_project": 0,
        }
        params_path = output / "pilot_params.json"
        params_path.write_text(json.dumps(params, indent=2), encoding="utf-8")
        before = output / "lmstat_before.txt"
        license_records.append(
            _lmstat_snapshot(args.lmutil, args.license_server, before)
        )

        server, _server_thread, scheduler_url = start_control_plane(state)
        host = AedtSessionHost(
            ControlPlaneClient(
                scheduler_url,
                bootstrap_token=state.bootstrap_token,
            ),
            allocation_id=1,
            node_name=socket.gethostname(),
            heartbeat_seconds=5,
        )

        def run_host() -> None:
            host_result.append(host.run())

        host_thread = threading.Thread(target=run_host, daemon=True)
        host_thread.start()
        deadline = time.monotonic() + 300
        while not state.session["endpoint"] and time.monotonic() < deadline:
            time.sleep(1)
        if not state.session["endpoint"]:
            raise RuntimeError("session host did not register within 300 seconds")
        try:
            desktop_pid = int(state.session["process_id"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("session host did not report a Desktop PID") from exc
        if desktop_pid <= 0:
            raise RuntimeError("session host reported an invalid Desktop PID")

        env = os.environ.copy()
        env.update({
            "MFT_AEDT_BACKEND": "pooled",
            "MFT_AEDT_EXCLUSIVE_1TO1": "1",
            "MFT_AEDT_SCHEDULER_URL": scheduler_url,
            "MFT_SLURM_SCHEDULER_ROOT": str(ROOT),
            "MFT_PYAEDT_LIBRARY_ROOT": str(library),
            "MFT_AEDT_LEASE_WAIT_SECONDS": "300",
            "MFT_AEDT_RELEASE_WAIT_SECONDS": "300",
        })
        command = [
            sys.executable,
            "run_simulation_260706.py",
            "--fixed",
            "--params",
            str(params_path),
            "--headless",
        ]
        stdout_path = output / "runner.stdout.log"
        stderr_path = output / "runner.stderr.log"
        run_deadline = time.monotonic() + max(1, args.timeout_seconds)
        with stdout_path.open("w", encoding="utf-8") as stdout_file, \
                stderr_path.open("w", encoding="utf-8") as stderr_file:
            run = subprocess.Popen(
                command,
                cwd=mft,
                env=env,
                text=True,
                stdout=stdout_file,
                stderr=stderr_file,
            )
            during_index = 0
            checkout_seen = False
            while run.poll() is None:
                during_path = output / f"lmstat_during_{during_index:04d}.txt"
                license_records.append(
                    _lmstat_snapshot(
                        args.lmutil, args.license_server, during_path
                    )
                )
                text = during_path.read_text(
                    encoding="utf-8", errors="replace"
                )
                checkout_seen = checkout_seen or _feature_pid_present(
                    text,
                    "electronics_desktop",
                    getpass.getuser(),
                    socket.gethostname(),
                    desktop_pid,
                )
                during_index += 1
                if time.monotonic() > run_deadline:
                    state.force_drain("MFT 1:1 pilot runner timeout")
                    run.terminate()
                    try:
                        run.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        run.kill()
                        run.wait(timeout=30)
                    raise TimeoutError("MFT 1:1 pilot timed out")
                time.sleep(5)
            run.wait(timeout=60)
        stdout = stdout_path.read_text(encoding="utf-8", errors="replace")
        stderr = stderr_path.read_text(encoding="utf-8", errors="replace")
        if run.returncode != 0:
            failures.append(f"runner_exit={run.returncode}")

        if host_thread:
            host_thread.join(timeout=300)
            if host_thread.is_alive():
                failures.append("session_host_did_not_exit")
        result = parse_result_json(stdout)
        required_result = {
            "result_valid_em": 1,
            "aedt_backend": "pooled",
            "aedt_exclusive_session": 1,
            "matrix_solve_attempts": 1,
        }
        for key, expected in required_result.items():
            if result.get(key) != expected:
                failures.append(
                    f"result_{key}_mismatch:{result.get(key)!r}!={expected!r}"
                )
        if int(result.get("aedt_lease_id") or 0) != 1:
            failures.append("result_lease_identity_mismatch")
        if state.lease is None or state.lease.get("state") != "released":
            failures.append("lease_not_released")
        if not state.project_close_ack:
            failures.append("project_close_ack_missing")
        if state.session.get("state") != "closed" or not state.closed_ack:
            failures.append("desktop_close_ack_missing")
        if host_result != [0]:
            failures.append(f"session_host_exit={host_result!r}")
        if process_alive(str(state.session.get("process_id") or "")):
            failures.append("desktop_process_still_alive")
        project_name = str(result.get("project_name") or "")
        project_dir = mft / "simulation" / project_name
        if not project_name or project_dir.exists():
            failures.append("project_workspace_not_cleaned")

        returned = False
        return_deadline = time.monotonic() + max(
            1, args.license_return_wait_seconds
        )
        after_index = 0
        while time.monotonic() < return_deadline:
            after_path = output / f"lmstat_after_{after_index:04d}.txt"
            license_records.append(
                _lmstat_snapshot(
                    args.lmutil, args.license_server, after_path
                )
            )
            after_text = after_path.read_text(
                encoding="utf-8", errors="replace"
            )
            if not _feature_pid_present(
                after_text,
                "electronics_desktop",
                getpass.getuser(),
                socket.gethostname(),
                desktop_pid,
            ):
                returned = True
                break
            after_index += 1
            time.sleep(5)
        if not checkout_seen:
            failures.append("desktop_license_checkout_not_observed")
        if not returned:
            failures.append("desktop_license_checkout_not_returned")

        evidence = {
            "schema_version": 1,
            "pilot": "aedt_pool_mft_exclusive_1to1",
            "production_pool_enabled": False,
            "projects_per_aedt_tested": 1,
            "passed": not failures,
            "failures": failures,
            "mft_revision": args.mft_revision,
            "scheduler_revision": subprocess.check_output(
                ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
            ).strip(),
            "library_revision": args.library_revision,
            "runner_returncode": run.returncode,
            "host_returncode": host_result[0] if host_result else None,
            "terminal_result": result,
            "lease": state.lease,
            "session": state.session,
            "project_close_ack": state.project_close_ack,
            "desktop_closed_ack": state.closed_ack,
            "license_checkout_observed": checkout_seen,
            "license_checkout_returned": returned,
            "license_records": license_records,
            "events": state.events,
            "completed_at": _now(),
        }
        evidence_path.write_text(
            json.dumps(evidence, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(json.dumps(evidence, indent=2, ensure_ascii=False))
        return 0 if evidence["passed"] else 2
    except Exception as exc:
        failures.append(f"{type(exc).__name__}: {exc}")
        evidence_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "pilot": "aedt_pool_mft_exclusive_1to1",
                    "production_pool_enabled": False,
                    "passed": False,
                    "failures": failures,
                    "lease": state.lease,
                    "session": state.session,
                    "events": state.events,
                    "completed_at": _now(),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        raise
    finally:
        if host_thread and host_thread.is_alive():
            state.force_drain("pilot finalizer requested disposable session drain")
            host_thread.join(timeout=60)
        if host_thread and host_thread.is_alive() and host is not None:
            host.request_stop()
            host_thread.join(timeout=40)
        if server is not None:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    raise SystemExit(main())
