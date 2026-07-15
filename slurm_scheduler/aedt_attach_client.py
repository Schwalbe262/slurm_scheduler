from __future__ import annotations

import http.client
import hashlib
import json
import math
import multiprocessing
import os
import random
import re
import secrets
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .aedt_automation_lock import SessionAutomationLock

DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS = 360.0
CONTROL_PLANE_OUTAGE_ENV = "AEDT_POOL_CONTROL_PLANE_OUTAGE_SECONDS"
TRANSIENT_HTTP_STATUSES = {408, 425, 429}


def normalize_aedt_version(value: Any) -> str:
    """Normalize AEDT's verbose version without importing host-only code."""

    match = re.search(r"(?<!\d)(20\d{2}\.\d)(?!\d)", str(value or ""))
    return match.group(1) if match else ""


def _keepalive_delay(
    lease_id: int,
    client_token: str,
    interval_seconds: int,
    cycle: int,
    *,
    initial: bool = False,
) -> float:
    """Stable per-lease jitter prevents a synchronized 500-writer burst."""

    interval = max(5.0, float(interval_seconds))
    digest = hashlib.sha256(
        f"{int(lease_id)}:{client_token}:{int(cycle)}".encode("utf-8")
    ).digest()
    fraction = int.from_bytes(digest[:8], "big") / float((1 << 64) - 1)
    return fraction * interval if initial else interval * (0.75 + 0.5 * fraction)


def _lease_keepalive_worker(
    scheduler_url: str,
    bootstrap_token: str,
    lease_id: int,
    client_token: str,
    heartbeat_seconds: int,
    stop_event: Any,
) -> None:
    """Independent keepalive so a blocked solver/main interpreter cannot starve it."""

    http = AedtPoolHttpClient(scheduler_url, bootstrap_token=bootstrap_token)
    interval = max(5, int(heartbeat_seconds))
    cycle = 0
    if stop_event.wait(
        _keepalive_delay(
            lease_id, client_token, interval, cycle, initial=True
        )
    ):
        return
    while not stop_event.is_set():
        try:
            http.request(
                "POST",
                f"/api/aedt-pool/leases/{int(lease_id)}/heartbeat",
                {},
                lease_token=client_token,
            )
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUSES and not 500 <= exc.code < 600:
                return
        except (
            urllib.error.URLError,
            OSError,
            json.JSONDecodeError,
            http.client.HTTPException,
        ):
            pass
        cycle += 1
        if stop_event.wait(
            _keepalive_delay(lease_id, client_token, interval, cycle)
        ):
            return


def _control_plane_outage_seconds_from_env() -> float:
    value = os.environ.get(CONTROL_PLANE_OUTAGE_ENV, "").strip()
    if not value:
        return DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS
    try:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError
        return max(0.0, parsed)
    except ValueError:
        return DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS


class AedtLeaseError(RuntimeError):
    pass


class AedtPoolHttpClient:
    def __init__(
        self,
        scheduler_url: str,
        *,
        client_credential: str = "",
        client_credential_file: str = "",
        bootstrap_token: str = "",
        bootstrap_token_file: str = "",
    ) -> None:
        self.scheduler_url = scheduler_url.rstrip("/")
        self.client_credential = (
            str(client_credential or "").strip()
            or str(bootstrap_token or "").strip()
            or os.environ.get("SLURM_AEDT_POOL_CLIENT_TOKEN", "").strip()
        )
        token_file = (
            str(client_credential_file or "").strip()
            or str(bootstrap_token_file or "").strip()
            or os.environ.get("SLURM_AEDT_POOL_CLIENT_TOKEN_FILE", "").strip()
        )
        if not self.client_credential and token_file:
            self.client_credential = Path(token_file).read_text(
                encoding="utf-8"
            ).strip()
        # Compatibility for callers that inspect the old field.  It is a
        # lease-create credential and is never sent as an admin/bootstrap header.
        self.bootstrap_token = self.client_credential

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        lease_token: str = "",
        timeout: int = 30,
    ) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if lease_token:
            headers["X-AEDT-Lease-Token"] = lease_token
        elif (
            self.client_credential
            and method.upper() not in {"GET", "HEAD", "OPTIONS"}
        ):
            headers["X-AEDT-Client-Token"] = self.client_credential
        request = urllib.request.Request(
            f"{self.scheduler_url}{path}", data=data, headers=headers, method=method
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8") or "{}")

    def request_with_retry(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        lease_token: str = "",
        outage_seconds: float | None = None,
    ) -> dict[str, Any]:
        budget = (
            _control_plane_outage_seconds_from_env()
            if outage_seconds is None
            else max(0.0, float(outage_seconds))
        )
        started = time.monotonic()
        retry_index = 0
        while True:
            try:
                return self.request(
                    method, path, payload, lease_token=lease_token
                )
            except urllib.error.HTTPError as exc:
                if not (
                    exc.code in TRANSIENT_HTTP_STATUSES or 500 <= exc.code < 600
                ):
                    raise
                last_error: BaseException = exc
            except (
                urllib.error.URLError,
                OSError,
                json.JSONDecodeError,
                http.client.HTTPException,
            ) as exc:
                last_error = exc
            remaining = budget - max(0.0, time.monotonic() - started)
            if remaining <= 0:
                raise last_error
            base = min(20.0, 2 ** min(retry_index, 20))
            time.sleep(
                min(remaining, base * 0.5 + random.uniform(0.0, base * 0.5))
            )
            retry_index += 1


@dataclass
class AedtProjectLease:
    """Project-side lease; it never owns the shared Desktop lifecycle."""

    http: AedtPoolHttpClient
    lease_id: int
    client_token: str
    project_name: str
    state: str = "queued"
    endpoint: str = ""
    exclusive_session: bool = False
    protocol_version: int = 1
    workload_family: str = ""
    session_profile: str = ""
    project_namespace: str = ""
    isolation_policy: str = "family"
    workspace_path: str = ""
    automation_lock_path: str = ""
    session_key: str = ""
    session_process_id: str = ""
    expected_aedt_version: str = ""
    solve_permit_required: bool = False
    solve_permit_granted: bool = False
    solve_permit_generation: int = 0
    session_active_lease_count: int = 0
    session_live_lease_count: int = 0
    session_slots_total: int = 0
    control_plane_outage_seconds: float = field(
        default_factory=_control_plane_outage_seconds_from_env
    )
    _heartbeat_stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _heartbeat_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _keepalive_process: Any = field(default=None, init=False, repr=False)
    _keepalive_stop: Any = field(default=None, init=False, repr=False)
    _desktop_proxy: Any = field(default=None, init=False, repr=False)
    _automation_lock: SessionAutomationLock | None = field(
        default=None, init=False, repr=False
    )
    heartbeat_error: str = field(default="", init=False)
    detach_error: str = field(default="", init=False)

    def __post_init__(self) -> None:
        outage_seconds = float(self.control_plane_outage_seconds)
        if not math.isfinite(outage_seconds):
            raise ValueError("control-plane outage budget must be finite")
        self.control_plane_outage_seconds = max(0.0, outage_seconds)

    def _call(self, method: str, suffix: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.http.request(
            method,
            f"/api/aedt-pool/leases/{self.lease_id}{suffix}",
            payload,
            lease_token=self.client_token,
        )

    def _call_with_retry(
        self,
        method: str,
        suffix: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        outage_started_at = time.monotonic()
        retry_index = 0
        while True:
            try:
                return self._call(method, suffix, payload)
            except urllib.error.HTTPError as exc:
                if not (
                    exc.code in TRANSIENT_HTTP_STATUSES or 500 <= exc.code < 600
                ):
                    raise
                last_error: BaseException = exc
            except (
                urllib.error.URLError,
                OSError,
                json.JSONDecodeError,
                http.client.HTTPException,
            ) as exc:
                last_error = exc
            remaining = self.control_plane_outage_seconds - max(
                0.0, time.monotonic() - outage_started_at
            )
            if remaining <= 0:
                raise last_error
            base_delay = min(20.0, 2 ** min(retry_index, 20))
            delay = min(
                remaining,
                base_delay * 0.5 + random.uniform(0.0, base_delay * 0.5),
            )
            if self._heartbeat_thread is threading.current_thread():
                if self._heartbeat_stop.wait(delay):
                    raise AedtLeaseError("lease operation retry stopped") from last_error
            else:
                time.sleep(delay)
            retry_index += 1

    def status(self) -> dict[str, Any]:
        status = self._call_with_retry("GET", "")
        self._apply_status(status)
        return status

    def _apply_status(self, status: dict[str, Any]) -> None:
        self.state = str(status.get("state") or "")
        self.endpoint = str(status.get("endpoint") or "")
        self.automation_lock_path = str(
            status.get("automation_lock_path") or self.automation_lock_path
        )
        self.session_key = str(status.get("session_key") or self.session_key)
        self.session_process_id = str(
            status.get("session_process_id") or self.session_process_id
        )
        self.expected_aedt_version = str(
            status.get("expected_aedt_version") or self.expected_aedt_version
        )
        self.solve_permit_required = bool(
            status.get("solve_permit_required", self.solve_permit_required)
        )
        self.solve_permit_granted = bool(
            status.get("solve_permit_granted", self.solve_permit_granted)
        )
        self.solve_permit_generation = int(
            status.get("solve_permit_generation")
            or self.solve_permit_generation
            or 0
        )
        self.session_active_lease_count = int(
            status.get("session_active_lease_count") or 0
        )
        self.session_live_lease_count = int(
            status.get("session_live_lease_count") or 0
        )
        self.session_slots_total = int(status.get("session_slots_total") or 0)

    def heartbeat(self) -> dict[str, Any]:
        status = self._call_with_retry("POST", "/heartbeat", {})
        self._apply_status(status)
        self.heartbeat_error = ""
        return status

    def wait_until_leased(
        self,
        *,
        timeout_seconds: int = 1800,
        heartbeat_seconds: int = 20,
    ) -> dict[str, Any]:
        """Heartbeat while queued; node waits must not expire a valid request."""
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        interval = max(5, int(heartbeat_seconds))
        first_poll = True
        poll_cycle = 0
        try:
            while time.monotonic() < deadline:
                # The create call already persisted a fresh heartbeat.  Start
                # with a read so a 500-client wave does not immediately become
                # a 500-writer SQLite wave.
                keepalive = self._keepalive_process
                keepalive_alive = bool(
                    keepalive is not None and keepalive.is_alive()
                )
                status = (
                    self.status()
                    if first_poll or keepalive_alive
                    else self.heartbeat()
                )
                first_poll = False
                if self.state == "offered":
                    status = self._call_with_retry("POST", "/accept", {})
                    self.state = str(status.get("state") or "")
                    self.endpoint = str(status.get("endpoint") or "")
                if self.state in {"leased", "attaching", "active"}:
                    if not self.endpoint:
                        raise AedtLeaseError("lease has no session-host endpoint")
                    if not self._keepalive_process:
                        self.start_heartbeat(heartbeat_seconds=interval)
                    return status
                if self.state not in {"queued"}:
                    raise AedtLeaseError(
                        f"AEDT lease {self.lease_id} became {self.state}: "
                        f"{status.get('failure_message') or ''}"
                    )
                time.sleep(
                    _keepalive_delay(
                        self.lease_id,
                        self.client_token,
                        interval,
                        poll_cycle,
                    )
                )
                poll_cycle += 1
        except Exception:
            if self.state in {"queued", "offered", "leased", "attaching"}:
                try:
                    self.cancel(reason="lease admission failed or timed out")
                except Exception:
                    pass
            raise
        try:
            self.cancel(reason="lease admission timed out")
        finally:
            raise TimeoutError(f"timed out waiting for AEDT lease {self.lease_id}")

    def start_process_keepalive(self, *, heartbeat_seconds: int = 20) -> None:
        """Keep the lease alive in a child process from queued through release."""

        process = self._keepalive_process
        if process is not None and process.is_alive():
            return
        if multiprocessing.current_process().daemon:
            self.start_heartbeat(heartbeat_seconds=heartbeat_seconds)
            return
        context = multiprocessing.get_context("spawn")
        stop_event = context.Event()
        process = context.Process(
            target=_lease_keepalive_worker,
            args=(
                self.http.scheduler_url,
                self.http.bootstrap_token,
                self.lease_id,
                self.client_token,
                max(5, int(heartbeat_seconds)),
                stop_event,
            ),
            name=f"aedt-lease-keepalive-{self.lease_id}",
            daemon=True,
        )
        try:
            process.start()
        except (AssertionError, OSError):
            self.start_heartbeat(heartbeat_seconds=heartbeat_seconds)
            return
        self._keepalive_stop = stop_event
        self._keepalive_process = process

    def stop_process_keepalive(self) -> None:
        process = self._keepalive_process
        stop_event = self._keepalive_stop
        if stop_event is not None:
            stop_event.set()
        if process is not None:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
        self._keepalive_process = None
        self._keepalive_stop = None

    def start_heartbeat(self, *, heartbeat_seconds: int = 60) -> None:
        """Keep ownership alive throughout a long solver call."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        process = self._keepalive_process
        if process is not None and process.is_alive():
            return
        self._heartbeat_stop.clear()
        interval = max(5, int(heartbeat_seconds))

        def run() -> None:
            cycle = 0
            if self._heartbeat_stop.wait(
                _keepalive_delay(
                    self.lease_id,
                    self.client_token,
                    interval,
                    cycle,
                    initial=True,
                )
            ):
                return
            while not self._heartbeat_stop.is_set():
                try:
                    self.heartbeat()
                except Exception as exc:
                    self.heartbeat_error = str(exc)
                    return
                cycle += 1
                if self._heartbeat_stop.wait(
                    _keepalive_delay(
                        self.lease_id,
                        self.client_token,
                        interval,
                        cycle,
                    )
                ):
                    return

        self._heartbeat_thread = threading.Thread(
            target=run,
            name=f"aedt-lease-heartbeat-{self.lease_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=5)
        self._heartbeat_thread = None

    def connect_desktop(
        self,
        *,
        version: str = "",
        non_graphical: bool = True,
        desktop_factory: Any = None,
        endpoint_probe: Any = None,
    ) -> Any:
        """Attach PyAEDT to the leased host without taking shutdown ownership.

        Do not use the returned object as a context manager and do not call
        release_desktop(close_desktop=True).  `release()` asks the node-side
        owner to close only this project.
        """
        if self.state not in {"leased", "attaching", "active"} or not self.endpoint:
            self.wait_until_leased()
        # Refresh the token-authorized session identity immediately before the
        # wrapper attaches; do not rely on stale fields from the offer response.
        self.status()
        using_default_factory = desktop_factory is None
        if desktop_factory is None:
            try:
                from ansys.aedt.core import Desktop as desktop_factory
            except ImportError as exc:
                raise AedtLeaseError("PyAEDT is required by the project client") from exc
        # PyAEDT otherwise keeps a process-global ``Desktop`` singleton and a
        # second sequential lease can silently reuse the wrapper for the first
        # endpoint.  Set this before constructing *any* production wrapper.
        # Custom factories remain usable in tests/environments without PyAEDT,
        # but when PyAEDT is present the setting is still mandatory.
        self._enable_pyaedt_multi_desktop(required=using_default_factory)
        machine, port_text = self.endpoint.rsplit(":", 1)
        endpoint_port = int(port_text)
        probe = endpoint_probe or self._endpoint_is_listening
        if not bool(probe(machine, endpoint_port)):
            message = (
                f"authorized AEDT endpoint {machine}:{endpoint_port} is not listening; "
                "refusing PyAEDT constructor auto-launch fallback"
            )
            try:
                self.report_fault(
                    "attach_failed",
                    phase="attach",
                    evidence={
                        "session_key": self.session_key,
                        "expected_process_id": self.session_process_id,
                        "endpoint": self.endpoint,
                    },
                    failure_message=message,
                )
            except Exception:
                pass
            raise AedtLeaseError(message)
        kwargs: dict[str, Any] = {
            "new_desktop": False,
            "non_graphical": non_graphical,
            "close_on_exit": False,
            "machine": machine,
            "port": endpoint_port,
        }
        resolved_version = str(version or "").strip()
        if self.protocol_version >= 2:
            if not self.expected_aedt_version:
                raise AedtLeaseError(
                    "authorized expected AEDT version is unavailable"
                )
            if (
                resolved_version
                and normalize_aedt_version(resolved_version)
                != self.expected_aedt_version
            ):
                message = (
                    f"requested AEDT version {resolved_version!r} does not match "
                    f"authorized {self.expected_aedt_version}"
                )
                try:
                    self.report_fault(
                        "attach_failed",
                        phase="attach",
                        failure_message=message,
                    )
                except Exception:
                    pass
                raise AedtLeaseError(message)
            resolved_version = self.expected_aedt_version
        if resolved_version:
            kwargs["version"] = resolved_version
        with self.automation_guard():
            desktop = desktop_factory(**kwargs)
            self._desktop_proxy = desktop
            # Attaching is intentionally distinct from active: the client must
            # create/open and fully model its first design before activation.
            try:
                self._attest_attached_desktop(desktop)
            except Exception as exc:
                try:
                    self.report_fault(
                        "attach_failed",
                        phase="attach",
                        evidence={
                            "session_key": self.session_key,
                            "expected_endpoint": self.endpoint,
                            "expected_process_id": self.session_process_id,
                            "expected_aedt_version": self.expected_aedt_version,
                        },
                        failure_message=str(exc),
                    )
                except Exception:
                    pass
                if self.state in {"released", "failed", "cancelled", "expired"}:
                    self._detach_wrapper_best_effort()
                raise AedtLeaseError(f"AEDT attach identity check failed: {exc}") from exc
        self.heartbeat()
        if not self._keepalive_process:
            self.start_heartbeat()
        return desktop

    def automation_lock(self) -> SessionAutomationLock:
        """Return this session's re-entrant Desktop-global automation lock."""

        if not self.automation_lock_path:
            self.status()
        if not self.automation_lock_path:
            raise AedtLeaseError(
                "authorized AEDT session has no automation lock path"
            )
        if (
            self._automation_lock is None
            or self._automation_lock.path != self.automation_lock_path
        ):
            raw_timeout = os.environ.get(
                "AEDT_POOL_AUTOMATION_LOCK_TIMEOUT_SECONDS", "1800"
            ).strip()
            try:
                timeout_seconds = float(raw_timeout)
            except (TypeError, ValueError) as exc:
                raise AedtLeaseError(
                    "AEDT_POOL_AUTOMATION_LOCK_TIMEOUT_SECONDS must be numeric"
                ) from exc
            if not 30 <= timeout_seconds <= 7200:
                raise AedtLeaseError(
                    "AEDT_POOL_AUTOMATION_LOCK_TIMEOUT_SECONDS must be between "
                    "30 and 7200"
                )
            self._automation_lock = SessionAutomationLock(
                self.automation_lock_path,
                timeout_seconds=timeout_seconds,
            )
        return self._automation_lock

    def automation_guard(self):
        """Protocol-v1 compatibility; v2 always requires the host lock."""

        if self.protocol_version < 2:
            return nullcontext()
        return self.automation_lock()

    def native_solve_window(self):
        """Release a held automation transaction around exact native Analyze."""

        if self.protocol_version < 2:
            return nullcontext()
        return self.automation_lock().suspended()

    @staticmethod
    def _endpoint_is_listening(machine: str, port: int) -> bool:
        try:
            with socket.create_connection((machine, int(port)), timeout=2.0):
                return True
        except OSError:
            return False

    @staticmethod
    def _enable_pyaedt_multi_desktop(*, required: bool) -> None:
        try:
            # Public PyAEDT import (including the pinned 0.22.0 runtime).
            from ansys.aedt.core import settings
        except ImportError:
            try:
                # Compatibility with older builds that did not re-export it.
                from ansys.aedt.core.generic.settings import settings
            except ImportError as exc:
                if required:
                    raise AedtLeaseError(
                        "PyAEDT multi-desktop settings are unavailable"
                    ) from exc
                return
        try:
            settings.use_multi_desktop = True
        except Exception as exc:
            raise AedtLeaseError(
                "failed to enable PyAEDT multi-desktop endpoint isolation"
            ) from exc
        if getattr(settings, "use_multi_desktop", None) is not True:
            raise AedtLeaseError(
                "PyAEDT refused multi-desktop endpoint isolation"
            )

    def _detach_wrapper_best_effort(self) -> None:
        desktop = self._desktop_proxy
        if desktop is None:
            return
        try:
            with self.automation_guard():
                desktop.release_desktop(
                    close_projects=False,
                    close_on_exit=False,
                )
                self.detach_error = ""
        except Exception as exc:
            self.detach_error = str(exc)
        finally:
            self._desktop_proxy = None

    @staticmethod
    def _wrapper_port(desktop: Any) -> int:
        for name in ("port", "grpc_port"):
            value = getattr(desktop, name, 0)
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
        return 0

    @staticmethod
    def _wrapper_process_id(desktop: Any) -> str:
        for name in ("aedt_process_id", "process_id"):
            value = getattr(desktop, name, "")
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return str(parsed)
        return ""

    def _attest_attached_desktop(self, desktop: Any) -> None:
        if self.protocol_version < 2:
            return
        odesktop = getattr(desktop, "odesktop", None)
        if odesktop is None:
            raise RuntimeError("Desktop wrapper has no initialized odesktop")
        get_version = getattr(odesktop, "GetVersion", None)
        if not callable(get_version):
            raise RuntimeError("Desktop native GetVersion API is unavailable")
        try:
            expected_port = int(self.endpoint.rsplit(":", 1)[1])
        except (IndexError, TypeError, ValueError) as exc:
            raise RuntimeError("authorized session endpoint has no valid port") from exc
        actual_port = self._wrapper_port(desktop)
        if actual_port != expected_port:
            raise RuntimeError(
                f"Desktop port mismatch: expected {expected_port}, got {actual_port or '<unknown>'}"
            )
        actual_pid = self._wrapper_process_id(desktop)
        if not self.session_process_id:
            raise RuntimeError("authorized session process_id is unavailable")
        if actual_pid != self.session_process_id:
            raise RuntimeError(
                "Desktop PID mismatch: "
                f"expected {self.session_process_id}, got {actual_pid or '<unknown>'}"
            )
        actual_version = normalize_aedt_version(get_version())
        if not self.expected_aedt_version:
            raise RuntimeError("authorized expected AEDT version is unavailable")
        if actual_version != self.expected_aedt_version:
            raise RuntimeError(
                "AEDT version mismatch: "
                f"expected {self.expected_aedt_version}, got {actual_version or '<unknown>'}"
            )

    def activate(self, project_name: str = "") -> dict[str, Any]:
        """Confirm project creation and transition attaching -> active once."""

        normalized_project = str(project_name or "").strip()
        if normalized_project and normalized_project != self.project_name:
            self.bind_project_name(normalized_project)
        if self.protocol_version < 2:
            return self.heartbeat()
        status = self.status()
        if self.state == "active":
            if self.solve_permit_required and not self.solve_permit_granted:
                return self.wait_for_solve_permit()
            return status
        if self.state != "attaching":
            raise AedtLeaseError(
                f"AEDT lease {self.lease_id} cannot activate from {self.state}"
            )
        activated = self._call_with_retry("POST", "/activate", {})
        self._apply_status(activated)
        if self.state != "active":
            raise AedtLeaseError(
                f"AEDT lease {self.lease_id} activation returned {self.state}"
            )
        # Protocol-v2 activation is also the native-solve barrier.  The first
        # project waits without holding AEDT automation, allowing its sibling
        # to attach/activate.  A full batch receives one atomic generation; an
        # underfilled batch is sealed after a bounded wait so no late lease can
        # attach after native analyze begins.
        if self.solve_permit_required and not self.solve_permit_granted:
            return self.wait_for_solve_permit()
        return activated

    def wait_for_solve_permit(
        self,
        *,
        fill_timeout_seconds: float | None = None,
        poll_seconds: float = 2.0,
    ) -> dict[str, Any]:
        if self.protocol_version < 2 or self.solve_permit_granted:
            return self.status()
        if fill_timeout_seconds is None:
            raw_timeout = os.environ.get(
                "MFT_AEDT_POOL_FILL_TIMEOUT_SECONDS", "900"
            ).strip()
            try:
                fill_timeout_seconds = float(raw_timeout)
            except ValueError as exc:
                raise AedtLeaseError(
                    "MFT_AEDT_POOL_FILL_TIMEOUT_SECONDS must be numeric"
                ) from exc
        timeout = float(fill_timeout_seconds)
        if not math.isfinite(timeout) or not 0 <= timeout <= 900:
            raise AedtLeaseError(
                "pooled AEDT fill timeout must be between 0 and 900 seconds"
            )
        deadline = time.monotonic() + timeout
        last_inline_heartbeat = time.monotonic()
        latest: dict[str, Any] = {}
        while True:
            process = self._keepalive_process
            process_alive = bool(
                process is not None
                and getattr(process, "is_alive", lambda: False)()
            )
            thread_alive = bool(
                self._heartbeat_thread is not None
                and self._heartbeat_thread.is_alive()
            )
            now = time.monotonic()
            if (
                not process_alive
                and not thread_alive
                and now - last_inline_heartbeat >= 20.0
            ):
                latest = self.heartbeat()
                last_inline_heartbeat = now
            else:
                latest = self.status()
            if self.solve_permit_granted:
                return latest
            if self.state != "active":
                raise AedtLeaseError(
                    f"AEDT lease {self.lease_id} became {self.state} while "
                    "waiting for solve permit"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                latest = self._call_with_retry(
                    "POST", "/solve-permit", {"seal_underfilled": True}
                )
                self._apply_status(latest)
                if self.solve_permit_granted:
                    return latest
                raise AedtLeaseError(
                    "AEDT solve permit was refused because a sibling attach "
                    "is still incomplete; refusing to start native analyze"
                )
            time.sleep(min(max(0.1, float(poll_seconds)), remaining))

    def release(self, *, wait_seconds: int = 300) -> dict[str, Any]:
        status = self._call_with_retry(
            "POST", "/cancel", {"reason": "client released lease"}
        )
        deadline = time.monotonic() + max(0, int(wait_seconds))
        while status.get("state") == "releasing" and time.monotonic() < deadline:
            time.sleep(2)
            status = self.status()
        self.state = str(status.get("state") or "")
        if self.state in {"released", "failed", "cancelled", "expired"}:
            self._detach_wrapper_best_effort()
            self.stop_heartbeat()
            self.stop_process_keepalive()
        return status

    def cancel(
        self,
        *,
        reason: str = "client cancelled lease",
        wait_seconds: int = 300,
    ) -> dict[str, Any]:
        status = self._call_with_retry("POST", "/cancel", {"reason": reason})
        deadline = time.monotonic() + max(0, int(wait_seconds))
        while status.get("state") == "releasing" and time.monotonic() < deadline:
            time.sleep(2)
            status = self.status()
        self.state = str(status.get("state") or "")
        if self.state in {"released", "failed", "cancelled", "expired"}:
            self._detach_wrapper_best_effort()
            self.stop_heartbeat()
            self.stop_process_keepalive()
        return status

    def bind_project_name(self, project_name: str) -> dict[str, Any]:
        status = self._call_with_retry(
            "PATCH",
            "/project-name",
            {"project_name": project_name},
        )
        self.project_name = project_name
        return status

    def report_fault(
        self,
        fault_kind: str,
        *,
        phase: str = "",
        evidence: dict[str, Any] | None = None,
        failure_message: str = "",
        sibling_grace_seconds: int = 900,
    ) -> dict[str, Any]:
        status = self._call_with_retry(
            "POST",
            "/fault",
            {
                "fault_kind": fault_kind,
                "phase": phase,
                "evidence": evidence or {},
                "failure_message": failure_message,
                "sibling_grace_seconds": sibling_grace_seconds,
            },
        )
        self.state = str(status.get("state") or "")
        return status


def acquire_project_lease(
    scheduler_url: str,
    project_name: str,
    *,
    bootstrap_token: str = "",
    bootstrap_token_file: str = "",
    request_key: str = "",
    task_id: int = 0,
    allocation_id: int = 0,
    node_name: str = "",
    exclusive_session: bool = False,
    workload_family: str = "",
    session_profile: Any = "",
    project_namespace: str = "",
    isolation_policy: str = "family",
    workspace_path: str = "",
    protocol_version: int = 2,
    heartbeat_seconds: int = 20,
    admission_timeout_seconds: int = 1800,
) -> AedtProjectLease:
    """Create a lease request; call `wait_until_leased` before opening a project."""
    if type(exclusive_session) is not bool:
        raise ValueError("exclusive_session must be a boolean")
    http = AedtPoolHttpClient(
        scheduler_url,
        bootstrap_token=bootstrap_token,
        bootstrap_token_file=bootstrap_token_file,
    )
    key = request_key.strip() or (
        f"{project_name}:{task_id or os.getpid()}:{uuid.uuid4().hex}"
    )
    client_token = secrets.token_urlsafe(32)
    request_create = getattr(http, "request_with_retry", http.request)
    payload = request_create(
        "POST",
        "/api/aedt-pool/leases",
        {
            "request_key": key,
            "project_name": project_name,
            "task_id": max(0, int(task_id)),
            "allocation_id": max(0, int(allocation_id)),
            "node_name": node_name,
            "exclusive_session": exclusive_session,
            "workload_family": workload_family,
            "session_profile": session_profile,
            "project_namespace": project_namespace,
            "isolation_policy": isolation_policy,
            "workspace_path": workspace_path,
            "protocol_version": protocol_version,
            "client_token": client_token,
            "admission_timeout_seconds": admission_timeout_seconds,
        },
    )
    lease = payload["lease"]
    project_lease = AedtProjectLease(
        http=http,
        lease_id=int(lease["id"]),
        client_token=str(payload.get("client_token") or client_token),
        project_name=project_name,
        state=str(lease.get("state") or "queued"),
        endpoint=str(lease.get("endpoint") or ""),
        exclusive_session=bool(lease.get("exclusive_session")),
        protocol_version=int(lease.get("protocol_version") or protocol_version),
        workload_family=str(lease.get("workload_family") or workload_family),
        session_profile=str(lease.get("session_profile") or ""),
        project_namespace=str(lease.get("project_namespace") or project_namespace),
        isolation_policy=str(lease.get("isolation_policy") or isolation_policy),
        workspace_path=str(lease.get("workspace_path") or workspace_path),
        automation_lock_path=str(lease.get("automation_lock_path") or ""),
        session_key=str(lease.get("session_key") or ""),
        session_process_id=str(lease.get("session_process_id") or ""),
        expected_aedt_version=str(lease.get("expected_aedt_version") or ""),
    )
    if hasattr(http, "scheduler_url") and hasattr(http, "bootstrap_token"):
        project_lease.start_process_keepalive(heartbeat_seconds=heartbeat_seconds)
    return project_lease
