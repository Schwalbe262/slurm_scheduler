from __future__ import annotations

import http.client
import json
import math
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS = 360.0
CONTROL_PLANE_OUTAGE_ENV = "AEDT_POOL_CONTROL_PLANE_OUTAGE_SECONDS"
TRANSIENT_HTTP_STATUSES = {408, 425, 429}


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
        bootstrap_token: str = "",
        bootstrap_token_file: str = "",
    ) -> None:
        self.scheduler_url = scheduler_url.rstrip("/")
        self.bootstrap_token = (
            str(bootstrap_token or "").strip()
            or os.environ.get("SLURM_AEDT_POOL_BOOTSTRAP_TOKEN", "").strip()
        )
        token_file = (
            str(bootstrap_token_file or "").strip()
            or os.environ.get("SLURM_AEDT_POOL_BOOTSTRAP_TOKEN_FILE", "").strip()
        )
        if not self.bootstrap_token and token_file:
            self.bootstrap_token = Path(token_file).read_text(encoding="utf-8").strip()

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
        if self.bootstrap_token and method.upper() not in {"GET", "HEAD", "OPTIONS"}:
            headers["X-AEDT-Bootstrap-Token"] = self.bootstrap_token
        if lease_token:
            headers["X-AEDT-Lease-Token"] = lease_token
        request = urllib.request.Request(
            f"{self.scheduler_url}{path}", data=data, headers=headers, method=method
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8") or "{}")


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
    control_plane_outage_seconds: float = field(
        default_factory=_control_plane_outage_seconds_from_env
    )
    _heartbeat_stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _heartbeat_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    heartbeat_error: str = field(default="", init=False)

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

    def status(self) -> dict[str, Any]:
        status = self._call("GET", "")
        self.state = str(status.get("state") or "")
        self.endpoint = str(status.get("endpoint") or "")
        return status

    def heartbeat(self) -> dict[str, Any]:
        outage_started_at = time.monotonic()
        retry_index = 0
        while True:
            try:
                status = self._call("POST", "/heartbeat", {})
                break
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
                    raise AedtLeaseError("lease heartbeat retry stopped") from last_error
            else:
                time.sleep(delay)
            retry_index += 1
        self.state = str(status.get("state") or "")
        self.endpoint = str(status.get("endpoint") or "")
        self.heartbeat_error = ""
        return status

    def wait_until_leased(
        self,
        *,
        timeout_seconds: int = 1800,
        heartbeat_seconds: int = 60,
    ) -> dict[str, Any]:
        """Heartbeat while queued; node waits must not expire a valid request."""
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        interval = max(5, int(heartbeat_seconds))
        while time.monotonic() < deadline:
            status = self.heartbeat()
            if self.state in {"leased", "active"}:
                if not self.endpoint:
                    raise AedtLeaseError("lease has no session-host endpoint")
                self.start_heartbeat(heartbeat_seconds=interval)
                return status
            if self.state not in {"queued"}:
                raise AedtLeaseError(
                    f"AEDT lease {self.lease_id} became {self.state}: "
                    f"{status.get('failure_message') or ''}"
                )
            time.sleep(interval)
        raise TimeoutError(f"timed out waiting for AEDT lease {self.lease_id}")

    def start_heartbeat(self, *, heartbeat_seconds: int = 60) -> None:
        """Keep ownership alive throughout a long solver call."""
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()
        interval = max(5, int(heartbeat_seconds))

        def run() -> None:
            while not self._heartbeat_stop.wait(interval):
                try:
                    self.heartbeat()
                except Exception as exc:
                    self.heartbeat_error = str(exc)
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
    ) -> Any:
        """Attach PyAEDT to the leased host without taking shutdown ownership.

        Do not use the returned object as a context manager and do not call
        release_desktop(close_desktop=True).  `release()` asks the node-side
        owner to close only this project.
        """
        if self.state not in {"leased", "active"} or not self.endpoint:
            self.wait_until_leased()
        if desktop_factory is None:
            try:
                from ansys.aedt.core import Desktop as desktop_factory
            except ImportError as exc:
                raise AedtLeaseError("PyAEDT is required by the project client") from exc
        machine, port_text = self.endpoint.rsplit(":", 1)
        kwargs: dict[str, Any] = {
            "new_desktop": False,
            "non_graphical": non_graphical,
            "close_on_exit": False,
            "machine": machine,
            "port": int(port_text),
        }
        if version:
            kwargs["version"] = version
        desktop = desktop_factory(**kwargs)
        self.heartbeat()
        self.start_heartbeat()
        return desktop

    def release(self, *, wait_seconds: int = 300) -> dict[str, Any]:
        self.stop_heartbeat()
        status = self._call("POST", "/release", {})
        deadline = time.monotonic() + max(0, int(wait_seconds))
        while status.get("state") == "releasing" and time.monotonic() < deadline:
            time.sleep(2)
            status = self.status()
        self.state = str(status.get("state") or "")
        return status

    def bind_project_name(self, project_name: str) -> dict[str, Any]:
        status = self._call(
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
        failure_message: str = "",
        sibling_grace_seconds: int = 900,
    ) -> dict[str, Any]:
        status = self._call(
            "POST",
            "/fault",
            {
                "fault_kind": fault_kind,
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
    payload = http.request(
        "POST",
        "/api/aedt-pool/leases",
        {
            "request_key": key,
            "project_name": project_name,
            "task_id": max(0, int(task_id)),
            "allocation_id": max(0, int(allocation_id)),
            "node_name": node_name,
            "exclusive_session": exclusive_session,
        },
    )
    lease = payload["lease"]
    return AedtProjectLease(
        http=http,
        lease_id=int(lease["id"]),
        client_token=str(payload["client_token"]),
        project_name=project_name,
        state=str(lease.get("state") or "queued"),
        endpoint=str(lease.get("endpoint") or ""),
        exclusive_session=bool(lease.get("exclusive_session")),
    )
