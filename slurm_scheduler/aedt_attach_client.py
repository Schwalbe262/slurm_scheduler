from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any


class AedtLeaseError(RuntimeError):
    pass


class AedtPoolHttpClient:
    def __init__(self, scheduler_url: str) -> None:
        self.scheduler_url = scheduler_url.rstrip("/")

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
        request = urllib.request.Request(
            f"{self.scheduler_url}{path}", data=data, headers=headers, method=method
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
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
    _heartbeat_stop: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _heartbeat_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    heartbeat_error: str = field(default="", init=False)

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
        status = self._call("POST", "/heartbeat", {})
        self.state = str(status.get("state") or "")
        self.endpoint = str(status.get("endpoint") or "")
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
    request_key: str = "",
    task_id: int = 0,
    allocation_id: int = 0,
    node_name: str = "",
) -> AedtProjectLease:
    """Create a lease request; call `wait_until_leased` before opening a project."""
    http = AedtPoolHttpClient(scheduler_url)
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
    )
