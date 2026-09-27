from __future__ import annotations

import os
import sys
import threading
import time
import types
import urllib.error

import pytest

from scripts.aedt_pool_loopback import (
    ExclusiveControlPlane,
    start_control_plane,
)
from slurm_scheduler.aedt_attach_client import (
    AedtProjectLease,
    AedtPoolHttpClient,
    acquire_project_lease,
)
from slurm_scheduler.aedt_session_host import (
    AedtSessionHost,
    ControlPlaneClient,
    EXPECTED_AEDT_VERSION,
    EXPECTED_PYAEDT_VERSION,
    EXPECTED_SESSION_PROFILE,
    EXPECTED_SESSION_PROFILE_JSON,
)


class FakeDesktop:
    def __init__(self, port=50051) -> None:
        self.port = port
        self.aedt_process_id = os.getpid()
        self.projects: list[str] = []
        self.closed: list[str] = []
        self.odesktop = self

    def GetVersion(self):
        return "Ansys Electronics Desktop 2025.2.0"

    def GetProjectList(self):
        return list(self.projects)

    def close_project(self, project_name, save_project=False):
        assert save_project is False
        self.closed.append(project_name)
        if project_name in self.projects:
            self.projects.remove(project_name)


@pytest.fixture(autouse=True)
def _thread_only_keepalive(monkeypatch):
    """Loopback tests need lease heartbeats without spawning a console."""
    monkeypatch.setattr(
        AedtProjectLease,
        "start_process_keepalive",
        lambda lease, heartbeat_seconds=20: lease.start_heartbeat(
            heartbeat_seconds=heartbeat_seconds
        ),
    )


def test_loopback_pilot_performs_exclusive_attach_and_close_ack(
    monkeypatch, tmp_path
):
    state = ExclusiveControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop(port=int(server.server_address[1]))
    ansys_module = types.ModuleType("ansys")
    ansys_module.__path__ = []
    aedt_module = types.ModuleType("ansys.aedt")
    aedt_module.__path__ = []
    pyaedt_core = types.ModuleType("ansys.aedt.core")
    pyaedt_core.__version__ = EXPECTED_PYAEDT_VERSION
    ansys_module.aedt = aedt_module
    aedt_module.core = pyaedt_core
    monkeypatch.setitem(sys.modules, "ansys", ansys_module)
    monkeypatch.setitem(sys.modules, "ansys.aedt", aedt_module)
    monkeypatch.setitem(sys.modules, "ansys.aedt.core", pyaedt_core)
    monkeypatch.setenv(
        "CONDA_DEFAULT_ENV",
        str(EXPECTED_SESSION_PROFILE["python_environment"]),
    )
    workspace = tmp_path / "aedt-task-1"
    workspace.mkdir()
    host = AedtSessionHost(
        ControlPlaneClient(
            scheduler_url,
            bootstrap_token=state.bootstrap_token,
        ),
        allocation_id=1,
        node_name="node-test",
        heartbeat_seconds=5,
        aedt_version=EXPECTED_AEDT_VERSION,
        artifact_root=str(tmp_path / "host-artifacts"),
        session_profile=EXPECTED_SESSION_PROFILE_JSON,
    )
    host.heartbeat_seconds = 0.05
    monkeypatch.setattr(host, "_start_desktop", lambda: desktop)
    monkeypatch.setattr(
        host,
        "_desktop_process_listener_liveness_proof",
        lambda: (True, ""),
    )

    def close_desktop(*, global_stop, timeout_seconds=30):
        assert global_stop is False
        host.desktop = None
        return True

    monkeypatch.setattr(host, "_bounded_close_desktop", close_desktop)
    result: list[int] = []
    host_thread = threading.Thread(target=lambda: result.append(host.run()))
    host_thread.start()
    try:
        deadline = time.monotonic() + 3
        while not state.session["endpoint"] and time.monotonic() < deadline:
            time.sleep(0.01)
        assert state.session["endpoint"]

        lease = acquire_project_lease(
            scheduler_url,
            "pending",
            request_key="unit-exclusive",
            task_id=1,
            exclusive_session=True,
            workload_family="unit-exclusive",
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            isolation_policy="exclusive",
            workspace_path=str(workspace),
            protocol_version=2,
        )
        lease.wait_until_leased(timeout_seconds=3, heartbeat_seconds=5)
        attach_calls = []
        attached = lease.connect_desktop(
            desktop_factory=lambda **kwargs: attach_calls.append(kwargs) or desktop,
            endpoint_probe=lambda _machine, port: port == desktop.port,
        )
        assert attached is not None
        assert attach_calls == [{
            "new_desktop": False,
            "non_graphical": True,
            "close_on_exit": False,
            "machine": state.session["endpoint"].rsplit(":", 1)[0],
            "port": desktop.port,
            "version": EXPECTED_AEDT_VERSION,
        }]

        lease.bind_project_name("simulation_pilot")
        desktop.projects.append("simulation_pilot")
        released = lease.release(wait_seconds=5)
        host_thread.join(timeout=3)

        assert released["state"] == "released"
        assert result == [0]
        assert desktop.closed == ["simulation_pilot"]
        assert state.project_close_ack is True
        assert state.closed_ack is True
        assert state.session["state"] == "closed"
        assert state.lease["exclusive_session"] == 1
    finally:
        if host_thread.is_alive():
            state.force_drain("unit test cleanup")
            host_thread.join(timeout=3)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)


def test_loopback_pilot_rejects_nonexclusive_lease(tmp_path):
    state = ExclusiveControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    workspace = tmp_path / "aedt-task-2"
    workspace.mkdir()
    try:
        http = AedtPoolHttpClient(scheduler_url)
        with pytest.raises(urllib.error.HTTPError) as error:
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {
                    "project_name": "unsafe",
                    "task_id": 2,
                    "exclusive_session": False,
                    "protocol_version": 2,
                    "workload_family": "unit-exclusive",
                    "session_profile": EXPECTED_SESSION_PROFILE_JSON,
                    "isolation_policy": "exclusive",
                    "workspace_path": str(workspace),
                },
            )
        assert error.value.code == 422
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)
