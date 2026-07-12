from __future__ import annotations

import threading
import time
import urllib.error

import pytest

from scripts.aedt_pool_1to1_pilot import (
    PilotControlPlane,
    _feature_local_pids,
    parse_result_json,
    start_control_plane,
)
from slurm_scheduler.aedt_attach_client import (
    AedtPoolHttpClient,
    acquire_project_lease,
)
from slurm_scheduler.aedt_session_host import (
    AedtSessionHost,
    ControlPlaneClient,
)


class FakeDesktop:
    port = 50051
    aedt_process_id = 987654321

    def __init__(self) -> None:
        self.projects: list[str] = []
        self.closed: list[str] = []
        self.odesktop = self

    def GetProjectList(self):
        return list(self.projects)

    def close_project(self, project_name, save_project=False):
        assert save_project is False
        self.closed.append(project_name)
        if project_name in self.projects:
            self.projects.remove(project_name)


def test_loopback_pilot_performs_exclusive_attach_and_close_ack(monkeypatch):
    state = PilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop()
    host = AedtSessionHost(
        ControlPlaneClient(
            scheduler_url,
            bootstrap_token=state.bootstrap_token,
        ),
        allocation_id=1,
        node_name="node-test",
        heartbeat_seconds=5,
    )
    host.heartbeat_seconds = 0.05
    monkeypatch.setattr(host, "_start_desktop", lambda: desktop)

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
            exclusive_session=True,
        )
        lease.wait_until_leased(timeout_seconds=3, heartbeat_seconds=5)
        attach_calls = []
        attached = lease.connect_desktop(
            desktop_factory=lambda **kwargs: attach_calls.append(kwargs) or object()
        )
        assert attached is not None
        assert attach_calls == [{
            "new_desktop": False,
            "non_graphical": True,
            "close_on_exit": False,
            "machine": state.session["endpoint"].rsplit(":", 1)[0],
            "port": 50051,
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


def test_loopback_pilot_rejects_nonexclusive_lease():
    state = PilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    try:
        http = AedtPoolHttpClient(scheduler_url)
        with pytest.raises(urllib.error.HTTPError) as error:
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {"project_name": "unsafe", "exclusive_session": False},
            )
        assert error.value.code == 422
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)


def test_license_parser_matches_desktop_pid_on_node_alias():
    lmstat = """
Users of electronics_desktop:  (Total of 550 licenses issued;  Total of 2 licenses in use)
    user1 nib040.hpc n040.hpc 1278977 (v2025.0506) (server/1055 1), start Mon 7/13 3:35
    user1 nib040.hpc n040.hpc 1278999 (v2025.0506) (server/1055 2), start Mon 7/13 3:35
Users of another_feature:  (Total of 1 license issued;  Total of 0 licenses in use)
"""
    assert _feature_local_pids(
        lmstat, "electronics_desktop", "user1", "n040.hpc"
    ) == {1278977, 1278999}


def test_parse_result_json_requires_one_terminal_record():
    assert parse_result_json('noise\nRESULT_JSON {"result_valid_em": 1}\n') == {
        "result_valid_em": 1
    }
    with pytest.raises(RuntimeError, match="found 0"):
        parse_result_json("noise only")
