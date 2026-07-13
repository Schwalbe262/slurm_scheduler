from __future__ import annotations

import threading
import time
import urllib.error

import pytest

from scripts.aedt_pool_1to1_pilot import start_control_plane
from scripts.aedt_pool_1to2_pilot import (
    SharedPilotControlPlane,
    _valid_matrix_result,
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
    port = 50052
    aedt_process_id = 987654322

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


def _wait(predicate, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition did not become true")


def test_shared_loopback_closes_aborted_project_without_stopping_sibling(monkeypatch):
    state = SharedPilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop()
    host = AedtSessionHost(
        ControlPlaneClient(scheduler_url, bootstrap_token=state.bootstrap_token),
        allocation_id=1,
        node_name="node-test",
        heartbeat_seconds=5,
    )
    host.heartbeat_seconds = 0.05
    monkeypatch.setattr(host, "_start_desktop", lambda: desktop)
    bounded_close_calls = []

    def close_desktop(*, global_stop, timeout_seconds=30):
        bounded_close_calls.append(global_stop)
        host.desktop = None
        return True

    monkeypatch.setattr(host, "_bounded_close_desktop", close_desktop)
    host_result: list[int] = []
    host_thread = threading.Thread(target=lambda: host_result.append(host.run()))
    host_thread.start()
    try:
        _wait(lambda: bool(state.session["endpoint"]))
        leases = []
        for label in ("A", "B"):
            lease = acquire_project_lease(
                scheduler_url,
                f"pending-{label}",
                request_key=f"unit-shared-{label}",
                exclusive_session=False,
            )
            lease.wait_until_leased(timeout_seconds=3, heartbeat_seconds=5)
            lease.bind_project_name(f"simulation_{label}")
            desktop.projects.append(f"simulation_{label}")
            leases.append(lease)

        leases[0].report_fault(
            "pre_solve",
            failure_message="injected client abort before solve",
        )
        _wait(lambda: state.project_close_acks.get(1) is True)

        assert desktop.closed == ["simulation_A"]
        assert desktop.projects == ["simulation_B"]
        assert state.leases[1]["state"] == "released"
        assert state.leases[2]["state"] == "active"
        assert host_thread.is_alive()
        assert bounded_close_calls == []

        released_b = leases[1].release(wait_seconds=5)
        host_thread.join(timeout=3)

        assert released_b["state"] == "released"
        assert host_result == [0]
        assert desktop.closed == ["simulation_A", "simulation_B"]
        assert state.project_close_acks == {1: True, 2: True}
        assert state.closed_ack is True
        assert bounded_close_calls == [False]
    finally:
        for lease in locals().get("leases", []):
            lease.stop_heartbeat()
        if host_thread.is_alive():
            state.force_drain("unit test cleanup")
            host_thread.join(timeout=3)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)


def test_shared_loopback_rejects_exclusive_or_third_lease():
    state = SharedPilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    try:
        http = AedtPoolHttpClient(scheduler_url)
        with pytest.raises(urllib.error.HTTPError) as exclusive:
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {"project_name": "unsafe", "exclusive_session": True},
            )
        assert exclusive.value.code == 422
        for label in ("A", "B"):
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {"project_name": label, "exclusive_session": False},
            )
        with pytest.raises(urllib.error.HTTPError) as third:
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {"project_name": "C", "exclusive_session": False},
            )
        assert third.value.code == 409
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)


def test_terminal_validator_rejects_prior_pid_grpc_false_positive():
    invalid = {
        "result_valid_em": 0,
        "aedt_backend": "pooled",
        "aedt_exclusive_session": 0,
        "matrix_solve_attempts": 1,
        "matrix_solution_queries": 0,
        "Llt_phys": None,
        "project_name": "simulation_B",
        "sibling_pid_survived": True,
        "grpc_survived": True,
    }
    failures = _valid_matrix_result(invalid)
    assert failures
    assert any(item.startswith("result_valid_em") for item in failures)
    assert "matrix_solution_queries<1" in failures
    assert "Llt_phys_missing" in failures


def test_terminal_validator_accepts_complete_matrix_result():
    assert _valid_matrix_result({
        "result_valid_em": 1,
        "aedt_backend": "pooled",
        "aedt_exclusive_session": 0,
        "matrix_solve_attempts": 1,
        "matrix_solution_queries": 1,
        "Llt_phys": 27.5,
        "project_name": "simulation_B",
    }) == []
