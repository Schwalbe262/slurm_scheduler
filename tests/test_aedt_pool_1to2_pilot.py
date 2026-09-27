from __future__ import annotations

import sys
import threading
import time
import urllib.error
from types import ModuleType

import pytest

from scripts.aedt_pool_1to1_pilot import start_control_plane
from scripts.aedt_pool_1to2_pilot import (
    SharedPilotControlPlane,
    _run_host,
    _valid_matrix_result,
    _validate_project_evidence,
)
from scripts.aedt_pool_1to2_runner import acquire_pinned_mft_lease
from slurm_scheduler.aedt_attach_client import (
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
    SUPPORTED_DSO_PROFILE,
)


class FakeDesktop:
    port = 50052
    aedt_process_id = 987654322

    def __init__(self) -> None:
        self.projects: list[str] = []
        self.closed: list[str] = []
        self.odesktop = self

    def GetVersion(self):
        return "2025.2"

    def GetProjectList(self):
        return list(self.projects)

    def close_project(self, project_name, save_project=False):
        assert save_project is False
        self.closed.append(project_name)
        if project_name in self.projects:
            self.projects.remove(project_name)


def _v2_lease_fields(tmp_path, task_id):
    workspace = tmp_path / f"lease-{task_id}"
    workspace.mkdir()
    return {
        "protocol_version": 2,
        "task_id": task_id,
        "workload_family": "unit-shared-pilot",
        "session_profile": EXPECTED_SESSION_PROFILE_JSON,
        "isolation_policy": "family",
        "workspace_path": str(workspace),
    }


def _install_attested_runtime(monkeypatch):
    ansys = ModuleType("ansys")
    aedt = ModuleType("ansys.aedt")
    core = ModuleType("ansys.aedt.core")
    core.__version__ = EXPECTED_PYAEDT_VERSION
    ansys.aedt = aedt
    aedt.core = core
    monkeypatch.setitem(sys.modules, "ansys", ansys)
    monkeypatch.setitem(sys.modules, "ansys.aedt", aedt)
    monkeypatch.setitem(sys.modules, "ansys.aedt.core", core)
    monkeypatch.setenv(
        "CONDA_DEFAULT_ENV",
        EXPECTED_SESSION_PROFILE["python_environment"],
    )


def _activate_without_solve_wait(lease):
    status = lease._call_with_retry("POST", "/activate", {})
    lease._apply_status(status)


def _wait(predicate, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition did not become true")


def test_run_host_and_pinned_runner_share_v2_http_contract(monkeypatch, tmp_path):
    _install_attested_runtime(monkeypatch)
    state = SharedPilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop()
    initialized_dso_profiles = []
    original_host_init = AedtSessionHost.__init__

    def initialize_fast_host(host, *args, **kwargs):
        original_host_init(host, *args, **kwargs)
        host.heartbeat_seconds = 0.05

    monkeypatch.setattr(AedtSessionHost, "__init__", initialize_fast_host)
    monkeypatch.setattr(AedtSessionHost, "_start_desktop", lambda _host: desktop)
    monkeypatch.setattr(
        AedtSessionHost,
        "_initialize_dso_configuration",
        lambda host: initialized_dso_profiles.append(host.dso_profile),
    )
    monkeypatch.setattr(
        AedtSessionHost,
        "_desktop_process_listener_liveness_proof",
        lambda _host: (True, ""),
    )

    def close_desktop(host, *, global_stop, timeout_seconds=30):
        host.desktop = None
        return True

    monkeypatch.setattr(AedtSessionHost, "_bounded_close_desktop", close_desktop)
    artifact_root = tmp_path / "host-artifacts"
    host = None
    host_thread = None
    runner_lease = None
    try:
        host, host_thread, host_result = _run_host(
            state,
            scheduler_url,
            artifact_root,
        )

        assert state.session["endpoint"]
        assert host.aedt_version == EXPECTED_AEDT_VERSION
        assert host.artifact_root == str(artifact_root.resolve())
        assert host.dso_profile == SUPPORTED_DSO_PROFILE
        assert host.session_profile == EXPECTED_SESSION_PROFILE_JSON
        assert initialized_dso_profiles == [SUPPORTED_DSO_PROFILE]
        assert host.automation_lock_path
        assert host.runtime_metadata["automation_lock_path"] == (
            host.automation_lock_path
        )
        assert host.runtime_metadata["session_profile"] == (
            EXPECTED_SESSION_PROFILE_JSON
        )
        assert state.session["artifact_dir"] == host.artifact_dir
        assert state.session["runtime_metadata"] == host.runtime_metadata

        _wait(lambda: state.host_heartbeat_count > 0)
        commands = host.client.request(
            "GET",
            "/api/aedt-pool/sessions/1/commands",
            host_token=host.host_token,
        )
        assert commands["sibling_live_count"] == 0
        assert server_thread.is_alive()

        runner_task_id = 304781
        runner_workspace = tmp_path / f"aedt-normal-{runner_task_id}-a"
        runner_workspace.mkdir()
        monkeypatch.setenv("MFT_AEDT_BACKEND", "pooled")
        monkeypatch.setenv("MFT_AEDT_SHARED_1TO2_PILOT", "1")
        monkeypatch.setenv("MFT_AEDT_SCHEDULER_URL", scheduler_url)
        monkeypatch.setenv("MFT_AEDT_PILOT_CLIENT_LABEL", "A")
        monkeypatch.setenv(
            "MFT_AEDT_PILOT_WORKSPACE", str(runner_workspace.resolve())
        )
        monkeypatch.setenv("SLURM_SCHED_TASK_ID", str(runner_task_id))
        with pytest.raises(urllib.error.HTTPError) as unbridged:
            acquire_project_lease(
                scheduler_url,
                f"mft-pending-{runner_task_id}-unbridged",
                request_key=f"mft-1to2:{runner_task_id}:unbridged",
                task_id=runner_task_id,
                exclusive_session=False,
            )
        assert unbridged.value.code == 422

        # Match the pinned fd3b02c MFT adapter: it passes no v2 profile,
        # workload, isolation, or workspace keyword arguments.
        runner_lease = acquire_pinned_mft_lease(
            acquire_project_lease,
            scheduler_url,
            f"mft-pending-{runner_task_id}-unit",
            request_key=f"mft-1to2:{runner_task_id}:unit",
            task_id=runner_task_id,
            exclusive_session=False,
        )
        assert runner_lease.protocol_version == 2
        assert runner_lease.workload_family == "mft"
        assert runner_lease.session_profile == EXPECTED_SESSION_PROFILE_JSON
        assert runner_lease.workspace_path == str(runner_workspace.resolve())
        assert runner_lease.project_namespace == "mft-1to2-a"
        assert server_thread.is_alive()
    finally:
        if runner_lease is not None:
            runner_lease.stop_heartbeat()
        if host_thread is not None and host_thread.is_alive():
            monkeypatch.setattr(state, "_all_projects_closed", lambda: True)
            state.force_drain("unit test cleanup")
            host_thread.join(timeout=3)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)

    assert host_result == [0]


def test_shared_loopback_closes_aborted_project_without_stopping_sibling(
    monkeypatch, tmp_path
):
    _install_attested_runtime(monkeypatch)
    state = SharedPilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop()
    host = AedtSessionHost(
        ControlPlaneClient(scheduler_url, bootstrap_token=state.bootstrap_token),
        allocation_id=1,
        node_name="node-test",
        heartbeat_seconds=5,
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
        for task_id, label in enumerate(("A", "B"), start=1):
            lease = acquire_project_lease(
                scheduler_url,
                f"pending-{label}",
                request_key=f"unit-shared-{label}",
                exclusive_session=False,
                **_v2_lease_fields(tmp_path, task_id),
            )
            lease.wait_until_leased(timeout_seconds=3, heartbeat_seconds=5)
            lease.bind_project_name(f"simulation_{label}")
            desktop.projects.append(f"simulation_{label}")
            leases.append(lease)

        for lease in leases:
            _activate_without_solve_wait(lease)

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


def test_shared_loopback_timeout_quarantines_then_recycles_after_sibling(
    monkeypatch, tmp_path
):
    _install_attested_runtime(monkeypatch)
    state = SharedPilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop()
    host = AedtSessionHost(
        ControlPlaneClient(scheduler_url, bootstrap_token=state.bootstrap_token),
        allocation_id=1,
        node_name="node-test",
        heartbeat_seconds=5,
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
    bounded_close_calls = []

    def close_desktop(*, global_stop, timeout_seconds=30):
        bounded_close_calls.append(global_stop)
        host.desktop = None
        return True

    monkeypatch.setattr(host, "_bounded_close_desktop", close_desktop)
    host_result: list[int] = []
    host_thread = threading.Thread(target=lambda: host_result.append(host.run()))
    host_thread.start()
    leases = []
    try:
        _wait(lambda: bool(state.session["endpoint"]))
        for task_id, label in enumerate(("A", "B"), start=1):
            lease = acquire_project_lease(
                scheduler_url,
                f"pending-{label}",
                request_key=f"unit-timeout-{label}",
                exclusive_session=False,
                **_v2_lease_fields(tmp_path, task_id),
            )
            lease.wait_until_leased(timeout_seconds=3, heartbeat_seconds=5)
            lease.bind_project_name(f"simulation_{label}")
            desktop.projects.append(f"simulation_{label}")
            leases.append(lease)

        for lease in leases:
            _activate_without_solve_wait(lease)

        leases[0].report_fault(
            "solver_timeout",
            failure_message="injected active-solve timeout",
            sibling_grace_seconds=60,
        )
        _wait(lambda: state.quarantine_reason == "solver_timeout")

        assert state.leases[1]["state"] == "releasing"
        assert state.leases[2]["state"] == "active"
        assert desktop.closed == []
        assert host_thread.is_alive()
        assert bounded_close_calls == []

        with pytest.raises(urllib.error.HTTPError) as third:
            AedtPoolHttpClient(scheduler_url).request(
                "POST",
                "/api/aedt-pool/leases",
                {
                    "request_key": "unit-timeout-C",
                    "project_name": "simulation_C",
                    "exclusive_session": False,
                    **_v2_lease_fields(tmp_path, 3),
                },
            )
        assert third.value.code == 409

        released_b = leases[1].release(wait_seconds=5)
        host_thread.join(timeout=3)

        assert released_b["state"] == "released"
        assert host_result == [2]
        assert desktop.closed == ["simulation_B"]
        assert state.project_close_acks == {2: True}
        assert state.leases[1]["state"] == "queued"
        assert state.requeued_lease_ids == [1]
        assert state.closed_ack is True
        assert bounded_close_calls == [True]
    finally:
        for lease in leases:
            lease.stop_heartbeat()
        if host_thread.is_alive():
            state.force_drain("unit test cleanup")
            host_thread.join(timeout=3)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)


def test_shared_loopback_rejects_exclusive_or_third_lease(tmp_path):
    state = SharedPilotControlPlane()
    server, server_thread, scheduler_url = start_control_plane(state)
    try:
        http = AedtPoolHttpClient(scheduler_url)
        with pytest.raises(urllib.error.HTTPError) as exclusive:
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {
                    "request_key": "unit-exclusive",
                    "project_name": "unsafe",
                    "exclusive_session": True,
                    **_v2_lease_fields(tmp_path, 1),
                },
            )
        assert exclusive.value.code == 422
        for task_id, label in enumerate(("A", "B"), start=2):
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {
                    "request_key": f"unit-shared-{label}",
                    "project_name": label,
                    "exclusive_session": False,
                    **_v2_lease_fields(tmp_path, task_id),
                },
            )
        with pytest.raises(urllib.error.HTTPError) as third:
            http.request(
                "POST",
                "/api/aedt-pool/leases",
                {
                    "request_key": "unit-shared-C",
                    "project_name": "C",
                    "exclusive_session": False,
                    **_v2_lease_fields(tmp_path, 4),
                },
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
        "Llt": None,
        "project_name": "simulation_B",
        "sibling_pid_survived": True,
        "grpc_survived": True,
    }
    failures = _valid_matrix_result(invalid)
    assert failures
    assert any(item.startswith("result_valid_em") for item in failures)
    assert "matrix_solution_queries<1" in failures
    assert "Llt_missing" in failures


def test_terminal_validator_accepts_complete_matrix_result():
    assert _valid_matrix_result({
        "result_valid_em": 1,
        "aedt_backend": "pooled",
        "aedt_exclusive_session": 0,
        "matrix_solve_attempts": 1,
        "matrix_solution_queries": 1,
        "Llt": 27.5,
        "project_name": "simulation_B",
    }) == []


def test_n_project_evidence_rejects_replayed_or_misattributed_result():
    leases = {
        lease_id: {"project_name": f"simulation_{lease_id}"}
        for lease_id in range(1, 5)
    }
    results = {
        str(lease_id): {
            "aedt_lease_id": lease_id,
            "project_name": f"simulation_{lease_id}",
            "result_valid_em": 1,
            "aedt_backend": "pooled",
            "aedt_exclusive_session": 0,
            "matrix_solve_attempts": 1,
            "matrix_solution_queries": 1,
            "Llt": 27.5 + lease_id,
        }
        for lease_id in leases
    }
    expected = {str(lease_id): lease_id for lease_id in leases}
    assert _validate_project_evidence(results, leases, expected) == []

    replayed = {label: dict(result) for label, result in results.items()}
    replayed["4"] = dict(replayed["3"])
    failures = _validate_project_evidence(replayed, leases, expected)
    assert "runner_4_lease_id_duplicate" in failures
    assert "runner_4_lease_id_mismatch" in failures
    assert "runner_4_project_name_duplicate" in failures

    swapped = {label: dict(result) for label, result in results.items()}
    swapped["3"], swapped["4"] = swapped["4"], swapped["3"]
    failures = _validate_project_evidence(swapped, leases, expected)
    assert "runner_3_lease_id_mismatch" in failures
    assert "runner_4_lease_id_mismatch" in failures

    misattributed = {label: dict(result) for label, result in results.items()}
    misattributed["4"]["project_name"] = "wrong_project"
    failures = _validate_project_evidence(misattributed, leases, expected)
    assert "runner_4_project_lease_mismatch" in failures

    missing = {label: result for label, result in results.items() if label != "4"}
    assert any(
        failure.startswith("terminal_result_labels=")
        for failure in _validate_project_evidence(missing, leases, expected)
    )


@pytest.mark.parametrize("break_cohort", [False, True])
def test_n_project_native_pipeline_barrier_is_exact(tmp_path, break_cohort):
    state = SharedPilotControlPlane(max_projects=4)
    state.session.update({
        "state": "ready",
        "endpoint": "127.0.0.1:50052",
        "process_id": "987654322",
        "artifact_dir": str(tmp_path),
    })
    headers = {}
    for lease_id in range(1, 5):
        status, created = state.dispatch(
            "POST",
            "/api/aedt-pool/leases",
            {
                "project_name": f"pending-{lease_id}",
                "exclusive_session": False,
                **_v2_lease_fields(tmp_path, lease_id),
            },
            {},
        )
        assert status == 200
        assert created["lease"]["id"] == lease_id
        headers[lease_id] = {"X-AEDT-Lease-Token": created["client_token"]}
        for method, suffix, payload in (
            ("POST", "/accept", {}),
            ("PATCH", "/project-name", {"project_name": f"simulation_{lease_id}"}),
            ("POST", "/activate", {}),
        ):
            status, _ = state.dispatch(
                method, f"/api/aedt-pool/leases/{lease_id}{suffix}",
                payload, headers[lease_id],
            )
            assert status == 200

    generations = {lease["solve_permit_generation"] for lease in state.leases.values()}
    assert len(generations) == 1
    generation = generations.pop()
    assert generation > 0
    for lease_id in range(1, 4):
        status, response = state.dispatch(
            "POST", f"/api/aedt-pool/leases/{lease_id}/native-pipeline-complete",
            {"solve_permit_generation": generation}, headers[lease_id],
        )
        assert status == 200
        assert response["native_pipeline_expected_count"] == 4
        assert response["native_pipeline_barrier_granted"] is False

    if break_cohort:
        status, _ = state.dispatch(
            "POST", "/api/aedt-pool/leases/4/fault",
            {"fault_kind": "script_error", "failure_message": "injected"},
            headers[4],
        )
        assert status == 200
        assert state._public_lease(1)["native_pipeline_barrier_broken"] is True
    else:
        status, response = state.dispatch(
            "POST", "/api/aedt-pool/leases/4/native-pipeline-complete",
            {"solve_permit_generation": generation}, headers[4],
        )
        assert status == 200
        assert response["native_pipeline_barrier_granted"] is True
        assert all(
            state._public_lease(lease_id)["native_pipeline_barrier_granted"]
            for lease_id in range(1, 5)
        )


@pytest.mark.parametrize("project_count", [3, 4])
@pytest.mark.parametrize("fault_kind", ["pre_solve", "solver_timeout"])
def test_shared_loopback_n_projects_isolates_one_fault(
    monkeypatch, tmp_path, project_count, fault_kind
):
    """A failed project must not close or invalidate any live sibling."""
    _install_attested_runtime(monkeypatch)
    state = SharedPilotControlPlane(max_projects=project_count)
    server, server_thread, scheduler_url = start_control_plane(state)
    desktop = FakeDesktop()
    host = AedtSessionHost(
        ControlPlaneClient(scheduler_url, bootstrap_token=state.bootstrap_token),
        allocation_id=1,
        node_name="node-test",
        heartbeat_seconds=5,
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
    bounded_close_calls = []

    def close_desktop(*, global_stop, timeout_seconds=30):
        bounded_close_calls.append(global_stop)
        host.desktop = None
        return True

    monkeypatch.setattr(host, "_bounded_close_desktop", close_desktop)
    host_result: list[int] = []
    host_thread = threading.Thread(target=lambda: host_result.append(host.run()))
    host_thread.start()
    leases = []
    try:
        _wait(lambda: bool(state.session["endpoint"]))
        for task_id in range(1, project_count + 1):
            label = chr(ord("A") + task_id - 1)
            lease = acquire_project_lease(
                scheduler_url,
                f"pending-{label}",
                request_key=f"unit-n-{fault_kind}-{label}",
                exclusive_session=False,
                **_v2_lease_fields(tmp_path, task_id),
            )
            lease.wait_until_leased(timeout_seconds=3, heartbeat_seconds=5)
            lease.bind_project_name(f"simulation_{label}")
            desktop.projects.append(f"simulation_{label}")
            leases.append(lease)

        for lease in leases:
            _activate_without_solve_wait(lease)

        assert state.session["slots_total"] == project_count
        assert {lease["slot_index"] for lease in state.leases.values()} == set(
            range(project_count)
        )
        assert all(
            lease["solve_permit_granted"] for lease in state.leases.values()
        )
        assert len({
            lease["solve_permit_generation"] for lease in state.leases.values()
        }) == 1

        leases[0].report_fault(
            fault_kind,
            failure_message=f"injected {fault_kind}",
            **({"sibling_grace_seconds": 60} if fault_kind == "solver_timeout" else {}),
        )
        _wait(lambda: state.leases[1]["state"] == "releasing"
              or state.leases[1]["state"] == "released")
        assert all(
            state.leases[lease_id]["state"] == "active"
            for lease_id in range(2, project_count + 1)
        )
        assert host_thread.is_alive()
        assert bounded_close_calls == []

        if fault_kind == "pre_solve":
            _wait(lambda: state.project_close_acks.get(1) is True)
            assert desktop.closed == ["simulation_A"]
            assert desktop.projects == [
                f"simulation_{chr(ord('A') + index)}"
                for index in range(1, project_count)
            ]
        else:
            assert state.quarantine_reason == "solver_timeout"
            assert desktop.closed == []
            assert not state.project_close_acks
            with pytest.raises(urllib.error.HTTPError) as rejected:
                AedtPoolHttpClient(scheduler_url).request(
                    "POST",
                    "/api/aedt-pool/leases",
                    {
                        "request_key": "unit-n-quarantined-extra",
                        "project_name": "simulation_extra",
                        "exclusive_session": False,
                        **_v2_lease_fields(tmp_path, project_count + 1),
                    },
                )
            assert rejected.value.code == 409

        for lease_id in range(2, project_count + 1):
            released = leases[lease_id - 1].release(wait_seconds=5)
            assert released["state"] == "released"
            if lease_id < project_count:
                assert host_thread.is_alive()
                assert bounded_close_calls == []

        host_thread.join(timeout=3)
        assert not host_thread.is_alive()
        assert host_result == ([2] if fault_kind == "solver_timeout" else [0])
        expected_acks = set(range(2, project_count + 1))
        if fault_kind == "pre_solve":
            expected_acks.add(1)
        assert set(state.project_close_acks) == expected_acks
        assert all(state.project_close_acks.values())
        assert state.closed_ack is True
        if fault_kind == "solver_timeout":
            assert state.requeued_lease_ids == [1]
            assert "simulation_A" not in desktop.closed
            assert bounded_close_calls == [True]
            events = [event["event"] for event in state.events]
            assert events.index("global_stop_allowed") > max(
                index for index, event in enumerate(state.events)
                if event["event"] == "release_requested"
                and event.get("lease_id") == project_count
            )
        else:
            assert state.requeued_lease_ids == []
            assert bounded_close_calls == [False]
            assert set(desktop.closed) == {
                f"simulation_{chr(ord('A') + index)}"
                for index in range(project_count)
            }
    finally:
        for lease in leases:
            lease.stop_heartbeat()
        if host_thread.is_alive():
            state.force_drain("unit test cleanup")
            host.request_stop()
            host_thread.join(timeout=3)
        if not host_thread.is_alive():
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=3)
