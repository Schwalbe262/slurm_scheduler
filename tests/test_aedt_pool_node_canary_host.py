from scripts.aedt_pool_1to2_pilot import SharedPilotControlPlane
from scripts.aedt_pool_node_canary_host import _evidence


def test_node_canary_evidence_requires_two_clean_released_projects():
    state = SharedPilotControlPlane()
    state.leases = {
        1: {"state": "released", "project_name": "simulation_A", "failure_message": ""},
        2: {"state": "released", "project_name": "simulation_B", "failure_message": ""},
    }
    state.project_close_acks = {1: True, 2: True}
    state.session["state"] = "closed"
    state.closed_ack = True

    evidence = _evidence(state, [0])

    assert evidence["passed"] is True
    assert evidence["production_pool_enabled"] is False


def test_node_canary_evidence_fails_on_project_fault_or_missing_ack():
    state = SharedPilotControlPlane()
    state.leases = {
        1: {"state": "released", "project_name": "simulation_A", "failure_message": "timeout"},
        2: {"state": "released", "project_name": "simulation_B", "failure_message": ""},
    }
    state.project_close_acks = {2: True}
    state.session["state"] = "closed"
    state.closed_ack = True

    evidence = _evidence(state, [0])

    assert evidence["passed"] is False
    assert "project_failure_reported" in evidence["failures"]


def test_shared_control_plane_accepts_a_future_bounded_n():
    state = SharedPilotControlPlane(max_projects=3)
    assert state.max_projects == 3
    assert state._all_projects_closed() is False
