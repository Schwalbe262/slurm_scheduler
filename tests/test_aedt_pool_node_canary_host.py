from scripts.aedt_pool_1to2_pilot import SharedPilotControlPlane
from scripts.aedt_pool_node_canary_host import _evidence
from slurm_scheduler.aedt_canary_admission import node_local_aedt_canary_admission


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


class _CanaryDb:
    def __init__(self, host, tasks=None):
        self.host = host
        self.tasks = list(tasks or [])

    def get_task(self, task_id):
        return self.host if int(task_id) == int(self.host["id"]) else None

    def list_tasks(self, limit=5000):
        return self.tasks[:limit]


def _client(host_id=41, bundle="bundle-a"):
    return {
        "id": 51,
        "status": "queued",
        "entrypoint": "aedt_node_canary_client",
        "same_node_as_task_id": host_id,
        "payload_json": (
            '{"aedt_canary_bundle_id":"%s","aedt_canary_expected_projects":2}'
            % bundle
        ),
    }


def test_node_local_canary_admission_is_explicit_bounded_and_colocated():
    host = {
        "id": 41,
        "status": "running",
        "entrypoint": "aedt_node_canary_host",
        "payload_json": (
            '{"aedt_canary_bundle_id":"bundle-a","aedt_canary_expected_projects":2}'
        ),
    }
    allowed, reason = node_local_aedt_canary_admission(_CanaryDb(host), _client())
    assert allowed is True
    assert "admitted" in reason

    assert node_local_aedt_canary_admission(
        _CanaryDb(host), _client(bundle="other")
    )[0] is False

    already_claimed = [
        {**_client(), "id": 61, "status": "running"},
        {**_client(), "id": 62, "status": "completed"},
    ]
    assert node_local_aedt_canary_admission(
        _CanaryDb(host, already_claimed), _client()
    )[0] is False
