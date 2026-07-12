from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from slurm_scheduler.aedt_attach_client import AedtProjectLease
from slurm_scheduler.aedt_pool import AedtPoolRuntime, AedtPoolService
from slurm_scheduler.aedt_pool_api import create_aedt_pool_router
from slurm_scheduler.aedt_session_host import AedtSessionHost
from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.scheduler import Scheduler


PASSING_EVIDENCE = {
    "baseline_desktops": 2,
    "pooled_desktops": 1,
    "baseline_projects": 2,
    "pooled_projects": 2,
    "runtime_ratio": 1.0,
    "desktop_license_delta": -1,
    "output_parity_passed": True,
    "cancellation_isolation_passed": True,
    "crash_recovery_passed": True,
    "timeout_fault_injection_passed": True,
    "sibling_completion_passed": True,
    "sibling_terminal_output_passed": True,
    "sibling_data_rows_passed": True,
    "sibling_field_solution_passed": True,
    "fault_checkout_released_after_recycle_passed": True,
    "faulted_desktop_not_reused_passed": True,
    "baseline_artifact": "baseline.json",
    "pooled_artifact": "pooled.json",
    "license_artifact": "lmstat.jsonl",
}


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 13, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class AedtPoolTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "scheduler.db"))
        self.db.init()
        self.clock = Clock()
        self.service = AedtPoolService(self.db, bootstrap_token="secret", now=self.clock.now)
        self.service.init()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def add_dedicated_allocation(self, *, cpus: int = 64, node: str = "cpu-01") -> int:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu",
            node_name=node,
            total_cpus=cpus,
            total_memory_mb=512 * 1024,
        )
        self.db.update_allocation(
            allocation_id,
            state="active",
            slurm_job_id=f"job-{allocation_id}",
            drain_reason="AEDT pool project demand",
            started_at="CURRENT_TIMESTAMP",
        )
        return allocation_id

    def make_operational(self) -> None:
        validation = self.service.record_validation(PASSING_EVIDENCE)
        self.assertEqual(validation["status"], "passed")
        self.service.set_adapter_ready(True)
        self.service.set_enabled(True)

    def request(self, key: str, *, allocation_id: int = 0, node: str = ""):
        return self.service.request_lease(
            request_key=key,
            project_name=f"project-{key}",
            allocation_id=allocation_id,
            node_name=node,
        )

    def start_one_session(self, allocation_id: int, node: str = "cpu-01"):
        self.service.reconcile(execute=True)
        starts = self.service.starting_sessions()
        self.assertTrue(starts)
        session = starts[0]
        claimed = self.service.claim_start(
            allocation_id=allocation_id,
            node_name=node,
            host_id="host-1",
            bootstrap_token="secret",
        )
        self.assertEqual(claimed["id"], session["id"])
        return self.service.register_session(
            session_id=int(session["id"]),
            host_id="host-1",
            endpoint="cpu-01:50001",
            process_id="123",
            bootstrap_token="secret",
        )


class AedtPoolGateTests(AedtPoolTestCase):
    def test_requested_250_500_is_staged_but_disabled(self) -> None:
        config = self.service.config()
        self.assertEqual(config.max_sessions, 250)
        self.assertEqual(config.target_projects, 500)
        self.assertFalse(config.enabled)
        self.assertFalse(config.operational)
        self.assertEqual(self.service.summary()["sessions"], [])

    def test_operator_edits_only_desktop_ceiling_and_project_target_is_derived(self) -> None:
        config = self.service.set_operator_limit(250)
        self.assertEqual(config.max_sessions, 250)
        self.assertEqual(config.target_projects, 500)
        with self.assertRaisesRegex(ValueError, "between 0 and 550"):
            self.service.set_operator_limit(551)

    def test_activation_requires_adapter_and_fault_injection_validation(self) -> None:
        with self.assertRaisesRegex(ValueError, "validation"):
            self.service.set_enabled(True)
        failed = self.service.record_validation(
            {**PASSING_EVIDENCE, "sibling_completion_passed": False}
        )
        self.assertEqual(failed["status"], "failed")
        self.service.record_validation(PASSING_EVIDENCE)
        with self.assertRaisesRegex(ValueError, "adapter"):
            self.service.set_enabled(True)
        self.service.set_adapter_ready(True)
        self.assertTrue(self.service.set_enabled(True).operational)

    def test_false_positive_reopen_probe_pid_and_grpc_without_artifacts_fails_gate(self) -> None:
        evidence = {
            **PASSING_EVIDENCE,
            "sibling_completion_passed": False,
            "sibling_terminal_output_passed": False,
            "sibling_data_rows_passed": False,
            "sibling_field_solution_passed": False,
            "sibling_pid_survived": True,
            "grpc_survived": True,
            "pilot_task_id": 732549,
        }
        validation = self.service.record_validation(evidence)
        self.assertEqual(validation["status"], "failed")
        self.assertIn("sibling_terminal_output_passed", validation["failure_message"])
        with self.assertRaisesRegex(ValueError, "validation"):
            self.service.set_enabled(True)

    def test_unrelated_allocations_are_never_used(self) -> None:
        allocation_id = self.db.create_allocation("a", "cpu", "prod-node", 64, 512 * 1024)
        self.db.update_allocation(
            allocation_id,
            state="active",
            slurm_job_id="production",
            drain_reason="queued FEA task",
        )
        self.make_operational()
        self.request("one")
        plan = self.service.reconcile(execute=True)
        self.assertEqual(plan["hard_session_count"], 0)
        self.assertGreaterEqual(plan["node_requests"], 1)
        self.assertEqual(self.service.starting_sessions(), [])

    def test_api_operator_surface_accepts_only_desktop_ceiling(self) -> None:
        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool/config"
        )

        class Request:
            def __init__(self, payload):
                self.payload = payload

            async def json(self):
                return self.payload

        response = asyncio.run(endpoint(Request({"max_aedt_sessions": 250})))
        self.assertEqual(response["target_project_concurrency"], 500)
        from fastapi import HTTPException

        with self.assertRaises(HTTPException) as raised:
            asyncio.run(
                endpoint(
                    Request(
                        {
                            "max_aedt_sessions": 250,
                            "target_project_concurrency": 999,
                        }
                    )
                )
            )
        self.assertEqual(raised.exception.status_code, 422)


class AedtLeaseLifecycleTests(AedtPoolTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.allocation_id = self.add_dedicated_allocation()
        self.make_operational()

    def test_one_session_has_exactly_two_slots(self) -> None:
        leases = [self.request(f"r{i}", allocation_id=self.allocation_id, node="cpu-01") for i in range(2)]
        session, host_token = self.start_one_session(self.allocation_id)
        for lease, _token in leases:
            current = self.service.get_lease(int(lease["id"]))
            self.assertEqual(current["session_id"], session["id"])
            self.assertIn(current["slot_index"], {0, 1})
        self.assertEqual(
            {self.service.get_lease(int(lease[0]["id"]))["slot_index"] for lease in leases},
            {0, 1},
        )
        self.assertEqual(self.service.get_session(int(session["id"]))["slots_total"], 2)
        self.service.heartbeat_session(int(session["id"]), host_token)

    def test_release_is_two_phase_and_slot_is_not_reused_early(self) -> None:
        self.service.set_operator_limit(1)
        first, first_token = self.request("first", allocation_id=self.allocation_id, node="cpu-01")
        second, _ = self.request("second", allocation_id=self.allocation_id, node="cpu-01")
        session, host_token = self.start_one_session(self.allocation_id)
        third, _ = self.request("third", allocation_id=self.allocation_id, node="cpu-01")
        self.assertEqual(self.service.get_lease(int(third["id"]))["state"], "queued")
        releasing = self.service.release_lease(int(first["id"]), first_token)
        self.assertEqual(releasing["state"], "releasing")
        self.assertEqual(self.service.get_lease(int(third["id"]))["state"], "queued")
        self.service.complete_release(
            int(session["id"]), host_token, int(first["id"]), success=True
        )
        self.assertEqual(self.service.get_lease(int(third["id"]))["state"], "leased")
        self.assertEqual(self.service.get_lease(int(second["id"]))["state"], "leased")

    def test_client_binds_actual_aedt_project_name_after_session_attach(self) -> None:
        lease, token = self.request("temporary", allocation_id=self.allocation_id, node="cpu-01")
        self.start_one_session(self.allocation_id)
        updated = self.service.bind_lease_project_name(
            int(lease["id"]), token, "simulation-pilot-0001"
        )
        self.assertEqual(updated["project_name"], "simulation-pilot-0001")

    def test_queued_client_heartbeat_survives_slow_node_wait(self) -> None:
        # Disable mutation so this remains a queued request.
        self.service.set_enabled(False)
        lease, token = self.request("slow-node")
        self.clock.advance(170)
        self.service.heartbeat_lease(int(lease["id"]), token)
        self.clock.advance(170)
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_lease(int(lease["id"]))["state"], "queued")
        self.clock.advance(181)
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_lease(int(lease["id"]))["state"], "expired")

    def test_solver_timeout_quarantines_session_and_preserves_sibling_grace(self) -> None:
        first, first_token = self.request("timeout", allocation_id=self.allocation_id, node="cpu-01")
        second, second_token = self.request("sibling", allocation_id=self.allocation_id, node="cpu-01")
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.heartbeat_lease(int(first["id"]), first_token)
        self.service.heartbeat_lease(int(second["id"]), second_token)
        self.service.report_project_fault(
            int(first["id"]),
            first_token,
            fault_kind="solver_timeout",
            sibling_grace_seconds=900,
        )
        commands = self.service.session_commands(int(session["id"]), host_token)
        self.assertTrue(commands["drain"])
        self.assertFalse(commands["global_stop_allowed"])
        self.assertEqual([row["id"] for row in commands["deferred_projects"]], [first["id"]])
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "draining")
        self.assertTrue(self.service.config().enabled)
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "draining")
        third, _ = self.request("must-not-reuse", allocation_id=self.allocation_id, node="cpu-01")
        self.assertEqual(self.service.get_lease(int(third["id"]))["state"], "queued")
        self.assertEqual(self.service.starting_sessions(), [])
        self.assertGreaterEqual(self.service.dry_run()["node_requests"], 1)

        # Healthy sibling closes normally; only then may the host use the
        # session-wide stop/recycle path for the timed-out solver.
        self.service.release_lease(int(second["id"]), second_token)
        self.service.complete_release(
            int(session["id"]), host_token, int(second["id"]), success=True
        )
        commands = self.service.session_commands(int(session["id"]), host_token)
        self.assertEqual(commands["sibling_live_count"], 0)
        self.assertTrue(commands["global_stop_allowed"])
        self.assertEqual([row["id"] for row in commands["deferred_projects"]], [first["id"]])
        # Whole-session failure/recycle, not a local close ACK, requeues A.
        self.service.close_session(
            int(session["id"]),
            host_token,
            success=False,
            failure_message="faulted Desktop recycled",
            requeue_siblings=True,
        )
        retried = self.service.get_lease(int(first["id"]))
        self.assertEqual(retried["state"], "queued")
        self.assertIsNone(retried["session_id"])
        self.assertEqual(retried["requested_allocation_id"], 0)
        self.assertEqual(retried["requested_node_name"], "")
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "draining")

    def test_aedt_death_requeues_every_sibling_lease(self) -> None:
        first, _ = self.request("death-a", allocation_id=self.allocation_id, node="cpu-01")
        second, _ = self.request("death-b", allocation_id=self.allocation_id, node="cpu-01")
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.close_session(
            int(session["id"]),
            host_token,
            success=False,
            failure_message="gRPC died",
            requeue_siblings=True,
        )
        for lease in (first, second):
            current = self.service.get_lease(int(lease["id"]))
            self.assertEqual(current["state"], "queued")
            self.assertIsNone(current["session_id"])

    def test_counted_session_claim_prevents_allocation_close_safety_check(self) -> None:
        self.request("claim", allocation_id=self.allocation_id, node="cpu-01")
        self.service.reconcile(execute=True)
        self.assertTrue(self.db.allocation_has_aedt_pool_claim(self.allocation_id))

    def test_idle_session_waits_for_ttl_before_drain(self) -> None:
        lease, token = self.request("idle-ttl", allocation_id=self.allocation_id, node="cpu-01")
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.release_lease(int(lease["id"]), token)
        self.service.complete_release(int(session["id"]), host_token, int(lease["id"]), success=True)
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "ready")
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "ready")
        for _ in range(9):
            self.clock.advance(100)
            self.service.heartbeat_session(int(session["id"]), host_token)
        self.clock.advance(1)
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "draining")

    def test_explicit_operator_disable_drains_without_cancelling_sibling(self) -> None:
        lease, _ = self.request("manual-disable", allocation_id=self.allocation_id, node="cpu-01")
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.set_enabled(False)
        current = self.service.get_session(int(session["id"]))
        self.assertEqual(current["state"], "draining")
        self.assertEqual(self.service.get_lease(int(lease["id"]))["state"], "leased")
        commands = self.service.session_commands(int(session["id"]), host_token)
        self.assertTrue(commands["drain"])
        self.assertFalse(commands["global_stop_allowed"])

    def test_generic_scheduler_scale_in_cannot_close_dedicated_pool(self) -> None:
        scheduler = Scheduler(
            self.db,
            [AccountConfig("a", "invalid", 22, "a", "key", "/work")],
            30,
            client_factory=lambda _account: object(),
        )
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertFalse(scheduler.close_allocation(allocation, "idle scale-in"))
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")


class FakeRuntimeScheduler:
    allocation_cpus = 64

    def __init__(self) -> None:
        self.open_calls: list[dict] = []

    def open_allocation_record(self, reason: str, **kwargs):
        self.open_calls.append({"reason": reason, **kwargs})
        return {"id": len(self.open_calls), "state": "pending"}


class AedtRuntimeTests(AedtPoolTestCase):
    def test_node_request_uses_full_scheduler_shape_not_eight_cpu_micro_node(self) -> None:
        self.make_operational()
        self.request("needs-node")
        fake = FakeRuntimeScheduler()
        runtime = AedtPoolRuntime(self.service, fake, interval_seconds=30)
        plan = runtime.tick()
        self.assertEqual(plan["node_allocations_opened"], 1)
        self.assertEqual(fake.open_calls[0]["requested_cpus"], 64)
        self.assertLessEqual(self.service.config().node_cpu_factor, 2.0)

    def test_pending_dedicated_node_prevents_duplicate_scale_out(self) -> None:
        self.make_operational()
        self.request("needs-one-node")

        class PersistentScheduler(FakeRuntimeScheduler):
            def __init__(self, db):
                super().__init__()
                self.db = db

            def open_allocation_record(self, reason: str, **kwargs):
                self.open_calls.append({"reason": reason, **kwargs})
                allocation_id = self.db.create_allocation(
                    "a", "cpu", "cpu-pending", kwargs["requested_cpus"], 512 * 1024
                )
                self.db.update_allocation(
                    allocation_id,
                    state="pending",
                    slurm_job_id=f"pending-{allocation_id}",
                    drain_reason=reason,
                )
                return self.db.get_allocation(allocation_id)

        fake = PersistentScheduler(self.db)
        runtime = AedtPoolRuntime(self.service, fake, interval_seconds=30)
        self.assertEqual(runtime.tick()["node_allocations_opened"], 1)
        self.assertEqual(runtime.tick()["node_allocations_opened"], 0)
        self.assertEqual(len(fake.open_calls), 1)


class FakeLeaseHttp:
    def __init__(self) -> None:
        self.calls = 0

    def request(self, method, path, payload=None, lease_token=""):
        self.calls += 1
        if self.calls < 3:
            return {"id": 1, "state": "queued", "endpoint": ""}
        return {"id": 1, "state": "leased", "endpoint": "cpu-01:50001"}


class AttachClientTests(unittest.TestCase):
    def test_waiting_client_heartbeats_while_queued(self) -> None:
        http = FakeLeaseHttp()
        lease = AedtProjectLease(http, 1, "token", "p")
        with patch("slurm_scheduler.aedt_attach_client.time.sleep", return_value=None):
            result = lease.wait_until_leased(timeout_seconds=2, heartbeat_seconds=0)
        self.assertEqual(result["state"], "leased")
        self.assertEqual(http.calls, 3)
        lease.stop_heartbeat()


class PilotPriorityApiTests(unittest.TestCase):
    def test_explicit_pilot_priority_round_trips_through_task_submit_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            accounts = root / "accounts.yaml"
            accounts.write_text(
                "\n".join(
                    [
                        "accounts:",
                        "  - name: pilot",
                        "    host: invalid",
                        "    port: 22",
                        "    username: pilot",
                        "    private_key_path: key",
                        "    remote_workspace: /work",
                    ]
                ),
                encoding="utf-8",
            )
            config = root / "app.yaml"
            config.write_text(
                "\n".join(
                    [
                        f'database_path: "{(root / "scheduler.db").as_posix()}"',
                        f'accounts_path: "{accounts.as_posix()}"',
                        "min_warm_allocations: 0",
                        "reconcile_on_start: false",
                        "backup_enabled: false",
                        "web_listener_watchdog_enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            previous = os.environ.get("SLURM_SCHEDULER_CONFIG")
            os.environ["SLURM_SCHEDULER_CONFIG"] = str(config)
            try:
                from slurm_scheduler.app import create_app

                app = create_app(str(config))
            finally:
                if previous is None:
                    os.environ.pop("SLURM_SCHEDULER_CONFIG", None)
                else:
                    os.environ["SLURM_SCHEDULER_CONFIG"] = previous
            app.router.on_startup.clear()
            app.router.on_shutdown.clear()
            endpoint = next(
                route.endpoint
                for route in app.routes
                if getattr(route, "path", "") == "/api/tasks"
                and "POST" in getattr(route, "methods", set())
            )

            class Request:
                def __init__(self, payload):
                    self.payload = payload

                async def json(self):
                    return self.payload

            low = asyncio.run(
                endpoint(Request({"name": "production", "remote_cwd": "/work", "command": "run", "priority": 0}))
            )
            pilot = asyncio.run(
                endpoint(
                    Request(
                        {
                            "name": "aedt-pool-validation",
                            "remote_cwd": "/work",
                            "command": "run-pilot",
                            "priority": 10000,
                        }
                    )
                )
            )
            low_payload = json.loads(low.body)
            pilot_payload = json.loads(pilot.body)
            self.assertEqual(low_payload["status"], "queued")
            self.assertEqual(pilot_payload["status"], "queued")
            self.assertEqual(pilot_payload["priority"], 10000)
            ordered = app.state.scheduler.queued_demand_tasks()
            self.assertEqual([task["name"] for task in ordered[:2]], ["aedt-pool-validation", "production"])


class FakeDesktopApi:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def StopSimulations(self, _clean: bool) -> None:
        self.events.append("global-stop")


class FakeDesktop:
    port = 50001
    aedt_process_id = 123

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.odesktop = FakeDesktopApi(events)

    def release_desktop(self, close_projects=True, close_desktop=True) -> None:
        self.events.append("desktop-close")


class FakeHostControlPlane:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.command_count = 0

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("claim-start"):
            return {"session": {"id": 1}}
        if path.endswith("/register"):
            return {"host_token": "host-token", "session": {"id": 1}}
        if path.endswith("/heartbeat"):
            return {}
        if path.endswith("/commands"):
            self.command_count += 1
            if self.command_count == 1:
                self.events.append("grace-active")
                return {
                    "close_projects": [],
                    "global_stop_allowed": False,
                    "drain": True,
                    "sibling_live_count": 1,
                }
            self.events.append("grace-complete")
            return {
                "close_projects": [],
                "global_stop_allowed": True,
                "drain": True,
                "sibling_live_count": 0,
            }
        if path.endswith("/closed"):
            self.events.append("closed-ack")
            return {}
        raise AssertionError(path)


class SessionHostTests(unittest.TestCase):
    def test_global_stop_occurs_only_after_control_plane_sibling_grace(self) -> None:
        events: list[str] = []
        host = AedtSessionHost(
            FakeHostControlPlane(events),
            allocation_id=1,
            node_name="cpu-01",
            heartbeat_seconds=5,
        )
        desktop = FakeDesktop(events)
        host._start_desktop = lambda: desktop
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 2)
        self.assertEqual(events.count("global-stop"), 1)
        self.assertLess(events.index("grace-active"), events.index("grace-complete"))
        self.assertLess(events.index("grace-complete"), events.index("global-stop"))

    def test_unconfirmed_desktop_exit_stays_counted_without_closed_ack(self) -> None:
        events: list[str] = []
        control = FakeHostControlPlane(events)
        host = AedtSessionHost(control, allocation_id=1, node_name="cpu-01", heartbeat_seconds=5)
        host._start_desktop = lambda: FakeDesktop(events)
        host._bounded_close_desktop = lambda **_kwargs: False
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 3)
        self.assertNotIn("closed-ack", events)


if __name__ == "__main__":
    unittest.main()
