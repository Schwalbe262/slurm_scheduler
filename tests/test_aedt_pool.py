from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from slurm_scheduler.aedt_attach_client import (
    AedtPoolHttpClient,
    AedtProjectLease,
    acquire_project_lease,
)
from slurm_scheduler.aedt_pool import AedtPoolRuntime, AedtPoolService
from slurm_scheduler.aedt_pool_api import create_aedt_pool_router
from slurm_scheduler.aedt_session_host import AedtSessionHost, ControlPlaneClient
from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.models import TaskCreate, TaskStatus
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

    def request(
        self,
        key: str,
        *,
        allocation_id: int = 0,
        node: str = "",
        exclusive_session: bool = False,
    ):
        return self.service.request_lease(
            request_key=key,
            project_name=f"project-{key}",
            allocation_id=allocation_id,
            node_name=node,
            exclusive_session=exclusive_session,
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
        self.assertEqual(config.min_idle_sessions, 0)
        self.assertEqual(config.target_projects, 500)
        self.assertFalse(config.enabled)
        self.assertFalse(config.operational)
        self.assertEqual(self.service.summary()["sessions"], [])

    def test_legacy_operator_limit_derives_project_target(self) -> None:
        config = self.service.set_operator_limit(250)
        self.assertEqual(config.max_sessions, 250)
        self.assertEqual(config.target_projects, 500)
        with self.assertRaisesRegex(ValueError, "between 0 and 550"):
            self.service.set_operator_limit(551)

    def test_operator_can_set_all_three_durable_limits_while_disabled(self) -> None:
        config = self.service.set_operator_limits(
            max_sessions=250,
            min_idle_sessions=1,
            target_projects=400,
            projects_per_session=2,
        )
        self.assertEqual(config.max_sessions, 250)
        self.assertEqual(config.min_idle_sessions, 1)
        self.assertEqual(config.target_projects, 400)
        self.assertEqual(config.projects_per_session, 2)
        self.assertFalse(config.enabled)
        gated_plan = self.service.summary()["plan"]
        self.assertEqual(gated_plan["warm_spare_starts_authorized"], 0)
        self.assertIn("not operational", gated_plan["warm_spare_status_reason"])
        reloaded = AedtPoolService(self.db, bootstrap_token="secret")
        self.assertEqual(reloaded.config().target_projects, 400)

    def test_operator_limits_fail_closed_on_invalid_topology(self) -> None:
        config = self.service.set_operator_limits(
            max_sessions=100,
            target_projects=300,
            projects_per_session=3,
        )
        self.assertEqual(config.projects_per_session, 3)
        self.assertEqual(self.service.summary()["plan"]["sessions_per_new_node"], 5)
        with self.assertRaisesRegex(ValueError, "projects_per_aedt"):
            self.service.set_operator_limits(projects_per_session=4)
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            self.service.set_operator_limits(
                max_sessions=100,
                target_projects=301,
                projects_per_session=3,
            )
        with self.assertRaisesRegex(ValueError, "between 0 and 1650"):
            self.service.set_operator_limits(target_projects=1651)

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

    def test_api_operator_surface_accepts_three_bounded_limits(self) -> None:
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

        response = asyncio.run(endpoint(Request({
            "max_aedt_sessions": 250,
            "min_idle_aedt_sessions": 1,
            "target_project_concurrency": 400,
            "projects_per_aedt": 2,
        })))
        self.assertEqual(response["target_project_concurrency"], 400)
        self.assertEqual(response["min_idle_aedt_sessions"], 1)
        self.assertEqual(response["projects_per_aedt"], 2)
        from fastapi import HTTPException

        with self.assertRaises(HTTPException) as raised:
            asyncio.run(
                endpoint(
                    Request(
                        {
                            "max_aedt_sessions": 250,
                            "unexpected": 999,
                        }
                    )
                )
            )
        self.assertEqual(raised.exception.status_code, 422)

    def test_every_mutating_api_route_requires_bootstrap_token(self) -> None:
        from fastapi import HTTPException

        router = create_aedt_pool_router(self.service)
        mutating_routes = [
            route
            for route in router.routes
            if set(getattr(route, "methods", set())) & {"POST", "PATCH", "PUT", "DELETE"}
        ]
        self.assertEqual(len(mutating_routes), 15)
        guards = set()
        for route in mutating_routes:
            route_guards = [
                dependency.call
                for dependency in route.dependant.dependencies
                if getattr(dependency.call, "__name__", "") == "require_bootstrap"
            ]
            self.assertEqual(
                len(route_guards),
                1,
                f"{sorted(route.methods)} {route.path} is missing the bootstrap guard",
            )
            guards.update(route_guards)

        self.assertEqual(len(guards), 1)
        guard = guards.pop()
        for token in ("", "wrong"):
            with self.assertRaises(HTTPException) as raised:
                guard(token)
            self.assertEqual(raised.exception.status_code, 403)
        guard("secret")

    def test_projects_per_aedt_change_requires_disabled_drained_pool(self) -> None:
        self.make_operational()
        with self.assertRaisesRegex(ValueError, "disable and fully drain"):
            self.service.set_operator_limits(projects_per_session=1)

    def test_web_ui_exposes_limits_usage_leases_and_fail_closed_gates(self) -> None:
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "aedt_pool.html"
        ).read_text(encoding="utf-8")
        for required in (
            'id="max-aedt-sessions"',
            'id="min-idle-aedt-sessions"',
            'id="target-projects" name="target_project_concurrency" type="number" min="0" max="1650"',
            'id="projects-per-aedt" name="projects_per_aedt" type="number" min="1" max="3"',
            'id="aedt-bootstrap-token"',
            '"X-AEDT-Bootstrap-Token"',
            'id="lease-rows"',
            'id="enable-pool"',
            "latest_validation",
            "idle_session_count",
            "warm_spare_status_reason",
            "<th>Session ID</th><th>Host task</th><th>Node</th><th>Account</th><th>State</th><th>Started</th><th>Last heartbeat</th><th>Attached projects</th><th>Failure/quarantine reason</th>",
            "cell.colSpan = 9",
            "link.href = `/tasks/${taskId}`",
            "session.allocation_account_name",
            "session.started_at",
            "session.last_heartbeat_at",
            "session.active_lease_count",
            "session.attached_project_names",
            'fetch("/api/aedt-pool/enable"',
        ):
            self.assertIn(required, template)
        for removed in ("node-local-session", "summary.node_local", "renderNodeLocal"):
            self.assertNotIn(removed, template)

        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool"
        )
        self.assertNotIn("node_local", endpoint())

    def test_main_dashboard_uses_fea_aedt_arithmetic_and_links_pool_limits(self) -> None:
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "dashboard.html"
        ).read_text(encoding="utf-8")
        for required in (
            'href="/aedt-pool"',
            'id="aedt-dashboard-sessions"',
            "aedt_pool_summary.plan.hard_session_count",
            "aedt_pool_summary.config.max_aedt_sessions",
            'id="aedt-dashboard-projects"',
            "aedt_pool_summary.plan.live_projects",
            "aedt_pool_summary.config.target_project_concurrency",
            '<span class="summary-label">FEA / AEDT</span>',
            "task_summary.fea",
            "task_summary.aedt",
            'data-aedt-pool-sessions="{{ task_summary.aedt_pool_sessions }}"',
            'data-aedt-backend="{{ task.aedt_backend or \'standalone\' }}"',
            'data-project="{{ task.project or \'\' }}"',
            'project !== "_aedt_pool_hosts"',
            'status === "running" || status === "attaching"',
            "counts.standaloneAedt",
            "tasks.aedt",
            "tasks.aedt_pool_sessions",
            'summaryUrl.searchParams.set("task_name_contains", taskFilter)',
        ):
            self.assertIn(required, template)
        self.assertNotIn("session hosts", template)
        self.assertNotIn("tasks.session_hosts", template)
        self.assertNotIn('id="aedt-dashboard-node-local"', template)
        self.assertNotIn("aedt_pool_summary.node_local", template)


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

    def test_summary_enriches_session_operator_details(self) -> None:
        leases = [
            self.request(f"summary-{index}", allocation_id=self.allocation_id, node="cpu-01")
            for index in range(2)
        ]
        session, _host_token = self.start_one_session(self.allocation_id)
        session_id = int(session["id"])
        older_host_task_id = self.db.create_task(
            TaskCreate(
                name=f"aedt-session-host-{session_id}",
                remote_cwd="~/aedt-host",
                command="run-host",
                scheduling_profile="fea_bursty",
                dedupe_key=f"aedt-session-host:{session_id}",
                project="_aedt_pool_hosts",
            )
        )
        self.db.update_task(
            older_host_task_id,
            status=TaskStatus.COMPLETED.value,
            finished_at="CURRENT_TIMESTAMP",
        )
        host_task_id = self.db.create_task(
            TaskCreate(
                name=f"aedt-session-host-{session_id}",
                remote_cwd="~/aedt-host",
                command="retry-host",
                scheduling_profile="fea_bursty",
                project="_aedt_pool_hosts",
            )
        )
        self.db.update_task(
            host_task_id,
            status=TaskStatus.FAILED.value,
            finished_at="CURRENT_TIMESTAMP",
        )
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET account_name = 'stale-copy' WHERE id = ?",
                (session_id,),
            )

        summary_session = next(
            item for item in self.service.summary()["sessions"] if int(item["id"]) == session_id
        )

        self.assertEqual(summary_session["host_task_id"], host_task_id)
        self.assertEqual(summary_session["allocation_account_name"], "a")
        self.assertEqual(summary_session["active_lease_count"], 2)
        self.assertEqual(
            summary_session["attached_project_names"],
            [f"project-summary-{index}" for index in range(2)],
        )
        self.assertEqual(
            {int(self.service.get_lease(int(lease["id"]))["session_id"]) for lease, _ in leases},
            {session_id},
        )

    def test_min_idle_session_starts_and_refills_when_last_idle_is_leased(self) -> None:
        self.service.set_operator_limits(
            max_sessions=2,
            min_idle_sessions=1,
            target_projects=4,
        )
        self.service.reconcile(execute=True)
        session, _host_token = self.start_one_session(self.allocation_id)
        before = self.service.summary()
        self.assertEqual(before["config"]["min_idle_aedt_sessions"], 1)
        self.assertEqual(before["plan"]["idle_session_count"], 1)

        lease, _token = self.request(
            "take-last-idle",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )

        self.assertEqual(self.service.get_lease(int(lease["id"]))["session_id"], session["id"])
        self.assertEqual(len(self.service.starting_sessions()), 1)
        after = self.service.summary()["plan"]
        self.assertEqual(after["hard_session_count"], 2)
        self.assertEqual(after["idle_session_count"], 0)
        self.assertIn("startup", after["warm_spare_status_reason"])

    def test_min_idle_session_never_exceeds_session_ceiling(self) -> None:
        self.service.set_operator_limits(
            max_sessions=1,
            min_idle_sessions=1,
            target_projects=2,
        )
        self.service.reconcile(execute=True)
        self.start_one_session(self.allocation_id)

        self.request("ceiling", allocation_id=self.allocation_id, node="cpu-01")

        plan = self.service.summary()["plan"]
        self.assertEqual(plan["hard_session_count"], 1)
        self.assertEqual(self.service.starting_sessions(), [])
        self.assertEqual(plan["idle_session_count"], 0)
        self.assertIn("ceiling", plan["warm_spare_status_reason"])

    def test_min_idle_session_skips_start_without_license_headroom(self) -> None:
        self.service.set_operator_limits(
            max_sessions=2,
            min_idle_sessions=1,
            target_projects=4,
        )
        self.service.set_warm_spare_admission_checker(
            lambda requested: (0, "license capacity exhausted for electronics_desktop")
        )

        plan = self.service.reconcile(execute=True)

        self.assertEqual(self.service.starting_sessions(), [])
        self.assertEqual(plan["hard_session_count"], 0)
        self.assertEqual(plan["node_requests"], 0)
        self.assertEqual(plan["warm_spare_starts_authorized"], 0)
        self.assertIn("license capacity exhausted", plan["warm_spare_status_reason"])

    def test_min_idle_session_replaces_unhealthy_owner_below_ceiling(self) -> None:
        self.service.set_operator_limits(
            max_sessions=2,
            min_idle_sessions=1,
            target_projects=4,
        )
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO aedt_sessions (
                    session_key, allocation_id, account_name, node_name,
                    slots_total, state, failure_message
                ) VALUES ('unhealthy-owner', ?, 'a', 'cpu-01', 2, 'unhealthy', 'lost heartbeat')
                """,
                (self.allocation_id,),
            )

        self.service.reconcile(execute=True)

        plan = self.service.summary()["plan"]
        self.assertEqual(plan["unavailable_session_count"], 1)
        self.assertEqual(plan["hard_session_count"], 2)
        self.assertEqual(len(self.service.starting_sessions()), 1)

    def test_exclusive_1to1_lease_never_accepts_a_sibling(self) -> None:
        self.service.set_operator_limit(1)
        exclusive, _ = self.request(
            "exclusive",
            allocation_id=self.allocation_id,
            node="cpu-01",
            exclusive_session=True,
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        current = self.service.get_lease(int(exclusive["id"]))
        self.assertEqual(current["session_id"], session["id"])
        self.assertEqual(current["exclusive_session"], 1)

        sibling, _ = self.request(
            "sibling-blocked",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        self.assertEqual(
            self.service.get_lease(int(sibling["id"]))["state"], "queued"
        )
        with self.db.connect() as conn:
            live_on_session = conn.execute(
                """
                SELECT COUNT(*) FROM aedt_project_leases
                WHERE session_id = ?
                  AND state IN ('leased','active','releasing')
                """,
                (int(session["id"]),),
            ).fetchone()[0]
        self.assertEqual(live_on_session, 1)

    def test_exclusive_demand_counts_one_session_per_project(self) -> None:
        self.service.set_operator_limit(2)
        self.request("exclusive-a", exclusive_session=True)
        self.request("exclusive-b", exclusive_session=True)
        plan = self.service.dry_run()
        self.assertEqual(plan["exclusive_projects"], 2)
        self.assertEqual(plan["desired_sessions"], 2)

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
    def test_control_plane_url_setting_is_normalized_and_durable(self) -> None:
        self.assertEqual(self.service.config().control_plane_url, "")
        configured = self.service.set_control_plane_url(
            "  http://gate2:18790/  "
        )
        self.assertEqual(configured.control_plane_url, "http://gate2:18790")

        reloaded = AedtPoolService(
            self.db, bootstrap_token="secret", now=self.clock.now
        )
        self.assertEqual(
            reloaded.config().control_plane_url, "http://gate2:18790"
        )
        self.assertEqual(
            self.service.set_control_plane_url("").control_plane_url, ""
        )

    def test_relay_mode_fails_closed_before_mutating_reconcile_without_url(self) -> None:
        self.make_operational()
        self.request("relay-url-missing")
        fake = FakeRuntimeScheduler()
        runtime = AedtPoolRuntime(
            self.service,
            fake,
            interval_seconds=30,
            require_published_control_plane_url=True,
        )

        with patch.object(
            self.service,
            "reconcile",
            side_effect=AssertionError("mutating reconcile must not run"),
        ) as reconcile:
            plan = runtime.tick()

        reconcile.assert_not_called()
        self.assertFalse(plan["control_plane_ready"])
        self.assertIn("not published", plan["control_plane_error"])
        self.assertEqual(plan["node_allocations_opened"], 0)
        self.assertEqual(plan["host_tasks_started"], 0)
        self.assertEqual(fake.open_calls, [])

    def test_relay_mode_resolves_each_published_url_dynamically(self) -> None:
        self.service.set_control_plane_url("http://gate2:18790")
        runtime = AedtPoolRuntime(
            self.service,
            FakeRuntimeScheduler(),
            interval_seconds=30,
            scheduler_url="http://static.invalid:8000",
            host_remote_cwd="/work/aedt",
            host_bootstrap_token_file="/shared/aedt-token",
            require_published_control_plane_url=True,
        )
        session = {"allocation_id": 7, "node_name": "cpu-07"}
        self.assertTrue(runtime.host_launch_configured)
        first = runtime._host_command(session)
        self.assertIn("http://gate2:18790", first)
        self.assertNotIn("static.invalid", first)

        self.service.set_control_plane_url("http://gate2:18791/")
        second = runtime._host_command(session)
        self.assertIn("http://gate2:18791", second)
        self.assertNotEqual(first, second)

    def test_static_scheduler_url_remains_default_when_published_url_exists(self) -> None:
        self.service.set_control_plane_url("http://gate2:18790")
        runtime = AedtPoolRuntime(
            self.service,
            FakeRuntimeScheduler(),
            interval_seconds=30,
            scheduler_url="http://scheduler-local:8000/",
            host_remote_cwd="/work/aedt",
            host_bootstrap_token_file="/shared/aedt-token",
        )
        command = runtime._host_command(
            {"allocation_id": 8, "node_name": "cpu-08"}
        )
        self.assertIn("http://scheduler-local:8000", command)
        self.assertNotIn("gate2", command)

    def test_relay_mode_reconciles_after_url_is_published(self) -> None:
        self.make_operational()
        self.request("relay-url-ready")
        self.service.set_control_plane_url("http://gate2:18790")
        fake = FakeRuntimeScheduler()
        runtime = AedtPoolRuntime(
            self.service,
            fake,
            interval_seconds=30,
            require_published_control_plane_url=True,
        )
        plan = runtime.tick()
        self.assertTrue(plan["control_plane_ready"])
        self.assertEqual(plan["control_plane_error"], "")
        self.assertEqual(plan["node_allocations_opened"], 1)
        self.assertEqual(len(fake.open_calls), 1)

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

    def test_mft_pilot_can_request_an_exclusive_session(self) -> None:
        calls = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                calls.append((method, path, payload))
                return {
                    "lease": {
                        "id": 7,
                        "state": "queued",
                        "endpoint": "",
                        "exclusive_session": 1,
                    },
                    "client_token": "token",
                }

        with patch("slurm_scheduler.aedt_attach_client.AedtPoolHttpClient") as client_factory:
            client_factory.return_value = Http()
            lease = acquire_project_lease(
                "http://scheduler",
                "mft-pilot",
                bootstrap_token="bootstrap",
                request_key="pilot-1to1",
                exclusive_session=True,
            )
        client_factory.assert_called_once_with(
            "http://scheduler",
            bootstrap_token="bootstrap",
            bootstrap_token_file="",
        )
        self.assertTrue(lease.exclusive_session)
        self.assertTrue(calls[0][2]["exclusive_session"])


class ControlPlaneHttpClientTests(unittest.TestCase):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"{}"

    def test_session_host_sends_bootstrap_and_scoped_host_tokens(self) -> None:
        with patch(
            "slurm_scheduler.aedt_session_host.urllib.request.build_opener",
        ) as build_opener:
            build_opener.return_value.open.return_value = self.Response()
            ControlPlaneClient(
                "http://relay",
                bootstrap_token="bootstrap",
            ).request(
                "POST",
                "/api/aedt-pool/sessions/1/heartbeat",
                {},
                host_token="host",
            )

        request = build_opener.return_value.open.call_args.args[0]
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["x-aedt-bootstrap-token"], "bootstrap")
        self.assertEqual(headers["x-aedt-host-token"], "host")
        self.assertEqual(build_opener.call_args.args[0].proxies, {})

    def test_lease_client_sends_bootstrap_and_scoped_lease_tokens(self) -> None:
        with patch(
            "slurm_scheduler.aedt_attach_client.urllib.request.build_opener",
        ) as build_opener:
            build_opener.return_value.open.return_value = self.Response()
            AedtPoolHttpClient(
                "http://relay",
                bootstrap_token="bootstrap",
            ).request(
                "POST",
                "/api/aedt-pool/leases/1/heartbeat",
                {},
                lease_token="lease",
            )

        request = build_opener.return_value.open.call_args.args[0]
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["x-aedt-bootstrap-token"], "bootstrap")
        self.assertEqual(headers["x-aedt-lease-token"], "lease")
        self.assertEqual(build_opener.call_args.args[0].proxies, {})

    def test_read_only_control_plane_requests_do_not_expose_bootstrap_token(self) -> None:
        with patch(
            "slurm_scheduler.aedt_session_host.urllib.request.build_opener",
        ) as build_opener:
            build_opener.return_value.open.return_value = self.Response()
            ControlPlaneClient(
                "http://relay",
                bootstrap_token="bootstrap",
            ).request("GET", "/api/aedt-pool/sessions/1/commands")

        request = build_opener.return_value.open.call_args.args[0]
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertNotIn("x-aedt-bootstrap-token", headers)

    def test_lease_client_can_load_bootstrap_from_node_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_file = Path(tmp) / "bootstrap-token"
            token_file.write_text("from-file\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "SLURM_AEDT_POOL_BOOTSTRAP_TOKEN": "",
                    "SLURM_AEDT_POOL_BOOTSTRAP_TOKEN_FILE": str(token_file),
                },
                clear=False,
            ):
                client = AedtPoolHttpClient("http://relay")
        self.assertEqual(client.bootstrap_token, "from-file")


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
