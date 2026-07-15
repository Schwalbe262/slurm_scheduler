from __future__ import annotations

import asyncio
import http.client
import io
import json
import os
import tempfile
import threading
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from slurm_scheduler.aedt_attach_client import (
    AedtPoolHttpClient,
    AedtProjectLease,
    acquire_project_lease,
)
from slurm_scheduler.aedt_pool import (
    UNHEALTHY_ALLOCATION_RECYCLE_REASON,
    AedtPoolRuntime,
    AedtPoolService,
)
from slurm_scheduler.aedt_pool_api import create_aedt_pool_router
from slurm_scheduler.aedt_session_host import (
    AedtSessionHost,
    ControlPlaneClient,
    _install_pyaedt_psutil_cmdline_shim,
)
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
        task_id: int = 0,
        project_name: str | None = None,
    ):
        return self.service.request_lease(
            request_key=key,
            project_name=project_name or f"project-{key}",
            task_id=task_id,
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
        self.assertEqual(config.unhealthy_recycle_grace_seconds, 180)
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

    def test_operator_timeouts_update_liveness_windows(self) -> None:
        config = self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
            unhealthy_recycle_grace_seconds=240,
        )
        self.assertEqual(config.lease_ttl_seconds, 900)
        self.assertEqual(config.session_heartbeat_timeout_seconds, 600)
        self.assertEqual(config.unhealthy_recycle_grace_seconds, 240)
        with self.assertRaises(ValueError):
            self.service.set_operator_timeouts(lease_ttl_seconds=59)
        with self.assertRaises(ValueError):
            self.service.set_operator_timeouts(
                session_heartbeat_timeout_seconds=3601
            )
        with self.assertRaises(ValueError):
            self.service.set_operator_timeouts(
                unhealthy_recycle_grace_seconds=3601
            )
        unchanged = self.service.set_operator_timeouts(lease_ttl_seconds=600)
        self.assertEqual(unchanged.lease_ttl_seconds, 600)
        self.assertEqual(unchanged.session_heartbeat_timeout_seconds, 600)
        self.assertEqual(unchanged.unhealthy_recycle_grace_seconds, 240)

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

        response = endpoint(
            {
                "max_aedt_sessions": 250,
                "min_idle_aedt_sessions": 1,
                "target_project_concurrency": 400,
                "projects_per_aedt": 2,
            }
        )
        self.assertEqual(response["target_project_concurrency"], 400)
        self.assertEqual(response["min_idle_aedt_sessions"], 1)
        self.assertEqual(response["projects_per_aedt"], 2)
        from fastapi import HTTPException

        with self.assertRaises(HTTPException) as raised:
            endpoint(
                {
                    "max_aedt_sessions": 250,
                    "unexpected": 999,
                }
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
            "<h3>Current sessions</h3>",
            "<th>Session ID</th><th>Host task</th><th>Node</th><th>Account</th><th>State</th><th>Started</th><th>Last heartbeat</th><th>Attached FEA</th>",
            '<details id="session-history" class="section-gap">',
            '<tbody id="session-history-rows"></tbody>',
            "<th>Closed</th><th>Failure/quarantine reason</th>",
            'appendEmptyTableRow(body, 8, "No live AEDT pool sessions.")',
            "link.href = `/tasks/${taskId}`",
            "session.allocation_account_name",
            "session.started_at",
            "session.last_heartbeat_at",
            "liveSessionStates.has",
            "historySessionStates.has",
            "lease.task_id",
            "lease.project_name",
            "taskLink(taskId)",
            "session.slots_total",
            "session.free_slot_count",
            "${attachedLeases.length}/${slotsTotal} slots",
            "${freeSlots} free",
            "sessionFailureReason(session)",
            ".slice(0, sessionHistoryLimit)",
            'fetch("/api/aedt-pool/enable"',
        ):
            self.assertIn(required, template)
        for removed in ("node-local-session", "summary.node_local", "renderNodeLocal"):
            self.assertNotIn(removed, template)

        current_table_index = template.index('id="session-rows"')
        history_index = template.index('<details id="session-history"')
        self.assertLess(current_table_index, history_index)
        history_opening_tag = template[
            history_index:template.index(">", history_index) + 1
        ]
        self.assertNotIn(" open", history_opening_tag)

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


class AedtSessionStartRaceTests(AedtPoolTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.allocation_id = self.add_dedicated_allocation()

    def _create_starting_sessions(self, count: int) -> list[dict]:
        self.service.set_operator_limits(
            max_sessions=count,
            min_idle_sessions=count,
            target_projects=0,
        )
        self.make_operational()
        self.service.reconcile(execute=True)
        starts = self.service.starting_sessions()
        self.assertEqual(len(starts), count)
        return starts

    def test_exact_claims_and_idempotent_registration_reject_true_duplicate(self) -> None:
        first, second = self._create_starting_sessions(2)
        first_id = int(first["id"])
        second_id = int(second["id"])

        claimed_first = self.service.claim_start(
            session_id=first_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="host-a",
            bootstrap_token="secret",
        )
        self.assertEqual(int(claimed_first["id"]), first_id)
        replayed_first = self.service.claim_start(
            session_id=first_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="host-a",
            bootstrap_token="secret",
        )
        self.assertEqual(int(replayed_first["id"]), first_id)
        self.assertIsNone(
            self.service.claim_start(
                session_id=first_id,
                allocation_id=self.allocation_id,
                node_name="cpu-01",
                host_id="host-b",
                bootstrap_token="secret",
            )
        )
        claimed_second = self.service.claim_start(
            session_id=second_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="host-b",
            bootstrap_token="secret",
        )
        self.assertEqual(int(claimed_second["id"]), second_id)

        registration_barrier = threading.Barrier(2)

        def register(host_id: str, endpoint: str, process_id: str, token: str):
            registration_barrier.wait()
            try:
                return self.service.register_session(
                    session_id=first_id,
                    host_id=host_id,
                    endpoint=endpoint,
                    process_id=process_id,
                    bootstrap_token="secret",
                    host_token=token,
                )
            except Exception as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            legitimate_future = executor.submit(
                register, "host-a", "cpu-01:50001", "101", "token-a"
            )
            duplicate_future = executor.submit(
                register, "host-b", "cpu-01:50002", "202", "token-b"
            )
            legitimate = legitimate_future.result()
            duplicate = duplicate_future.result()

        self.assertIsInstance(legitimate, tuple)
        self.assertIsInstance(duplicate, ValueError)
        session, token = legitimate
        self.assertEqual(token, "token-a")
        self.assertEqual(session["host_id"], "host-a")
        self.assertEqual(session["endpoint"], "cpu-01:50001")
        self.assertEqual(session["process_id"], "101")

        retried, retried_token = self.service.register_session(
            session_id=first_id,
            host_id="host-a",
            endpoint="cpu-01:50001",
            process_id="101",
            bootstrap_token="secret",
            host_token="token-a",
        )
        self.assertEqual(retried_token, "token-a")
        self.assertEqual(retried["id"], first_id)

        second_session, second_token = self.service.register_session(
            session_id=second_id,
            host_id="host-b",
            endpoint="cpu-01:50002",
            process_id="202",
            bootstrap_token="secret",
            host_token="token-b",
        )
        self.assertEqual(second_token, "token-b")
        self.assertEqual(second_session["host_id"], "host-b")

    def test_claim_gets_full_ack_window_even_when_row_waited_in_queue(self) -> None:
        session = self._create_starting_sessions(1)[0]
        session_id = int(session["id"])
        timeout = self.service.config().session_start_timeout_seconds
        self.clock.advance(timeout - 5)
        claimed = self.service.claim_start(
            session_id=session_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="slow-owner",
            bootstrap_token="secret",
        )
        self.assertEqual(claimed["host_id"], "slow-owner")

        self.clock.advance(10)
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_session(session_id)["state"], "starting")
        registered, token = self.service.register_session(
            session_id=session_id,
            host_id="slow-owner",
            endpoint="cpu-01:50003",
            process_id="303",
            bootstrap_token="secret",
            host_token="slow-token",
        )
        self.assertEqual(token, "slow-token")
        self.assertIn(registered["state"], {"ready", "busy"})

    def test_claim_owner_can_register_after_ack_timeout_without_409(self) -> None:
        session = self._create_starting_sessions(1)[0]
        session_id = int(session["id"])
        self.service.claim_start(
            session_id=session_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="late-owner",
            bootstrap_token="secret",
        )
        self.clock.advance(self.service.config().session_start_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        timed_out = self.service.get_session(session_id)
        self.assertEqual(timed_out["state"], "unhealthy")
        self.assertIn("acknowledgement timed out", timed_out["failure_message"])

        registered, token = self.service.register_session(
            session_id=session_id,
            host_id="late-owner",
            endpoint="cpu-01:50004",
            process_id="404",
            bootstrap_token="secret",
            host_token="late-token",
        )
        self.assertEqual(token, "late-token")
        self.assertIn(registered["state"], {"ready", "busy", "draining"})
        heartbeat = self.service.heartbeat_session(session_id, token)
        self.assertEqual(heartbeat["id"], session_id)
        with self.assertRaisesRegex(ValueError, "not owned"):
            self.service.register_session(
                session_id=session_id,
                host_id="late-duplicate",
                endpoint="cpu-01:50005",
                process_id="405",
                bootstrap_token="secret",
                host_token="duplicate-token",
            )

    def test_late_owner_does_not_cancel_unrelated_allocation_drain(self) -> None:
        session = self._create_starting_sessions(1)[0]
        session_id = int(session["id"])
        self.service.claim_start(
            session_id=session_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="draining-owner",
            bootstrap_token="secret",
        )
        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason="age limit",
            drain_at="CURRENT_TIMESTAMP",
        )
        self.clock.advance(self.service.config().session_start_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        self.assertEqual(
            self.db.get_allocation(self.allocation_id)["drain_reason"], "age limit"
        )

        registered, token = self.service.register_session(
            session_id=session_id,
            host_id="draining-owner",
            endpoint="cpu-01:50006",
            process_id="406",
            bootstrap_token="secret",
            host_token="draining-token",
        )
        self.assertEqual(token, "draining-token")
        self.assertEqual(registered["state"], "draining")
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(allocation["drain_reason"], "age limit")
        self.assertIsNotNone(allocation["drain_at"])
        self.assertTrue(self.service.session_commands(session_id, token)["drain"])


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
        self.service.set_enabled(False)
        self.service.set_operator_limits(projects_per_session=3)
        self.service.set_enabled(True)
        lease_specs = [
            (35447, "simulation_741449"),
            (35448, "simulation_741450"),
            (35449, "simulation_741451"),
        ]
        leases = [
            self.request(
                f"summary-{index}",
                allocation_id=self.allocation_id,
                node="cpu-01",
                task_id=task_id,
                project_name=project_name,
            )
            for index, (task_id, project_name) in enumerate(lease_specs)
        ]
        session, host_token = self.start_one_session(self.allocation_id)
        session_id = int(session["id"])
        released_lease, released_token = leases[-1]
        self.service.release_lease(int(released_lease["id"]), released_token)
        self.service.complete_release(
            session_id,
            host_token,
            int(released_lease["id"]),
            success=True,
        )
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
        self.assertEqual(summary_session["slots_total"], 3)
        self.assertEqual(summary_session["active_lease_count"], 2)
        self.assertEqual(summary_session["free_slot_count"], 1)
        self.assertEqual(
            summary_session["attached_project_names"],
            [project_name for _task_id, project_name in lease_specs[:2]],
        )
        self.assertEqual(
            [
                (item["task_id"], item["project_name"], item["state"])
                for item in summary_session["attached_leases"]
            ],
            [
                (task_id, project_name, "leased")
                for task_id, project_name in lease_specs[:2]
            ],
        )
        self.assertEqual(
            {int(self.service.get_lease(int(lease["id"]))["session_id"]) for lease, _ in leases},
            {session_id},
        )
        self.assertEqual(
            self.service.get_lease(int(released_lease["id"]))["state"],
            "released",
        )
        self.assertEqual(self.service.summary()["session_history"], [])

    def test_summary_separates_live_sessions_from_recent_history(self) -> None:
        live_states = ("starting", "ready", "busy", "draining", "unhealthy")
        history_base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with self.db.connect() as conn:
            for state in live_states:
                conn.execute(
                    """
                    INSERT INTO aedt_sessions (
                        session_key, account_name, node_name, slots_total, state
                    ) VALUES (?, 'a', 'cpu-live', 3, ?)
                    """,
                    (f"live-{state}", state),
                )
            # Insert newest first so ordering by id would produce the wrong result.
            for index in range(34, -1, -1):
                closed_at = (history_base + timedelta(minutes=index)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                conn.execute(
                    """
                    INSERT INTO aedt_sessions (
                        session_key, account_name, node_name, slots_total, state,
                        failure_message, quarantine_reason, created_at, started_at,
                        closed_at, updated_at
                    ) VALUES (?, 'a', 'cpu-history', 3, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"history-{index}",
                        "failed" if index % 2 else "closed",
                        f"failure-{index}",
                        f"solver_timeout-{index}",
                        closed_at,
                        closed_at,
                        closed_at,
                        closed_at,
                    ),
                )

        summary = self.service.summary()

        self.assertEqual(len(summary["sessions"]), len(live_states))
        self.assertEqual(
            {session["state"] for session in summary["sessions"]},
            set(live_states),
        )
        history = summary["session_history"]
        self.assertEqual(len(history), 30)
        self.assertEqual(
            [session["session_key"] for session in history],
            [f"history-{index}" for index in range(34, 4, -1)],
        )
        self.assertEqual({session["state"] for session in history}, {"failed", "closed"})
        self.assertEqual(history[0]["failure_message"], "failure-34")
        self.assertEqual(history[0]["quarantine_reason"], "solver_timeout-34")
        self.assertEqual(history[-1]["session_key"], "history-5")

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

    def test_reaps_stale_unhealthy_session_and_frees_its_allocation_claim(
        self,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO aedt_sessions (
                    session_key, allocation_id, account_name, node_name,
                    slots_total, state, failure_message, last_heartbeat_at
                ) VALUES ('dead-host', ?, 'a', 'cpu-01', 2, 'unhealthy',
                          'lost heartbeat', '2020-01-01 00:00:00')
                """,
                (self.allocation_id,),
            )
            stale_id = int(
                conn.execute(
                    "SELECT id FROM aedt_sessions WHERE session_key = 'dead-host'"
                ).fetchone()[0]
            )
            conn.execute(
                """
                INSERT INTO aedt_project_leases (
                    request_key, project_name, session_id, state, expires_at,
                    client_token_hash
                ) VALUES ('stuck-release', 'p', ?, 'releasing',
                          '2020-01-01 00:00:00', 'x')
                """,
                (stale_id,),
            )
            conn.execute(
                """
                INSERT INTO aedt_sessions (
                    session_key, allocation_id, account_name, node_name,
                    slots_total, state, failure_message
                ) VALUES ('fresh-unhealthy', ?, 'a', 'cpu-01', 2, 'unhealthy',
                          'lost heartbeat')
                """,
                (self.allocation_id,),
            )

        self.service.reconcile(execute=True)

        reaped = self.service.get_session(stale_id, include_secret_hash=False)
        self.assertEqual(reaped["state"], "failed")
        with self.db.connect() as conn:
            lease_state = conn.execute(
                "SELECT state FROM aedt_project_leases WHERE request_key = 'stuck-release'"
            ).fetchone()[0]
            fresh_state = conn.execute(
                "SELECT state FROM aedt_sessions WHERE session_key = 'fresh-unhealthy'"
            ).fetchone()[0]
        self.assertEqual(lease_state, "failed")
        self.assertEqual(fresh_state, "unhealthy")
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(
            allocation["drain_reason"], UNHEALTHY_ALLOCATION_RECYCLE_REASON
        )

    def test_reaped_allocation_is_not_reused_by_same_reconcile_plan(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO aedt_sessions (
                    session_key, allocation_id, account_name, node_name,
                    slots_total, state, failure_message, last_heartbeat_at
                ) VALUES ('stale-only-host', ?, 'a', 'cpu-01', 2, 'unhealthy',
                          'lost heartbeat', '2020-01-01 00:00:00')
                """,
                (self.allocation_id,),
            )

        lease, _token = self.request(
            "replace-stale-host",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )

        self.assertEqual(lease["state"], "queued")
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(self.service.starting_sessions(), [])
        self.assertGreaterEqual(self.service.summary()["plan"]["node_requests"], 1)

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

    def test_release_completion_exact_replay_is_idempotent(self) -> None:
        lease, lease_token = self.request(
            "release-replay", allocation_id=self.allocation_id, node="cpu-01"
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.release_lease(int(lease["id"]), lease_token)

        first = self.service.complete_release(
            int(session["id"]), host_token, int(lease["id"]), success=True
        )
        replay = self.service.complete_release(
            int(session["id"]), host_token, int(lease["id"]), success=True
        )

        self.assertEqual(first["state"], "released")
        self.assertEqual(replay["state"], "released")
        with self.assertRaisesRegex(ValueError, "lease is released"):
            self.service.complete_release(
                int(session["id"]),
                host_token,
                int(lease["id"]),
                success=False,
                failure_message="contradictory replay",
            )

        failed_lease, failed_token = self.request(
            "failed-release-replay",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        self.service.release_lease(int(failed_lease["id"]), failed_token)
        failed = self.service.complete_release(
            int(session["id"]),
            host_token,
            int(failed_lease["id"]),
            success=False,
            failure_message=" close failed ",
        )
        failed_replay = self.service.complete_release(
            int(session["id"]),
            host_token,
            int(failed_lease["id"]),
            success=False,
            failure_message="close failed",
        )
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed_replay["state"], "failed")
        with self.assertRaisesRegex(ValueError, "lease is failed"):
            self.service.complete_release(
                int(session["id"]),
                host_token,
                int(failed_lease["id"]),
                success=False,
                failure_message="different failure",
            )

    def test_valid_heartbeat_recovers_only_heartbeat_timeout_unhealthy(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
        )
        self.request(
            "heartbeat-recovery",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        timed_out = self.service.get_session(int(session["id"]))
        self.assertEqual(timed_out["state"], "unhealthy")
        self.assertEqual(timed_out["failure_message"], "session heartbeat expired")
        self.assertIsNotNone(timed_out["drain_requested_at"])
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "active")
        self.assertIsNone(allocation["drain_at"])

        recovered = self.service.heartbeat_session(int(session["id"]), host_token)

        self.assertEqual(recovered["state"], "busy")
        self.assertEqual(recovered["failure_message"], "")
        self.assertIsNone(recovered["drain_requested_at"])
        self.assertFalse(
            self.service.session_commands(int(session["id"]), host_token)["drain"]
        )
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")

    def test_valid_heartbeat_recovers_idle_session_to_ready(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
        )
        lease, lease_token = self.request(
            "ready-heartbeat-recovery",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.release_lease(int(lease["id"]), lease_token)
        self.service.complete_release(
            int(session["id"]), host_token, int(lease["id"]), success=True
        )
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "ready"
        )
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        timed_out = self.service.get_session(int(session["id"]))
        self.assertEqual(timed_out["state"], "unhealthy")
        self.assertEqual(timed_out["failure_message"], "session heartbeat expired")
        self.assertIsNotNone(timed_out["drain_requested_at"])

        recovered = self.service.heartbeat_session(int(session["id"]), host_token)

        self.assertEqual(recovered["state"], "ready")
        self.assertEqual(recovered["failure_message"], "")
        self.assertIsNone(recovered["drain_requested_at"])
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")

    def test_unhealthy_allocation_recycle_waits_for_configured_grace(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
            unhealthy_recycle_grace_seconds=180,
        )
        self.request(
            "heartbeat-recycle-grace",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        unhealthy = self.service.get_session(int(session["id"]))
        self.assertEqual(unhealthy["failure_message"], "session heartbeat expired")
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")

        self.clock.advance(self.service.config().unhealthy_recycle_grace_seconds)
        self.service.reconcile(execute=True)
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")

        self.clock.advance(1)
        self.service.reconcile(execute=True)
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(
            allocation["drain_reason"], UNHEALTHY_ALLOCATION_RECYCLE_REASON
        )

    def test_late_heartbeat_cancels_only_heartbeat_recycle_drain(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
            unhealthy_recycle_grace_seconds=180,
        )
        self.request(
            "late-heartbeat-recovery",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        self.clock.advance(self.service.config().unhealthy_recycle_grace_seconds + 1)
        self.service.reconcile(execute=True)
        draining = self.db.get_allocation(self.allocation_id)
        self.assertEqual(draining["state"], "draining")
        self.assertEqual(
            draining["drain_reason"], UNHEALTHY_ALLOCATION_RECYCLE_REASON
        )

        recovered = self.service.heartbeat_session(int(session["id"]), host_token)

        self.assertEqual(recovered["state"], "busy")
        self.assertEqual(recovered["failure_message"], "")
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "active")
        self.assertIsNone(allocation["drain_at"])

    def test_long_recycle_grace_delays_reap_until_allocation_recycles(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=3600,
            session_heartbeat_timeout_seconds=600,
            unhealthy_recycle_grace_seconds=3600,
        )
        lease, lease_token = self.request(
            "long-recycle-grace",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.release_lease(int(lease["id"]), lease_token)
        self.service.complete_release(
            int(session["id"]), host_token, int(lease["id"]), success=True
        )
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "unhealthy"
        )

        # Cross the stale-host reap horizon while still inside the configured
        # unhealthy recycle grace.  The recoverable row must remain intact.
        self.clock.advance(
            5 * self.service.config().session_heartbeat_timeout_seconds
        )
        self.service.reconcile(execute=True)
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "unhealthy"
        )
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")

        self.clock.advance(self.service.config().unhealthy_recycle_grace_seconds)
        self.service.reconcile(execute=True)
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "failed"
        )
        self.assertEqual(
            self.db.get_allocation(self.allocation_id)["state"], "draining"
        )

    def test_heartbeat_does_not_revive_session_that_was_already_draining(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
        )
        self.request(
            "draining-heartbeat-timeout",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE aedt_sessions
                SET state = 'draining', failure_message = 'operator drain',
                    drain_requested_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (int(session["id"]),),
            )
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)

        heartbeat = self.service.heartbeat_session(int(session["id"]), host_token)

        self.assertEqual(heartbeat["state"], "unhealthy")
        self.assertIn("already draining", heartbeat["failure_message"])
        self.assertTrue(
            self.service.session_commands(int(session["id"]), host_token)["drain"]
        )

    def test_lease_expiry_after_host_timeout_prevents_heartbeat_recovery(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
            unhealthy_recycle_grace_seconds=3600,
        )
        lease, _lease_token = self.request(
            "lease-expiry-after-host-timeout",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        heartbeat_only = self.service.get_session(int(session["id"]))
        self.assertEqual(heartbeat_only["failure_message"], "session heartbeat expired")
        self.assertEqual(heartbeat_only["quarantine_reason"], "")

        self.clock.advance(
            self.service.config().lease_ttl_seconds
            - self.service.config().session_heartbeat_timeout_seconds
        )
        self.service.reconcile(execute=True)
        quarantined = self.service.get_session(int(session["id"]))
        self.assertEqual(
            self.service.get_lease(int(lease["id"]))["state"], "releasing"
        )
        self.assertEqual(quarantined["state"], "unhealthy")
        self.assertEqual(
            quarantined["quarantine_reason"], "lease_heartbeat_expired"
        )
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "draining")

        heartbeat = self.service.heartbeat_session(int(session["id"]), host_token)

        self.assertEqual(heartbeat["state"], "unhealthy")
        self.assertEqual(heartbeat["quarantine_reason"], "lease_heartbeat_expired")
        self.assertTrue(
            self.service.session_commands(int(session["id"]), host_token)["drain"]
        )

    def test_operator_disable_prevents_heartbeat_timeout_recovery(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=900,
            session_heartbeat_timeout_seconds=600,
        )
        self.request(
            "disabled-heartbeat-recovery",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.clock.advance(self.service.config().session_heartbeat_timeout_seconds + 1)
        self.service.reconcile(execute=True)
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "unhealthy"
        )

        self.service.set_enabled(False)
        heartbeat = self.service.heartbeat_session(int(session["id"]), host_token)

        self.assertEqual(heartbeat["state"], "draining")
        self.assertIn("operator disabled", heartbeat["failure_message"])
        self.assertTrue(
            self.service.session_commands(int(session["id"]), host_token)["drain"]
        )

    def test_default_liveness_windows_cover_five_minute_control_plane_outage(
        self,
    ) -> None:
        config = self.service.config()
        self.assertGreaterEqual(config.lease_ttl_seconds, 360)
        self.assertGreaterEqual(config.session_heartbeat_timeout_seconds, 360)
        lease, lease_token = self.request(
            "five-minute-live-work",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)

        self.clock.advance(300)
        self.service.reconcile(execute=True)

        self.assertIn(
            self.service.get_lease(int(lease["id"]))["state"], {"leased", "active"}
        )
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "busy"
        )
        self.service.heartbeat_lease(int(lease["id"]), lease_token)
        self.service.heartbeat_session(int(session["id"]), host_token)
        self.assertFalse(
            self.service.session_commands(int(session["id"]), host_token)["drain"]
        )

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
        self.clock.advance(self.service.config().lease_ttl_seconds)
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

    def test_client_death_quarantine_does_not_409_legitimate_host_heartbeat(self) -> None:
        lease, lease_token = self.request(
            "death-report", allocation_id=self.allocation_id, node="cpu-01"
        )
        session, host_token = self.start_one_session(self.allocation_id)
        session_id = int(session["id"])
        self.service.report_project_fault(
            int(lease["id"]),
            lease_token,
            fault_kind="aedt_death",
            failure_message="attach raced session registration",
        )
        quarantined = self.service.get_session(session_id)
        self.assertEqual(quarantined["state"], "unhealthy")
        self.assertEqual(quarantined["quarantine_reason"], "aedt_death_reported")
        failure_message = quarantined["failure_message"]
        drain_requested_at = quarantined["drain_requested_at"]
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")

        heartbeat = self.service.heartbeat_session(session_id, host_token)
        self.assertEqual(heartbeat["state"], "unhealthy")
        self.assertEqual(heartbeat["failure_message"], failure_message)
        self.assertEqual(heartbeat["drain_requested_at"], drain_requested_at)
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "draining")
        commands = self.service.session_commands(session_id, host_token)
        self.assertTrue(commands["drain"])
        with self.assertRaises(PermissionError):
            self.service.heartbeat_session(session_id, "other-host-token")

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


class FakeHostLaunchScheduler(FakeRuntimeScheduler):
    def __init__(self) -> None:
        super().__init__()
        self.account = object()
        self.host_tasks: list[dict] = []

    def account_by_name(self, _name: str):
        return self.account

    def reserve_task_on_allocation(self, task, _allocation, _account):
        return task

    def start_background_task_attach(self, task, _allocation, _account) -> None:
        self.host_tasks.append(task)


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
        session = {"id": 17, "allocation_id": 7, "node_name": "cpu-07"}
        self.assertTrue(runtime.host_launch_configured)
        first = runtime._host_command(session)
        self.assertIn("http://gate2:18790", first)
        self.assertIn("--session-id 17", first)
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
            {"id": 18, "allocation_id": 8, "node_name": "cpu-08"}
        )
        self.assertIn("http://scheduler-local:8000", command)
        self.assertIn("--session-id 18", command)
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

    def test_relay_recovery_skips_session_host_starts_for_one_tick(self) -> None:
        self.make_operational()
        runtime = AedtPoolRuntime(
            self.service,
            FakeRuntimeScheduler(),
            interval_seconds=30,
            require_published_control_plane_url=True,
        )

        self.assertFalse(runtime.tick()["control_plane_ready"])
        self.service.set_control_plane_url("http://gate2:18790")
        with patch.object(runtime, "_ensure_session_hosts", return_value=2) as ensure:
            recovery = runtime.tick()
            settled = runtime.tick()
            self.service.set_control_plane_url("")
            runtime.tick()
            self.service.set_control_plane_url("http://gate2:18790")
            second_recovery = runtime.tick()

        self.assertEqual(recovery["host_tasks_started"], 0)
        self.assertEqual(settled["host_tasks_started"], 2)
        self.assertEqual(second_recovery["host_tasks_started"], 0)
        ensure.assert_called_once()

    def test_same_allocation_session_host_launches_are_staggered(self) -> None:
        self.service.set_operator_limits(
            max_sessions=3,
            min_idle_sessions=3,
            target_projects=0,
        )
        self.add_dedicated_allocation()
        self.make_operational()
        self.service.reconcile(execute=True)
        scheduler = FakeHostLaunchScheduler()
        runtime = AedtPoolRuntime(
            self.service,
            scheduler,
            interval_seconds=30,
            scheduler_url="http://scheduler:8000",
            host_remote_cwd="/work/aedt",
            host_bootstrap_token_file="/shared/aedt-token",
            host_launch_stagger_seconds=15,
        )

        started = runtime._ensure_session_hosts(self.service.config())

        self.assertEqual(started, 3)
        commands = [task["command"] for task in scheduler.host_tasks]
        self.assertFalse(commands[0].startswith("sleep "))
        self.assertTrue(commands[1].startswith("sleep 15 && exec "))
        self.assertTrue(commands[2].startswith("sleep 30 && exec "))
        self.assertTrue(all("--session-id" in command for command in commands))
        self.assertTrue(
            all("control-plane-outage" not in command for command in commands)
        )

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


class TransientLeaseHeartbeatHttp:
    def __init__(self) -> None:
        self.calls = 0

    def request(self, method, path, payload=None, lease_token=""):
        self.calls += 1
        if self.calls < 3:
            raise urllib.error.HTTPError(
                path, 503, "Service Unavailable", None, None
            )
        return {"id": 1, "state": "active", "endpoint": "cpu-01:50001"}


class AttachClientTests(unittest.TestCase):
    def test_lease_heartbeat_retries_transient_5xx(self) -> None:
        http = TransientLeaseHeartbeatHttp()
        lease = AedtProjectLease(http, 1, "token", "p")
        with (
            patch("slurm_scheduler.aedt_attach_client.time.sleep") as sleep,
            patch(
                "slurm_scheduler.aedt_attach_client.random.uniform", return_value=0
            ),
        ):
            heartbeat = lease.heartbeat()

        self.assertEqual(heartbeat["state"], "active")
        self.assertEqual(http.calls, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.5, 1.0])

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
    # An empty fake PID keeps bounded cleanup from ever signalling an unrelated
    # real process that happens to own a low numeric PID on the test host.
    aedt_process_id = ""

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


class RegistrationRaceControlPlane(FakeHostControlPlane):
    def __init__(self, events: list[str], *, conflicts: int) -> None:
        super().__init__(events)
        self.conflicts = conflicts
        self.registration_tokens: list[str] = []

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("/register"):
            self.registration_tokens.append(host_token)
            self.events.append(f"register-{len(self.registration_tokens)}")
            if len(self.registration_tokens) <= self.conflicts:
                raise urllib.error.HTTPError(path, 409, "Conflict", None, None)
        if path.endswith("/start-failed"):
            self.events.append("start-failed")
            return {}
        return super().request(method, path, payload, host_token=host_token)


class ClaimRaceControlPlane(FakeHostControlPlane):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.claim_payloads: list[dict] = []

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("claim-start"):
            self.claim_payloads.append(dict(payload or {}))
            self.events.append(f"claim-{len(self.claim_payloads)}")
            if len(self.claim_payloads) == 1:
                raise urllib.error.URLError("claim response lost after commit")
            return {"session": {"id": int(payload["session_id"])}}
        return super().request(method, path, payload, host_token=host_token)


class RetryClock:
    def __init__(self) -> None:
        self.seconds = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.seconds += seconds


class FiveMinuteOutageControlPlane(FakeHostControlPlane):
    def __init__(self, events: list[str], clock: RetryClock) -> None:
        super().__init__(events)
        self.clock = clock
        self.heartbeat_attempts = 0

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("/heartbeat"):
            self.heartbeat_attempts += 1
            if self.clock.seconds < 300:
                # Model the real client's 30-second request timeout, including
                # a final failed call that straddles relay recovery at t=300.
                self.clock.seconds += 30
                self.events.append("heartbeat-503")
                raise urllib.error.HTTPError(
                    path, 503, "Service Unavailable", None, None
                )
            self.events.append("heartbeat-recovered")
        return super().request(method, path, payload, host_token=host_token)


class TerminalRegistrationControlPlane(FakeHostControlPlane):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.registration_attempts = 0

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("/register"):
            self.registration_attempts += 1
            body = io.BytesIO(
                json.dumps(
                    {"detail": "session start claim is not owned by this host"}
                ).encode("utf-8")
            )
            raise urllib.error.HTTPError(path, 409, "Conflict", None, body)
        if path.endswith("/start-failed"):
            self.events.append("start-failed")
            return {}
        return super().request(method, path, payload, host_token=host_token)


class TerminalHeartbeatControlPlane(FakeHostControlPlane):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.heartbeat_attempts = 0

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("/heartbeat"):
            self.heartbeat_attempts += 1
            raise urllib.error.HTTPError(path, 403, "Forbidden", None, None)
        return super().request(method, path, payload, host_token=host_token)


class RemoteDisconnectRegistrationControlPlane(FakeHostControlPlane):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.registration_attempts = 0

    def request(self, method, path, payload=None, host_token=""):
        if path.endswith("/register"):
            self.registration_attempts += 1
            self.events.append(f"register-{self.registration_attempts}")
            if self.registration_attempts == 1:
                raise http.client.RemoteDisconnected(
                    "Remote end closed connection without response"
                )
        return super().request(method, path, payload, host_token=host_token)


class SessionHostTests(unittest.TestCase):
    def test_psutil_shim_sanitizes_none_cmdline_and_is_idempotent(self) -> None:
        calls: list[tuple[tuple, dict]] = []
        processes = [
            SimpleNamespace(info={"pid": 10, "cmdline": None}),
            SimpleNamespace(info={"pid": 11, "cmdline": ["ansysedt", "-ng"]}),
        ]

        def process_iter(*args, **kwargs):
            calls.append((args, kwargs))
            return iter(processes)

        fake_psutil = SimpleNamespace(process_iter=process_iter)
        _install_pyaedt_psutil_cmdline_shim(fake_psutil)
        installed = fake_psutil.process_iter
        _install_pyaedt_psutil_cmdline_shim(fake_psutil)

        yielded = list(fake_psutil.process_iter(attrs=("pid", "cmdline")))

        self.assertIs(fake_psutil.process_iter, installed)
        self.assertEqual(yielded, processes)
        self.assertEqual(processes[0].info["cmdline"], [])
        self.assertEqual(processes[1].info["cmdline"], ["ansysedt", "-ng"])
        self.assertEqual(calls, [((), {"attrs": ("pid", "cmdline")})])

    def test_failed_launch_cleanup_matches_exact_current_user_port(self) -> None:
        matching = SimpleNamespace(
            info={
                "pid": 3824121,
                "name": "ansysedt",
                "username": "cluster-user",
                "cmdline": ["/ansys/ansysedt", "-grpcsrv", "44773", "-ng"],
                "create_time": 105.0,
            }
        )
        wrong_port = SimpleNamespace(
            info={
                **matching.info,
                "pid": 3824122,
                "cmdline": ["/ansys/ansysedt", "-grpcsrv", "44774", "-ng"],
            }
        )
        processes = [matching, wrong_port]
        fake_psutil = SimpleNamespace(
            Process=lambda _pid: SimpleNamespace(username=lambda: "cluster-user"),
            process_iter=lambda **_kwargs: iter(processes),
        )

        with patch.dict("sys.modules", {"psutil": fake_psutil}):
            self.assertEqual(
                AedtSessionHost._owned_desktop_pid_on_port(44773, 100.0),
                "3824121",
            )
            processes.append(
                SimpleNamespace(info={**matching.info, "pid": 3824123})
            )
            self.assertEqual(
                AedtSessionHost._owned_desktop_pid_on_port(44773, 100.0), ""
            )

    def test_desktop_launch_failure_recovers_by_explicit_port_attach(self) -> None:
        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        calls: list[dict] = []
        recovered = SimpleNamespace(
            port=44773, aedt_process_id="3824121", odesktop=object()
        )

        def create_desktop(*, new_desktop: bool, port: int):
            calls.append({"new_desktop": new_desktop, "port": port})
            if new_desktop:
                raise RuntimeError("session initialization failed")
            return recovered

        host._create_desktop = create_desktop
        with (
            patch.object(host, "_find_free_desktop_port", return_value=44773),
            patch.object(host, "_desktop_port_is_listening", return_value=True),
            patch(
                "slurm_scheduler.aedt_session_host._install_pyaedt_psutil_cmdline_shim"
            ) as shim,
            patch("slurm_scheduler.aedt_session_host.time.sleep") as sleep,
        ):
            desktop = host._start_desktop()

        self.assertIs(desktop, recovered)
        self.assertEqual(
            calls,
            [
                {"new_desktop": True, "port": 44773},
                {"new_desktop": False, "port": 44773},
            ],
        )
        shim.assert_called_once_with()
        sleep.assert_not_called()

    def test_desktop_launch_retries_two_full_cycles_before_success(self) -> None:
        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        calls: list[dict] = []

        def create_desktop(*, new_desktop: bool, port: int):
            calls.append({"new_desktop": new_desktop, "port": port})
            if len(calls) < 3:
                raise RuntimeError(f"launch call {len(calls)} failed")
            return SimpleNamespace(
                port=port, aedt_process_id="3824121", odesktop=object()
            )

        host._create_desktop = create_desktop
        with (
            patch.object(
                host,
                "_find_free_desktop_port",
                side_effect=[44001, 44002, 44003],
            ),
            patch.object(host, "_desktop_port_is_listening", return_value=False),
            patch.object(host, "_cleanup_failed_desktop_launch") as cleanup,
            patch(
                "slurm_scheduler.aedt_session_host._install_pyaedt_psutil_cmdline_shim"
            ),
            patch("slurm_scheduler.aedt_session_host.time.sleep") as sleep,
        ):
            desktop = host._start_desktop()

        self.assertEqual(desktop.port, 44003)
        self.assertEqual(
            [(item["new_desktop"], item["port"]) for item in calls],
            [
                (True, 44001),
                (True, 44002),
                (True, 44003),
            ],
        )
        self.assertEqual(cleanup.call_count, 2)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list], [2.0, 2.0]
        )

    def test_registration_remote_disconnect_retries_without_closing_desktop(
        self,
    ) -> None:
        events: list[str] = []
        control = RemoteDisconnectRegistrationControlPlane(events)
        host = AedtSessionHost(control, allocation_id=1, node_name="cpu-01")
        host._start_desktop = lambda: FakeDesktop(events)

        with (
            patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None),
            patch("slurm_scheduler.aedt_session_host.random.uniform", return_value=0),
        ):
            self.assertEqual(host.run(), 2)

        self.assertEqual(control.registration_attempts, 2)
        self.assertLess(events.index("register-2"), events.index("desktop-close"))

    def test_transient_5xx_outage_survives_five_minutes_with_desktop(self) -> None:
        events: list[str] = []
        clock = RetryClock()
        control = FiveMinuteOutageControlPlane(events, clock)
        host = AedtSessionHost(
            control,
            allocation_id=1,
            node_name="cpu-01",
            heartbeat_seconds=5,
        )
        host._start_desktop = lambda: FakeDesktop(events)

        with (
            patch(
                "slurm_scheduler.aedt_session_host.time.monotonic",
                side_effect=clock.monotonic,
            ),
            patch(
                "slurm_scheduler.aedt_session_host.time.sleep",
                side_effect=clock.sleep,
            ),
            patch(
                "slurm_scheduler.aedt_session_host.random.uniform", return_value=0
            ) as jitter,
        ):
            self.assertEqual(host.run(), 2)

        self.assertGreaterEqual(host.control_plane_outage_seconds, 300)
        self.assertGreater(control.heartbeat_attempts, 1)
        self.assertGreaterEqual(clock.seconds, 300)
        self.assertEqual(clock.sleeps[:6], [0.5, 1, 2, 4, 8, 10])
        jitter.assert_called()
        self.assertLess(
            events.index("heartbeat-recovered"), events.index("desktop-close")
        )

    def test_control_plane_outage_budget_must_be_finite(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be finite"):
            AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                control_plane_outage_seconds=float("inf"),
            )

    def test_terminal_registration_409_exits_immediately(self) -> None:
        events: list[str] = []
        control = TerminalRegistrationControlPlane(events)
        host = AedtSessionHost(control, allocation_id=1, node_name="cpu-01")
        host._start_desktop = lambda: FakeDesktop(events)

        with patch(
            "slurm_scheduler.aedt_session_host.time.sleep", return_value=None
        ) as sleep:
            self.assertEqual(host.run(), 1)

        self.assertEqual(control.registration_attempts, 1)
        sleep.assert_not_called()
        self.assertIn("desktop-close", events)

    def test_terminal_403_heartbeat_exits_without_retry(self) -> None:
        events: list[str] = []
        control = TerminalHeartbeatControlPlane(events)
        host = AedtSessionHost(control, allocation_id=1, node_name="cpu-01")
        host._start_desktop = lambda: FakeDesktop(events)

        with patch(
            "slurm_scheduler.aedt_session_host.time.sleep", return_value=None
        ) as sleep:
            self.assertEqual(host.run(), 1)

        self.assertEqual(control.heartbeat_attempts, 1)
        self.assertEqual(control.command_count, 0)
        sleep.assert_not_called()
        self.assertIn("desktop-close", events)

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

    def test_registration_409_is_retried_without_closing_desktop(self) -> None:
        events: list[str] = []
        control = RegistrationRaceControlPlane(events, conflicts=1)
        host = AedtSessionHost(
            control,
            allocation_id=1,
            node_name="cpu-01",
            session_id=41,
            heartbeat_seconds=5,
        )
        host._start_desktop = lambda: FakeDesktop(events)
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 2)

        self.assertEqual(len(control.registration_tokens), 2)
        self.assertTrue(control.registration_tokens[0])
        self.assertEqual(control.registration_tokens[0], control.registration_tokens[1])
        self.assertLess(events.index("register-2"), events.index("desktop-close"))

    def test_lost_claim_response_is_retried_before_desktop_starts(self) -> None:
        events: list[str] = []
        control = ClaimRaceControlPlane(events)
        host = AedtSessionHost(
            control,
            allocation_id=1,
            node_name="cpu-01",
            session_id=43,
            heartbeat_seconds=5,
        )

        def start_desktop():
            events.append("desktop-start")
            return FakeDesktop(events)

        host._start_desktop = start_desktop
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 2)

        self.assertEqual(len(control.claim_payloads), 2)
        self.assertEqual(control.claim_payloads[0], control.claim_payloads[1])
        self.assertEqual(control.claim_payloads[0]["session_id"], 43)
        self.assertLess(events.index("claim-2"), events.index("desktop-start"))

    def test_registration_conflict_closes_desktop_only_after_retry_exhaustion(self) -> None:
        events: list[str] = []
        control = RegistrationRaceControlPlane(events, conflicts=3)
        host = AedtSessionHost(
            control,
            allocation_id=1,
            node_name="cpu-01",
            session_id=42,
            heartbeat_seconds=5,
        )
        host._start_desktop = lambda: FakeDesktop(events)
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 1)

        self.assertEqual(len(control.registration_tokens), 3)
        self.assertEqual(len(set(control.registration_tokens)), 1)
        self.assertLess(events.index("register-3"), events.index("desktop-close"))
        self.assertIn("start-failed", events)


if __name__ == "__main__":
    unittest.main()
