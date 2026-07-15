from __future__ import annotations

import asyncio
import http.client
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from slurm_scheduler.aedt_attach_client import (
    AedtLeaseError,
    AedtPoolHttpClient,
    AedtProjectLease,
    _keepalive_delay,
    acquire_project_lease,
)
from slurm_scheduler.aedt_automation_lock import (
    SessionAutomationLock,
    create_automation_lock_file,
)
from slurm_scheduler.aedt_pool import (
    ALLOCATION_AGE_ROTATION_REASON,
    FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON,
    UNHEALTHY_ALLOCATION_RECYCLE_REASON,
    AedtPoolRuntime,
    AedtPoolService,
    _derive_placement_group,
)
from slurm_scheduler.aedt_pool_api import create_aedt_pool_router
from slurm_scheduler.aedt_session_host import (
    AedtSessionHost,
    ControlPlaneUnavailable,
    ControlPlaneClient,
    EXPECTED_SESSION_PROFILE_JSON,
    LEGACY_DSO_PROFILE,
    NATIVE_PROBE_DEFERRED_BUSY,
    NATIVE_PROBE_FAILED,
    NATIVE_PROBE_OK,
    SUPPORTED_DSO_PROFILE,
    _install_pyaedt_psutil_cmdline_shim,
    _load_pyaedt_dso_template,
    _render_dso_configuration,
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

VALID_PYAEDT_DSO_TEMPLATE = """$begin 'Configs'
    $begin 'Configs'
        $begin 'DSOConfig'
            ConfigName='pyaedt_config'
            DesignType='HFSS'
            $begin 'DSOMachineList'
                $begin 'DSOMachineInfo'
                    MachineName='localhost'
                    NumEngines=1
                    NumCores=4
                    IsEnabled=true
                    RAMPercent=90
                    NumJobCores=0
                    NumGPUs=0
                $end 'DSOMachineInfo'
            $end 'DSOMachineList'
            UseAutoSettings=true
            NumVariationsToDistribute=1
            $begin 'DSOJobDistributionInfo'
                AllowedDistributionTypes[9: 'Variations', 'Frequencies', 'Mesh Assembly','Mesher', 'Transient Excitations', 'Domain Solver', 'Solver', 'Iterative Solver', 'Direct Solver']
                Enable2LevelDistribution=false
                NumL1Engines=1
                UseDefaultsForDistributionTypes=false
                Context()
            $end 'DSOJobDistributionInfo'
            $begin 'DSOMachineOptionsInfo'
                MenuValues()
                IntValues()
                BoolValues(AllowOffCore=true)
                DoubleValues()
            $end 'DSOMachineOptionsInfo'
        $end 'DSOConfig'
    $end 'Configs'
$end 'Configs'
"""


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
        self.service = AedtPoolService(
            self.db,
            bootstrap_token="secret",
            lease_client_token="client-secret",
            now=self.clock.now,
        )
        self.service.init()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def add_dedicated_allocation(self, *, cpus: int = 64, node: str = "cpu-01") -> int:
        now = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
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
            created_at=now,
            started_at=now,
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
        placement_group: str | None = None,
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
            placement_group=placement_group,
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
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            bootstrap_token="secret",
        )


class AedtPoolGateTests(AedtPoolTestCase):
    def test_placement_group_derivation_and_explicit_override(self) -> None:
        cases = {
            "mft-pending-39812-stage": "mft",
            "simulation_745147_2759990": "mft",
            "IPMSM_v2_stage3_001": "ipmsm",
            "motor-prototype-ipmsm-stage": "ipmsm",
            "Alpha42-stage-7": "alpha42",
        }
        for project_name, expected in cases.items():
            with self.subTest(project_name=project_name):
                self.assertEqual(_derive_placement_group(project_name), expected)

        lease, _token = self.request(
            "explicit-placement-group",
            project_name="simulation_745147_2759990",
            placement_group="motor-team",
        )
        self.assertEqual(lease["placement_group"], "motor-team")

    def test_requested_250_500_is_staged_but_disabled(self) -> None:
        config = self.service.config()
        self.assertEqual(config.max_sessions, 250)
        self.assertEqual(config.min_idle_sessions, 0)
        self.assertEqual(config.target_projects, 500)
        self.assertEqual(config.unhealthy_recycle_grace_seconds, 180)
        self.assertEqual(config.idle_ttl_seconds, 3600)
        self.assertEqual(config.allocation_max_age_seconds, 158400)
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
            idle_ttl_seconds=7200,
            allocation_max_age_seconds=86400,
        )
        self.assertEqual(config.lease_ttl_seconds, 900)
        self.assertEqual(config.session_heartbeat_timeout_seconds, 600)
        self.assertEqual(config.unhealthy_recycle_grace_seconds, 240)
        self.assertEqual(config.idle_ttl_seconds, 7200)
        self.assertEqual(config.allocation_max_age_seconds, 86400)
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
        for invalid_idle_ttl in (59, True, 86401):
            with self.subTest(invalid_idle_ttl=invalid_idle_ttl):
                with self.assertRaisesRegex(ValueError, "between 60 and 86400"):
                    self.service.set_operator_timeouts(
                        idle_ttl_seconds=invalid_idle_ttl
                    )
        for invalid_max_age in (-1, True, 172801):
            with self.subTest(invalid_max_age=invalid_max_age):
                with self.assertRaisesRegex(ValueError, "between 0 and 172800"):
                    self.service.set_operator_timeouts(
                        allocation_max_age_seconds=invalid_max_age
                    )
        unchanged = self.service.set_operator_timeouts(lease_ttl_seconds=600)
        self.assertEqual(unchanged.lease_ttl_seconds, 600)
        self.assertEqual(unchanged.session_heartbeat_timeout_seconds, 600)
        self.assertEqual(unchanged.unhealthy_recycle_grace_seconds, 240)
        self.assertEqual(unchanged.idle_ttl_seconds, 7200)
        self.assertEqual(unchanged.allocation_max_age_seconds, 86400)

        disabled = self.service.set_operator_timeouts(
            allocation_max_age_seconds=0
        )
        self.assertEqual(disabled.allocation_max_age_seconds, 0)

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

    def test_api_concurrent_simulations_derives_project_and_session_limits(self) -> None:
        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool/config"
        )

        response = endpoint({"concurrent_simulations": 401})

        self.assertEqual(response["concurrent_simulations"], 401)
        self.assertEqual(response["target_project_concurrency"], 401)
        self.assertEqual(response["max_aedt_sessions"], 201)
        config = self.service.config()
        self.assertEqual(config.target_projects, 401)
        self.assertEqual(config.max_sessions, 201)

    def test_api_operator_surface_accepts_durable_rotation_and_idle_timeouts(
        self,
    ) -> None:
        from fastapi import HTTPException

        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool/config"
        )

        response = endpoint(
            {
                "idle_ttl_seconds": 7200,
                "allocation_max_age_seconds": 86400,
            }
        )

        self.assertEqual(response["idle_ttl_seconds"], 7200)
        self.assertEqual(response["allocation_max_age_seconds"], 86400)
        reloaded = AedtPoolService(self.db, bootstrap_token="secret")
        self.assertEqual(reloaded.config().idle_ttl_seconds, 7200)
        self.assertEqual(reloaded.config().allocation_max_age_seconds, 86400)

        invalid_cases = (
            ({"idle_ttl_seconds": 59}, "between 60 and 86400"),
            ({"idle_ttl_seconds": 86401}, "between 60 and 86400"),
            ({"allocation_max_age_seconds": -1}, "between 0 and 172800"),
            ({"allocation_max_age_seconds": 172801}, "between 0 and 172800"),
        )
        for payload, message in invalid_cases:
            with self.subTest(payload=payload):
                with self.assertRaises(HTTPException) as raised:
                    endpoint(payload)
                self.assertEqual(raised.exception.status_code, 422)
                self.assertIn(message, raised.exception.detail)

        disabled = endpoint({"allocation_max_age_seconds": 0})
        self.assertEqual(disabled["allocation_max_age_seconds"], 0)

    def test_api_concurrent_simulations_uses_same_request_project_slots(self) -> None:
        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool/config"
        )

        response = endpoint(
            {
                "concurrent_simulations": 1650,
                "projects_per_aedt": 3,
            }
        )

        self.assertEqual(response["concurrent_simulations"], 1650)
        self.assertEqual(response["target_project_concurrency"], 1650)
        self.assertEqual(response["max_aedt_sessions"], 550)
        self.assertEqual(response["projects_per_aedt"], 3)

    def test_api_concurrent_simulations_rejects_derived_session_overflow(self) -> None:
        from fastapi import HTTPException

        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool/config"
        )

        with self.assertRaises(HTTPException) as raised:
            endpoint({"concurrent_simulations": 1101})

        self.assertEqual(raised.exception.status_code, 422)
        self.assertIn("maximum is 550", raised.exception.detail)

        for invalid_value in (-1, True, 1651):
            with self.subTest(invalid_value=invalid_value):
                with self.assertRaises(HTTPException) as invalid:
                    endpoint({"concurrent_simulations": invalid_value})
                self.assertEqual(invalid.exception.status_code, 422)
                self.assertIn("integer between 0 and 1650", invalid.exception.detail)

    def test_api_concurrent_simulations_rejects_derived_key_mixing(self) -> None:
        from fastapi import HTTPException

        router = create_aedt_pool_router(self.service)
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/aedt-pool/config"
        )

        for explicit_key in (
            "max_aedt_sessions",
            "target_project_concurrency",
        ):
            with self.subTest(explicit_key=explicit_key):
                with self.assertRaises(HTTPException) as raised:
                    endpoint(
                        {
                            "concurrent_simulations": 400,
                            explicit_key: 200,
                        }
                    )
                self.assertEqual(raised.exception.status_code, 422)
                self.assertIn("cannot be combined", raised.exception.detail)

    def test_mutating_routes_separate_operator_client_and_lease_authority(self) -> None:
        from fastapi import HTTPException

        router = create_aedt_pool_router(self.service)
        mutating_routes = [
            route
            for route in router.routes
            if set(getattr(route, "methods", set())) & {"POST", "PATCH", "PUT", "DELETE"}
        ]
        self.assertEqual(len(mutating_routes), 20)
        bootstrap_routes = []
        client_routes = []
        lease_scoped_routes = []
        for route in mutating_routes:
            route_guards = [
                dependency.call
                for dependency in route.dependant.dependencies
                if getattr(dependency.call, "__name__", "") == "require_bootstrap"
            ]
            client_guards = [
                dependency.call
                for dependency in route.dependant.dependencies
                if getattr(dependency.call, "__name__", "")
                == "require_lease_client"
            ]
            if route.path == "/api/aedt-pool/leases":
                self.assertEqual(len(client_guards), 1)
                self.assertEqual(route_guards, [])
                client_routes.append(route)
            elif route.path.startswith("/api/aedt-pool/leases/"):
                self.assertEqual(route_guards, [])
                self.assertEqual(client_guards, [])
                lease_scoped_routes.append(route)
            else:
                self.assertEqual(len(route_guards), 1)
                self.assertEqual(client_guards, [])
                bootstrap_routes.append(route)

        self.assertEqual(len(bootstrap_routes), 11)
        self.assertEqual(len(client_routes), 1)
        self.assertEqual(len(lease_scoped_routes), 8)
        guard = next(
            dependency.call
            for dependency in bootstrap_routes[0].dependant.dependencies
            if getattr(dependency.call, "__name__", "") == "require_bootstrap"
        )
        for token in ("", "wrong"):
            with self.assertRaises(HTTPException) as raised:
                guard(token)
            self.assertEqual(raised.exception.status_code, 403)
        guard("secret")
        client_guard = next(
            dependency.call
            for dependency in client_routes[0].dependant.dependencies
            if getattr(dependency.call, "__name__", "") == "require_lease_client"
        )
        for token in ("", "secret", "wrong"):
            with self.assertRaises(HTTPException) as raised:
                client_guard(token)
            self.assertEqual(raised.exception.status_code, 403)
        client_guard("client-secret")

    def test_projects_per_aedt_change_requires_disabled_drained_pool(self) -> None:
        self.make_operational()
        with self.assertRaisesRegex(ValueError, "disable and fully drain"):
            self.service.set_operator_limits(projects_per_session=1)

    def test_web_ui_exposes_limits_usage_leases_and_fail_closed_gates(self) -> None:
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "aedt_pool.html"
        ).read_text(encoding="utf-8")
        for required in (
            'id="concurrent-simulations-form"',
            'id="concurrent-simulations" name="concurrent_simulations" type="number" min="0" max="1650"',
            'id="derived-aedt-sessions"',
            '<details id="advanced-operator-limits" class="section-gap">',
            "동시 시뮬레이션 수",
            "Math.ceil(simulations / projectsPerAedt)",
            "localStorage.getItem(bootstrapTokenStorageKey)",
            "localStorage.setItem(",
            "bootstrap token 오류",
            "HTTP ${response.status}",
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
                    session_profile=EXPECTED_SESSION_PROFILE_JSON,
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
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
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
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
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
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            bootstrap_token="secret",
            host_token="slow-token",
        )
        self.assertEqual(token, "slow-token")
        self.assertIn(registered["state"], {"ready", "busy"})

    def test_start_failure_persists_exact_artifact_and_runtime_evidence(self) -> None:
        session = self._create_starting_sessions(1)[0]
        session_id = int(session["id"])
        self.service.claim_start(
            session_id=session_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
            host_id="evidence-owner",
            bootstrap_token="secret",
        )
        failed = self.service.fail_session_start(
            session_id=session_id,
            host_id="evidence-owner",
            bootstrap_token="secret",
            failure_message="DSO readback mismatch",
            artifact_dir="/gpfs/aedt/session-1",
            error_log_path="/gpfs/aedt/session-1/pyaedt.log",
            journal_path="/gpfs/aedt/session-1/session-events.jsonl",
            runtime_metadata={
                "python_executable": "/opt/conda/pyaedt2026v1/bin/python",
                "aedt_version": "2025.2",
                "startup_failure": "DSO readback mismatch",
            },
        )
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["artifact_dir"], "/gpfs/aedt/session-1")
        self.assertTrue(failed["error_log_path"].endswith("pyaedt.log"))
        self.assertTrue(failed["journal_path"].endswith("session-events.jsonl"))
        metadata = json.loads(failed["runtime_metadata_json"])
        self.assertEqual(metadata["aedt_version"], "2025.2")
        self.assertIn("DSO readback mismatch", failed["failure_message"])

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
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
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
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
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
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
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
    def test_protocol_v2_rejects_blank_or_drifted_runtime_contract(self) -> None:
        with self.assertRaisesRegex(ValueError, "workspace_path"):
            self.service.request_lease(
                request_key="v2-blank-workspace",
                project_name="mft-v2-blank-workspace",
                workload_family="mft",
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
                protocol_version=2,
            )
        with self.assertRaisesRegex(ValueError, "workload_family"):
            self.service.request_lease(
                request_key="v2-blank-family",
                project_name="mft-v2-blank-family",
                workload_family="",
                workspace_path="/shared/v2",
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
                protocol_version=2,
            )
        drifted = json.loads(EXPECTED_SESSION_PROFILE_JSON)
        drifted["aedt_version"] = "2026.1"
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.service.request_lease(
                request_key="v2-profile-drift",
                project_name="mft-v2-profile-drift",
                workload_family="mft",
                workspace_path="/shared/v2",
                session_profile=drifted,
                protocol_version=2,
            )

    def test_shared_mft_ipmsm_admission_requires_dedicated_mixed_validation(
        self,
    ) -> None:
        request = {
            "request_key": "mixed-gated",
            "project_name": "mft-ipmsm-mixed",
            "workload_family": "mft",
            "workspace_path": "/shared/mixed/mft",
            "session_profile": EXPECTED_SESSION_PROFILE_JSON,
            "protocol_version": 2,
            "isolation_policy": "shared_if_compatible",
        }
        with self.assertRaisesRegex(ValueError, "mixed MFT/IPMSM"):
            self.service.request_lease(**request)

        mixed_validation = self.service.record_validation(
            {
                **PASSING_EVIDENCE,
                "mixed_mft_ipmsm_isolation_passed": True,
                "mixed_validation_artifact": "mixed-mft-ipmsm.json",
            }
        )
        self.assertEqual(mixed_validation["status"], "passed")
        self.assertEqual(
            mixed_validation["mixed_mft_ipmsm_isolation_passed"], 1
        )
        lease, _token = self.service.request_lease(**request)
        self.assertEqual(lease["isolation_policy"], "shared_if_compatible")

    def test_explicit_1800_second_admission_stays_live_past_600_with_heartbeats(
        self,
    ) -> None:
        lease, token = self.service.request_lease(
            request_key="v2-long-admission",
            project_name="mft-v2-long-admission",
            workload_family="mft",
            workspace_path="/shared/v2-long",
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            protocol_version=2,
            admission_timeout_seconds=1800,
        )
        deadline = datetime.strptime(
            lease["client_deadline_at"], "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        self.assertEqual(int((deadline - self.clock.now()).total_seconds()), 1800)

        self.clock.advance(500)
        self.assertEqual(
            self.service.heartbeat_lease(int(lease["id"]), token)["state"],
            "queued",
        )
        self.clock.advance(201)
        self.assertEqual(
            self.service.heartbeat_lease(int(lease["id"]), token)["state"],
            "queued",
        )

    def test_terminal_cancel_cannot_be_resurrected_by_racing_heartbeat(self) -> None:
        lease, token = self.request("atomic-cancel-heartbeat")
        barrier = threading.Barrier(2)

        def cancel():
            barrier.wait()
            return self.service.cancel_lease(int(lease["id"]), token)

        def heartbeat():
            barrier.wait()
            try:
                return self.service.heartbeat_lease(int(lease["id"]), token)
            except ValueError as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            cancelled_future = executor.submit(cancel)
            heartbeat_future = executor.submit(heartbeat)
            cancelled_future.result()
            heartbeat_future.result()

        terminal = self.service.get_lease(int(lease["id"]))
        self.assertEqual(terminal["state"], "cancelled")
        with self.assertRaisesRegex(ValueError, "cancelled"):
            self.service.heartbeat_lease(int(lease["id"]), token)
        with self.assertRaisesRegex(ValueError, "cancelled"):
            self.service.bind_lease_project_name(
                int(lease["id"]), token, "resurrected-project"
            )
        self.assertEqual(
            self.service.cancel_lease(int(lease["id"]), token)["state"],
            "cancelled",
        )

    def test_500_concurrent_heartbeats_finish_inside_heartbeat_window(self) -> None:
        lease, token = self.request("heartbeat-contention")
        durations: list[float] = []
        durations_lock = threading.Lock()

        def heartbeat(_index: int) -> str:
            started = time.monotonic()
            state = self.service.heartbeat_lease(int(lease["id"]), token)["state"]
            with durations_lock:
                durations.append(time.monotonic() - started)
            return state

        with ThreadPoolExecutor(max_workers=100) as executor:
            states = list(executor.map(heartbeat, range(500)))
        self.assertEqual(set(states), {"queued"})
        p95 = sorted(durations)[int(len(durations) * 0.95) - 1]
        self.assertLess(p95, 20)
        self.assertLess(max(durations), 30)

    def test_repeated_native_suspect_freezes_admission_without_recycling(self) -> None:
        self.request(
            "native-suspect-owner",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        session_id = int(session["id"])
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET runtime_metadata_json = ? WHERE id = ?",
                (json.dumps({"runtime_attested": True}), session_id),
            )
        for count in range(1, 5):
            self.clock.advance(300)
            suspect = self.service.report_session_fault(
                session_id,
                host_token,
                kind="native_probe_suspect",
                failure_message=f"GetVersion blocked {count}",
                evidence={"process_id": "123", "port": 50001},
            )
            self.assertEqual(suspect["state"], "unhealthy")
            self.service.reconcile(execute=True)
            self.assertEqual(self.service.get_session(session_id)["state"], "unhealthy")
            self.assertEqual(
                self.db.get_allocation(self.allocation_id)["state"], "active"
            )

        evidence_session = self.service.get_session(session_id)
        self.assertEqual(
            json.loads(evidence_session["runtime_metadata_json"]),
            {"runtime_attested": True},
        )
        self.assertEqual(
            json.loads(evidence_session["last_fault_evidence_json"])["port"],
            50001,
        )

        recovered = self.service.heartbeat_session(
            session_id,
            host_token,
            liveness_confirmed=True,
            process_id="123",
            port=50001,
            native_probe="GetVersion",
        )
        self.assertIn(recovered["state"], {"ready", "busy"})

    def test_durable_request_replays_terminal_or_abandoned_intent_with_new_token(self) -> None:
        first_token = "first-client-token-0001"
        first, _ = self.service.request_lease(
            request_key="durable-intent",
            project_name="mft-durable-intent",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            client_token=first_token,
        )
        replay, returned_token = self.service.request_lease(
            request_key="durable-intent",
            project_name="mft-durable-intent",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            client_token=first_token,
        )
        self.assertEqual(replay["id"], first["id"])
        self.assertEqual(returned_token, first_token)
        with self.assertRaisesRegex(ValueError, "owned by a live lease"):
            self.service.request_lease(
                request_key="durable-intent",
                project_name="mft-durable-intent",
                workload_family="mft",
                protocol_version=2,
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
                workspace_path="/shared/test-v2",
                client_token="second-client-token-0002",
            )

        self.service.cancel_lease(int(first["id"]), first_token)
        replacement, replacement_token = self.service.request_lease(
            request_key="durable-intent",
            project_name="mft-durable-intent",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            client_token="second-client-token-0002",
        )
        self.assertNotEqual(replacement["id"], first["id"])
        self.assertEqual(replacement["state"], "queued")
        self.assertEqual(replacement_token, "second-client-token-0002")
        archived = self.service.get_lease(int(first["id"]))
        self.assertEqual(archived["state"], "cancelled")
        self.assertTrue(str(archived["request_key"]).startswith("durable-intent#superseded:"))

        stale, _ = self.service.request_lease(
            request_key="abandoned-intent",
            project_name="mft-abandoned-intent",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            client_token="abandoned-token-00001",
        )
        self.clock.advance(self.service.config().admission_deadline_seconds + 1)
        recovered, _ = self.service.request_lease(
            request_key="abandoned-intent",
            project_name="mft-abandoned-intent",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            client_token="recovered-token-00002",
        )
        self.assertNotEqual(recovered["id"], stale["id"])
        self.assertEqual(self.service.get_lease(int(stale["id"]))["state"], "expired")

    def test_task_provenance_does_not_pin_central_pool_without_explicit_affinity(self) -> None:
        task_id = self.db.create_task(
            TaskCreate(name="motor-worker", remote_cwd="/work", command="true")
        )
        self.db.update_task(
            task_id,
            allocation_id=self.allocation_id,
            node_name="worker-01",
            status=TaskStatus.RUNNING.value,
        )
        lease, _ = self.service.request_lease(
            request_key="central-unpinned",
            project_name="motor-central-unpinned",
            workload_family="motor",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/motor-test-v2",
            task_id=task_id,
        )
        self.assertEqual(int(lease["requested_allocation_id"]), 0)
        self.assertEqual(lease["requested_node_name"], "")

        pinned, _ = self.service.request_lease(
            request_key="explicitly-pinned",
            project_name="motor-explicitly-pinned",
            workload_family="motor",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/motor-test-v2",
            task_id=task_id,
            allocation_id=self.allocation_id,
            node_name="cpu-01.example",
        )
        self.assertEqual(int(pinned["requested_allocation_id"]), self.allocation_id)
        self.assertEqual(pinned["requested_node_name"], "cpu-01")

    def test_protocol_v2_offer_accept_activate_and_cancel(self) -> None:
        lease, token = self.service.request_lease(
            request_key="v2-lifecycle",
            project_name="mft-v2-unique",
            workload_family="mft",
            project_namespace="mft-v2",
            isolation_policy="family",
            workspace_path="/shared/mft-v2",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        lease = self.service.get_lease(int(lease["id"]))
        self.assertEqual(lease["state"], "offered")
        accepted = self.service.accept_lease(int(lease["id"]), token)
        self.assertEqual(accepted["state"], "attaching")
        self.assertEqual(
            self.service.heartbeat_lease(int(lease["id"]), token)["state"],
            "attaching",
        )
        active = self.service.activate_lease(int(lease["id"]), token)
        self.assertEqual(active["state"], "active")
        releasing = self.service.cancel_lease(int(lease["id"]), token)
        self.assertEqual(releasing["state"], "releasing")
        released = self.service.complete_release(
            int(session["id"]), host_token, int(lease["id"]), success=True
        )
        self.assertEqual(released["state"], "released")

    def test_full_active_batch_gets_one_atomic_solve_permit_generation(self) -> None:
        # Exercise the production ceiling, not only the historical 1:2 default.
        self.service.set_enabled(False)
        self.service.set_operator_limits(projects_per_session=3)
        self.service.set_enabled(True)
        leases = [
            self.service.request_lease(
                request_key=f"solve-batch-{index}",
                project_name=f"mft-solve-batch-{index}",
                workload_family="mft",
                workspace_path=f"/shared/solve-batch/{index}",
                protocol_version=2,
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
                allocation_id=self.allocation_id,
                node_name="cpu-01",
            )
            for index in range(3)
        ]
        session, _host_token = self.start_one_session(self.allocation_id)
        self.assertEqual(int(session["slots_total"]), 3)
        active_statuses = []
        for index, (lease, token) in enumerate(leases):
            self.service.accept_lease(int(lease["id"]), token)
            active = self.service.activate_lease(int(lease["id"]), token)
            active_statuses.append(active)
            if index < 2:
                self.assertFalse(active["solve_permit_granted"])
                self.assertFalse(
                    bool(
                        self.service.get_session(int(session["id"]))[
                            "solve_batch_sealed_at"
                        ]
                    )
                )

        permitted = [
            self.service.lease_status(int(lease["id"]), token)
            for lease, token in leases
        ]
        self.assertTrue(all(item["solve_permit_granted"] for item in permitted))
        generations = {
            int(item["solve_permit_generation"])
            for item in permitted
        }
        self.assertEqual(len(generations), 1)
        self.assertGreater(next(iter(generations)), 0)
        self.assertTrue(active_statuses[-1]["solve_permit_granted"])
        self.assertTrue(
            self.service.get_session(int(session["id"]))["solve_batch_sealed_at"]
        )

    def test_underfilled_solve_permit_seals_session_against_late_attach(self) -> None:
        first, first_token = self.service.request_lease(
            request_key="underfilled-first",
            project_name="mft-underfilled-first",
            workload_family="mft",
            workspace_path="/shared/underfilled/first",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.accept_lease(int(first["id"]), first_token)
        active = self.service.activate_lease(int(first["id"]), first_token)
        self.assertFalse(active["solve_permit_granted"])
        permitted = self.service.request_solve_permit(
            int(first["id"]), first_token, seal_underfilled=True
        )
        self.assertTrue(permitted["solve_permit_granted"])

        late, _late_token = self.service.request_lease(
            request_key="underfilled-late",
            project_name="mft-underfilled-late",
            workload_family="mft",
            workspace_path="/shared/underfilled/late",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            allocation_id=self.allocation_id,
            node_name="cpu-01",
        )
        self.assertEqual(self.service.get_lease(int(late["id"]))["state"], "queued")

        self.service.cancel_lease(int(first["id"]), first_token)
        self.service.complete_release(
            int(session["id"]), host_token, int(first["id"]), success=True
        )
        self.assertFalse(
            bool(self.service.get_session(int(session["id"]))["solve_batch_sealed_at"])
        )
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_lease(int(late["id"]))["state"], "offered")

    def test_unaccepted_v2_offer_requeues_through_five_minute_outage(self) -> None:
        lease, token = self.service.request_lease(
            request_key="v2-abandoned-offer",
            project_name="mft-v2-abandoned",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            allocation_id=self.allocation_id,
            node_name="cpu-01",
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        lease = self.service.get_lease(int(lease["id"]))
        self.assertEqual(lease["state"], "offered")
        self.clock.advance(self.service.config().offer_ack_seconds + 1)
        self.service.reconcile(execute=True)
        # A fresh client can be re-offered immediately; losing one short offer
        # ACK is never terminal while the durable admission deadline is live.
        reoffered = self.service.get_lease(int(lease["id"]))
        self.assertEqual(reoffered["state"], "offered")

        self.clock.advance(240)
        self.service.reconcile(execute=True)
        queued = self.service.get_lease(int(lease["id"]))
        self.assertEqual(queued["state"], "queued")
        self.assertIsNone(queued["session_id"])
        current_session = self.service.get_session(int(session["id"]))
        self.assertEqual(current_session["state"], "ready")
        self.assertEqual(current_session["quarantine_reason"], "")

        resumed = self.service.heartbeat_lease(int(lease["id"]), token)
        self.assertEqual(resumed["state"], "offered")

    def test_terminal_scheduler_task_cancels_unstarted_lease(self) -> None:
        task_id = self.db.create_task(
            TaskCreate(name="dead-client", remote_cwd="/work", command="true")
        )
        lease, _token = self.service.request_lease(
            request_key="dead-client-lease",
            project_name="mft-dead-client",
            workload_family="mft",
            protocol_version=2,
            session_profile=EXPECTED_SESSION_PROFILE_JSON,
            workspace_path="/shared/test-v2",
            task_id=task_id,
        )
        self.db.update_task(
            task_id,
            status=TaskStatus.FAILED.value,
            failure_message="client exited",
            finished_at="CURRENT_TIMESTAMP",
        )
        self.service.reconcile(execute=True)
        cancelled = self.service.get_lease(int(lease["id"]))
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertIn("terminal", cancelled["failure_message"])

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

    def test_sessions_pack_one_placement_group_and_keep_groups_separate(self) -> None:
        self.service.set_enabled(False)
        self.service.set_operator_limits(
            max_sessions=2,
            min_idle_sessions=2,
            target_projects=6,
            projects_per_session=3,
        )
        self.service.set_enabled(True)
        self.service.reconcile(execute=True)
        starts = self.service.starting_sessions()
        self.assertEqual(len(starts), 2)

        sessions = []
        for index, start in enumerate(starts):
            host_id = f"placement-host-{index}"
            claimed = self.service.claim_start(
                session_id=int(start["id"]),
                allocation_id=self.allocation_id,
                node_name="cpu-01",
                host_id=host_id,
                bootstrap_token="secret",
            )
            self.assertEqual(int(claimed["id"]), int(start["id"]))
            session, _host_token = self.service.register_session(
                session_id=int(start["id"]),
                host_id=host_id,
                endpoint=f"cpu-01:{50001 + index}",
                process_id=str(100 + index),
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
                bootstrap_token="secret",
            )
            sessions.append(session)

        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason="operator maintenance",
            drain_at=self.clock.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        first_mft, _ = self.request(
            "placement-mft-first",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="mft-pending-39812-stage",
        )
        simulation_mft, _ = self.request(
            "placement-mft-simulation",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="simulation_745147_2759990",
        )
        ipmsm, _ = self.request(
            "placement-ipmsm",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="ipmsm_v2_stage3_001",
        )
        third_mft, _ = self.request(
            "placement-mft-third",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="mft-pending-39813-stage",
        )
        for lease in (first_mft, simulation_mft, ipmsm, third_mft):
            self.assertEqual(lease["state"], "queued")

        self.db.update_allocation(
            self.allocation_id,
            state="active",
            drain_reason="AEDT pool project demand",
            drain_at=None,
        )
        self.service.reconcile(execute=True)
        first_mft = self.service.get_lease(int(first_mft["id"]))
        simulation_mft = self.service.get_lease(int(simulation_mft["id"]))
        ipmsm = self.service.get_lease(int(ipmsm["id"]))
        third_mft = self.service.get_lease(int(third_mft["id"]))

        first_session_id = int(first_mft["session_id"])
        second_session_id = int(ipmsm["session_id"])
        self.assertEqual(first_mft["placement_group"], "mft")
        self.assertEqual(simulation_mft["placement_group"], "mft")
        self.assertEqual(ipmsm["placement_group"], "ipmsm")
        self.assertEqual(int(simulation_mft["session_id"]), first_session_id)
        self.assertEqual(int(third_mft["session_id"]), first_session_id)
        self.assertNotEqual(second_session_id, first_session_id)
        self.assertEqual(
            {int(session["id"]) for session in sessions},
            {first_session_id, second_session_id},
        )
        with self.db.connect() as conn:
            occupants = {
                (
                    int(row["session_id"]),
                    str(row["placement_group"]),
                    int(row["count"]),
                )
                for row in conn.execute(
                    """
                    SELECT session_id, placement_group, COUNT(*) AS count
                    FROM aedt_project_leases
                    WHERE state IN ('leased','active','releasing')
                    GROUP BY session_id, placement_group
                    """
                ).fetchall()
            }
        self.assertEqual(
            occupants,
            {
                (first_session_id, "mft", 3),
                (second_session_id, "ipmsm", 1),
            },
        )

    def test_allocation_age_rotation_drains_only_old_capacity(self) -> None:
        existing_lease, existing_token = self.request(
            "rotation-existing",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        active_lease = self.service.heartbeat_lease(
            int(existing_lease["id"]), existing_token
        )
        self.assertEqual(active_lease["state"], "active")

        younger_allocation_id = self.add_dedicated_allocation(node="cpu-02")
        max_age_seconds = 300
        self.db.update_allocation(
            self.allocation_id,
            created_at=(
                self.clock.now() - timedelta(seconds=max_age_seconds + 1)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        )
        self.db.update_allocation(
            younger_allocation_id,
            created_at=self.clock.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        self.service.set_operator_timeouts(allocation_max_age_seconds=0)
        self.service.reconcile(execute=True)
        self.assertEqual(
            self.db.get_allocation(self.allocation_id)["state"], "active"
        )

        self.service.set_operator_timeouts(
            allocation_max_age_seconds=max_age_seconds
        )
        queued_lease, _queued_token = self.request(
            "rotation-new-placement",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        self.service.reconcile(execute=True)

        rotated_allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(rotated_allocation["state"], "draining")
        self.assertEqual(
            rotated_allocation["drain_reason"],
            ALLOCATION_AGE_ROTATION_REASON,
        )
        self.assertIsNotNone(rotated_allocation["drain_at"])
        younger_allocation = self.db.get_allocation(younger_allocation_id)
        self.assertEqual(younger_allocation["state"], "active")
        self.assertEqual(
            younger_allocation["drain_reason"], "AEDT pool project demand"
        )
        self.assertIsNone(younger_allocation["drain_at"])

        draining_session = self.service.get_session(int(session["id"]))
        self.assertEqual(draining_session["state"], "draining")
        self.assertEqual(
            draining_session["failure_message"],
            ALLOCATION_AGE_ROTATION_REASON,
        )
        self.assertIsNotNone(draining_session["drain_requested_at"])
        preserved_lease = self.service.get_lease(int(existing_lease["id"]))
        self.assertEqual(preserved_lease["state"], "active")
        self.assertEqual(preserved_lease["session_id"], session["id"])
        self.assertEqual(
            self.service.get_lease(int(queued_lease["id"]))["state"], "queued"
        )
        commands = self.service.session_commands(int(session["id"]), host_token)
        self.assertTrue(commands["drain"])
        self.assertFalse(commands["global_stop_allowed"])

        starting_sessions = self.service.starting_sessions()
        self.assertEqual(len(starting_sessions), 1)
        self.assertEqual(
            int(starting_sessions[0]["allocation_id"]), younger_allocation_id
        )

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
        self.service.reconcile(execute=True)

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
        self.service.reconcile(execute=True)

        self.assertEqual(lease["state"], "queued")
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(self.service.starting_sessions(), [])
        self.assertGreaterEqual(self.service.summary()["plan"]["node_requests"], 1)

    def test_recycle_draining_allocation_with_healthy_session_is_restored(self) -> None:
        self.service.set_operator_limits(
            max_sessions=1,
            min_idle_sessions=1,
            target_projects=2,
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        drain_at = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE aedt_sessions
                SET drain_requested_at = ?
                WHERE id = ?
                """,
                (drain_at, int(session["id"])),
            )
        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason=FAULTED_DESKTOP_ALLOCATION_RECYCLE_REASON,
            drain_at=drain_at,
        )

        lease, _token = self.request(
            "recovered-allocation-placement",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="mft-pending-39812-stage",
        )
        self.service.reconcile(execute=True)
        lease = self.service.get_lease(int(lease["id"]))

        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "active")
        self.assertEqual(allocation["drain_reason"], "AEDT pool project demand")
        self.assertIsNone(allocation["drain_at"])
        self.assertEqual(lease["state"], "leased")
        self.assertEqual(int(lease["session_id"]), int(session["id"]))
        recovered_session = self.service.get_session(int(session["id"]))
        self.assertEqual(recovered_session["state"], "busy")
        self.assertIsNone(recovered_session["drain_requested_at"])

        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET drain_requested_at = ? WHERE id = ?",
                (drain_at, int(session["id"])),
            )
        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason=UNHEALTHY_ALLOCATION_RECYCLE_REASON,
            drain_at=drain_at,
        )

        self.service.reconcile(execute=True)

        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "active")
        self.assertEqual(allocation["drain_reason"], "AEDT pool project demand")
        self.assertIsNone(allocation["drain_at"])
        self.assertIsNone(
            self.service.get_session(int(session["id"]))["drain_requested_at"]
        )

    def test_age_rotation_draining_allocation_is_not_restored(self) -> None:
        self.service.set_operator_limits(
            max_sessions=1,
            min_idle_sessions=1,
            target_projects=2,
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        drain_at = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET drain_requested_at = ? WHERE id = ?",
                (drain_at, int(session["id"])),
            )
        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason=ALLOCATION_AGE_ROTATION_REASON,
            drain_at=drain_at,
        )

        self.service.reconcile(execute=True)

        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(allocation["drain_reason"], ALLOCATION_AGE_ROTATION_REASON)
        self.assertIsNotNone(allocation["drain_at"])
        draining_session = self.service.get_session(int(session["id"]))
        self.assertEqual(draining_session["state"], "draining")
        self.assertIsNotNone(draining_session["drain_requested_at"])

    def test_recycle_draining_allocation_with_quarantine_is_not_restored(self) -> None:
        self.service.set_operator_limits(
            max_sessions=1,
            min_idle_sessions=1,
            target_projects=2,
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        drain_at = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET drain_requested_at = ? WHERE id = ?",
                (drain_at, int(session["id"])),
            )
            conn.execute(
                """
                INSERT INTO aedt_sessions (
                    session_key, allocation_id, account_name, node_name,
                    slots_total, state, quarantine_reason
                ) VALUES ('live-quarantine', ?, 'a', 'cpu-01', 2, 'draining',
                          'solver_timeout')
                """,
                (self.allocation_id,),
            )
        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason=UNHEALTHY_ALLOCATION_RECYCLE_REASON,
            drain_at=drain_at,
        )

        self.service.reconcile(execute=True)

        # A LIVE quarantined sibling keeps the allocation recycling; the
        # healthy session stays on its way out rather than serving.
        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "draining")
        self.assertEqual(
            allocation["drain_reason"], UNHEALTHY_ALLOCATION_RECYCLE_REASON
        )
        healthy_session = self.service.get_session(int(session["id"]))
        self.assertIn(healthy_session["state"], ("ready", "draining"))
        self.assertIsNotNone(healthy_session["drain_requested_at"])

    def test_recycle_draining_allocation_recovers_despite_terminal_history(
        self,
    ) -> None:
        self.service.set_operator_limits(
            max_sessions=1,
            min_idle_sessions=1,
            target_projects=2,
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        drain_at = self.clock.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_sessions SET drain_requested_at = ? WHERE id = ?",
                (drain_at, int(session["id"])),
            )
            # Terminal siblings keep their quarantine history forever; that
            # history must not pin the allocation once only healthy sessions
            # remain (production: 2 ready Desktops idled behind 3 failed
            # siblings while 249 leases queued).
            conn.execute(
                """
                INSERT INTO aedt_sessions (
                    session_key, allocation_id, account_name, node_name,
                    slots_total, state, quarantine_reason
                ) VALUES ('dead-quarantine', ?, 'a', 'cpu-01', 2, 'failed',
                          'aedt_death_reported')
                """,
                (self.allocation_id,),
            )
        self.db.update_allocation(
            self.allocation_id,
            state="draining",
            drain_reason=UNHEALTHY_ALLOCATION_RECYCLE_REASON,
            drain_at=drain_at,
        )

        self.service.reconcile(execute=True)

        allocation = self.db.get_allocation(self.allocation_id)
        self.assertEqual(allocation["state"], "active")
        self.assertEqual(allocation["drain_reason"], "AEDT pool project demand")
        healthy_session = self.service.get_session(int(session["id"]))
        self.assertEqual(healthy_session["state"], "ready")
        self.assertIsNone(healthy_session["drain_requested_at"])

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

    def test_exclusive_lease_requires_an_empty_session(self) -> None:
        self.service.set_operator_limit(1)
        shared, _ = self.request(
            "shared-first",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="mft-pending-39812-stage",
        )
        session, _host_token = self.start_one_session(self.allocation_id)
        current_shared = self.service.get_lease(int(shared["id"]))
        self.assertEqual(int(current_shared["session_id"]), int(session["id"]))

        exclusive, _ = self.request(
            "exclusive-after-shared",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="simulation_745147_2759990",
            exclusive_session=True,
        )
        self.assertEqual(exclusive["placement_group"], "mft")
        self.assertEqual(exclusive["state"], "queued")
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

    def test_shared_demand_packs_each_placement_group_separately(self) -> None:
        self.service.set_enabled(False)
        self.service.set_operator_limits(
            max_sessions=2,
            target_projects=6,
            projects_per_session=3,
        )
        self.service.set_enabled(True)
        self.request(
            "demand-mft",
            project_name="mft-pending-39812-stage",
        )
        self.request(
            "demand-ipmsm",
            project_name="ipmsm_v2_stage3_001",
        )

        plan = self.service.dry_run()
        self.assertEqual(plan["live_projects"], 2)
        self.assertEqual(plan["exclusive_projects"], 0)
        self.assertEqual(plan["demand_sessions"], 2)
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
        lease, lease_token = self.request(
            "lease-expiry-after-host-timeout",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.assertEqual(
            self.service.heartbeat_lease(int(lease["id"]), lease_token)["state"],
            "active",
        )
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

    def test_stale_leased_never_active_releases_without_harming_session(self) -> None:
        self.service.set_operator_timeouts(
            lease_ttl_seconds=600,
            session_heartbeat_timeout_seconds=3600,
        )
        stale, _stale_token = self.request(
            "stale-never-active",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        sibling, sibling_token = self.request(
            "live-sibling",
            allocation_id=self.allocation_id,
            node="cpu-01",
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.assertEqual(self.service.get_lease(int(stale["id"]))["state"], "leased")
        self.assertEqual(
            self.service.heartbeat_lease(int(sibling["id"]), sibling_token)["state"],
            "active",
        )
        sibling_before = self.service.get_lease(int(sibling["id"]))

        self.clock.advance(599)
        self.service.heartbeat_lease(int(sibling["id"]), sibling_token)
        self.clock.advance(2)
        self.service.heartbeat_session(int(session["id"]), host_token)
        self.service.reconcile(execute=True)

        expired = self.service.get_lease(int(stale["id"]))
        live_sibling = self.service.get_lease(int(sibling["id"]))
        current_session = self.service.get_session(int(session["id"]))
        commands = self.service.session_commands(int(session["id"]), host_token)
        self.assertEqual(expired["state"], "releasing")
        self.assertEqual(live_sibling["state"], "active")
        self.assertEqual(live_sibling["session_id"], session["id"])
        self.assertEqual(live_sibling["slot_index"], sibling_before["slot_index"])
        self.assertEqual(
            live_sibling["failure_message"], sibling_before["failure_message"]
        )
        self.assertEqual(current_session["state"], "busy")
        self.assertEqual(current_session["quarantine_reason"], "")
        self.assertFalse(commands["drain"])
        self.assertEqual(
            [int(item["id"]) for item in commands["close_projects"]],
            [int(stale["id"])],
        )
        self.assertEqual(commands["deferred_projects"], [])
        self.assertEqual(self.db.get_allocation(self.allocation_id)["state"], "active")

        released = self.service.complete_release(
            int(session["id"]), host_token, int(stale["id"]), success=True
        )
        self.assertEqual(released["state"], "released")
        self.assertEqual(
            self.service.get_lease(int(sibling["id"]))["state"], "active"
        )
        self.assertEqual(
            self.service.get_session(int(session["id"]))["state"], "busy"
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
        lease, token = self.request(
            "temporary",
            allocation_id=self.allocation_id,
            node="cpu-01",
            project_name="mft-pending-39812-stage",
        )
        self.assertEqual(lease["placement_group"], "mft")
        self.start_one_session(self.allocation_id)
        updated = self.service.bind_lease_project_name(
            int(lease["id"]), token, "simulation_745147_2759990"
        )
        self.assertEqual(updated["project_name"], "simulation_745147_2759990")
        self.assertEqual(updated["placement_group"], "mft")

        custom, custom_token = self.request(
            "custom-bind-stability",
            project_name="custom-pending-001",
        )
        rebound = self.service.bind_lease_project_name(
            int(custom["id"]), custom_token, "simulation_999999_111111"
        )
        self.assertEqual(rebound["placement_group"], "custom")

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

    def test_stale_queued_lease_expires_after_short_heartbeat_grace(self) -> None:
        self.service.set_operator_timeouts(lease_ttl_seconds=3600)
        self.service.set_enabled(False)
        lease, _token = self.request("stale-queued")
        self.assertEqual(lease["state"], "queued")
        self.assertEqual(lease["last_heartbeat_at"], "2026-07-13 00:00:00")

        self.clock.advance(300)
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_lease(int(lease["id"]))["state"], "queued")

        self.clock.advance(1)
        self.service.reconcile(execute=True)
        expired = self.service.get_lease(int(lease["id"]))
        self.assertEqual(expired["state"], "expired")
        self.assertEqual(expired["failure_message"], "lease request heartbeat expired")

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
        lease, token = self.request(
            "idle-ttl", allocation_id=self.allocation_id, node="cpu-01"
        )
        session, host_token = self.start_one_session(self.allocation_id)
        self.service.release_lease(int(lease["id"]), token)
        self.service.complete_release(
            int(session["id"]), host_token, int(lease["id"]), success=True
        )
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "ready")
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "ready")

        idle_ttl_seconds = self.service.config().idle_ttl_seconds
        elapsed = 0
        while elapsed < idle_ttl_seconds - 1:
            step = min(300, idle_ttl_seconds - 1 - elapsed)
            self.clock.advance(step)
            self.service.heartbeat_session(int(session["id"]), host_token)
            elapsed += step
        self.service.reconcile(execute=True)
        self.assertEqual(self.service.get_session(int(session["id"]))["state"], "ready")

        self.clock.advance(1)
        self.service.heartbeat_session(int(session["id"]), host_token)
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
    def test_canonical_host_command_pins_aedt_version_and_profile(self) -> None:
        runtime = AedtPoolRuntime(
            self.service,
            FakeRuntimeScheduler(),
            scheduler_url="http://relay:18790",
            host_remote_cwd="/gpfs/scheduler",
            host_bootstrap_token_file="/gpfs/token",
            host_artifact_root="/gpfs/aedt-artifacts",
            host_dso_profile=SUPPORTED_DSO_PROFILE,
            host_session_profile=EXPECTED_SESSION_PROFILE_JSON,
        )
        command = runtime._host_command(
            {"id": 17, "allocation_id": 9, "node_name": "cpu-09"}
        )
        self.assertIn("--aedt-version 2025.2", command)
        self.assertIn(f"--dso-profile {SUPPORTED_DSO_PROFILE}", command)
        self.assertIn("--artifact-root /gpfs/aedt-artifacts", command)
        self.assertIn("--session-profile", command)

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
        self.assertTrue(
            all(int(task["requested_allocation_id"]) > 0 for task in scheduler.host_tasks)
        )
        self.assertTrue(all(int(task["cpus"]) == 8 for task in scheduler.host_tasks))
        self.assertTrue(
            all(int(task["memory_mb"]) >= 64 * 1024 for task in scheduler.host_tasks)
        )

    def test_session_capacity_is_bounded_by_free_cpu_and_memory(self) -> None:
        config = self.service.config()
        one_by_memory = self.service._allocation_session_capacity(
            {
                "total_cpus": 64,
                "free_cpus": 64,
                "total_memory_mb": 64 * 1024,
                "free_memory_mb": 64 * 1024,
            },
            config,
        )
        self.assertEqual(one_by_memory, 1)

        occupied_plus_free = self.service._allocation_session_capacity(
            {
                "total_cpus": 64,
                "free_cpus": 48,
                "total_memory_mb": 512 * 1024,
                "free_memory_mb": 384 * 1024,
            },
            config,
            current_sessions=2,
        )
        self.assertEqual(occupied_plus_free, 8)

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
    def setUp(self) -> None:
        self.lock_tmp = tempfile.TemporaryDirectory()
        self.automation_lock_path = create_automation_lock_file(
            str(Path(self.lock_tmp.name) / "desktop-automation.lock")
        )

    def tearDown(self) -> None:
        self.lock_tmp.cleanup()

    def test_connect_path_forces_pyaedt_multi_desktop_setting(self) -> None:
        ansys_module = ModuleType("ansys")
        aedt_module = ModuleType("ansys.aedt")
        core_module = ModuleType("ansys.aedt.core")
        core_module.settings = SimpleNamespace(use_multi_desktop=False)
        ansys_module.aedt = aedt_module
        aedt_module.core = core_module
        with patch.dict(
            sys.modules,
            {
                "ansys": ansys_module,
                "ansys.aedt": aedt_module,
                "ansys.aedt.core": core_module,
            },
        ):
            AedtProjectLease._enable_pyaedt_multi_desktop(required=True)
        self.assertIs(core_module.settings.use_multi_desktop, True)

    def test_keepalive_jitter_spreads_500_clients_within_bounded_window(self) -> None:
        initial = [
            _keepalive_delay(index, f"token-{index}", 20, 0, initial=True)
            for index in range(1, 501)
        ]
        periodic = [
            _keepalive_delay(index, f"token-{index}", 20, 1)
            for index in range(1, 501)
        ]
        self.assertEqual(
            initial,
            [
                _keepalive_delay(index, f"token-{index}", 20, 0, initial=True)
                for index in range(1, 501)
            ],
        )
        self.assertGreater(max(initial) - min(initial), 19)
        self.assertTrue(all(0 <= item <= 20 for item in initial))
        self.assertTrue(all(15 <= item <= 25 for item in periodic))
        self.assertGreater(len({round(item, 3) for item in initial}), 490)

    def test_connect_does_not_activate_until_project_is_bound(self) -> None:
        calls: list[tuple[str, str, dict | None]] = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                calls.append((method, path, payload))
                if path.endswith("/project-name"):
                    return {"state": "attaching", "endpoint": "cpu-01:50001"}
                if path.endswith("/activate"):
                    return {"state": "active", "endpoint": "cpu-01:50001"}
                return {
                    "state": "attaching",
                    "endpoint": "cpu-01:50001",
                    "session_key": "session-7",
                    "session_process_id": "7001",
                    "expected_aedt_version": "2025.2",
                }

        lease = AedtProjectLease(
            Http(),
            7,
            "client-token",
            "pending-name",
            state="attaching",
            endpoint="cpu-01:50001",
            protocol_version=2,
            automation_lock_path=self.automation_lock_path,
        )
        desktop_kwargs: list[dict] = []
        attached_desktop = SimpleNamespace(
            port=50001,
            aedt_process_id="7001",
            odesktop=SimpleNamespace(GetVersion=lambda: "2025.2.0"),
        )
        desktop = lease.connect_desktop(
            desktop_factory=lambda **kwargs: (
                desktop_kwargs.append(kwargs) or attached_desktop
            ),
            endpoint_probe=lambda _machine, _port: True,
        )
        self.assertIsNotNone(desktop)
        self.assertEqual(desktop_kwargs[0]["version"], "2025.2")
        self.assertFalse(any(path.endswith("/activate") for _m, path, _p in calls))
        self.assertEqual(lease.state, "attaching")

        activated = lease.activate("actual-project")
        self.assertEqual(activated["state"], "active")
        self.assertTrue(any(path.endswith("/project-name") for _m, path, _p in calls))
        self.assertTrue(any(path.endswith("/activate") for _m, path, _p in calls))
        self.assertEqual(lease.project_name, "actual-project")
        lease.stop_heartbeat()

    def test_connect_rejects_caller_version_override_before_factory(self) -> None:
        calls: list[tuple[str, str, dict | None]] = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                calls.append((method, path, payload))
                if path.endswith("/fault"):
                    return {"state": "cancelled", "endpoint": ""}
                return {
                    "state": "attaching",
                    "endpoint": "cpu-01:50001",
                    "session_key": "session-7",
                    "session_process_id": "7001",
                    "expected_aedt_version": "2025.2",
                }

        lease = AedtProjectLease(
            Http(),
            7,
            "client-token",
            "pending-name",
            state="attaching",
            endpoint="cpu-01:50001",
            protocol_version=2,
        )
        factory_called = False

        def factory(**_kwargs):
            nonlocal factory_called
            factory_called = True
            return object()

        with self.assertRaisesRegex(AedtLeaseError, "does not match authorized"):
            lease.connect_desktop(
                version="2026.1",
                desktop_factory=factory,
                endpoint_probe=lambda _machine, _port: True,
            )
        self.assertFalse(factory_called)
        self.assertTrue(any(path.endswith("/fault") for _m, path, _p in calls))

    def test_connect_refuses_unreachable_endpoint_before_desktop_factory(self) -> None:
        calls: list[tuple[str, str, dict | None]] = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                calls.append((method, path, payload))
                if path.endswith("/fault"):
                    return {"state": "cancelled", "endpoint": ""}
                return {
                    "state": "attaching",
                    "endpoint": "cpu-01:50001",
                    "session_key": "session-7",
                    "session_process_id": "7001",
                    "expected_aedt_version": "2025.2",
                }

        lease = AedtProjectLease(
            Http(),
            7,
            "client-token",
            "pending-name",
            state="attaching",
            endpoint="cpu-01:50001",
            protocol_version=2,
        )
        factory_called = False

        def factory(**_kwargs):
            nonlocal factory_called
            factory_called = True
            return object()

        with self.assertRaisesRegex(
            AedtLeaseError, "refusing PyAEDT constructor auto-launch fallback"
        ):
            lease.connect_desktop(
                desktop_factory=factory,
                endpoint_probe=lambda _machine, _port: False,
            )
        self.assertFalse(factory_called)
        fault = next(
            payload for _method, path, payload in calls if path.endswith("/fault")
        )
        self.assertEqual(fault["fault_kind"], "attach_failed")
        self.assertEqual(fault["evidence"]["expected_process_id"], "7001")

    def test_activate_waits_for_shared_solve_permit_before_returning(self) -> None:
        calls: list[tuple[str, str]] = []
        status_reads = 0

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                nonlocal status_reads
                calls.append((method, path))
                base = {
                    "state": "active",
                    "endpoint": "cpu-01:50001",
                    "solve_permit_required": True,
                    "solve_permit_granted": False,
                    "session_active_lease_count": 1,
                    "session_live_lease_count": 2,
                    "session_slots_total": 2,
                }
                if path.endswith("/activate"):
                    return base
                if method == "GET":
                    status_reads += 1
                    if status_reads == 1:
                        return {
                            **base,
                            "state": "attaching",
                            "solve_permit_required": False,
                        }
                    if status_reads >= 3:
                        return {
                            **base,
                            "solve_permit_required": False,
                            "solve_permit_granted": True,
                            "solve_permit_generation": 4,
                            "session_active_lease_count": 2,
                        }
                    return base
                raise AssertionError((method, path, payload))

        lease = AedtProjectLease(
            Http(),
            9,
            "client-token",
            "project",
            state="attaching",
            endpoint="cpu-01:50001",
            protocol_version=2,
        )
        with patch("slurm_scheduler.aedt_attach_client.time.sleep", return_value=None):
            activated = lease.activate()

        self.assertTrue(activated["solve_permit_granted"])
        self.assertEqual(lease.solve_permit_generation, 4)
        self.assertEqual(status_reads, 3)
        self.assertEqual(calls[0], ("GET", "/api/aedt-pool/leases/9"))
        self.assertEqual(calls[1], ("POST", "/api/aedt-pool/leases/9/activate"))

    def test_replayed_active_activation_cannot_bypass_solve_barrier(self) -> None:
        reads = 0

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                nonlocal reads
                self.assert_get(method)
                reads += 1
                granted = reads >= 2
                return {
                    "state": "active",
                    "endpoint": "cpu-01:50001",
                    "solve_permit_required": not granted,
                    "solve_permit_granted": granted,
                    "solve_permit_generation": 11 if granted else 0,
                    "session_active_lease_count": 2 if granted else 1,
                    "session_live_lease_count": 2,
                    "session_slots_total": 2,
                }

            @staticmethod
            def assert_get(method):
                if method != "GET":
                    raise AssertionError(method)

        lease = AedtProjectLease(
            Http(),
            11,
            "client-token",
            "project",
            state="active",
            endpoint="cpu-01:50001",
            protocol_version=2,
        )
        with patch("slurm_scheduler.aedt_attach_client.time.sleep", return_value=None):
            replayed = lease.activate()

        self.assertTrue(replayed["solve_permit_granted"])
        self.assertEqual(lease.solve_permit_generation, 11)
        self.assertEqual(reads, 2)

    def test_activate_timeout_atomically_seals_underfilled_batch(self) -> None:
        calls: list[tuple[str, str, dict | None]] = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                calls.append((method, path, payload))
                base = {
                    "state": "active",
                    "endpoint": "cpu-01:50001",
                    "solve_permit_required": True,
                    "solve_permit_granted": False,
                    "session_active_lease_count": 1,
                    "session_live_lease_count": 1,
                    "session_slots_total": 2,
                }
                if path.endswith("/solve-permit"):
                    return {
                        **base,
                        "solve_permit_required": False,
                        "solve_permit_granted": True,
                        "solve_permit_generation": 8,
                    }
                return base

        lease = AedtProjectLease(
            Http(),
            10,
            "client-token",
            "project",
            state="attaching",
            endpoint="cpu-01:50001",
            protocol_version=2,
        )
        with patch.dict(
            os.environ, {"MFT_AEDT_POOL_FILL_TIMEOUT_SECONDS": "0"}, clear=False
        ):
            activated = lease.activate()

        self.assertTrue(activated["solve_permit_granted"])
        seal_call = next(
            payload for method, path, payload in calls if path.endswith("/solve-permit")
        )
        self.assertEqual(seal_call, {"seal_underfilled": True})

    def test_connect_rejects_stale_port_pid_and_version_before_project_work(self) -> None:
        cases = {
            "port": SimpleNamespace(
                port=50002,
                aedt_process_id="7001",
                odesktop=SimpleNamespace(GetVersion=lambda: "2025.2.0"),
            ),
            "PID": SimpleNamespace(
                port=50001,
                aedt_process_id="7999",
                odesktop=SimpleNamespace(GetVersion=lambda: "2025.2.0"),
            ),
            "version": SimpleNamespace(
                port=50001,
                aedt_process_id="7001",
                odesktop=SimpleNamespace(GetVersion=lambda: "2026.1.0"),
            ),
        }
        for expected_error, desktop in cases.items():
            with self.subTest(expected_error=expected_error):
                calls: list[tuple[str, str, dict | None]] = []

                class Http:
                    def request(self, method, path, payload=None, **_kwargs):
                        calls.append((method, path, payload))
                        if path.endswith("/fault"):
                            return {"state": "cancelled", "endpoint": ""}
                        return {
                            "state": "attaching",
                            "endpoint": "cpu-01:50001",
                            "session_key": "session-7",
                            "session_process_id": "7001",
                            "expected_aedt_version": "2025.2",
                        }

                lease = AedtProjectLease(
                    Http(),
                    7,
                    "client-token",
                    "pending-name",
                    state="attaching",
                    endpoint="cpu-01:50001",
                    protocol_version=2,
                    automation_lock_path=self.automation_lock_path,
                )
                with self.assertRaisesRegex(AedtLeaseError, expected_error):
                    lease.connect_desktop(
                        desktop_factory=lambda **_kwargs: desktop,
                        endpoint_probe=lambda _machine, _port: True,
                    )
                fault = next(
                    payload for _method, path, payload in calls if path.endswith("/fault")
                )
                self.assertEqual(fault["fault_kind"], "attach_failed")
                self.assertEqual(fault["phase"], "attach")
                self.assertFalse(any(path.endswith("/activate") for _m, path, _p in calls))

    def test_release_detaches_wrapper_only_after_terminal_host_ack(self) -> None:
        events: list[object] = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                if path.endswith("/cancel"):
                    events.append("cancel-releasing")
                    return {"state": "releasing", "endpoint": "cpu-01:50001"}
                if method == "GET":
                    events.append("host-released")
                    return {"state": "released", "endpoint": ""}
                raise AssertionError((method, path, payload))

        class Desktop:
            def release_desktop(self, **kwargs):
                events.append(("detach", kwargs))

        lease = AedtProjectLease(
            Http(), 8, "client-token", "project", state="active"
        )
        lease._desktop_proxy = Desktop()
        with patch("slurm_scheduler.aedt_attach_client.time.sleep", return_value=None):
            result = lease.release(wait_seconds=5)

        self.assertEqual(result["state"], "released")
        self.assertEqual(
            events,
            [
                "cancel-releasing",
                "host-released",
                (
                    "detach",
                    {"close_projects": False, "close_on_exit": False},
                ),
            ],
        )
        self.assertIsNone(lease._desktop_proxy)

    def test_two_sequential_leases_attach_to_distinct_authorized_endpoints(self) -> None:
        events: list[object] = []
        factory_ports: list[int] = []

        class Http:
            def __init__(self, endpoint: str, process_id: str) -> None:
                self.endpoint = endpoint
                self.process_id = process_id

            def request(self, method, path, payload=None, **_kwargs):
                if path.endswith("/cancel"):
                    return {"state": "released", "endpoint": ""}
                return {
                    "state": "attaching",
                    "endpoint": self.endpoint,
                    "session_key": f"session-{self.process_id}",
                    "session_process_id": self.process_id,
                    "expected_aedt_version": "2025.2",
                }

        class Desktop:
            def __init__(self, port: int, process_id: str) -> None:
                self.port = port
                self.aedt_process_id = process_id
                self.odesktop = SimpleNamespace(GetVersion=lambda: "2025.2.0")

            def release_desktop(self, **kwargs):
                events.append(("detached", self.port, kwargs))

        process_by_port = {50001: "7001", 50002: "7002"}

        def factory(**kwargs):
            port = int(kwargs["port"])
            factory_ports.append(port)
            return Desktop(port, process_by_port[port])

        first = AedtProjectLease(
            Http("cpu-01:50001", "7001"),
            1,
            "first-token",
            "project-a",
            state="attaching",
            endpoint="cpu-01:50001",
            protocol_version=2,
            automation_lock_path=self.automation_lock_path,
        )
        first.start_heartbeat = lambda **_kwargs: None
        first_desktop = first.connect_desktop(
            desktop_factory=factory,
            endpoint_probe=lambda _machine, _port: True,
        )
        first.release(wait_seconds=0)

        second = AedtProjectLease(
            Http("cpu-01:50002", "7002"),
            2,
            "second-token",
            "project-b",
            state="attaching",
            endpoint="cpu-01:50002",
            protocol_version=2,
            automation_lock_path=self.automation_lock_path,
        )
        second.start_heartbeat = lambda **_kwargs: None
        second_desktop = second.connect_desktop(
            desktop_factory=factory,
            endpoint_probe=lambda _machine, _port: True,
        )

        self.assertEqual(factory_ports, [50001, 50002])
        self.assertEqual(first_desktop.port, 50001)
        self.assertEqual(second_desktop.port, 50002)
        self.assertEqual(
            events,
            [
                (
                    "detached",
                    50001,
                    {"close_projects": False, "close_on_exit": False},
                )
            ],
        )

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

    def test_wait_polling_uses_get_only_while_process_keepalive_is_alive(self) -> None:
        methods: list[str] = []

        class Http:
            def request(self, method, path, payload=None, **_kwargs):
                methods.append(method)
                state = "leased" if len(methods) >= 3 else "queued"
                return {
                    "id": 1,
                    "state": state,
                    "endpoint": "cpu-01:50001" if state == "leased" else "",
                }

        lease = AedtProjectLease(Http(), 1, "token", "p")
        lease._keepalive_process = SimpleNamespace(is_alive=lambda: True)
        with patch("slurm_scheduler.aedt_attach_client.time.sleep", return_value=None):
            result = lease.wait_until_leased(timeout_seconds=2, heartbeat_seconds=5)
        self.assertEqual(result["state"], "leased")
        self.assertEqual(set(methods), {"GET"})
        lease._keepalive_process = None

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
        self.assertEqual(calls[0][2]["admission_timeout_seconds"], 1800)


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

    def test_lease_client_never_sends_admin_token_after_lease_creation(self) -> None:
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
        self.assertEqual(headers["x-aedt-lease-token"], "lease")
        self.assertNotIn("x-aedt-bootstrap-token", headers)
        self.assertNotIn("x-aedt-client-token", headers)
        self.assertEqual(build_opener.call_args.args[0].proxies, {})

    def test_lease_creation_uses_limited_client_credential(self) -> None:
        with patch(
            "slurm_scheduler.aedt_attach_client.urllib.request.build_opener",
        ) as build_opener:
            build_opener.return_value.open.return_value = self.Response()
            AedtPoolHttpClient(
                "http://relay",
                client_credential="limited-client",
            ).request("POST", "/api/aedt-pool/leases", {})

        request = build_opener.return_value.open.call_args.args[0]
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["x-aedt-client-token"], "limited-client")
        self.assertNotIn("x-aedt-bootstrap-token", headers)
        self.assertNotIn("x-aedt-lease-token", headers)

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

    def test_lease_client_can_load_limited_credential_from_node_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_file = Path(tmp) / "bootstrap-token"
            token_file.write_text("from-file\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "SLURM_AEDT_POOL_CLIENT_TOKEN": "",
                    "SLURM_AEDT_POOL_CLIENT_TOKEN_FILE": str(token_file),
                },
                clear=False,
            ):
                client = AedtPoolHttpClient("http://relay")
        self.assertEqual(client.client_credential, "from-file")


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

    def GetVersion(self) -> str:
        return "2025.2.0"


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


def trust_fake_desktop_liveness(host: AedtSessionHost) -> None:
    host._desktop_liveness_proof = lambda: (True, "")


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
    def test_native_liveness_never_declares_death_while_pid_and_port_are_alive(
        self,
    ) -> None:
        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        host.desktop = SimpleNamespace(odesktop=object())
        host.desktop_process_id = "3824121"
        host.desktop_process_marker = "marker"
        host.desktop_port = 44773
        with (
            patch.object(host, "_process_alive", return_value=True),
            patch.object(host, "_desktop_port_is_listening", return_value=True),
            patch.object(
                host,
                "_native_desktop_responds",
                side_effect=[False, False, False, True],
            ),
            patch(
                "slurm_scheduler.aedt_session_host.time.monotonic",
                side_effect=[100.0, 130.0, 161.0],
            ),
        ):
            first = host._desktop_liveness_proof()
            second = host._desktop_liveness_proof()
            third = host._desktop_liveness_proof()
            recovered = host._desktop_liveness_proof()

        self.assertTrue(first[0])
        self.assertIn("transiently failed", first[1])
        self.assertTrue(second[0])
        self.assertTrue(third[0])
        self.assertIn("3 consecutive", third[1])
        self.assertEqual(recovered, (True, ""))
        self.assertEqual(host.native_probe_failures, 0)

    def test_blocked_native_probe_keeps_only_one_inflight_thread(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        calls = 0

        def blocked_get_version():
            nonlocal calls
            calls += 1
            entered.set()
            release.wait(2)
            return "2025.2"

        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        host.desktop = SimpleNamespace(
            odesktop=SimpleNamespace(GetVersion=blocked_get_version)
        )
        self.assertEqual(
            host._native_desktop_responds(timeout_seconds=0.1),
            NATIVE_PROBE_FAILED,
        )
        self.assertTrue(entered.is_set())
        self.assertEqual(
            host._native_desktop_responds(timeout_seconds=0.1),
            NATIVE_PROBE_FAILED,
        )
        self.assertEqual(
            host._native_desktop_responds(timeout_seconds=0.1),
            NATIVE_PROBE_FAILED,
        )
        self.assertEqual(calls, 1)
        release.set()
        assert host._native_probe_thread is not None
        host._native_probe_thread.join(timeout=1)
        self.assertEqual(
            host._native_desktop_responds(timeout_seconds=0.1),
            NATIVE_PROBE_OK,
        )
        self.assertIsNone(host._native_probe_thread)

    def test_client_automation_lock_defers_probe_without_getversion_or_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            lock_path = create_automation_lock_file(
                str(Path(root) / "desktop-automation.lock")
            )
            get_version_calls = 0

            def get_version():
                nonlocal get_version_calls
                get_version_calls += 1
                return "2025.2"

            host = AedtSessionHost(
                FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
            )
            host.automation_lock_path = lock_path
            host.desktop = SimpleNamespace(
                odesktop=SimpleNamespace(GetVersion=get_version)
            )
            host.desktop_process_id = "3824121"
            host.desktop_process_marker = "marker"
            host.desktop_port = 44773
            host.native_probe_failures = 2
            host.native_probe_first_failure_at = 91.0
            client_lock = SessionAutomationLock(lock_path, timeout_seconds=1.0)

            with (
                client_lock,
                patch.object(host, "_process_alive", return_value=True),
                patch.object(
                    host, "_desktop_port_is_listening", return_value=True
                ),
            ):
                self.assertEqual(
                    host._native_desktop_responds(timeout_seconds=0.1),
                    NATIVE_PROBE_DEFERRED_BUSY,
                )
                proof = host._desktop_liveness_proof()
                # Keep the client transaction busy beyond the requested probe
                # timeout.  A deferred worker must already be gone and must
                # never call GetVersion later when this lock is released.
                time.sleep(0.15)
                self.assertEqual(get_version_calls, 0)
                self.assertEqual(host._native_probe_errors, [])
                self.assertIsNone(host._native_probe_thread)

            self.assertEqual(proof, (True, ""))
            self.assertEqual(
                host.last_native_probe_outcome, NATIVE_PROBE_DEFERRED_BUSY
            )
            self.assertEqual(get_version_calls, 0)
            self.assertEqual(host.native_probe_failures, 2)
            self.assertEqual(host.native_probe_first_failure_at, 91.0)
            self.assertIsNone(host._native_probe_thread)

    def test_deferred_busy_probe_heartbeats_without_suspect_fault(self) -> None:
        class RecordingControl(FakeHostControlPlane):
            def __init__(self, events):
                super().__init__(events)
                self.paths: list[str] = []
                self.heartbeats: list[dict] = []

            def request(self, method, path, payload=None, host_token=""):
                self.paths.append(path)
                if path.endswith("/heartbeat"):
                    self.heartbeats.append(dict(payload or {}))
                return super().request(
                    method, path, payload, host_token=host_token
                )

        with tempfile.TemporaryDirectory() as root:
            events: list[str] = []
            control = RecordingControl(events)
            host = AedtSessionHost(
                control,
                allocation_id=1,
                node_name="cpu-01",
                heartbeat_seconds=0,
                artifact_root=root,
            )
            host._start_desktop = lambda: FakeDesktop(events)

            def deferred_liveness():
                host.last_native_probe_outcome = NATIVE_PROBE_DEFERRED_BUSY
                return True, ""

            host._desktop_liveness_proof = deferred_liveness
            with patch(
                "slurm_scheduler.aedt_session_host.time.sleep", return_value=None
            ):
                self.assertEqual(host.run(), 2)

            journal = Path(host.journal_path).read_text(encoding="utf-8")

        self.assertFalse(any(path.endswith("/fault") for path in control.paths))
        self.assertGreaterEqual(len(control.heartbeats), 1)
        self.assertTrue(
            all(item["native_probe"] == "" for item in control.heartbeats)
        )
        self.assertTrue(
            all(
                item["native_probe_outcome"] == NATIVE_PROBE_DEFERRED_BUSY
                for item in control.heartbeats
            )
        )
        self.assertIn("desktop_native_probe_deferred_busy", journal)
        self.assertNotIn("desktop_native_probe_suspect", journal)

    def test_canonical_runtime_attestation_derives_and_checks_aedt_version(
        self,
    ) -> None:
        ansys_module = ModuleType("ansys")
        aedt_module = ModuleType("ansys.aedt")
        core_module = ModuleType("ansys.aedt.core")
        core_module.__version__ = "0.22.0"
        ansys_module.aedt = aedt_module
        aedt_module.core = core_module
        with tempfile.TemporaryDirectory() as root:
            host = AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                artifact_root=root,
                dso_profile=SUPPORTED_DSO_PROFILE,
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
            )
            self.assertEqual(host.aedt_version, "2025.2")
            host.session_id = 17
            host._prepare_artifacts({"session_key": "attestation"})
            host.desktop = SimpleNamespace(
                odesktop=SimpleNamespace(GetVersion=lambda: "Ansys 2025 R2 (2025.2.0)")
            )
            with (
                patch.dict(
                    sys.modules,
                    {
                        "ansys": ansys_module,
                        "ansys.aedt": aedt_module,
                        "ansys.aedt.core": core_module,
                    },
                ),
                patch.dict(
                    os.environ, {"CONDA_DEFAULT_ENV": "pyaedt2026v1"}, clear=False
                ),
            ):
                metadata = host._attest_runtime_profile()

            self.assertEqual(metadata["aedt_version"], "2025.2")
            self.assertEqual(metadata["pyaedt_version"], "0.22.0")
            self.assertEqual(metadata["python_executable"], sys.executable)
            evidence = Path(host.artifact_dir) / "runtime-attestation.json"
            self.assertTrue(evidence.is_file())

            with (
                patch.dict(
                    sys.modules,
                    {
                        "ansys": ansys_module,
                        "ansys.aedt": aedt_module,
                        "ansys.aedt.core": core_module,
                    },
                ),
                patch.dict(
                    os.environ, {"CONDA_DEFAULT_ENV": "wrong-env"}, clear=False
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "environment drift"):
                    host._attest_runtime_profile()

    def test_artifact_prepare_failure_is_reported_immediately_with_paths(self) -> None:
        reports: list[dict] = []

        class Control:
            def request(self, method, path, payload=None, host_token=""):
                if path.endswith("claim-start"):
                    return {"session": {"id": 19, "session_key": "mkdir-fail"}}
                if path.endswith("/start-failed"):
                    reports.append(dict(payload or {}))
                    return {}
                raise AssertionError(path)

        host = AedtSessionHost(
            Control(),
            allocation_id=1,
            node_name="cpu-01",
            artifact_root="/shared/aedt-artifacts",
        )
        with patch.object(Path, "mkdir", side_effect=PermissionError("GPFS denied")):
            self.assertEqual(host.run(), 1)

        self.assertEqual(len(reports), 1)
        self.assertIn("GPFS denied", reports[0]["failure_message"])
        self.assertTrue(reports[0]["artifact_dir"].endswith("19-mkdir-fail"))
        self.assertTrue(reports[0]["error_log_path"].endswith("pyaedt.log"))
        self.assertTrue(reports[0]["journal_path"].endswith("session-events.jsonl"))
        self.assertEqual(
            reports[0]["runtime_metadata"]["startup_failure"], "GPFS denied"
        )

    def test_native_diagnostic_snapshot_captures_messages_and_log_tail(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            host = AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                artifact_root=root,
            )
            host.session_id = 23
            host._prepare_artifacts({"session_key": "native-snapshot"})
            Path(host.error_log_path).write_text(
                "old line\nAEDT transport warning\n", encoding="utf-8"
            )
            host.desktop_process_id = "23001"
            host.desktop_port = 50023
            host.desktop = SimpleNamespace(
                odesktop=SimpleNamespace(
                    GetMessages=lambda *_args: ["solver busy", "native timeout"]
                )
            )
            snapshot_path = host._capture_native_diagnostics("GetVersion blocked")
            snapshot = json.loads(Path(snapshot_path).read_text(encoding="utf-8"))

        self.assertEqual(snapshot["process_id"], "23001")
        self.assertIn("native timeout", snapshot["messages"])
        self.assertIn("AEDT transport warning", snapshot["pyaedt_log_tail"])

    def test_native_diagnostic_write_failure_is_best_effort(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            host = AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                artifact_root=root,
            )
            host.session_id = 24
            host._prepare_artifacts({"session_key": "snapshot-write-failure"})
            host.desktop_process_id = "24001"
            host.desktop_port = 50024
            host.desktop = SimpleNamespace(
                odesktop=SimpleNamespace(GetMessages=lambda *_args: [])
            )
            with patch.object(
                Path, "write_text", side_effect=PermissionError("GPFS read-only")
            ):
                snapshot_path = host._capture_native_diagnostics(
                    "GetVersion blocked"
                )
            with (
                patch.object(host, "_process_alive", return_value=True),
                patch.object(host, "_desktop_port_is_listening", return_value=True),
                patch.object(host, "_native_desktop_responds", return_value=False),
            ):
                healthy, reason = host._desktop_liveness_proof()

        self.assertEqual(snapshot_path, "")
        self.assertTrue(healthy)
        self.assertIn("native GetVersion", reason)

    def test_exhausted_control_plane_window_preserves_live_desktop(self) -> None:
        events: list[str] = []
        host = AedtSessionHost(
            FakeHostControlPlane(events),
            allocation_id=1,
            node_name="cpu-01",
            heartbeat_seconds=5,
        )
        host._start_desktop = lambda: FakeDesktop(events)
        trust_fake_desktop_liveness(host)
        heartbeat_count = 0

        def control_request(method, path, payload=None, **_kwargs):
            nonlocal heartbeat_count
            if path.endswith("claim-start"):
                return {"session": {"id": 1, "session_key": "offline-safe"}}
            if path.endswith("/register"):
                return {"host_token": "host-token"}
            if path.endswith("/heartbeat"):
                heartbeat_count += 1
                if heartbeat_count == 1:
                    events.append("control-plane-window-exhausted")
                    raise ControlPlaneUnavailable("relay remained offline")
                events.append("heartbeat-recovered")
                return {}
            if path.endswith("/commands"):
                return {"drain": True, "sibling_live_count": 0}
            return {}

        host._control_plane_request = control_request
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 0)

        self.assertLess(
            events.index("control-plane-window-exhausted"),
            events.index("heartbeat-recovered"),
        )
        self.assertLess(events.index("heartbeat-recovered"), events.index("desktop-close"))

    def test_missing_pyaedt_dso_template_fails_before_registry_load(self) -> None:
        with patch(
            "slurm_scheduler.aedt_session_host.importlib.resources.files",
            side_effect=FileNotFoundError("missing template"),
        ):
            with self.assertRaisesRegex(RuntimeError, "bundled DSO template is unavailable"):
                _load_pyaedt_dso_template()

    def test_minimal_pyaedt_dso_template_is_rejected(self) -> None:
        minimal = """$begin 'DSOConfig'
ConfigName='pyaedt_config'
DesignType='HFSS'
NumEngines=1
NumCores=4
NumGPUs=0
UseAutoSettings=true
$end 'DSOConfig'
"""
        with self.assertRaisesRegex(RuntimeError, "incomplete; missing ACF blocks"):
            _render_dso_configuration(
                minimal,
                design_type="Maxwell 3D",
                use_auto_settings=True,
            )

    def test_host_initializes_maxwell_and_icepak_dso_profiles_once(self) -> None:
        class RegistryDesktop:
            def __init__(self) -> None:
                self.loaded: list[str] = []
                self.values: dict[str, str] = {}

            def SetRegistryFromFile(self, path: str):
                self.loaded.append(Path(path).read_text(encoding="utf-8"))
                return True

            def SetRegistryString(self, key: str, value: str):
                self.values[key] = value
                return True

            def GetRegistryString(self, key: str):
                return self.values.get(key, "")

        with tempfile.TemporaryDirectory() as root, patch(
            "slurm_scheduler.aedt_session_host._load_pyaedt_dso_template",
            return_value=VALID_PYAEDT_DSO_TEMPLATE,
        ):
            registry = RegistryDesktop()
            host = AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                artifact_root=root,
                dso_profile=SUPPORTED_DSO_PROFILE,
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
            )
            host.session_id = 7
            host._prepare_artifacts({"session_key": "dso-test"})
            host.desktop = SimpleNamespace(odesktop=registry)
            host._initialize_dso_configuration()

            self.assertEqual(len(registry.loaded), 3)
            for text in registry.loaded:
                self.assertEqual(text.count("$begin 'Configs'"), 2)
                self.assertEqual(text.count("$end 'Configs'"), 2)
                self.assertEqual(text.count("$begin 'DSOConfig'"), 1)
                self.assertIn("$begin 'DSOMachineList'", text)
                self.assertIn("$begin 'DSOMachineInfo'", text)
                self.assertIn("$begin 'DSOJobDistributionInfo'", text)
                self.assertIn("$begin 'DSOMachineOptionsInfo'", text)
                self.assertIn("AllowedDistributionTypes[9:", text)
                self.assertIn("BoolValues(AllowOffCore=true)", text)
                self.assertLess(
                    text.index("$begin 'DSOMachineList'"),
                    text.index("$begin 'DSOMachineInfo'"),
                )
                self.assertLess(
                    text.index("$end 'DSOMachineInfo'"),
                    text.index("$end 'DSOMachineList'"),
                )
            self.assertTrue(any("DesignType='Maxwell 3D'" in text for text in registry.loaded))
            self.assertTrue(any("DesignType='Maxwell 2D'" in text for text in registry.loaded))
            self.assertTrue(any("DesignType='Icepak'" in text for text in registry.loaded))
            self.assertTrue(all("NumCores=4" in text for text in registry.loaded))
            self.assertTrue(all("NumEngines=1" in text for text in registry.loaded))
            icepak = next(
                text for text in registry.loaded if "DesignType='Icepak'" in text
            )
            self.assertIn("UseAutoSettings=false", icepak)
            self.assertTrue(
                all(
                    "UseAutoSettings=true" in text
                    for text in registry.loaded
                    if "DesignType='Maxwell" in text
                )
            )
            self.assertEqual(
                registry.values["Desktop/ActiveDSOConfigurations/Maxwell 3D"],
                "pyaedt_config",
            )
            self.assertEqual(
                registry.values["Desktop/ActiveDSOConfigurations/Maxwell 2D"],
                "pyaedt_config",
            )
            self.assertEqual(
                registry.values["Desktop/ActiveDSOConfigurations/Icepak"],
                "pyaedt_config",
            )

    def test_legacy_maxwell_profile_alias_also_initializes_icepak(self) -> None:
        registry = SimpleNamespace(
            values={},
            SetRegistryFromFile=lambda _path: True,
        )
        registry.SetRegistryString = lambda key, value: (
            registry.values.__setitem__(key, value) or True
        )
        registry.GetRegistryString = lambda key: registry.values.get(key, "")
        with tempfile.TemporaryDirectory() as root, patch(
            "slurm_scheduler.aedt_session_host._load_pyaedt_dso_template",
            return_value=VALID_PYAEDT_DSO_TEMPLATE,
        ):
            host = AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                artifact_root=root,
                dso_profile=LEGACY_DSO_PROFILE,
            )
            host.session_id = 8
            host._prepare_artifacts({"session_key": "legacy-dso-test"})
            host.desktop = SimpleNamespace(odesktop=registry)
            host._initialize_dso_configuration()

        self.assertEqual(
            registry.values["Desktop/ActiveDSOConfigurations/Icepak"],
            "pyaedt_config",
        )

    def test_canonical_dso_profile_rejects_missing_or_drifted_session_profile(self) -> None:
        with self.assertRaisesRegex(ValueError, "exact host session profile"):
            AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                dso_profile=SUPPORTED_DSO_PROFILE,
            )
        drifted = json.loads(EXPECTED_SESSION_PROFILE_JSON)
        drifted["desktop_dso"]["designs"]["Icepak"]["use_auto_settings"] = True
        with self.assertRaisesRegex(ValueError, "does not match"):
            AedtSessionHost(
                FakeHostControlPlane([]),
                allocation_id=1,
                node_name="cpu-01",
                dso_profile=SUPPORTED_DSO_PROFILE,
                session_profile=drifted,
            )

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

    def test_wrapper_pid_disambiguates_linux_launcher_child_layout(self) -> None:
        wrapper = SimpleNamespace(
            port=46529, aedt_process_id="3824121", odesktop=object()
        )
        reported_process = SimpleNamespace(
            name=lambda: "ansysedt",
            exe=lambda: "/ansys/v252/Linux64/ansysedt",
            username=lambda: "cluster-user",
            uids=lambda: SimpleNamespace(effective=1001),
            create_time=lambda: 105.0,
            cmdline=lambda: ["/ansys/v252/Linux64/ansysedt", "-ng"],
            net_connections=lambda **_kwargs: [],
        )

        def process(pid):
            if int(pid) == os.getpid():
                return SimpleNamespace(
                    username=lambda: "cluster-user",
                    uids=lambda: SimpleNamespace(effective=1001),
                )
            if int(pid) == 3824121:
                return reported_process
            raise AssertionError(pid)

        with (
            patch.dict(
                "sys.modules",
                {"psutil": SimpleNamespace(Process=process, CONN_LISTEN="LISTEN")},
            ),
            patch.object(
                AedtSessionHost, "_owned_desktop_pid_on_port", return_value=""
            ),
            patch.object(
                AedtSessionHost, "_desktop_port_is_listening", return_value=True
            ),
        ):
            validated = AedtSessionHost._validate_owned_desktop(
                wrapper, expected_port=46529, started_after=100.0
            )
        self.assertIs(validated, wrapper)

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
            patch.object(
                AedtSessionHost,
                "_owned_desktop_pid_on_port",
                return_value="3824121",
            ),
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

    def test_launch_retry_does_not_reuse_first_pyaedt_wrapper_on_new_port(self) -> None:
        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        settings = SimpleNamespace(use_multi_desktop=False)
        cached: list[SimpleNamespace] = []
        constructor_calls: list[tuple[bool, int, bool]] = []

        def desktop_factory(**kwargs):
            new_desktop = bool(kwargs["new_desktop"])
            port = int(kwargs["port"])
            constructor_calls.append(
                (new_desktop, port, bool(settings.use_multi_desktop))
            )
            if cached and (not settings.use_multi_desktop or not new_desktop):
                return cached[0]
            desktop = SimpleNamespace(
                port=port,
                aedt_process_id=str(3800000 + port),
                odesktop=object(),
            )
            cached[:] = [desktop]
            return desktop

        ansys_module = ModuleType("ansys")
        aedt_module = ModuleType("ansys.aedt")
        core_module = ModuleType("ansys.aedt.core")
        core_module.Desktop = desktop_factory
        core_module.settings = settings
        validation_calls = 0

        def validate(desktop, *, expected_port, started_after):
            nonlocal validation_calls
            validation_calls += 1
            if validation_calls <= 2:
                raise RuntimeError("injected ownership proof race")
            return AedtSessionHost._validate_desktop(
                desktop, expected_port=expected_port
            )

        with (
            patch.dict(
                sys.modules,
                {
                    "ansys": ansys_module,
                    "ansys.aedt": aedt_module,
                    "ansys.aedt.core": core_module,
                },
            ),
            patch.object(
                host, "_find_free_desktop_port", side_effect=[46529, 47981]
            ),
            patch.object(host, "_desktop_port_is_listening", return_value=True),
            patch.object(host, "_validate_owned_desktop", side_effect=validate),
            patch.object(host, "_cleanup_failed_desktop_launch"),
            patch(
                "slurm_scheduler.aedt_session_host._install_pyaedt_psutil_cmdline_shim"
            ),
            patch("slurm_scheduler.aedt_session_host.time.sleep"),
        ):
            desktop = host._start_desktop()

        self.assertEqual(desktop.port, 47981)
        self.assertTrue(settings.use_multi_desktop)
        self.assertEqual(
            constructor_calls,
            [
                (True, 46529, True),
                (False, 46529, True),
                (True, 47981, True),
            ],
        )

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
            patch.object(
                AedtSessionHost,
                "_owned_desktop_pid_on_port",
                return_value="3824121",
            ),
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
            [call.args[0] for call in sleep.call_args_list],
            [
                host._desktop_launch_retry_delay(1),
                host._desktop_launch_retry_delay(2),
            ],
        )
        self.assertGreater(sleep.call_args_list[1].args[0], sleep.call_args_list[0].args[0])

    def test_primary_launch_rejects_wrapper_pid_mismatch(self) -> None:
        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        wrapper = SimpleNamespace(
            port=44773, aedt_process_id="3824121", odesktop=object()
        )
        host._create_desktop = lambda **_kwargs: wrapper
        with (
            patch.object(host, "_find_free_desktop_port", return_value=44773),
            patch.object(
                AedtSessionHost,
                "_owned_desktop_pid_on_port",
                return_value="3824999",
            ),
            patch.object(host, "_desktop_port_is_listening", return_value=True),
            patch.object(host, "_cleanup_failed_desktop_launch"),
            patch("slurm_scheduler.aedt_session_host.time.sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "reported PID 3824121"):
                host._start_desktop()

    def test_failed_launch_cleanup_refuses_unattested_wrapper_pid(self) -> None:
        host = AedtSessionHost(
            FakeHostControlPlane([]), allocation_id=1, node_name="cpu-01"
        )
        wrapper = SimpleNamespace(aedt_process_id="3824121")
        with (
            patch.object(
                AedtSessionHost,
                "_owned_desktop_pid_on_port",
                return_value="3824999",
            ),
            patch.object(host, "_force_kill_owned_desktop") as force_kill,
        ):
            host._cleanup_failed_desktop_launch(
                wrapper, port=44773, started_after=100.0
            )
        force_kill.assert_not_called()

    def test_registration_remote_disconnect_retries_without_closing_desktop(
        self,
    ) -> None:
        events: list[str] = []
        control = RemoteDisconnectRegistrationControlPlane(events)
        host = AedtSessionHost(control, allocation_id=1, node_name="cpu-01")
        host._start_desktop = lambda: FakeDesktop(events)
        trust_fake_desktop_liveness(host)

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
        trust_fake_desktop_liveness(host)

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
        trust_fake_desktop_liveness(host)

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
        trust_fake_desktop_liveness(host)

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
        trust_fake_desktop_liveness(host)
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
        trust_fake_desktop_liveness(host)
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
        trust_fake_desktop_liveness(host)
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
        trust_fake_desktop_liveness(host)
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
        trust_fake_desktop_liveness(host)
        with patch("slurm_scheduler.aedt_session_host.time.sleep", return_value=None):
            self.assertEqual(host.run(), 1)

        self.assertEqual(len(control.registration_tokens), 3)
        self.assertEqual(len(set(control.registration_tokens)), 1)
        self.assertLess(events.index("register-3"), events.index("desktop-close"))
        self.assertIn("start-failed", events)


if __name__ == "__main__":
    unittest.main()
