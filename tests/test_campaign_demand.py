from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fastapi import HTTPException, Response

from slurm_scheduler.db import Database
from slurm_scheduler.campaign_mutation_lock import campaign_mutation_lock
from slurm_scheduler.models import TaskCreate, TaskStatus


PROJECT = "MFT_1MW_2026v1"


class CampaignMutationLockTests(unittest.TestCase):
    def test_lock_excludes_another_process_on_the_same_host_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "campaign-mutation.lock"
            child = """
import sys
from slurm_scheduler.campaign_mutation_lock import campaign_mutation_lock
try:
    with campaign_mutation_lock(sys.argv[1], timeout_seconds=0.15, poll_seconds=0.01):
        print('unexpected-acquire')
except TimeoutError:
    print('blocked-as-expected')
"""
            with campaign_mutation_lock(lock_path, timeout_seconds=1):
                result = subprocess.run(
                    [sys.executable, "-c", child, str(lock_path)],
                    cwd=Path(__file__).resolve().parents[1],
                    text=True,
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "blocked-as-expected")


class CampaignDemandDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "scheduler.db")
        self.db = Database(self.db_path)
        self.db.init()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_mft_default_is_500_and_persists_without_resetting_a_decrease(self) -> None:
        project_id = self.db.create_project(PROJECT, max_active_tasks=30)
        created = self.db.get_project(project_id)
        self.assertEqual(created["campaign_total_simulations"], 500)
        self.assertEqual(created["campaign_demand_revision"], 1)

        status, increased = self.db.update_project_campaign_demand(
            PROJECT,
            total_simulations=750,
            expected_revision=1,
            updated_by="test-operator",
        )
        self.assertEqual(status, "updated")
        self.assertEqual(increased["campaign_total_simulations"], 750)
        self.assertEqual(increased["campaign_demand_revision"], 2)

        status, decreased = self.db.update_project_campaign_demand(
            PROJECT,
            total_simulations=0,
            expected_revision=2,
            updated_by="test-operator",
        )
        self.assertEqual(status, "updated")
        self.assertEqual(decreased["campaign_total_simulations"], 0)

        reopened = Database(self.db_path)
        reopened.init()
        persisted = reopened.get_project_by_name(PROJECT)
        self.assertEqual(persisted["campaign_total_simulations"], 0)
        self.assertEqual(persisted["campaign_demand_revision"], 3)
        self.assertEqual(persisted["campaign_demand_updated_by"], "test-operator")

    def test_existing_projects_table_migrates_default_exactly_once(self) -> None:
        legacy_path = str(Path(self.tmp.name) / "legacy.db")
        conn = sqlite3.connect(legacy_path)
        try:
            conn.executescript(
                """
                CREATE TABLE projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    repos TEXT NOT NULL DEFAULT '[]',
                    setup TEXT NOT NULL DEFAULT '',
                    entrypoints TEXT NOT NULL DEFAULT '[]',
                    cleanup_globs TEXT NOT NULL DEFAULT '',
                    output_globs TEXT NOT NULL DEFAULT '',
                    sim_subdir TEXT NOT NULL DEFAULT 'simulation',
                    auto_pull INTEGER NOT NULL DEFAULT 0,
                    max_active_tasks INTEGER NOT NULL DEFAULT 0,
                    desired_simulations INTEGER NOT NULL DEFAULT 0,
                    policy_revision INTEGER NOT NULL DEFAULT 1,
                    validated_concurrency_limit INTEGER NOT NULL DEFAULT 0,
                    scale_down_mode TEXT NOT NULL DEFAULT 'drain',
                    aedt_backend TEXT NOT NULL DEFAULT 'standalone',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO projects(
                    name, max_active_tasks, desired_simulations,
                    validated_concurrency_limit
                ) VALUES('MFT_1MW_2026v1', 500, 30, 30);
                """
            )
            conn.commit()
        finally:
            conn.close()

        migrated = Database(legacy_path)
        migrated.init()
        project = migrated.get_project_by_name(PROJECT)
        self.assertEqual(project["campaign_total_simulations"], 500)
        self.assertEqual(project["campaign_demand_updated_by"], "migration:q22-default")
        status, _ = migrated.update_project_campaign_demand(
            PROJECT,
            total_simulations=0,
            expected_revision=1,
            updated_by="operator-decrease",
        )
        self.assertEqual(status, "updated")
        migrated.init()
        self.assertEqual(
            migrated.get_project_by_name(PROJECT)["campaign_total_simulations"], 0
        )

    def test_absolute_same_value_is_idempotent_and_stale_cas_conflicts(self) -> None:
        self.db.create_project(PROJECT)
        status, unchanged = self.db.update_project_campaign_demand(
            PROJECT,
            total_simulations=500,
            expected_revision=1,
            updated_by="retry",
        )
        self.assertEqual(status, "unchanged")
        self.assertEqual(unchanged["campaign_demand_revision"], 1)
        self.assertFalse(
            any(event["kind"] == "project_campaign_demand_updated" for event in self.db.list_events())
        )

        status, updated = self.db.update_project_campaign_demand(
            PROJECT,
            total_simulations=600,
            expected_revision=1,
            updated_by="operator-a",
        )
        self.assertEqual(status, "updated")
        self.assertEqual(updated["campaign_demand_revision"], 2)
        status, current = self.db.update_project_campaign_demand(
            PROJECT,
            total_simulations=700,
            expected_revision=1,
            updated_by="stale-operator",
        )
        self.assertEqual(status, "conflict")
        self.assertEqual(current["campaign_total_simulations"], 600)
        self.assertEqual(current["campaign_demand_revision"], 2)

    def test_decrease_does_not_mutate_or_cancel_any_live_task(self) -> None:
        self.db.create_project(PROJECT)
        task_ids = []
        for index, status in enumerate(
            (TaskStatus.QUEUED.value, TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value)
        ):
            task_id = self.db.create_task(
                TaskCreate(
                    f"campaign-live-{index}",
                    "~/case",
                    "run",
                    project=PROJECT,
                )
            )
            self.db.update_task(task_id, status=status)
            task_ids.append(task_id)

        before = {task_id: self.db.get_task(task_id)["status"] for task_id in task_ids}
        status, updated = self.db.update_project_campaign_demand(
            PROJECT,
            total_simulations=0,
            expected_revision=1,
            updated_by="decrease-test",
        )
        self.assertEqual(status, "updated")
        self.assertEqual(updated["campaign_total_simulations"], 0)
        after = {task_id: self.db.get_task(task_id)["status"] for task_id in task_ids}
        self.assertEqual(after, before)
        audit = next(
            event
            for event in self.db.list_events()
            if event["kind"] == "project_campaign_demand_updated"
        )
        self.assertEqual(json.loads(audit["message"])["tasks_cancelled"], 0)


class CampaignDemandApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        accounts_path = root / "accounts.yaml"
        accounts_path.write_text(
            "\n".join(
                [
                    "accounts:",
                    "  - name: test",
                    "    host: invalid",
                    "    port: 22",
                    "    username: test",
                    "    private_key_path: key",
                    "    remote_workspace: /work",
                ]
            ),
            encoding="utf-8",
        )
        config_path = root / "app.yaml"
        config_path.write_text(
            "\n".join(
                [
                    f'database_path: "{(root / "scheduler.db").as_posix()}"',
                    f'accounts_path: "{accounts_path.as_posix()}"',
                    f'mft_campaign_mutation_lock_path: "{(root / "campaign-mutation.lock").as_posix()}"',
                    "mft_campaign_mutation_lock_timeout_seconds: 2",
                    "min_warm_allocations: 0",
                    "cluster_refresh_interval_seconds: 0",
                    "reconcile_on_start: false",
                    "backup_enabled: false",
                ]
            ),
            encoding="utf-8",
        )
        previous = os.environ.get("SLURM_SCHEDULER_CONFIG")
        os.environ["SLURM_SCHEDULER_CONFIG"] = str(config_path)
        try:
            from slurm_scheduler.app import create_app
        finally:
            if previous is None:
                os.environ.pop("SLURM_SCHEDULER_CONFIG", None)
            else:
                os.environ["SLURM_SCHEDULER_CONFIG"] = previous
        self.app = create_app(str(config_path))
        self.app.router.on_startup.clear()
        self.app.router.on_shutdown.clear()
        self.app.state.db.create_project(PROJECT)
        self.get_demand = self._route("/api/projects/{name}/campaign-demand", "GET")
        self.patch_demand = self._route("/api/projects/{name}/campaign-demand", "PATCH")
        self.get_policy = self._route("/api/projects/{name}/simulation-policy", "GET")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _route(self, path: str, method: str):
        return next(
            route.endpoint
            for route in self.app.routes
            if getattr(route, "path", "") == path
            and method in getattr(route, "methods", set())
        )

    @staticmethod
    def _request(payload: dict, *, actor: str = "api-test"):
        request_headers = {"x-operator-identity": actor}

        class JsonRequest:
            headers = request_headers

            async def json(self) -> dict:
                return payload

        return JsonRequest()

    def test_get_patch_audit_etag_and_stale_cas(self) -> None:
        get_response = Response()
        initial = self.get_demand(PROJECT, get_response)
        self.assertEqual(initial["total_simulations"], 500)
        self.assertEqual(initial["demand_revision"], 1)
        self.assertEqual(initial["progress_source"], "feeder_manifest")
        self.assertIsNone(initial["accepted_simulations"])
        self.assertEqual(get_response.headers["etag"], 'W/"campaign-demand-1"')

        patch_response = Response()
        updated = asyncio.run(
            self.patch_demand(
                PROJECT,
                self._request(
                    {"total_simulations": 900, "expected_revision": 1},
                    actor="web-operator",
                ),
                patch_response,
            )
        )
        self.assertEqual(updated["total_simulations"], 900)
        self.assertEqual(updated["demand_revision"], 2)
        self.assertEqual(updated["updated_by"], "web-operator")
        self.assertEqual(patch_response.headers["etag"], 'W/"campaign-demand-2"')

        with self.assertRaises(HTTPException) as conflict:
            asyncio.run(
                self.patch_demand(
                    PROJECT,
                    self._request({"total_simulations": 1000, "expected_revision": 1}),
                    Response(),
                )
            )
        self.assertEqual(conflict.exception.status_code, 409)
        self.assertEqual(conflict.exception.detail["current"]["total_simulations"], 900)
        self.assertEqual(conflict.exception.detail["current"]["demand_revision"], 2)

    def test_invalid_payloads_fail_without_changing_demand(self) -> None:
        invalid_payloads = (
            {"total_simulations": True, "expected_revision": 1},
            {"total_simulations": -1, "expected_revision": 1},
            {"total_simulations": 500, "expected_revision": 0},
            {"total_simulations": 500, "expected_revision": 1, "cancel": True},
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload), self.assertRaises(HTTPException) as invalid:
                asyncio.run(
                    self.patch_demand(PROJECT, self._request(payload), Response())
                )
            self.assertEqual(invalid.exception.status_code, 422)
        self.assertEqual(
            self.get_demand(PROJECT, Response())["total_simulations"], 500
        )

    def test_mft_active_concurrency_remains_capped_at_30(self) -> None:
        project = self.app.state.db.get_project_by_name(PROJECT)
        self.app.state.db.update_project(
            int(project["id"]),
            max_active_tasks=500,
            desired_simulations=500,
            validated_concurrency_limit=500,
        )
        policy = self.get_policy(PROJECT)
        self.assertEqual(policy["desired_simulations"], 30)
        self.assertEqual(policy["effective_simulations"], 30)
        self.assertEqual(policy["max_desired_simulations"], 30)


class CampaignDemandUiContractTests(unittest.TestCase):
    def test_project_ui_enables_distinct_total_and_active_controls(self) -> None:
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "project_detail.html"
        ).read_text(encoding="utf-8")
        self.assertIn("Total simulations requested (campaign budget)", template)
        self.assertIn("Active simulations at once (rolling concurrency, max 30)", template)
        self.assertIn('id="campaign-total-simulations"', template)
        self.assertIn('id="desired-active-simulations"', template)
        self.assertNotIn('id="campaign-total-simulations" disabled', template)
        self.assertNotIn('id="desired-active-simulations" disabled', template)
        self.assertIn("/campaign-demand`", template)
        self.assertIn("/simulation-policy`", template)
        self.assertIn("queued, attaching,", template)
        self.assertIn("never cancelled", template)


if __name__ == "__main__":
    unittest.main()
