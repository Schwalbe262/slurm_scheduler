from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from slurm_scheduler.db import Database
from slurm_scheduler.aedt_pool import AedtPoolService
from slurm_scheduler.scheduler import Scheduler
from slurm_scheduler.slurm import SlurmAccountClient


async def request(app, path: str, method: str = "GET") -> tuple[int, bytes]:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "root_path": "",
    }
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    messages: list[dict] = []

    async def send(message):
        messages.append(dict(message))

    await app(scope, receive, send)
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
    return status, body


class ObserverModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        root = Path(self.tmp.name)
        self.database_path = root / "scheduler.db"
        Database(str(self.database_path)).init()
        accounts_path = root / "accounts.yaml"
        accounts_path.write_text(
            "accounts:\n"
            "  - name: test\n"
            "    host: invalid\n"
            "    port: 22\n"
            "    username: test\n"
            "    private_key_path: key\n"
            "    remote_workspace: /work\n",
            encoding="utf-8",
        )
        self.config_path = root / "app.yaml"
        self.config_path.write_text(
            f'database_path: "{self.database_path.as_posix()}"\n'
            f'accounts_path: "{accounts_path.as_posix()}"\n'
            "observer_mode: true\n"
            "min_warm_allocations: 1\n"
            "gpu_prewarm:\n  enabled: true\n  min_warm_allocations: 2\n"
            "cleanup:\n  enabled: true\n  orphan_sweep_enabled: true\n"
            "backup_enabled: true\n"
            "reconcile_on_start: true\n",
            encoding="utf-8",
        )
        with mock.patch.dict(os.environ, {"SLURM_SCHEDULER_CONFIG": str(self.config_path)}):
            from slurm_scheduler.app import create_app

            self.app = create_app(str(self.config_path))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_startup_skips_scheduler_and_all_slurm_mutation(self) -> None:
        with (
            mock.patch.object(Scheduler, "start") as start,
            mock.patch.object(SlurmAccountClient, "submit") as submit,
            mock.patch.object(SlurmAccountClient, "submit_allocation") as allocation,
            mock.patch.object(SlurmAccountClient, "cancel") as cancel,
            mock.patch.object(SlurmAccountClient, "cancel_task") as cancel_task,
            mock.patch("slurm_scheduler.app.cleanup_local_temp_artifacts") as cleanup,
        ):
            self.app.router.on_startup[-1]()
            self.app.router.on_shutdown[-1]()
            for path in ("/healthz", "/api/health", "/api/inventory/freshness", "/"):
                status, _ = asyncio.run(request(self.app, path))
                self.assertEqual(status, 200, path)
            start.assert_not_called()
            cleanup.assert_not_called()
            submit.assert_not_called()
            allocation.assert_not_called()
            cancel.assert_not_called()
            cancel_task.assert_not_called()
        self.assertTrue(json.loads(asyncio.run(request(self.app, "/api/health"))[1])["observer_mode"])

    def test_all_mutating_methods_are_blocked_before_route_execution(self) -> None:
        for method, path in (
            ("POST", "/api/tasks"),
            ("PATCH", "/api/projects/test/max-active-tasks"),
            ("PUT", "/api/tasks/1"),
            ("DELETE", "/api/tasks/1"),
            ("POST", "/api/placement/dry-run"),
        ):
            with self.subTest(method=method, path=path):
                status, body = asyncio.run(request(self.app, path, method))
                self.assertEqual(status, 403)
                self.assertIn(b"observer mode", body)

    def test_database_connections_cannot_write(self) -> None:
        with self.assertRaises(sqlite3.OperationalError):
            self.app.state.db.set_setting("observer-test", "blocked")
        with self.assertRaises(RuntimeError):
            self.app.state.db.init()

    def test_missing_database_fails_at_startup_without_creating_it(self) -> None:
        missing = self.database_path.parent / "missing.db"
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                self.database_path.as_posix(), missing.as_posix()
            ),
            encoding="utf-8",
        )
        from slurm_scheduler.app import create_app

        with self.assertRaises(FileNotFoundError):
            create_app(str(self.config_path))
        self.assertFalse(missing.exists())

    def test_enabled_pool_stays_inactive_and_dashboard_can_read_existing_pool_schema(self) -> None:
        AedtPoolService(Database(str(self.database_path))).init()
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8")
            + "aedt_pool:\n  module_enabled: true\n  session_host_enabled: true\n"
            + "control_plane_relay:\n  enabled: true\n",
            encoding="utf-8",
        )
        from slurm_scheduler.app import create_app

        app = create_app(str(self.config_path))
        with mock.patch.object(Scheduler, "start") as start:
            app.router.on_startup[-1]()
            self.assertIsNone(app.state.aedt_pool_runtime)
            self.assertIsNotNone(app.state.aedt_pool)
            self.assertEqual(asyncio.run(request(app, "/"))[0], 200)
            start.assert_not_called()
        with sqlite3.connect(self.database_path) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT value FROM scheduler_settings WHERE key = 'aedt_pool_adapter_ready'"
                ).fetchone()
            )

    def test_observer_mode_requires_yaml_boolean(self) -> None:
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                "observer_mode: true", 'observer_mode: "false"'
            ),
            encoding="utf-8",
        )
        from slurm_scheduler.config import load_app_config

        with self.assertRaisesRegex(ValueError, "observer_mode must be a YAML boolean"):
            load_app_config(self.config_path)
