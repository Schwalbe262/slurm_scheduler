from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException


class AedtPoolModuleFlagTests(unittest.TestCase):
    def _create_app(self, root: Path, *, module_enabled: bool | None = None):
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
        config_lines = [
            f'database_path: "{(root / "scheduler.db").as_posix()}"',
            f'accounts_path: "{accounts_path.as_posix()}"',
            "min_warm_allocations: 0",
            "cluster_refresh_interval_seconds: 0",
            "reconcile_on_start: false",
            "backup_enabled: false",
            "web_listener_watchdog_enabled: false",
        ]
        if module_enabled is not None:
            config_lines.extend(
                [
                    "aedt_pool:",
                    f"  module_enabled: {str(module_enabled).lower()}",
                ]
            )
        config_path = root / "app.yaml"
        config_path.write_text("\n".join(config_lines), encoding="utf-8")
        with patch.dict(
            os.environ,
            {
                "SLURM_SCHEDULER_CONFIG": str(config_path),
                "SLURM_SCHEDULER_SERVICE_PROCESS": "",
            },
            clear=False,
        ):
            from slurm_scheduler.app import create_app

            app = create_app(str(config_path))
        return app, root / "scheduler.db"

    @staticmethod
    def _route_paths(app) -> set[str]:
        return {
            str(getattr(route, "path", ""))
            for route in app.routes
            if getattr(route, "path", "")
        }

    def test_default_disables_pool_tables_routes_runtime_and_submission(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            pool_threads_before = {
                thread.ident
                for thread in threading.enumerate()
                if thread.name == "aedt-pool"
            }
            app, database_path = self._create_app(Path(tmp))
            app.router.on_startup.clear()
            app.router.on_shutdown.clear()

            self.assertIsNone(app.state.aedt_pool)
            self.assertIsNone(app.state.aedt_pool_runtime)
            self.assertIsNone(app.state.control_plane_relay)
            self.assertEqual(
                {
                    thread.ident
                    for thread in threading.enumerate()
                    if thread.name == "aedt-pool"
                },
                pool_threads_before,
            )
            paths = self._route_paths(app)
            self.assertNotIn("/aedt-pool", paths)
            self.assertFalse(any(path.startswith("/api/aedt-pool") for path in paths))
            with sqlite3.connect(database_path) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            self.assertFalse(any(name.startswith("aedt_") for name in tables))

            endpoint = next(
                route.endpoint
                for route in app.routes
                if getattr(route, "path", "") == "/api/tasks"
                and "POST" in getattr(route, "methods", set())
            )

            class Request:
                async def json(self):
                    return {
                        "name": "pooled-disabled",
                        "remote_cwd": "/work",
                        "command": "true",
                        "aedt_backend": "pooled",
                    }

            with self.assertRaises(HTTPException) as raised:
                asyncio.run(endpoint(Request()))
            self.assertEqual(raised.exception.status_code, 422)
            self.assertEqual(
                raised.exception.detail,
                "AEDT pooled backend module is disabled",
            )

    def test_enabled_initializes_service_and_mounts_router(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            app, database_path = self._create_app(
                Path(tmp), module_enabled=True
            )
            app.router.on_startup.clear()
            app.router.on_shutdown.clear()

            self.assertIsNotNone(app.state.aedt_pool)
            self.assertIsNotNone(app.state.aedt_pool_runtime)
            self.assertIn("/aedt-pool", self._route_paths(app))
            self.assertIn("/api/aedt-pool", self._route_paths(app))
            with sqlite3.connect(database_path) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            self.assertIn("aedt_sessions", tables)
            self.assertIn("aedt_project_leases", tables)


if __name__ == "__main__":
    unittest.main()
