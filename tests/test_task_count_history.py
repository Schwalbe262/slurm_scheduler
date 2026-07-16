from __future__ import annotations

import os
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from starlette.requests import Request

from slurm_scheduler.aedt_pool import AedtPoolService
from slurm_scheduler.db import Database, TASK_COUNT_SAMPLE_RETENTION_SECONDS
from slurm_scheduler.models import TaskCreate, TaskStatus
from slurm_scheduler.scheduler import Scheduler


class TaskCountSamplerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "scheduler.db"))
        self.db.init()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_sampler_writes_dashboard_counts_every_minute_and_prunes_seven_days(self) -> None:
        now = 2_000_000_000
        expired_at = now - TASK_COUNT_SAMPLE_RETENTION_SECONDS - 1
        retained_at = now - TASK_COUNT_SAMPLE_RETENTION_SECONDS + 120
        self.db.record_task_count_sample(
            sampled_at=expired_at,
            total_active=8,
            running=4,
            queued=3,
            attaching=1,
        )
        self.db.record_task_count_sample(
            sampled_at=retained_at,
            total_active=5,
            running=2,
            queued=2,
            attaching=1,
        )

        self.db.create_task(TaskCreate("queued", "~/case", "run"))
        running_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        attaching_id = self.db.create_task(TaskCreate("attaching", "~/case", "run"))
        completed_id = self.db.create_task(TaskCreate("completed", "~/case", "run"))
        self.db.update_task(running_id, status=TaskStatus.RUNNING.value)
        self.db.update_task(attaching_id, status=TaskStatus.ATTACHING.value)
        self.db.update_task(completed_id, status=TaskStatus.COMPLETED.value)

        scheduler = Scheduler(self.db, [], 30)
        try:
            self.assertTrue(scheduler.sample_task_counts_if_due(now=now))
            with self.db.connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM task_count_samples ORDER BY sampled_at"
                ).fetchall()
            self.assertEqual([int(row["sampled_at"]) for row in rows], [retained_at, now])
            self.assertEqual(
                {
                    "total_active": int(rows[-1]["total_active"]),
                    "running": int(rows[-1]["running"]),
                    "queued": int(rows[-1]["queued"]),
                    "attaching": int(rows[-1]["attaching"]),
                },
                {"total_active": 3, "running": 1, "queued": 1, "attaching": 1},
            )

            self.assertFalse(scheduler.sample_task_counts_if_due(now=now + 30))
            self.assertTrue(scheduler.sample_task_counts_if_due(now=now + 60))
            with self.db.connect() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) AS count FROM task_count_samples"
                ).fetchone()
            self.assertEqual(int(count["count"]), 3)
        finally:
            scheduler.stop()

    def test_dashboard_summary_preserves_filters_caps_and_tile_classifications(self) -> None:
        older_active = self.db.create_task(
            TaskCreate(
                "campaign-older-active",
                "~/case",
                "run",
                scheduling_profile="fea_bursty",
                gpus=1,
            )
        )
        middle_active = self.db.create_task(
            TaskCreate(
                "campaign-middle-active",
                "~/case",
                "run",
                same_node_as_task_id=123,
            )
        )
        newer_active = self.db.create_task(
            TaskCreate(
                "campaign-newer-active",
                "~/case",
                "run",
                scheduling_profile="fea_bursty",
                gpus=1,
            )
        )
        self.db.update_task(older_active, status=TaskStatus.RUNNING.value)
        self.db.update_task(middle_active, status=TaskStatus.ATTACHING.value)
        self.db.update_task(newer_active, status=TaskStatus.RUNNING.value)
        session_host = self.db.create_task(
            TaskCreate(
                "campaign-session-host",
                "~/case",
                "run",
                scheduling_profile="fea_bursty",
                project="_aedt_pool_hosts",
            )
        )
        self.db.update_task(session_host, status=TaskStatus.RUNNING.value)
        self.db.create_task(TaskCreate("campaign-older-queued", "~/case", "run"))
        self.db.create_task(TaskCreate("campaign-newer-queued", "~/case", "run"))
        unrelated = self.db.create_task(TaskCreate("unrelated", "~/case", "run"))
        self.db.update_task(unrelated, status=TaskStatus.RUNNING.value)

        summary = self.db.task_activity_summary(
            name_contains="campaign",
            active_limit=3,
            queued_limit=1,
        )

        self.assertEqual(
            summary,
            {
                "total": 4,
                "running": 2,
                "attaching": 1,
                "queued": 1,
                "fea": 1,
                "fea_running": 1,
                "standard": 2,
                "gpu": 1,
                "cpu": 3,
                "same_node": 1,
                "aedt_pool_sessions": 0,
                "aedt": 1,
            },
        )

    def test_fea_aedt_counts_pool_sessions_and_only_standalone_running_desktops(self) -> None:
        AedtPoolService(self.db).init()
        with self.db.connect() as conn:
            conn.executemany(
                "INSERT INTO aedt_sessions(session_key, state) VALUES (?, ?)",
                [
                    ("session-starting", "starting"),
                    ("session-ready", "ready"),
                    ("session-busy", "busy"),
                    ("session-draining", "draining"),
                    ("session-unhealthy", "unhealthy"),
                    ("session-closed", "closed"),
                    ("session-failed", "failed"),
                ],
            )

        task_specs = (
            ("desktop-running-standalone", TaskStatus.RUNNING, "fea_bursty", "standalone", ""),
            ("desktop-running-pooled", TaskStatus.RUNNING, "fea_bursty", "pooled", ""),
            ("desktop-attaching-standalone", TaskStatus.ATTACHING, "fea_bursty", "standalone", ""),
            ("desktop-attaching-pooled", TaskStatus.ATTACHING, "fea_bursty", "pooled", ""),
            ("desktop-queued-standalone", TaskStatus.QUEUED, "fea_bursty", "standalone", ""),
            ("desktop-session-host", TaskStatus.RUNNING, "fea_bursty", "standalone", "_aedt_pool_hosts"),
            ("desktop-standard-running", TaskStatus.RUNNING, "standard", "standalone", ""),
        )
        for name, status, profile, backend, project in task_specs:
            task_id = self.db.create_task(
                TaskCreate(
                    name,
                    "~/case",
                    "run",
                    scheduling_profile=profile,
                    aedt_backend=backend,
                    project=project,
                )
            )
            if status != TaskStatus.QUEUED:
                self.db.update_task(task_id, status=status.value)

        summary = self.db.task_activity_summary(name_contains="desktop-")

        self.assertEqual(summary["fea"], 4)
        self.assertEqual(summary["fea_running"], 2)
        self.assertEqual(summary["aedt_pool_sessions"], 5)
        self.assertEqual(summary["aedt"], 6)

    def test_scheduler_tick_runs_sampler_before_later_stage_failure(self) -> None:
        scheduler = Scheduler(
            self.db,
            [],
            30,
            min_warm_allocations=0,
            cluster_refresh_interval_seconds=0,
            cleanup_enabled=False,
            watchdog_enabled=False,
        )
        try:
            with mock.patch.object(
                scheduler,
                "fail_stale_same_node_tasks",
                side_effect=RuntimeError("later stage failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "later stage failed"):
                    scheduler.tick()

            with self.db.connect() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) AS count FROM task_count_samples"
                ).fetchone()
            self.assertEqual(int(count["count"]), 1)
            self.assertIn(
                "task_count_sample",
                scheduler.health_status()["last_tick_stage_seconds"],
            )
        finally:
            scheduler.stop()


class TaskCountHistoryRouteTests(unittest.TestCase):
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
                    "min_warm_allocations: 0",
                    "cluster_refresh_interval_seconds: 0",
                    "reconcile_on_start: false",
                    "backup_enabled: false",
                ]
            ),
            encoding="utf-8",
        )

        previous_config = os.environ.get("SLURM_SCHEDULER_CONFIG")
        os.environ["SLURM_SCHEDULER_CONFIG"] = str(config_path)
        try:
            from slurm_scheduler.app import create_app
        finally:
            if previous_config is None:
                os.environ.pop("SLURM_SCHEDULER_CONFIG", None)
            else:
                os.environ["SLURM_SCHEDULER_CONFIG"] = previous_config
        self.app = create_app(str(config_path))
        self.app.router.on_startup.clear()
        self.app.router.on_shutdown.clear()

    def tearDown(self) -> None:
        self.app.state.scheduler.stop()
        self.tmp.cleanup()

    def route_endpoint(self, path: str, method: str):
        return next(
            route.endpoint
            for route in self.app.routes
            if getattr(route, "path", "") == path
            and method in getattr(route, "methods", set())
        )

    def dashboard_request(self, query_string: bytes = b"") -> Request:
        return Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/",
                "raw_path": b"/",
                "query_string": query_string,
                "headers": [],
                "client": ("testclient", 50000),
                "server": ("testserver", 80),
                "root_path": "",
                "app": self.app,
            }
        )

    def test_history_api_downsamples_to_six_hundred_points_and_keeps_endpoints(self) -> None:
        now = int(time.time())
        start = now - (1000 * 60)
        samples = [
            (start + index * 60, index, index // 2, index - (index // 2), 0)
            for index in range(1000)
        ]
        with self.app.state.db.connect() as conn:
            conn.execute(
                """
                INSERT INTO task_count_samples(
                    sampled_at, total_active, running, queued, attaching
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (now - 25 * 60 * 60, 9999, 9999, 0, 0),
            )
            conn.executemany(
                """
                INSERT INTO task_count_samples(
                    sampled_at, total_active, running, queued, attaching
                ) VALUES (?, ?, ?, ?, ?)
                """,
                samples,
            )

        endpoint = self.route_endpoint("/api/task-count-history", "GET")
        payload = endpoint(hours=24)

        self.assertEqual(len(payload), 600)
        self.assertEqual(payload[0]["total"], 0)
        self.assertEqual(payload[-1]["total"], 999)
        self.assertEqual(
            set(payload[0]),
            {"t", "total", "running", "queued", "attaching"},
        )
        self.assertTrue(payload[0]["t"].endswith("Z"))
        self.assertEqual(
            [point["t"] for point in payload],
            sorted(point["t"] for point in payload),
        )

    def test_dashboard_renders_collapsible_history_above_task_summary(self) -> None:
        response = self.route_endpoint("/", "GET")(self.dashboard_request())
        html = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('<details id="task-count-history"', html)
        self.assertIn("실행 추이", html)
        self.assertIn('id="task-count-chart"', html)
        self.assertIn('data-task-history-hours="72"', html)
        self.assertIn("/api/task-count-history?hours=", html)
        self.assertLess(
            html.index('id="task-count-history"'),
            html.index('aria-label="Attached task summary"'),
        )

    def test_dashboard_initial_filtered_and_live_fea_aedt_counts_match(self) -> None:
        with self.app.state.db.connect() as conn:
            conn.executemany(
                "INSERT INTO aedt_sessions(session_key, state) VALUES (?, ?)",
                [
                    ("route-ready", "ready"),
                    ("route-draining", "draining"),
                    ("route-closed", "closed"),
                ],
            )

        task_specs = (
            ("counter-match-standalone-running", TaskStatus.RUNNING, "standalone"),
            ("counter-match-pooled-running", TaskStatus.RUNNING, "pooled"),
            ("counter-match-pooled-attaching", TaskStatus.ATTACHING, "pooled"),
            ("counter-match-standalone-queued", TaskStatus.QUEUED, "standalone"),
            ("counter-other-standalone-running", TaskStatus.RUNNING, "standalone"),
        )
        for name, status, backend in task_specs:
            task_id = self.app.state.db.create_task(
                TaskCreate(
                    name,
                    "~/case",
                    "run",
                    scheduling_profile="fea_bursty",
                    aedt_backend=backend,
                )
            )
            if status != TaskStatus.QUEUED:
                self.app.state.db.update_task(task_id, status=status.value)

        dashboard = self.route_endpoint("/", "GET")
        initial_html = dashboard(self.dashboard_request()).body.decode("utf-8")
        filtered_html = dashboard(
            self.dashboard_request(b"task_name_contains=counter-match")
        ).body.decode("utf-8")
        live = self.route_endpoint("/api/dashboard-summary", "GET")(
            task_name_contains="counter-match"
        )

        self.assertIn('data-aedt-pool-sessions="2">4 / 4</span>', initial_html)
        self.assertIn('data-aedt-pool-sessions="2">3 / 3</span>', filtered_html)
        self.assertEqual(live["tasks"]["fea"], 3)
        self.assertEqual(live["tasks"]["aedt_pool_sessions"], 2)
        self.assertEqual(live["tasks"]["aedt"], 3)

    def test_dashboard_pages_large_active_population_without_hiding_rows(self) -> None:
        task_ids = [
            self.app.state.db.create_task(
                TaskCreate(f"dashboard-page-{index:03d}", "~/case", "run")
            )
            for index in range(105)
        ]
        dashboard = self.route_endpoint("/", "GET")

        first_html = dashboard(self.dashboard_request()).body.decode("utf-8")
        second_html = dashboard(
            self.dashboard_request(b"active_page=1")
        ).body.decode("utf-8")

        self.assertEqual(len(re.findall(r"<tr\s+data-task-row", first_html)), 100)
        self.assertEqual(len(re.findall(r"<tr\s+data-task-row", second_html)), 5)
        self.assertIn(f'data-id="{task_ids[-1]}"', first_html)
        self.assertNotIn(f'data-id="{task_ids[0]}"', first_html)
        self.assertIn(f'data-id="{task_ids[0]}"', second_html)
        self.assertIn("Active page 1 / 2 (105 tasks, 100 per page)", first_html)
        self.assertIn("Active page 2 / 2 (105 tasks, 100 per page)", second_html)


if __name__ == "__main__":
    unittest.main()
