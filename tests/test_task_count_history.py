from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from starlette.requests import Request

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
        self.db.create_task(TaskCreate("campaign-older-queued", "~/case", "run"))
        self.db.create_task(TaskCreate("campaign-newer-queued", "~/case", "run"))
        unrelated = self.db.create_task(TaskCreate("unrelated", "~/case", "run"))
        self.db.update_task(unrelated, status=TaskStatus.RUNNING.value)

        summary = self.db.task_activity_summary(
            name_contains="campaign",
            active_limit=2,
            queued_limit=1,
        )

        self.assertEqual(
            summary,
            {
                "total": 3,
                "running": 1,
                "attaching": 1,
                "queued": 1,
                "fea": 1,
                "fea_running": 1,
                "standard": 2,
                "gpu": 1,
                "cpu": 2,
                "same_node": 1,
            },
        )

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
        request = Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/",
                "raw_path": b"/",
                "query_string": b"",
                "headers": [],
                "client": ("testclient", 50000),
                "server": ("testserver", 80),
                "root_path": "",
                "app": self.app,
            }
        )
        response = self.route_endpoint("/", "GET")(request)
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


if __name__ == "__main__":
    unittest.main()
