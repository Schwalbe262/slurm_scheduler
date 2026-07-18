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
from slurm_scheduler.pestat import PestatNode
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
                "aedt_pool_active_sessions": 0,
                "aedt_pool_starting_sessions": 0,
                "aedt_pool_draining_sessions": 0,
                "aedt_pool_unhealthy_sessions": 0,
                "aedt_pool_project_workers": 0,
                "aedt": 1,
            },
        )

    def test_fea_aedt_counts_pool_sessions_and_only_standalone_running_desktops(self) -> None:
        AedtPoolService(self.db).init()
        allocation_id = self.db.create_allocation(
            "a", "cpu", "cpu-a", 48, 512 * 1024
        )
        self.db.update_allocation(allocation_id, state="warm")
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT INTO aedt_sessions(
                    session_key, state, allocation_id, last_heartbeat_at
                ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    ("session-starting", "starting", allocation_id),
                    ("session-ready", "ready", allocation_id),
                    ("session-busy", "busy", allocation_id),
                    ("session-busy-drain-requested", "busy", allocation_id),
                    ("session-draining", "draining", allocation_id),
                    ("session-unhealthy", "unhealthy", allocation_id),
                    ("session-closed", "closed", allocation_id),
                    ("session-failed", "failed", allocation_id),
                ],
            )
            conn.execute(
                """
                UPDATE aedt_sessions
                SET drain_requested_at = CURRENT_TIMESTAMP
                WHERE session_key = 'session-busy-drain-requested'
                """
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

        self.assertEqual(summary["fea"], 2)
        self.assertEqual(summary["fea_running"], 1)
        self.assertEqual(summary["aedt_pool_sessions"], 2)
        self.assertEqual(summary["aedt_pool_active_sessions"], 2)
        self.assertEqual(summary["aedt_pool_starting_sessions"], 1)
        self.assertEqual(summary["aedt_pool_draining_sessions"], 1)
        self.assertEqual(summary["aedt_pool_unhealthy_sessions"], 1)
        self.assertEqual(summary["aedt"], 3)

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
        allocation_id = self.app.state.db.create_allocation(
            "a", "cpu", "cpu-a", 48, 512 * 1024
        )
        self.app.state.db.update_allocation(allocation_id, state="warm")
        with self.app.state.db.connect() as conn:
            conn.executemany(
                """
                INSERT INTO aedt_sessions(
                    session_key, state, allocation_id, last_heartbeat_at
                ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    ("route-ready", "ready", allocation_id),
                    ("route-draining", "draining", allocation_id),
                    ("route-closed", "closed", allocation_id),
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

        self.assertIn('data-aedt-pool-sessions="1">2 / 3</span>', initial_html)
        self.assertIn('data-aedt-pool-sessions="1">1 / 2</span>', filtered_html)
        self.assertEqual(live["tasks"]["fea"], 1)
        self.assertEqual(live["tasks"]["aedt_pool_sessions"], 1)
        self.assertEqual(live["tasks"]["aedt_pool_draining_sessions"], 1)
        self.assertEqual(live["tasks"]["aedt"], 2)

    def test_dashboard_distinguishes_allocation_drain_from_physical_node_state(self) -> None:
        allocation_id = self.app.state.db.create_allocation(
            "a", "cpu", "cpu-a", 48, 512 * 1024
        )
        self.app.state.db.update_allocation(
            allocation_id,
            state="draining",
            slurm_job_id="allocation-draining-node-mix",
            drain_reason="AEDT pool solver fault quarantine",
        )
        self.app.state.db.replace_pestat_nodes(
            [
                PestatNode(
                    hostname="cpu-a",
                    partition="cpu",
                    state="mix",
                    cpu_used=24,
                    cpu_total=64,
                    cpu_load=20.0,
                    memory_mb=1024 * 1024,
                    free_memory_mb=512 * 1024,
                )
            ]
        )

        html = self.route_endpoint("/", "GET")(self.dashboard_request()).body.decode(
            "utf-8"
        )

        self.assertIn("<th>Allocation State</th>", html)
        self.assertIn("<th>Node State</th>", html)
        self.assertIn('data-allocation-state="draining"', html)
        self.assertIn('data-node-state="mix"', html)

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
        marker = first_html.index("Active page 1 / 2")
        self.assertGreater(marker, first_html.index('id="attached-tasks-table"'))
        self.assertLess(marker, first_html.index("<summary>Finished tasks:"))

    def test_dashboard_searches_and_sorts_full_population_before_paging(self) -> None:
        expected_names = []
        for index in range(159):
            if index % 3 == 0:
                name = f"whole-search-{52 - (index // 3):03d}"
                expected_names.append(name)
            else:
                name = f"page-noise-{index:03d}"
            self.app.state.db.create_task(TaskCreate(name, "~/case", "run"))

        dashboard = self.route_endpoint("/", "GET")
        html = dashboard(
            self.dashboard_request(
                b"task_name_contains=whole-search&task_sort_key=name&task_sort_direction=asc"
            )
        ).body.decode("utf-8")

        self.assertEqual(len(re.findall(r"<tr\s+data-task-row", html)), 53)
        self.assertIn("Active page 1 / 1 (53 tasks, 100 per page)", html)
        positions = [html.index(f">{name}</a>") for name in sorted(expected_names)]
        self.assertEqual(positions, sorted(positions))
        self.assertIn(
            "Search and sorting are applied to all matching tasks before the 100-row page",
            html,
        )
        self.assertIn('const serverSortKey = "name";', html)
        self.assertIn('url.searchParams.set("task_sort_key", state.sortKey)', html)
        self.assertIn("window.setTimeout(navigateToCurrentState, 350)", html)
        self.assertIn('filterInput.addEventListener("compositionstart"', html)
        self.assertIn("event.isComposing", html)
        self.assertNotIn("const sortedRows = [...rowSet]", html)

    def test_paged_tasks_api_returns_filtered_metadata_and_keeps_legacy_list(self) -> None:
        for index in range(159):
            is_match = index % 3 == 0
            self.app.state.db.create_task(
                TaskCreate(
                    (
                        f"api-whole-search-{52 - (index // 3):03d}"
                        if is_match
                        else f"api-page-noise-{index:03d}"
                    ),
                    "~/case",
                    "run",
                    project="mft" if is_match else "other",
                )
            )

        endpoint = self.route_endpoint("/api/tasks", "GET")
        payload = endpoint(
            include_diagnostics=False,
            compact=True,
            limit=0,
            before_id=0,
            project="mft",
            name_prefix="",
            name_contains="api-whole-search",
            status=[TaskStatus.QUEUED.value],
            sort_by="name",
            sort_order="asc",
            paged=True,
            page=1,
            page_size=100,
        )

        self.assertEqual(payload["filtered_total"], 53)
        self.assertEqual(payload["page"], 1)
        self.assertEqual(payload["page_size"], 100)
        self.assertEqual(payload["page_count"], 1)
        self.assertFalse(payload["has_previous"])
        self.assertFalse(payload["has_next"])
        self.assertEqual(
            payload["pagination"],
            {
                "filtered_total": 53,
                "page": 1,
                "page_size": 100,
                "page_count": 1,
                "has_previous": False,
                "has_next": False,
            },
        )
        self.assertEqual(len(payload["items"]), 53)
        self.assertEqual(
            [item["name"] for item in payload["items"]],
            sorted(item["name"] for item in payload["items"]),
        )

        legacy = endpoint(
            include_diagnostics=False,
            compact=True,
            limit=100,
            before_id=0,
            project="mft",
            name_prefix="",
            name_contains="",
            status=[TaskStatus.QUEUED.value],
            sort_by="id",
            sort_order="desc",
            paged=False,
            page=1,
            page_size=100,
        )
        self.assertIsInstance(legacy, list)
        self.assertEqual(len(legacy), 53)

    def test_dashboard_template_accepts_previous_generation_context_during_staging(self) -> None:
        response = self.route_endpoint("/", "GET")(self.dashboard_request())
        legacy_context = dict(response.context)
        for key in (
            "active_page",
            "active_page_count",
            "active_task_count",
            "active_page_size",
        ):
            legacy_context.pop(key, None)

        legacy_html = response.template.render(legacy_context)

        self.assertIn("Active page 1 / 1", legacy_html)
        self.assertIn("5000 per page", legacy_html)


if __name__ == "__main__":
    unittest.main()
