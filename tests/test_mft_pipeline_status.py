from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from starlette.requests import Request

from slurm_scheduler.db import Database
from slurm_scheduler.mft_pipeline_status import MftPipelineStatusReader
from slurm_scheduler.models import TaskCreate, TaskStatus


def write_json(root: Path, relative_path: str, payload: dict) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def write_complete_runtime(root: Path) -> None:
    write_json(
        root,
        "mft_pipeline/surrogate_status.json",
        {
            "state": "waiting_for_next_dataset_check",
            "strict_full_rows": 120,
            "updated_at": "2026-07-17T01:00:01+09:00",
        },
    )
    write_json(
        root,
        "mft_pipeline/experimental_continuous/status.json",
        {
            "state": "wave_running",
            "active_wave": "wave-test",
            "wave_phase": "candidate_training",
            "observed_strict_full_rows": 120,
            "next_refresh_strict_rows": 150,
            "updated_at": "2026-07-17T01:00:00+09:00",
            "active_wave_detail": {
                "worker_pid": 123,
                "jobs": [
                    {
                        "target": "Llt_phys",
                        "status": "exited",
                        "result_ready": True,
                        "model_threads": 2,
                        "trials": 50,
                    },
                    {
                        "target": "P_loss",
                        "status": "running",
                        "alive": True,
                        "result_ready": False,
                        "model_threads": 2,
                        "trials": 50,
                        "cpu_seconds": 12.5,
                    },
                ],
                "strict_snapshot": {"raw_rows": 300, "strict_full_rows": 120},
            },
            "hpo_contract": {
                "targets": ["Llt_phys", "P_loss"],
                "actual_hpo_processes": 2,
                "total_hpo_thread_budget": 4,
                "trials_per_target": 50,
            },
        },
    )
    write_json(
        root,
        "mft_pipeline/experimental_surrogate.json",
        {
            "lane": "experimental",
            "training_run_id": "model-001",
            "dataset_sha256": "a" * 64,
            "generation_report_sha256": "b" * 64,
            "strict_full_rows": 100,
            "generation": r"C:\runtime\generations\model-001",
            "fea_solver_revision": "solver-rev",
            "fea_library_revision": "library-rev",
            "incumbent_comparison": {
                "aggregate_loss_ratio": 0.95,
                "worst_target_loss_ratio": 1.02,
            },
        },
    )
    write_json(
        root,
        "mft_pipeline/experimental_continuous/state.json",
        {
            "active_wave": "wave-test",
            "active_wave_strict_rows": 120,
            "last_attempted_strict_rows": 100,
            "last_result_phase": "candidate_rejected",
            "last_finished_at": "2026-07-17T01:00:00+09:00",
            "last_result": {
                "candidate_training_run_id": "candidate-002",
                "promoted": False,
                "evaluated_at": "2026-07-17T01:00:00+09:00",
                "comparison": {
                    "method": "quality_gate",
                    "passed": False,
                    "reasons": ["aggregate_temperature_quality_blocked"],
                    "aggregate_temperature_safety_gate": {
                        "passed": False,
                        "quality_blocked_targets": ["T_max_core"],
                    },
                },
            },
        },
    )
    write_json(
        root,
        "mft_nsga_continuous/status.json",
        {
            "state": "running",
            "completed_run_count": 2,
            "completed_outcomes": {
                "feasible_complete": 1,
                "infeasible_complete": 1,
            },
            "next_seed_base": 104,
            "active_run": {
                "run_id": "run-3",
                "seeds": [100, 101, 102, 103],
                "model_id": "model-A",
                "model_lane": "experimental",
                "warm_start": {
                    "source_run_id": "run-2",
                    "artifact_kind": "pareto_X",
                    "provenance_match": True,
                    "reevaluation_required": True,
                },
            },
            "last_infeasibility": {
                "least_violation_archive": {
                    "best_total_positive_violation": 2.5,
                    "candidate_count": 400,
                },
                "seed_invariant_zero_pass_constraints": ["Llt_robust_band"],
            },
        },
    )
    write_json(
        root,
        "mft_nsga_newmodel_fastlane_v2/status.json",
        {
            "state": "running",
            "completed_run_count": 1,
            "completed_outcomes": {"infeasible_complete": 1},
            "next_seed_base": 204,
            "active_run": {
                "run_id": "run-fast-2",
                "seeds": [200, 201, 202, 203],
                "model_id": "model-B",
                "model_lane": "experimental",
            },
        },
    )
    write_json(
        root,
        "mft_nsga_fea_validation/status.json",
        {
            "state": "active",
            "global_lane_inventory": {
                "tasks": [
                    {"task_id": 1, "status": "running"},
                    {"task_id": 2, "status": "queued"},
                    {
                        "task_id": 9,
                        "name": "mft-nsgafea-f-full-model",
                        "status": "running",
                    },
                ]
            },
            "active_tasks": [{"task_id": 1, "status": "running"}],
        },
    )
    write_json(
        root,
        "mft_nsga_fea_validation/state.json",
        {
            "candidates": {
                "pass": {
                    "candidate_digest": "pass",
                    "collection_state": "collector_succeeded",
                    "result_contract_valid": True,
                    "standard_fea_spec_pass": True,
                },
                "fail": {
                    "candidate_digest": "fail",
                    "collection_state": "collector_succeeded",
                    "result_contract_valid": True,
                    "standard_fea_spec_pass": False,
                },
            }
        },
    )
    write_json(
        root,
        "mft_nsga_fastlane_fea_validation/status.json",
        {
            "state": "active",
            "global_lane_inventory": {
                "tasks": [
                    {"task_id": 2, "status": "queued"},
                    {"task_id": 3, "status": "running"},
                    {
                        "task_id": 9,
                        "name": "mft-nsgafea-f-full-model",
                        "status": "running",
                    },
                ]
            },
            "active_tasks": [{"task_id": 3, "status": "running"}],
        },
    )
    write_json(root, "mft_nsga_fastlane_fea_validation/state.json", {"candidates": {}})
    write_json(
        root,
        "mft_nsga_full_model_validation/status.json",
        {
            "state": "active",
            # Shared inventory contains Standard FEA and must not inflate the
            # full-model active count.
            "global_lane_inventory": {
                "tasks": [
                    {"task_id": 1, "status": "running"},
                    {"task_id": 2, "status": "queued"},
                    {"task_id": 3, "status": "running"},
                ]
            },
            "active_tasks": [
                {"task_id": 9, "status": "running", "candidate_digest": "pass"}
            ],
            "counts": {"full_model_pass": 1, "standard_pass_discovered": 2},
        },
    )
    write_json(
        root,
        "mft_nsga_full_model_validation/state.json",
        {
            "candidates": {
                "full-pass": {
                    "candidate_digest": "pass",
                    "collection_state": "collector_succeeded",
                    "full_model_spec_pass": True,
                    "standard_task_id": 11,
                    "standard_actual_volume_L": 1344.4,
                    "standard_actual_total_loss_W": 6278.8,
                    "fine_task_id": 9,
                    "fine_task_status": "running",
                    "run_id": "run-3",
                    "model_id": "model-A",
                }
            }
        },
    )


class MftPipelineStatusReaderTests(unittest.TestCase):
    def test_canonical_status_wins_while_experimental_wave_is_waiting(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            write_json(
                root,
                "mft_pipeline/experimental_continuous/status.json",
                {
                    "state": "waiting_for_dataset_growth",
                    "last_attempted_strict_rows": 120,
                    "updated_at": "2026-07-17T05:40:00+09:00",
                },
            )
            write_json(
                root,
                "mft_pipeline/surrogate_status.json",
                {
                    "state": "waiting_for_next_dataset_check",
                    "raw_rows": 350,
                    "strict_full_rows": 137,
                    "updated_at": "2026-07-16T21:00:00+00:00",
                },
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        self.assertEqual(payload["data"]["dataset_rows"], 137)
        self.assertEqual(payload["data"]["raw_rows"], 350)
        self.assertEqual(payload["data"]["strict_rows"], 137)
        self.assertEqual(payload["data"]["strict_delta_since_model"], 37)
        self.assertEqual(
            payload["data"]["updated_at"], "2026-07-16T21:00:00+00:00"
        )

    def test_reader_composes_all_parallel_stages_and_deduplicates_shared_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        self.assertTrue(payload["available"])
        self.assertEqual(payload["data"]["dataset_rows"], 120)
        self.assertEqual(payload["data"]["raw_rows"], 300)
        self.assertEqual(payload["data"]["strict_delta_since_model"], 20)
        self.assertEqual(payload["surrogate"]["phase"], "candidate_training")
        self.assertTrue(payload["surrogate"]["training_active"])
        self.assertEqual(payload["surrogate"]["training_jobs_ready"], 1)
        self.assertEqual(payload["surrogate"]["hpo"]["ready"], 1)
        self.assertEqual(payload["surrogate"]["hpo"]["total"], 2)
        self.assertEqual(
            payload["surrogate"]["hpo"]["targets"][1]["target"], "P_loss"
        )
        self.assertEqual(
            payload["surrogate"]["last_decision"]["outcome"], "rejected"
        )
        self.assertEqual(
            payload["surrogate"]["last_decision"]["blocked_targets"],
            ["T_max_core"],
        )
        self.assertEqual(
            payload["surrogate"]["active_model"]["generation_id"], "model-001"
        )
        self.assertEqual(
            payload["surrogate"]["active_model"]["dataset_sha256"], "a" * 64
        )
        self.assertEqual(
            payload["surrogate"]["active_model"]["metrics"][
                "aggregate_loss_ratio"
            ],
            0.95,
        )
        self.assertEqual(payload["nsga"]["active_seed_workers"], 8)
        self.assertEqual(payload["nsga"]["completed_runs"], 3)
        self.assertEqual(payload["nsga"]["feasible_runs"], 1)
        self.assertEqual(payload["nsga"]["pareto_runs"], 1)
        self.assertEqual(
            payload["nsga"]["lanes"][0]["least_violation"][
                "best_total_positive_violation"
            ],
            2.5,
        )
        self.assertEqual(len(payload["nsga"]["designs"]["items"]), 1)
        self.assertEqual(
            payload["nsga"]["designs"]["items"][0]["standard"]["task_id"],
            11,
        )
        self.assertEqual(
            payload["nsga"]["designs"]["items"][0]["full"]["task_id"], 9
        )
        self.assertEqual(payload["standard_fea"]["active"], 3)
        self.assertEqual(payload["standard_fea"]["running"], 2)
        self.assertEqual(payload["standard_fea"]["queued"], 1)
        self.assertEqual(payload["standard_fea"]["pass"], 1)
        self.assertEqual(payload["standard_fea"]["fail"], 1)
        self.assertEqual(payload["full_model"]["active"], 1)
        self.assertEqual(payload["full_model"]["pass"], 1)
        self.assertEqual(payload["errors"], [])

    def test_oversize_and_missing_sources_fail_soft(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "mft_pipeline/experimental_continuous/status.json"
            path.parent.mkdir(parents=True)
            path.write_text("x" * 2048, encoding="utf-8")
            payload = MftPipelineStatusReader(
                root, max_json_bytes=1024, cache_seconds=0
            ).snapshot()

        self.assertFalse(payload["available"])
        self.assertFalse(payload["data"]["available"])
        self.assertGreaterEqual(len(payload["errors"]), 1)
        self.assertTrue(
            any("limit is 1024" in str(error["message"]) for error in payload["errors"])
        )

    def test_transient_invalid_json_uses_last_good_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            reader = MftPipelineStatusReader(root, cache_seconds=0)
            first = reader.snapshot()
            status_path = root / "mft_pipeline/experimental_continuous/status.json"
            status_path.write_text("{broken", encoding="utf-8")
            second = reader.snapshot()

        self.assertEqual(first["data"]["dataset_rows"], 120)
        self.assertEqual(second["data"]["dataset_rows"], 120)
        self.assertTrue(
            any(
                error["source"] == "surrogate_status" and error["stale"]
                for error in second["errors"]
            )
        )

    def test_hpo_targets_are_bounded_and_report_truncation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            jobs = [
                {
                    "target": f"target-{index:02d}",
                    "status": "running",
                    "alive": True,
                    "result_ready": index < 3,
                }
                for index in range(25)
            ]
            write_json(
                root,
                "mft_pipeline/experimental_continuous/status.json",
                {
                    "state": "wave_running",
                    "active_wave": "wave-many",
                    "active_wave_detail": {"jobs": jobs},
                    "hpo_contract": {
                        "targets": [item["target"] for item in jobs]
                    },
                },
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        self.assertEqual(len(payload["surrogate"]["hpo"]["targets"]), 20)
        self.assertEqual(payload["surrogate"]["hpo"]["ready"], 3)
        self.assertTrue(payload["surrogate"]["hpo"]["truncated"])

    def test_design_rows_prioritize_active_full_and_cap_at_twelve(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            candidates = {
                f"digest-{index:02d}": {
                    "candidate_digest": f"digest-{index:02d}",
                    "standard_task_id": 1000 + index,
                    "standard_actual_volume_L": 1300.0 + index,
                    "standard_actual_total_loss_W": 6400.0 - index,
                    "run_id": f"run-{index:02d}",
                    "model_id": "model-test",
                }
                for index in range(14)
            }
            write_json(
                root,
                "mft_nsga_full_model_validation/state.json",
                {"candidates": candidates},
            )
            write_json(
                root,
                "mft_nsga_full_model_validation/status.json",
                {
                    "state": "active",
                    "active_tasks": [
                        {
                            "task_id": 5013,
                            "candidate_digest": "digest-13",
                            "status": "queued",
                        }
                    ],
                },
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        designs = payload["nsga"]["designs"]
        self.assertEqual(len(designs["items"]), 12)
        self.assertEqual(designs["items"][0]["candidate_digest"], "digest-13")
        self.assertEqual(designs["items"][0]["full"]["task_id"], 5013)
        self.assertEqual(designs["nondominated_count"], 14)
        self.assertTrue(designs["truncated"])

    def test_controller_age_is_stale_but_event_pointer_age_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            write_json(
                root,
                "mft_nsga_continuous/status.json",
                {
                    "state": "running",
                    "updated_at": "2000-01-01T00:00:00+00:00",
                },
            )
            write_json(
                root,
                "mft_pipeline/experimental_surrogate.json",
                {
                    "lane": "experimental",
                    "training_run_id": "immutable-model",
                    "published_at": "2000-01-01T00:00:00+00:00",
                },
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        self.assertTrue(payload["sources"]["nsga_main"]["stale"])
        self.assertFalse(payload["sources"]["surrogate_pointer"]["stale"])
        self.assertIn("nsga_main", payload["stale_sources"])


class StandaloneCampaignSummaryTests(unittest.TestCase):
    def test_summary_excludes_pooled_and_validation_lanes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(str(Path(tmpdir) / "scheduler.db"))
            db.init()
            task_specs = [
                ("mft-camp-run", "standalone", TaskStatus.RUNNING),
                ("mft-camp-attach", "standalone", TaskStatus.ATTACHING),
                ("mft-camp-queue", "standalone", TaskStatus.QUEUED),
                ("mft-camp-pool", "pooled", TaskStatus.RUNNING),
                ("mft-nsgafea-validation", "standalone", TaskStatus.RUNNING),
            ]
            for name, backend, status in task_specs:
                task_id = db.create_task(
                    TaskCreate(
                        name,
                        "~/case",
                        "run",
                        scheduling_profile="fea_bursty",
                        aedt_backend=backend,
                        project="MFT_1MW_2026v1",
                    )
                )
                if status != TaskStatus.QUEUED:
                    db.update_task(task_id, status=status.value)

            summary = db.standalone_campaign_activity_summary("MFT_1MW_2026v1")

        self.assertEqual(
            summary,
            {"active": 3, "running": 1, "attaching": 1, "queued": 1},
        )


class MftPipelineStatusRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.runtime_root = root / "runtime"
        write_complete_runtime(self.runtime_root)
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
        config_path = root / "app.yaml"
        config_path.write_text(
            f'database_path: "{(root / "scheduler.db").as_posix()}"\n'
            f'accounts_path: "{accounts_path.as_posix()}"\n'
            "min_warm_allocations: 0\n"
            "cluster_refresh_interval_seconds: 0\n"
            "reconcile_on_start: false\n"
            "backup_enabled: false\n"
            "standalone_aedt_max_running_by_project:\n"
            "  MFT_1MW_2026v1: 100\n",
            encoding="utf-8",
        )
        with mock.patch.dict(
            os.environ,
            {
                "SLURM_MFT_PIPELINE_RUNTIME_ROOT": str(self.runtime_root),
                "SLURM_SCHEDULER_CONFIG": str(config_path),
            },
        ):
            from slurm_scheduler.app import create_app

            self.app = create_app(str(config_path))
        self.app.router.on_startup.clear()
        self.app.router.on_shutdown.clear()
        db = self.app.state.db
        project_id = db.create_project("MFT_1MW_2026v1")
        db.update_project(project_id, desired_simulations=400)
        for index, status in enumerate(
            (TaskStatus.RUNNING, TaskStatus.QUEUED), start=1
        ):
            task_id = db.create_task(
                TaskCreate(
                    f"mft-camp-test-{index}",
                    "~/case",
                    "run",
                    scheduling_profile="fea_bursty",
                    aedt_backend="standalone",
                    project="MFT_1MW_2026v1",
                )
            )
            if status != TaskStatus.QUEUED:
                db.update_task(task_id, status=status.value)

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

    def dashboard_request(self) -> Request:
        return Request(
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

    def test_api_combines_runtime_status_with_exact_standalone_target(self) -> None:
        payload = self.route_endpoint("/api/mft-pipeline/status", "GET")()

        self.assertEqual(payload["standalone"]["active"], 2)
        self.assertEqual(payload["standalone"]["target"], 400)
        self.assertEqual(payload["standalone"]["running"], 1)
        self.assertEqual(payload["standalone"]["running_target"], 100)
        self.assertEqual(payload["nsga"]["active_seed_workers"], 8)
        self.assertEqual(payload["surrogate"]["hpo"]["total"], 2)
        self.assertEqual(payload["surrogate"]["last_decision"]["outcome"], "rejected")
        self.assertEqual(payload["nsga"]["designs"]["items"][0]["full"]["task_id"], 9)

    def test_dashboard_contains_read_only_auto_refresh_panel(self) -> None:
        response = self.route_endpoint("/", "GET")(self.dashboard_request())
        html = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="mft-continuous-pipeline"', html)
        self.assertIn('data-mft-pipeline="standalone"', html)
        self.assertIn('id="mft-nsga-lanes"', html)
        self.assertIn('id="mft-surrogate-live-results"', html)
        self.assertIn('id="mft-nsga-live-results"', html)
        self.assertIn('id="mft-surrogate-hpo-targets"', html)
        self.assertIn('id="mft-validated-designs"', html)
        self.assertIn('fetch("/api/mft-pipeline/status"', html)
        self.assertIn("window.setInterval(refresh, 15000)", html)
        self.assertIn('document.createElement("a")', html)
        self.assertIn("Number.isSafeInteger(parsed) && parsed > 0", html)
        self.assertIn("link.href = `/tasks/${id}`", html)
        self.assertIn("stale / last good", html)
        self.assertIn("Standard/Full PASS는 검증 evidence", html)
        self.assertIn("activeMetrics.aggregate_loss_ratio", html)
        self.assertIn("activeModel.dataset_sha256", html)
        self.assertIn("lane.infeasible_runs", html)
        self.assertIn("zero-pass:", html)
        panel = html[
            html.index('id="mft-continuous-pipeline"') : html.index(
                '<section class="panel">',
                html.index('id="mft-continuous-pipeline"'),
            )
        ]
        self.assertNotIn("<form", panel)
        self.assertNotIn(".innerHTML", panel)


if __name__ == "__main__":
    unittest.main()
