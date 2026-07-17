from __future__ import annotations

import json
import hashlib
import math
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from starlette.requests import Request

from slurm_scheduler.db import Database
from slurm_scheduler.mft_pipeline_status import (
    MftPipelineStatusReader,
    NSGA_DESIGN_FIELDS,
)
from slurm_scheduler.models import TaskCreate, TaskStatus


def write_json(root: Path, relative_path: str, payload: dict) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def completed_hpo_fixture() -> dict:
    best_params = {
        "n_estimators": 800,
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "subsample": 0.8,
        "colsample_bytree": 0.9,
        "reg_lambda": 0.1,
    }
    second_best_params = {**best_params, "num_leaves": 95}

    def target_auth(params: dict, digit: str) -> dict:
        tuned = hashlib.sha256(
            json.dumps(
                params,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return {
            "generation_id": digit * 64,
            "params_sha256": "a" * 64,
            "manifest_sha256": "b" * 64,
            "tuned_override_sha256": tuned,
            "target_task_ids_sha256": "c" * 64,
            "hpo_train_task_ids_sha256": "f" * 64,
        }

    value = {
        "schema_version": 1,
        "status": "completed",
        "evidence_authentication": "complete",
        "wave": "wave-" + "d" * 16,
        "result_phase": "candidate_rejected",
        "completed_at": "2026-07-17T01:00:00+09:00",
        "dataset_generation": "d" * 64,
        "dataset_sha256": "e" * 64,
        "strict_full_rows": 2997,
        "target_count": 2,
        "targets": [
            {
                "target": "Llt_phys",
                "family": "lightgbm",
                "cv_mse_transformed": 0.002359,
                "eligible_rows": 2997,
                "target_rows": 2997,
                "hpo_train_rows": 2397,
                "trials": 16,
                "model_threads": 2,
                "best_params": best_params,
                "authentication": target_auth(best_params, "1"),
            },
            {
                "target": "Tprobe_Rx_main_leeward_max",
                "family": "lightgbm",
                "cv_mse_transformed": 0.022064,
                "eligible_rows": 2997,
                "target_rows": 2997,
                "hpo_train_rows": 2397,
                "trials": 16,
                "model_threads": 2,
                "best_params": second_best_params,
                "authentication": target_auth(second_best_params, "2"),
            },
        ],
        "common_authentication": {
            "dataset_manifest_sha256": "3" * 64,
            "data_contract_sha256": "5" * 64,
            "search_implementation_sha256": "6" * 64,
            "training_split_contract_sha256": "7" * 64,
            "feature_schema_sha256": "8" * 64,
        },
        "wave_outcome": {
            "candidate_training_run_id": "candidate-hpo-001",
            "promoted": False,
            "quality_status_sha256": "4" * 64,
        },
    }
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    value["evidence_sha256"] = hashlib.sha256(encoded).hexdigest()
    return value


def resign_completed_hpo(value: dict) -> None:
    unsigned = {key: item for key, item in value.items() if key != "evidence_sha256"}
    value["evidence_sha256"] = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def schema2_objective_contract(target: str) -> dict:
    if not target.startswith("T"):
        return {
            "schema_version": "mft-hpo-objective-v1",
            "name": "transformed_mse_v1",
            "units": "transformed_target_squared",
            "scope": "model_fit_partition_cv_only",
            "fold_aggregation": "mean",
            "components": {"mse_transformed": 1.0},
        }
    return {
        "schema_version": "mft-hpo-objective-v1",
        "name": "temperature_gate_normalized_rmse_c_plus_p90_ape_v1",
        "units": "dimensionless_gate_ratio_sum",
        "scope": "model_fit_partition_cv_only",
        "fold_aggregation": "mean",
        "components": {
            "rmse_C": {
                "weight": 1.0,
                "normalizer": 5.0,
                "normalizer_units": "degC",
                "gate_metric": "max_rmse",
            },
            "p90_ape_pct": {
                "weight": 1.0,
                "normalizer": 10.0,
                "normalizer_units": "percent",
                "gate_metric": "max_p90_ape_pct",
            },
        },
        "p90_quantile_method": "numpy_linear",
        "relative_error_denominator": "absolute_truth_temperature_c",
        "quality_thresholds_sha256": (
            "4aeb0a376d9017cde2a78dd8b965e99ed89134b0663185dd87547580c4cbc9b0"
        ),
    }


def completed_hpo_schema2_fixture() -> dict:
    value = completed_hpo_fixture()
    value["schema_version"] = 2
    for item in value["targets"]:
        objective_value = item.pop("cv_mse_transformed")
        item["cv_objective_value"] = objective_value
        item["objective_contract"] = schema2_objective_contract(item["target"])
    resign_completed_hpo(value)
    return value


def write_complete_runtime(root: Path) -> None:
    write_json(
        root,
        "mft_pipeline/surrogate_status.json",
        {
            "schema_version": 1,
            "state": "waiting_for_next_dataset_check",
            "cycle": 40,
            "raw_rows": 300,
            "strict_full_rows": 120,
            "activation_minimum_strict_full_rows": 100,
            "first_tuning_strict_full_rows": 4000,
            "queue": {
                "cancelled": 3,
                "failed": 12,
                "queued": 0,
                "retry_wait": 0,
                "running": 1,
                "succeeded": 264,
            },
            "last_jobs": {"collect": 279, "train": 280},
            "dataset_generation": "dataset:" + "c" * 64,
            "blocked": {},
            "last_error": None,
            "active_model_state": "awaiting_activation",
            "last_checkpoint_result": {
                "schema_version": 1,
                "status": "failed",
                "kind": "quality_rejected",
                "threshold": 3000,
                "actual_strict_full_rows": 3011,
                "quality_passed": False,
                "completed_at": "2026-07-17T15:35:51+09:00",
                "training_run_id": "candidate-canonical-001",
                "generation": "generations/candidate-canonical-001",
                "reason_count": 2,
                "reasons": ["T_max_core:mape_pct", "P_core_total:r2"],
                "target_count": 2,
                "failed_target_count": 1,
                "target_summaries": [
                    {
                        "target": "T_max_core",
                        "passed": False,
                        "blocking": True,
                        "reason_count": 1,
                        "reasons": ["mape_pct"],
                        "metrics": {"r2": 0.91, "mape_pct": 7.2},
                    },
                    {
                        "target": "Llt_phys",
                        "passed": True,
                        "blocking": True,
                        "reason_count": 0,
                        "reasons": [],
                        "metrics": {"r2": 0.99, "mape_pct": 1.2},
                    },
                ],
                "evidence_authentication": "complete",
            },
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
            "last_completed_hpo_results": completed_hpo_fixture(),
            "last_completed_hpo_results_error": None,
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
                "comparisons": {
                    "Llt_phys": {
                        "metric": "normalized_rmse_pct",
                        "candidate": 2.8019900580506305,
                        "incumbent": 3.400370638116442,
                        "ratio": 0.8240248950046014,
                    },
                    "P_loss": {
                        "metric": "normalized_rmse_pct",
                        "candidate": 5.016840851927763,
                        "incumbent": 7.110606823742417,
                        "ratio": 0.7055432786940856,
                    },
                },
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
            "current_model_id": "model-A",
            "current_model_completed_run_count": 1,
            "current_model_completed_outcomes": {"infeasible_complete": 1},
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
            "current_model_last_infeasibility": {
                "least_violation_archive": {
                    "best_total_positive_violation": 2.5,
                    "candidate_count": 400,
                },
                "seed_invariant_zero_pass_constraints": ["Llt_robust_band"],
            },
            "latest_feasible_pareto_lifetime": {
                "schema_version": "mft-nsga-pareto-status-v1",
                "available": True,
                "scope": "lifetime",
                "run_id": "run-000001",
                "model_id": "model-old",
                "model_lane": "experimental",
                "production_eligible": False,
                "fea_submission_approved": False,
                "seeds": [92, 93, 94, 95],
                "finished_at": "2026-07-17T00:10:00+09:00",
                "point_count": 2,
                "points": [
                    {
                        "candidate_index": 0,
                        "row_number": 1,
                        "volume_L": 1300.5,
                        "total_loss_W": 6200.25,
                        "design": {
                            **{name: 1 for name in NSGA_DESIGN_FIELDS},
                            "N1_main": 6,
                            "gap1": 4.9,
                        },
                    },
                    {
                        "candidate_index": 1,
                        "row_number": 2,
                        "volume_L": 1310.75,
                        "total_loss_W": 6100.5,
                        "design": {
                            **{name: 1 for name in NSGA_DESIGN_FIELDS},
                            "N1_main": 7,
                            "gap1": 5.1,
                        },
                    },
                ],
                "limit": 500,
                "truncated": False,
                "objective_names": ["volume_L", "total_loss_W"],
                "objective_units": {"volume_L": "L", "total_loss_W": "W"},
                "objective_source": "surrogate_prediction",
                "fea_verified": False,
                "optimization_manifest_sha256": "d" * 64,
                "pareto_front_sha256": "e" * 64,
                "run_manifest_sha256": "f" * 64,
                "model_source_sha256": "a" * 64,
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
            "current_model_id": "model-B",
            "current_model_completed_run_count": 1,
            "current_model_completed_outcomes": {"infeasible_complete": 1},
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
                    "schema_version": 1,
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
        self.assertTrue(payload["canonical_training"]["available"])
        self.assertTrue(payload["canonical_training"]["pipeline_active"])
        self.assertEqual(payload["canonical_training"]["queue"]["running"], 1)
        self.assertEqual(payload["canonical_training"]["last_jobs"]["train"], 280)
        self.assertEqual(payload["canonical_training"]["rows_until_activation"], 0)
        self.assertEqual(payload["canonical_training"]["rows_until_first_tuning"], 3880)
        self.assertEqual(
            payload["canonical_training"]["active_model_state"],
            "awaiting_activation",
        )
        checkpoint = payload["canonical_training"]["last_checkpoint_result"]
        self.assertTrue(checkpoint["available"])
        self.assertEqual(checkpoint["kind"], "quality_rejected")
        self.assertFalse(checkpoint["quality_passed"])
        self.assertEqual(checkpoint["actual_strict_rows"], 3011)
        self.assertEqual(checkpoint["target_summaries"][0]["target"], "T_max_core")
        self.assertEqual(checkpoint["target_summaries"][0]["metrics"]["r2"], 0.91)
        self.assertEqual(payload["surrogate"]["phase"], "candidate_training")
        self.assertTrue(payload["surrogate"]["training_active"])
        self.assertEqual(payload["surrogate"]["training_jobs_ready"], 1)
        self.assertEqual(payload["surrogate"]["hpo"]["ready"], 1)
        self.assertEqual(payload["surrogate"]["hpo"]["total"], 2)
        self.assertEqual(
            payload["surrogate"]["hpo"]["targets"][1]["target"], "P_loss"
        )
        completed_hpo = payload["surrogate"]["last_completed_hpo_results"]
        self.assertTrue(completed_hpo["available"])
        self.assertEqual(completed_hpo["target_count"], 2)
        self.assertEqual(completed_hpo["targets"][0]["target"], "Llt_phys")
        self.assertEqual(
            completed_hpo["targets"][0]["cv_mse_transformed"], 0.002359
        )
        self.assertEqual(completed_hpo["targets"][0]["best_params"]["num_leaves"], 63)
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
        target_metrics = payload["surrogate"]["active_model"]["target_metrics"]
        self.assertEqual(target_metrics["total"], 2)
        self.assertFalse(target_metrics["truncated"])
        self.assertEqual(target_metrics["items"][0]["target"], "Llt_phys")
        self.assertEqual(
            target_metrics["items"][0]["candidate"], 2.8019900580506305
        )
        self.assertEqual(
            target_metrics["items"][0]["incumbent"], 3.400370638116442
        )
        self.assertEqual(
            target_metrics["items"][0]["ratio"], 0.8240248950046014
        )
        self.assertEqual(payload["nsga"]["active_seed_workers"], 8)
        self.assertEqual(payload["nsga"]["completed_runs"], 3)
        self.assertEqual(payload["nsga"]["feasible_runs"], 1)
        self.assertEqual(payload["nsga"]["pareto_runs"], 1)
        self.assertEqual(payload["nsga"]["current_model_completed_runs"], 2)
        self.assertEqual(payload["nsga"]["lifetime_completed_runs"], 3)
        self.assertEqual(payload["nsga"]["lanes"][0]["current_model"]["feasible_runs"], 0)
        self.assertEqual(payload["nsga"]["lanes"][0]["lifetime"]["feasible_runs"], 1)
        self.assertEqual(len(payload["nsga"]["pareto_results"]), 1)
        self.assertEqual(payload["nsga"]["pareto_results"][0]["scope"], "lifetime")
        self.assertEqual(payload["nsga"]["pareto_results"][0]["points"][0]["design"]["N1_main"], 6.0)
        self.assertEqual(
            payload["nsga"]["lanes"][0]["least_violation"][
                "best_total_positive_violation"
            ],
            2.5,
        )
        self.assertEqual(
            payload["nsga"]["lanes"][0]["least_violation"][
                "zero_pass_constraints"
            ],
            ["Llt_robust_band"],
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

    def test_reader_rejects_invalid_canonical_schema_and_integer_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            write_json(
                root,
                "mft_pipeline/surrogate_status.json",
                {
                    "schema_version": 999,
                    "strict_full_rows": -5,
                    "queue": {"running": 1.9},
                    "last_jobs": {"train": 280.9},
                    "blocked": [],
                },
            )
            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        self.assertFalse(payload["canonical_training"]["available"])
        self.assertFalse(payload["canonical_training"]["pipeline_active"])
        self.assertIsNone(payload["canonical_training"]["strict_rows"])
        self.assertTrue(
            any(
                error["source"] == "canonical_surrogate_status"
                and "invalid canonical status contract" in error["message"]
                for error in payload["errors"]
            )
        )

    def test_reader_requires_exact_integer_canonical_schema_v1(self) -> None:
        for invalid_schema in (True, 1.0, "1"):
            with self.subTest(schema_version=invalid_schema):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    write_complete_runtime(root)
                    write_json(
                        root,
                        "mft_pipeline/surrogate_status.json",
                        {
                            "schema_version": invalid_schema,
                            "strict_full_rows": 120,
                            "queue": {"running": 1},
                        },
                    )
                    payload = MftPipelineStatusReader(
                        root, cache_seconds=0
                    ).snapshot()

                self.assertFalse(payload["canonical_training"]["available"])
                self.assertFalse(payload["canonical_training"]["pipeline_active"])

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

    def test_completed_hpo_result_fingerprint_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            status_path = root / "mft_pipeline/experimental_continuous/status.json"
            status = json.loads(status_path.read_text(encoding="utf-8"))
            status["last_completed_hpo_results"]["targets"][0][
                "cv_mse_transformed"
            ] = 0.0
            write_json(
                root,
                "mft_pipeline/experimental_continuous/status.json",
                status,
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        completed = payload["surrogate"]["last_completed_hpo_results"]
        self.assertFalse(completed["available"])
        self.assertEqual(
            completed["evidence_error"],
            "completed HPO result fingerprint mismatch",
        )

    def test_completed_hpo_signed_but_invalid_contract_fails_closed(self) -> None:
        completed = completed_hpo_fixture()
        completed["targets"][0]["family"] = "unexpected"
        resign_completed_hpo(completed)

        result = MftPipelineStatusReader._completed_hpo_results(
            {"last_completed_hpo_results": completed}
        )

        self.assertFalse(result["available"])
        self.assertEqual(
            result["evidence_error"], "invalid completed HPO target contract"
        )

    def test_completed_hpo_schema2_objectives_are_validated_and_visible(self) -> None:
        completed = completed_hpo_schema2_fixture()

        result = MftPipelineStatusReader._completed_hpo_results(
            {"last_completed_hpo_results": completed}
        )

        self.assertTrue(result["available"])
        self.assertEqual(result["schema_version"], 2)
        transformed = result["targets"][0]
        self.assertEqual(transformed["cv_objective_value"], 0.002359)
        self.assertEqual(transformed["objective_name"], "transformed_mse_v1")
        self.assertIn(
            "transformed-target squared error",
            transformed["objective_contract_summary"],
        )
        temperature = result["targets"][1]
        self.assertEqual(
            temperature["objective_name"],
            "temperature_gate_normalized_rmse_c_plus_p90_ape_v1",
        )
        self.assertIn("RMSE/5 degC", temperature["objective_contract_summary"])
        self.assertEqual(
            temperature["objective_contract"]["quality_thresholds_sha256"],
            "4aeb0a376d9017cde2a78dd8b965e99ed89134b0663185dd87547580c4cbc9b0",
        )

    def test_completed_hpo_schema2_tampered_objective_fails_closed(self) -> None:
        completed = completed_hpo_schema2_fixture()
        completed["targets"][1]["objective_contract"]["components"]["rmse_C"][
            "normalizer"
        ] = 6.0
        resign_completed_hpo(completed)

        result = MftPipelineStatusReader._completed_hpo_results(
            {"last_completed_hpo_results": completed}
        )

        self.assertFalse(result["available"])
        self.assertEqual(
            result["evidence_error"], "invalid completed HPO target contract"
        )

    def test_completed_hpo_schema2_boolean_objective_fails_closed(self) -> None:
        completed = completed_hpo_schema2_fixture()
        completed["targets"][0]["cv_objective_value"] = True
        resign_completed_hpo(completed)

        result = MftPipelineStatusReader._completed_hpo_results(
            {"last_completed_hpo_results": completed}
        )

        self.assertFalse(result["available"])
        self.assertEqual(
            result["evidence_error"], "invalid completed HPO target contract"
        )

    def test_completed_hpo_boolean_schema_version_fails_closed(self) -> None:
        completed = completed_hpo_fixture()
        completed["schema_version"] = True
        resign_completed_hpo(completed)

        result = MftPipelineStatusReader._completed_hpo_results(
            {"last_completed_hpo_results": completed}
        )

        self.assertFalse(result["available"])
        self.assertEqual(
            result["evidence_error"], "invalid completed HPO result contract"
        )

    def test_checkpoint_quality_pass_requires_complete_evidence(self) -> None:
        result = MftPipelineStatusReader._checkpoint_result({
            "schema_version": 1,
            "status": "evidence_error",
            "kind": "evidence_error",
            "quality_passed": True,
            "evidence_authentication": "failed",
            "error": {"type": "RuntimeError", "message": "tampered"},
        })

        self.assertFalse(result["available"])
        self.assertEqual(result["error"], "invalid checkpoint result contract")

    def test_checkpoint_evidence_error_is_visible(self) -> None:
        result = MftPipelineStatusReader._checkpoint_result({
            "schema_version": 1,
            "status": "evidence_error",
            "kind": "evidence_error",
            "quality_passed": None,
            "evidence_authentication": "failed",
            "error": {"type": "RuntimeError", "message": "artifact mismatch"},
        })

        self.assertFalse(result["available"])
        self.assertEqual(result["status"], "evidence_error")
        self.assertIn("artifact mismatch", result["evidence_error"])

    def test_invalid_pareto_points_fail_closed_and_surface_error(self) -> None:
        mutations = (
            lambda points: points[0]["design"].pop("gap1"),
            lambda points: points.__setitem__(0, "not-an-object"),
            lambda points: points[0].update(candidate_index=1, row_number=2),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    write_complete_runtime(root)
                    path = root / "mft_nsga_continuous/status.json"
                    status = json.loads(path.read_text(encoding="utf-8"))
                    mutate(status["latest_feasible_pareto_lifetime"]["points"])
                    write_json(root, "mft_nsga_continuous/status.json", status)

                    payload = MftPipelineStatusReader(
                        root, cache_seconds=0
                    ).snapshot()

                self.assertEqual(payload["nsga"]["pareto_results"], [])
                self.assertEqual(len(payload["nsga"]["pareto_errors"]), 1)
                self.assertIn(
                    "contract", payload["nsga"]["pareto_errors"][0]["error"]
                )

    def test_current_model_aggregate_reports_partial_lane_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            path = root / "mft_nsga_newmodel_fastlane_v2/status.json"
            status = json.loads(path.read_text(encoding="utf-8"))
            status.pop("current_model_completed_outcomes")
            write_json(root, "mft_nsga_newmodel_fastlane_v2/status.json", status)

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        self.assertFalse(payload["nsga"]["current_model_aggregate_complete"])
        self.assertEqual(payload["nsga"]["current_model_result_lanes"], 1)
        self.assertEqual(payload["nsga"]["current_model_expected_lanes"], 2)

    def test_checkpoint_result_reasons_and_targets_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            status_path = root / "mft_pipeline/surrogate_status.json"
            status = json.loads(status_path.read_text(encoding="utf-8"))
            status["last_checkpoint_result"] = {
                "schema_version": 1,
                "status": "failed",
                "kind": "quality_rejected",
                "threshold": 3000,
                "actual_strict_full_rows": 3011,
                "quality_passed": False,
                "completed_at": "2026-07-17T15:35:51+09:00",
                "reason_count": 30,
                "reasons": [f"reason-{index}" for index in range(12)],
                "reasons_truncated": True,
                "target_count": 30,
                "failed_target_count": 30,
                "target_summaries": [
                    {
                        "target": f"target-{index}",
                        "passed": False,
                        "blocking": True,
                        "reasons": ["r1", "r2", "r3", "r4"],
                        "reason_count": 5,
                        "reasons_truncated": True,
                        "metrics": {"r2": 0.5},
                    }
                    for index in range(24)
                ],
                "target_summaries_truncated": True,
                "evidence_authentication": "complete",
            }
            write_json(root, "mft_pipeline/surrogate_status.json", status)

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        checkpoint = payload["canonical_training"]["last_checkpoint_result"]
        self.assertEqual(len(checkpoint["reasons"]), 12)
        self.assertTrue(checkpoint["reasons_truncated"])
        self.assertEqual(len(checkpoint["target_summaries"]), 24)
        self.assertTrue(checkpoint["target_summaries_truncated"])
        self.assertEqual(
            set(checkpoint["target_summaries"][0]["metrics"]), {"r2"}
        )
        self.assertEqual(
            len(checkpoint["target_summaries"][0]["reasons"]), 4
        )

    def test_active_model_target_metrics_are_finite_bounded_and_text_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            comparisons = {
                '<img src=x onerror="alert(1)">': {
                    "metric": "normalized_rmse_pct",
                    "candidate": 0.12345678901234568,
                    "incumbent": 0.9876543210987654,
                    "ratio": 0.12500000000158204,
                }
            }
            comparisons.update(
                {
                    f"target-{index:02d}": {
                        "metric": "normalized_rmse_pct",
                        "candidate": index + 0.1,
                        "incumbent": index + 1.1,
                        "ratio": (index + 0.1) / (index + 1.1),
                    }
                    for index in range(27)
                }
            )
            comparisons.update(
                {
                    "bad-nan": {
                        "metric": "normalized_rmse_pct",
                        "candidate": "NaN",
                        "incumbent": 1.0,
                        "ratio": 1.0,
                    },
                    "bad-infinity": {
                        "metric": "normalized_rmse_pct",
                        "candidate": 1.0,
                        "incumbent": 1.0,
                        "ratio": "Infinity",
                    },
                    "bad-shape": "not-an-object",
                    "bad-empty-metric": {
                        "metric": "",
                        "candidate": 1.0,
                        "incumbent": 1.0,
                        "ratio": 1.0,
                    },
                }
            )
            write_json(
                root,
                "mft_pipeline/experimental_surrogate.json",
                {
                    "lane": "experimental",
                    "training_run_id": "model-target-metrics",
                    "incumbent_comparison": {"comparisons": comparisons},
                },
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        target_metrics = payload["surrogate"]["active_model"]["target_metrics"]
        self.assertEqual(target_metrics["limit"], 25)
        self.assertEqual(target_metrics["total"], 28)
        self.assertEqual(len(target_metrics["items"]), 25)
        self.assertTrue(target_metrics["truncated"])
        self.assertEqual(
            target_metrics["items"][0]["target"],
            '<img src=x onerror="alert(1)">',
        )
        self.assertEqual(
            target_metrics["items"][0]["candidate"], 0.12345678901234568
        )
        for item in target_metrics["items"]:
            self.assertLessEqual(len(item["target"]), 96)
            self.assertLessEqual(len(item["metric"]), 96)
            self.assertTrue(math.isfinite(item["candidate"]))
            self.assertTrue(math.isfinite(item["incumbent"]))
            self.assertTrue(math.isfinite(item["ratio"]))

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
        self.assertEqual(designs["total_count"], 14)
        self.assertEqual(designs["displayed_count"], 12)
        self.assertTrue(designs["truncated"])

    def test_all_twelve_standard_pass_designs_include_dominated_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_complete_runtime(root)
            candidates = {
                f"digest-{index:02d}": {
                    "candidate_digest": f"digest-{index:02d}",
                    "standard_task_id": 2000 + index,
                    "standard_actual_volume_L": 1300.0 + index,
                    "standard_actual_total_loss_W": 6000.0 + index,
                    "run_id": f"run-{index:02d}",
                    "model_id": "model-test",
                }
                for index in range(12)
            }
            write_json(
                root,
                "mft_nsga_full_model_validation/state.json",
                {"candidates": candidates},
            )
            write_json(
                root,
                "mft_nsga_full_model_validation/status.json",
                {"state": "active", "active_tasks": []},
            )

            payload = MftPipelineStatusReader(root, cache_seconds=0).snapshot()

        designs = payload["nsga"]["designs"]
        self.assertEqual(designs["total_count"], 12)
        self.assertEqual(designs["displayed_count"], 12)
        self.assertEqual(len(designs["items"]), 12)
        self.assertEqual(designs["nondominated_count"], 1)
        self.assertEqual(
            sum(1 for item in designs["items"] if not item["nondominated"]),
            11,
        )

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
        self.assertIn('id="mft-canonical-training-live"', html)
        self.assertIn('id="mft-canonical-training-state"', html)
        self.assertIn('id="mft-canonical-training-thresholds"', html)
        self.assertIn('id="mft-canonical-training-jobs"', html)
        self.assertIn('id="mft-canonical-training-issues"', html)
        self.assertIn('id="mft-canonical-checkpoint-summary"', html)
        self.assertIn('id="mft-canonical-checkpoint-targets"', html)
        self.assertIn('id="mft-nsga-live-results"', html)
        self.assertIn('id="mft-surrogate-hpo-targets"', html)
        self.assertIn('id="mft-completed-hpo-summary"', html)
        self.assertIn('id="mft-completed-hpo-targets"', html)
        self.assertIn('id="mft-surrogate-target-metrics"', html)
        self.assertIn('id="mft-surrogate-target-metrics-count"', html)
        self.assertIn('id="mft-validated-designs"', html)
        self.assertIn('id="mft-nsga-pareto-chart"', html)
        self.assertIn('id="mft-nsga-pareto-points"', html)
        self.assertIn('fetch("/api/mft-pipeline/status"', html)
        self.assertIn("window.setInterval(refresh, 15000)", html)
        self.assertIn('`${shown(full.running)} running`', html)
        self.assertIn('queue ${shown(full.queued)}', html)
        self.assertIn('PASS ${shown(full.pass)} / FAIL ${shown(full.fail)}', html)
        self.assertIn('document.createElement("a")', html)
        self.assertIn("Number.isSafeInteger(parsed) && parsed > 0", html)
        self.assertIn("link.href = `/tasks/${id}`", html)
        self.assertIn("stale / last good", html)
        self.assertIn("Standard/Full PASS는 검증 evidence", html)
        self.assertIn("activeMetrics.aggregate_loss_ratio", html)
        self.assertIn("activeModel.target_metrics", html)
        self.assertIn("targetMetrics.items.slice(0, 25)", html)
        self.assertIn("targetMetricBody.textContent = \"\"", html)
        self.assertIn("targetMetric.candidate", html)
        self.assertIn("activeModel.dataset_sha256", html)
        self.assertIn("canonicalTraining.pipeline_active", html)
        self.assertIn("canonicalTraining.available === true", html)
        self.assertIn("정식 파이프라인 상태 소스 unavailable", html)
        self.assertIn("canonicalLastJobs.train", html)
        self.assertIn("canonicalTraining.rows_until_first_tuning", html)
        self.assertIn("canonicalQueue.cancelled", html)
        self.assertIn("canonicalTraining.raw_rows", html)
        self.assertIn('.filter((item) => item && typeof item === "object")', html)
        self.assertIn("currentModel.infeasible_runs", html)
        self.assertIn("lifetime.infeasible_runs", html)
        self.assertIn("surrogate.last_completed_hpo_results", html)
        self.assertIn("CV objective", html)
        self.assertIn("Objective contract", html)
        self.assertIn("target.cv_objective_value", html)
        self.assertIn("target.objective_name", html)
        self.assertIn("target.objective_contract_summary", html)
        self.assertIn("canonicalTraining.last_checkpoint_result", html)
        self.assertIn("nsga.pareto_results", html)
        self.assertIn('document.createElementNS(svg, "circle")', html)
        self.assertIn("point.candidate_index", html)
        self.assertIn("zero-pass:", html)
        panel = html[
            html.index('id="mft-continuous-pipeline"') : html.index(
                '<section class="panel">',
                html.index('id="mft-continuous-pipeline"'),
            )
        ]
        self.assertNotIn("<form", panel)
        self.assertNotIn(".innerHTML", panel)

    def test_dashboard_distinguishes_surrogate_gate_from_actual_fea(self) -> None:
        response = self.route_endpoint("/", "GET")(self.dashboard_request())
        html = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('id="mft-nsga-scope-summary"', html)
        self.assertIn('id="mft-nsga-surrogate-gate"', html)
        self.assertIn('id="mft-nsga-actual-fea"', html)
        self.assertIn('id="mft-nsga-zero-pass-constraints"', html)
        self.assertIn("실제 FEA 결과 아님", html)
        self.assertIn("물리 실패 판정 아님", html)
        self.assertIn("nsga.current_model_feasible_runs", html)
        self.assertIn("designs.known_standard_pass_count", html)
        self.assertIn("designs.nondominated_count", html)
        self.assertIn("lane.least_violation.zero_pass_constraints", html)
        self.assertIn("dominantZeroPassConstraints", html)
        self.assertIn("zeroPassConstraintCounts", html)


if __name__ == "__main__":
    unittest.main()
