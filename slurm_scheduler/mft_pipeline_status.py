from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_RUNTIME_ROOT = Path(r"C:\Users\peets\slurm_scheduler_runtime")
DEFAULT_MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_HPO_TARGETS = 20
MAX_COMPLETED_HPO_TARGETS = 8
MAX_ACTIVE_MODEL_TARGET_METRICS = 25
MAX_CHECKPOINT_TARGETS = 24
MAX_CHECKPOINT_REASONS = 12
MAX_NSGA_PARETO_POINTS = 500
MAX_NSGA_PARETO_ROWS = 10_000
MAX_VALIDATED_DESIGNS = 12
MAX_COMPLETED_HPO_STATUS_BYTES = 32_768
TARGETED_HPO_STATUS_SCHEMA = "mft-targeted-hpo-batch-v1"
ISOLATED_NSGA_STATUS_SCHEMA = "mft-isolated-nsga-live-v1"
POST_TARGETED_FULL25_STATUS_SCHEMA = "mft-post-targeted-full25-v1"
APPROVED_NSGA_STATUS_SCHEMA = "mft-continuous-nsga-v1"
MAX_DYNAMIC_STATUS_FILES = 16
POST_TARGETED_FULL25_PHASES = frozenset({
    "waiting_targeted_hpo",
    "targeted_hpo_authenticated",
    "candidate_training",
    "candidate_quality_gate",
    "candidate_pass24_replay",
    "candidate_quality_failed_closed",
    "candidate_atomic_activation",
    "approved_nsga_launch",
    "approved_model_active_nsga_running",
    "candidate_failed_closed",
    "activation_committed_nsga_launch_failed_closed",
})
POST_TARGETED_FULL25_TERMINAL_PHASES = frozenset({
    "candidate_quality_failed_closed",
    "approved_model_active_nsga_running",
    "candidate_failed_closed",
    "activation_committed_nsga_launch_failed_closed",
})
CHECKPOINT_METRIC_KEYS = (
    "n_train",
    "n_calibration",
    "n_evaluation",
    "n_holdout",
    "r2",
    "rmse",
    "normalized_rmse_pct",
    "mape_pct",
    "p90_ape_pct",
    "interval_coverage",
    "interval_p90_half_width_pct",
    "interval_p90_width",
)
NSGA_DESIGN_FIELDS = frozenset({
    "N1_main", "N1_side", "N2_main", "N2_side",
    "l1", "l2", "h1", "w1", "n_core_group",
    "core_plate_t", "core_plate_on", "cw1", "gap1", "cw2", "gap2",
    "nwh1", "nwh2", "cc_w2c_space_x", "cc_w2c_space_y",
    "w2c_w1c_space_x", "w2c_w1c_space_y", "w1c_w2s_space_x",
    "w2s_w1s_space_x", "w1s_w2s_space_y", "w1s_cs_space_x",
    "cs_w1s_space_y", "wcp_t", "wcp_pad_t", "wcp_len_x", "wcp_on",
    "core_plate_pad_t", "core_depth_min", "core_depth_max",
    "core_depth_each",
})
HPO_PARAM_NAMES = (
    "n_estimators",
    "learning_rate",
    "num_leaves",
    "min_child_samples",
    "subsample",
    "colsample_bytree",
    "reg_lambda",
)
HPO_PARAM_RANGES = {
    "n_estimators": (int, 400, 3000),
    "learning_rate": (float, 0.01, 0.15),
    "num_leaves": (int, 31, 255),
    "min_child_samples": (int, 5, 60),
    "subsample": (float, 0.6, 1.0),
    "colsample_bytree": (float, 0.6, 1.0),
    "reg_lambda": (float, 0.001, 10.0),
}
COMPLETED_HPO_TOP_KEYS = frozenset({
    "schema_version",
    "status",
    "evidence_authentication",
    "wave",
    "result_phase",
    "completed_at",
    "dataset_generation",
    "dataset_sha256",
    "strict_full_rows",
    "target_count",
    "targets",
    "common_authentication",
    "wave_outcome",
    "evidence_sha256",
})
COMPLETED_HPO_SCHEMA_VERSIONS = frozenset({1, 2})
COMPLETED_HPO_TARGET_KEYS_V1 = frozenset({
    "target",
    "family",
    "cv_mse_transformed",
    "eligible_rows",
    "target_rows",
    "hpo_train_rows",
    "trials",
    "model_threads",
    "best_params",
    "authentication",
})
COMPLETED_HPO_TARGET_KEYS_V2 = frozenset(
    COMPLETED_HPO_TARGET_KEYS_V1.difference({"cv_mse_transformed"})
    | {"cv_objective_value", "objective_contract"}
)
COMPLETED_HPO_TARGET_AUTH_KEYS = frozenset({
    "generation_id",
    "params_sha256",
    "manifest_sha256",
    "tuned_override_sha256",
    "target_task_ids_sha256",
    "hpo_train_task_ids_sha256",
})
COMPLETED_HPO_COMMON_AUTH_KEYS = frozenset({
    "dataset_manifest_sha256",
    "data_contract_sha256",
    "search_implementation_sha256",
    "training_split_contract_sha256",
    "feature_schema_sha256",
})
HPO_OBJECTIVE_SCHEMA = "mft-hpo-objective-v1"
TRANSFORMED_MSE_OBJECTIVE = "transformed_mse_v1"
TEMPERATURE_PHYSICAL_OBJECTIVE = (
    "temperature_gate_normalized_rmse_c_plus_p90_ape_v1"
)
TEMPERATURE_HPO_QUALITY_THRESHOLDS_SHA256 = (
    "4aeb0a376d9017cde2a78dd8b965e99ed89134b0663185dd87547580c4cbc9b0"
)
SURROGATE_TEMPERATURE_TARGETS = frozenset({
    "T_max_Tx",
    "T_max_Rx_main",
    "T_max_Rx_side",
    "T_max_core",
    "Tprobe_Tx_leeward_max",
    "Tprobe_Rx_main_leeward_max",
    "Tprobe_Rx_side_leeward_max",
    "Tprobe_core_center_max",
    "Tprobe_core_center_leg_max",
    "Tprobe_core_side_leg_max",
    "Tprobe_core_top_yoke_max",
})
HPO_OBJECTIVE_COMMON_KEYS = frozenset({
    "schema_version",
    "name",
    "units",
    "scope",
    "fold_aggregation",
    "components",
})
HPO_TEMPERATURE_OBJECTIVE_KEYS = frozenset(
    HPO_OBJECTIVE_COMMON_KEYS
    | {
        "p90_quantile_method",
        "relative_error_denominator",
        "quality_thresholds_sha256",
    }
)
HPO_TEMPERATURE_COMPONENT_KEYS = frozenset({
    "weight",
    "normalizer",
    "normalizer_units",
    "gate_metric",
})
NSGA_PARETO_KEYS = frozenset({
    "schema_version",
    "available",
    "scope",
    "run_id",
    "model_id",
    "model_lane",
    "production_eligible",
    "fea_submission_approved",
    "seeds",
    "finished_at",
    "point_count",
    "points",
    "limit",
    "truncated",
    "objective_names",
    "objective_units",
    "objective_source",
    "fea_verified",
    "optimization_manifest_sha256",
    "pareto_front_sha256",
    "run_manifest_sha256",
    "model_source_sha256",
})
NSGA_PARETO_POINT_KEYS = frozenset({
    "candidate_index",
    "row_number",
    "volume_L",
    "total_loss_W",
    "design",
})
CHECKPOINT_COMPLETE_KINDS = frozenset({
    "metrics_only",
    "accepted_generation",
    "quality_rejected",
})
CHECKPOINT_PARTIAL_EVIDENCE = frozenset({
    "state_only",
    "state_and_metrics",
    "state_metrics_and_candidate",
})
_HEX_64_RE = re.compile(r"[0-9a-f]{64}")
_HEX_40_RE = re.compile(r"[0-9a-f]{40}")
_HPO_TARGET_RE = re.compile(r"[A-Za-z0-9_]{1,128}")
_HPO_RUN_RE = re.compile(r"[A-Za-z0-9._-]{1,160}")
_HPO_WAVE_RE = re.compile(r"wave-[0-9a-f]{16}")
_NSGA_RUN_RE = re.compile(r"run-[0-9]{6}")

STALE_AFTER_SECONDS = {
    "canonical_surrogate_status": 150.0,
    "surrogate_status": 75.0,
    "nsga": 75.0,
    "validation": 90.0,
    "targeted_hpo": 75.0,
    "isolated_nsga": 75.0,
    "post_targeted_full25": 75.0,
    "approved_nsga": 75.0,
}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _canonical_json_sha256(value: object) -> str | None:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(encoded).hexdigest()


def _items(value: object) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        values: Iterable[object] = value.values()
    elif isinstance(value, list):
        values = value
    else:
        return []
    return [dict(item) for item in values if isinstance(item, Mapping)]


def _integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _nonnegative_integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return result if result is not None and math.isfinite(result) else None


def _strict_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    normalized = float(value)
    return normalized if math.isfinite(normalized) else None


def _exact_sha256(value: object) -> str | None:
    text = str(value or "").strip().lower()
    return text if _HEX_64_RE.fullmatch(text) else None


def _positive_integer(value: object, *, maximum: int | None = None) -> int | None:
    normalized = _nonnegative_integer(value)
    if normalized is None or normalized == 0:
        return None
    if maximum is not None and normalized > maximum:
        return None
    return normalized


def _validated_hpo_params(value: object) -> dict[str, int | float] | None:
    if not isinstance(value, Mapping) or set(value) != set(HPO_PARAM_RANGES):
        return None
    normalized: dict[str, int | float] = {}
    for name, (kind, lower, upper) in HPO_PARAM_RANGES.items():
        raw = value.get(name)
        if kind is int:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return None
            number: int | float = raw
        else:
            strict = _strict_number(raw)
            if strict is None:
                return None
            number = strict
        if float(number) < lower or float(number) > upper:
            return None
        normalized[name] = number
    return normalized


def _exact_sha_mapping(value: object, expected_keys: frozenset[str]) -> bool:
    return bool(
        isinstance(value, Mapping)
        and set(value) == expected_keys
        and all(_exact_sha256(value.get(key)) is not None for key in expected_keys)
    )


def _validated_hpo_objective_contract(
    target: str, value: object
) -> dict[str, Any] | None:
    """Validate and normalize the authenticated schema-2 HPO objective."""
    if not isinstance(value, Mapping):
        return None
    contract = dict(value)
    is_temperature = target in SURROGATE_TEMPERATURE_TARGETS
    expected_keys = (
        HPO_TEMPERATURE_OBJECTIVE_KEYS
        if is_temperature
        else HPO_OBJECTIVE_COMMON_KEYS
    )
    if (
        set(contract) != expected_keys
        or contract.get("schema_version") != HPO_OBJECTIVE_SCHEMA
        or contract.get("scope") != "model_fit_partition_cv_only"
        or contract.get("fold_aggregation") != "mean"
    ):
        return None

    components = contract.get("components")
    if not isinstance(components, Mapping):
        return None
    if not is_temperature:
        component_value = _strict_number(components.get("mse_transformed"))
        if (
            contract.get("name") != TRANSFORMED_MSE_OBJECTIVE
            or contract.get("units") != "transformed_target_squared"
            or set(components) != {"mse_transformed"}
            or component_value != 1.0
        ):
            return None
        normalized_contract = {
            "schema_version": HPO_OBJECTIVE_SCHEMA,
            "name": TRANSFORMED_MSE_OBJECTIVE,
            "units": "transformed_target_squared",
            "scope": "model_fit_partition_cv_only",
            "fold_aggregation": "mean",
            "components": {"mse_transformed": 1.0},
        }
        return {
            "name": TRANSFORMED_MSE_OBJECTIVE,
            "summary": "model-fit CV only; mean transformed-target squared error",
            "contract": normalized_contract,
        }

    if (
        contract.get("name") != TEMPERATURE_PHYSICAL_OBJECTIVE
        or contract.get("units") != "dimensionless_gate_ratio_sum"
        or contract.get("p90_quantile_method") != "numpy_linear"
        or contract.get("relative_error_denominator")
        != "absolute_truth_temperature_c"
        or contract.get("quality_thresholds_sha256")
        != TEMPERATURE_HPO_QUALITY_THRESHOLDS_SHA256
        or set(components) != {"rmse_C", "p90_ape_pct"}
    ):
        return None
    component_contracts = {
        "rmse_C": (5.0, "degC", "max_rmse"),
        "p90_ape_pct": (10.0, "percent", "max_p90_ape_pct"),
    }
    normalized_components: dict[str, dict[str, Any]] = {}
    for name, (expected_normalizer, units, gate_metric) in component_contracts.items():
        component = components.get(name)
        if not isinstance(component, Mapping):
            return None
        weight = _strict_number(component.get("weight"))
        normalizer = _strict_number(component.get("normalizer"))
        if (
            set(component) != HPO_TEMPERATURE_COMPONENT_KEYS
            or weight != 1.0
            or normalizer != expected_normalizer
            or component.get("normalizer_units") != units
            or component.get("gate_metric") != gate_metric
        ):
            return None
        normalized_components[name] = {
            "weight": 1.0,
            "normalizer": expected_normalizer,
            "normalizer_units": units,
            "gate_metric": gate_metric,
        }
    normalized_contract = {
        "schema_version": HPO_OBJECTIVE_SCHEMA,
        "name": TEMPERATURE_PHYSICAL_OBJECTIVE,
        "units": "dimensionless_gate_ratio_sum",
        "scope": "model_fit_partition_cv_only",
        "fold_aggregation": "mean",
        "components": normalized_components,
        "p90_quantile_method": "numpy_linear",
        "relative_error_denominator": "absolute_truth_temperature_c",
        "quality_thresholds_sha256": TEMPERATURE_HPO_QUALITY_THRESHOLDS_SHA256,
    }
    return {
        "name": TEMPERATURE_PHYSICAL_OBJECTIVE,
        "summary": (
            "model-fit CV only; mean(RMSE/5 degC + P90 APE/10%); "
            f"thresholds {TEMPERATURE_HPO_QUALITY_THRESHOLDS_SHA256[:12]}"
        ),
        "contract": normalized_contract,
    }


def _text_items(value: object, *, limit: int = 20) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [
        str(item)
        for item in value[:limit]
        if item is not None and str(item).strip()
    ]


def _bounded_text_items(
    value: object, *, item_limit: int, text_limit: int = 300
) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [
        text
        for item in value[:item_limit]
        if (text := _bounded_text(item, limit=text_limit))
    ]


def _bounded_text(value: object, *, limit: int = 200) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, (Mapping, list, tuple)):
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            text = str(value)
    else:
        text = str(value)
    text = " ".join(text.split())
    return text[:limit]


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _model_id(pointer: Mapping[str, Any]) -> str:
    explicit = str(pointer.get("model_id") or "").strip()
    if explicit:
        return explicit
    lane = str(pointer.get("lane") or "experimental").strip() or "experimental"
    training_run = str(pointer.get("training_run_id") or "").strip()
    dataset_hash = str(pointer.get("dataset_sha256") or "").strip()
    report_hash = str(pointer.get("generation_report_sha256") or "").strip()
    if training_run and dataset_hash and report_hash:
        return f"{lane}:{training_run}:{dataset_hash[:16]}:{report_hash[:16]}"
    return training_run


def _canonical_contract_errors(payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    schema_version = payload.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        schema_text = _bounded_text(schema_version, limit=80) or "<missing>"
        errors.append(f"unsupported schema_version={schema_text}")
    for name in (
        "cycle",
        "raw_rows",
        "strict_full_rows",
        "activation_minimum_strict_full_rows",
        "first_tuning_strict_full_rows",
    ):
        if name in payload and _nonnegative_integer(payload.get(name)) is None:
            errors.append(f"{name} must be a nonnegative integer")
    queue = payload.get("queue")
    if queue is not None and not isinstance(queue, Mapping):
        errors.append("queue must be an object")
    elif isinstance(queue, Mapping):
        for name in (
            "running",
            "queued",
            "retry_wait",
            "succeeded",
            "failed",
            "cancelled",
        ):
            if name in queue and _nonnegative_integer(queue.get(name)) is None:
                errors.append(f"queue.{name} must be a nonnegative integer")
    last_jobs = payload.get("last_jobs")
    if last_jobs is not None and not isinstance(last_jobs, Mapping):
        errors.append("last_jobs must be an object")
    elif isinstance(last_jobs, Mapping):
        for name in ("collect", "train"):
            if name not in last_jobs or last_jobs.get(name) is None:
                continue
            job_id = _nonnegative_integer(last_jobs.get(name))
            if job_id is None or job_id == 0:
                errors.append(f"last_jobs.{name} must be a positive integer")
    if "blocked" in payload and not isinstance(payload.get("blocked"), Mapping):
        errors.append("blocked must be an object")
    return errors


class MftPipelineStatusReader:
    """Read the MFT controllers' small, local status contracts.

    The scheduler loop never calls this reader.  Only the read-only WEB API
    does, and it reads a fixed list of local JSON files with a hard byte cap.
    A short cache and a non-blocking single-flight lock keep refresh bursts
    from consuming WEB worker threads.  Every source fails independently so a
    controller replacing one status file cannot blank the rest of the panel.
    """

    def __init__(
        self,
        runtime_root: str | os.PathLike[str] | None = None,
        *,
        max_json_bytes: int = DEFAULT_MAX_JSON_BYTES,
        cache_seconds: float = 5.0,
    ) -> None:
        configured_root = runtime_root or os.environ.get(
            "SLURM_MFT_PIPELINE_RUNTIME_ROOT"
        )
        self.runtime_root = Path(configured_root) if configured_root else DEFAULT_RUNTIME_ROOT
        self.max_json_bytes = max(1024, int(max_json_bytes))
        self.cache_seconds = max(0.0, float(cache_seconds))
        self._cache_lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._snapshot: dict[str, Any] | None = None
        self._snapshot_at = 0.0
        self._file_cache: dict[
            Path, tuple[int, int, dict[str, Any]]
        ] = {}
        self._last_good: dict[Path, dict[str, Any]] = {}

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._cache_lock:
            cached = self._snapshot
            if (
                cached is not None
                and self.cache_seconds > 0
                and now - self._snapshot_at <= self.cache_seconds
            ):
                return copy.deepcopy(cached)

        if not self._refresh_lock.acquire(blocking=False):
            if cached is not None:
                return copy.deepcopy(cached)
            return self._empty_snapshot("status refresh already in progress")

        try:
            result = self._build_snapshot()
            with self._cache_lock:
                self._snapshot = result
                self._snapshot_at = time.monotonic()
            return copy.deepcopy(result)
        except Exception as exc:  # The visibility endpoint must always fail soft.
            if cached is not None:
                fallback = copy.deepcopy(cached)
                fallback.setdefault("errors", []).append(
                    {"source": "reader", "message": f"{type(exc).__name__}: {exc}"}
                )
                return fallback
            return self._empty_snapshot(f"{type(exc).__name__}: {exc}")
        finally:
            self._refresh_lock.release()

    def _empty_snapshot(self, message: str) -> dict[str, Any]:
        return {
            "schema_version": "mft-pipeline-visibility-v2",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "data": {"available": False},
            "canonical_training": {
                "available": False,
                "pipeline_active": False,
                "queue": {
                    "running": 0,
                    "queued": 0,
                    "retry_wait": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "cancelled": 0,
                },
                "last_jobs": {"collect": None, "train": None},
                "blocked": [],
                "last_checkpoint_result": {"available": False},
            },
            "surrogate": {
                "available": False,
                "hpo": {"ready": 0, "total": 0, "targets": []},
                "last_completed_hpo_results": {
                    "available": False,
                    "targets": [],
                },
                "last_decision": {"available": False},
                "post_targeted_full25": {"available": False},
            },
            "nsga": {
                "available": False,
                "lanes": [],
                "active_seed_workers": 0,
                "current_model_completed_runs": 0,
                "lifetime_completed_runs": 0,
                "pareto_results": [],
                "pareto_errors": [],
                "current_model_result_lanes": 0,
                "current_model_expected_lanes": 0,
                "current_model_aggregate_complete": False,
                "designs": {
                    "items": [],
                    "nondominated_count": 0,
                    "active_full_count": 0,
                    "total_count": 0,
                    "displayed_count": 0,
                    "ready_full_count": 0,
                    "known_standard_pass_count": 0,
                    "full_discovered_standard_pass_count": 0,
                    "pending_full_discovery_count": 0,
                    "limit": MAX_VALIDATED_DESIGNS,
                    "truncated": False,
                },
            },
            "standard_fea": {"available": False, "active": 0, "pass": 0, "fail": 0},
            "full_model": {"available": False, "active": 0, "pass": 0},
            "sources": {},
            "stale_sources": [],
            "errors": [{"source": "reader", "message": message}],
        }

    @staticmethod
    def _freshness(
        payload: Mapping[str, Any],
        meta: Mapping[str, Any],
        *,
        stale_after_seconds: float | None,
    ) -> dict[str, Any]:
        updated_at = payload.get("updated_at")
        if not updated_at:
            updated_at = _mapping(payload.get("active_wave_detail")).get("updated_at")
        parsed = _parse_timestamp(updated_at)
        age_seconds = None
        if parsed is not None:
            age_seconds = max(
                0.0,
                (datetime.now(timezone.utc) - parsed).total_seconds(),
            )
        stale = bool(meta.get("stale")) or bool(
            stale_after_seconds is not None
            and age_seconds is not None
            and age_seconds > stale_after_seconds
        )
        return {
            "available": bool(meta.get("available")),
            "stale": stale,
            "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            "stale_after_seconds": stale_after_seconds,
            "updated_at": updated_at,
            "last_good_fallback": bool(meta.get("stale")),
        }

    def _read_json(self, source: str, relative_path: str) -> tuple[dict[str, Any], dict[str, Any]]:
        path = self.runtime_root / Path(relative_path)
        try:
            stat = path.stat()
            if stat.st_size > self.max_json_bytes:
                raise ValueError(
                    f"JSON file is {stat.st_size} bytes; limit is {self.max_json_bytes}"
                )
            cached = self._file_cache.get(path)
            if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
                return copy.deepcopy(cached[2]), {
                    "source": source,
                    "available": True,
                    "stale": False,
                    "size_bytes": stat.st_size,
                }
            with path.open("rb") as stream:
                # fstat describes the exact file handle being read even when a
                # producer atomically replaces the path between stat/open.
                opened_stat = os.fstat(stream.fileno())
                if opened_stat.st_size > self.max_json_bytes:
                    raise ValueError(
                        f"JSON file is {opened_stat.st_size} bytes; limit is {self.max_json_bytes}"
                    )
                raw = stream.read(self.max_json_bytes + 1)
            if len(raw) > self.max_json_bytes:
                raise ValueError(f"JSON read exceeded {self.max_json_bytes} bytes")
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("JSON root must be an object")
            normalized = dict(payload)
            self._file_cache[path] = (
                opened_stat.st_mtime_ns,
                opened_stat.st_size,
                normalized,
            )
            self._last_good[path] = normalized
            return copy.deepcopy(normalized), {
                "source": source,
                "available": True,
                "stale": False,
                "size_bytes": opened_stat.st_size,
            }
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
            last_good = self._last_good.get(path)
            return copy.deepcopy(last_good or {}), {
                "source": source,
                "available": bool(last_good),
                "stale": bool(last_good),
                "message": f"{type(exc).__name__}: {exc}",
            }

    def _read_first(
        self, source: str, relative_paths: Iterable[str]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        errors: list[str] = []
        stale: tuple[dict[str, Any], dict[str, Any]] | None = None
        for relative_path in relative_paths:
            payload, meta = self._read_json(source, relative_path)
            if meta.get("available") and not meta.get("stale"):
                return payload, meta
            if meta.get("available") and stale is None:
                stale = (payload, meta)
            if meta.get("message"):
                errors.append(str(meta["message"]))
        if stale is not None:
            return stale
        return {}, {
            "source": source,
            "available": False,
            "stale": False,
            "message": "; ".join(errors) or "status file unavailable",
        }

    def _dynamic_json_sources(
        self,
        source_prefix: str,
        relative_pattern: str,
    ) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        """Read a bounded newest-first set of status files below runtime_root."""

        try:
            root = self.runtime_root.resolve()
            candidates: list[tuple[int, Path]] = []
            for path in self.runtime_root.glob(relative_pattern):
                try:
                    resolved = path.resolve(strict=True)
                    resolved.relative_to(root)
                    stat = resolved.stat()
                except (OSError, RuntimeError, ValueError):
                    continue
                if resolved.is_file():
                    candidates.append((stat.st_mtime_ns, resolved))
            candidates.sort(key=lambda item: (-item[0], str(item[1])))
        except (OSError, RuntimeError, ValueError):
            candidates = []

        results: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for index, (_mtime, path) in enumerate(
            candidates[:MAX_DYNAMIC_STATUS_FILES]
        ):
            relative = path.relative_to(root).as_posix()
            payload, meta = self._read_json(
                f"{source_prefix}_{index}", relative
            )
            results.append((payload, {**meta, "relative_path": relative}))
        return results

    @staticmethod
    def _targeted_hpo(
        status: Mapping[str, Any],
        meta: Mapping[str, Any],
    ) -> dict[str, Any]:
        unavailable = {
            "available": False,
            "phase": "",
            "wave": "",
            "hpo": {"ready": 0, "running": 0, "total": 0, "targets": []},
            "result": {},
            "freshness": MftPipelineStatusReader._freshness(
                {}, meta, stale_after_seconds=STALE_AFTER_SECONDS["targeted_hpo"]
            ),
        }
        if not meta.get("available"):
            return unavailable

        phase = str(status.get("phase") or "")
        allowed_phases = {
            "hpo_queued",
            "hpo_running",
            "hpo_merging",
            "hpo_completed_no_publish",
            "hpo_failed_closed",
        }
        targets = status.get("targets")
        jobs = status.get("jobs")
        target_count = _positive_integer(
            status.get("target_count"), maximum=MAX_HPO_TARGETS
        )
        budget = _mapping(status.get("budget"))
        publication = _mapping(status.get("publication"))
        dataset = _mapping(status.get("dataset"))
        code = _mapping(status.get("code"))
        pins = _mapping(status.get("pins"))
        objective_contracts = _mapping(status.get("objective_contracts"))
        valid_targets = bool(
            target_count is not None
            and isinstance(targets, list)
            and isinstance(jobs, list)
            and len(targets) == target_count
            and len(jobs) == target_count
            and len(set(targets)) == target_count
            and all(
                isinstance(target, str)
                and _HPO_TARGET_RE.fullmatch(target) is not None
                for target in targets
            )
        )
        valid_hashes = bool(
            _exact_sha256(dataset.get("sha256"))
            and set(code) == {
                "runner_sha256",
                "tune_optuna_sha256",
                "merge_params_implementation_sha256",
            }
            and all(_exact_sha256(value) for value in code.values())
            and set(objective_contracts) == set(targets or [])
            and all(
                _exact_sha256(_mapping(value).get("sha256"))
                == _canonical_json_sha256(_mapping(value).get("contract"))
                for value in objective_contracts.values()
            )
            and all(
                re.fullmatch(r"[0-9a-f]{40}", str(pins.get(name) or ""))
                for name in ("solver_revision", "library_revision")
            )
            and _exact_sha256(pins.get("data_contract_sha256"))
        )
        valid_contract = bool(
            status.get("schema_version") == TARGETED_HPO_STATUS_SCHEMA
            and phase in allowed_phases
            and _HPO_RUN_RE.fullmatch(str(status.get("wave") or ""))
            and valid_targets
            and valid_hashes
            and _nonnegative_integer(budget.get("max_processes")) == 4
            and _nonnegative_integer(budget.get("model_threads_per_process")) == 2
            and _nonnegative_integer(budget.get("maximum_total_model_threads")) == 8
            and publication.get("allowed") is False
            and publication.get("attempted") is False
        )
        normalized: list[dict[str, Any]] = []
        if valid_contract:
            allowed_states = {
                "queued",
                "running",
                "completed",
                "failed",
                "cancelled_after_peer_failure",
            }
            for index, (target, raw_job) in enumerate(zip(targets, jobs)):
                job = _mapping(raw_job)
                state = str(job.get("state") or "")
                result_sha = job.get("result_json_sha256")
                if (
                    _nonnegative_integer(job.get("index")) != index
                    or job.get("target") != target
                    or job.get("family") != "lightgbm"
                    or state not in allowed_states
                    or _nonnegative_integer(job.get("model_threads")) != 2
                    or _positive_integer(job.get("trials")) is None
                    or (
                        state == "completed"
                        and _exact_sha256(result_sha) is None
                    )
                ):
                    valid_contract = False
                    normalized = []
                    break
                normalized.append({
                    "target": target,
                    "family": "lightgbm",
                    "status": state,
                    "alive": state == "running",
                    "result_ready": state == "completed",
                    "model_threads": 2,
                    "trials": _positive_integer(job.get("trials")),
                    "cpu_seconds": None,
                    "source": "targeted_failed_gate_cohort",
                })
        if not valid_contract:
            return {
                **unavailable,
                "error": "invalid targeted HPO live-status contract",
            }

        result = _mapping(status.get("result"))
        completed = phase == "hpo_completed_no_publish"
        result_hash = _exact_sha256(result.get("merged_params_sha256"))
        if completed and (
            result_hash is None
            or _nonnegative_integer(result.get("completed_target_count"))
            != target_count
            or _exact_sha256(result.get("tuning_evidence_sha256")) is None
        ):
            return {
                **unavailable,
                "error": "invalid targeted HPO completion evidence",
            }
        return {
            "available": True,
            "phase": phase,
            "wave": str(status.get("wave") or ""),
            "eligibility": _bounded_text(status.get("eligibility"), limit=80),
            "dataset_sha256": str(dataset.get("sha256") or ""),
            "hpo": {
                "ready": sum(1 for job in normalized if job["result_ready"]),
                "running": sum(1 for job in normalized if job["alive"]),
                "total": target_count,
                "targets": normalized,
                "processes": 4,
                "thread_budget": 8,
                "trials_per_target": (
                    normalized[0]["trials"] if normalized else None
                ),
                "total_trials": sum(job["trials"] or 0 for job in normalized),
                "limit": MAX_HPO_TARGETS,
                "truncated": False,
            },
            "result": {
                "available": completed,
                "merged_params_sha256": result_hash or "",
                "completed_target_count": _nonnegative_integer(
                    result.get("completed_target_count")
                ),
                "tuning_evidence_sha256": str(
                    result.get("tuning_evidence_sha256") or ""
                ),
            },
            "error": _bounded_text(
                _mapping(status.get("error")).get("message"), limit=300
            ),
            "freshness": MftPipelineStatusReader._freshness(
                status,
                meta,
                stale_after_seconds=STALE_AFTER_SECONDS["targeted_hpo"],
            ),
            "updated_at": status.get("updated_at"),
        }

    @classmethod
    def _post_targeted_full25(
        cls,
        status: Mapping[str, Any],
        meta: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Normalize the isolated full-25 train/gate/replay/activation watcher."""

        phase = _bounded_text(status.get("phase"), limit=80)
        lane = _bounded_text(status.get("lane"), limit=80)
        eligibility = _bounded_text(status.get("eligibility"), limit=120)
        code = _mapping(status.get("code"))
        cpu = _mapping(status.get("cpu_budget"))
        publication = _mapping(status.get("publication"))
        deployment_commit = str(code.get("deployment_commit") or "").lower()
        code_root = _bounded_text(code.get("root"), limit=500)
        code_root_name = code_root.replace("\\", "/").rstrip("/").split("/")[-1].lower()
        allowed_eligibility = {
            "NO-POINTER-NO-FEA-NO-NSGA-SEARCH",
            "APPROVED-MODEL-ACTIVE-NSGA-LAUNCH-PENDING",
            "APPROVED-MODEL-ACTIVE-NSGA-SEARCH-FEA-DISABLED",
            "APPROVED-MODEL-ACTIVE-NSGA-LAUNCH-FAILED-CLOSED",
        }
        base_valid = bool(
            meta.get("available")
            and status.get("schema_version") == POST_TARGETED_FULL25_STATUS_SCHEMA
            and phase in POST_TARGETED_FULL25_PHASES
            and lane in {"isolated_candidate", "approved_model"}
            and eligibility in allowed_eligibility
            and _positive_integer(status.get("watcher_pid")) is not None
            and _HEX_40_RE.fullmatch(deployment_commit)
            and code_root_name == deployment_commit
            and _exact_sha256(code.get("watcher_sha256")) is not None
            and _exact_sha256(code.get("continuous_nsga_sha256")) is not None
            and _positive_integer(cpu.get("target_workers")) == 8
            and _positive_integer(cpu.get("model_threads_per_target")) == 1
            and _positive_integer(cpu.get("maximum_total_model_threads")) == 8
            and isinstance(publication.get("attempted"), bool)
            and isinstance(publication.get("allowed"), bool)
            and isinstance(publication.get("pointer_mutation_performed"), bool)
        )

        targeted = _mapping(status.get("targeted"))
        targeted_authenticated = bool(
            _nonnegative_integer(targeted.get("target_count")) == 14
            and _exact_sha256(targeted.get("targeted_status_sha256")) is not None
            and _exact_sha256(targeted.get("launch_manifest_sha256")) is not None
            and _exact_sha256(targeted.get("dataset_sha256")) is not None
            and _exact_sha256(targeted.get("merged_params_sha256")) is not None
            and _positive_integer(targeted.get("strict_full_rows")) is not None
        )
        candidate = _mapping(status.get("candidate"))
        candidate_available = bool(
            _bounded_text(candidate.get("training_run_id"), limit=160)
            and _exact_sha256(candidate.get("generation_report_sha256")) is not None
            and _exact_sha256(candidate.get("dataset_sha256")) is not None
            and _positive_integer(candidate.get("strict_full_rows")) is not None
        )
        candidate_full25_attested = bool(
            candidate_available
            and _positive_integer(candidate.get("full_target_count")) == 25
        )
        quality = _mapping(status.get("quality"))
        quality_available = bool(
            isinstance(quality.get("passed"), bool)
            and _exact_sha256(quality.get("status_sha256")) is not None
        )
        replay = _mapping(status.get("pass24_replay"))
        replay_available = bool(
            replay.get("passed") is True
            and _positive_integer(replay.get("required_model_count")) == 24
            and _exact_sha256(replay.get("status_sha256")) is not None
        )
        activation = _mapping(status.get("activation"))
        activation_available = bool(
            activation.get("schema_version") == "mft-full25-atomic-activation-v1"
            and activation.get("production_quality_passed") is True
            and activation.get("pointer_mutation_performed") is True
            and _exact_sha256(activation.get("pointer_sha256")) is not None
            and _exact_sha256(activation.get("generation_report_sha256")) is not None
            and _exact_sha256(activation.get("quality_gate_sha256")) is not None
            and _exact_sha256(activation.get("pass24_replay_sha256")) is not None
        )
        launch = _mapping(status.get("nsga_launch"))
        launch_seeds = [
            seed
            for seed in (_nonnegative_integer(item) for item in launch.get("seeds") or [])
            if seed is not None
        ]
        launch_available = bool(
            launch.get("schema_version") == "mft-full25-approved-nsga-launch-v1"
            and launch.get("model_lane") == "approved"
            and launch.get("fea_submission_enabled") is False
            and _positive_integer(launch.get("parallel_seed_workers")) == 4
            and launch_seeds == [51000, 51001, 51002, 51003]
            and _positive_integer(launch.get("controller_pid")) is not None
            and _positive_integer(launch.get("child_pid")) is not None
            and _bounded_text(launch.get("runtime_root"), limit=500)
            and _bounded_text(launch.get("manifest"), limit=500)
            and _bounded_text(launch.get("status"), limit=500)
            and _bounded_text(launch.get("state"), limit=500)
            and _bounded_text(launch.get("training_run_id"), limit=160)
            and _exact_sha256(launch.get("command_sha256")) is not None
            and _exact_sha256(launch.get("manifest_sha256")) is not None
            and _exact_sha256(launch.get("status_sha256")) is not None
            and _exact_sha256(launch.get("state_sha256")) is not None
            and _exact_sha256(launch.get("dataset_sha256")) is not None
            and _exact_sha256(launch.get("activation_pointer_sha256")) is not None
            and _exact_sha256(launch.get("activation_evidence_sha256")) is not None
        )

        evidence_errors: list[str] = []
        authenticated_phases = POST_TARGETED_FULL25_PHASES.difference({
            "waiting_targeted_hpo",
            "candidate_failed_closed",
            "activation_committed_nsga_launch_failed_closed",
        })
        if phase in authenticated_phases and not targeted_authenticated:
            evidence_errors.append("invalid targeted-HPO authentication")
        if phase in {
            "candidate_quality_gate",
            "candidate_pass24_replay",
            "candidate_quality_failed_closed",
            "candidate_atomic_activation",
            "approved_nsga_launch",
            "approved_model_active_nsga_running",
        } and not candidate_available:
            evidence_errors.append("invalid full-25 candidate evidence")
        if phase in {
            "candidate_quality_failed_closed",
            "candidate_atomic_activation",
            "approved_nsga_launch",
            "approved_model_active_nsga_running",
        } and not candidate_full25_attested:
            evidence_errors.append("full-25 target inventory is not attested")
        if phase in {
            "candidate_pass24_replay",
            "candidate_quality_failed_closed",
            "candidate_atomic_activation",
            "approved_nsga_launch",
            "approved_model_active_nsga_running",
        } and not quality_available:
            evidence_errors.append("invalid production quality evidence")
        if phase in {
            "candidate_quality_failed_closed",
            "candidate_atomic_activation",
            "approved_nsga_launch",
            "approved_model_active_nsga_running",
        } and not replay_available:
            evidence_errors.append("invalid PASS24 replay evidence")
        if phase in {
            "approved_nsga_launch",
            "approved_model_active_nsga_running",
        } and not activation_available:
            evidence_errors.append("invalid approved activation evidence")
        if phase == "approved_model_active_nsga_running" and not launch_available:
            evidence_errors.append("invalid approved NSGA launch evidence")
        activation_failure_evidence = _mapping(
            status.get("activation_evidence")
        )
        activation_failure_authenticated = bool(
            status.get("activation_committed") is True
            and _exact_sha256(activation_failure_evidence.get("sha256")) is not None
        )
        if (
            phase == "activation_committed_nsga_launch_failed_closed"
            and not activation_failure_authenticated
        ):
            evidence_errors.append("invalid committed activation failure evidence")

        terminal = phase in POST_TARGETED_FULL25_TERMINAL_PHASES
        freshness = cls._freshness(
            status,
            meta,
            stale_after_seconds=(
                None if terminal else STALE_AFTER_SECONDS["post_targeted_full25"]
            ),
        )
        if (
            not terminal
            and meta.get("available")
            and freshness.get("age_seconds") is None
        ):
            freshness["stale"] = True
        valid = base_valid and not evidence_errors
        error_payload = _mapping(status.get("error"))
        error_message = _bounded_text(error_payload.get("message"), limit=500)
        if not base_valid:
            evidence_errors.insert(0, "invalid post-targeted full-25 status contract")
        return {
            "available": valid,
            "phase": phase or "unavailable",
            "terminal": terminal,
            "lane": lane,
            "eligibility": eligibility,
            "watcher_pid": _positive_integer(status.get("watcher_pid")),
            "training_active": phase == "candidate_training",
            "worker_pid": _positive_integer(status.get("worker_pid")),
            "cpu_budget": {
                "target_workers": _positive_integer(cpu.get("target_workers")),
                "model_threads_per_target": _positive_integer(
                    cpu.get("model_threads_per_target")
                ),
                "maximum_total_model_threads": _positive_integer(
                    cpu.get("maximum_total_model_threads")
                ),
            },
            "code": {
                "root": code_root,
                "deployment_commit": deployment_commit,
                "watcher_sha256": _exact_sha256(code.get("watcher_sha256")) or "",
                "continuous_nsga_sha256": (
                    _exact_sha256(code.get("continuous_nsga_sha256")) or ""
                ),
            },
            "targeted": {
                "authenticated": targeted_authenticated,
                "wave": _bounded_text(targeted.get("wave"), limit=160),
                "target_count": _nonnegative_integer(targeted.get("target_count")),
                "strict_full_rows": _nonnegative_integer(
                    targeted.get("strict_full_rows")
                ),
                "dataset_sha256": _exact_sha256(targeted.get("dataset_sha256")) or "",
                "merged_params_sha256": (
                    _exact_sha256(targeted.get("merged_params_sha256")) or ""
                ),
            },
            "candidate": {
                "available": candidate_available,
                "training_run_id": _bounded_text(
                    candidate.get("training_run_id"), limit=160
                ),
                "generation": _bounded_text(candidate.get("generation"), limit=500),
                "generation_report_sha256": (
                    _exact_sha256(candidate.get("generation_report_sha256")) or ""
                ),
                "dataset_sha256": _exact_sha256(candidate.get("dataset_sha256")) or "",
                "strict_full_rows": _nonnegative_integer(
                    candidate.get("strict_full_rows")
                ),
                "full_target_count": _nonnegative_integer(
                    candidate.get("full_target_count")
                ),
            },
            "quality": {
                "available": quality_available,
                "passed": quality.get("passed") if isinstance(quality.get("passed"), bool) else None,
                "reason_count": _nonnegative_integer(
                    quality.get("reason_count", quality.get("failed_target_count"))
                ),
                "status_sha256": _exact_sha256(quality.get("status_sha256")) or "",
            },
            "pass24_replay": {
                "available": replay_available,
                "passed": replay.get("passed") if isinstance(replay.get("passed"), bool) else None,
                "required_model_count": _nonnegative_integer(
                    replay.get("required_model_count")
                ),
                "status_sha256": _exact_sha256(replay.get("status_sha256")) or "",
            },
            "publication": {
                "attempted": publication.get("attempted") is True,
                "allowed": publication.get("allowed") is True,
                "pointer_mutation_performed": (
                    publication.get("pointer_mutation_performed") is True
                ),
                "reason": _bounded_text(publication.get("reason"), limit=240),
            },
            "activation": {
                "available": (
                    activation_available or activation_failure_authenticated
                ),
                "committed": (
                    activation.get("pointer_mutation_performed") is True
                    or activation_failure_authenticated
                ),
                "pointer_sha256": _exact_sha256(activation.get("pointer_sha256")) or "",
                "pointer_path": _bounded_text(
                    activation.get("pointer_path"), limit=500
                ),
                "generation_report_sha256": (
                    _exact_sha256(activation.get("generation_report_sha256")) or ""
                ),
                "evidence_sha256": (
                    _exact_sha256(activation.get("evidence_sha256"))
                    or _exact_sha256(activation_failure_evidence.get("sha256"))
                    or ""
                ),
            },
            "nsga_launch": {
                "available": launch_available,
                "controller_pid": _positive_integer(launch.get("controller_pid")),
                "child_pid": _positive_integer(launch.get("child_pid")),
                "run_id": _bounded_text(launch.get("run_id"), limit=80),
                "model_id": _bounded_text(launch.get("model_id"), limit=180),
                "model_lane": _bounded_text(launch.get("model_lane"), limit=40),
                "training_run_id": _bounded_text(
                    launch.get("training_run_id"), limit=160
                ),
                "runtime_root": _bounded_text(
                    launch.get("runtime_root"), limit=500
                ),
                "manifest": _bounded_text(launch.get("manifest"), limit=500),
                "status": _bounded_text(launch.get("status"), limit=500),
                "state": _bounded_text(launch.get("state"), limit=500),
                "seeds": launch_seeds,
                "parallel_seed_workers": _positive_integer(
                    launch.get("parallel_seed_workers")
                ),
                "fea_submission_enabled": launch.get("fea_submission_enabled") is True,
                "manifest_sha256": _exact_sha256(launch.get("manifest_sha256")) or "",
                "status_sha256": _exact_sha256(launch.get("status_sha256")) or "",
                "state_sha256": _exact_sha256(launch.get("state_sha256")) or "",
                "activation_pointer_sha256": (
                    _exact_sha256(launch.get("activation_pointer_sha256")) or ""
                ),
                "evidence_sha256": _exact_sha256(launch.get("evidence_sha256")) or "",
            },
            "error": error_message,
            "contract_errors": evidence_errors[:8],
            "freshness": freshness,
            "updated_at": status.get("updated_at"),
        }

    @classmethod
    def _isolated_nsga_lane(
        cls,
        status: Mapping[str, Any],
        meta: Mapping[str, Any],
    ) -> dict[str, Any]:
        lane_name = _bounded_text(status.get("lane"), limit=80)
        state = _bounded_text(status.get("state"), limit=40)
        config = _mapping(status.get("config"))
        model = _mapping(status.get("model"))
        eligibility = model.get("eligibility")
        eligibility_contract = _mapping(eligibility)
        monitor = _mapping(status.get("monitor"))
        terminal = _mapping(status.get("terminal"))
        summary = _mapping(terminal.get("summary"))
        seed_base = _nonnegative_integer(config.get("seed_base"))
        restart_count = _positive_integer(config.get("restarts"), maximum=64)
        population = _positive_integer(config.get("population"), maximum=100_000)
        max_generations = _positive_integer(
            config.get("max_generations"), maximum=1_000_000
        )
        workers = _positive_integer(config.get("workers"), maximum=64)
        pid = _positive_integer(status.get("pid"))
        alive = status.get("alive")
        elapsed = _strict_number(status.get("elapsed_seconds"))
        valid = bool(
            meta.get("available")
            and status.get("schema_version") == ISOLATED_NSGA_STATUS_SCHEMA
            and lane_name
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", lane_name)
            and state in {"waiting", "running", "completed", "infeasible", "failed"}
            and pid is not None
            and isinstance(alive, bool)
            and elapsed is not None
            and elapsed >= 0
            and seed_base is not None
            and restart_count is not None
            and population is not None
            and max_generations is not None
            and workers is not None
            and _exact_sha256(model.get("dataset_sha256")) is not None
            and _exact_sha256(model.get("generation_report_sha256")) is not None
            and (
                str(eligibility or "") == "FEA-NOT-APPROVED"
                or (
                    eligibility_contract.get("production_eligible") is False
                    and eligibility_contract.get("fea_submission_approved") is False
                    and eligibility_contract.get("standard_fea_only") is True
                    and eligibility_contract.get("direct_full_fea_allowed") is False
                )
            )
        )
        freshness = cls._freshness(
            status,
            meta,
            stale_after_seconds=STALE_AFTER_SECONDS["isolated_nsga"],
        )
        if meta.get("available") and freshness.get("age_seconds") is None:
            freshness["stale"] = True
        if not valid:
            return {
                "name": lane_name or "isolated-invalid",
                "available": False,
                "stale": bool(meta.get("stale")),
                "state": "invalid",
                "run_id": "",
                "seeds": [],
                "seed_workers": 0,
                "model_id": "",
                "current_model_id": "",
                "model_lane": "isolated",
                "completed_runs": 0,
                "feasible_runs": 0,
                "infeasible_runs": 0,
                "pareto_runs": 0,
                "current_model": {
                    "available": False,
                    "completed_runs": None,
                    "feasible_runs": 0,
                    "infeasible_runs": 0,
                    "pareto_runs": 0,
                    "error": "invalid isolated NSGA live-status contract",
                },
                "lifetime": {
                    "completed_runs": 0,
                    "feasible_runs": 0,
                    "infeasible_runs": 0,
                    "pareto_runs": 0,
                },
                "latest_pareto": {"available": False, "scope": "current_model", "points": []},
                "lifetime_latest_pareto": {"available": False, "scope": "lifetime", "points": []},
                "least_violation": {
                    "best_total_positive_violation": None,
                    "candidate_count": None,
                    "zero_pass_constraints": [],
                },
                "warm_start": {},
                "freshness": freshness,
                "updated_at": status.get("updated_at"),
                "error": "invalid isolated NSGA live-status contract",
            }

        completed_runs = (
            _nonnegative_integer(summary.get("completed_restarts")) or 0
        )
        feasible_restarts = _nonnegative_integer(
            summary.get("feasible_restarts")
        )
        if feasible_restarts is None:
            feasible_restarts = (
                completed_runs
                if terminal.get("outcome") == "feasible_complete"
                else 0
            )
        feasible_runs = feasible_restarts
        infeasible_runs = max(0, completed_runs - feasible_runs)
        pareto_points = _nonnegative_integer(summary.get("pareto_points")) or 0
        terminal_available = terminal.get("available") is True
        seeds = list(range(seed_base, seed_base + restart_count))
        training_run_id = _bounded_text(model.get("training_run_id"), limit=160)
        model_id = (
            f"isolated:{training_run_id}:"
            f"{str(model.get('dataset_sha256'))[:16]}:"
            f"{str(model.get('generation_report_sha256'))[:16]}"
        )
        pareto = cls._nsga_pareto(
            terminal.get("pareto"), scope="current_model"
        )
        return {
            "name": lane_name,
            "available": True,
            "stale": bool(freshness.get("stale")),
            "source_type": "isolated_transition_audit",
            "state": state,
            "run_id": _bounded_text(
                status.get("run_id"), limit=80
            ) or f"seed-{seed_base}",
            "seeds": seeds,
            "seed_workers": (
                workers if alive and state in {"waiting", "running"} else 0
            ),
            "model_id": model_id,
            "current_model_id": model_id,
            "model_lane": "isolated",
            "completed_runs": completed_runs,
            "feasible_runs": feasible_runs,
            "infeasible_runs": infeasible_runs,
            "pareto_runs": feasible_runs,
            "current_model": {
                "available": terminal_available,
                "completed_runs": completed_runs if terminal_available else None,
                "feasible_runs": feasible_runs,
                "infeasible_runs": infeasible_runs,
                "pareto_runs": feasible_runs,
                "error": "",
            },
            "lifetime": {
                "completed_runs": completed_runs,
                "feasible_runs": feasible_runs,
                "infeasible_runs": infeasible_runs,
                "pareto_runs": feasible_runs,
            },
            "latest_pareto": pareto,
            "lifetime_latest_pareto": {
                **pareto,
                "scope": "lifetime",
            } if pareto.get("available") else {
                "available": False,
                "scope": "lifetime",
                "points": [],
            },
            "next_seed_base": seed_base + restart_count,
            "model_switch_policy": "pinned isolated generation; no hot swap",
            "fea_submission_enabled": False,
            "least_violation": {
                "best_total_positive_violation": _number(
                    summary.get("best_violation")
                ),
                "candidate_count": pareto_points or None,
                "zero_pass_constraints": _text_items(
                    summary.get("zero_pass_constraints"), limit=12
                ),
            },
            "warm_start": {
                "artifact_kind": _bounded_text(
                    status.get("warm_start_kind"), limit=80
                ),
                "reevaluation_required": True,
            },
            "elapsed_seconds": elapsed,
            "config": {
                "seed_base": seed_base,
                "restarts": restart_count,
                "population": population,
                "max_generations": max_generations,
                "workers": workers,
            },
            "eligibility": "FEA-NOT-APPROVED",
            "handoff_state": _bounded_text(
                monitor.get("bridge_state"), limit=40
            ),
            "authenticated_handoff_complete": (
                terminal.get("authenticated_handoff_complete") is True
            ),
            "terminal_outcome": _bounded_text(
                terminal.get("outcome"), limit=80
            ),
            "freshness": freshness,
            "updated_at": status.get("updated_at"),
        }

    @classmethod
    def _nsga_lane(
        cls,
        name: str,
        status: Mapping[str, Any],
        meta: Mapping[str, Any],
    ) -> dict[str, Any]:
        active_run = _mapping(status.get("active_run"))
        seeds = [
            seed for seed in (_integer(item) for item in active_run.get("seeds") or [])
            if seed is not None
        ]
        outcomes = _mapping(status.get("completed_outcomes"))
        current_outcomes_raw = status.get("current_model_completed_outcomes")
        current_outcomes = _mapping(current_outcomes_raw)
        current_model_id = _bounded_text(
            status.get("current_model_id") or active_run.get("model_id"),
            limit=160,
        )
        current_completed = _nonnegative_integer(
            status.get("current_model_completed_run_count")
        )
        allowed_current_outcomes = {
            "feasible_complete",
            "infeasible_complete",
            "failed",
        }
        current_outcomes_valid = (
            isinstance(current_outcomes_raw, Mapping)
            and set(current_outcomes).issubset(allowed_current_outcomes)
            and all(
                _nonnegative_integer(value) is not None
                for value in current_outcomes.values()
            )
        )
        current_contract_available = bool(
            meta.get("available")
            and current_model_id
            and current_completed is not None
            and current_outcomes_valid
            and sum(current_outcomes.values()) == current_completed
        )
        current_contract_error = ""
        if isinstance(current_outcomes_raw, Mapping) and not current_contract_available:
            current_contract_error = "invalid current-model NSGA result contract"
        infeasibility = _mapping(
            status.get("current_model_last_infeasibility")
            if "current_model_last_infeasibility" in status
            else {}
        )
        archive = _mapping(infeasibility.get("least_violation_archive"))
        warm_start = _mapping(active_run.get("warm_start"))
        feasible_runs = _integer(outcomes.get("feasible_complete"))
        infeasible_runs = _integer(outcomes.get("infeasible_complete"))
        current_feasible = (
            _nonnegative_integer(current_outcomes.get("feasible_complete")) or 0
        )
        current_infeasible = (
            _nonnegative_integer(current_outcomes.get("infeasible_complete")) or 0
        )
        return {
            "name": name,
            "available": bool(meta.get("available")),
            "stale": bool(meta.get("stale")),
            "state": str(status.get("state") or "unavailable"),
            "run_id": str(active_run.get("run_id") or ""),
            "seeds": seeds,
            "seed_workers": len(seeds),
            "model_id": str(active_run.get("model_id") or ""),
            "current_model_id": current_model_id,
            "model_lane": str(active_run.get("model_lane") or ""),
            "completed_runs": _integer(status.get("completed_run_count")) or 0,
            "feasible_runs": feasible_runs if feasible_runs is not None else 0,
            "infeasible_runs": infeasible_runs if infeasible_runs is not None else 0,
            # Every feasible completed run publishes an authenticated Pareto
            # front.  Keep the producer's outcome terminology as well.
            "pareto_runs": feasible_runs if feasible_runs is not None else 0,
            "current_model": {
                "available": current_contract_available,
                "completed_runs": current_completed,
                "feasible_runs": current_feasible,
                "infeasible_runs": current_infeasible,
                "pareto_runs": current_feasible,
                "error": current_contract_error,
            },
            "lifetime": {
                "completed_runs": _integer(status.get("completed_run_count")) or 0,
                "feasible_runs": feasible_runs if feasible_runs is not None else 0,
                "infeasible_runs": infeasible_runs if infeasible_runs is not None else 0,
                "pareto_runs": feasible_runs if feasible_runs is not None else 0,
            },
            "latest_pareto": cls._nsga_pareto(
                status.get("latest_feasible_pareto"), scope="current_model"
            ),
            "lifetime_latest_pareto": cls._nsga_pareto(
                status.get("latest_feasible_pareto_lifetime"), scope="lifetime"
            ),
            "next_seed_base": _integer(status.get("next_seed_base")),
            "model_switch_policy": str(status.get("model_switch_policy") or ""),
            "fea_submission_enabled": status.get("fea_submission_enabled") is True,
            "least_violation": {
                "best_total_positive_violation": _number(
                    archive.get("best_total_positive_violation")
                ),
                "candidate_count": _integer(archive.get("candidate_count")),
                "zero_pass_constraints": _text_items(
                    infeasibility.get("seed_invariant_zero_pass_constraints"),
                    limit=12,
                ),
            },
            "warm_start": {
                "source_run_id": str(warm_start.get("source_run_id") or ""),
                "artifact_kind": str(warm_start.get("artifact_kind") or ""),
                "provenance_match": warm_start.get("provenance_match"),
                "reevaluation_required": warm_start.get("reevaluation_required"),
            },
            "freshness": cls._freshness(
                status,
                meta,
                stale_after_seconds=STALE_AFTER_SECONDS["nsga"],
            ),
            "updated_at": status.get("updated_at"),
        }

    @classmethod
    def _approved_nsga_lane(
        cls,
        name: str,
        status: Mapping[str, Any],
        meta: Mapping[str, Any],
        manifest: Mapping[str, Any],
        manifest_meta: Mapping[str, Any],
        state: Mapping[str, Any],
        state_meta: Mapping[str, Any],
        post_targeted: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Expose only an authenticated approved-model full-25 NSGA lane."""

        active_run = _mapping(status.get("active_run"))
        state_active_run = _mapping(state.get("active_run"))
        parallelism = _mapping(status.get("parallelism"))
        seeds = [
            seed
            for seed in (
                _nonnegative_integer(item) for item in active_run.get("seeds") or []
            )
            if seed is not None
        ]
        manifest_seed_start = _nonnegative_integer(manifest.get("seed_start"))
        consecutive_seed_batch = bool(
            manifest_seed_start is not None
            and len(seeds) == 4
            and seeds == list(range(seeds[0], seeds[0] + 4))
            and seeds[0] >= manifest_seed_start
            and (seeds[0] - manifest_seed_start) % 4 == 0
        )
        post_launch = _mapping(post_targeted.get("nsga_launch"))
        post_activation = _mapping(post_targeted.get("activation"))

        def normalized_path(value: object) -> str:
            return str(value or "").replace("\\", "/").rstrip("/").lower()

        relative_status = normalized_path(meta.get("relative_path"))
        launch_status = normalized_path(post_launch.get("status"))
        launch_manifest = normalized_path(post_launch.get("manifest"))
        launch_state = normalized_path(post_launch.get("state"))
        launch_root = normalized_path(post_launch.get("runtime_root"))
        pointer_path = normalized_path(post_activation.get("pointer_path"))
        parent_bound = bool(
            post_targeted.get("available") is True
            and post_launch.get("available") is True
            and post_activation.get("available") is True
            and relative_status
            and launch_status.endswith("/" + relative_status)
            and launch_manifest == launch_status.rsplit("/", 1)[0] + "/manifest.json"
            and launch_state == launch_status.rsplit("/", 1)[0] + "/state.json"
            and launch_root == launch_status.rsplit("/", 1)[0]
            and normalized_path(manifest.get("code_root"))
            == normalized_path(_mapping(post_targeted.get("code")).get("root"))
            and normalized_path(manifest.get("approved_registry"))
            == pointer_path.rsplit("/", 1)[0]
            and post_launch.get("activation_pointer_sha256")
            == post_activation.get("pointer_sha256")
            and _positive_integer(status.get("controller_pid"))
            == _positive_integer(post_launch.get("controller_pid"))
            and str(active_run.get("model_id") or "").startswith(
                f"approved:{post_launch.get('training_run_id')}:"
            )
        )
        contract_valid = bool(
            meta.get("available")
            and manifest_meta.get("available")
            and state_meta.get("available")
            and status.get("schema_version") == APPROVED_NSGA_STATUS_SCHEMA
            and manifest.get("schema_version") == APPROVED_NSGA_STATUS_SCHEMA
            and state.get("schema_version") == APPROVED_NSGA_STATUS_SCHEMA
            and status.get("require_approved") is True
            and status.get("approved_pointer_present") is True
            and status.get("approved_source_error") is None
            and status.get("fea_submission_enabled") is False
            and _positive_integer(status.get("controller_pid")) is not None
            and active_run.get("model_lane") == "approved"
            and str(active_run.get("model_id") or "").startswith("approved:")
            and _positive_integer(active_run.get("pid")) is not None
            and state_active_run.get("run_id") == active_run.get("run_id")
            and state_active_run.get("model_id") == active_run.get("model_id")
            and consecutive_seed_batch
            and _positive_integer(parallelism.get("parallel_seed_workers")) == 4
            and manifest.get("require_approved") is True
            and _bounded_text(manifest.get("approved_registry"), limit=500)
            and _positive_integer(manifest.get("restarts")) == 4
            and _positive_integer(manifest.get("workers")) == 4
            and _positive_integer(manifest.get("population")) == 120
            and _positive_integer(manifest.get("max_generations")) == 600
            and manifest_seed_start == 51000
            and parent_bound
        )
        normalized_meta = dict(meta)
        normalized_meta["available"] = contract_valid
        lane = cls._nsga_lane(name, status, normalized_meta)
        freshness = cls._freshness(
            status,
            meta,
            stale_after_seconds=STALE_AFTER_SECONDS["approved_nsga"],
        )
        if meta.get("available") and freshness.get("age_seconds") is None:
            freshness["stale"] = True
        lane.update({
            "available": contract_valid,
            "stale": bool(freshness.get("stale")),
            "source_type": "post_targeted_full25_approved",
            "seed_workers": (
                4
                if contract_valid
                and not freshness.get("stale")
                and _positive_integer(active_run.get("pid")) is not None
                else 0
            ),
            "fea_submission_enabled": False,
            "approval": {
                "require_approved": status.get("require_approved") is True,
                "approved_pointer_present": (
                    status.get("approved_pointer_present") is True
                ),
                "manifest_bound": bool(manifest_meta.get("available")),
                "state_bound": bool(state_meta.get("available")),
                "parent_activation_bound": parent_bound,
                "approved_registry": _bounded_text(
                    manifest.get("approved_registry"), limit=500
                ),
            },
            "freshness": freshness,
            "error": (
                "" if contract_valid else "invalid approved full-25 NSGA contract"
            ),
        })
        return lane

    @staticmethod
    def _nsga_pareto(value: object, *, scope: str) -> dict[str, Any]:
        raw = _mapping(value)
        if not raw:
            return {"available": False, "scope": scope, "points": []}
        if raw.get("available") is not True:
            return {
                "available": False,
                "scope": scope,
                "run_id": _bounded_text(raw.get("run_id"), limit=32),
                "model_id": _bounded_text(raw.get("model_id"), limit=160),
                "error": _bounded_text(raw.get("error"), limit=240),
                "points": [],
            }
        if (
            set(raw) != NSGA_PARETO_KEYS
            or raw.get("schema_version") != "mft-nsga-pareto-status-v1"
            or raw.get("scope") != scope
            or raw.get("objective_source") != "surrogate_prediction"
            or raw.get("fea_verified") is not False
            or raw.get("fea_submission_approved") is not False
            or not isinstance(raw.get("production_eligible"), bool)
            or raw.get("objective_names") != ["volume_L", "total_loss_W"]
            or raw.get("objective_units")
            != {"volume_L": "L", "total_loss_W": "W"}
            or raw.get("limit") != MAX_NSGA_PARETO_POINTS
        ):
            return {
                "available": False,
                "scope": scope,
                "error": "invalid NSGA Pareto provenance",
                "points": [],
            }
        run_id = str(raw.get("run_id") or "")
        model_id = str(raw.get("model_id") or "")
        model_lane = str(raw.get("model_lane") or "")
        finished_at = str(raw.get("finished_at") or "")
        point_count = _positive_integer(
            raw.get("point_count"), maximum=MAX_NSGA_PARETO_ROWS
        )
        raw_seeds = raw.get("seeds")
        hashes = {
            name: _exact_sha256(raw.get(name))
            for name in (
                "optimization_manifest_sha256",
                "pareto_front_sha256",
                "run_manifest_sha256",
                "model_source_sha256",
            )
        }
        if (
            _NSGA_RUN_RE.fullmatch(run_id) is None
            or not model_id
            or len(model_id) > 160
            or not model_lane
            or len(model_lane) > 32
            or not finished_at
            or len(finished_at) > 64
            or point_count is None
            or not isinstance(raw_seeds, list)
            or not 1 <= len(raw_seeds) <= 16
            or any(
                _nonnegative_integer(seed) is None for seed in raw_seeds
            )
            or any(value is None for value in hashes.values())
        ):
            return {
                "available": False,
                "scope": scope,
                "error": "invalid NSGA Pareto identity contract",
                "points": [],
            }
        points: list[dict[str, Any]] = []
        raw_points_value = raw.get("points")
        expected_published = min(point_count, MAX_NSGA_PARETO_POINTS)
        if (
            not isinstance(raw_points_value, list)
            or len(raw_points_value) != expected_published
            or raw.get("truncated") is not (
                point_count > MAX_NSGA_PARETO_POINTS
            )
        ):
            return {
                "available": False,
                "scope": scope,
                "error": "invalid NSGA Pareto point contract",
                "points": [],
            }
        for expected_index, raw_point_value in enumerate(raw_points_value):
            if not isinstance(raw_point_value, Mapping):
                return {
                    "available": False,
                    "scope": scope,
                    "error": "invalid NSGA Pareto point contract",
                    "points": [],
                }
            raw_point = dict(raw_point_value)
            volume = _strict_number(raw_point.get("volume_L"))
            loss = _strict_number(raw_point.get("total_loss_W"))
            candidate_index = _nonnegative_integer(
                raw_point.get("candidate_index")
            )
            row_number = _nonnegative_integer(raw_point.get("row_number"))
            if (
                set(raw_point) != NSGA_PARETO_POINT_KEYS
                or volume is None
                or loss is None
                or candidate_index != expected_index
                or row_number != expected_index + 1
            ):
                return {
                    "available": False,
                    "scope": scope,
                    "error": "invalid NSGA Pareto point contract",
                    "points": [],
                }
            raw_design = raw_point.get("design")
            if not isinstance(raw_design, Mapping) or set(raw_design) != NSGA_DESIGN_FIELDS:
                return {
                    "available": False,
                    "scope": scope,
                    "error": "invalid NSGA Pareto design contract",
                    "points": [],
                }
            design: dict[str, float] = {}
            for key in NSGA_DESIGN_FIELDS:
                number = _strict_number(raw_design.get(key))
                if number is None:
                    return {
                        "available": False,
                        "scope": scope,
                        "error": "invalid NSGA Pareto design contract",
                        "points": [],
                    }
                design[key] = number
            points.append({
                "candidate_index": candidate_index,
                "row_number": row_number,
                "volume_L": volume,
                "total_loss_W": loss,
                "design": design,
            })
        return {
            "available": True,
            "scope": scope,
            "run_id": run_id,
            "model_id": model_id,
            "model_lane": model_lane,
            "seeds": list(raw_seeds),
            "finished_at": finished_at,
            "point_count": point_count,
            "points": points,
            "limit": MAX_NSGA_PARETO_POINTS,
            "truncated": point_count > len(points),
            "objective_source": "surrogate_prediction",
            "fea_verified": False,
            "production_eligible": raw.get("production_eligible"),
            "fea_submission_approved": False,
            **hashes,
        }

    @staticmethod
    def _active_task_inventory(
        statuses: Iterable[Mapping[str, Any]], *, include_global: bool = True,
        exclude_name_prefixes: tuple[str, ...] = (),
    ) -> dict[int | str, str]:
        tasks: dict[int | str, str] = {}
        for status in statuses:
            inventory = _mapping(status.get("global_lane_inventory"))
            rows = _items(status.get("active_tasks"))
            if include_global:
                rows = [*_items(inventory.get("tasks")), *rows]
            for row in rows:
                task_name = str(row.get("task_name") or row.get("name") or "")
                if task_name.startswith(exclude_name_prefixes):
                    continue
                task_id = _integer(row.get("task_id"))
                key: int | str = task_id if task_id is not None else task_name
                task_status = str(row.get("status") or row.get("task_status") or "").lower()
                if key != "" and task_status in {"queued", "attaching", "running"}:
                    tasks[key] = task_status
        return tasks

    @staticmethod
    def _standard_results(states: Iterable[Mapping[str, Any]]) -> tuple[int, int, int]:
        passed: set[str] = set()
        failed: set[str] = set()
        valid: set[str] = set()
        terminal_task_states = {
            "cancelled", "completed", "failed", "timed_out", "timeout"
        }
        for state in states:
            for candidate in _items(state.get("candidates")):
                identity = str(
                    candidate.get("candidate_digest")
                    or candidate.get("task_id")
                    or candidate.get("task_name")
                    or ""
                )
                if not identity:
                    continue
                task_status = str(candidate.get("task_status") or "").lower()
                if task_status not in terminal_task_states:
                    continue
                authenticated_result = bool(
                    candidate.get("result_state") == "valid"
                    and candidate.get("result_contract_valid") is True
                    and candidate.get("candidate_identity_matches") is True
                )
                if authenticated_result:
                    valid.add(identity)
                # Durable authenticated solver evidence outranks a later AEDT
                # teardown exit or collector failure.  Missing any exact gate
                # remains fail-closed, so an actual solver failure is never
                # converted into PASS merely because its task is terminal.
                if (
                    authenticated_result
                    and candidate.get("standard_fea_spec_pass") is True
                ):
                    passed.add(identity)
                else:
                    failed.add(identity)
        failed.difference_update(passed)
        return len(passed), len(failed), len(valid)

    @staticmethod
    def _full_results(states: Iterable[Mapping[str, Any]]) -> tuple[int, int]:
        passed: set[str] = set()
        failed: set[str] = set()
        for state in states:
            for candidate in _items(state.get("candidates")):
                identity = str(
                    candidate.get("candidate_digest")
                    or candidate.get("task_id")
                    or candidate.get("task_name")
                    or ""
                )
                if not identity or str(candidate.get("collection_state") or "").lower() != "collector_succeeded":
                    continue
                if candidate.get("full_model_spec_pass") is True:
                    passed.add(identity)
                elif candidate.get("full_model_spec_pass") is False:
                    failed.add(identity)
        failed.difference_update(passed)
        return len(passed), len(failed)

    @staticmethod
    def _surrogate_hpo(status: Mapping[str, Any]) -> dict[str, Any]:
        detail = _mapping(status.get("active_wave_detail"))
        contract = _mapping(status.get("hpo_contract"))
        jobs = _items(detail.get("jobs"))
        plan = _items(contract.get("target_plan"))
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for job in jobs:
            target = str(job.get("target") or "").strip()
            if not target or target in seen:
                continue
            seen.add(target)
            normalized.append(
                {
                    "target": target,
                    "family": str(job.get("family") or ""),
                    "status": str(job.get("status") or "unknown"),
                    "alive": job.get("alive"),
                    "result_ready": job.get("result_ready") is True,
                    "model_threads": _integer(job.get("model_threads")),
                    "trials": _integer(job.get("trials")),
                    "cpu_seconds": _number(job.get("cpu_seconds")),
                    "source": str(job.get("source") or ""),
                }
            )
            if len(normalized) >= MAX_HPO_TARGETS:
                break
        if len(normalized) < MAX_HPO_TARGETS:
            for item in plan:
                target = str(item.get("target") or "").strip()
                if not target or target in seen:
                    continue
                seen.add(target)
                normalized.append(
                    {
                        "target": target,
                        "family": "",
                        "status": "planned",
                        "alive": None,
                        "result_ready": False,
                        "model_threads": _integer(item.get("model_threads")),
                        "trials": _integer(contract.get("trials_per_target")),
                        "cpu_seconds": None,
                        "source": str(item.get("source") or ""),
                    }
                )
                if len(normalized) >= MAX_HPO_TARGETS:
                    break
        raw_expected_targets = contract.get("targets")
        expected_total = (
            len(raw_expected_targets)
            if isinstance(raw_expected_targets, (list, tuple))
            else 0
        )
        total = max(len(jobs), len(normalized), expected_total)
        return {
            "ready": sum(1 for job in normalized if job["result_ready"]),
            "running": sum(1 for job in normalized if job["alive"] is True),
            "total": total,
            "targets": normalized,
            "processes": _integer(contract.get("actual_hpo_processes")),
            "thread_budget": _integer(contract.get("total_hpo_thread_budget"))
            or _integer(detail.get("total_model_thread_budget")),
            "trials_per_target": _integer(contract.get("trials_per_target")),
            "total_trials": _integer(detail.get("total_trials")),
            "limit": MAX_HPO_TARGETS,
            "truncated": len(jobs) > MAX_HPO_TARGETS
            or expected_total > MAX_HPO_TARGETS,
        }

    @staticmethod
    def _completed_hpo_results(status: Mapping[str, Any]) -> dict[str, Any]:
        raw = _mapping(status.get("last_completed_hpo_results"))
        raw_error = _mapping(status.get("last_completed_hpo_results_error"))
        error_message = _bounded_text(raw_error.get("message"), limit=500)
        error_wave = _bounded_text(raw_error.get("wave"), limit=80)

        def invalid(message: str) -> dict[str, Any]:
            return {
                "available": False,
                "targets": [],
                "evidence_error": message,
                "error_wave": error_wave,
            }

        if not raw:
            return invalid(error_message)
        schema_version = raw.get("schema_version")
        if (
            set(raw) != COMPLETED_HPO_TOP_KEYS
            or isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version not in COMPLETED_HPO_SCHEMA_VERSIONS
            or raw.get("status") != "completed"
            or raw.get("evidence_authentication") != "complete"
        ):
            return invalid("invalid completed HPO result contract")
        evidence_sha = str(raw.get("evidence_sha256") or "").lower()
        unsigned = {
            key: value for key, value in raw.items() if key != "evidence_sha256"
        }
        try:
            canonical = json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            full_canonical = json.dumps(
                raw,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            canonical = b""
            full_canonical = b""
        if (
            _exact_sha256(evidence_sha) is None
            or hashlib.sha256(canonical).hexdigest() != evidence_sha
            or not full_canonical
            or len(full_canonical) > MAX_COMPLETED_HPO_STATUS_BYTES
        ):
            return invalid("completed HPO result fingerprint mismatch")
        wave = str(raw.get("wave") or "")
        phase = str(raw.get("result_phase") or "")
        completed_at = str(raw.get("completed_at") or "")
        generation = _exact_sha256(raw.get("dataset_generation"))
        dataset_sha = _exact_sha256(raw.get("dataset_sha256"))
        strict_rows = _positive_integer(raw.get("strict_full_rows"))
        target_count = _positive_integer(
            raw.get("target_count"), maximum=MAX_COMPLETED_HPO_TARGETS
        )
        raw_targets = raw.get("targets")
        outcome = raw.get("wave_outcome")
        if (
            _HPO_WAVE_RE.fullmatch(wave) is None
            or generation is None
            or wave != f"wave-{generation[:16]}"
            or dataset_sha is None
            or phase not in {"candidate_promoted", "candidate_rejected"}
            or not completed_at
            or len(completed_at) > 64
            or strict_rows is None
            or target_count is None
            or not isinstance(raw_targets, list)
            or len(raw_targets) != target_count
            or not _exact_sha_mapping(
                raw.get("common_authentication"),
                COMPLETED_HPO_COMMON_AUTH_KEYS,
            )
            or not isinstance(outcome, Mapping)
            or set(outcome)
            != {
                "candidate_training_run_id",
                "promoted",
                "quality_status_sha256",
            }
            or not isinstance(outcome.get("promoted"), bool)
            or outcome.get("promoted") is not (phase == "candidate_promoted")
            or _HPO_RUN_RE.fullmatch(
                str(outcome.get("candidate_training_run_id") or "")
            )
            is None
            or _exact_sha256(outcome.get("quality_status_sha256")) is None
        ):
            return invalid("invalid completed HPO result contract")
        targets: list[dict[str, Any]] = []
        seen_targets: set[str] = set()
        for raw_item in raw_targets:
            if not isinstance(raw_item, Mapping):
                return invalid("invalid completed HPO target contract")
            item = dict(raw_item)
            target = str(item.get("target") or "")
            target_keys = (
                COMPLETED_HPO_TARGET_KEYS_V2
                if schema_version == 2
                else COMPLETED_HPO_TARGET_KEYS_V1
            )
            if schema_version == 2:
                cv_objective = _strict_number(item.get("cv_objective_value"))
                objective = _validated_hpo_objective_contract(
                    target, item.get("objective_contract")
                )
            else:
                cv_objective = _strict_number(item.get("cv_mse_transformed"))
                objective = {
                    "name": "cv_mse_transformed",
                    "summary": "legacy schema 1; transformed-space CV MSE",
                    "contract": None,
                }
            eligible_rows = _positive_integer(item.get("eligible_rows"))
            target_rows = _positive_integer(item.get("target_rows"))
            train_rows = _positive_integer(item.get("hpo_train_rows"))
            trials = _positive_integer(item.get("trials"), maximum=100_000)
            model_threads = _positive_integer(
                item.get("model_threads"), maximum=64
            )
            best_params = _validated_hpo_params(item.get("best_params"))
            authentication = item.get("authentication")
            if (
                set(item) != target_keys
                or _HPO_TARGET_RE.fullmatch(target) is None
                or target in seen_targets
                or item.get("family") != "lightgbm"
                or cv_objective is None
                or cv_objective < 0
                or objective is None
                or eligible_rows is None
                or target_rows is None
                or eligible_rows != target_rows
                or target_rows > strict_rows
                or train_rows is None
                or train_rows >= target_rows
                or trials is None
                or model_threads is None
                or best_params is None
                or not _exact_sha_mapping(
                    authentication, COMPLETED_HPO_TARGET_AUTH_KEYS
                )
            ):
                return invalid("invalid completed HPO target contract")
            raw_best_params = dict(item["best_params"])
            params_sha = hashlib.sha256(
                json.dumps(
                    raw_best_params,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()
            if authentication.get("tuned_override_sha256") != params_sha:
                return invalid("invalid completed HPO target contract")
            seen_targets.add(target)
            normalized_target = {
                "target": target,
                "family": "lightgbm",
                "cv_objective_value": cv_objective,
                "objective_name": objective["name"],
                "objective_contract_summary": objective["summary"],
                "objective_contract": objective["contract"],
                "eligible_rows": eligible_rows,
                "target_rows": target_rows,
                "hpo_train_rows": train_rows,
                "trials": trials,
                "model_threads": model_threads,
                "best_params": best_params,
            }
            if schema_version == 1:
                normalized_target["cv_mse_transformed"] = cv_objective
            targets.append(normalized_target)
        return {
            "available": True,
            "schema_version": schema_version,
            "wave": wave,
            "result_phase": phase,
            "completed_at": completed_at,
            "dataset_generation": generation,
            "dataset_sha256": dataset_sha,
            "strict_rows": strict_rows,
            "target_count": target_count,
            "targets": targets,
            "candidate_training_run_id": str(
                outcome.get("candidate_training_run_id")
            ),
            "promoted": outcome.get("promoted"),
            "evidence_sha256": evidence_sha,
            "evidence_authentication": "complete",
            "evidence_error": error_message,
            "error_wave": error_wave,
        }

    @staticmethod
    def _surrogate_decision(state: Mapping[str, Any]) -> dict[str, Any]:
        result = _mapping(state.get("last_result"))
        comparison = _mapping(result.get("comparison"))
        gate = _mapping(comparison.get("aggregate_temperature_safety_gate"))
        paired = _mapping(comparison.get("paired_evidence"))
        if not result:
            return {
                "available": False,
                "phase": str(state.get("last_result_phase") or ""),
            }
        promoted = result.get("promoted") is True
        metrics: list[dict[str, Any]] = []
        for name in (
            "aggregate_loss_ratio",
            "worst_target_loss_ratio",
            "maximum_aggregate_loss_ratio",
            "maximum_target_loss_ratio",
        ):
            value = _number(comparison.get(name))
            if value is None:
                value = _number(paired.get(name))
            if value is not None:
                metrics.append({"name": name, "value": value})
        if gate:
            metrics.append(
                {
                    "name": "aggregate_temperature_gate_passed",
                    "value": gate.get("passed") is True,
                }
            )
            metrics.append(
                {
                    "name": "quality_blocked_target_count",
                    "value": len(
                        _text_items(gate.get("quality_blocked_targets"), limit=20)
                    ),
                }
            )
        return {
            "available": True,
            "phase": str(state.get("last_result_phase") or ""),
            "outcome": "promoted" if promoted else "rejected",
            "promoted": promoted,
            "candidate_model_id": str(
                result.get("candidate_training_run_id") or ""
            ),
            "evaluated_at": result.get("evaluated_at")
            or state.get("last_finished_at"),
            "comparison_method": str(comparison.get("method") or ""),
            "comparison_passed": comparison.get("passed"),
            "reasons": _text_items(comparison.get("reasons"), limit=12),
            "blocked_targets": _text_items(
                gate.get("quality_blocked_targets"), limit=20
            ),
            "metrics": metrics[:8],
        }

    @staticmethod
    def _checkpoint_result(value: object) -> dict[str, Any]:
        result = _mapping(value)
        if not result:
            return {"available": False, "target_summaries": [], "reasons": []}

        def invalid(message: str) -> dict[str, Any]:
            return {
                "available": False,
                "error": message,
                "evidence_error": message,
                "target_summaries": [],
                "reasons": [],
            }

        if result.get("schema_version") != 1:
            return invalid("unsupported checkpoint result schema")
        status = _bounded_text(result.get("status"), limit=80)
        kind = _bounded_text(result.get("kind"), limit=80)
        evidence = _bounded_text(
            result.get("evidence_authentication"), limit=80
        )
        error = _mapping(result.get("error"))
        evidence_error = (
            f"{_bounded_text(error.get('type'), limit=80)}: "
            f"{_bounded_text(error.get('message'), limit=500)}"
        ).strip(": ") if error else ""
        if evidence == "failed":
            if (
                status == "evidence_error"
                and kind == "evidence_error"
                and result.get("quality_passed") is None
                and evidence_error
            ):
                failed = invalid(evidence_error)
                failed.update({
                    "status": status,
                    "kind": kind,
                    "evidence_authentication": evidence,
                })
                return failed
            return invalid("invalid checkpoint result contract")
        threshold = _nonnegative_integer(result.get("threshold"))
        actual_rows = _nonnegative_integer(
            result.get("actual_strict_full_rows")
        )
        reason_count = _nonnegative_integer(result.get("reason_count"))
        target_count = _nonnegative_integer(result.get("target_count"))
        failed_target_count = _nonnegative_integer(
            result.get("failed_target_count")
        )
        raw_reasons = result.get("reasons")
        raw_targets = result.get("target_summaries")
        completed_at = _bounded_text(result.get("completed_at"), limit=80)
        if (
            evidence not in ({"complete"} | CHECKPOINT_PARTIAL_EVIDENCE)
            or status not in {
                "completed",
                "failed",
                "promotion_committed_state_recovery_required",
            }
            or not kind
            or threshold is None
            or actual_rows is None
            or reason_count is None
            or target_count is None
            or failed_target_count is None
            or failed_target_count > target_count
            or not completed_at
            or not isinstance(raw_reasons, list)
            or len(raw_reasons) > MAX_CHECKPOINT_REASONS
            or reason_count < len(raw_reasons)
            or not isinstance(raw_targets, list)
            or len(raw_targets) > MAX_CHECKPOINT_TARGETS
            or target_count < len(raw_targets)
            or (reason_count > len(raw_reasons))
            is not (result.get("reasons_truncated") is True)
            or (target_count > len(raw_targets))
            is not (result.get("target_summaries_truncated") is True)
        ):
            return invalid("invalid checkpoint result contract")
        quality_passed = result.get("quality_passed")
        if evidence == "complete":
            quality_contract = {
                "metrics_only": None,
                "accepted_generation": True,
                "quality_rejected": False,
            }
            if (
                kind not in CHECKPOINT_COMPLETE_KINDS
                or quality_passed is not quality_contract[kind]
                or (kind == "metrics_only" and status != "completed")
                or (kind == "quality_rejected" and status != "failed")
                or (
                    kind == "accepted_generation"
                    and status not in {
                        "completed",
                        "promotion_committed_state_recovery_required",
                    }
                )
            ):
                return invalid("invalid checkpoint result contract")
        elif (
            status != "failed"
            or quality_passed is not None
            or target_count != 0
            or failed_target_count != 0
        ):
            return invalid("invalid checkpoint result contract")
        targets: list[dict[str, Any]] = []
        seen_targets: set[str] = set()
        for raw_target_value in raw_targets:
            if not isinstance(raw_target_value, Mapping):
                return invalid("invalid checkpoint target contract")
            raw_target = dict(raw_target_value)
            target = _bounded_text(raw_target.get("target"), limit=160)
            if not target or target in seen_targets:
                return invalid("invalid checkpoint target contract")
            seen_targets.add(target)
            raw_metrics = raw_target.get("metrics")
            if (
                not isinstance(raw_metrics, Mapping)
                or not set(raw_metrics).issubset(CHECKPOINT_METRIC_KEYS)
            ):
                return invalid("invalid checkpoint target contract")
            metrics = {
                name: value_number
                for name in CHECKPOINT_METRIC_KEYS
                if (value_number := _strict_number(
                    raw_metrics.get(name)
                )) is not None
            }
            if len(metrics) != len(raw_metrics):
                return invalid("invalid checkpoint target contract")
            target_reasons = raw_target.get("reasons", [])
            target_reason_count = raw_target.get("reason_count", 0)
            if (
                not isinstance(target_reasons, list)
                or len(target_reasons) > 4
                or _nonnegative_integer(target_reason_count) is None
                or target_reason_count < len(target_reasons)
                or (target_reason_count > len(target_reasons))
                is not (raw_target.get("reasons_truncated") is True)
                or (
                    kind != "metrics_only"
                    and (
                        not isinstance(raw_target.get("passed"), bool)
                        or not isinstance(raw_target.get("blocking"), bool)
                    )
                )
            ):
                return invalid("invalid checkpoint target contract")
            targets.append({
                "target": target,
                "passed": (
                    raw_target.get("passed")
                    if isinstance(raw_target.get("passed"), bool)
                    else None
                ),
                "blocking": raw_target.get("blocking") is True,
                "reasons": _bounded_text_items(
                    target_reasons, item_limit=4, text_limit=300
                ),
                "reason_count": target_reason_count,
                "reasons_truncated": raw_target.get("reasons_truncated") is True,
                "metrics": metrics,
            })
        reasons = _bounded_text_items(
            raw_reasons,
            item_limit=MAX_CHECKPOINT_REASONS,
            text_limit=500,
        )
        if len(reasons) != len(raw_reasons):
            return invalid("invalid checkpoint result contract")
        return {
            "available": True,
            "status": status,
            "kind": kind,
            "threshold": threshold,
            "actual_strict_rows": actual_rows,
            "quality_passed": quality_passed,
            "completed_at": completed_at,
            "training_run_id": _bounded_text(
                result.get("training_run_id"), limit=160
            ),
            "generation": _bounded_text(result.get("generation"), limit=160),
            "reason_count": reason_count,
            "reasons": reasons,
            "reasons_truncated": result.get("reasons_truncated") is True,
            "target_count": target_count,
            "failed_target_count": failed_target_count,
            "target_summaries": targets,
            "target_summaries_truncated": result.get(
                "target_summaries_truncated"
            ) is True,
            "evidence_authentication": evidence,
            "evidence_error": evidence_error,
        }

    @staticmethod
    def _active_model_target_metrics(
        incumbent_comparison: Mapping[str, Any],
    ) -> dict[str, Any]:
        comparisons = _mapping(incumbent_comparison.get("comparisons"))
        items: list[dict[str, Any]] = []
        valid_count = 0
        for raw_target, raw_comparison in comparisons.items():
            comparison = _mapping(raw_comparison)
            target = _bounded_text(raw_target, limit=96)
            metric = _bounded_text(comparison.get("metric"), limit=96)
            candidate = _number(comparison.get("candidate"))
            incumbent = _number(comparison.get("incumbent"))
            ratio = _number(comparison.get("ratio"))
            if (
                not target
                or not metric
                or candidate is None
                or incumbent is None
                or ratio is None
            ):
                continue
            valid_count += 1
            if len(items) >= MAX_ACTIVE_MODEL_TARGET_METRICS:
                continue
            items.append(
                {
                    "target": target,
                    "metric": metric,
                    "candidate": candidate,
                    "incumbent": incumbent,
                    "ratio": ratio,
                }
            )
        return {
            "items": items,
            "total": valid_count,
            "limit": MAX_ACTIVE_MODEL_TARGET_METRICS,
            "truncated": valid_count > MAX_ACTIVE_MODEL_TARGET_METRICS,
        }

    @staticmethod
    def _validated_designs(
        full_state: Mapping[str, Any],
        full_status: Mapping[str, Any],
        standard_states: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        standard_by_digest: dict[str, dict[str, Any]] = {}
        for state in standard_states:
            for candidate in _items(state.get("candidates")):
                digest = str(candidate.get("candidate_digest") or "")
                if digest:
                    standard_by_digest[digest] = candidate

        active_full: dict[str, dict[str, Any]] = {}
        for task in _items(full_status.get("active_tasks")):
            digest = str(task.get("candidate_digest") or "")
            if digest:
                active_full[digest] = task

        candidates: list[dict[str, Any]] = []
        for candidate in _items(full_state.get("candidates")):
            digest = str(candidate.get("candidate_digest") or "").strip()
            volume = _number(candidate.get("standard_actual_volume_L"))
            loss = _number(candidate.get("standard_actual_total_loss_W"))
            if not digest or volume is None or loss is None:
                continue
            standard = standard_by_digest.get(digest, {})
            standard_task_id = _integer(
                candidate.get("standard_task_id") or standard.get("task_id")
            )
            if standard_task_id is not None and standard_task_id <= 0:
                standard_task_id = None
            full_task = active_full.get(digest, {})
            full_task_id = _integer(
                full_task.get("task_id")
                or candidate.get("fine_task_id")
                or candidate.get("full_task_id")
            )
            if full_task_id is not None and full_task_id <= 0:
                full_task_id = None
            full_task_status = str(
                full_task.get("status")
                or candidate.get("fine_task_status")
                or candidate.get("full_task_status")
                or ""
            ).lower()
            standard_pass = standard.get("standard_fea_spec_pass")
            # The full-model controller discovers only authenticated Standard
            # PASS rows, so its fixed state contract is authoritative when an
            # older Standard state has already been compacted.
            if not isinstance(standard_pass, bool):
                standard_pass = True
            candidates.append(
                {
                    "candidate_digest": digest,
                    "objectives": {
                        "volume_L": volume,
                        "total_loss_W": loss,
                    },
                    "objective_source": "standard_fea_actual",
                    "run_id": str(candidate.get("run_id") or ""),
                    "model_id": str(candidate.get("model_id") or ""),
                    "standard": {
                        "task_id": standard_task_id,
                        "pass": standard_pass,
                        "status": "pass" if standard_pass else "fail",
                    },
                    "full": {
                        "task_id": full_task_id,
                        "status": full_task_status or "not_submitted",
                        "pass": candidate.get("full_model_spec_pass"),
                    },
                    "active_full": full_task_status
                    in {"queued", "attaching", "running"},
                    "discovered_at": candidate.get("discovered_at"),
                }
            )

        for candidate in candidates:
            volume = candidate["objectives"]["volume_L"]
            loss = candidate["objectives"]["total_loss_W"]
            candidate["nondominated"] = not any(
                other is not candidate
                and other["objectives"]["volume_L"] <= volume
                and other["objectives"]["total_loss_W"] <= loss
                and (
                    other["objectives"]["volume_L"] < volume
                    or other["objectives"]["total_loss_W"] < loss
                )
                for other in candidates
            )

        selected = list(candidates)
        selected.sort(
            key=lambda item: (
                not item["active_full"],
                not item["nondominated"],
                item["objectives"]["volume_L"],
                item["objectives"]["total_loss_W"],
                item["candidate_digest"],
            )
        )
        return {
            "items": selected[:MAX_VALIDATED_DESIGNS],
            "nondominated_count": sum(
                1 for candidate in candidates if candidate["nondominated"]
            ),
            "active_full_count": sum(
                1 for candidate in candidates if candidate["active_full"]
            ),
            "eligible_count": sum(
                1 for candidate in candidates
                if candidate["active_full"] or candidate["nondominated"]
            ),
            "total_count": len(candidates),
            "displayed_count": min(len(candidates), MAX_VALIDATED_DESIGNS),
            "ready_full_count": sum(
                1 for candidate in candidates
                if not candidate["active_full"]
                and candidate["full"]["status"] == "not_submitted"
            ),
            "limit": MAX_VALIDATED_DESIGNS,
            "truncated": len(candidates) > MAX_VALIDATED_DESIGNS,
            "objective_names": ["volume_L", "total_loss_W"],
            "objective_source": "standard_fea_actual",
        }

    def _build_snapshot(self) -> dict[str, Any]:
        sources: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
            # The canonical controller re-audits the live parquet every cycle.
            # Unlike the experimental refresh status below, it keeps reporting
            # the latest strict row count while no training wave is active.
            "canonical_surrogate_status": self._read_json(
                "canonical_surrogate_status", "mft_pipeline/surrogate_status.json"
            ),
            "surrogate_status": self._read_json(
                "surrogate_status", "mft_pipeline/experimental_continuous/status.json"
            ),
            "surrogate_pointer": self._read_json(
                "surrogate_pointer", "mft_pipeline/experimental_surrogate.json"
            ),
            "surrogate_state": self._read_json(
                "surrogate_state", "mft_pipeline/experimental_continuous/state.json"
            ),
            "nsga_main": self._read_json(
                "nsga_main", "mft_nsga_continuous/status.json"
            ),
            "nsga_fast": self._read_first(
                "nsga_fast",
                (
                    "mft_nsga_newmodel_fastlane_v2/status.json",
                    "mft_nsga_newmodel_fastlane/status.json",
                ),
            ),
            "standard_fea_main": self._read_json(
                "standard_fea_main", "mft_nsga_fea_validation/status.json"
            ),
            "standard_fea_main_state": self._read_json(
                "standard_fea_main_state", "mft_nsga_fea_validation/state.json"
            ),
            "standard_fea_fast": self._read_json(
                "standard_fea_fast", "mft_nsga_fastlane_fea_validation/status.json"
            ),
            "standard_fea_fast_state": self._read_json(
                "standard_fea_fast_state", "mft_nsga_fastlane_fea_validation/state.json"
            ),
            "full_model": self._read_json(
                "full_model", "mft_nsga_full_model_validation/status.json"
            ),
            "full_model_state": self._read_json(
                "full_model_state", "mft_nsga_full_model_validation/state.json"
            ),
        }
        targeted_sources = self._dynamic_json_sources(
            "targeted_hpo",
            "mft_pipeline/targeted_hpo_exact14/*/status.json",
        )
        sources["targeted_hpo"] = (
            targeted_sources[0]
            if targeted_sources
            else ({}, {"source": "targeted_hpo", "available": False, "stale": False})
        )
        post_targeted_sources = self._dynamic_json_sources(
            "post_targeted_full25",
            "mft_pipeline/post_targeted_full25/*/status.json",
        )
        sources["post_targeted_full25"] = (
            post_targeted_sources[0]
            if post_targeted_sources
            else ({}, {
                "source": "post_targeted_full25",
                "available": False,
                "stale": False,
            })
        )
        isolated_sources = self._dynamic_json_sources(
            "isolated_nsga",
            "mft_nsga_transition_audit/*/isolated_nsga/*/ui_status.json",
        )
        for index, source in enumerate(isolated_sources):
            sources[f"isolated_nsga_{index}"] = source
        approved_status_sources = self._dynamic_json_sources(
            "approved_nsga",
            "mft_pipeline/post_targeted_full25/*/approved_nsga_continuous/status.json",
        )
        approved_sources: list[tuple[
            tuple[dict[str, Any], dict[str, Any]],
            tuple[dict[str, Any], dict[str, Any]],
            tuple[dict[str, Any], dict[str, Any]],
        ]] = []
        for index, status_source in enumerate(approved_status_sources):
            approved_status, approved_meta = status_source
            relative_status = Path(str(approved_meta.get("relative_path") or ""))
            relative_parent = relative_status.parent
            manifest_source = self._read_json(
                f"approved_nsga_manifest_{index}",
                (relative_parent / "manifest.json").as_posix(),
            )
            state_source = self._read_json(
                f"approved_nsga_state_{index}",
                (relative_parent / "state.json").as_posix(),
            )
            sources[f"approved_nsga_{index}"] = (
                approved_status,
                approved_meta,
            )
            sources[f"approved_nsga_manifest_{index}"] = manifest_source
            sources[f"approved_nsga_state_{index}"] = state_source
            approved_sources.append((status_source, manifest_source, state_source))
        canonical_status, canonical_meta = sources["canonical_surrogate_status"]
        if canonical_meta.get("available"):
            contract_errors = _canonical_contract_errors(canonical_status)
            if contract_errors:
                canonical_status = {}
                canonical_meta = {
                    **canonical_meta,
                    "available": False,
                    "message": "invalid canonical status contract: "
                    + "; ".join(contract_errors[:8]),
                }
                sources["canonical_surrogate_status"] = (
                    canonical_status,
                    canonical_meta,
                )

        errors = [
            {
                "source": meta.get("source"),
                "message": meta.get("message"),
                "stale": bool(meta.get("stale")),
            }
            for _payload, meta in sources.values()
            if meta.get("message")
        ]

        surrogate_status, surrogate_meta = sources["surrogate_status"]
        pointer, pointer_meta = sources["surrogate_pointer"]
        surrogate_state, surrogate_state_meta = sources["surrogate_state"]
        targeted_status, targeted_meta = sources["targeted_hpo"]
        targeted_hpo = self._targeted_hpo(targeted_status, targeted_meta)
        if targeted_hpo.get("error") and targeted_hpo.get("available") is not True:
            errors.append({
                "source": targeted_meta.get("source"),
                "message": targeted_hpo.get("error"),
                "stale": bool(targeted_meta.get("stale")),
            })
        post_targeted_status, post_targeted_meta = sources["post_targeted_full25"]
        post_targeted_full25 = self._post_targeted_full25(
            post_targeted_status, post_targeted_meta
        )
        if (
            post_targeted_status
            and post_targeted_full25.get("available") is not True
        ):
            errors.append({
                "source": post_targeted_meta.get("source"),
                "message": "; ".join(
                    post_targeted_full25.get("contract_errors") or []
                ) or "invalid post-targeted full-25 status contract",
                "stale": bool(post_targeted_meta.get("stale")),
            })
        wave_detail = _mapping(surrogate_status.get("active_wave_detail"))
        strict_snapshot = _mapping(wave_detail.get("strict_snapshot"))
        raw_rows = max(
            (
                value
                for value in (
                    _integer(canonical_status.get("raw_rows")),
                    _integer(strict_snapshot.get("raw_rows")),
                )
                if value is not None
            ),
            default=None,
        )
        strict_candidates = [
            _integer(canonical_status.get("strict_full_rows")),
            _integer(surrogate_status.get("observed_strict_full_rows")),
            _integer(strict_snapshot.get("strict_full_rows")),
            _integer(surrogate_state.get("active_wave_strict_rows")),
            _integer(surrogate_state.get("last_attempted_strict_rows")),
        ]
        strict_rows = max(
            (value for value in strict_candidates if value is not None),
            default=None,
        )
        model_rows = _integer(pointer.get("strict_full_rows"))
        strict_delta = (
            strict_rows - model_rows
            if strict_rows is not None and model_rows is not None
            else None
        )
        model_id = _model_id(pointer)
        canonical_phase = str(
            surrogate_status.get("wave_phase") or wave_detail.get("phase") or ""
        )
        canonical_training_jobs = _items(wave_detail.get("jobs"))
        canonical_training_active = bool(
            wave_detail.get("worker_pid")
            or wave_detail.get("supervisor_pid")
            or any(job.get("alive") is True for job in canonical_training_jobs)
            or surrogate_status.get("state") == "wave_running"
            or canonical_phase in {
                "candidate_training",
                "experimental_hpo",
                "hpo_running",
                "training",
            }
        )
        targeted_visible = targeted_hpo.get("available") is True
        if targeted_visible:
            phase = str(targeted_hpo.get("phase") or "")
            training_jobs = list(
                _mapping(targeted_hpo.get("hpo")).get("targets") or []
            )
            training_active = phase in {
                "hpo_queued",
                "hpo_running",
                "hpo_merging",
            }
            surrogate_hpo = _mapping(targeted_hpo.get("hpo"))
        else:
            phase = canonical_phase
            training_jobs = canonical_training_jobs
            training_active = canonical_training_active
            surrogate_hpo = self._surrogate_hpo(surrogate_status)
        completed_hpo_results = self._completed_hpo_results(surrogate_status)
        surrogate_decision = self._surrogate_decision(surrogate_state)
        incumbent_comparison = _mapping(pointer.get("incumbent_comparison"))
        active_model_metrics = {
            name: value
            for name in (
                "aggregate_loss_ratio",
                "worst_target_loss_ratio",
                "maximum_aggregate_loss_ratio",
                "maximum_target_loss_ratio",
            )
            if (value := _number(incumbent_comparison.get(name))) is not None
        }
        active_model_target_metrics = self._active_model_target_metrics(
            incumbent_comparison
        )

        nsga_main, main_meta = sources["nsga_main"]
        nsga_fast, fast_meta = sources["nsga_fast"]
        nsga_lanes = [
            self._nsga_lane("main", nsga_main, main_meta),
            self._nsga_lane("new-model-fast", nsga_fast, fast_meta),
        ]
        isolated_lanes: list[dict[str, Any]] = []
        for isolated_status, isolated_meta in isolated_sources:
            lane = self._isolated_nsga_lane(isolated_status, isolated_meta)
            nsga_lanes.append(lane)
            isolated_lanes.append(lane)
            if lane.get("error"):
                errors.append({
                    "source": isolated_meta.get("source"),
                    "message": lane.get("error"),
                    "stale": bool(isolated_meta.get("stale")),
                })
        approved_lanes: list[dict[str, Any]] = []
        for index, (status_source, manifest_source, state_source) in enumerate(
            approved_sources
        ):
            approved_status, approved_meta = status_source
            approved_manifest, approved_manifest_meta = manifest_source
            approved_state, approved_state_meta = state_source
            lane = self._approved_nsga_lane(
                "approved-full25" if index == 0 else f"approved-full25-{index + 1}",
                approved_status,
                approved_meta,
                approved_manifest,
                approved_manifest_meta,
                approved_state,
                approved_state_meta,
                post_targeted_full25,
            )
            nsga_lanes.append(lane)
            approved_lanes.append(lane)
            if lane.get("error"):
                errors.append({
                    "source": approved_meta.get("source"),
                    "message": lane.get("error"),
                    "stale": bool(approved_meta.get("stale")),
                })
        active_nsga_lanes = [
            lane
            for lane in nsga_lanes
            if lane["available"]
            and not _mapping(lane.get("freshness")).get("stale")
        ]
        pareto_results: list[dict[str, Any]] = []
        pareto_errors: list[dict[str, Any]] = []
        seen_pareto: set[tuple[str, str, str]] = set()
        for lane in nsga_lanes:
            for field in ("latest_pareto", "lifetime_latest_pareto"):
                pareto = _mapping(lane.get(field))
                if pareto.get("available") is not True:
                    if pareto.get("error"):
                        pareto_errors.append({
                            "lane": _bounded_text(lane.get("name"), limit=40),
                            "scope": _bounded_text(
                                pareto.get("scope"), limit=40
                            ),
                            "run_id": _bounded_text(
                                pareto.get("run_id"), limit=32
                            ),
                            "model_id": _bounded_text(
                                pareto.get("model_id"), limit=160
                            ),
                            "error": _bounded_text(
                                pareto.get("error"), limit=240
                            ),
                            "stale": bool(
                                _mapping(lane.get("freshness")).get("stale")
                            ),
                        })
                    continue
                identity = (
                    str(lane.get("name") or ""),
                    str(pareto.get("run_id") or ""),
                    str(pareto.get("model_id") or ""),
                )
                if identity in seen_pareto:
                    continue
                seen_pareto.add(identity)
                pareto_results.append({
                    "lane": lane.get("name"),
                    "stale": bool(
                        _mapping(lane.get("freshness")).get("stale")
                    ),
                    **pareto,
                })

        fea_statuses = [
            sources["standard_fea_main"][0],
            sources["standard_fea_fast"][0],
        ]
        fea_states = [
            sources["standard_fea_main_state"][0],
            sources["standard_fea_fast_state"][0],
        ]
        # Standard controllers expose the entire shared validation lane in
        # global_lane_inventory, including full-model task names.  Keep the
        # shared inventory for cross-controller/stale-reader coverage, but do
        # not count fine/full tasks twice in the Standard FEA card.
        fea_tasks = self._active_task_inventory(
            fea_statuses, exclude_name_prefixes=("mft-nsgafea-f-",)
        )
        standard_pass, standard_fail, standard_valid = self._standard_results(fea_states)

        full_status, full_meta = sources["full_model"]
        full_state, _full_state_meta = sources["full_model_state"]
        # The full-model controller exposes the shared standard+full lane in
        # global_lane_inventory.  Only active_tasks are actual full-model jobs.
        full_tasks = self._active_task_inventory(
            [full_status], include_global=False
        )
        full_pass_state, full_fail = self._full_results([full_state])
        full_counts = _mapping(full_status.get("counts"))
        full_pass = max(
            full_pass_state,
            _integer(full_counts.get("full_model_pass")) or 0,
        )
        validated_designs = self._validated_designs(
            full_state,
            full_status,
            fea_states,
        )
        full_discovered_standard_pass = validated_designs["total_count"]
        validated_designs.update({
            "known_standard_pass_count": max(
                standard_pass, full_discovered_standard_pass
            ),
            "full_discovered_standard_pass_count": (
                full_discovered_standard_pass
            ),
            "pending_full_discovery_count": max(
                0, standard_pass - full_discovered_standard_pass
            ),
            "source_scope": "full_controller_discovered_standard_pass",
        })

        source_freshness = {
            "canonical_surrogate_status": self._freshness(
                canonical_status,
                canonical_meta,
                stale_after_seconds=STALE_AFTER_SECONDS[
                    "canonical_surrogate_status"
                ],
            ),
            "surrogate_status": self._freshness(
                surrogate_status,
                surrogate_meta,
                stale_after_seconds=STALE_AFTER_SECONDS["surrogate_status"],
            ),
            # Pointer and terminal decision state are event-driven immutable
            # evidence; age alone does not make them stale.
            "surrogate_pointer": self._freshness(
                pointer, pointer_meta, stale_after_seconds=None
            ),
            "surrogate_state": self._freshness(
                surrogate_state, surrogate_state_meta, stale_after_seconds=None
            ),
            "targeted_hpo": _mapping(targeted_hpo.get("freshness")),
            "post_targeted_full25": _mapping(
                post_targeted_full25.get("freshness")
            ),
            "nsga_main": nsga_lanes[0]["freshness"],
            "nsga_fast": nsga_lanes[1]["freshness"],
            "standard_fea_main": self._freshness(
                fea_statuses[0],
                sources["standard_fea_main"][1],
                stale_after_seconds=STALE_AFTER_SECONDS["validation"],
            ),
            "standard_fea_fast": self._freshness(
                fea_statuses[1],
                sources["standard_fea_fast"][1],
                stale_after_seconds=STALE_AFTER_SECONDS["validation"],
            ),
            "full_model": self._freshness(
                full_status,
                full_meta,
                stale_after_seconds=STALE_AFTER_SECONDS["validation"],
            ),
        }
        for index, lane in enumerate(isolated_lanes):
            source_freshness[f"isolated_nsga_{index}"] = _mapping(
                lane.get("freshness")
            )
        for index, lane in enumerate(approved_lanes):
            source_freshness[f"approved_nsga_{index}"] = _mapping(
                lane.get("freshness")
            )
        stale_sources = sorted(
            name
            for name, freshness in source_freshness.items()
            if freshness["stale"]
        )

        canonical_queue_payload = _mapping(canonical_status.get("queue"))
        canonical_queue = {
            name: _nonnegative_integer(canonical_queue_payload.get(name)) or 0
            for name in (
                "running",
                "queued",
                "retry_wait",
                "succeeded",
                "failed",
                "cancelled",
            )
        }
        canonical_last_jobs_payload = _mapping(canonical_status.get("last_jobs"))
        canonical_last_jobs: dict[str, int | None] = {}
        for name in ("collect", "train"):
            job_id = _nonnegative_integer(canonical_last_jobs_payload.get(name))
            canonical_last_jobs[name] = job_id if job_id else None
        canonical_strict_rows = _nonnegative_integer(
            canonical_status.get("strict_full_rows")
        )
        activation_minimum = _nonnegative_integer(
            canonical_status.get("activation_minimum_strict_full_rows")
        )
        first_tuning = _nonnegative_integer(
            canonical_status.get("first_tuning_strict_full_rows")
        )
        canonical_blocked = [
            {
                "name": _bounded_text(name, limit=64),
                "detail": _bounded_text(detail, limit=200),
            }
            for name, detail in sorted(
                _mapping(canonical_status.get("blocked")).items(),
                key=lambda item: str(item[0]),
            )[:20]
        ]
        checkpoint_result = self._checkpoint_result(
            canonical_status.get("last_checkpoint_result")
        )
        nsga_available_lanes = sum(1 for lane in nsga_lanes if lane["available"])
        nsga_current_result_lanes = sum(
            1
            for lane in nsga_lanes
            if lane["available"] and lane["current_model"]["available"]
        )
        nsga_current_aggregate_complete = bool(
            nsga_available_lanes > 0
            and nsga_current_result_lanes == nsga_available_lanes
        )

        available = any(bool(meta.get("available")) for _payload, meta in sources.values())
        return {
            "schema_version": "mft-pipeline-visibility-v2",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "available": available,
            "data": {
                "available": bool(
                    canonical_meta.get("available")
                    or surrogate_meta.get("available")
                    or surrogate_state_meta.get("available")
                ),
                # strict_rows is the latest quality-gated population from the
                # canonical controller.  The experimental wave status is only
                # a fallback because it can remain pinned to its last attempted
                # wave while the append-only canonical dataset keeps growing.
                # raw_rows remains optional until a producer publishes it.
                "dataset_rows": strict_rows,
                "raw_rows": raw_rows,
                "strict_rows": strict_rows,
                "model_training_rows": model_rows,
                "strict_delta_since_model": strict_delta,
                "source_kind": "strict_full_rows",
                "updated_at": canonical_status.get("updated_at")
                or surrogate_status.get("updated_at"),
            },
            "canonical_training": {
                "available": bool(canonical_meta.get("available")),
                "state": _bounded_text(
                    canonical_status.get("state"), limit=80
                )
                or "unavailable",
                "cycle": _nonnegative_integer(canonical_status.get("cycle")),
                "raw_rows": _nonnegative_integer(canonical_status.get("raw_rows")),
                "strict_rows": canonical_strict_rows,
                "activation_minimum_strict_full_rows": activation_minimum,
                "rows_until_activation": (
                    max(0, activation_minimum - canonical_strict_rows)
                    if activation_minimum is not None
                    and canonical_strict_rows is not None
                    else None
                ),
                "first_tuning_strict_full_rows": first_tuning,
                "rows_until_first_tuning": (
                    max(0, first_tuning - canonical_strict_rows)
                    if first_tuning is not None and canonical_strict_rows is not None
                    else None
                ),
                # The canonical queue aggregates collect/train/optimize/verify
                # jobs.  Do not claim a train-specific state from this count.
                "pipeline_active": canonical_queue["running"] > 0,
                "queue": canonical_queue,
                "last_jobs": canonical_last_jobs,
                "dataset_generation": _bounded_text(
                    canonical_status.get("dataset_generation"), limit=160
                ),
                "active_model_state": _bounded_text(
                    canonical_status.get("active_model_state"), limit=80
                ),
                "solver_revision": _bounded_text(
                    canonical_status.get("solver_revision"), limit=80
                ),
                "library_revision": _bounded_text(
                    canonical_status.get("library_revision"), limit=80
                ),
                "blocked": canonical_blocked,
                "last_checkpoint_result": checkpoint_result,
                "last_error": _bounded_text(
                    canonical_status.get("last_error"), limit=300
                ),
                "freshness": source_freshness["canonical_surrogate_status"],
                "updated_at": canonical_status.get("updated_at"),
            },
            "surrogate": {
                "available": bool(
                    surrogate_meta.get("available")
                    or surrogate_state_meta.get("available")
                    or pointer_meta.get("available")
                    or post_targeted_meta.get("available")
                ),
                "state": (
                    phase
                    if targeted_visible
                    else str(surrogate_status.get("state") or "unavailable")
                ),
                "controller_state": str(
                    surrogate_status.get("state") or "unavailable"
                ),
                "wave": str(
                    targeted_hpo.get("wave")
                    if targeted_visible
                    else (
                        surrogate_status.get("active_wave")
                        or surrogate_state.get("active_wave")
                        or ""
                    )
                ),
                "phase": phase,
                "last_result_phase": str(
                    surrogate_state.get("last_result_phase") or ""
                ),
                "training_active": training_active,
                "training_jobs": len(training_jobs),
                "training_jobs_ready": sum(
                    1 for job in training_jobs if job.get("result_ready") is True
                ),
                "model_id": model_id,
                "model_rows": model_rows,
                "active_model": {
                    "model_id": model_id,
                    "training_run_id": str(pointer.get("training_run_id") or ""),
                    "rows": model_rows,
                    "published_at": pointer.get("published_at"),
                    "lane": str(pointer.get("lane") or ""),
                    "dataset_sha256": str(pointer.get("dataset_sha256") or ""),
                    "generation_id": str(pointer.get("generation") or "")
                    .replace("\\", "/")
                    .rstrip("/")
                    .split("/")[-1],
                    "generation_report_sha256": str(
                        pointer.get("generation_report_sha256") or ""
                    ),
                    "solver_revision": str(
                        pointer.get("fea_solver_revision")
                        or pointer.get("solver_revision")
                        or ""
                    ),
                    "library_revision": str(
                        pointer.get("fea_library_revision")
                        or pointer.get("library_revision")
                        or ""
                    ),
                    "metrics": active_model_metrics,
                    "target_metrics": active_model_target_metrics,
                },
                "hpo": surrogate_hpo,
                "targeted_hpo": targeted_hpo,
                "post_targeted_full25": post_targeted_full25,
                "last_completed_hpo_results": completed_hpo_results,
                "last_decision": surrogate_decision,
                "next_refresh_strict_rows": (
                    _integer(surrogate_status.get("next_refresh_strict_rows"))
                    or (
                        (_integer(surrogate_state.get("last_attempted_strict_rows")) or 0)
                        + (_integer(surrogate_status.get("minimum_new_strict_rows")) or 0)
                    )
                    or None
                ),
                "freshness": (
                    source_freshness["targeted_hpo"]
                    if targeted_visible
                    else source_freshness["surrogate_status"]
                ),
                "updated_at": (
                    targeted_hpo.get("updated_at")
                    if targeted_visible
                    else (
                        surrogate_status.get("updated_at")
                        or pointer.get("published_at")
                    )
                ),
            },
            "nsga": {
                "available": any(lane["available"] for lane in nsga_lanes),
                "lanes": nsga_lanes,
                # Seeds in a stale controller snapshot are historical evidence,
                # not live OS workers.  Preserve them in each lane while keeping
                # the aggregate active-worker count honest.
                "active_seed_workers": sum(
                    lane["seed_workers"] for lane in active_nsga_lanes
                ),
                "current_model_completed_runs": sum(
                    lane["current_model"]["completed_runs"] or 0
                    for lane in nsga_lanes
                    if lane["current_model"]["available"]
                ),
                "current_model_feasible_runs": sum(
                    lane["current_model"]["feasible_runs"]
                    for lane in nsga_lanes
                    if lane["current_model"]["available"]
                ),
                "current_model_infeasible_runs": sum(
                    lane["current_model"]["infeasible_runs"]
                    for lane in nsga_lanes
                    if lane["current_model"]["available"]
                ),
                "current_model_pareto_runs": sum(
                    lane["current_model"]["pareto_runs"]
                    for lane in nsga_lanes
                    if lane["current_model"]["available"]
                ),
                "current_model_result_lanes": nsga_current_result_lanes,
                "current_model_expected_lanes": nsga_available_lanes,
                "current_model_aggregate_complete": (
                    nsga_current_aggregate_complete
                ),
                "lifetime_completed_runs": sum(
                    lane["lifetime"]["completed_runs"] for lane in nsga_lanes
                ),
                "completed_runs": sum(lane["completed_runs"] for lane in nsga_lanes),
                "feasible_runs": sum(lane["feasible_runs"] for lane in nsga_lanes),
                "infeasible_runs": sum(
                    lane["infeasible_runs"] for lane in nsga_lanes
                ),
                "pareto_runs": sum(lane["pareto_runs"] for lane in nsga_lanes),
                "active_models": sorted(
                    {lane["model_id"] for lane in nsga_lanes if lane["model_id"]}
                ),
                "pareto_results": pareto_results,
                "pareto_errors": pareto_errors,
                "designs": validated_designs,
            },
            "standard_fea": {
                "available": any(
                    sources[name][1].get("available")
                    for name in ("standard_fea_main", "standard_fea_fast")
                ),
                "active": len(fea_tasks),
                "running": sum(1 for status in fea_tasks.values() if status == "running"),
                "attaching": sum(1 for status in fea_tasks.values() if status == "attaching"),
                "queued": sum(1 for status in fea_tasks.values() if status == "queued"),
                "pass": standard_pass,
                "fail": standard_fail,
                "valid_results": standard_valid,
                "states": [
                    str(status.get("state") or "unavailable") for status in fea_statuses
                ],
                "freshness": {
                    "stale": any(
                        source_freshness[name]["stale"]
                        for name in ("standard_fea_main", "standard_fea_fast")
                    ),
                    "sources": [
                        source_freshness["standard_fea_main"],
                        source_freshness["standard_fea_fast"],
                    ],
                },
                "updated_at": max(
                    (str(status.get("updated_at") or "") for status in fea_statuses),
                    default="",
                ),
            },
            "full_model": {
                "available": bool(full_meta.get("available")),
                "state": str(full_status.get("state") or "unavailable"),
                "active": len(full_tasks),
                "running": sum(1 for status in full_tasks.values() if status == "running"),
                "attaching": sum(
                    1 for status in full_tasks.values() if status == "attaching"
                ),
                "queued": sum(1 for status in full_tasks.values() if status == "queued"),
                "pass": full_pass,
                "fail": full_fail,
                "standard_pass_discovered": _integer(
                    full_counts.get("standard_pass_discovered")
                ) or 0,
                "freshness": source_freshness["full_model"],
                "updated_at": full_status.get("updated_at"),
            },
            "sources": source_freshness,
            "stale_sources": stale_sources,
            "errors": errors,
        }
