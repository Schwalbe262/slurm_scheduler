from __future__ import annotations

import copy
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_RUNTIME_ROOT = Path(r"C:\Users\peets\slurm_scheduler_runtime")
DEFAULT_MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_HPO_TARGETS = 20
MAX_VALIDATED_DESIGNS = 12

STALE_AFTER_SECONDS = {
    "canonical_surrogate_status": 150.0,
    "surrogate_status": 75.0,
    "nsga": 75.0,
    "validation": 90.0,
}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


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


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return result if result is not None and math.isfinite(result) else None


def _text_items(value: object, *, limit: int = 20) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [
        str(item)
        for item in value[:limit]
        if item is not None and str(item).strip()
    ]


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
            "surrogate": {
                "available": False,
                "hpo": {"ready": 0, "total": 0, "targets": []},
                "last_decision": {"available": False},
            },
            "nsga": {
                "available": False,
                "lanes": [],
                "active_seed_workers": 0,
                "designs": {
                    "items": [],
                    "nondominated_count": 0,
                    "active_full_count": 0,
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
        infeasibility = _mapping(status.get("last_infeasibility"))
        archive = _mapping(infeasibility.get("least_violation_archive"))
        warm_start = _mapping(active_run.get("warm_start"))
        feasible_runs = _integer(outcomes.get("feasible_complete"))
        infeasible_runs = _integer(outcomes.get("infeasible_complete"))
        return {
            "name": name,
            "available": bool(meta.get("available")),
            "stale": bool(meta.get("stale")),
            "state": str(status.get("state") or "unavailable"),
            "run_id": str(active_run.get("run_id") or ""),
            "seeds": seeds,
            "seed_workers": len(seeds),
            "model_id": str(active_run.get("model_id") or ""),
            "model_lane": str(active_run.get("model_lane") or ""),
            "completed_runs": _integer(status.get("completed_run_count")) or 0,
            "feasible_runs": feasible_runs if feasible_runs is not None else 0,
            "infeasible_runs": infeasible_runs if infeasible_runs is not None else 0,
            # Every feasible completed run publishes an authenticated Pareto
            # front.  Keep the producer's outcome terminology as well.
            "pareto_runs": feasible_runs if feasible_runs is not None else 0,
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
        failure_states = {
            "collector_failed",
            "failed",
            "invalid_result",
            "scheduler_failed",
            "task_failed",
        }
        terminal_task_states = {"cancelled", "failed", "timed_out", "timeout"}
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
                collection_state = str(candidate.get("collection_state") or "").lower()
                task_status = str(candidate.get("task_status") or "").lower()
                if collection_state == "collector_succeeded":
                    if candidate.get("result_contract_valid") is True:
                        valid.add(identity)
                    if candidate.get("standard_fea_spec_pass") is True:
                        passed.add(identity)
                    elif candidate.get("standard_fea_spec_pass") is False:
                        failed.add(identity)
                elif collection_state in failure_states or task_status in terminal_task_states:
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

        selected = [
            candidate
            for candidate in candidates
            if candidate["active_full"] or candidate["nondominated"]
        ]
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
            "eligible_count": len(selected),
            "limit": MAX_VALIDATED_DESIGNS,
            "truncated": len(selected) > MAX_VALIDATED_DESIGNS,
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
        errors = [
            {
                "source": meta.get("source"),
                "message": meta.get("message"),
                "stale": bool(meta.get("stale")),
            }
            for _payload, meta in sources.values()
            if meta.get("message")
        ]

        canonical_status, canonical_meta = sources["canonical_surrogate_status"]
        surrogate_status, surrogate_meta = sources["surrogate_status"]
        pointer, pointer_meta = sources["surrogate_pointer"]
        surrogate_state, surrogate_state_meta = sources["surrogate_state"]
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
        phase = str(surrogate_status.get("wave_phase") or wave_detail.get("phase") or "")
        training_jobs = _items(wave_detail.get("jobs"))
        training_active = bool(
            wave_detail.get("worker_pid")
            or wave_detail.get("supervisor_pid")
            or any(job.get("alive") is True for job in training_jobs)
            or surrogate_status.get("state") == "wave_running"
            or phase in {
                "candidate_training",
                "experimental_hpo",
                "hpo_running",
                "training",
            }
        )
        surrogate_hpo = self._surrogate_hpo(surrogate_status)
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

        nsga_main, main_meta = sources["nsga_main"]
        nsga_fast, fast_meta = sources["nsga_fast"]
        nsga_lanes = [
            self._nsga_lane("main", nsga_main, main_meta),
            self._nsga_lane("new-model-fast", nsga_fast, fast_meta),
        ]

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
        stale_sources = sorted(
            name
            for name, freshness in source_freshness.items()
            if freshness["stale"]
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
            "surrogate": {
                "available": bool(
                    surrogate_meta.get("available")
                    or surrogate_state_meta.get("available")
                    or pointer_meta.get("available")
                ),
                "state": str(surrogate_status.get("state") or "unavailable"),
                "wave": str(
                    surrogate_status.get("active_wave")
                    or surrogate_state.get("active_wave")
                    or ""
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
                },
                "hpo": surrogate_hpo,
                "last_decision": surrogate_decision,
                "next_refresh_strict_rows": (
                    _integer(surrogate_status.get("next_refresh_strict_rows"))
                    or (
                        (_integer(surrogate_state.get("last_attempted_strict_rows")) or 0)
                        + (_integer(surrogate_status.get("minimum_new_strict_rows")) or 0)
                    )
                    or None
                ),
                "freshness": source_freshness["surrogate_status"],
                "updated_at": surrogate_status.get("updated_at")
                or pointer.get("published_at"),
            },
            "nsga": {
                "available": any(lane["available"] for lane in nsga_lanes),
                "lanes": nsga_lanes,
                "active_seed_workers": sum(lane["seed_workers"] for lane in nsga_lanes),
                "completed_runs": sum(lane["completed_runs"] for lane in nsga_lanes),
                "feasible_runs": sum(lane["feasible_runs"] for lane in nsga_lanes),
                "infeasible_runs": sum(
                    lane["infeasible_runs"] for lane in nsga_lanes
                ),
                "pareto_runs": sum(lane["pareto_runs"] for lane in nsga_lanes),
                "active_models": sorted(
                    {lane["model_id"] for lane in nsga_lanes if lane["model_id"]}
                ),
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
