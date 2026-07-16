from __future__ import annotations

import copy
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_RUNTIME_ROOT = Path(r"C:\Users\peets\slurm_scheduler_runtime")
DEFAULT_MAX_JSON_BYTES = 2 * 1024 * 1024


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
            "schema_version": "mft-pipeline-visibility-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "available": False,
            "data": {"available": False},
            "surrogate": {"available": False},
            "nsga": {"available": False, "lanes": [], "active_seed_workers": 0},
            "standard_fea": {"available": False, "active": 0, "pass": 0, "fail": 0},
            "full_model": {"available": False, "active": 0, "pass": 0},
            "errors": [{"source": "reader", "message": message}],
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
                }
            with path.open("rb") as stream:
                raw = stream.read(self.max_json_bytes + 1)
            if len(raw) > self.max_json_bytes:
                raise ValueError(f"JSON read exceeded {self.max_json_bytes} bytes")
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("JSON root must be an object")
            normalized = dict(payload)
            self._file_cache[path] = (stat.st_mtime_ns, stat.st_size, normalized)
            self._last_good[path] = normalized
            return copy.deepcopy(normalized), {
                "source": source,
                "available": True,
                "stale": False,
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

    @staticmethod
    def _nsga_lane(name: str, status: Mapping[str, Any], meta: Mapping[str, Any]) -> dict[str, Any]:
        active_run = _mapping(status.get("active_run"))
        seeds = [
            seed for seed in (_integer(item) for item in active_run.get("seeds") or [])
            if seed is not None
        ]
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
            "next_seed_base": _integer(status.get("next_seed_base")),
            "model_switch_policy": str(status.get("model_switch_policy") or ""),
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

    def _build_snapshot(self) -> dict[str, Any]:
        sources: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
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

        surrogate_status, surrogate_meta = sources["surrogate_status"]
        pointer, pointer_meta = sources["surrogate_pointer"]
        surrogate_state, surrogate_state_meta = sources["surrogate_state"]
        wave_detail = _mapping(surrogate_status.get("active_wave_detail"))
        strict_snapshot = _mapping(wave_detail.get("strict_snapshot"))
        raw_rows = _integer(strict_snapshot.get("raw_rows"))
        strict_candidates = [
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
            or phase in {"candidate_training", "hpo_running", "training"}
        )

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

        available = any(bool(meta.get("available")) for _payload, meta in sources.values())
        return {
            "schema_version": "mft-pipeline-visibility-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "available": available,
            "data": {
                "available": bool(
                    surrogate_meta.get("available")
                    or surrogate_state_meta.get("available")
                ),
                # strict_rows is the quality-gated population used by the
                # continuous model loop.  Keep raw_rows as a separate optional
                # diagnostic because a controller waiting between waves may
                # intentionally omit it from status.json.
                "dataset_rows": strict_rows,
                "raw_rows": raw_rows,
                "strict_rows": strict_rows,
                "model_training_rows": model_rows,
                "strict_delta_since_model": strict_delta,
                "source_kind": "strict_full_rows",
                "updated_at": surrogate_status.get("updated_at"),
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
                "next_refresh_strict_rows": (
                    _integer(surrogate_status.get("next_refresh_strict_rows"))
                    or (
                        (_integer(surrogate_state.get("last_attempted_strict_rows")) or 0)
                        + (_integer(surrogate_status.get("minimum_new_strict_rows")) or 0)
                    )
                    or None
                ),
                "updated_at": surrogate_status.get("updated_at")
                or pointer.get("published_at"),
            },
            "nsga": {
                "available": any(lane["available"] for lane in nsga_lanes),
                "lanes": nsga_lanes,
                "active_seed_workers": sum(lane["seed_workers"] for lane in nsga_lanes),
                "completed_runs": sum(lane["completed_runs"] for lane in nsga_lanes),
                "active_models": sorted(
                    {lane["model_id"] for lane in nsga_lanes if lane["model_id"]}
                ),
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
                "queued": sum(1 for status in full_tasks.values() if status == "queued"),
                "pass": full_pass,
                "fail": full_fail,
                "standard_pass_discovered": _integer(
                    full_counts.get("standard_pass_discovered")
                ) or 0,
                "updated_at": full_status.get("updated_at"),
            },
            "errors": errors,
        }
