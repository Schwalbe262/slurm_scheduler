from __future__ import annotations

import json
import os
import re
import shlex
import threading
import time
from pathlib import Path
from math import ceil
import posixpath
from datetime import datetime, timezone

from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .allocation_metrics import annotate_allocation_fea_pressure, annotate_allocation_node_metrics
from .conda_sync import CondaEnvSyncManager
from .config import AppConfig, load_accounts, load_app_config
from .db import Database
from .git_auth import find_git_credential, git_task_payload
from .models import JobCreate, SchedulingProfile, TaskCreate, normalize_scheduling_profile
from .inventory import partition_rank
from .pestat import PestatNode, plan_dynamic_allocations
from .scheduler import Scheduler
from .slurm import SlurmAccountClient
from .task_commands import ACCOUNT_WORKSPACE_PLACEHOLDER, build_git_task_command

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def cleanup_local_temp_artifacts() -> None:
    """Remove conda-env-sync tarballs orphaned by a crash mid-sync."""
    import glob
    import tempfile

    for path in glob.glob(os.path.join(tempfile.gettempdir(), "conda-env-sync-*.tar.gz")):
        try:
            os.unlink(path)
        except OSError:
            pass


def build_token_chart(points: list[dict]) -> str:
    if not points:
        return ""
    width = 760
    height = 220
    pad = 28
    totals = [max(0, int(point["total_tokens"])) for point in points]
    max_total = max(totals) or 1
    if len(points) == 1:
        coords = [(width // 2, height - pad - int((totals[0] / max_total) * (height - 2 * pad)))]
    else:
        coords = []
        for index, total in enumerate(totals):
            x = pad + int(index * ((width - 2 * pad) / (len(points) - 1)))
            y = height - pad - int((total / max_total) * (height - 2 * pad))
            coords.append((x, y))
    polyline = " ".join(f"{x},{y}" for x, y in coords)
    circles = "\n".join(f'<circle cx="{x}" cy="{y}" r="3"></circle>' for x, y in coords)
    return f"""
    <svg viewBox="0 0 {width} {height}" role="img" aria-label="Token usage over time">
      <line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}"></line>
      <line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height-pad}"></line>
      <polyline points="{polyline}"></polyline>
      {circles}
    </svg>
    """


def build_capability_summary(accounts: list, overlays: list[dict]) -> list[dict]:
    by_capability: dict[str, dict] = {}

    def item_for(capability: str) -> dict:
        item = by_capability.setdefault(
            capability,
            {
                "capability": capability,
                "accounts": [],
                "profiles": [],
                "sources": [],
                "details": [],
            },
        )
        return item

    for account in accounts:
        account_profiles = sorted((account.env_profiles or {}).keys())
        for capability in sorted(account.capabilities or []):
            item = item_for(capability)
            if account.name not in item["accounts"]:
                item["accounts"].append(account.name)
            for profile in matching_profiles_for_capability(capability, account_profiles):
                if profile not in item["profiles"]:
                    item["profiles"].append(profile)
            if "accounts.yaml" not in item["sources"]:
                item["sources"].append("accounts.yaml")
            item["details"].append(
                {
                    "account": account.name,
                    "profiles": matching_profiles_for_capability(capability, account_profiles),
                    "source": "accounts.yaml",
                    "condition": "account declares capability",
                }
            )

    for overlay in overlays:
        capability = str(overlay.get("capability") or "").strip()
        account_name = str(overlay.get("account_name") or "").strip()
        profile = str(overlay.get("env_profile") or "").strip()
        if not capability or not account_name:
            continue
        item = item_for(capability)
        if account_name not in item["accounts"]:
            item["accounts"].append(account_name)
        if profile and profile not in item["profiles"]:
            item["profiles"].append(profile)
        if "conda-sync-overlay" not in item["sources"]:
            item["sources"].append("conda-sync-overlay")
        item["details"].append(
            {
                "account": account_name,
                "profiles": [profile] if profile else [],
                "source": "conda-sync-overlay",
                "condition": f"synced env {overlay.get('env_name') or ''}".strip(),
                "installed_prefix": overlay.get("installed_prefix") or "",
            }
        )

    for item in by_capability.values():
        item["accounts"] = sorted(item["accounts"])
        item["profiles"] = sorted(item["profiles"])
        item["sources"] = sorted(item["sources"])
        item["details"] = sorted(item["details"], key=lambda detail: (detail["account"], detail["source"]))
    return sorted(by_capability.values(), key=lambda item: item["capability"])


def matching_profiles_for_capability(capability: str, profiles: list[str]) -> list[str]:
    if not capability:
        return []
    candidates = {capability}
    if capability.startswith("conda:"):
        candidates.add(capability.split(":", 1)[1])
    normalized = capability.replace("-", "_")
    candidates.add(normalized)
    if normalized.startswith("conda:"):
        candidates.add(normalized.split(":", 1)[1])
    return [profile for profile in profiles if profile in candidates]


def parse_memory_mb(value: str) -> int:
    raw = (value or "").strip().lower()
    if not raw:
        return 4096
    try:
        if raw.endswith("gb") or raw.endswith("g"):
            return int(float(raw.rstrip("gb")) * 1024)
        if raw.endswith("mb") or raw.endswith("m"):
            return int(float(raw.rstrip("mb")))
        return int(float(raw))
    except ValueError:
        return 4096


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def attach_task_elapsed(tasks: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    hydrated: list[dict] = []
    for task in tasks:
        item = dict(task)
        start = parse_timestamp(item.get("started_at") or item.get("attached_at") or item.get("created_at"))
        if item.get("status") in {"completed", "failed", "cancelled"}:
            end = parse_timestamp(item.get("finished_at")) or now
        else:
            end = now
        item["elapsed_text"] = format_elapsed((end - start).total_seconds()) if start else ""
        item["log_path"] = item.get("stdout_path") or item.get("stderr_path") or ""
        hydrated.append(item)
    return hydrated


def job_elapsed(jobs: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    hydrated: list[dict] = []
    for job in jobs:
        item = dict(job)
        start = parse_timestamp(item.get("submitted_at") or item.get("created_at"))
        end = parse_timestamp(item.get("finished_at")) or now
        item["elapsed_text"] = format_elapsed((end - start).total_seconds()) if start else ""
        hydrated.append(item)
    return hydrated


def allocation_elapsed(allocations: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    hydrated: list[dict] = []
    for allocation in allocations:
        item = dict(allocation)
        end = parse_timestamp(item.get("closed_at")) or now
        requested_at = parse_timestamp(item.get("submitted_at") or item.get("created_at"))
        started_at = parse_timestamp(item.get("started_at"))
        if not started_at and item.get("state") in {"warm", "active", "draining", "closing"}:
            started_at = parse_timestamp(item.get("created_at"))
        request_elapsed = format_elapsed((end - requested_at).total_seconds()) if requested_at else ""
        held_elapsed = format_elapsed((end - started_at).total_seconds()) if started_at else ""
        item["request_elapsed_text"] = request_elapsed
        item["held_elapsed_text"] = held_elapsed
        item["is_warm_pool"] = (
            item.get("state") == "warm"
            or "warm pool" in str(item.get("drain_reason") or "").lower()
        )
        hydrated.append(item)
    return hydrated


def allocation_sort_key(allocation: dict) -> tuple[int, int]:
    state_rank = {
        "active": 0,
        "warm": 1,
        "pending": 2,
        "draining": 3,
        "closing": 4,
        "failed": 5,
    }
    return (state_rank.get(str(allocation.get("state") or ""), 9), -int(allocation.get("id") or 0))


def allocation_usage_summary(allocations: list[dict]) -> dict[str, int]:
    total_cpus = sum(int(item.get("total_cpus") or 0) for item in allocations)
    free_cpus = sum(int(item.get("free_cpus") or 0) for item in allocations)
    total_gpus = sum(int(item.get("total_gpus") or 0) for item in allocations)
    free_gpus = sum(int(item.get("free_gpus") or 0) for item in allocations)
    total_memory_mb = sum(int(item.get("total_memory_mb") or 0) for item in allocations)
    free_memory_mb = sum(int(item.get("free_memory_mb") or 0) for item in allocations)
    return {
        "cpu_used": max(0, total_cpus - free_cpus),
        "cpu_total": total_cpus,
        "gpu_used": max(0, total_gpus - free_gpus),
        "gpu_total": total_gpus,
        "memory_used_mb": max(0, total_memory_mb - free_memory_mb),
        "memory_total_mb": total_memory_mb,
        "memory_used_gb": round(max(0, total_memory_mb - free_memory_mb) / 1024),
        "memory_total_gb": round(total_memory_mb / 1024),
    }


def task_activity_summary(tasks: list[dict]) -> dict[str, int]:
    summary = {
        "total": 0,
        "running": 0,
        "attaching": 0,
        "queued": 0,
        "fea": 0,
        "fea_running": 0,
        "standard": 0,
        "gpu": 0,
        "cpu": 0,
        "same_node": 0,
    }
    for task in tasks:
        status = str(task.get("status") or "")
        if status not in {"running", "attaching", "queued"}:
            continue
        summary["total"] += 1
        if status in summary:
            summary[status] += 1
        if normalize_scheduling_profile(str(task.get("scheduling_profile") or "")) == SchedulingProfile.FEA_BURSTY.value:
            summary["fea"] += 1
            if status == "running":
                summary["fea_running"] += 1
        else:
            summary["standard"] += 1
        if int(task.get("gpus") or 0) > 0:
            summary["gpu"] += 1
        else:
            summary["cpu"] += 1
        if int(task.get("same_node_as_task_id") or 0) > 0:
            summary["same_node"] += 1
    return summary


def task_state_for_api(status: str) -> str:
    return "succeeded" if status == "completed" else status


def task_display_sort_key(task: dict) -> tuple[int, int]:
    rank = {
        "running": 0,
        "attaching": 1,
        "queued": 2,
    }.get(str(task.get("status") or ""), 9)
    return (rank, -int(task.get("id") or 0))


def task_result_urls(task_id: int) -> dict[str, str]:
    return {
        "status": f"/api/tasks/{task_id}",
        "stdout": f"/api/tasks/{task_id}/stdout",
        "stderr": f"/api/tasks/{task_id}/stderr",
        "remote_file": f"/api/tasks/{task_id}/remote-file",
    }


def last_json_object(text: str) -> object | None:
    decoder = json.JSONDecoder()
    for index in range(len(text) - 1, -1, -1):
        if text[index] not in "[{":
            continue
        try:
            parsed, end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if text[index + end :].strip():
            continue
        return parsed
    return None


def create_app(config_path: str = "config/app.yaml") -> FastAPI:
    config = load_app_config(config_path)
    accounts = load_accounts(config.accounts_path)
    db = Database(config.database_path)
    db.init()
    client_factory = lambda account: SlurmAccountClient(
        account,
        config.git_credentials,
        accounts,
        command_timeout=config.ssh_command_timeout_seconds,
        slow_command_timeout=config.ssh_slow_command_timeout_seconds,
    )
    scheduler = Scheduler(
        db,
        accounts,
        config.poll_interval_seconds,
        client_factory=client_factory,
        cluster_refresh_interval_seconds=config.cluster_refresh_interval_seconds,
        min_warm_allocations=config.min_warm_allocations,
        allocation_partition=config.allocation_partition,
        allocation_cpus=config.allocation_cpus,
        allocation_memory=config.allocation_memory,
        allocation_time_limit=config.allocation_time_limit,
        allocation_scale_out_usage_threshold=config.allocation_scale_out_usage_threshold,
        allocation_scale_in_idle_seconds=config.allocation_scale_in_idle_seconds,
        allocation_drain_after_seconds=config.allocation_drain_after_seconds,
        allocation_attach_stop_before_drain_seconds=config.allocation_attach_stop_before_drain_seconds,
        allocation_force_cancel_after_seconds=config.allocation_force_cancel_after_seconds,
        allocation_pending_timeout_seconds=config.allocation_pending_timeout_seconds,
        allocation_pending_backoff_seconds=config.allocation_pending_backoff_seconds,
        allocation_reserved_job_slots=config.allocation_reserved_job_slots,
        allocation_max_new_per_loop=config.allocation_max_new_per_loop,
        cpu_pool_allow_gpu_partitions=config.cpu_pool_allow_gpu_partitions,
        warm_pool_preferred_accounts=config.warm_pool_preferred_accounts,
        gpu_warm_pool_preferred_accounts=config.gpu_warm_pool_preferred_accounts,
        single_job_per_node_partitions=config.single_job_per_node_partitions,
        cpu_partition_allocation_limits=config.cpu_partition_allocation_limits,
        gpu_cpu_reserve=config.gpu_cpu_reserve,
        gpu_prewarm_enabled=config.gpu_prewarm_enabled,
        gpu_prewarm_preferred_models=config.gpu_prewarm_preferred_models,
        gpu_prewarm_min_warm_allocations=config.gpu_prewarm_min_warm_allocations,
        gpu_prewarm_max_warm_allocations=config.gpu_prewarm_max_warm_allocations,
        gpu_prewarm_gpus_per_allocation=config.gpu_prewarm_gpus_per_allocation,
        gpu_prewarm_min_gpus_per_allocation=config.gpu_prewarm_min_gpus_per_allocation,
        gpu_prewarm_cpus_per_allocation=config.gpu_prewarm_cpus_per_allocation,
        gpu_prewarm_cpu_reserve_per_free_gpu=config.gpu_prewarm_cpu_reserve_per_free_gpu,
        gpu_prewarm_stagger_seconds=config.gpu_prewarm_stagger_seconds,
        gpu_prewarm_memory=config.gpu_prewarm_memory,
        gpu_prewarm_partition=config.gpu_prewarm_partition,
        gpu_prewarm_time_limit=config.gpu_prewarm_time_limit,
        gpu_prewarm_pinned_pending_timeout_seconds=config.gpu_prewarm_pinned_pending_timeout_seconds,
        fea_soft_memory_free_percent=config.fea_soft_memory_free_percent,
        fea_hard_memory_free_percent=config.fea_hard_memory_free_percent,
        fea_load_target=config.fea_load_target,
        fea_max_attach_per_loop=config.fea_max_attach_per_loop,
        fea_node_name_policy=config.fea_node_name_policy,
        fea_overload_scale_out_load_factor=config.fea_overload_scale_out_load_factor,
        fea_overload_scale_out_seconds=config.fea_overload_scale_out_seconds,
        fea_pressure_max_attempts=config.fea_pressure_max_attempts,
        fea_max_attach_per_node_per_loop=config.fea_max_attach_per_node_per_loop,
        cleanup_enabled=config.cleanup_enabled,
        cleanup_interval_seconds=config.cleanup_interval_seconds,
        cleanup_finished_task_ttl_seconds=config.cleanup_finished_task_ttl_seconds,
        cleanup_finished_job_ttl_seconds=config.cleanup_finished_job_ttl_seconds,
        cleanup_closed_allocation_ttl_seconds=config.cleanup_closed_allocation_ttl_seconds,
        cleanup_orphan_sweep_enabled=config.cleanup_orphan_sweep_enabled,
        cleanup_orphan_sweep_interval_seconds=config.cleanup_orphan_sweep_interval_seconds,
        cleanup_orphan_min_age_seconds=config.cleanup_orphan_min_age_seconds,
        cleanup_db_row_ttl_seconds=config.cleanup_db_row_ttl_seconds,
        cleanup_event_ttl_seconds=config.cleanup_event_ttl_seconds,
        watchdog_enabled=config.scheduler_watchdog_enabled,
        watchdog_stall_seconds=config.scheduler_watchdog_stall_seconds,
        ssh_parallelism=config.scheduler_ssh_parallelism,
    )

    app = FastAPI(title="Slurm Scheduler")
    app.state.config = config
    app.state.db = db
    app.state.scheduler = scheduler
    env_sync_manager = CondaEnvSyncManager(db, accounts)
    app.state.env_sync_manager = env_sync_manager
    remote_read_limit = max(1, int(config.web_remote_read_concurrency or 1))
    remote_read_semaphore = threading.BoundedSemaphore(remote_read_limit)
    remote_read_cache_seconds = max(0, int(config.web_remote_read_cache_seconds or 0))
    remote_read_cache: dict[tuple, tuple[float, str]] = {}
    remote_read_cache_lock = threading.Lock()

    def remote_command_timeout() -> int | None:
        timeout = int(config.web_remote_command_timeout_seconds or 0)
        return timeout if timeout > 0 else None

    def remote_read(cache_key: tuple, reader) -> str:
        if remote_read_cache_seconds > 0:
            now = time.monotonic()
            with remote_read_cache_lock:
                cached = remote_read_cache.get(cache_key)
                if cached and now - cached[0] <= remote_read_cache_seconds:
                    return cached[1]
        if not remote_read_semaphore.acquire(blocking=False):
            raise HTTPException(
                status_code=429,
                detail=f"remote log/file reads are busy; retry shortly (limit {remote_read_limit})",
            )
        try:
            text = reader()
        finally:
            remote_read_semaphore.release()
        if remote_read_cache_seconds > 0:
            with remote_read_cache_lock:
                remote_read_cache[cache_key] = (time.monotonic(), text)
                if len(remote_read_cache) > 128:
                    oldest = sorted(remote_read_cache, key=lambda key: remote_read_cache[key][0])[:32]
                    for key in oldest:
                        remote_read_cache.pop(key, None)
        return text

    def bounded_remote_window(tail_lines: int = 0, max_bytes: int = 0, default_max_bytes: int | None = None) -> tuple[int, int]:
        tail = max(0, int(tail_lines or 0))
        requested = max(0, int(max_bytes or 0))
        if requested <= 0:
            requested = int(
                config.web_remote_file_default_max_bytes
                if default_max_bytes is None
                else default_max_bytes
            )
        hard = max(0, int(config.web_remote_file_hard_max_bytes or 0))
        if hard > 0 and (requested <= 0 or requested > hard):
            requested = hard
        return tail, max(0, requested)

    def apply_text_window(text: str, tail_lines: int = 0, max_bytes: int = 0) -> str:
        if tail_lines > 0:
            text = "\n".join(text.splitlines()[-tail_lines:])
            if text:
                text += "\n"
        if max_bytes > 0:
            data = text.encode("utf-8", errors="replace")
            if len(data) > max_bytes:
                text = data[-max_bytes:].decode("utf-8", errors="replace")
        return text

    def read_task_file_text(task: dict, field: str, limit: int = 65536, tail_lines: int = 0, max_bytes: int = 0) -> str:
        path = task.get(field) or ""
        if not path or not task.get("account_name"):
            return ""
        account = next((item for item in accounts if item.name == task["account_name"]), None)
        if not account:
            return ""
        try:
            tail, byte_limit = bounded_remote_window(tail_lines=tail_lines, max_bytes=max_bytes, default_max_bytes=limit)
            text = remote_read(
                ("task-file", task.get("id"), field, path, tail, byte_limit),
                lambda: SlurmAccountClient(account).read_text_file(
                    path,
                    tail_lines=tail,
                    max_bytes=byte_limit,
                    timeout=remote_command_timeout(),
                ),
            )
        except Exception:
            return ""
        if limit > 0 and not max_bytes and not tail_lines and len(text) > limit:
            text = text[-limit:]
        return apply_text_window(text, tail_lines=tail_lines, max_bytes=max_bytes)

    def account_for_task(task: dict):
        return next((item for item in accounts if item.name == task.get("account_name")), None)

    def task_remote_root(task: dict, account, base: str) -> str:
        if base == "remote_dir":
            return task.get("remote_dir") or ""
        if base == "git_workdir":
            return posixpath.join(account.remote_workspace, "git_tasks", f"task-{task['id']}")
        if base == "git_repo":
            return posixpath.join(account.remote_workspace, "git_tasks", f"task-{task['id']}", "repo")
        if base == "stdout":
            return posixpath.dirname(task.get("stdout_path") or "")
        if base == "stderr":
            return posixpath.dirname(task.get("stderr_path") or "")
        return (task.get("remote_cwd") or task.get("remote_dir") or "").replace(
            ACCOUNT_WORKSPACE_PLACEHOLDER,
            account.remote_workspace,
        )

    def task_failure_message(task: dict) -> str:
        existing = task.get("failure_message") or ""
        if existing:
            return existing
        if task.get("status") != "failed":
            return ""
        stderr = read_task_file_text(task, "stderr_path", limit=8192, tail_lines=3)
        return "\n".join(line for line in stderr.splitlines()[:3]).strip()

    def task_view_fields(
        task: dict,
        allocation_rows: list[dict] | None = None,
        allocation_by_id: dict[int, dict] | None = None,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
        include_diagnostics: bool = True,
    ) -> dict:
        allocation = None
        if task.get("allocation_id"):
            allocation_id = int(task["allocation_id"])
            allocation = allocation_by_id.get(allocation_id) if allocation_by_id is not None else db.get_allocation(allocation_id)
        requested_node_name = task.get("node_name") or ""
        allocation_node_name = allocation.get("node_name") if allocation else ""
        same_node_as_task_id = int(task.get("same_node_as_task_id") or task.get("same_node_as") or 0)
        same_node_target = scheduler.same_node_target_for_task(task) if same_node_as_task_id else None
        preferred_node_relaxed = (
            scheduler.task_can_relax_preferred_node(task)
            and bool(requested_node_name)
            and bool(allocation_node_name)
            and requested_node_name != allocation_node_name
        )
        if include_diagnostics and task.get("status") == "queued":
            queue_fields = scheduler.task_queue_diagnostics(
                task,
                allocation_rows=allocation_rows,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            )
        else:
            queue_fields = {
                "ready_fit_slots": 0,
                "pending_fit_slots": 0,
                "inflight_fit_slots": 0,
                "queue_state": task.get("status") or "",
                "queue_reason": "",
                "preferred_node_relaxed": preferred_node_relaxed,
            }
        return {
            **queue_fields,
            "cpus": int(task.get("cpus") or 1),
            "memory_mb": int(task.get("memory_mb") or 4096),
            "gpus": int(task.get("gpus") or 0),
            "gpu_model": task.get("gpu_model") or "",
            "partition": task.get("partition") or "auto",
            "node_name": requested_node_name,
            "requested_node_name": requested_node_name,
            "allocation_node_name": allocation_node_name,
            "actual_node_name": allocation_node_name,
            "same_node_as_task_id": same_node_as_task_id,
            "same_node_as_node_name": same_node_target.get("node_name") if same_node_target else "",
            "same_node_as_allocation_id": same_node_target.get("allocation_id") if same_node_target else None,
            "env_profile": task.get("env_profile") or "",
        }

    def task_json(
        task: dict,
        include_output: bool = False,
        output_limit: int = 65536,
        derive_failure_message: bool = True,
        allocation_rows: list[dict] | None = None,
        allocation_by_id: dict[int, dict] | None = None,
        active_task_allocation_ids: set[int] | None = None,
        active_exclusive_allocation_ids: set[int] | None = None,
        include_diagnostics: bool = False,
    ) -> dict:
        allocation = None
        if task.get("allocation_id"):
            allocation_id = int(task["allocation_id"])
            allocation = allocation_by_id.get(allocation_id) if allocation_by_id is not None else db.get_allocation(allocation_id)
        payload = {
            "task_id": task["id"],
            "id": task["id"],
            "name": task["name"],
            "status": task["status"],
            "state": task_state_for_api(task["status"]),
            "exit_code": task.get("exit_code"),
            "failure_message": task_failure_message(task) if derive_failure_message else (task.get("failure_message") or ""),
            "created_at": task.get("created_at"),
            "attached_at": task.get("attached_at"),
            "started_at": task.get("started_at"),
            "finished_at": task.get("finished_at"),
            "assigned_allocation": task.get("allocation_id"),
            "allocation_id": task.get("allocation_id"),
            "account_name": task.get("account_name") or "",
            "slurm_job_id": allocation.get("slurm_job_id") if allocation else "",
            "remote_cwd": task.get("remote_cwd") or "",
            "remote_dir": task.get("remote_dir") or "",
            "stdout_path": task.get("stdout_path") or "",
            "stderr_path": task.get("stderr_path") or "",
            "exit_code_path": task.get("exit_code_path") or "",
            "required_capability": task.get("required_capability") or "",
            "env_profile": task.get("env_profile") or "",
            "scheduling_profile": normalize_scheduling_profile(task.get("scheduling_profile") or ""),
            "priority": int(task.get("priority") or 0),
            "timeout_seconds": int(task.get("timeout_seconds") or 0),
            "dedupe_key": task.get("dedupe_key") or "",
            "max_workers_per_node": int(task.get("max_workers_per_node") or 0),
            "same_node_as_task_id": int(task.get("same_node_as_task_id") or 0),
            "urls": task_result_urls(int(task["id"])),
        }
        payload.update(
            task_view_fields(
                task,
                allocation_rows=allocation_rows,
                allocation_by_id=allocation_by_id,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                include_diagnostics=include_diagnostics,
            )
        )
        if include_output:
            stdout = read_task_file_text(task, "stdout_path", output_limit)
            payload["stdout"] = stdout
            payload["stderr"] = read_task_file_text(task, "stderr_path", output_limit)
            payload["result_json"] = last_json_object(stdout)
        return payload

    def parse_account_list(value: str | list[str]) -> list[str]:
        raw = ",".join(value) if isinstance(value, list) else value
        return [item.strip() for item in re.split(r"[\s,;/|]+", raw or "") if item.strip()]

    def env_sync_job_json(job: dict, include_targets: bool = True) -> dict:
        try:
            target_accounts = json.loads(job.get("target_accounts") or "[]")
        except json.JSONDecodeError:
            target_accounts = []
        payload = {
            "id": int(job["id"]),
            "status": job.get("status") or "",
            "reference_account": job.get("reference_account") or "",
            "source_env_name": job.get("source_env_name") or "",
            "target_env_name": job.get("target_env_name") or "",
            "target_accounts": target_accounts,
            "failure_message": job.get("failure_message") or "",
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
        }
        if include_targets:
            payload["targets"] = db.list_env_sync_targets(int(job["id"]))
        return payload

    def task_submission_payload(task: dict) -> dict:
        raw_payload_json = task.get("payload_json") or ""
        if raw_payload_json:
            try:
                payload_json = json.loads(raw_payload_json)
            except json.JSONDecodeError:
                payload_json = raw_payload_json
        else:
            payload_json = ""
        return {
            "name": task.get("name") or "remote-task",
            "remote_cwd": task.get("remote_cwd") or "",
            "command": task.get("command") or "",
            "env_setup": task.get("env_setup") or "",
            "required_capability": task.get("required_capability") or "",
            "env_profile": task.get("env_profile") or "",
            "account_name": task.get("account_name") or "",
            "cpus": int(task.get("cpus") or 1),
            "memory_mb": int(task.get("memory_mb") or 4096),
            "scheduling_profile": normalize_scheduling_profile(task.get("scheduling_profile") or ""),
            "gpus": int(task.get("gpus") or 0),
            "gpu_model": task.get("gpu_model") or "",
            "partition": task.get("partition") or "auto",
            "node_name": task.get("node_name") or "",
            "exclusive_node": bool(task.get("exclusive_node") or False),
            "priority": int(task.get("priority") or 0),
            "timeout_seconds": int(task.get("timeout_seconds") or 0),
            "dedupe_key": task.get("dedupe_key") or "",
            "max_workers_per_node": int(task.get("max_workers_per_node") or 0),
            "same_node_as_task_id": int(task.get("same_node_as_task_id") or 0),
            "payload_json": payload_json,
        }

    def task_submission_examples(task: dict) -> dict:
        payload = task_submission_payload(task)
        pretty_json = json.dumps(payload, ensure_ascii=False, indent=2)
        compact_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return {
            "json": pretty_json,
            "curl": f"curl -sS -X POST \"$SCHEDULER_URL/api/tasks\" -H 'Content-Type: application/json' --data {shlex.quote(compact_json)}",
            "python": (
                "import requests\n\n"
                "base = \"http://100.112.168.31:8000\"\n"
                f"payload = {pretty_json}\n"
                "response = requests.post(f\"{base}/api/tasks\", json=payload, timeout=10)\n"
                "response.raise_for_status()\n"
                "print(response.json())\n"
            ),
        }

    def git_task_fields(
        repo_url: str,
        git_ref: str,
        entrypoint: str,
        arguments: str,
        git_credential_id: str = "",
    ) -> tuple[str, str]:
        credential = find_git_credential(config.git_credentials, repo_url, git_credential_id)
        clone_url = credential.clone_url if credential and credential.clone_url else repo_url
        payload_json = git_task_payload(repo_url, git_ref, entrypoint, arguments, credential)
        return build_git_task_command(clone_url, git_ref, entrypoint, arguments), payload_json

    def maybe_assign_same_node_task(task_id: int) -> None:
        task = db.get_task(task_id)
        if not task or not int(task.get("same_node_as_task_id") or task.get("same_node_as") or 0):
            return
        scheduler.fail_stale_same_node_tasks()
        task = db.get_task(task_id)
        if task:
            scheduler.assign_queued_task(task)

    def create_task_record(payload: dict) -> tuple[int, bool]:
        dedupe_key = str(payload.get("dedupe_key") or "").strip()
        if dedupe_key:
            existing = db.find_active_task_by_dedupe_key(dedupe_key)
            if existing:
                return int(existing["id"]), True
        raw_payload = payload.get("payload_json", "")
        if isinstance(raw_payload, (dict, list)):
            payload_json = json.dumps(raw_payload, ensure_ascii=False, separators=(",", ":"))
        else:
            payload_json = str(raw_payload or "")
        task = TaskCreate(
            name=str(payload.get("name") or "remote-task"),
            remote_cwd=str(payload.get("remote_cwd") or ""),
            command=str(payload.get("command") or ""),
            env_setup=str(payload.get("env_setup") or ""),
            required_capability=str(payload.get("required_capability") or ""),
            env_profile=str(payload.get("env_profile") or ""),
            account_name=str(payload.get("account_name") or ""),
            cpus=max(1, int(payload.get("cpus") or 1)),
            memory_mb=max(1, int(payload.get("memory_mb") or 4096)),
            scheduling_profile=normalize_scheduling_profile(str(payload.get("scheduling_profile") or "")),
            gpus=max(0, int(payload.get("gpus") or 0)),
            gpu_model=str(payload.get("gpu_model") or ""),
            partition=str(payload.get("partition") or "auto"),
            node_name=str(payload.get("node_name") or ""),
            exclusive_node=bool(payload.get("exclusive_node") or False),
            priority=int(payload.get("priority") or 0),
            timeout_seconds=max(0, int(payload.get("timeout_seconds") or payload.get("timeout") or 0)),
            dedupe_key=dedupe_key,
            max_workers_per_node=max(0, int(payload.get("max_workers_per_node") or 0)),
            same_node_as_task_id=max(0, int(payload.get("same_node_as_task_id") or payload.get("same_node_as") or 0)),
            payload_json=payload_json,
        )
        if not task.remote_cwd:
            raise HTTPException(status_code=422, detail="remote_cwd is required")
        if not task.command:
            raise HTTPException(status_code=422, detail="command is required")
        task_id = db.create_task(task)
        if task.same_node_as_task_id:
            maybe_assign_same_node_task(task_id)
        return task_id, False

    @app.on_event("startup")
    def _startup() -> None:
        cleanup_local_temp_artifacts()
        scheduler.start()

    @app.on_event("shutdown")
    def _shutdown() -> None:
        scheduler.stop()

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request) -> HTMLResponse:
        attached_task_name_filter = (request.query_params.get("task_name_contains") or "").strip()
        snapshots = scheduler.cached_snapshots()
        snapshot_error = "" if snapshots else "Account status will appear after the background scheduler refreshes."
        allocations = annotate_allocation_fea_pressure(
            annotate_allocation_node_metrics(db.list_allocations_with_live(limit=500), db.list_pestat_nodes()),
            scheduler.fea_owned_node_pressures(),
        )
        node_fea_worker_counts = scheduler.node_fea_worker_counts()
        for allocation in allocations:
            allocation["node_fea_worker_count"] = node_fea_worker_counts.get(str(allocation.get("node_name") or ""), 0)
        allocation_by_id = {int(allocation["id"]): allocation for allocation in allocations}
        active_task_allocation_ids, active_exclusive_allocation_ids = scheduler.active_task_allocation_sets()
        terminal_allocation_states = {"closed", "failed"}
        active_allocations = allocation_elapsed(sorted(
            [item for item in allocations if item["state"] not in terminal_allocation_states],
            key=allocation_sort_key,
        ))
        allocated_summary_rows = [
            item
            for item in active_allocations
            if item["state"] in {"active", "warm", "draining", "closing"}
        ]
        allocation_summary = allocation_usage_summary(allocated_summary_rows)
        closed_allocations = [item for item in allocations if item["state"] in terminal_allocation_states]
        active_running_rows = db.list_tasks_by_statuses(
            ["running", "attaching"],
            limit=5000,
            name_contains=attached_task_name_filter,
        )
        visible_queued_rows = db.list_tasks_by_statuses(
            ["queued"],
            limit=50,
            name_contains=attached_task_name_filter,
        )
        summary_queued_rows = db.list_tasks_by_statuses(
            ["queued"],
            limit=5000,
            name_contains=attached_task_name_filter,
        )
        task_summary = task_activity_summary(active_running_rows + summary_queued_rows)
        active_task_rows = {
            int(task["id"]): task
            for task in (active_running_rows + visible_queued_rows)
        }
        queued_diagnostics_remaining = 5
        active_task_items = []
        for task in attach_task_elapsed(list(active_task_rows.values())):
            include_diagnostics = False
            if task.get("status") == "queued" and queued_diagnostics_remaining > 0:
                include_diagnostics = True
                queued_diagnostics_remaining -= 1
            active_task_items.append(
                {
                    **task,
                    **task_view_fields(
                        task,
                        allocation_rows=allocations,
                        allocation_by_id=allocation_by_id,
                        active_task_allocation_ids=active_task_allocation_ids,
                        active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                        include_diagnostics=include_diagnostics,
                    ),
                }
            )
        active_tasks = sorted(
            active_task_items,
            key=task_display_sort_key,
        )
        terminal_task_statuses = ["completed", "failed", "cancelled"]
        finished_tasks = [
            {
                **task,
                **task_view_fields(
                    task,
                    allocation_rows=allocations,
                    allocation_by_id=allocation_by_id,
                    active_task_allocation_ids=active_task_allocation_ids,
                    active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                    include_diagnostics=False,
                ),
            }
            for task in attach_task_elapsed(
                db.list_tasks_by_statuses(
                    terminal_task_statuses,
                    limit=50,
                    name_contains=attached_task_name_filter,
                )
            )
        ]
        finished_task_count = db.count_tasks_by_statuses(
            terminal_task_statuses,
            name_contains=attached_task_name_filter,
        )
        jobs = job_elapsed(db.list_jobs())
        active_jobs = [item for item in jobs if item["status"] not in {"completed", "failed", "cancelled"}]
        finished_jobs = [item for item in jobs if item["status"] in {"completed", "failed", "cancelled"}]
        env_overlays = db.list_account_env_overlays()
        capabilities = build_capability_summary(accounts, env_overlays)
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "jobs": active_jobs,
                "finished_jobs": finished_jobs[:50],
                "finished_job_count": len(finished_jobs),
                "tasks": active_tasks,
                "task_summary": task_summary,
                "finished_tasks": finished_tasks,
                "finished_task_count": finished_task_count,
                "attached_task_name_filter": attached_task_name_filter,
                "allocations": active_allocations,
                "allocation_summary": allocation_summary,
                "gpu_prewarm_enabled": scheduler.gpu_prewarm_enabled,
                "closed_allocations": closed_allocations[:20],
                "closed_allocation_count": len(closed_allocations),
                "snapshots": snapshots,
                "snapshot_error": snapshot_error,
                "token_usage": db.list_token_usage(),
                "token_summary": db.token_usage_summary(),
                "token_chart": build_token_chart(db.list_token_usage()),
                "cpu_partitions": partition_rank(db.list_node_inventory(), needs_gpu=False),
                "gpu_partitions": partition_rank(db.list_node_inventory(), needs_gpu=True),
                "gpu_capacity": scheduler.gpu_capacity_summary(),
                "account_names": [account.name for account in accounts],
                "capabilities": capabilities,
                "env_sync_jobs": [env_sync_job_json(job) for job in db.list_env_sync_jobs(limit=10)],
                "env_overlays": env_overlays,
                "scheduler_events": db.list_events(limit=50),
                "scheduler_health": scheduler.health_status(),
            },
        )

    @app.post("/conda-env-sync")
    def create_conda_env_sync_form(
        reference_account: str = Form(...),
        source_env_name: str = Form(...),
        target_accounts: str = Form(...),
    ) -> Response:
        try:
            env_sync_manager.start(reference_account, source_env_name, parse_account_list(target_accounts))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return RedirectResponse("/", status_code=303)

    @app.post("/jobs")
    def create_job(
        job_mode: str = Form("python_git"),
        repo_url: str = Form(""),
        git_ref: str = Form("main"),
        entrypoint: str = Form(...),
        arguments: str = Form(""),
        git_credential_id: str = Form(""),
        env_setup: str = Form(""),
        required_capability: str = Form(""),
        env_profile: str = Form(""),
        account_name: str = Form(""),
        partition: str = Form("auto"),
        time_limit: str = Form("01:00:00"),
        cpus: int = Form(1),
        memory: str = Form("4G"),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
        same_node_as_task_id: int = Form(0),
        job_name: str = Form("web-job"),
        remote_path: str = Form(""),
        total_simulations: int = Form(1),
        simulations_per_job: int = Form(1),
        cpus_per_simulation: int = Form(1),
        mem_per_simulation_gb: float = Form(1.0),
        max_workers_per_job: int = Form(32),
        max_new_jobs: int = Form(10),
        oversubscribe_factor: float = Form(1.5),
        load_target: float = Form(0.75),
        ramp_interval_seconds: int = Form(900),
    ) -> Response:
        if job_mode == "dynamic_packed_srun":
            nodes = [
                PestatNode(
                    hostname=row["hostname"],
                    partition=row["partition"],
                    state=row["state"],
                    cpu_used=row["cpu_used"],
                    cpu_total=row["cpu_total"],
                    cpu_load=row["cpu_load"],
                    memory_mb=row["memory_mb"],
                    free_memory_mb=row["free_memory_mb"],
                )
                for row in db.list_pestat_nodes()
            ]
            plans = plan_dynamic_allocations(
                nodes=nodes,
                total_simulations=total_simulations,
                cpus_per_simulation=cpus_per_simulation,
                mem_per_simulation_gb=mem_per_simulation_gb,
                max_workers_per_allocation=max_workers_per_job,
                max_allocations=max_new_jobs,
                partition=partition,
                oversubscribe_factor=oversubscribe_factor,
            )
            if not plans:
                plans = []
                per_job = max(1, simulations_per_job)
                for batch_index in range(max_new_jobs):
                    start = batch_index * per_job + 1
                    if start > total_simulations:
                        break
                    count = min(per_job, total_simulations - batch_index * per_job)
                    plans.append(
                        type(
                            "FallbackPlan",
                            (),
                            {
                                "partition": partition,
                                "node_name": "",
                                "workers": count,
                                "initial_workers": min(count, max(1, count)),
                                "cpus_per_worker": cpus_per_simulation,
                                "total_cpus": count * cpus_per_simulation,
                                "simulation_start": start,
                                "simulation_count": count,
                            },
                        )()
                    )
            for plan in plans:
                db.create_job(
                    JobCreate(
                        repo_url="",
                        git_ref="",
                        entrypoint=entrypoint,
                        arguments=arguments,
                        env_setup=env_setup,
                        required_capability=required_capability,
                        env_profile=env_profile,
                        account_name=account_name,
                        partition=plan.partition,
                        time_limit=time_limit,
                        cpus=plan.total_cpus,
                        memory=memory,
                        gpus=gpus,
                        gpu_model=gpu_model,
                        job_name=f"{job_name}-{plan.simulation_start}-{plan.simulation_start + plan.simulation_count - 1}",
                        job_mode="packed_srun",
                        remote_path=remote_path,
                        simulations_per_job=plan.workers,
                        cpus_per_simulation=plan.cpus_per_worker,
                        simulation_start=plan.simulation_start,
                        simulation_count=plan.simulation_count,
                        node_name=node_name or plan.node_name,
                        exclusive_node=exclusive_node,
                        mem_per_simulation_gb=mem_per_simulation_gb,
                        max_workers_per_job=max_workers_per_job,
                        initial_workers=plan.initial_workers,
                        load_target=load_target,
                        ramp_interval_seconds=ramp_interval_seconds,
                    )
                )
        elif job_mode == "packed_srun":
            per_job = max(1, simulations_per_job)
            total = max(1, total_simulations)
            for batch_index in range(ceil(total / per_job)):
                start = batch_index * per_job + 1
                count = min(per_job, total - batch_index * per_job)
                db.create_job(
                    JobCreate(
                        repo_url="",
                        git_ref="",
                        entrypoint=entrypoint,
                        arguments=arguments,
                        env_setup=env_setup,
                        required_capability=required_capability,
                        env_profile=env_profile,
                        account_name=account_name,
                        partition=partition,
                        time_limit=time_limit,
                        cpus=count * max(1, cpus_per_simulation),
                        memory=memory,
                        gpus=gpus,
                        gpu_model=gpu_model,
                        job_name=f"{job_name}-{start}-{start + count - 1}",
                        job_mode=job_mode,
                        remote_path=remote_path,
                        simulations_per_job=per_job,
                        cpus_per_simulation=cpus_per_simulation,
                        simulation_start=start,
                        simulation_count=count,
                        node_name=node_name,
                        exclusive_node=exclusive_node,
                        mem_per_simulation_gb=mem_per_simulation_gb,
                        max_workers_per_job=max_workers_per_job,
                        initial_workers=min(count, max(1, cpus // max(1, cpus_per_simulation))),
                        load_target=load_target,
                        ramp_interval_seconds=ramp_interval_seconds,
                    )
                )
        else:
            command, payload_json = git_task_fields(repo_url, git_ref, entrypoint, arguments, git_credential_id)
            task_id = db.create_task(
                TaskCreate(
                    name=job_name or "git-task",
                    remote_cwd=ACCOUNT_WORKSPACE_PLACEHOLDER,
                    command=command,
                    env_setup=env_setup,
                    required_capability=required_capability,
                    env_profile=env_profile,
                    account_name=account_name,
                    cpus=max(1, cpus),
                    memory_mb=max(1, parse_memory_mb(memory)),
                    scheduling_profile=SchedulingProfile.STANDARD.value,
                    gpus=max(0, gpus),
                    gpu_model=gpu_model,
                    partition=partition,
                    node_name=node_name,
                    exclusive_node=exclusive_node,
                    same_node_as_task_id=max(0, same_node_as_task_id),
                    payload_json=payload_json,
                )
            )
            maybe_assign_same_node_task(task_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/tasks")
    def create_task(
        name: str = Form("remote-task"),
        remote_cwd: str = Form(...),
        command: str = Form(...),
        env_setup: str = Form(""),
        required_capability: str = Form(""),
        env_profile: str = Form(""),
        account_name: str = Form(""),
        cpus: int = Form(1),
        memory_mb: int = Form(4096),
        scheduling_profile: str = Form(SchedulingProfile.STANDARD.value),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        partition: str = Form("auto"),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
        same_node_as_task_id: int = Form(0),
    ) -> Response:
        task_id = db.create_task(
            TaskCreate(
                name=name,
                remote_cwd=remote_cwd,
                command=command,
                env_setup=env_setup,
                required_capability=required_capability,
                env_profile=env_profile,
                account_name=account_name,
                cpus=max(1, cpus),
                memory_mb=max(1, memory_mb),
                scheduling_profile=normalize_scheduling_profile(scheduling_profile),
                gpus=max(0, gpus),
                gpu_model=gpu_model,
                partition=partition,
                node_name=node_name,
                exclusive_node=exclusive_node,
                same_node_as_task_id=max(0, same_node_as_task_id),
            )
        )
        maybe_assign_same_node_task(task_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/tasks/git")
    def create_git_task(
        job_name: str = Form("git-task"),
        repo_url: str = Form(...),
        git_ref: str = Form("main"),
        entrypoint: str = Form(...),
        arguments: str = Form(""),
        git_credential_id: str = Form(""),
        env_setup: str = Form(""),
        required_capability: str = Form(""),
        env_profile: str = Form(""),
        account_name: str = Form(""),
        partition: str = Form("auto"),
        cpus: int = Form(1),
        memory: str = Form("4G"),
        scheduling_profile: str = Form(SchedulingProfile.STANDARD.value),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
        same_node_as_task_id: int = Form(0),
    ) -> Response:
        command, payload_json = git_task_fields(repo_url, git_ref, entrypoint, arguments, git_credential_id)
        task_id = db.create_task(
            TaskCreate(
                name=job_name or "git-task",
                remote_cwd=ACCOUNT_WORKSPACE_PLACEHOLDER,
                command=command,
                env_setup=env_setup,
                required_capability=required_capability,
                env_profile=env_profile,
                account_name=account_name,
                cpus=max(1, cpus),
                memory_mb=max(1, parse_memory_mb(memory)),
                scheduling_profile=normalize_scheduling_profile(scheduling_profile),
                gpus=max(0, gpus),
                gpu_model=gpu_model,
                partition=partition,
                node_name=node_name,
                exclusive_node=exclusive_node,
                same_node_as_task_id=max(0, same_node_as_task_id),
                payload_json=payload_json,
            )
        )
        maybe_assign_same_node_task(task_id)
        return RedirectResponse("/", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(job_id: int, request: Request) -> HTMLResponse:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse("job_detail.html", {"request": request, "job": job})

    @app.get("/tasks/{task_id}", response_class=HTMLResponse)
    def task_detail(task_id: int, request: Request) -> HTMLResponse:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        task = {**task, **task_view_fields(task)}
        return templates.TemplateResponse(
            "task_detail.html",
            {
                "request": request,
                "task": task,
                "submission": task_submission_examples(task),
            },
        )

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: int) -> Response:
        scheduler.cancel(job_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: int) -> Response:
        try:
            scheduler.cancel_task(task_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return RedirectResponse("/", status_code=303)

    @app.post("/scheduler/gpu-prewarm")
    def set_gpu_prewarm_form(enabled: str = Form(...)) -> Response:
        scheduler.set_gpu_prewarm_enabled(enabled.strip().lower() in {"1", "true", "on", "yes"})
        return RedirectResponse("/", status_code=303)

    @app.post("/allocations/{allocation_id}/close")
    def close_allocation(allocation_id: int, force: bool = Form(False)) -> Response:
        try:
            scheduler.request_close_allocation(allocation_id, force=force, allow_protected=True)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return RedirectResponse("/", status_code=303)

    @app.post("/jobs/{job_id}/simulation-count")
    def update_simulation_count(job_id: int, simulation_count: int = Form(...)) -> Response:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        if job["status"] != "queued":
            raise HTTPException(status_code=409, detail="only queued jobs can be edited")
        if job.get("job_mode") not in {"packed_srun", "dynamic_packed_srun"}:
            raise HTTPException(status_code=409, detail="only packed jobs have a simulation count")
        count = max(1, simulation_count)
        cpus_per_sim = max(1, int(job.get("cpus_per_simulation") or 1))
        initial_workers = max(1, min(count, int(job.get("initial_workers") or count)))
        max_workers = max(initial_workers, min(count, int(job.get("max_workers_per_job") or count)))
        db.update_job(
            job_id,
            simulation_count=count,
            simulations_per_job=count,
            cpus=initial_workers * cpus_per_sim,
            initial_workers=initial_workers,
            max_workers_per_job=max_workers,
        )
        return RedirectResponse("/", status_code=303)

    @app.post("/token-usage")
    def create_token_usage(
        provider: str = Form("codex"),
        project: str = Form(...),
        input_tokens: int = Form(0),
        output_tokens: int = Form(0),
        total_tokens: int = Form(0),
        reset_cycle: str = Form(""),
        note: str = Form(""),
    ) -> Response:
        db.create_token_usage(
            provider=provider,
            project=project,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens or None,
            reset_cycle=reset_cycle,
            note=note,
        )
        return RedirectResponse("/", status_code=303)

    @app.get("/api/jobs")
    def api_jobs() -> list[dict]:
        return db.list_jobs()

    @app.get("/api/tasks")
    def api_tasks(include_diagnostics: bool = False) -> list[dict]:
        allocation_rows = None
        allocation_by_id = None
        active_task_allocation_ids = None
        active_exclusive_allocation_ids = None
        if include_diagnostics:
            allocation_rows = db.list_allocations_with_live(limit=500)
            allocation_by_id = {int(allocation["id"]): allocation for allocation in allocation_rows}
            active_task_allocation_ids, active_exclusive_allocation_ids = scheduler.active_task_allocation_sets()
        return [
            task_json(
                task,
                derive_failure_message=False,
                allocation_rows=allocation_rows,
                allocation_by_id=allocation_by_id,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
                include_diagnostics=include_diagnostics,
            )
            for task in db.list_tasks_with_active()
        ]

    @app.post("/api/tasks")
    async def api_create_task(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="request body must be JSON") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="request body must be a JSON object")
        task_id, deduped = create_task_record(payload)
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=500, detail="task was not created")
        response = task_json(task)
        response["deduped"] = deduped
        return JSONResponse(response, status_code=200 if deduped else 201)

    @app.post("/api/tasks/git")
    async def api_create_git_task(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="request body must be JSON") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="request body must be a JSON object")
        repo_url = str(payload.get("repo_url") or "").strip()
        entrypoint = str(payload.get("entrypoint") or "").strip()
        if not repo_url:
            raise HTTPException(status_code=422, detail="repo_url is required")
        if not entrypoint:
            raise HTTPException(status_code=422, detail="entrypoint is required")
        git_ref = str(payload.get("git_ref") or "main")
        arguments = str(payload.get("arguments") or "")
        command, payload_json = git_task_fields(repo_url, git_ref, entrypoint, arguments, str(payload.get("git_credential_id") or ""))
        task_payload = {
            "name": str(payload.get("name") or payload.get("job_name") or "git-task"),
            "remote_cwd": ACCOUNT_WORKSPACE_PLACEHOLDER,
            "command": command,
            "env_setup": str(payload.get("env_setup") or ""),
            "required_capability": str(payload.get("required_capability") or ""),
            "env_profile": str(payload.get("env_profile") or ""),
            "account_name": str(payload.get("account_name") or ""),
            "cpus": max(1, int(payload.get("cpus") or 1)),
            "memory_mb": max(1, int(payload.get("memory_mb") or parse_memory_mb(str(payload.get("memory") or "4G")))),
            "scheduling_profile": normalize_scheduling_profile(str(payload.get("scheduling_profile") or "")),
            "gpus": max(0, int(payload.get("gpus") or 0)),
            "gpu_model": str(payload.get("gpu_model") or ""),
            "partition": str(payload.get("partition") or "auto"),
            "node_name": str(payload.get("node_name") or ""),
            "exclusive_node": bool(payload.get("exclusive_node") or False),
            "priority": int(payload.get("priority") or 0),
            "timeout_seconds": max(0, int(payload.get("timeout_seconds") or payload.get("timeout") or 0)),
            "dedupe_key": str(payload.get("dedupe_key") or ""),
            "max_workers_per_node": max(0, int(payload.get("max_workers_per_node") or 0)),
            "same_node_as_task_id": max(0, int(payload.get("same_node_as_task_id") or payload.get("same_node_as") or 0)),
            "payload_json": payload_json,
        }
        task_id, deduped = create_task_record(task_payload)
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=500, detail="task was not created")
        response = task_json(task)
        response["deduped"] = deduped
        return JSONResponse(response, status_code=200 if deduped else 201)

    @app.post("/api/tasks/cancel")
    def api_cancel_tasks(
        name_contains: str = "",
        statuses: str = "queued,attaching,running",
        limit: int = 5000,
    ) -> dict:
        requested_statuses = {part.strip() for part in statuses.split(",") if part.strip()}
        valid_statuses = {"queued", "attaching", "running", "completed", "failed", "cancelled"}
        unknown = sorted(requested_statuses - valid_statuses)
        if unknown:
            raise HTTPException(status_code=400, detail=f"unknown task status: {', '.join(unknown)}")
        cancelled = scheduler.cancel_tasks(name_contains=name_contains, statuses=requested_statuses, limit=max(1, limit))
        return {"cancelled": cancelled, "count": len(cancelled)}

    @app.get("/api/allocations")
    def api_allocations() -> list[dict]:
        return annotate_allocation_fea_pressure(
            annotate_allocation_node_metrics(db.list_allocations_with_live(), db.list_pestat_nodes()),
            scheduler.fea_owned_node_pressures(),
        )

    @app.post("/api/allocations/{allocation_id}/close")
    def api_close_allocation(allocation_id: int, force: bool = False) -> dict:
        try:
            return scheduler.request_close_allocation(allocation_id, force=force)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/scheduler/gpu-prewarm")
    def api_get_gpu_prewarm() -> dict:
        return {
            "enabled": scheduler.gpu_prewarm_enabled,
            "config_default": scheduler.gpu_prewarm_enabled_default,
        }

    @app.post("/api/scheduler/gpu-prewarm")
    async def api_set_gpu_prewarm(request: Request) -> dict:
        payload = await request.json()
        scheduler.set_gpu_prewarm_enabled(bool(payload.get("enabled")))
        return {"enabled": scheduler.gpu_prewarm_enabled}

    @app.get("/api/gpu-capacity")
    def api_gpu_capacity() -> list[dict]:
        return scheduler.gpu_capacity_summary()

    @app.get("/api/task-capacity")
    def api_task_capacity(
        cpus: int = 1,
        memory_mb: int = 4096,
        scheduling_profile: str = SchedulingProfile.STANDARD.value,
        gpus: int = 0,
        gpu_model: str = "",
        required_capability: str = "",
        env_profile: str = "",
        account_name: str = "",
        partition: str = "auto",
        node_name: str = "",
        max_workers_per_node: int = 0,
    ) -> dict:
        task = {
            "cpus": max(1, cpus),
            "memory_mb": max(1, memory_mb),
            "scheduling_profile": normalize_scheduling_profile(scheduling_profile),
            "gpus": max(0, gpus),
            "gpu_model": gpu_model,
            "required_capability": required_capability,
            "env_profile": env_profile,
            "account_name": account_name,
            "partition": partition,
            "node_name": node_name,
            "exclusive_node": 0,
            "max_workers_per_node": max(0, max_workers_per_node),
        }
        allocation_rows = db.list_allocations_with_live(limit=500)
        active_task_allocation_ids, active_exclusive_allocation_ids = scheduler.active_task_allocation_sets()
        capacity = scheduler.task_fit_capacity(
            task,
            allocation_rows=allocation_rows,
            active_task_allocation_ids=active_task_allocation_ids,
            active_exclusive_allocation_ids=active_exclusive_allocation_ids,
        )
        capacity.update(
            scheduler.task_queue_diagnostics(
                task,
                capacity=capacity,
                allocation_rows=allocation_rows,
                active_task_allocation_ids=active_task_allocation_ids,
                active_exclusive_allocation_ids=active_exclusive_allocation_ids,
            )
        )
        return capacity

    @app.get("/api/health")
    def api_health(response: Response) -> dict:
        health = scheduler.health_status()
        ok = bool(health.get("scheduler_ok"))
        if not ok:
            response.status_code = 503
        return {
            "ok": ok,
            "accounts": len(accounts),
            "jobs": len(db.list_jobs()),
            "tasks": len(db.list_tasks()),
            "allocations": len(db.list_allocations()),
            **health,
        }

    @app.get("/api/events")
    def api_events(limit: int = 200) -> list[dict]:
        return db.list_events(limit=max(1, min(1000, limit)))

    @app.get("/api/jobs/{job_id}")
    def api_job(job_id: int) -> dict:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        return job

    @app.get("/api/jobs/{job_id}/remote-file")
    def api_job_remote_file(
        job_id: int,
        path: str,
        base: str = "remote_path",
        tail_lines: int = 0,
        max_bytes: int = 0,
    ) -> Response:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        if not job["account_name"]:
            raise HTTPException(status_code=409, detail="job has no account")
        if path.startswith("/") or ".." in Path(path).parts:
            raise HTTPException(status_code=400, detail="path must be a safe relative path")
        account = next((item for item in accounts if item.name == job["account_name"]), None)
        if not account:
            raise HTTPException(status_code=404, detail="account not found")
        if base == "remote_job_dir":
            root = job.get("remote_job_dir") or ""
        else:
            root = job.get("remote_path") or job.get("remote_job_dir") or ""
        if not root:
            raise HTTPException(status_code=409, detail="job has no remote base path")
        remote_file = posixpath.join(root, path)
        tail, byte_limit = bounded_remote_window(tail_lines=tail_lines, max_bytes=max_bytes)
        try:
            text = remote_read(
                ("job-remote-file", job_id, base, remote_file, tail, byte_limit),
                lambda: SlurmAccountClient(account).read_text_file(
                    remote_file,
                    tail_lines=tail,
                    max_bytes=byte_limit,
                    timeout=remote_command_timeout(),
                ),
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        return Response(text, media_type="text/plain")

    @app.get("/api/tasks/{task_id}")
    def api_task(
        task_id: int,
        include_output: bool = False,
        output_limit: int = 65536,
        include_diagnostics: bool = False,
    ) -> dict:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        return task_json(
            task,
            include_output=include_output,
            output_limit=max(0, output_limit),
            include_diagnostics=include_diagnostics,
        )

    @app.post("/api/tasks/{task_id}/cancel")
    def api_cancel_task(task_id: int) -> dict:
        try:
            return scheduler.request_cancel_task(task_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/tasks/{task_id}/remote-file")
    def api_task_remote_file(
        task_id: int,
        path: str,
        base: str = "remote_cwd",
        tail_lines: int = 0,
        max_bytes: int = 0,
    ) -> Response:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        if not task["account_name"]:
            raise HTTPException(status_code=409, detail="task has no account")
        if path.startswith("/") or ".." in Path(path).parts:
            raise HTTPException(status_code=400, detail="path must be a safe relative path")
        account = account_for_task(task)
        if not account:
            raise HTTPException(status_code=404, detail="account not found")
        root = task_remote_root(task, account, base)
        if not root:
            raise HTTPException(status_code=409, detail="task has no remote base path")
        remote_file = posixpath.join(root, path)
        tail, byte_limit = bounded_remote_window(tail_lines=tail_lines, max_bytes=max_bytes)
        try:
            text = remote_read(
                ("task-remote-file", task_id, base, remote_file, tail, byte_limit),
                lambda: SlurmAccountClient(account).read_text_file(
                    remote_file,
                    tail_lines=tail,
                    max_bytes=byte_limit,
                    timeout=remote_command_timeout(),
                ),
            )
        except FileNotFoundError:
            text = ""
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        return Response(apply_text_window(text, tail_lines=max(0, tail_lines), max_bytes=max(0, max_bytes)), media_type="text/plain")

    @app.get("/api/tasks/{task_id}/remote-files")
    def api_task_remote_files(task_id: int, glob: str, base: str = "remote_cwd") -> dict:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        if not task["account_name"]:
            raise HTTPException(status_code=409, detail="task has no account")
        if glob.startswith("/") or ".." in Path(glob).parts:
            raise HTTPException(status_code=400, detail="glob must be a safe relative pattern")
        account = account_for_task(task)
        if not account:
            raise HTTPException(status_code=404, detail="account not found")
        root = task_remote_root(task, account, base)
        if not root:
            raise HTTPException(status_code=409, detail="task has no remote base path")
        try:
            files = remote_read(
                ("task-remote-files", task_id, base, root, glob),
                lambda: SlurmAccountClient(account).list_files(root, glob, timeout=remote_command_timeout()),
            )
        except FileNotFoundError:
            files = []
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        return {"base": base, "glob": glob, "files": files}

    @app.get("/api/tasks/{task_id}/stdout")
    def api_task_stdout(task_id: int, tail_lines: int = 0, max_bytes: int = 0) -> Response:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        if not task["account_name"]:
            return Response("", media_type="text/plain")
        if not task.get("stdout_path"):
            return Response("", media_type="text/plain")
        account = account_for_task(task)
        if not account:
            return Response("", media_type="text/plain")
        tail, byte_limit = bounded_remote_window(tail_lines=tail_lines, max_bytes=max_bytes)
        try:
            text = remote_read(
                ("task-stdout", task_id, task["stdout_path"], tail, byte_limit),
                lambda: SlurmAccountClient(account).read_text_file(
                    task["stdout_path"],
                    tail_lines=tail,
                    max_bytes=byte_limit,
                    timeout=remote_command_timeout(),
                ),
            )
        except FileNotFoundError:
            text = ""
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        return Response(apply_text_window(text, tail_lines=max(0, tail_lines), max_bytes=max(0, max_bytes)), media_type="text/plain")

    @app.get("/api/tasks/{task_id}/stderr")
    def api_task_stderr(task_id: int, tail_lines: int = 0, max_bytes: int = 0) -> Response:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        if not task["account_name"]:
            return Response("", media_type="text/plain")
        if not task.get("stderr_path"):
            return Response("", media_type="text/plain")
        account = account_for_task(task)
        if not account:
            return Response("", media_type="text/plain")
        tail, byte_limit = bounded_remote_window(tail_lines=tail_lines, max_bytes=max_bytes)
        try:
            text = remote_read(
                ("task-stderr", task_id, task["stderr_path"], tail, byte_limit),
                lambda: SlurmAccountClient(account).read_text_file(
                    task["stderr_path"],
                    tail_lines=tail,
                    max_bytes=byte_limit,
                    timeout=remote_command_timeout(),
                ),
            )
        except FileNotFoundError:
            text = ""
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        return Response(apply_text_window(text, tail_lines=max(0, tail_lines), max_bytes=max(0, max_bytes)), media_type="text/plain")

    @app.get("/api/accounts/status")
    def api_accounts() -> list[dict]:
        return [snapshot.__dict__ for snapshot in scheduler.cached_snapshots()]

    @app.get("/api/accounts/status/live")
    def api_accounts_live() -> list[dict]:
        return [snapshot.__dict__ for snapshot in scheduler.snapshots()]

    @app.get("/api/capabilities")
    def api_capabilities() -> list[dict]:
        return build_capability_summary(accounts, db.list_account_env_overlays())

    @app.post("/api/conda-env-sync")
    async def api_create_conda_env_sync(request: Request) -> JSONResponse:
        payload = await request.json()
        try:
            sync_job_id = env_sync_manager.start(
                str(payload.get("reference_account") or ""),
                str(payload.get("source_env_name") or ""),
                parse_account_list(payload.get("target_accounts") or []),
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        job = db.get_env_sync_job(sync_job_id)
        return JSONResponse(env_sync_job_json(job) if job else {"id": sync_job_id, "status": "queued"})

    @app.get("/api/conda-env-sync")
    def api_list_conda_env_sync() -> list[dict]:
        return [env_sync_job_json(job) for job in db.list_env_sync_jobs(limit=50)]

    @app.get("/api/conda-env-sync/{sync_job_id}")
    def api_get_conda_env_sync(sync_job_id: int) -> dict:
        job = db.get_env_sync_job(sync_job_id)
        if not job:
            raise HTTPException(status_code=404)
        return env_sync_job_json(job)

    @app.post("/api/conda-env-sync/{sync_job_id}/cancel")
    def api_cancel_conda_env_sync(sync_job_id: int) -> dict:
        try:
            env_sync_manager.cancel(sync_job_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        job = db.get_env_sync_job(sync_job_id)
        return env_sync_job_json(job) if job else {"id": sync_job_id, "status": "cancelled"}

    @app.get("/api/token-usage")
    def api_token_usage() -> list[dict]:
        return db.list_token_usage()

    return app


app = create_app(os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml"))
