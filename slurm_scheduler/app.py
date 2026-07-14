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

from fastapi import FastAPI, Form, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .allocation_metrics import annotate_allocation_fea_pressure, annotate_allocation_node_metrics
from .aedt_pool import AedtPoolRuntime, AedtPoolService
from .aedt_pool_api import create_aedt_pool_router
from .conda_sync import CondaEnvSyncManager, conda_bootstrap
from .config import AppConfig, load_accounts, load_app_config
from .control_plane_relay import ControlPlaneRelay
from .db import Database
from .git_auth import find_git_credential, git_task_payload
from .models import AedtBackend, JobCreate, SchedulingProfile, TaskCreate, TaskStatus, normalize_aedt_backend, normalize_scheduling_profile
from .inventory import partition_rank
from .pestat import PestatNode, plan_dynamic_allocations
from .project_env import ProjectEnvManager, repo_dir_name
from .scheduler import Scheduler
from .slurm import SlurmAccountClient, SSHSession
from .task_commands import ACCOUNT_WORKSPACE_PLACEHOLDER, build_git_task_command

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def parse_aedt_backend(value: object) -> str:
    try:
        return normalize_aedt_backend(str(value or ""))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def normalize_cleanup_globs(value: object) -> str:
    """Accept a list or comma string of basename globs; keep only safe ones."""
    if value is None:
        return ""
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        parts = [str(part).strip() for part in value]
    else:
        return ""
    return ",".join(part for part in parts if part and Scheduler._workspace_prune_glob_ok(part))


def normalize_task_status_filters(values: list[str] | str | None) -> list[str] | None:
    """Normalize repeated and comma-separated task status query values."""
    if values is None:
        return None
    raw_values = [values] if isinstance(values, str) else list(values)
    normalized: list[str] = []
    for raw in raw_values:
        for item in str(raw).split(","):
            value = item.strip().lower()
            if value and value not in normalized:
                normalized.append(value)
    allowed = {status.value for status in TaskStatus}
    invalid = sorted(value for value in normalized if value not in allowed)
    if invalid or not normalized:
        allowed_text = ", ".join(sorted(allowed))
        invalid_text = ", ".join(invalid) if invalid else "<empty>"
        raise HTTPException(
            status_code=422,
            detail=f"status must contain only {allowed_text}; invalid: {invalid_text}",
        )
    return normalized


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


def allocation_usage_summary(allocations: list[dict], pending: int = 0) -> dict[str, int]:
    node_names = {str(item.get("node_name") or "").strip() for item in allocations}
    node_names.discard("")
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
        "nodes": len(node_names),
        "allocations": len(allocations),
        "pending": max(0, int(pending)),
    }


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
    db = Database(config.database_path, journal_mode=config.sqlite_journal_mode)
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
        cpu_pool_partition_spread=config.cpu_pool_partition_spread,
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
        fea_baseline_max_attach_per_loop=config.fea_baseline_max_attach_per_loop,
        fea_node_name_policy=config.fea_node_name_policy,
        fea_overload_scale_out_load_factor=config.fea_overload_scale_out_load_factor,
        fea_overload_scale_out_seconds=config.fea_overload_scale_out_seconds,
        fea_pressure_max_attempts=config.fea_pressure_max_attempts,
        fea_max_attach_per_node_per_loop=config.fea_max_attach_per_node_per_loop,
        fea_node_requested_cpu_factor=config.fea_node_requested_cpu_factor,
        fea_footprint_maturity_seconds=config.fea_footprint_maturity_seconds,
        fea_cpu_footprint_maturity_seconds=config.fea_cpu_footprint_maturity_seconds,
        fea_alloc_util_enabled=config.fea_alloc_util_enabled,
        fea_alloc_util_target=config.fea_alloc_util_target,
        fea_alloc_util_sample_interval_seconds=config.fea_alloc_util_sample_interval_seconds,
        fea_shared_memory_estimate_fraction=config.fea_shared_memory_estimate_fraction,
        fea_shared_memory_min_estimate_mb=config.fea_shared_memory_min_estimate_mb,
        task_refresh_max_per_tick=config.task_refresh_max_per_tick,
        fea_adaptive_memory_relax_enabled=config.fea_adaptive_memory_relax_enabled,
        fea_adaptive_memory_window_seconds=config.fea_adaptive_memory_window_seconds,
        fea_adaptive_memory_min_coverage_seconds=config.fea_adaptive_memory_min_coverage_seconds,
        fea_adaptive_memory_margin_percent=config.fea_adaptive_memory_margin_percent,
        fea_adaptive_memory_max_attach_per_tick=config.fea_adaptive_memory_max_attach_per_tick,
        cleanup_enabled=config.cleanup_enabled,
        cleanup_interval_seconds=config.cleanup_interval_seconds,
        cleanup_finished_task_ttl_seconds=config.cleanup_finished_task_ttl_seconds,
        cleanup_finished_job_ttl_seconds=config.cleanup_finished_job_ttl_seconds,
        cleanup_closed_allocation_ttl_seconds=config.cleanup_closed_allocation_ttl_seconds,
        reconcile_on_start=config.reconcile_on_start,
        backup_enabled=config.backup_enabled,
        backup_interval_seconds=config.backup_interval_seconds,
        backup_keep=config.backup_keep,
        backup_dir=config.backup_dir,
        cleanup_orphan_sweep_enabled=config.cleanup_orphan_sweep_enabled,
        cleanup_orphan_sweep_interval_seconds=config.cleanup_orphan_sweep_interval_seconds,
        cleanup_orphan_min_age_seconds=config.cleanup_orphan_min_age_seconds,
        cleanup_workspace_prune_globs=config.cleanup_workspace_prune_globs,
        cleanup_workspace_prune_interval_seconds=config.cleanup_workspace_prune_interval_seconds,
        cleanup_workspace_prune_min_age_seconds=config.cleanup_workspace_prune_min_age_seconds,
        cleanup_finished_task_log_max_bytes=config.cleanup_finished_task_log_max_bytes,
        cleanup_finished_task_log_trim_after_seconds=config.cleanup_finished_task_log_trim_after_seconds,
        orphan_process_sweep_enabled=config.orphan_process_sweep_enabled,
        orphan_process_sweep_interval_seconds=config.orphan_process_sweep_interval_seconds,
        orphan_process_min_age_seconds=config.orphan_process_min_age_seconds,
        orphan_process_name_patterns=config.orphan_process_name_patterns,
        storage_guard_min_free_gb=config.storage_guard_min_free_gb,
        license_monitor_enabled=config.license_monitor_enabled,
        license_monitor_account=config.license_monitor_account,
        license_monitor_lmutil_path=config.license_monitor_lmutil_path,
        license_monitor_license_server=config.license_monitor_license_server,
        license_monitor_interval_seconds=config.license_monitor_interval_seconds,
        license_monitor_watch_features=config.license_monitor_watch_features,
        license_monitor_display=config.license_monitor_display,
        license_admission_enabled=config.license_admission_enabled,
        license_admission_snapshot_max_age_seconds=config.license_admission_snapshot_max_age_seconds,
        license_admission_settlement_seconds=config.license_admission_settlement_seconds,
        license_admission_reserve_by_feature=config.license_admission_reserve_by_feature,
        license_admission_persistent_cost_by_project=config.license_admission_persistent_cost_by_project,
        license_admission_reserve_exempt_projects=config.license_admission_reserve_exempt_projects,
        license_admission_unknown_fea_project_policy=config.license_admission_unknown_fea_project_policy,
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
    aedt_pool_bootstrap_token = os.environ.get("SLURM_AEDT_POOL_BOOTSTRAP_TOKEN", "").strip()
    aedt_pool = AedtPoolService(db, bootstrap_token=aedt_pool_bootstrap_token)
    aedt_pool.init()
    aedt_pool.set_warm_spare_admission_checker(
        scheduler.aedt_pool_warm_spare_admission
    )
    def aedt_backend_admission(task) -> tuple[bool, str]:
        pool_config = aedt_pool.config()
        if pool_config.operational:
            if (
                config.control_plane_relay_enabled
                and not pool_config.control_plane_url
            ):
                return False, "AEDT pool control-plane relay is unavailable"
            return True, ""
        return False, "AEDT pooled backend is not operational"

    scheduler.set_aedt_backend_admission_checker(aedt_backend_admission)
    relay_account = scheduler.account_by_name(config.control_plane_relay_account.strip())
    relay_bind_host = config.bind_host.strip() or "127.0.0.1"
    if relay_bind_host == "0.0.0.0":
        relay_bind_host = "127.0.0.1"
    elif relay_bind_host in {"::", "[::]"}:
        relay_bind_host = "::1"
    elif relay_bind_host.startswith("[") and relay_bind_host.endswith("]"):
        relay_bind_host = relay_bind_host[1:-1]
    control_plane_relay = ControlPlaneRelay(
        enabled=config.control_plane_relay_enabled,
        account=relay_account,
        relay_port=config.control_plane_relay_port,
        remote_path=config.control_plane_relay_remote_path,
        allowed_prefixes=config.control_plane_relay_allowed_prefixes,
        local_host=relay_bind_host,
        local_port=config.bind_port,
        interval_seconds=config.poll_interval_seconds,
        publish_url=aedt_pool.set_control_plane_url,
    )
    control_plane_relay_configured = bool(
        control_plane_relay.enabled
        and control_plane_relay.account is not None
        and control_plane_relay.remote_path
        and 1 <= control_plane_relay.relay_port <= 65535
        and 1 <= control_plane_relay.local_port <= 65535
        and control_plane_relay.allowed_prefixes
        and all(prefix.startswith("/") for prefix in control_plane_relay.allowed_prefixes)
    )
    control_plane_endpoint_configured = (
        control_plane_relay_configured
        if config.control_plane_relay_enabled
        else bool(config.aedt_pool_scheduler_url.strip())
    )
    aedt_adapter_configured = bool(
        config.aedt_pool_session_host_enabled
        and aedt_pool_bootstrap_token
        and control_plane_endpoint_configured
        and config.aedt_pool_host_remote_cwd.strip()
        and config.aedt_pool_host_bootstrap_token_file.strip()
    )
    # Recompute on every start so a stale DB flag cannot outlive a removed
    # token file/launch configuration.
    aedt_pool.set_adapter_ready(aedt_adapter_configured)
    aedt_pool_runtime = AedtPoolRuntime(
        aedt_pool,
        scheduler,
        interval_seconds=config.poll_interval_seconds,
        scheduler_url=config.aedt_pool_scheduler_url,
        host_remote_cwd=config.aedt_pool_host_remote_cwd,
        host_python=config.aedt_pool_host_python,
        host_env_setup=config.aedt_pool_host_env_setup,
        host_bootstrap_token_file=config.aedt_pool_host_bootstrap_token_file,
        host_task_memory_mb=config.aedt_pool_host_task_memory_mb,
        require_published_control_plane_url=config.control_plane_relay_enabled,
    )
    app.state.aedt_pool = aedt_pool
    app.state.aedt_pool_runtime = aedt_pool_runtime
    app.state.control_plane_relay = control_plane_relay
    app.include_router(create_aedt_pool_router(aedt_pool))
    env_sync_manager = CondaEnvSyncManager(db, accounts)
    app.state.env_sync_manager = env_sync_manager
    project_env_manager = ProjectEnvManager(
        db, accounts, projects_root=getattr(config, "projects_root", "slurm_scheduler/projects")
    )
    app.state.project_env_manager = project_env_manager
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
            "requested_allocation_id": int(task.get("requested_allocation_id") or 0),
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
            "launch_started_at": task.get("launch_started_at"),
            "started_at": task.get("started_at"),
            "finished_at": task.get("finished_at"),
            "assigned_allocation": task.get("allocation_id"),
            "allocation_id": task.get("allocation_id"),
            "account_name": task.get("account_name") or "",
            "requested_account_name": task.get("requested_account_name") or "",
            "slurm_job_id": allocation.get("slurm_job_id") if allocation else "",
            "remote_cwd": task.get("remote_cwd") or "",
            "remote_dir": task.get("remote_dir") or "",
            "stdout_path": task.get("stdout_path") or "",
            "stderr_path": task.get("stderr_path") or "",
            "exit_code_path": task.get("exit_code_path") or "",
            "required_capability": task.get("required_capability") or "",
            "env_profile": task.get("env_profile") or "",
            "project": task.get("project") or "",
            "entrypoint": task.get("entrypoint") or "",
            "scheduling_profile": normalize_scheduling_profile(task.get("scheduling_profile") or ""),
            "aedt_backend": normalize_aedt_backend(task.get("aedt_backend") or ""),
            "priority": int(task.get("priority") or 0),
            "timeout_seconds": int(task.get("timeout_seconds") or 0),
            "dedupe_key": task.get("dedupe_key") or "",
            "max_workers_per_node": int(task.get("max_workers_per_node") or 0),
            "same_node_as_task_id": int(task.get("same_node_as_task_id") or 0),
            "requested_allocation_id": int(task.get("requested_allocation_id") or 0),
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
            "account_name": (
                task.get("requested_account_name")
                if "requested_account_name" in task and task.get("requested_account_name") is not None
                else task.get("account_name")
            ) or "",
            "cpus": int(task.get("cpus") or 1),
            "memory_mb": int(task.get("memory_mb") or 4096),
            "scheduling_profile": normalize_scheduling_profile(task.get("scheduling_profile") or ""),
            "aedt_backend": normalize_aedt_backend(task.get("aedt_backend") or ""),
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

    def project_json(project: dict, include_deployments: bool = True) -> dict:
        def _loads(value, default):
            try:
                return json.loads(value) if value else default
            except (TypeError, json.JSONDecodeError):
                return default

        project_name = str(project.get("name") or "")
        queued_count = db.count_tasks_by_project(project_name, ["queued"])
        attaching_count = db.count_tasks_by_project(project_name, ["attaching"])
        executing_count = db.count_tasks_by_project(project_name, ["running"])
        payload = {
            "id": int(project["id"]),
            "name": project_name,
            "repos": _loads(project.get("repos"), []),
            "setup": project.get("setup") or "",
            "entrypoints": _loads(project.get("entrypoints"), []),
            "cleanup_globs": project.get("cleanup_globs") or "",
            "output_globs": project.get("output_globs") or "",
            "sim_subdir": project.get("sim_subdir") or "simulation",
            "auto_pull": bool(project.get("auto_pull")),
            "max_active_tasks": max(0, int(project.get("max_active_tasks") or 0)),
            "aedt_backend": normalize_aedt_backend(project.get("aedt_backend") or ""),
            "queued_count": queued_count,
            "attaching_count": attaching_count,
            "executing_count": executing_count,
            "logical_active_count": queued_count + attaching_count + executing_count,
            "running_count": attaching_count + executing_count,
            "total_count": db.count_tasks_by_project(project_name),
            "created_at": project.get("created_at"),
            "updated_at": project.get("updated_at"),
        }
        if include_deployments:
            payload["deployments"] = db.list_project_deployments(int(project["id"]))
        return payload

    def parse_repo_lines(text: str) -> list[dict]:
        """One repo per line: ``url[|ref|subdir]``."""
        repos = []
        for line in (text or "").splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split("|")]
            repo = {"url": parts[0]}
            if len(parts) > 1 and parts[1]:
                repo["ref"] = parts[1]
            if len(parts) > 2 and parts[2]:
                repo["subdir"] = parts[2]
            repos.append(repo)
        return repos

    def parse_entrypoint_lines(text: str) -> list[dict]:
        """One entrypoint per line: ``path[|conda_env|workdir]``."""
        entrypoints = []
        for line in (text or "").splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split("|")]
            entrypoint = {"path": parts[0]}
            if len(parts) > 1 and parts[1]:
                entrypoint["conda_env"] = parts[1]
            if len(parts) > 2 and parts[2]:
                entrypoint["workdir"] = parts[2]
            entrypoints.append(entrypoint)
        return entrypoints

    def upsert_project_from_payload(payload: dict) -> int:
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("name is required")
        repos = payload.get("repos") or []
        if isinstance(repos, str):
            repos = parse_repo_lines(repos)
        entrypoints = payload.get("entrypoints") or []
        if isinstance(entrypoints, str):
            entrypoints = parse_entrypoint_lines(entrypoints)
        setup = str(payload.get("setup") or "")
        cleanup_globs = normalize_cleanup_globs(payload.get("cleanup_globs"))
        output_globs = str(payload.get("output_globs") or "").strip()
        sim_subdir = str(payload.get("sim_subdir") or "simulation").strip() or "simulation"
        auto_pull = bool(payload.get("auto_pull") or False)
        existing = db.get_project_by_name(name)
        aedt_backend = normalize_aedt_backend(
            payload.get("aedt_backend", existing.get("aedt_backend") if existing else "")
        )
        raw_max_active_tasks = payload.get(
            "max_active_tasks",
            existing.get("max_active_tasks") if existing else 0,
        )
        max_active_tasks_error = (
            "max_active_tasks must be an integer between 0 and "
            f"{config.project_max_active_tasks_ceiling}"
        )
        if isinstance(raw_max_active_tasks, bool):
            raise ValueError(max_active_tasks_error)
        if isinstance(raw_max_active_tasks, int):
            max_active_tasks = raw_max_active_tasks
        elif isinstance(raw_max_active_tasks, str) and raw_max_active_tasks.strip().isdigit():
            max_active_tasks = int(raw_max_active_tasks.strip())
        else:
            raise ValueError(max_active_tasks_error)
        if not 0 <= max_active_tasks <= config.project_max_active_tasks_ceiling:
            raise ValueError(max_active_tasks_error)
        if existing:
            db.update_project(
                int(existing["id"]),
                repos=json.dumps(repos, ensure_ascii=False),
                setup=setup,
                entrypoints=json.dumps(entrypoints, ensure_ascii=False),
                cleanup_globs=cleanup_globs,
                output_globs=output_globs,
                sim_subdir=sim_subdir,
                auto_pull=1 if auto_pull else 0,
                max_active_tasks=max_active_tasks,
                aedt_backend=aedt_backend,
            )
            return int(existing["id"])
        return db.create_project(
            name,
            repos=repos,
            setup=setup,
            entrypoints=entrypoints,
            cleanup_globs=cleanup_globs,
            output_globs=output_globs,
            sim_subdir=sim_subdir,
            auto_pull=auto_pull,
            max_active_tasks=max_active_tasks,
            aedt_backend=aedt_backend,
        )

    def apply_project_to_payload(payload: dict) -> dict:
        """When a task references a project + entrypoint, expand it into a concrete
        env_setup (project setup + conda activate + optional git pull), remote_cwd,
        and command — reusing the same env_setup channel env_profiles ride on."""
        project_name = str(payload.get("project") or "").strip()
        if not project_name:
            return payload
        project = db.get_project_by_name(project_name)
        if not project:
            raise HTTPException(status_code=422, detail=f"project not found: {project_name}")
        entrypoint = str(payload.get("entrypoint") or "").strip()
        rel_dir = project_env_manager.project_rel_dir(project_name)
        project_dir = posixpath.join("$HOME", rel_dir)
        try:
            entrypoints = json.loads(project.get("entrypoints") or "[]")
        except (TypeError, json.JSONDecodeError):
            entrypoints = []
        match = next((item for item in entrypoints if str(item.get("path")) == entrypoint), None)
        conda_env = str((match or {}).get("conda_env") or "")
        workdir = str((match or {}).get("workdir") or "").strip().strip("/")
        remote_cwd = posixpath.join(project_dir, workdir) if workdir else project_dir
        parts = []
        setup = str(project.get("setup") or "").strip()
        if setup:
            parts.append(setup)
        parts.append(conda_bootstrap())
        if conda_env:
            parts.append(f"conda activate {shlex.quote(conda_env)}")
        if project.get("auto_pull"):
            try:
                repos = json.loads(project.get("repos") or "[]")
            except (TypeError, json.JSONDecodeError):
                repos = []
            for index, repo in enumerate(repos):
                repo_rel = posixpath.join(rel_dir, repo_dir_name(repo, index))
                parts.append(f'git -C "$HOME/"{shlex.quote(repo_rel)} pull -q --ff-only || true')
        generated_setup = "\n".join(parts)
        existing_setup = str(payload.get("env_setup") or "").strip()
        merged_setup = generated_setup if not existing_setup else f"{generated_setup}\n{existing_setup}"
        updated = {**payload, "env_setup": merged_setup}
        if not str(payload.get("aedt_backend") or "").strip():
            updated["aedt_backend"] = normalize_aedt_backend(project.get("aedt_backend") or "")
        if not str(payload.get("remote_cwd") or "").strip():
            updated["remote_cwd"] = remote_cwd
        if not str(payload.get("command") or "").strip() and entrypoint:
            args = str(payload.get("arguments") or "").strip()
            updated["command"] = f"python {shlex.quote(entrypoint)}" + (f" {args}" if args else "")
        if not payload.get("cleanup_globs") and project.get("cleanup_globs"):
            updated["cleanup_globs"] = project.get("cleanup_globs")
        return updated

    def create_task_record(payload: dict) -> tuple[int, bool]:
        payload = apply_project_to_payload(payload)
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
        same_node_as_task_id = max(
            0,
            int(payload.get("same_node_as_task_id") or payload.get("same_node_as") or 0),
        )
        requested_allocation_id = max(0, int(payload.get("requested_allocation_id") or 0))
        entrypoint = str(payload.get("entrypoint") or "")
        if requested_allocation_id:
            raise HTTPException(
                status_code=422,
                detail="requested_allocation_id is not accepted for new tasks",
            )
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
            aedt_backend=parse_aedt_backend(payload.get("aedt_backend")),
            gpus=max(0, int(payload.get("gpus") or 0)),
            gpu_model=str(payload.get("gpu_model") or ""),
            partition=str(payload.get("partition") or "auto"),
            node_name=str(payload.get("node_name") or ""),
            exclusive_node=bool(payload.get("exclusive_node") or False),
            priority=int(payload.get("priority") or 0),
            timeout_seconds=max(0, int(payload.get("timeout_seconds") or payload.get("timeout") or 0)),
            dedupe_key=dedupe_key,
            max_workers_per_node=max(0, int(payload.get("max_workers_per_node") or 0)),
            same_node_as_task_id=same_node_as_task_id,
            payload_json=payload_json,
            cleanup_globs=normalize_cleanup_globs(payload.get("cleanup_globs")),
            project=str(payload.get("project") or ""),
            entrypoint=entrypoint,
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
        control_plane_relay.start()
        aedt_pool_runtime.start()

    @app.on_event("shutdown")
    def _shutdown() -> None:
        aedt_pool_runtime.stop()
        control_plane_relay.stop()
        scheduler.stop()

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request) -> HTMLResponse:
        attached_task_name_filter = (request.query_params.get("task_name_contains") or "").strip()
        finished_page_size = 50
        try:
            finished_page = max(0, int(request.query_params.get("finished_page") or 0))
        except ValueError:
            finished_page = 0
        snapshots = scheduler.cached_snapshots()
        snapshot_error = "" if snapshots else "Account status will appear after the background scheduler refreshes."
        alloc_fea_pressures = scheduler.fea_allocation_pressures()
        allocations = annotate_allocation_fea_pressure(
            annotate_allocation_node_metrics(db.list_allocations_with_live(limit=500), db.list_pestat_nodes()),
            alloc_fea_pressures,
        )
        alloc_utils = scheduler.allocation_utilizations()
        for allocation in allocations:
            allocation["node_fea_worker_count"] = int(
                (alloc_fea_pressures.get(int(allocation.get("id") or 0)) or {}).get("workers") or 0
            )
            util_info = alloc_utils.get(int(allocation.get("id") or 0))
            allocation["alloc_busy_cores"] = util_info["busy_cores"] if util_info else None
            allocation["alloc_util_percent"] = util_info["util_percent"] if util_info else None
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
        allocation_summary = allocation_usage_summary(
            allocated_summary_rows,
            pending=sum(1 for item in active_allocations if item["state"] == "pending"),
        )
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
        task_summary = db.task_activity_summary(
            name_contains=attached_task_name_filter,
        )
        aedt_dashboard_summary = aedt_pool.summary()
        active_task_rows = {
            int(task["id"]): task
            for task in (active_running_rows + visible_queued_rows)
        }
        # Queue reasons are shown on the task detail page only; computing them
        # for the dashboard list was the most expensive part of the render.
        queued_diagnostics_remaining = 0
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
                    limit=finished_page_size,
                    name_contains=attached_task_name_filter,
                    offset=finished_page * finished_page_size,
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
                "aedt_pool_summary": aedt_dashboard_summary,
                "finished_tasks": finished_tasks,
                "finished_task_count": finished_task_count,
                "finished_page": finished_page,
                "finished_page_size": finished_page_size,
                "finished_page_count": max(1, ceil(finished_task_count / finished_page_size)),
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
                "projects": [project_json(project) for project in db.list_projects()],
                "scheduler_events": db.list_events(limit=50),
                "scheduler_health": scheduler.health_status(),
                "license_usage": scheduler.license_usage(),
                "license_monitor_enabled": scheduler.license_monitor_enabled,
                "license_watch_features": scheduler.license_monitor_watch_features,
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
        aedt_backend: str = Form(AedtBackend.STANDALONE.value),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        partition: str = Form("auto"),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
        same_node_as_task_id: int = Form(0),
        priority: int = Form(0),
        cleanup_globs: str = Form(""),
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
                aedt_backend=parse_aedt_backend(aedt_backend),
                gpus=max(0, gpus),
                gpu_model=gpu_model,
                partition=partition,
                node_name=node_name,
                exclusive_node=exclusive_node,
                same_node_as_task_id=max(0, same_node_as_task_id),
                priority=priority,
                cleanup_globs=normalize_cleanup_globs(cleanup_globs),
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
        aedt_backend: str = Form(AedtBackend.STANDALONE.value),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
        same_node_as_task_id: int = Form(0),
        priority: int = Form(0),
        cleanup_globs: str = Form(""),
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
                aedt_backend=parse_aedt_backend(aedt_backend),
                gpus=max(0, gpus),
                gpu_model=gpu_model,
                partition=partition,
                node_name=node_name,
                exclusive_node=exclusive_node,
                same_node_as_task_id=max(0, same_node_as_task_id),
                payload_json=payload_json,
                priority=priority,
                cleanup_globs=normalize_cleanup_globs(cleanup_globs),
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

    @app.get("/nodes/{node_name}", response_class=HTMLResponse)
    def node_detail(node_name: str, request: Request) -> HTMLResponse:
        inventory = next(
            (row for row in db.list_node_inventory() if str(row.get("node_name") or "") == node_name),
            None,
        )
        pestat = next(
            (row for row in db.list_pestat_nodes() if str(row.get("hostname") or "") == node_name),
            None,
        )
        # Representative shape: the oldest queued FEA task, else a sane default.
        queued_fea = next(
            (
                task
                for task in db.list_tasks_by_statuses(["queued"], limit=200)
                if normalize_scheduling_profile(str(task.get("scheduling_profile") or ""))
                == SchedulingProfile.FEA_BURSTY.value
            ),
            None,
        )
        shape = queued_fea or {
            "cpus": 4,
            "memory_mb": 32768,
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
        }
        task_shape = {
            "cpus": max(1, int(shape.get("cpus") or 4)),
            "memory_mb": max(1, int(shape.get("memory_mb") or 32768)),
            "scheduling_profile": SchedulingProfile.FEA_BURSTY.value,
            "max_workers_per_node": 0,
        }
        diagnosis = scheduler.node_fill_diagnosis(node_name, task_shape)
        node_tasks = []
        allocation_ids = {int(item["allocation"]["id"]) for item in diagnosis["allocations"]}
        now = datetime.now(timezone.utc)
        for task in db.list_tasks_by_statuses(["attaching", "running"], limit=5000):
            if int(task.get("allocation_id") or 0) not in allocation_ids:
                continue
            node_tasks.append({**task, **task_view_fields(task, include_diagnostics=False)})
        return templates.TemplateResponse(
            "node_detail.html",
            {
                "request": request,
                "node_name": node_name,
                "inventory": inventory,
                "pestat": pestat,
                "diagnosis": diagnosis,
                "node_tasks": attach_task_elapsed(node_tasks),
                "shape_from_queue": bool(queued_fea),
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

    @app.post("/tasks/{task_id}/priority")
    def set_task_priority(task_id: int, priority: int = Form(...)) -> Response:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        if task["status"] != "queued":
            raise HTTPException(status_code=409, detail="only queued tasks can be reprioritized")
        db.update_task(task_id, priority=priority)
        return RedirectResponse("/", status_code=303)

    @app.post("/api/tasks/{task_id}/priority")
    async def api_set_task_priority(task_id: int, request: Request) -> dict:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        if task["status"] != "queued":
            raise HTTPException(status_code=409, detail="only queued tasks can be reprioritized")
        payload = await request.json()
        priority = int(payload.get("priority") or 0)
        db.update_task(task_id, priority=priority)
        return {"id": task_id, "priority": priority}

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
    def api_tasks(
        include_diagnostics: bool = False,
        limit: int = 0,
        project: str = "",
        name_prefix: str = "",
        status: list[str] | None = Query(default=None),
    ) -> list[dict]:
        allocation_rows = None
        allocation_by_id = None
        active_task_allocation_ids = None
        active_exclusive_allocation_ids = None
        if include_diagnostics:
            allocation_rows = db.list_allocations_with_live(limit=500)
            allocation_by_id = {int(allocation["id"]): allocation for allocation in allocation_rows}
            active_task_allocation_ids, active_exclusive_allocation_ids = scheduler.active_task_allocation_sets()
        statuses = normalize_task_status_filters(status)
        filtered = bool(project or name_prefix or statuses is not None)
        if filtered:
            # A filtered campaign read defaults to the API cap.  The database
            # applies every WHERE clause before LIMIT, so unrelated global
            # history cannot hide matching rows.
            task_limit = max(1, min(int(limit), 10000)) if limit else 10000
            tasks = db.list_tasks(
                limit=task_limit,
                project=project,
                name_prefix=name_prefix,
                statuses=statuses,
            )
        elif limit:
            # Preserve the existing explicit-limit behavior for unfiltered reads.
            tasks = db.list_tasks(limit=max(1, min(int(limit), 10000)))
        else:
            # Preserve the existing newest-plus-active behavior with no filters.
            tasks = db.list_tasks_with_active()
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
            for task in tasks
        ]

    @app.get("/api/tasks/summary")
    def api_tasks_summary(name_prefix: str = "") -> dict:
        """Counts by status, optionally for one campaign prefix — replaces
        client-side full-list scans."""
        counts = db.count_tasks_grouped_by_status(name_prefix=name_prefix)
        return {"name_prefix": name_prefix, "total": sum(counts.values()), "statuses": counts}

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
            "aedt_backend": parse_aedt_backend(payload.get("aedt_backend")),
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
            "requested_allocation_id": max(0, int(payload.get("requested_allocation_id") or 0)),
            "payload_json": payload_json,
            "cleanup_globs": payload.get("cleanup_globs"),
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
        task_ids: str = "",
    ) -> dict:
        requested_statuses = {part.strip() for part in statuses.split(",") if part.strip()}
        valid_statuses = {status.value for status in TaskStatus}
        unknown = sorted(requested_statuses - valid_statuses)
        if unknown:
            raise HTTPException(status_code=400, detail=f"unknown task status: {', '.join(unknown)}")
        ids = [int(part) for part in task_ids.split(",") if part.strip().isdigit()]
        if ids:
            cancelled_ids: list[int] = []
            for task_id in ids:
                try:
                    result = scheduler.request_cancel_task(task_id, expected_statuses=requested_statuses)
                    if result["cancelled"]:
                        cancelled_ids.append(task_id)
                except ValueError:
                    continue
            return {"cancelled": cancelled_ids, "count": len(cancelled_ids)}
        cancelled = scheduler.cancel_tasks(name_contains=name_contains, statuses=requested_statuses, limit=max(1, limit))
        return {"cancelled": cancelled, "count": len(cancelled)}

    @app.get("/api/allocations")
    def api_allocations() -> list[dict]:
        return annotate_allocation_fea_pressure(
            annotate_allocation_node_metrics(db.list_allocations_with_live(), db.list_pestat_nodes()),
            scheduler.fea_allocation_pressures(),
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
        aedt_backend: str = AedtBackend.STANDALONE.value,
        gpus: int = 0,
        gpu_model: str = "",
        required_capability: str = "",
        env_profile: str = "",
        project: str = "",
        account_name: str = "",
        partition: str = "auto",
        node_name: str = "",
        max_workers_per_node: int = 0,
    ) -> dict:
        task = {
            "cpus": max(1, cpus),
            "memory_mb": max(1, memory_mb),
            "scheduling_profile": normalize_scheduling_profile(scheduling_profile),
            "aedt_backend": parse_aedt_backend(aedt_backend),
            "gpus": max(0, gpus),
            "gpu_model": gpu_model,
            "required_capability": required_capability,
            "env_profile": env_profile,
            # Project is part of the FEA license-admission identity.  Without
            # it, a capacity probe for a configured project is indistinguish-
            # able from unknown/projectless work and correctly fails closed.
            "project": project.strip(),
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

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/control-plane-relay")
    def api_control_plane_relay() -> dict:
        return control_plane_relay.status()

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

    @app.post("/api/placement/dry-run")
    def api_placement_dry_run(
        cpus: int = Form(1),
        memory_mb: int = Form(4096),
        scheduling_profile: str = Form(SchedulingProfile.STANDARD.value),
        aedt_backend: str = Form(AedtBackend.STANDALONE.value),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        required_capability: str = Form(""),
        env_profile: str = Form(""),
        account_name: str = Form(""),
        partition: str = Form("auto"),
        node_name: str = Form(""),
        max_workers_per_node: int = Form(0),
    ) -> dict:
        task = {
            "cpus": max(1, cpus),
            "memory_mb": max(1, memory_mb),
            "scheduling_profile": normalize_scheduling_profile(scheduling_profile),
            "aedt_backend": parse_aedt_backend(aedt_backend),
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
        return scheduler.placement_dry_run(task)

    @app.get("/api/licenses")
    def api_licenses() -> dict:
        return scheduler.license_usage()

    @app.get("/api/dashboard-summary")
    def api_dashboard_summary(task_name_contains: str = "") -> dict:
        allocations = db.list_allocations_with_live(limit=500, live_limit=10000)
        allocated_rows = [
            item for item in allocations if item["state"] in {"active", "warm", "draining", "closing"}
        ]
        pending_count = sum(1 for item in allocations if item["state"] == "pending")
        return {
            "tasks": db.task_activity_summary(name_contains=task_name_contains),
            "allocations": allocation_usage_summary(allocated_rows, pending=pending_count),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    @app.get("/api/task-count-history")
    def api_task_count_history(
        hours: int = Query(default=24, ge=1, le=168),
    ) -> list[dict]:
        cutoff = time.time() - (hours * 60 * 60)
        return db.list_task_count_samples(since=cutoff, max_points=600)

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
    def api_cancel_task(task_id: int, expected_statuses: str = "") -> dict:
        requested_statuses = {part.strip() for part in expected_statuses.split(",") if part.strip()}
        valid_statuses = {status.value for status in TaskStatus}
        unknown = sorted(requested_statuses - valid_statuses)
        if unknown:
            raise HTTPException(status_code=400, detail=f"unknown task status: {', '.join(unknown)}")
        try:
            return scheduler.request_cancel_task(
                task_id,
                expected_statuses=requested_statuses if expected_statuses.strip() else None,
            )
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

    async def _json_body(request: Request) -> dict:
        try:
            data = await request.json()
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    @app.get("/api/projects")
    def api_projects() -> list[dict]:
        return [project_json(project) for project in db.list_projects()]

    @app.get("/api/projects/{name}")
    def api_get_project(name: str) -> dict:
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        return project_json(project)

    @app.patch("/api/projects/{name}/max-active-tasks")
    async def api_set_project_max_active_tasks(name: str, request: Request) -> dict:
        payload = await _json_body(request)
        if set(payload) != {"max_active_tasks"}:
            raise HTTPException(
                status_code=422,
                detail="request body must contain only max_active_tasks",
            )
        max_active_tasks = payload["max_active_tasks"]
        ceiling = config.project_max_active_tasks_ceiling
        if type(max_active_tasks) is not int or not 1 <= max_active_tasks <= ceiling:
            raise HTTPException(
                status_code=422,
                detail=f"max_active_tasks must be an integer between 1 and {ceiling}",
            )
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        project_id = int(project["id"])
        db.update_project(project_id, max_active_tasks=max_active_tasks)
        return project_json(db.get_project(project_id))

    @app.post("/api/projects")
    async def api_create_project(request: Request) -> JSONResponse:
        payload = await _json_body(request)
        try:
            project_id = upsert_project_from_payload(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return JSONResponse(project_json(db.get_project(project_id)), status_code=201)

    @app.delete("/api/projects/{name}")
    def api_delete_project(name: str) -> dict:
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        db.delete_project(int(project["id"]))
        return {"deleted": name}

    @app.post("/api/projects/{name}/deploy")
    async def api_deploy_project(name: str, request: Request) -> dict:
        payload = await _json_body(request)
        try:
            count = project_env_manager.deploy(name, parse_account_list(payload.get("target_accounts") or []))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"project": name, "deploying": count}

    @app.post("/api/projects/{name}/update")
    async def api_update_project(name: str, request: Request) -> dict:
        payload = await _json_body(request)
        targets = parse_account_list(payload.get("target_accounts") or []) or None
        try:
            count = project_env_manager.update(name, targets)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"project": name, "updating": count}

    @app.get("/api/projects/{name}/deployments")
    def api_project_deployments(name: str) -> list[dict]:
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        return db.list_project_deployments(int(project["id"]))

    @app.post("/projects")
    def create_project_form(
        name: str = Form(...),
        repos: str = Form(""),
        setup: str = Form(""),
        entrypoints: str = Form(""),
        cleanup_globs: str = Form(""),
        output_globs: str = Form(""),
        sim_subdir: str = Form("simulation"),
        auto_pull: str = Form(""),
        aedt_backend: str = Form(AedtBackend.STANDALONE.value),
    ) -> Response:
        try:
            upsert_project_from_payload(
                {
                    "name": name,
                    "repos": repos,
                    "setup": setup,
                    "entrypoints": entrypoints,
                    "cleanup_globs": cleanup_globs,
                    "output_globs": output_globs,
                    "sim_subdir": sim_subdir,
                    "auto_pull": bool(auto_pull),
                    "aedt_backend": aedt_backend,
                }
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return RedirectResponse(f"/projects/{name}", status_code=303)

    @app.post("/projects/{name}/deploy")
    def deploy_project_form(name: str, target_accounts: str = Form(...)) -> Response:
        try:
            project_env_manager.deploy(name, parse_account_list(target_accounts))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return RedirectResponse("/", status_code=303)

    @app.post("/projects/{name}/update")
    def update_project_form(name: str, target_accounts: str = Form("")) -> Response:
        try:
            project_env_manager.update(name, parse_account_list(target_accounts) or None)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return RedirectResponse("/", status_code=303)

    @app.post("/projects/{name}/delete")
    def delete_project_form(name: str) -> Response:
        project = db.get_project_by_name(name)
        if project:
            db.delete_project(int(project["id"]))
        return RedirectResponse("/", status_code=303)

    @app.get("/projects/{name}")
    def project_detail(request: Request, name: str) -> Response:
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            "project_detail.html",
            {
                "request": request,
                "project": project_json(project),
                "account_names": [account.name for account in accounts],
            },
        )

    def submit_project_run(
        name: str,
        parallel: int,
        entrypoint: str,
        account_name: str,
        cpus: int,
        memory_mb: int,
        arguments: str = "",
    ) -> int:
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404, detail=f"project not found: {name}")
        try:
            entrypoints = json.loads(project.get("entrypoints") or "[]")
        except (TypeError, json.JSONDecodeError):
            entrypoints = []
        chosen = (entrypoint or "").strip() or (str(entrypoints[0].get("path")) if entrypoints else "")
        if not chosen:
            raise HTTPException(status_code=422, detail="project has no entrypoint to run")
        count = max(1, min(int(parallel or 1), 1000))
        for index in range(count):
            create_task_record(
                {
                    "name": f"{name}-run-{index}",
                    "project": name,
                    "entrypoint": chosen,
                    "arguments": str(arguments or "").strip(),
                    "account_name": account_name or "",
                    "cpus": max(1, int(cpus or 4)),
                    "memory_mb": max(1, int(memory_mb or 32768)),
                    "scheduling_profile": "fea_bursty",
                }
            )
        return count

    @app.post("/projects/{name}/run")
    def run_project_form(
        name: str,
        parallel: int = Form(1),
        entrypoint: str = Form(""),
        arguments: str = Form(""),
        account_name: str = Form(""),
        cpus: int = Form(4),
        memory_mb: int = Form(32768),
    ) -> Response:
        submit_project_run(name, parallel, entrypoint, account_name, cpus, memory_mb, arguments)
        return RedirectResponse(f"/projects/{name}", status_code=303)

    @app.post("/api/projects/{name}/run")
    async def api_run_project(name: str, request: Request) -> dict:
        payload = await _json_body(request)
        count = submit_project_run(
            name,
            int(payload.get("parallel") or 1),
            str(payload.get("entrypoint") or ""),
            str(payload.get("account_name") or ""),
            int(payload.get("cpus") or 4),
            int(payload.get("memory_mb") or 32768),
            str(payload.get("arguments") or ""),
        )
        return {"project": name, "submitted": count}

    def collect_project_outputs(project: dict) -> list[tuple[str, str, bytes]]:
        """Gather files matching the project's output_globs from each deployed
        account's project tree. Returns (account, relative_path, content)."""
        globs = [g.strip() for g in str(project.get("output_globs") or "").split(",") if g.strip()]
        if not globs:
            return []
        project_rel = project_env_manager.project_rel_dir(str(project.get("name") or ""))
        name_expr = " -o ".join(f"-name {shlex.quote(g)}" for g in globs)
        collected: list[tuple[str, str, bytes]] = []
        for deployment in db.list_project_deployments(int(project["id"])):
            if deployment.get("status") != "deployed":
                continue
            account = next((item for item in accounts if item.name == deployment.get("account_name")), None)
            if not account:
                continue
            find_cmd = (
                f'cd "$HOME/"{shlex.quote(project_rel)} 2>/dev/null || exit 0; '
                f"find . -type f \\( {name_expr} \\) 2>/dev/null"
            )
            try:
                with SSHSession(account, default_timeout=300) as ssh:
                    result = ssh.run(find_cmd)
                    for line in result.stdout.splitlines():
                        rel = line.strip()
                        if rel.startswith("./"):
                            rel = rel[2:]
                        if not rel:
                            continue
                        try:
                            text = ssh.read_text_file(posixpath.join(project_rel, rel))
                        except Exception:
                            continue
                        collected.append((account.name, rel, text.encode("utf-8", "replace")))
            except Exception:
                # An unreachable/quota-full account just contributes no files.
                continue
        return collected

    @app.get("/projects/{name}/harvest.csv")
    def harvest_project_csv(name: str) -> Response:
        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        files = collect_project_outputs(project)
        lines: list[str] = []
        header: str | None = None
        for _account, _rel, content in files:
            rows = content.decode("utf-8", "replace").splitlines()
            if not rows:
                continue
            if header is None:
                header = rows[0]
                lines.append(header)
                lines.extend(rows[1:])
            else:
                lines.extend(rows[1:] if rows[0] == header else rows)
        body = ("\n".join(lines) + "\n") if lines else ""
        return Response(
            content=body,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{name}-results.csv"'},
        )

    @app.get("/projects/{name}/harvest.zip")
    def harvest_project_zip(name: str) -> Response:
        import io
        import zipfile

        project = db.get_project_by_name(name)
        if not project:
            raise HTTPException(status_code=404)
        files = collect_project_outputs(project)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for account_name, rel, content in files:
                archive.writestr(f"{account_name}/{rel}", content)
        return Response(
            content=buffer.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{name}-results.zip"'},
        )

    @app.get("/api/token-usage")
    def api_token_usage() -> list[dict]:
        return db.list_token_usage()

    return app


app = create_app(os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml"))
