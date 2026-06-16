from __future__ import annotations

import json
import os
from pathlib import Path
from math import ceil
import posixpath
from datetime import datetime, timezone

from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .config import AppConfig, load_accounts, load_app_config
from .db import Database
from .models import JobCreate, TaskCreate
from .inventory import partition_rank
from .pestat import PestatNode, plan_dynamic_allocations
from .scheduler import Scheduler
from .slurm import SlurmAccountClient
from .task_commands import ACCOUNT_WORKSPACE_PLACEHOLDER, build_git_task_command

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


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
    scheduler = Scheduler(
        db,
        accounts,
        config.poll_interval_seconds,
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
        cpu_pool_allow_gpu_partitions=config.cpu_pool_allow_gpu_partitions,
        warm_pool_preferred_accounts=config.warm_pool_preferred_accounts,
        gpu_warm_pool_preferred_accounts=config.gpu_warm_pool_preferred_accounts,
        single_job_per_node_partitions=config.single_job_per_node_partitions,
        gpu_cpu_reserve=config.gpu_cpu_reserve,
        gpu_prewarm_enabled=config.gpu_prewarm_enabled,
        gpu_prewarm_preferred_models=config.gpu_prewarm_preferred_models,
        gpu_prewarm_min_warm_allocations=config.gpu_prewarm_min_warm_allocations,
        gpu_prewarm_max_warm_allocations=config.gpu_prewarm_max_warm_allocations,
        gpu_prewarm_gpus_per_allocation=config.gpu_prewarm_gpus_per_allocation,
        gpu_prewarm_cpu_reserve_per_free_gpu=config.gpu_prewarm_cpu_reserve_per_free_gpu,
        gpu_prewarm_partition=config.gpu_prewarm_partition,
        gpu_prewarm_time_limit=config.gpu_prewarm_time_limit,
        cleanup_enabled=config.cleanup_enabled,
        cleanup_interval_seconds=config.cleanup_interval_seconds,
        cleanup_finished_task_ttl_seconds=config.cleanup_finished_task_ttl_seconds,
        cleanup_finished_job_ttl_seconds=config.cleanup_finished_job_ttl_seconds,
        cleanup_closed_allocation_ttl_seconds=config.cleanup_closed_allocation_ttl_seconds,
    )

    app = FastAPI(title="Slurm Scheduler")
    app.state.config = config
    app.state.db = db
    app.state.scheduler = scheduler

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
            text = SlurmAccountClient(account).read_text_file(path)
        except Exception:
            return ""
        if limit > 0 and not max_bytes and len(text) > limit:
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

    def task_json(task: dict, include_output: bool = False, output_limit: int = 65536, derive_failure_message: bool = True) -> dict:
        allocation = db.get_allocation(int(task["allocation_id"])) if task.get("allocation_id") else None
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
            "priority": int(task.get("priority") or 0),
            "timeout_seconds": int(task.get("timeout_seconds") or 0),
            "dedupe_key": task.get("dedupe_key") or "",
            "max_workers_per_node": int(task.get("max_workers_per_node") or 0),
            "urls": task_result_urls(int(task["id"])),
        }
        if include_output:
            stdout = read_task_file_text(task, "stdout_path", output_limit)
            payload["stdout"] = stdout
            payload["stderr"] = read_task_file_text(task, "stderr_path", output_limit)
            payload["result_json"] = last_json_object(stdout)
        return payload

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
            gpus=max(0, int(payload.get("gpus") or 0)),
            gpu_model=str(payload.get("gpu_model") or ""),
            partition=str(payload.get("partition") or "auto"),
            node_name=str(payload.get("node_name") or ""),
            exclusive_node=bool(payload.get("exclusive_node") or False),
            priority=int(payload.get("priority") or 0),
            timeout_seconds=max(0, int(payload.get("timeout_seconds") or payload.get("timeout") or 0)),
            dedupe_key=dedupe_key,
            max_workers_per_node=max(0, int(payload.get("max_workers_per_node") or 0)),
            payload_json=payload_json,
        )
        if not task.remote_cwd:
            raise HTTPException(status_code=422, detail="remote_cwd is required")
        if not task.command:
            raise HTTPException(status_code=422, detail="command is required")
        return db.create_task(task), False

    @app.on_event("startup")
    def _startup() -> None:
        scheduler.start()

    @app.on_event("shutdown")
    def _shutdown() -> None:
        scheduler.stop()

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request) -> HTMLResponse:
        snapshots = scheduler.cached_snapshots()
        snapshot_error = "" if snapshots else "Account status will appear after the background scheduler refreshes."
        allocations = db.list_allocations(limit=500)
        terminal_allocation_states = {"closed", "failed"}
        active_allocations = sorted(
            [item for item in allocations if item["state"] not in terminal_allocation_states],
            key=allocation_sort_key,
        )
        allocated_summary_rows = [
            item
            for item in active_allocations
            if item["state"] in {"active", "warm", "draining", "closing"}
        ]
        allocation_summary = allocation_usage_summary(allocated_summary_rows)
        closed_allocations = [item for item in allocations if item["state"] in terminal_allocation_states]
        active_task_rows = {
            int(task["id"]): task
            for task in (
                db.list_tasks_by_statuses(["running", "attaching"], limit=5000)
                + db.list_tasks_by_statuses(["queued"], limit=50)
            )
        }
        active_tasks = sorted(attach_task_elapsed(list(active_task_rows.values())), key=task_display_sort_key)
        finished_tasks = attach_task_elapsed(db.list_tasks_by_statuses(["completed", "failed", "cancelled"], limit=50))
        finished_task_count = db.count_tasks_by_statuses(["completed", "failed", "cancelled"])
        jobs = job_elapsed(db.list_jobs())
        active_jobs = [item for item in jobs if item["status"] not in {"completed", "failed", "cancelled"}]
        finished_jobs = [item for item in jobs if item["status"] in {"completed", "failed", "cancelled"}]
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "jobs": active_jobs,
                "finished_jobs": finished_jobs[:50],
                "finished_job_count": len(finished_jobs),
                "tasks": active_tasks,
                "finished_tasks": finished_tasks,
                "finished_task_count": finished_task_count,
                "allocations": active_allocations,
                "allocation_summary": allocation_summary,
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
            },
        )

    @app.post("/jobs")
    def create_job(
        job_mode: str = Form("python_git"),
        repo_url: str = Form(""),
        git_ref: str = Form("main"),
        entrypoint: str = Form(...),
        arguments: str = Form(""),
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
            db.create_task(
                TaskCreate(
                    name=job_name or "git-task",
                    remote_cwd=ACCOUNT_WORKSPACE_PLACEHOLDER,
                    command=build_git_task_command(repo_url, git_ref, entrypoint, arguments),
                    env_setup=env_setup,
                    required_capability=required_capability,
                    env_profile=env_profile,
                    account_name=account_name,
                    cpus=max(1, cpus),
                    memory_mb=max(1, parse_memory_mb(memory)),
                    gpus=max(0, gpus),
                    gpu_model=gpu_model,
                    partition=partition,
                    node_name=node_name,
                    exclusive_node=exclusive_node,
                )
            )
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
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        partition: str = Form("auto"),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
    ) -> Response:
        db.create_task(
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
                gpus=max(0, gpus),
                gpu_model=gpu_model,
                partition=partition,
                node_name=node_name,
                exclusive_node=exclusive_node,
            )
        )
        return RedirectResponse("/", status_code=303)

    @app.post("/tasks/git")
    def create_git_task(
        job_name: str = Form("git-task"),
        repo_url: str = Form(...),
        git_ref: str = Form("main"),
        entrypoint: str = Form(...),
        arguments: str = Form(""),
        env_setup: str = Form(""),
        required_capability: str = Form(""),
        env_profile: str = Form(""),
        account_name: str = Form(""),
        partition: str = Form("auto"),
        cpus: int = Form(1),
        memory: str = Form("4G"),
        gpus: int = Form(0),
        gpu_model: str = Form(""),
        node_name: str = Form(""),
        exclusive_node: bool = Form(False),
    ) -> Response:
        db.create_task(
            TaskCreate(
                name=job_name or "git-task",
                remote_cwd=ACCOUNT_WORKSPACE_PLACEHOLDER,
                command=build_git_task_command(repo_url, git_ref, entrypoint, arguments),
                env_setup=env_setup,
                required_capability=required_capability,
                env_profile=env_profile,
                account_name=account_name,
                cpus=max(1, cpus),
                memory_mb=max(1, parse_memory_mb(memory)),
                gpus=max(0, gpus),
                gpu_model=gpu_model,
                partition=partition,
                node_name=node_name,
                exclusive_node=exclusive_node,
            )
        )
        return RedirectResponse("/", status_code=303)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(job_id: int, request: Request) -> HTMLResponse:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse("job_detail.html", {"request": request, "job": job})

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
    def api_tasks() -> list[dict]:
        return [task_json(task, derive_failure_message=False) for task in db.list_tasks()]

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
        return db.list_allocations()

    @app.get("/api/gpu-capacity")
    def api_gpu_capacity() -> list[dict]:
        return scheduler.gpu_capacity_summary()

    @app.get("/api/task-capacity")
    def api_task_capacity(
        cpus: int = 1,
        memory_mb: int = 4096,
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
        return scheduler.task_fit_capacity(task)

    @app.get("/api/health")
    def api_health() -> dict:
        return {
            "ok": True,
            "accounts": len(accounts),
            "jobs": len(db.list_jobs()),
            "tasks": len(db.list_tasks()),
            "allocations": len(db.list_allocations()),
        }

    @app.get("/api/jobs/{job_id}")
    def api_job(job_id: int) -> dict:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        return job

    @app.get("/api/jobs/{job_id}/remote-file")
    def api_job_remote_file(job_id: int, path: str, base: str = "remote_path") -> Response:
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
        try:
            text = SlurmAccountClient(account).read_text_file(remote_file)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return Response(text, media_type="text/plain")

    @app.get("/api/tasks/{task_id}")
    def api_task(task_id: int, include_output: bool = False, output_limit: int = 65536) -> dict:
        task = db.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404)
        return task_json(task, include_output=include_output, output_limit=max(0, output_limit))

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
        try:
            text = SlurmAccountClient(account).read_text_file(remote_file)
        except FileNotFoundError:
            text = ""
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
            files = SlurmAccountClient(account).list_files(root, glob)
        except FileNotFoundError:
            files = []
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
        try:
            text = SlurmAccountClient(account).read_text_file(task["stdout_path"])
        except FileNotFoundError:
            text = ""
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
        try:
            text = SlurmAccountClient(account).read_text_file(task["stderr_path"])
        except FileNotFoundError:
            text = ""
        return Response(apply_text_window(text, tail_lines=max(0, tail_lines), max_bytes=max(0, max_bytes)), media_type="text/plain")

    @app.get("/api/accounts/status")
    def api_accounts() -> list[dict]:
        return [snapshot.__dict__ for snapshot in scheduler.cached_snapshots()]

    @app.get("/api/accounts/status/live")
    def api_accounts_live() -> list[dict]:
        return [snapshot.__dict__ for snapshot in scheduler.snapshots()]

    @app.get("/api/token-usage")
    def api_token_usage() -> list[dict]:
        return db.list_token_usage()

    return app


app = create_app(os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml"))
