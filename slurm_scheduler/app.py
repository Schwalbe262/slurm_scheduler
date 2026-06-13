from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .config import AppConfig, load_accounts, load_app_config
from .db import Database
from .models import JobCreate
from .inventory import partition_rank
from .scheduler import Scheduler

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


def create_app(config_path: str = "config/app.yaml") -> FastAPI:
    config = load_app_config(config_path)
    accounts = load_accounts(config.accounts_path)
    db = Database(config.database_path)
    db.init()
    scheduler = Scheduler(db, accounts, config.poll_interval_seconds)

    app = FastAPI(title="Slurm Scheduler")
    app.state.config = config
    app.state.db = db
    app.state.scheduler = scheduler

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
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "jobs": db.list_jobs(),
                "snapshots": snapshots,
                "snapshot_error": snapshot_error,
                "token_usage": db.list_token_usage(),
                "token_summary": db.token_usage_summary(),
                "token_chart": build_token_chart(db.list_token_usage()),
                "cpu_partitions": partition_rank(db.list_node_inventory(), needs_gpu=False),
                "gpu_partitions": partition_rank(db.list_node_inventory(), needs_gpu=True),
            },
        )

    @app.post("/jobs")
    def create_job(
        repo_url: str = Form(...),
        git_ref: str = Form("main"),
        entrypoint: str = Form(...),
        arguments: str = Form(""),
        env_setup: str = Form(""),
        partition: str = Form("auto"),
        time_limit: str = Form("01:00:00"),
        cpus: int = Form(1),
        memory: str = Form("4G"),
        gpus: int = Form(0),
        job_name: str = Form("web-job"),
    ) -> Response:
        db.create_job(
            JobCreate(
                repo_url=repo_url,
                git_ref=git_ref,
                entrypoint=entrypoint,
                arguments=arguments,
                env_setup=env_setup,
                partition=partition,
                time_limit=time_limit,
                cpus=cpus,
                memory=memory,
                gpus=gpus,
                job_name=job_name,
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
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)

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

    @app.get("/api/jobs/{job_id}")
    def api_job(job_id: int) -> dict:
        job = db.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404)
        return job

    @app.get("/api/accounts/status")
    def api_accounts() -> list[dict]:
        return [snapshot.__dict__ for snapshot in scheduler.cached_snapshots()]

    @app.get("/api/token-usage")
    def api_token_usage() -> list[dict]:
        return db.list_token_usage()

    return app


app = create_app(os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml"))
