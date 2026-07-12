from __future__ import annotations

import logging
import logging.handlers
import os
import signal
from pathlib import Path

import uvicorn

from .config import load_app_config
from .web_supervisor import WebWorkerSupervisor


def configure_logging(log_filename: str = "scheduler.log") -> None:
    level_name = os.environ.get("SLURM_SCHEDULER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    root.addHandler(stream)
    try:
        Path("logs").mkdir(exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            str(Path("logs") / log_filename),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except OSError:
        pass


def run_uvicorn_worker(config) -> None:
    uvicorn.run(
        "slurm_scheduler.app:app",
        host=config.bind_host,
        port=config.bind_port,
        reload=False,
        timeout_keep_alive=max(1, int(config.web_timeout_keep_alive_seconds or 5)),
        timeout_graceful_shutdown=max(1, int(config.web_timeout_graceful_shutdown_seconds or 15)),
        limit_concurrency=max(1, int(config.web_limit_concurrency or 64)),
    )


def main() -> None:
    config_path = os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml")
    config = load_app_config(config_path)
    worker_mode = os.environ.get("SLURM_SCHEDULER_UVICORN_WORKER") == "1"
    configure_logging(
        "scheduler.log"
        if worker_mode or not config.web_listener_watchdog_enabled
        else "web-supervisor.log"
    )
    if worker_mode or not config.web_listener_watchdog_enabled:
        run_uvicorn_worker(config)
        return
    supervisor = WebWorkerSupervisor(
        host=config.bind_host,
        port=config.bind_port,
        probe_interval_seconds=config.web_listener_probe_interval_seconds,
        startup_grace_seconds=config.web_listener_startup_grace_seconds,
        failure_threshold=config.web_listener_failure_threshold,
        probe_timeout_seconds=config.web_listener_probe_timeout_seconds,
        restart_delay_seconds=config.web_listener_restart_delay_seconds,
    )
    signal.signal(signal.SIGTERM, supervisor.request_stop)
    signal.signal(signal.SIGINT, supervisor.request_stop)
    raise SystemExit(supervisor.run())


if __name__ == "__main__":
    main()
