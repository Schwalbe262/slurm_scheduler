from __future__ import annotations

import logging
import logging.handlers
import os
import signal
import socket
from pathlib import Path

import uvicorn

from .config import load_app_config
from .web_supervisor import WebWorkerSupervisor, probe_listener


LOGGER = logging.getLogger(__name__)
DUPLICATE_LISTENER_EXIT_CODE = 98
UVICORN_STARTUP_FAILURE_EXIT_CODE = 3


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


def _reserve_listener(host: str, port: int) -> socket.socket:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    listener = socket.socket(family, socket.SOCK_STREAM)
    try:
        # Uvicorn's SO_REUSEADDR bind can admit identical pre-listen binds on
        # Windows.  A plain socket reserves this exact endpoint exclusively
        # while still coexisting with address-specific Tailscale listeners.
        if os.name != "nt":
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, int(port)))
        return listener
    except OSError:
        listener.close()
        raise


def run_uvicorn_worker(config) -> None:
    try:
        listener = _reserve_listener(config.bind_host, config.bind_port)
    except OSError as exc:
        LOGGER.error(
            "cannot bind listener %s:%s; refusing duplicate scheduler generation: %s",
            config.bind_host,
            config.bind_port,
            exc,
        )
        raise SystemExit(DUPLICATE_LISTENER_EXIT_CODE) from None

    server = None
    try:
        uvicorn_config = uvicorn.Config(
            "slurm_scheduler.app:app",
            host=config.bind_host,
            port=config.bind_port,
            reload=False,
            timeout_keep_alive=max(1, int(config.web_timeout_keep_alive_seconds or 5)),
            timeout_graceful_shutdown=max(
                1, int(config.web_timeout_graceful_shutdown_seconds or 15)
            ),
            limit_concurrency=max(1, int(config.web_limit_concurrency or 64)),
        )
        server = uvicorn.Server(uvicorn_config)
        try:
            server.run(sockets=[listener])
        except KeyboardInterrupt:
            pass
    finally:
        listener.close()
    if server is not None and not server.started:
        raise SystemExit(UVICORN_STARTUP_FAILURE_EXIT_CODE)


def main() -> None:
    config_path = os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml")
    config = load_app_config(config_path)
    worker_mode = os.environ.get("SLURM_SCHEDULER_UVICORN_WORKER") == "1"
    configure_logging(
        "scheduler.log"
        if worker_mode or not config.web_listener_watchdog_enabled
        else "web-supervisor.log"
    )
    if probe_listener(
        config.bind_host,
        config.bind_port,
        config.web_listener_probe_timeout_seconds,
    ):
        LOGGER.error(
            "listener %s:%s is already active; refusing to start a duplicate scheduler generation",
            config.bind_host,
            config.bind_port,
        )
        raise SystemExit(DUPLICATE_LISTENER_EXIT_CODE)
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
    )
    signal.signal(signal.SIGTERM, supervisor.request_stop)
    signal.signal(signal.SIGINT, supervisor.request_stop)
    raise SystemExit(supervisor.run())


if __name__ == "__main__":
    main()
