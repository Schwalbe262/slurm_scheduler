from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

import uvicorn

from .config import load_app_config


def configure_logging() -> None:
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
            "logs/scheduler.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except OSError:
        pass


def main() -> None:
    configure_logging()
    config_path = os.environ.get("SLURM_SCHEDULER_CONFIG", "config/app.yaml")
    config = load_app_config(config_path)
    uvicorn.run(
        "slurm_scheduler.app:app",
        host=config.bind_host,
        port=config.bind_port,
        reload=False,
        timeout_keep_alive=max(1, int(config.web_timeout_keep_alive_seconds or 5)),
        timeout_graceful_shutdown=max(1, int(config.web_timeout_graceful_shutdown_seconds or 15)),
        limit_concurrency=max(1, int(config.web_limit_concurrency or 64)),
    )


if __name__ == "__main__":
    main()
