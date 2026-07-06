from __future__ import annotations

import uvicorn
import os

from .config import load_app_config


def main() -> None:
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
