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
    )


if __name__ == "__main__":
    main()
