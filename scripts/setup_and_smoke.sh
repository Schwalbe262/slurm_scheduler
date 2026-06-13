#!/usr/bin/env bash
set -euo pipefail

if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3 venv support is missing."
  echo "Run: sudo apt update && sudo apt install -y python3.12-venv python3-pip"
  exit 1
fi

if [ ! -d .venv/bin ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python - <<'PY'
from slurm_scheduler.app import create_app

app = create_app("config/app.yaml")
routes = sorted(route.path for route in app.routes)
required = {"/", "/jobs", "/api/jobs", "/api/accounts/status", "/api/token-usage"}
missing = sorted(required.difference(routes))
if missing:
    raise SystemExit(f"missing routes: {missing}")
print("fastapi_smoke_ok routes=" + ",".join(sorted(required)))
PY

echo "Run server with: . .venv/bin/activate && python3 -m slurm_scheduler"
