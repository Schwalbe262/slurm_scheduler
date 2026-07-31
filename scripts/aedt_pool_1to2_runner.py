"""Fail-closed protocol-v2 bridge for the pinned MFT 1:2 pilot runner.

The disposable pilot's MFT revision predates the protocol-v2 lease keyword
arguments.  This process-local wrapper supplies the exact harness-owned
contract before that adapter calls the real scheduler attach client.  It does
not change the production client or relax the loopback control plane.
"""

from __future__ import annotations

import functools
import os
import re
import runpy
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slurm_scheduler import aedt_attach_client  # noqa: E402
from slurm_scheduler.aedt_session_host import (  # noqa: E402
    EXPECTED_SESSION_PROFILE_JSON,
    canonical_expected_session_profile,
)


RUNNER_WORKSPACE_ENV = "MFT_AEDT_PILOT_WORKSPACE"
RUNNER_LABEL_ENV = "MFT_AEDT_PILOT_CLIENT_LABEL"
TASK_ID_ENV = "SLURM_SCHED_TASK_ID"
SCHEDULER_URL_ENV = "MFT_AEDT_SCHEDULER_URL"


def _required_env(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for the 1:2 pilot runner")
    return value


def _runner_contract() -> dict[str, Any]:
    if os.environ.get("MFT_AEDT_BACKEND", "").strip().lower() != "pooled":
        raise RuntimeError("the 1:2 pilot runner requires MFT_AEDT_BACKEND=pooled")
    if os.environ.get("MFT_AEDT_SHARED_1TO2_PILOT", "").strip() != "1":
        raise RuntimeError(
            "the 1:2 pilot runner requires its explicit shared-pilot acknowledgement"
        )
    task_text = _required_env(TASK_ID_ENV)
    if not task_text.isdigit() or int(task_text) <= 0:
        raise RuntimeError(f"{TASK_ID_ENV} must be a positive integer")
    task_id = int(task_text)
    label = _required_env(RUNNER_LABEL_ENV)
    if label not in {"A", "B"}:
        raise RuntimeError(f"{RUNNER_LABEL_ENV} must be A or B")

    workspace_text = _required_env(RUNNER_WORKSPACE_ENV)
    workspace = os.path.normpath(workspace_text)
    workspace_real = os.path.realpath(workspace)
    if (
        not os.path.isabs(workspace)
        or workspace != workspace_text.rstrip("/\\")
        or workspace != workspace_real
        or not Path(workspace).is_dir()
    ):
        raise RuntimeError("the 1:2 pilot workspace must be an existing canonical path")
    task_token = re.compile(
        rf"(?:^|[-_.]){re.escape(str(task_id))}(?:$|[-_.])"
    )
    if task_token.search(os.path.basename(workspace)) is None:
        raise RuntimeError(
            "the 1:2 pilot workspace must contain its exact task id token"
        )

    return {
        "task_id": task_id,
        "exclusive_session": False,
        "workload_family": "mft",
        "session_profile": EXPECTED_SESSION_PROFILE_JSON,
        "project_namespace": f"mft-1to2-{label.lower()}",
        "isolation_policy": "family",
        "workspace_path": workspace,
        "protocol_version": 2,
    }


def acquire_pinned_mft_lease(
    acquire: Callable[..., Any],
    scheduler_url: str,
    project_name: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Enrich one exact legacy-adapter call, rejecting every mismatch."""

    if args:
        raise RuntimeError("the pinned MFT lease adapter used unexpected arguments")
    configured_url = _required_env(SCHEDULER_URL_ENV).rstrip("/")
    if str(scheduler_url).rstrip("/") != configured_url:
        raise RuntimeError("the pinned MFT lease adapter changed scheduler URL")

    contract = _runner_contract()
    task_id = int(contract["task_id"])
    if type(kwargs.get("task_id")) is not int or kwargs["task_id"] != task_id:
        raise RuntimeError("the pinned MFT lease task identity is invalid")
    if kwargs.get("exclusive_session") is not False:
        raise RuntimeError(
            "the pinned MFT lease shared-session acknowledgement is invalid"
        )
    request_key = str(kwargs.get("request_key") or "")
    if not request_key.startswith(f"mft-1to2:{task_id}:"):
        raise RuntimeError("the pinned MFT lease request key is invalid")
    if not str(project_name).startswith(f"mft-pending-{task_id}-"):
        raise RuntimeError("the pinned MFT pending project identity is invalid")

    for field, expected in contract.items():
        supplied = kwargs.get(field)
        if field == "session_profile" and supplied:
            try:
                supplied = canonical_expected_session_profile(supplied)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "the pinned MFT lease session profile is invalid"
                ) from exc
        if supplied not in {None, ""} and supplied != expected:
            raise RuntimeError(
                f"the pinned MFT lease {field} does not match the pilot contract"
            )
        kwargs[field] = expected

    return acquire(scheduler_url, project_name, **kwargs)


def install_runner_contract() -> None:
    """Patch only this disposable runner process's attach-client module."""

    original = aedt_attach_client.acquire_project_lease
    if getattr(original, "_aedt_1to2_pilot_contract", False):
        return

    @functools.wraps(original)
    def wrapped(
        scheduler_url: str,
        project_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return acquire_pinned_mft_lease(
            original,
            scheduler_url,
            project_name,
            *args,
            **kwargs,
        )

    wrapped._aedt_1to2_pilot_contract = True  # type: ignore[attr-defined]
    aedt_attach_client.acquire_project_lease = wrapped


def main(argv: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if not values:
        raise RuntimeError("the pinned MFT entrypoint is required")
    entrypoint = Path(values[0]).resolve()
    if (
        entrypoint.name != "run_simulation_260706.py"
        or not entrypoint.is_file()
        or entrypoint.parent != Path.cwd().resolve()
    ):
        raise RuntimeError("the pinned MFT entrypoint or working directory changed")

    workspace = Path(str(_runner_contract()["workspace_path"]))
    simulation_link = Path.cwd() / "simulation"
    if (
        not simulation_link.is_symlink()
        or simulation_link.resolve() != workspace
    ):
        raise RuntimeError("the pinned MFT simulation workspace link is invalid")

    install_runner_contract()
    sys.path.insert(0, str(entrypoint.parent))
    sys.argv = [str(entrypoint), *values[1:]]
    runpy.run_path(str(entrypoint), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
