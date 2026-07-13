"""Scheduler-managed, node-local shared AEDT canary host.

This is a bounded fallback for clusters that cannot route back to the live
scheduler HTTP service.  One scheduler task owns the loopback control plane
and one AEDT Desktop.  Exactly two co-located MFT tasks may attach.  It never
changes the production AEDT-pool database or enables the central pool.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.aedt_pool_1to1_pilot import _now, start_control_plane  # noqa: E402
from scripts.aedt_pool_1to2_pilot import (  # noqa: E402
    SharedPilotControlPlane,
    _run_host,
)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    os.chmod(temporary, 0o644)
    temporary.replace(path)


def _evidence(
    state: SharedPilotControlPlane,
    host_result: list[int],
    expected_projects: int | None = None,
) -> dict[str, Any]:
    failures = []
    expected = int(expected_projects or state.max_projects)
    if len(state.leases) != expected:
        failures.append(f"lease_count={len(state.leases)}")
    if (
        set(state.project_close_acks) != set(range(1, expected + 1))
        or not all(state.project_close_acks.values())
    ):
        failures.append(f"project_close_acks={state.project_close_acks!r}")
    if any(str(lease.get("failure_message") or "") for lease in state.leases.values()):
        failures.append("project_failure_reported")
    if any(str(lease.get("state") or "") != "released" for lease in state.leases.values()):
        failures.append(
            "lease_states="
            + repr({key: value.get("state") for key, value in state.leases.items()})
        )
    if state.session.get("state") != "closed" or not state.closed_ack:
        failures.append(f"session_state={state.session.get('state')!r}")
    if host_result != [0]:
        failures.append(f"session_host_exit={host_result!r}")
    return {
        "schema_version": 1,
        "mode": "scheduler_managed_node_local_canary",
        "passed": not failures,
        "failures": failures,
        "production_pool_enabled": False,
        "projects_per_aedt": expected,
        "lease_states": {
            str(key): value.get("state") for key, value in state.leases.items()
        },
        "project_names": {
            str(key): value.get("project_name") for key, value in state.leases.items()
        },
        "project_close_acks": state.project_close_acks,
        "session": state.session,
        "events": state.events,
        "completed_at": _now(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery-file", required=True)
    parser.add_argument("--evidence-file", required=True)
    parser.add_argument("--rollback-file", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=14400)
    parser.add_argument("--expected-projects", type=int, default=2)
    args = parser.parse_args(argv)

    discovery = Path(args.discovery_file).resolve()
    evidence_path = Path(args.evidence_file).resolve()
    rollback = Path(args.rollback_file).resolve()
    for path in (discovery, evidence_path, rollback):
        if not str(path).startswith("/tmp/"):
            raise SystemExit("node-local canary coordination files must be under /tmp")
    if discovery.exists() or evidence_path.exists():
        raise SystemExit("refusing to reuse stale node-local canary files")

    if not 1 <= int(args.expected_projects) <= 32:
        raise SystemExit("--expected-projects must be between 1 and 32")
    state = SharedPilotControlPlane(max_projects=args.expected_projects)
    server, server_thread, scheduler_url = start_control_plane(state)
    host = None
    host_thread = None
    host_result: list[int] = []
    stop_reason = ""

    def request_rollback(*_args: Any) -> None:
        nonlocal stop_reason
        stop_reason = "node-local canary rollback requested"

    signal.signal(signal.SIGTERM, request_rollback)
    signal.signal(signal.SIGINT, request_rollback)
    try:
        host, host_thread, host_result = _run_host(state, scheduler_url)
        nonce = uuid.uuid4().hex
        payload = {
            "schema_version": 1,
            "mode": "scheduler_managed_node_local_canary",
            "scheduler_url": scheduler_url,
            "expected_projects": int(args.expected_projects),
            "node": os.environ.get("SLURMD_NODENAME") or os.uname().nodename,
            "host_pid": os.getpid(),
            "desktop_pid": state.session.get("process_id"),
            "rollback_file": str(rollback),
            "nonce": nonce,
            "ready_at": _now(),
        }
        _atomic_json(discovery, payload)
        print("NODE_CANARY_DISCOVERY " + json.dumps(payload, sort_keys=True), flush=True)

        deadline = time.monotonic() + max(60, int(args.timeout_seconds))
        while host_thread.is_alive():
            if rollback.exists() or stop_reason:
                state.force_drain(stop_reason or "node-local canary rollback file observed")
            if time.monotonic() >= deadline:
                state.force_drain("node-local canary host deadline exceeded")
                stop_reason = "deadline"
            host_thread.join(timeout=1)
        result = _evidence(state, host_result, args.expected_projects)
        if stop_reason:
            result["failures"].append(f"rollback={stop_reason}")
            result["passed"] = False
        _atomic_json(evidence_path, result)
        print("NODE_CANARY_EVIDENCE " + json.dumps(result, sort_keys=True), flush=True)
        return 0 if result["passed"] else 2
    finally:
        if host_thread and host_thread.is_alive():
            state.force_drain("node-local canary finalizer")
            host_thread.join(timeout=60)
        if host_thread and host_thread.is_alive() and host is not None:
            host.request_stop()
            host_thread.join(timeout=40)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=10)
        try:
            if discovery.exists():
                discovery.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
