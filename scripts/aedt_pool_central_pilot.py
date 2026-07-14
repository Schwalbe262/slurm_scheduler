"""Disposable end-to-end pilot for the live central AEDT pool."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slurm_scheduler.aedt_attach_client import acquire_project_lease  # noqa: E402


@contextmanager
def _timed(timings: dict[str, float], name: str):
    started = time.monotonic()
    try:
        yield
    finally:
        timings[name] = round(time.monotonic() - started, 3)


def _emit(path: Path, evidence: dict[str, Any]) -> None:
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(evidence, separators=(",", ":"), ensure_ascii=False), flush=True)


def _short_host(value: str) -> str:
    return value.strip().split(".", 1)[0].casefold()

def _desktop_pid(desktop: Any) -> int:
    for name in ("aedt_process_id", "process_id", "pid"):
        try:
            pid = int(getattr(desktop, name, 0) or 0)
        except (TypeError, ValueError):
            continue
        if pid > 0:
            return pid
    get_pid = getattr(getattr(desktop, "odesktop", None), "GetProcessID", None)
    pid = int(get_pid() or 0) if callable(get_pid) else 0
    if pid <= 0:
        raise RuntimeError("could not determine the attached AEDT Desktop PID")
    return pid

def _grpc_round_trip(desktop: Any, lease: Any, design_name: str, saved_path: Path) -> Any:
    from ansys.aedt.core import get_pyaedt_app

    odesktop = getattr(desktop, "odesktop", None)
    if odesktop is None:
        raise RuntimeError("attached Desktop has no AEDT automation object")
    oproject = odesktop.NewProject()
    project_name = str(oproject.GetName() or "")
    if not project_name:
        raise RuntimeError("AEDT did not create a project")
    lease.bind_project_name(project_name)
    oproject.Rename(str(saved_path), False)
    project_name = str(oproject.GetName() or "")
    if not project_name:
        raise RuntimeError("AEDT did not name the saved project")
    lease.bind_project_name(project_name)
    oproject.InsertDesign("Maxwell 3D", design_name, "Magnetostatic", "")
    odesign = oproject.GetActiveDesign()
    if (
        odesign is None
        or str(odesign.GetName()) != design_name
        or str(odesign.GetDesignType()) != "Maxwell 3D"
    ):
        raise RuntimeError("AEDT did not insert the requested Maxwell 3D design")
    maxwell = get_pyaedt_app(project_name, design_name, desktop=desktop)
    if maxwell is None:
        raise RuntimeError("PyAEDT did not attach to the Maxwell 3D design")
    box = maxwell.modeler.create_box(
        [0, 0, 0], [1, 1, 1], name="CentralPilotBox", material="vacuum"
    )
    if not box:
        raise RuntimeError("AEDT did not create the pilot box")
    if maxwell.save_project() is False or not saved_path.is_file():
        raise RuntimeError(f"AEDT did not save the pilot project: {saved_path}")
    return maxwell


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scheduler-url", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--node-name", default=socket.gethostname().split(".")[0])
    default_name = f"central-pilot-{time.strftime('%Y%m%d-%H%M%S')}"
    parser.add_argument("--project-name", default=default_name)
    parser.add_argument("--lease-timeout-seconds", type=int, default=600)
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args(argv)

    output_path = Path(args.output_json).expanduser().resolve()
    saved_path = output_path.parent / f"{args.project_name}.aedt"
    timings: dict[str, float] = {}
    total_started = time.monotonic()
    phase = "prepare"
    lease = desktop = maxwell = None
    desktop_pid = session_port = None
    session_node_name = ""
    try:
        if not args.project_name or any(char in args.project_name for char in "/\\"):
            raise ValueError("project-name must be a non-empty file name")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if saved_path.exists():
            raise FileExistsError(f"pilot project already exists: {saved_path}")
        phase = "acquire_lease"
        with _timed(timings, phase):
            lease = acquire_project_lease(
                args.scheduler_url,
                args.project_name,
                bootstrap_token_file=args.token_file,
                node_name=args.node_name,
                task_id=int(os.environ.get("SLURM_SCHEDULER_TASK_ID", "0") or 0),
            )
        phase = "wait_until_leased"
        with _timed(timings, phase):
            status = lease.wait_until_leased(timeout_seconds=args.lease_timeout_seconds)
        phase = "start_heartbeat"
        lease.start_heartbeat()
        phase = "validate_endpoint"
        endpoint_host, port_text = lease.endpoint.rsplit(":", 1)
        session_port = int(port_text)
        session_node_name = str(status.get("session_node_name") or endpoint_host)
        expected = _short_host(args.node_name)
        if _short_host(session_node_name) != expected:
            raise RuntimeError(f"endpoint {lease.endpoint} is not on node {args.node_name}")
        phase = "connect_desktop"
        with _timed(timings, phase):
            desktop = lease.connect_desktop()
            desktop_pid = _desktop_pid(desktop)
        phase = "grpc_round_trip"
        with _timed(timings, phase):
            maxwell = _grpc_round_trip(desktop, lease, args.project_name, saved_path)
        # The Desktop is shared; only the session host may close or terminate it.
        phase = "release_close_ack"
        with _timed(timings, phase):
            release_status = lease.release(wait_seconds=300)
        final_state = str(release_status.get("state") or lease.state)
        if final_state != "released":
            raise RuntimeError(f"project close ACK failed; lease state is {final_state!r}")
        timings["total"] = round(time.monotonic() - total_started, 3)
        evidence = {
            "ok": True,
            "phase_timings_seconds": timings,
            "lease_id": lease.lease_id,
            "endpoint": lease.endpoint,
            "session_grpc_port": session_port,
            "desktop_pid": desktop_pid,
            "node_name": args.node_name,
            "session_node_name": session_node_name,
            "project_name": args.project_name,
            "saved_project_path": str(saved_path),
            "final_lease_state": final_state,
        }
        phase = "write_evidence"
        _emit(output_path, evidence)
        return 0
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
        fault_error = ""
        if lease is not None:
            try:
                lease.report_fault("script_error", failure_message=error[:2000])
            except Exception as fault_exc:
                fault_error = f"{type(fault_exc).__name__}: {fault_exc}"
            finally:
                try:
                    lease.stop_heartbeat()
                except Exception:
                    pass
        timings["total"] = round(time.monotonic() - total_started, 3)
        evidence = {
            "ok": False,
            "phase": phase,
            "error": error,
            "phase_timings_seconds": timings,
            "lease_id": getattr(lease, "lease_id", None),
            "endpoint": getattr(lease, "endpoint", ""),
            "final_lease_state": getattr(lease, "state", ""),
        }
        if fault_error:
            evidence["fault_report_error"] = fault_error
        try:
            _emit(output_path, evidence)
        except Exception as write_exc:
            print(f"could not write failure evidence: {write_exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
