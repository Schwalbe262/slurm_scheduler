"""Small, workload-neutral Maxwell 3D A/B pilot for a dedicated AEDT allocation.

Each ``worker`` invocation owns one independent project.  The operator starts N
baseline workers with separate Desktops, then N pooled workers in the same
reserved host session.  ``verify`` consumes the worker and lmstat artifacts.
No production pool settings or Slurm jobs are changed by this script.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import socket
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slurm_scheduler.aedt_attach_client import acquire_project_lease
from slurm_scheduler.aedt_session_host import (
    EXPECTED_AEDT_VERSION,
    EXPECTED_SESSION_PROFILE_JSON,
)

LMSTAT_IN_USE = re.compile(
    r"Users of\s+electronics_desktop\s*:[^\r\n]*Total of\s+(\d+)\s+licenses in use",
    re.IGNORECASE,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _pid(desktop: Any) -> int:
    for attr in ("aedt_process_id", "process_id", "pid"):
        value = getattr(desktop, attr, None)
        if value and int(value) > 0:
            return int(value)
    get_pid = getattr(getattr(desktop, "odesktop", None), "GetProcessID", None)
    if callable(get_pid) and int(get_pid()) > 0:
        return int(get_pid())
    raise RuntimeError("Desktop PID unavailable")


def _project_path(root: Path, mode: str, index: int) -> Path:
    return root / mode / f"project_{index:02d}" / f"pilot_{index:02d}.aedt"


def _field_rows(path: Path) -> list[list[float]]:
    """Read AEDT's ASCII field export; reject empty/nonfinite or header-only files."""
    rows: list[list[float]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        fields = re.split(r"[\s,;]+", line.strip())
        if len(fields) < 4:
            continue
        try:
            values = [float(item) for item in fields]
        except ValueError:
            continue
        if all(math.isfinite(value) for value in values):
            rows.append(values)
    if len(rows) < 3:
        raise RuntimeError(f"field export has fewer than three numeric rows: {path}")
    return rows


def _create_model(desktop: Any, project_path: Path, index: int, lease: Any = None) -> Any:
    from ansys.aedt.core import get_pyaedt_app

    odesktop = desktop.odesktop
    project = odesktop.NewProject()
    name = str(project.GetName() or "")
    if not name:
        raise RuntimeError("AEDT did not create a project")
    if lease is not None:
        lease.bind_project_name(name)
    project.Rename(str(project_path), False)
    name = str(project.GetName() or "")
    if name != project_path.stem:
        raise RuntimeError(f"AEDT project name drift: {name!r}")
    if lease is not None:
        lease.bind_project_name(name)
    design = "ElectrostaticPilot"
    project.InsertDesign("Maxwell 3D", design, "Electrostatic", "")
    app = get_pyaedt_app(name, design, desktop=desktop)
    if app is None:
        raise RuntimeError("PyAEDT could not attach to Maxwell 3D")
    # Identical geometry in both modes.  Each index has a distinct excitation,
    # making a swapped project or copied result detectable in the A/B check.
    app.modeler.model_units = "mm"
    positive = app.modeler.create_box([0, 0, 0], [10, 10, 1], name="Positive", material="copper")
    negative = app.modeler.create_box([0, 0, 6], [10, 10, 1], name="Negative", material="copper")
    region = app.modeler.create_region(pad_value=[100, 100, 100, 100, 100, 100])
    if not positive or not negative or not region:
        raise RuntimeError("AEDT failed to create the electrostatic geometry")
    # Maxwell's voltage API interprets amplitude in mV.
    if not app.assign_voltage(positive.faces, amplitude=1000.0 * index, name="Drive"):
        raise RuntimeError("positive voltage assignment failed")
    if not app.assign_voltage(negative.faces, amplitude=0.0, name="Ground"):
        raise RuntimeError("ground voltage assignment failed")
    setup = app.create_setup(name="PilotSetup")
    if not setup:
        raise RuntimeError("AEDT failed to create the solve setup")
    if app.save_project() is False or not project_path.is_file():
        raise RuntimeError("AEDT did not save the project")
    return app


def _solve_and_export(app: Any, project_path: Path) -> dict[str, Any]:
    if app.analyze_setup("PilotSetup", cores=1) is False:
        raise RuntimeError("AEDT solve returned failure")
    return _export_after_solve(app, project_path)


def worker(args: argparse.Namespace) -> int:
    root = Path(args.artifact_root).expanduser().resolve()
    project_path = _project_path(root, args.mode, args.index)
    result_path = project_path.parent / "result.json"
    if project_path.exists() or result_path.exists():
        raise FileExistsError(f"pilot artifact already exists: {project_path.parent}")
    project_path.parent.mkdir(parents=True, exist_ok=False)
    result: dict[str, Any] = {
        "mode": args.mode,
        "index": args.index,
        "project_name": project_path.stem,
        "project_path": str(project_path),
        "fault": args.fault,
        "node": socket.gethostname(),
        "task_id": args.task_id,
        "started_epoch": time.time(),
    }
    lease = desktop = app = None
    try:
        if args.mode == "pooled":
            if not args.scheduler_url or not args.token_file or args.task_id <= 0 or args.session_id <= 0:
                raise ValueError("pooled worker requires scheduler URL, token file, task ID, and reserved session ID")
            lease = acquire_project_lease(
                args.scheduler_url, project_path.stem,
                bootstrap_token_file=args.token_file,
                task_id=args.task_id,
                requested_session_id=args.session_id,
                workload_family="generic-electrostatic-pilot",
                session_profile=EXPECTED_SESSION_PROFILE_JSON,
                workspace_path=str(project_path.parent),
                project_namespace="generic-electrostatic-pilot",
            )
            lease.wait_until_leased(timeout_seconds=args.lease_timeout)
            desktop = lease.connect_desktop()
            result.update(lease_id=lease.lease_id, session_id=lease.session_id,
                          session_generation=lease.session_generation,
                          desktop_pid=_pid(desktop))
            if lease.session_id != args.session_id:
                raise RuntimeError("lease was placed on a different AEDT session")
            with lease.automation_guard():
                app = _create_model(desktop, project_path, args.index, lease)
            if args.hold_before_solve_seconds:
                time.sleep(args.hold_before_solve_seconds)
            lease.activate(project_path.stem)
            if args.fault == "pre_solve":
                lease.report_fault("script_error", phase="pre_solve", failure_message="intentional pilot abort")
                result["expected_fault_observed"] = True
            else:
                with lease.automation_guard():
                    with lease.native_solve_window():
                        if app.analyze_setup("PilotSetup", cores=1) is False:
                            raise RuntimeError("AEDT solve returned failure")
                lease.wait_for_native_pipeline_barrier()
                with lease.automation_guard():
                    result.update(_export_after_solve(app, project_path))
                if args.fault == "post_solve":
                    lease.report_fault("script_error", phase="post_solve", failure_message="intentional pilot fault")
                    result["expected_fault_observed"] = True
            status = lease.release(wait_seconds=300)
            result["final_lease_state"] = status.get("state")
            if args.fault == "none" and status.get("state") != "released":
                raise RuntimeError(f"project close acknowledgement failed: {status.get('state')}")
        else:
            from ansys.aedt.core import Desktop

            desktop = Desktop(version=EXPECTED_AEDT_VERSION, new_desktop=True,
                              non_graphical=True, close_on_exit=False)
            result["desktop_pid"] = _pid(desktop)
            app = _create_model(desktop, project_path, args.index)
            if args.hold_before_solve_seconds:
                time.sleep(args.hold_before_solve_seconds)
            result.update(_solve_and_export(app, project_path))
        result["ok"] = True
        return 0
    except BaseException as exc:
        result.update(ok=False, error=f"{type(exc).__name__}: {exc}")
        if lease is not None and args.fault == "none":
            try:
                lease.report_fault("script_error", failure_message=result["error"][:2000])
            except Exception as fault_exc:
                result["fault_report_error"] = str(fault_exc)
        if lease is not None:
            try:
                result["final_lease_state"] = lease.release(wait_seconds=300).get("state")
            except Exception as release_exc:
                result["release_error"] = str(release_exc)
        return 1
    finally:
        if lease is not None:
            lease.stop_heartbeat()
        elif desktop is not None:
            try:
                # Standalone worker owns exactly this Desktop.  Never call this on a lease.
                desktop.close_desktop()
            except Exception as release_exc:
                result["desktop_release_error"] = str(release_exc)
                result["ok"] = False
        result["finished_epoch"] = time.time()
        result["elapsed_seconds"] = result["finished_epoch"] - result["started_epoch"]
        _write_json(result_path, result)


def _export_after_solve(app: Any, project_path: Path) -> dict[str, Any]:
    field_path = project_path.with_suffix(".fld")
    points = [[5.0, 5.0, z] for z in (2.0, 3.5, 5.0)]
    if not app.post.export_field_file(
        quantity="Mag_E", solution="PilotSetup : LastAdaptive",
        output_file=str(field_path), assignment="AllObjects",
        sample_points=points, export_with_sample_points=True,
        export_in_si_system=False,
    ) or not field_path.is_file():
        raise RuntimeError("AEDT field solution export failed")
    return {
        "field_path": str(field_path),
        "field_sha256": hashlib.sha256(field_path.read_bytes()).hexdigest(),
        "field_rows": _field_rows(field_path),
        "field_solution_present": True,
    }


def _lmstat_peak(path: Path) -> int:
    raw = path.read_text(encoding="utf-8", errors="replace")
    counts = [int(value) for value in LMSTAT_IN_USE.findall(raw)]
    if not counts:
        raise ValueError(f"no electronics_desktop checkout samples in {path}")
    return max(counts)


def _read_lmstat_peak(path: Path, issues: list[str]) -> int | None:
    try:
        return _lmstat_peak(path)
    except (OSError, ValueError) as exc:
        issues.append(str(exc))
        return None


def verify(args: argparse.Namespace) -> int:
    root = Path(args.artifact_root).expanduser().resolve()
    issues: list[str] = []
    baseline: list[dict[str, Any]] = []
    pooled: list[dict[str, Any]] = []
    for mode, target in (("baseline", baseline), ("pooled", pooled)):
        for index in range(1, args.projects + 1):
            path = _project_path(root, mode, index).parent / "result.json"
            if not path.is_file():
                issues.append(f"missing {path}")
                continue
            item = json.loads(path.read_text(encoding="utf-8"))
            target.append(item)
            expected = _project_path(root, mode, index)
            if item.get("project_path") != str(expected) or item.get("project_name") != expected.stem:
                issues.append(f"{mode}/{index}: project identity mismatch")
            if not item.get("ok") or item.get("fault") != "none":
                issues.append(f"{mode}/{index}: unsuccessful normal run")
            if not item.get("field_solution_present") or len(item.get("field_rows", [])) < 3:
                issues.append(f"{mode}/{index}: missing field data")
            field_path = expected.with_suffix(".fld")
            if (
                not expected.is_file() or expected.stat().st_size == 0
                or item.get("field_path") != str(field_path)
                or not field_path.is_file()
            ):
                issues.append(f"{mode}/{index}: missing project or field file")
            elif hashlib.sha256(field_path.read_bytes()).hexdigest() != item.get("field_sha256"):
                issues.append(f"{mode}/{index}: field file checksum mismatch")
            else:
                try:
                    if _field_rows(field_path) != item.get("field_rows"):
                        issues.append(f"{mode}/{index}: reported field rows differ from file")
                except RuntimeError as exc:
                    issues.append(f"{mode}/{index}: {exc}")
    if len(baseline) == args.projects and len(pooled) == args.projects:
        for mode, group in (("baseline", baseline), ("pooled", pooled)):
            if max(item["started_epoch"] for item in group) >= min(item["finished_epoch"] for item in group):
                issues.append(f"{mode}: worker lifetimes did not overlap")
        baseline_pids = {item.get("desktop_pid") for item in baseline}
        pooled_pids = {item.get("desktop_pid") for item in pooled}
        pooled_sessions = {(item.get("session_id"), item.get("session_generation")) for item in pooled}
        if len(baseline_pids) != args.projects or None in baseline_pids:
            issues.append("baseline did not use N distinct Desktops")
        if len(pooled_pids) != 1 or None in pooled_pids or len(pooled_sessions) != 1:
            issues.append("pooled projects did not share one Desktop and session generation")
        if len({item.get("lease_id") for item in pooled}) != args.projects:
            issues.append("pooled lease IDs are missing or repeated")
        if any(item.get("final_lease_state") != "released" for item in pooled):
            issues.append("one or more pooled project close acknowledgements are missing")
        signatures = []
        for item in baseline:
            signature = tuple(round(abs(value), 9) for row in item.get("field_rows", []) for value in row[3:])
            signatures.append(signature)
        if any(not signature or max(signature) <= 0 for signature in signatures):
            issues.append("one or more baseline field solutions contain no nonzero values")
        if len(set(signatures)) != args.projects:
            issues.append("different excitations produced repeated field signatures")
        for left, right in zip(baseline, pooled):
            a, b = left.get("field_rows", []), right.get("field_rows", [])
            if len(a) != len(b):
                issues.append(f"index {left['index']}: field row count differs")
                continue
            for row_a, row_b in zip(a, b):
                if len(row_a) != len(row_b) or any(
                    not math.isclose(x, y, rel_tol=args.field_rtol, abs_tol=args.field_atol)
                    for x, y in zip(row_a, row_b)
                ):
                    issues.append(f"index {left['index']}: field values differ")
                    break
        baseline_seconds = max(item["finished_epoch"] for item in baseline) - min(item["started_epoch"] for item in baseline)
        pooled_seconds = max(item["finished_epoch"] for item in pooled) - min(item["started_epoch"] for item in pooled)
        if baseline_seconds <= 0 or pooled_seconds / baseline_seconds > args.max_runtime_ratio:
            issues.append("pooled runtime exceeds A/B gate")
    else:
        baseline_seconds = pooled_seconds = None
    baseline_idle = _read_lmstat_peak(Path(args.baseline_idle_lmstat), issues)
    pooled_idle = _read_lmstat_peak(Path(args.pooled_idle_lmstat), issues)
    baseline_peak = _read_lmstat_peak(Path(args.baseline_lmstat), issues)
    pooled_peak = _read_lmstat_peak(Path(args.pooled_lmstat), issues)
    if baseline_peak is not None and baseline_idle is not None and baseline_peak - baseline_idle != args.projects:
        issues.append("lmstat does not show N additional baseline Desktop checkouts")
    if pooled_peak is not None and pooled_idle is not None and pooled_peak - pooled_idle != 1:
        issues.append("lmstat does not show one additional pooled Desktop checkout")
    summary = {
        "passed": not issues, "issues": issues, "projects": args.projects,
        "baseline_seconds": baseline_seconds, "pooled_seconds": pooled_seconds,
        "baseline_desktop_checkout_peak": baseline_peak,
        "pooled_desktop_checkout_peak": pooled_peak,
        "baseline_idle_checkout": baseline_idle,
        "pooled_idle_checkout": pooled_idle,
        "field_rtol": args.field_rtol, "field_atol": args.field_atol,
    }
    _write_json(root / "verification.json", summary)
    print(json.dumps(summary, indent=2))
    return 0 if not issues else 1


def verify_fault(args: argparse.Namespace) -> int:
    root = Path(args.artifact_root).expanduser().resolve()
    issues: list[str] = []
    items: list[dict[str, Any]] = []
    for index in range(1, args.projects + 1):
        project_path = _project_path(root, "pooled", index)
        path = project_path.parent / "result.json"
        if not path.is_file():
            issues.append(f"missing {path}")
            continue
        item = json.loads(path.read_text(encoding="utf-8"))
        items.append(item)
        if item.get("index") != index or item.get("project_path") != str(project_path):
            issues.append(f"index {index}: project identity mismatch")
        if index == args.fault_index:
            if item.get("fault") != args.fault_kind or not item.get("expected_fault_observed"):
                issues.append(f"index {index}: intentional fault was not reported")
            continue
        field_path = project_path.with_suffix(".fld")
        if not item.get("ok") or item.get("final_lease_state") != "released":
            issues.append(f"index {index}: sibling did not finish and release")
        if not project_path.is_file() or project_path.stat().st_size == 0 or not field_path.is_file():
            issues.append(f"index {index}: sibling project or field export missing")
        elif hashlib.sha256(field_path.read_bytes()).hexdigest() != item.get("field_sha256"):
            issues.append(f"index {index}: sibling field checksum mismatch")
        else:
            try:
                if _field_rows(field_path) != item.get("field_rows"):
                    issues.append(f"index {index}: sibling field data mismatch")
            except RuntimeError as exc:
                issues.append(f"index {index}: {exc}")
    if len(items) == args.projects:
        if len({item.get("desktop_pid") for item in items}) != 1:
            issues.append("fault run did not use one Desktop")
        if len({(item.get("session_id"), item.get("session_generation")) for item in items}) != 1:
            issues.append("fault run did not use one session generation")
        if len({item.get("lease_id") for item in items}) != args.projects:
            issues.append("fault run lease IDs are missing or repeated")
    summary = {
        "passed": not issues, "issues": issues, "projects": args.projects,
        "fault_index": args.fault_index, "fault_kind": args.fault_kind,
        "scope": "fault report and surviving sibling outputs; host quarantine and solver checkout require separate live evidence",
    }
    _write_json(root / "fault_verification.json", summary)
    print(json.dumps(summary, indent=2))
    return 0 if not issues else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("worker")
    run.add_argument("--mode", choices=("baseline", "pooled"), required=True)
    run.add_argument("--index", type=int, required=True)
    run.add_argument("--artifact-root", required=True)
    run.add_argument("--scheduler-url", default="")
    run.add_argument("--token-file", default="")
    run.add_argument("--task-id", type=int, default=0)
    run.add_argument("--session-id", type=int, default=0)
    run.add_argument("--lease-timeout", type=int, default=1800)
    run.add_argument("--hold-before-solve-seconds", type=int, default=0)
    run.add_argument("--fault", choices=("none", "pre_solve", "post_solve"), default="none")
    check = sub.add_parser("verify")
    check.add_argument("--projects", type=int, required=True)
    check.add_argument("--artifact-root", required=True)
    check.add_argument("--baseline-lmstat", required=True)
    check.add_argument("--pooled-lmstat", required=True)
    check.add_argument("--baseline-idle-lmstat", required=True)
    check.add_argument("--pooled-idle-lmstat", required=True)
    check.add_argument("--max-runtime-ratio", type=float, default=1.2)
    check.add_argument("--field-rtol", type=float, default=0.05)
    check.add_argument("--field-atol", type=float, default=1e-8)
    fault = sub.add_parser("verify-fault")
    fault.add_argument("--projects", type=int, required=True)
    fault.add_argument("--artifact-root", required=True)
    fault.add_argument("--fault-index", type=int, required=True)
    fault.add_argument("--fault-kind", choices=("pre_solve", "post_solve"), required=True)
    args = parser.parse_args(argv)
    if args.command == "worker":
        if args.index <= 0:
            parser.error("--index must be positive")
        if not 0 <= args.hold_before_solve_seconds <= 300:
            parser.error("--hold-before-solve-seconds must be between 0 and 300")
        return worker(args)
    if args.projects < 2:
        parser.error("--projects must be at least 2")
    if args.command == "verify-fault":
        if not 1 <= args.fault_index <= args.projects:
            parser.error("--fault-index must be within --projects")
        return verify_fault(args)
    return verify(args)


if __name__ == "__main__":
    raise SystemExit(main())
