from __future__ import annotations

import json
from pathlib import Path

from scripts import aedt_pool_live_pilot as pilot


def _lmstat(path: Path, *counts: int) -> None:
    path.write_text(
        "\n".join(
            f"Users of electronics_desktop: (Total of 100 licenses issued; Total of {n} licenses in use)"
            for n in counts
        ),
        encoding="utf-8",
    )


def _result(root: Path, mode: str, index: int, *, fault: str = "none") -> dict:
    project = pilot._project_path(root, mode, index)
    project.parent.mkdir(parents=True)
    project.write_text("independent AEDT project placeholder", encoding="utf-8")
    field = project.with_suffix(".fld")
    field.write_text(
        "\n".join(f"5 5 {z} {float(index) * z}" for z in (2, 3.5, 5)) + "\n",
        encoding="utf-8",
    )
    item = {
        "mode": mode,
        "index": index,
        "project_name": project.stem,
        "project_path": str(project),
        "field_path": str(field),
        "field_sha256": __import__("hashlib").sha256(field.read_bytes()).hexdigest(),
        "field_rows": pilot._field_rows(field),
        "field_solution_present": True,
        "desktop_pid": 100 + index if mode == "baseline" else 200,
        "session_id": 12 if mode == "pooled" else None,
        "session_generation": 3 if mode == "pooled" else None,
        "lease_id": 1000 + index if mode == "pooled" else None,
        "final_lease_state": "released" if mode == "pooled" else None,
        "fault": fault,
        "ok": True,
        "started_epoch": 1000,
        "finished_epoch": 1010 if mode == "baseline" else 1011,
    }
    (project.parent / "result.json").write_text(json.dumps(item), encoding="utf-8")
    return item


def _run_verify(root: Path) -> int:
    for name, counts in (
        ("baseline_idle", (4,)),
        ("baseline", (5, 6)),
        ("pooled_idle", (4,)),
        ("pooled", (5,)),
    ):
        _lmstat(root / f"{name}.txt", *counts)
    return pilot.main(
        ["verify", "--projects", "2", "--artifact-root", str(root),
         "--baseline-idle-lmstat", str(root / "baseline_idle.txt"),
         "--baseline-lmstat", str(root / "baseline.txt"),
         "--pooled-idle-lmstat", str(root / "pooled_idle.txt"),
         "--pooled-lmstat", str(root / "pooled.txt")]
    )


def test_ab_verifier_accepts_distinct_projects_one_pooled_desktop(tmp_path):
    for mode in ("baseline", "pooled"):
        for index in (1, 2):
            _result(tmp_path, mode, index)
    assert _run_verify(tmp_path) == 0
    assert json.loads((tmp_path / "verification.json").read_text())["passed"]


def test_ab_verifier_rejects_swapped_field_and_extra_license(tmp_path):
    for mode in ("baseline", "pooled"):
        for index in (1, 2):
            _result(tmp_path, mode, index)
    swapped = pilot._project_path(tmp_path, "pooled", 1).with_suffix(".fld")
    swapped.write_bytes(pilot._project_path(tmp_path, "pooled", 2).with_suffix(".fld").read_bytes())
    assert _run_verify(tmp_path) == 1
    issues = json.loads((tmp_path / "verification.json").read_text())["issues"]
    assert any("checksum mismatch" in issue for issue in issues)
    _lmstat(tmp_path / "pooled.txt", 6)
    assert pilot.main(
        ["verify", "--projects", "2", "--artifact-root", str(tmp_path),
         "--baseline-idle-lmstat", str(tmp_path / "baseline_idle.txt"),
         "--baseline-lmstat", str(tmp_path / "baseline.txt"),
         "--pooled-idle-lmstat", str(tmp_path / "pooled_idle.txt"),
         "--pooled-lmstat", str(tmp_path / "pooled.txt")]
    ) == 1
    issues = json.loads((tmp_path / "verification.json").read_text())["issues"]
    assert any("one additional pooled" in issue for issue in issues)


def test_fault_verifier_checks_surviving_sibling(tmp_path):
    faulted = _result(tmp_path, "pooled", 1, fault="pre_solve")
    faulted["expected_fault_observed"] = True
    (pilot._project_path(tmp_path, "pooled", 1).parent / "result.json").write_text(json.dumps(faulted))
    _result(tmp_path, "pooled", 2)
    args = ["verify-fault", "--projects", "2", "--artifact-root", str(tmp_path),
            "--fault-index", "1", "--fault-kind", "pre_solve"]
    assert pilot.main(args) == 0
    pilot._project_path(tmp_path, "pooled", 2).with_suffix(".fld").unlink()
    assert pilot.main(args) == 1
    assert not json.loads((tmp_path / "fault_verification.json").read_text())["passed"]
