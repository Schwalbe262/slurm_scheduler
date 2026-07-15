from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest

from slurm_scheduler.aedt_automation_lock import (
    SessionAutomationLock,
    automation_lock_path,
    create_automation_lock_file,
)


@pytest.mark.parametrize("blank", ["", " ", "\t", None])
def test_host_lock_creation_rejects_blank_path(blank) -> None:
    with pytest.raises(ValueError, match="automation lock path is required"):
        create_automation_lock_file(blank)


def test_host_creates_one_regular_cross_account_lock(tmp_path: Path) -> None:
    path = automation_lock_path(str(tmp_path))
    assert path == str(tmp_path / "desktop-automation.lock")
    assert create_automation_lock_file(path) == path
    assert Path(path).is_file()
    assert Path(path).stat().st_size >= 1
    if os.name != "nt":
        assert Path(path).stat().st_mode & 0o777 == 0o666


def test_distinct_clients_are_mutually_exclusive(tmp_path: Path) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    first = SessionAutomationLock(path, timeout_seconds=1)
    second = SessionAutomationLock(path, timeout_seconds=0.1, poll_seconds=0.01)
    outcome: list[str] = []

    with first:
        thread = threading.Thread(
            target=lambda: _record_lock_attempt(second, outcome), daemon=True
        )
        thread.start()
        thread.join(timeout=1)
        assert outcome == ["timeout"]

    with second:
        outcome.append("acquired")
    assert outcome == ["timeout", "acquired"]


def _record_lock_attempt(lock: SessionAutomationLock, outcome: list[str]) -> None:
    try:
        with lock:
            outcome.append("acquired")
    except TimeoutError:
        outcome.append("timeout")


def test_native_solve_window_yields_and_restores_all_nesting(
    tmp_path: Path,
) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    owner = SessionAutomationLock(path, timeout_seconds=1)
    sibling = SessionAutomationLock(path, timeout_seconds=1)

    with owner:
        with owner:
            with owner.suspended():
                with sibling:
                    assert sibling.acquire_count == 1
            assert owner.acquire_count == 2
            assert owner._depth == 2
        assert owner._depth == 1
    assert owner._depth == 0


def test_client_cannot_create_missing_host_lock(tmp_path: Path) -> None:
    lock = SessionAutomationLock(
        str(tmp_path / "missing.lock"), timeout_seconds=0.1
    )
    with pytest.raises(FileNotFoundError):
        lock.acquire()
