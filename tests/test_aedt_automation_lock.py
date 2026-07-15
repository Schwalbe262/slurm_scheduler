from __future__ import annotations

import errno
import os
import subprocess
import sys
import threading
import types
from pathlib import Path

import pytest

import slurm_scheduler.aedt_automation_lock as lock_module
from slurm_scheduler.aedt_automation_lock import (
    SessionAutomationLock,
    automation_lock_path,
    create_automation_lock_file,
)


@pytest.mark.parametrize("blank", ["", " ", "\t", None])
def test_host_lock_creation_rejects_blank_path(blank) -> None:
    with pytest.raises(ValueError, match="automation lock path is required"):
        create_automation_lock_file(blank)


def test_remote_posix_lock_path_survives_windows_control_plane() -> None:
    assert automation_lock_path(
        "/gpfs/tmp_cpu2/mft_pool/aedt_session_logs/session-505"
    ) == (
        "/gpfs/tmp_cpu2/mft_pool/aedt_session_logs/session-505/"
        "desktop-automation.lock"
    )


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


def test_process_gate_blocks_before_second_instance_opens_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    owner = SessionAutomationLock(path, timeout_seconds=1)
    contender = SessionAutomationLock(path, timeout_seconds=0)

    def unexpected_open(_path: str) -> int:
        raise AssertionError("contender opened marker while process gate was held")

    monkeypatch.setattr(contender, "_open_existing", unexpected_open)
    with owner:
        with pytest.raises(TimeoutError, match="automation lock"):
            contender.acquire()


def test_non_owner_release_preserves_lock_state(tmp_path: Path) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    owner = SessionAutomationLock(path, timeout_seconds=1)
    contender = SessionAutomationLock(path, timeout_seconds=0)
    errors: list[BaseException] = []

    owner.acquire()
    thread = threading.Thread(
        target=lambda: _record_release_error(owner, errors), daemon=True
    )
    thread.start()
    thread.join(timeout=1)

    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "released by its owner" in str(errors[0])
    assert owner._depth == 1
    assert owner._descriptor is not None
    assert owner._process_gate_held is True
    with pytest.raises(TimeoutError, match="automation lock"):
        contender.acquire()
    owner.release()


def _record_release_error(
    lock: SessionAutomationLock, errors: list[BaseException]
) -> None:
    try:
        lock.release()
    except BaseException as exc:
        errors.append(exc)


def _record_lock_attempt(lock: SessionAutomationLock, outcome: list[str]) -> None:
    try:
        with lock:
            outcome.append("acquired")
    except TimeoutError:
        outcome.append("timeout")


def test_owner_marker_records_and_clears_exact_client_identity(
    tmp_path: Path,
) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    lock = SessionAutomationLock(
        path,
        timeout_seconds=1,
        owner_kind="client",
        owner_task_id=31337,
        owner_host="worker-a",
        owner_pid=9001,
    )

    with lock:
        owner = lock.owner_record
        assert owner is not None
        assert owner["kind"] == "client"
        assert owner["task_id"] == 31337
        assert owner["host"] == "worker-a"
        assert owner["pid"] == 9001
        assert len(owner["nonce"]) == 32
        with lock:
            assert lock.owner_record["nonce"] == owner["nonce"]

    inspection = SessionAutomationLock.inspect(path)
    assert inspection == {
        "busy": False,
        "local_process": False,
        "marker_valid": True,
        "owner": None,
    }


def test_inspection_requires_actual_external_lock_and_ignores_crash_stale_owner(
    tmp_path: Path,
) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    child_code = "\n".join(
        (
            "import os, sys",
            "from slurm_scheduler.aedt_automation_lock import SessionAutomationLock",
            "lock = SessionAutomationLock(sys.argv[1], timeout_seconds=5, "
            "owner_kind='client', owner_task_id=4242, "
            "owner_host='remote-client-test', owner_pid=os.getpid())",
            "lock.acquire()",
            "print('READY', flush=True)",
            "sys.stdin.buffer.read(1)",
        )
    )
    process = subprocess.Popen(
        [getattr(sys, "_base_executable", sys.executable), "-c", child_code, path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == b"READY"
        inspection = SessionAutomationLock.inspect(path)
        assert inspection["busy"] is True
        assert inspection["local_process"] is False
        assert inspection["marker_valid"] is True
        assert inspection["owner"]["task_id"] == 4242
        assert inspection["owner"]["host"] == "remote-client-test"
        assert inspection["owner"]["pid"] > 0
        assert inspection["owner"]["pid"] != os.getpid()

        # Simulate a client process dying after its held frame reached GPFS.
        # The OS releases the byte lock while the durable frame remains stale.
        process.terminate()
        process.wait(timeout=10)
        stale = SessionAutomationLock.inspect(path)
        assert stale["busy"] is False
        assert stale["marker_valid"] is True
        assert stale["owner"] is None
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=10)


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


def test_client_rejects_empty_host_lock_marker(tmp_path: Path) -> None:
    path = tmp_path / "empty.lock"
    path.touch()
    lock = SessionAutomationLock(str(path), timeout_seconds=0.1)
    with pytest.raises(RuntimeError, match="non-empty regular file"):
        lock.acquire()


def test_process_path_gate_prevents_second_descriptor_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    first = SessionAutomationLock(path, timeout_seconds=1)
    second = SessionAutomationLock(path, timeout_seconds=0.1, poll_seconds=0.01)
    opened: list[str] = []
    original_open = SessionAutomationLock._open_existing

    with first:
        def tracking_open(candidate: str) -> int:
            opened.append(candidate)
            return original_open(candidate)

        monkeypatch.setattr(
            SessionAutomationLock,
            "_open_existing",
            staticmethod(tracking_open),
        )
        outcome: list[str] = []
        thread = threading.Thread(
            target=lambda: _record_lock_attempt(second, outcome), daemon=True
        )
        thread.start()
        thread.join(timeout=1)
        assert outcome == ["timeout"]
        # POSIX closes release all record locks for this process/inode.  The
        # process gate must therefore stop the losing instance before os.open.
        assert opened == []

    with second:
        pass
    assert opened == [path]


def test_wrong_thread_cannot_corrupt_owned_lock(tmp_path: Path) -> None:
    path = create_automation_lock_file(automation_lock_path(str(tmp_path)))
    lock = SessionAutomationLock(path, timeout_seconds=1)
    failures: list[str] = []
    lock.acquire()

    def wrong_thread_release() -> None:
        try:
            lock.release()
        except RuntimeError as exc:
            failures.append(str(exc))

    thread = threading.Thread(target=wrong_thread_release, daemon=True)
    thread.start()
    thread.join(timeout=1)
    assert failures == [
        "AEDT automation lock can only be released by its owner"
    ]
    assert lock._depth == 1
    assert lock._descriptor is not None
    assert lock._process_gate_held is True
    lock.release()
    assert lock._depth == 0


def _fake_posix_lock_modules(
    monkeypatch: pytest.MonkeyPatch, *, busy_layer: str = "",
) -> tuple[types.ModuleType, list[tuple]]:
    calls: list[tuple] = []
    fake_fcntl = types.ModuleType("fcntl")
    fake_fcntl.LOCK_EX = 1
    fake_fcntl.LOCK_NB = 2
    fake_fcntl.LOCK_UN = 8

    def flock(descriptor: int, operation: int) -> None:
        calls.append(("flock", descriptor, operation))
        if busy_layer == "flock" and operation != fake_fcntl.LOCK_UN:
            raise OSError(errno.EACCES, "busy")

    def lockf(
        descriptor: int,
        operation: int,
        length: int,
        start: int,
        whence: int,
    ) -> None:
        calls.append(
            ("lockf", descriptor, operation, length, start, whence)
        )
        if busy_layer == "lockf" and operation != fake_fcntl.LOCK_UN:
            raise OSError(errno.EAGAIN, "busy")

    fake_fcntl.flock = flock
    fake_fcntl.lockf = lockf
    fake_os = types.SimpleNamespace(
        name="posix",
        lseek=os.lseek,
        SEEK_SET=os.SEEK_SET,
    )
    monkeypatch.setitem(sys.modules, "fcntl", fake_fcntl)
    monkeypatch.setattr(lock_module, "os", fake_os)
    return fake_fcntl, calls


def test_posix_dual_lock_and_reverse_unlock_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_fcntl, calls = _fake_posix_lock_modules(monkeypatch)
    path = tmp_path / "marker"
    path.write_bytes(b"\0")
    descriptor = os.open(path, os.O_RDWR)
    try:
        assert SessionAutomationLock._try_lock(descriptor) is True
        SessionAutomationLock._unlock(descriptor)
    finally:
        os.close(descriptor)

    acquire_operation = fake_fcntl.LOCK_EX | fake_fcntl.LOCK_NB
    assert calls == [
        ("flock", descriptor, acquire_operation),
        ("lockf", descriptor, acquire_operation, 1, 0, os.SEEK_SET),
        ("lockf", descriptor, fake_fcntl.LOCK_UN, 1, 0, os.SEEK_SET),
        ("flock", descriptor, fake_fcntl.LOCK_UN),
    ]


@pytest.mark.parametrize("busy_layer", ["flock", "lockf"])
def test_posix_busy_lock_rolls_back_local_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    busy_layer: str,
) -> None:
    fake_fcntl, calls = _fake_posix_lock_modules(
        monkeypatch, busy_layer=busy_layer
    )
    path = tmp_path / "marker"
    path.write_bytes(b"\0")
    descriptor = os.open(path, os.O_RDWR)
    try:
        assert SessionAutomationLock._try_lock(descriptor) is False
    finally:
        os.close(descriptor)

    acquire_operation = fake_fcntl.LOCK_EX | fake_fcntl.LOCK_NB
    if busy_layer == "flock":
        assert calls == [("flock", descriptor, acquire_operation)]
    else:
        assert calls == [
            ("flock", descriptor, acquire_operation),
            ("lockf", descriptor, acquire_operation, 1, 0, os.SEEK_SET),
            ("flock", descriptor, fake_fcntl.LOCK_UN),
        ]
