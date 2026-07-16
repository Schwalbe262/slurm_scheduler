from __future__ import annotations

import errno
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DEFAULT_TIMEOUT_SECONDS = 15 * 60
_PATH_GATES_GUARD = threading.Lock()
_PATH_GATES: dict[str, threading.RLock] = {}


def default_campaign_mutation_lock_path() -> Path:
    """Return the lock path shared with the MFT feeder and monitoring UI."""

    local_app_data = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if not local_app_data:
        local_app_data = str(Path.home() / "AppData" / "Local")
    return Path(local_app_data) / "MFT_1MW_2026" / "campaign-mutation.lock"


def _path_gate(path: Path) -> threading.RLock:
    key = os.path.normcase(os.path.abspath(os.path.normpath(str(path))))
    with _PATH_GATES_GUARD:
        gate = _PATH_GATES.get(key)
        if gate is None:
            gate = threading.RLock()
            _PATH_GATES[key] = gate
        return gate


def _would_block(exc: OSError) -> bool:
    return exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}


def _try_lock(descriptor: int) -> bool:
    if os.name == "nt":
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            # ``filelock.FileLock`` (used by the MFT feeder) takes the same
            # one-byte Windows record lock on this file.
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            return True
        except OSError as exc:
            if _would_block(exc):
                return False
            raise

    import fcntl

    try:
        # ``filelock.FileLock`` uses flock on POSIX hosts.
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError as exc:
        if _would_block(exc):
            return False
        raise


def _unlock(descriptor: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(descriptor, fcntl.LOCK_UN)


@contextmanager
def campaign_mutation_lock(
    path: str | Path | None = None,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    poll_seconds: float = 0.05,
) -> Iterator[Path]:
    """Serialize scheduler demand writes with every host-local MFT feeder.

    This deliberately implements the same OS lock protocol as ``filelock``
    without adding a scheduler runtime dependency.  A demand decrease can
    therefore commit only before or after a feeder submission cycle, never in
    the middle of one.
    """

    target = Path(path) if path else default_campaign_mutation_lock_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    timeout = max(0.0, float(timeout_seconds))
    deadline = time.monotonic() + timeout
    gate = _path_gate(target)
    if not gate.acquire(timeout=timeout):
        raise TimeoutError(f"timed out waiting for campaign mutation lock: {target}")

    descriptor: int | None = None
    try:
        descriptor = os.open(str(target), os.O_RDWR | os.O_CREAT, 0o666)
        # Keep the marker empty, matching ``filelock.FileLock``.  Windows can
        # lock one byte beyond EOF; attempting to initialize that byte while a
        # feeder owns it would itself raise ``PermissionError`` instead of
        # entering the normal bounded wait loop.
        while not _try_lock(descriptor):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out waiting for campaign mutation lock: {target}"
                )
            time.sleep(max(0.01, min(float(poll_seconds), deadline - time.monotonic())))
        try:
            yield target
        finally:
            _unlock(descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        gate.release()
