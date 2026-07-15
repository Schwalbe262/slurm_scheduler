from __future__ import annotations

import errno
import os
import stat
import threading
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any


AUTOMATION_LOCK_FILENAME = "desktop-automation.lock"


# POSIX record locks are process-associated: closing *any* descriptor for the
# inode can release every record lock that process owns on it.  Keep one gate
# per normalized path so a second SessionAutomationLock in the same process
# cannot open/close the marker while the first instance owns its record lock.
_PROCESS_PATH_GATES_GUARD = threading.Lock()
_PROCESS_PATH_GATES: dict[str, threading.Lock] = {}


def _process_path_gate(path: str) -> threading.Lock:
    key = os.path.normcase(os.path.abspath(os.path.normpath(path)))
    with _PROCESS_PATH_GATES_GUARD:
        gate = _PROCESS_PATH_GATES.get(key)
        if gate is None:
            gate = threading.Lock()
            _PROCESS_PATH_GATES[key] = gate
        return gate


def automation_lock_path(artifact_dir: str) -> str:
    """Return the one cross-account lock file owned by a session host."""

    normalized = str(artifact_dir or "").strip()
    if not normalized:
        return ""
    # The control plane runs on Windows while session artifacts live on the
    # Linux GPFS filesystem.  ``Path('/gpfs/...')`` would otherwise serialize
    # the lease contract as ``\\gpfs\\...``, which no Linux client can open.
    if normalized.startswith("/"):
        return str(PurePosixPath(normalized) / AUTOMATION_LOCK_FILENAME)
    return str(Path(normalized) / AUTOMATION_LOCK_FILENAME)


def create_automation_lock_file(path: str) -> str:
    """Create a regular, cross-account writable lock file before admission.

    The session artifact directory is unique to one host generation.  Clients
    are deliberately not allowed to create this file: only the process that
    owns the AEDT Desktop establishes the inode all attached workers lock.
    """

    normalized = str(path or "").strip()
    if not normalized:
        raise ValueError("AEDT automation lock path is required")
    target = Path(normalized)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(str(target), flags, 0o666)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError("AEDT automation lock must be one regular file")
        if info.st_size < 1:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        if os.name != "nt":
            os.fchmod(descriptor, 0o666)
    finally:
        os.close(descriptor)
    return str(target)


class SessionAutomationLock:
    """Re-entrant process lock for Desktop-global AEDT automation calls.

    Linux production takes both a BSD ``flock`` and a POSIX byte-range
    ``lockf``.  ``flock`` excludes distinct open descriptions on one node;
    GPFS propagates the POSIX record lock across compute nodes.  A process-wide
    path gate prevents an unrelated descriptor close from dropping that
    process's record lock.  All callers take these layers in one order and
    release them in reverse order.  The Windows branch retains its native
    one-byte lock for local scheduler tests.
    """

    def __init__(
        self,
        path: str,
        *,
        timeout_seconds: float = 1800.0,
        poll_seconds: float = 0.05,
    ) -> None:
        self.path = str(path or "").strip()
        if not self.path:
            raise ValueError("AEDT automation lock path is required")
        self.timeout_seconds = max(0.0, float(timeout_seconds))
        self.poll_seconds = max(0.01, float(poll_seconds))
        self.last_wait_seconds = 0.0
        self.total_wait_seconds = 0.0
        self.acquire_count = 0
        self._local_lock = threading.RLock()
        self._process_gate = _process_path_gate(self.path)
        self._process_gate_held = False
        self._depth = 0
        self._descriptor: int | None = None
        self._owner_thread_id: int | None = None

    @staticmethod
    def _open_existing(path: str) -> int:
        flags = os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_size < 1
        ):
            os.close(descriptor)
            raise RuntimeError(
                "AEDT automation lock must be one non-empty regular file"
            )
        return descriptor

    @staticmethod
    def _lock_would_block(exc: OSError) -> bool:
        return exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}

    @staticmethod
    def _try_lock(descriptor: int) -> bool:
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            try:
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                return True
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EDEADLK, errno.EAGAIN}:
                    return False
                raise
        import fcntl

        try:
            # GPFS propagates POSIX byte-range locks between compute nodes but
            # does not propagate ``flock`` consistently.  Keep both: flock
            # excludes distinct descriptors in this process/node, while
            # lockf is the cluster-wide exclusion primitive.
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if SessionAutomationLock._lock_would_block(exc):
                return False
            raise
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            fcntl.lockf(
                descriptor,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
                1,
                0,
                os.SEEK_SET,
            )
            return True
        except OSError as exc:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            if SessionAutomationLock._lock_would_block(exc):
                return False
            raise

    @staticmethod
    def _unlock(descriptor: int) -> None:
        if os.name == "nt":
            import msvcrt

            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            return
        import fcntl

        try:
            fcntl.lockf(
                descriptor,
                fcntl.LOCK_UN,
                1,
                0,
                os.SEEK_SET,
            )
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)

    def acquire(self) -> "SessionAutomationLock":
        started = time.monotonic()
        deadline = started + self.timeout_seconds
        if not self._local_lock.acquire(timeout=self.timeout_seconds):
            raise TimeoutError(
                "timed out waiting for AEDT Desktop automation lock: "
                f"{self.path}"
            )
        if self._depth:
            self._depth += 1
            return self

        descriptor: int | None = None
        try:
            remaining = max(0.0, deadline - time.monotonic())
            if not self._process_gate.acquire(timeout=remaining):
                raise TimeoutError(
                    "timed out waiting for AEDT Desktop automation lock: "
                    f"{self.path}"
                )
            self._process_gate_held = True
            descriptor = self._open_existing(self.path)
            while not self._try_lock(descriptor):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "timed out waiting for AEDT Desktop automation lock: "
                        f"{self.path}"
                    )
                time.sleep(min(self.poll_seconds, remaining))
            waited = max(0.0, time.monotonic() - started)
            self.last_wait_seconds = waited
            self.total_wait_seconds += waited
            self.acquire_count += 1
            self._descriptor = descriptor
            self._depth = 1
            self._owner_thread_id = threading.get_ident()
            return self
        except BaseException:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            if self._process_gate_held:
                self._process_gate_held = False
                self._process_gate.release()
            self._local_lock.release()
            raise

    def release(self) -> None:
        if self._depth <= 0:
            raise RuntimeError("AEDT automation lock is not held")
        if self._owner_thread_id != threading.get_ident():
            raise RuntimeError(
                "AEDT automation lock can only be released by its owner"
            )
        self._depth -= 1
        try:
            if self._depth == 0:
                descriptor = self._descriptor
                self._descriptor = None
                self._owner_thread_id = None
                try:
                    if descriptor is None:
                        raise RuntimeError(
                            "AEDT automation lock descriptor is absent"
                        )
                    try:
                        self._unlock(descriptor)
                    finally:
                        os.close(descriptor)
                finally:
                    if self._process_gate_held:
                        self._process_gate_held = False
                        self._process_gate.release()
        finally:
            self._local_lock.release()

    def __enter__(self) -> "SessionAutomationLock":
        return self.acquire()

    def __exit__(self, *_exc: Any) -> None:
        self.release()

    @contextmanager
    def suspended(self):
        """Temporarily release all current-thread nesting for a native solve.

        Callers use this only after capturing an exact project/design handle.
        The blocking ``oDesign.Analyze`` is project scoped, so sibling modeling
        can proceed while this thread waits for the native solver.
        """

        if self._owner_thread_id != threading.get_ident() or self._depth <= 0:
            raise RuntimeError(
                "AEDT automation lock can only be suspended by its owner"
            )
        depth = self._depth
        for _ in range(depth):
            self.release()
        try:
            yield
        finally:
            for _ in range(depth):
                self.acquire()
