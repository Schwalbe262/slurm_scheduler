from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import secrets
import socket
import stat
import struct
import threading
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any


AUTOMATION_LOCK_FILENAME = "desktop-automation.lock"
AUTOMATION_LOCK_MARKER_VERSION = 1
_LOCK_BYTE_OFFSET = 0
_ACTIVE_SLOT_OFFSET = 1
_OWNER_SLOT_SIZE = 4096
_OWNER_SLOT_OFFSETS = (2, 2 + _OWNER_SLOT_SIZE)
_OWNER_MARKER_SIZE = 2 + 2 * _OWNER_SLOT_SIZE
_OWNER_FRAME_MAGIC = b"AEDTOWN1"
_OWNER_FRAME_HEADER = struct.Struct(">8sQI32s")
_OWNER_KINDS = frozenset({"client", "session_host", "process"})


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


def _read_at(descriptor: int, size: int, offset: int) -> bytes:
    pread = getattr(os, "pread", None)
    if callable(pread):
        return pread(descriptor, size, offset)
    current = os.lseek(descriptor, 0, os.SEEK_CUR)
    try:
        os.lseek(descriptor, offset, os.SEEK_SET)
        return os.read(descriptor, size)
    finally:
        os.lseek(descriptor, current, os.SEEK_SET)


def _write_at(descriptor: int, payload: bytes, offset: int) -> None:
    position = 0
    pwrite = getattr(os, "pwrite", None)
    if callable(pwrite):
        while position < len(payload):
            written = pwrite(descriptor, payload[position:], offset + position)
            if written <= 0:
                raise OSError("short write to AEDT automation owner marker")
            position += written
        return
    current = os.lseek(descriptor, 0, os.SEEK_CUR)
    try:
        os.lseek(descriptor, offset, os.SEEK_SET)
        while position < len(payload):
            written = os.write(descriptor, payload[position:])
            if written <= 0:
                raise OSError("short write to AEDT automation owner marker")
            position += written
    finally:
        os.lseek(descriptor, current, os.SEEK_SET)


def _owner_frame(record: dict[str, Any], sequence: int) -> bytes:
    payload = json.dumps(
        {**record, "sequence": int(sequence)},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    capacity = _OWNER_SLOT_SIZE - _OWNER_FRAME_HEADER.size
    if len(payload) > capacity:
        raise ValueError("AEDT automation owner marker payload is too large")
    header = _OWNER_FRAME_HEADER.pack(
        _OWNER_FRAME_MAGIC,
        int(sequence),
        len(payload),
        hashlib.sha256(payload).digest(),
    )
    return header + payload + b"\0" * (capacity - len(payload))


def _decode_owner_frame(raw: bytes) -> tuple[int, dict[str, Any]] | None:
    if len(raw) != _OWNER_SLOT_SIZE:
        return None
    try:
        magic, sequence, payload_size, checksum = _OWNER_FRAME_HEADER.unpack(
            raw[: _OWNER_FRAME_HEADER.size]
        )
    except struct.error:
        return None
    capacity = _OWNER_SLOT_SIZE - _OWNER_FRAME_HEADER.size
    if magic != _OWNER_FRAME_MAGIC or not 0 < payload_size <= capacity:
        return None
    payload = raw[
        _OWNER_FRAME_HEADER.size : _OWNER_FRAME_HEADER.size + payload_size
    ]
    if not secrets.compare_digest(hashlib.sha256(payload).digest(), checksum):
        return None
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(record, dict):
        return None
    try:
        record_sequence = int(record.get("sequence", -1))
    except (TypeError, ValueError, OverflowError):
        return None
    if record_sequence != sequence:
        return None
    if int(record.get("version") or 0) != AUTOMATION_LOCK_MARKER_VERSION:
        return None
    if record.get("state") not in {"free", "held"}:
        return None
    return int(sequence), record


def _read_owner_record(descriptor: int) -> tuple[int, int, dict[str, Any]] | None:
    if os.fstat(descriptor).st_size < _OWNER_MARKER_SIZE:
        return None
    for _attempt in range(3):
        before = _read_at(descriptor, 1, _ACTIVE_SLOT_OFFSET)
        if before not in {b"0", b"1"}:
            return None
        slot = int(before)
        decoded = _decode_owner_frame(
            _read_at(descriptor, _OWNER_SLOT_SIZE, _OWNER_SLOT_OFFSETS[slot])
        )
        after = _read_at(descriptor, 1, _ACTIVE_SLOT_OFFSET)
        if before == after:
            if decoded is None:
                return None
            sequence, record = decoded
            return slot, sequence, record
    return None


def _initialize_owner_marker(descriptor: int) -> None:
    if _read_owner_record(descriptor) is not None:
        return
    if os.fstat(descriptor).st_size < 1:
        _write_at(descriptor, b"\0", _LOCK_BYTE_OFFSET)
    os.ftruncate(descriptor, _OWNER_MARKER_SIZE)
    free = {
        "version": AUTOMATION_LOCK_MARKER_VERSION,
        "state": "free",
        "cleared_at": time.time(),
    }
    _write_at(descriptor, _owner_frame(free, 0), _OWNER_SLOT_OFFSETS[0])
    _write_at(descriptor, b"\0" * _OWNER_SLOT_SIZE, _OWNER_SLOT_OFFSETS[1])
    os.fsync(descriptor)
    _write_at(descriptor, b"0", _ACTIVE_SLOT_OFFSET)
    os.fsync(descriptor)


def _commit_owner_record(
    descriptor: int, record: dict[str, Any]
) -> dict[str, Any]:
    _initialize_owner_marker(descriptor)
    current = _read_owner_record(descriptor)
    if current is None:
        raise RuntimeError("AEDT automation owner marker is unreadable")
    active_slot, sequence, _current_record = current
    next_slot = 1 - active_slot
    next_sequence = sequence + 1
    committed = {**record, "version": AUTOMATION_LOCK_MARKER_VERSION}
    _write_at(
        descriptor,
        _owner_frame(committed, next_sequence),
        _OWNER_SLOT_OFFSETS[next_slot],
    )
    os.fsync(descriptor)
    _write_at(descriptor, str(next_slot).encode("ascii"), _ACTIVE_SLOT_OFFSET)
    os.fsync(descriptor)
    return {**committed, "sequence": next_sequence}


def _validated_owner(record: dict[str, Any]) -> dict[str, Any] | None:
    if record.get("state") != "held":
        return None
    owner = record.get("owner")
    if not isinstance(owner, dict):
        return None
    kind = str(owner.get("kind") or "")
    host = str(owner.get("host") or "").strip()
    nonce = str(owner.get("nonce") or "")
    try:
        task_id = int(owner.get("task_id") or 0)
        pid = int(owner.get("pid") or 0)
        acquired_at = float(owner.get("acquired_at") or 0.0)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        kind not in _OWNER_KINDS
        or not host
        or len(host) > 255
        or task_id < 0
        or pid <= 0
        or not math.isfinite(acquired_at)
        or acquired_at <= 0
        or not secrets.compare_digest(nonce, nonce.lower())
        or len(nonce) != 32
        or any(character not in "0123456789abcdef" for character in nonce)
    ):
        return None
    return {
        "kind": kind,
        "task_id": task_id,
        "host": host,
        "pid": pid,
        "acquired_at": acquired_at,
        "nonce": nonce,
        "sequence": int(record.get("sequence") or 0),
    }


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
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
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
        _initialize_owner_marker(descriptor)
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
        owner_kind: str = "process",
        owner_task_id: int = 0,
        owner_host: str = "",
        owner_pid: int = 0,
    ) -> None:
        self.path = str(path or "").strip()
        if not self.path:
            raise ValueError("AEDT automation lock path is required")
        self.timeout_seconds = max(0.0, float(timeout_seconds))
        self.poll_seconds = max(0.01, float(poll_seconds))
        normalized_owner_kind = str(owner_kind or "").strip().lower()
        if normalized_owner_kind not in _OWNER_KINDS:
            raise ValueError("invalid AEDT automation lock owner kind")
        self.owner_kind = normalized_owner_kind
        self.owner_task_id = max(0, int(owner_task_id or 0))
        self.owner_host = str(owner_host or socket.gethostname()).strip()
        self.owner_pid = int(owner_pid or os.getpid())
        if not self.owner_host or self.owner_pid <= 0:
            raise ValueError("AEDT automation lock owner identity is incomplete")
        self.last_wait_seconds = 0.0
        self.total_wait_seconds = 0.0
        self.acquire_count = 0
        self._local_lock = threading.RLock()
        self._process_gate = _process_path_gate(self.path)
        self._process_gate_held = False
        self._depth = 0
        self._descriptor: int | None = None
        self._owner_thread_id: int | None = None
        self._owner_record: dict[str, Any] | None = None

    @staticmethod
    def _open_existing(path: str) -> int:
        flags = os.O_RDWR | getattr(os, "O_BINARY", 0)
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
            owner = {
                "kind": self.owner_kind,
                "task_id": self.owner_task_id,
                "host": self.owner_host,
                "pid": self.owner_pid,
                "acquired_at": time.time(),
                "nonce": secrets.token_hex(16),
            }
            committed = _commit_owner_record(
                descriptor,
                {"state": "held", "owner": owner},
            )
            validated_owner = _validated_owner(committed)
            if validated_owner is None:
                raise RuntimeError("AEDT automation owner marker commit failed")
            waited = max(0.0, time.monotonic() - started)
            self.last_wait_seconds = waited
            self.total_wait_seconds += waited
            self.acquire_count += 1
            self._descriptor = descriptor
            self._depth = 1
            self._owner_thread_id = threading.get_ident()
            self._owner_record = validated_owner
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
        release_error: BaseException | None = None
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
                        current = _read_owner_record(descriptor)
                        current_owner = (
                            _validated_owner(current[2]) if current else None
                        )
                        expected_nonce = str(
                            (self._owner_record or {}).get("nonce") or ""
                        )
                        if (
                            current_owner is None
                            or not expected_nonce
                            or not secrets.compare_digest(
                                str(current_owner.get("nonce") or ""),
                                expected_nonce,
                            )
                        ):
                            raise RuntimeError(
                                "AEDT automation owner marker changed while held"
                            )
                        _commit_owner_record(
                            descriptor,
                            {
                                "state": "free",
                                "cleared_at": time.time(),
                                "previous_nonce": expected_nonce,
                            },
                        )
                    except BaseException as exc:
                        release_error = exc
                    try:
                        self._unlock(descriptor)
                    except BaseException as exc:
                        if release_error is None:
                            release_error = exc
                    finally:
                        os.close(descriptor)
                finally:
                    self._owner_record = None
                    if self._process_gate_held:
                        self._process_gate_held = False
                        self._process_gate.release()
        finally:
            self._local_lock.release()
        if release_error is not None:
            raise release_error

    @property
    def owner_record(self) -> dict[str, Any] | None:
        """Return this instance's committed owner identity while held."""

        return dict(self._owner_record) if self._owner_record is not None else None

    @classmethod
    def inspect(cls, path: str) -> dict[str, Any]:
        """Read-only proof of actual lock state plus committed owner identity.

        The process-path gate is checked before opening the inode.  This is
        essential for POSIX record locks: closing an unrelated descriptor in
        the host process could otherwise release a lock held by another host
        thread.
        """

        normalized = str(path or "").strip()
        if not normalized:
            raise ValueError("AEDT automation lock path is required")
        gate = _process_path_gate(normalized)
        if not gate.acquire(blocking=False):
            return {
                "busy": True,
                "local_process": True,
                "marker_valid": False,
                "owner": None,
            }
        descriptor: int | None = None
        try:
            descriptor = cls._open_existing(normalized)
            if cls._try_lock(descriptor):
                try:
                    return {
                        "busy": False,
                        "local_process": False,
                        "marker_valid": _read_owner_record(descriptor)
                        is not None,
                        "owner": None,
                    }
                finally:
                    cls._unlock(descriptor)
            current = _read_owner_record(descriptor)
            owner = _validated_owner(current[2]) if current else None
            return {
                "busy": True,
                "local_process": False,
                "marker_valid": current is not None,
                "owner": owner,
            }
        finally:
            if descriptor is not None:
                os.close(descriptor)
            gate.release()

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
