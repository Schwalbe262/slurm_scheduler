from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Callable


LOGGER = logging.getLogger(__name__)


def probe_listener(host: str, port: int, timeout_seconds: float = 2.0) -> bool:
    """Probe the local TCP accept path, independent of scheduler tick health."""
    connect_host = host.strip()
    if connect_host in {"", "0.0.0.0", "::", "[::]"}:
        connect_host = "127.0.0.1"
    try:
        with socket.create_connection(
            (connect_host, int(port)), timeout=max(0.1, float(timeout_seconds))
        ):
            return True
    except OSError:
        return False


@dataclass
class ListenerFailureGate:
    threshold: int = 3
    consecutive_failures: int = 0
    was_healthy: bool = False

    def observe(self, healthy: bool) -> bool:
        """Return true only after a previously live listener repeatedly vanished."""
        if healthy:
            self.was_healthy = True
            self.consecutive_failures = 0
            return False
        self.consecutive_failures += 1
        return self.was_healthy and self.consecutive_failures >= max(1, int(self.threshold))


class WebWorkerSupervisor:
    """Keep a replaceable Uvicorn worker behind a stable parent process.

    A worker restart does not cancel Slurm allocations or remote task steps;
    those are durable in SQLite/Slurm and are reconciled by the replacement
    worker.  This watchdog exists for failures where the Python PID survives
    but the Windows TCP listener disappears (for example WinError 64 in the
    accept path), which the scheduler tick watchdog cannot observe.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        probe_interval_seconds: float = 5.0,
        startup_grace_seconds: float = 30.0,
        failure_threshold: int = 3,
        probe_timeout_seconds: float = 2.0,
        restart_delay_seconds: float = 5.0,
        probe: Callable[[str, int, float], bool] = probe_listener,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.probe_interval_seconds = max(1.0, float(probe_interval_seconds))
        self.startup_grace_seconds = max(1.0, float(startup_grace_seconds))
        self.failure_threshold = max(1, int(failure_threshold))
        self.probe_timeout_seconds = max(0.1, float(probe_timeout_seconds))
        self.restart_delay_seconds = max(0.0, float(restart_delay_seconds))
        self.probe = probe
        self.stop_requested = False
        self.child: subprocess.Popen | None = None

    def request_stop(self, *_args) -> None:
        self.stop_requested = True

    @staticmethod
    def _terminate(child: subprocess.Popen, grace_seconds: float = 10.0) -> None:
        if child.poll() is not None:
            return
        child.terminate()
        try:
            child.wait(timeout=max(0.1, grace_seconds))
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)

    def _spawn(self) -> subprocess.Popen:
        env = dict(os.environ)
        env["SLURM_SCHEDULER_UVICORN_WORKER"] = "1"
        command = [sys.executable, "-m", "slurm_scheduler"]
        LOGGER.info("starting web worker: %s", " ".join(command))
        return subprocess.Popen(command, env=env)

    def run(self) -> int:
        while not self.stop_requested:
            child = self._spawn()
            self.child = child
            gate = ListenerFailureGate(self.failure_threshold)
            started = time.monotonic()
            while not self.stop_requested and child.poll() is None:
                age = time.monotonic() - started
                healthy = self.probe(self.host, self.port, self.probe_timeout_seconds)
                if healthy:
                    gate.observe(True)
                elif age >= self.startup_grace_seconds:
                    restart = gate.observe(False)
                    restart = restart or (
                        not gate.was_healthy
                        and gate.consecutive_failures >= self.failure_threshold
                    )
                    if restart:
                        LOGGER.error(
                            "web worker PID %s is alive but listener %s:%s is unavailable for %s probes; restarting child only",
                            child.pid,
                            self.host,
                            self.port,
                            gate.consecutive_failures,
                        )
                        self._terminate(child)
                        break
                if child.poll() is None:
                    time.sleep(self.probe_interval_seconds)
            if self.stop_requested:
                self._terminate(child)
                return 0
            exit_code = child.poll()
            LOGGER.warning("web worker PID %s exited with code %s", child.pid, exit_code)
            if self.restart_delay_seconds:
                time.sleep(self.restart_delay_seconds)
        return 0
