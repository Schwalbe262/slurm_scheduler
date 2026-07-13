from __future__ import annotations

import os
import socket
import unittest
from types import SimpleNamespace
from unittest import mock

from slurm_scheduler import __main__ as scheduler_main
from slurm_scheduler.web_supervisor import ListenerFailureGate, WebWorkerSupervisor, probe_listener


class ListenerFailureGateTests(unittest.TestCase):
    def test_single_transient_failure_does_not_restart(self) -> None:
        gate = ListenerFailureGate(threshold=3)
        self.assertFalse(gate.observe(True))
        self.assertFalse(gate.observe(False))
        self.assertEqual(gate.consecutive_failures, 1)
        self.assertFalse(gate.observe(True))
        self.assertEqual(gate.consecutive_failures, 0)

    def test_live_pid_with_vanished_listener_triggers_after_threshold(self) -> None:
        gate = ListenerFailureGate(threshold=3)
        self.assertFalse(gate.observe(True))
        self.assertFalse(gate.observe(False))
        self.assertFalse(gate.observe(False))
        self.assertTrue(gate.observe(False))

    def test_tcp_probe_observes_listener_not_process_identity(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = int(listener.getsockname()[1])
        try:
            self.assertTrue(probe_listener("127.0.0.1", port, 0.5))
        finally:
            listener.close()
        self.assertFalse(probe_listener("127.0.0.1", port, 0.2))


class _ExitedChild:
    pid = 1234

    def poll(self) -> int:
        return 70


class WebWorkerSupervisorTests(unittest.TestCase):
    def test_exited_worker_exits_parent_without_self_respawn(self) -> None:
        supervisor = WebWorkerSupervisor(host="127.0.0.1", port=8000)
        with mock.patch.object(supervisor, "_spawn", return_value=_ExitedChild()) as spawn:
            self.assertEqual(supervisor.run(), 70)
        spawn.assert_called_once_with()

    def test_python_startup_rejects_existing_listener_before_worker_start(self) -> None:
        config = SimpleNamespace(
            bind_host="127.0.0.1",
            bind_port=8000,
            web_listener_probe_timeout_seconds=2,
            web_listener_watchdog_enabled=False,
        )
        with (
            mock.patch.object(scheduler_main, "load_app_config", return_value=config),
            mock.patch.object(scheduler_main, "configure_logging"),
            mock.patch.object(scheduler_main, "probe_listener", return_value=True),
            mock.patch.object(scheduler_main, "run_uvicorn_worker") as run_worker,
            self.assertRaises(SystemExit) as raised,
        ):
            scheduler_main.main()
        self.assertEqual(raised.exception.code, scheduler_main.DUPLICATE_LISTENER_EXIT_CODE)
        run_worker.assert_not_called()

    @unittest.skipUnless(os.name == "nt", "Windows listener reservation semantics")
    def test_windows_listener_reservation_rejects_duplicate_before_listen(self) -> None:
        first = scheduler_main._reserve_listener("127.0.0.1", 0)
        port = int(first.getsockname()[1])
        try:
            self.assertEqual(first.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR), 0)
            with self.assertRaises(OSError):
                scheduler_main._reserve_listener("127.0.0.1", port)
        finally:
            first.close()

    def test_worker_bind_failure_exits_before_uvicorn_server_start(self) -> None:
        owner = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner.bind(("127.0.0.1", 0))
        owner.listen(1)
        config = SimpleNamespace(
            bind_host="127.0.0.1",
            bind_port=int(owner.getsockname()[1]),
            web_timeout_keep_alive_seconds=5,
            web_timeout_graceful_shutdown_seconds=15,
            web_limit_concurrency=64,
        )
        try:
            with (
                mock.patch.object(scheduler_main.uvicorn, "Server") as server,
                self.assertRaises(SystemExit) as raised,
            ):
                scheduler_main.run_uvicorn_worker(config)
        finally:
            owner.close()
        self.assertEqual(raised.exception.code, scheduler_main.DUPLICATE_LISTENER_EXIT_CODE)
        server.assert_not_called()


if __name__ == "__main__":
    unittest.main()
