from __future__ import annotations

import socket
import unittest

from slurm_scheduler.web_supervisor import ListenerFailureGate, probe_listener


class ListenerFailureGateTests(unittest.TestCase):
    def test_single_transient_failure_does_not_restart(self) -> None:
        gate = ListenerFailureGate(threshold=3)
        self.assertFalse(gate.observe(True))
        self.assertFalse(gate.observe(False))
        self.assertEqual(gate.consecutive_failures, 1)
        self.assertFalse(gate.observe(True))
        self.assertEqual(gate.consecutive_failures, 0)

    def test_live_pid_with_vanished_listener_restarts_after_threshold(self) -> None:
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


if __name__ == "__main__":
    unittest.main()
