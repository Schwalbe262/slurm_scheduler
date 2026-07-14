from __future__ import annotations

import os
import shlex
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from slurm_scheduler.config import AccountConfig, load_app_config
from slurm_scheduler.control_plane_relay import ControlPlaneRelay, relay_script_source


class _DummyHandler(BaseHTTPRequestHandler):
    calls: list[str] = []

    def do_GET(self) -> None:
        type(self).calls.append(self.path)
        payload = self.path.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        type(self).calls.append(self.path)
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        payload = self.headers.get("X-AEDT-Bootstrap-Token", "").encode("utf-8") + b":" + body
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def test_deployed_relay_script_forwards_allowed_path_and_rejects_other_paths(
    tmp_path: Path,
) -> None:
    _DummyHandler.calls = []
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _DummyHandler)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()
    relay_port = _free_port()
    script = tmp_path / "relay.py"
    script.write_text(relay_script_source(), encoding="utf-8")
    pidfile = tmp_path / "relay.pid"
    marker_file = tmp_path / "relay.marker"
    desired_file = tmp_path / "relay.desired"
    desired_file.write_text("relay-test-fingerprint\n", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            str(script),
            "--listen-host",
            "127.0.0.1",
            "--listen-port",
            str(relay_port),
            "--target-host",
            "127.0.0.1",
            "--target-port",
            str(upstream.server_port),
            "--allow-prefix",
            "/api/aedt-pool/",
            "--allow-prefix",
            "/healthz",
            "--pidfile",
            str(pidfile),
            "--marker-file",
            str(marker_file),
            "--marker",
            "relay-test-marker",
            "--fingerprint",
            "relay-test-fingerprint",
            "--desired-fingerprint-file",
            str(desired_file),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        allowed_body = ""
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{relay_port}/api/aedt-pool/leases?probe=1",
                    timeout=1,
                ) as response:
                    allowed_body = response.read().decode("utf-8")
                break
            except Exception as exc:
                last_error = exc
                time.sleep(0.05)
        else:
            stderr = process.stderr.read() if process.poll() is not None and process.stderr else ""
            pytest.fail(f"relay did not become ready: {last_error}; stderr={stderr}")

        assert allowed_body == "/api/aedt-pool/leases?probe=1"
        with pytest.raises(urllib.error.HTTPError) as rejected:
            urllib.request.urlopen(
                f"http://127.0.0.1:{relay_port}/api/tasks", timeout=2
            )
        assert rejected.value.code == 403
        assert _DummyHandler.calls == ["/api/aedt-pool/leases?probe=1"]

        with socket.create_connection(("127.0.0.1", relay_port), timeout=2) as client:
            client.sendall(b"GET //[ HTTP/1.1\r\nHost: relay\r\n\r\n")
            assert client.recv(4096).startswith(b"HTTP/1.1 400 Bad Request")

        post = urllib.request.Request(
            f"http://127.0.0.1:{relay_port}/api/aedt-pool/leases",
            data=b'{"project":"test"}',
            headers={
                "Content-Type": "application/json",
                "X-AEDT-Bootstrap-Token": "bootstrap-secret",
            },
            method="POST",
        )
        with urllib.request.urlopen(post, timeout=2) as response:
            assert response.read() == b'bootstrap-secret:{"project":"test"}'

        # A second request on an allowed connection must never bypass the
        # allowlist.  Depending on TCP packet boundaries the relay either
        # rejects the pipeline or forwards only /healthz and closes upstream.
        with socket.create_connection(("127.0.0.1", relay_port), timeout=2) as client:
            client.sendall(
                b"GET /healthz HTTP/1.1\r\nHost: relay\r\nContent-Length: 0\r\n\r\n"
                b"GET /api/tasks HTTP/1.1\r\nHost: relay\r\n\r\n"
            )
            client.shutdown(socket.SHUT_WR)
            while client.recv(4096):
                pass
        assert "/api/tasks" not in _DummyHandler.calls
        assert _DummyHandler.calls[:2] == [
            "/api/aedt-pool/leases?probe=1",
            "/api/aedt-pool/leases",
        ]
        assert marker_file.read_text(encoding="utf-8").strip() == "relay-test-marker"
        assert int(pidfile.read_text(encoding="utf-8").strip()) > 0
        desired_file.write_text("replacement-fingerprint\n", encoding="utf-8")
        assert process.wait(timeout=8) == 0
        assert not marker_file.exists()
        assert not pidfile.exists()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stderr:
            process.stderr.close()
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join(timeout=5)


class _Result:
    def __init__(self, stdout: str = "", stderr: str = "", exit_code: int = 0):
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code


class _BufferedChannel:
    def __init__(self, incoming: bytes) -> None:
        self.incoming = bytearray(incoming)
        self.sent = bytearray()
        self.closed = False

    def settimeout(self, _timeout: float) -> None:
        return

    def recv(self, size: int) -> bytes:
        if not self.incoming:
            return b""
        data = bytes(self.incoming[:size])
        del self.incoming[:size]
        return data

    def sendall(self, data: bytes) -> None:
        self.sent.extend(data)

    def close(self) -> None:
        self.closed = True


class _FakeTransport:
    def __init__(self) -> None:
        self.active = True
        self.requests: list[tuple[str, int, object]] = []
        self.cancels: list[tuple[str, int]] = []

    def is_active(self) -> bool:
        return self.active

    def request_port_forward(self, host: str, port: int, handler: object = None) -> int:
        self.requests.append((host, port, handler))
        return port

    def cancel_port_forward(self, host: str, port: int) -> None:
        self.cancels.append((host, port))


class _FakeParamikoClient:
    def __init__(self, transport: _FakeTransport) -> None:
        self.transport = transport

    def get_transport(self) -> _FakeTransport:
        return self.transport


class _FakeSSH:
    def __init__(self, remote_files: dict[str, str]) -> None:
        self.remote_files = remote_files
        self.transport = _FakeTransport()
        self.client = _FakeParamikoClient(self.transport)
        self.commands: list[str] = []
        self.closed = False

    def ensure_connected(self) -> None:
        return

    def write_text_file(self, path: str, text: str) -> None:
        self.remote_files[path] = text

    def read_text_file(self, path: str) -> str:
        if path not in self.remote_files:
            raise FileNotFoundError(path)
        return self.remote_files[path]

    def run(self, command: str, timeout: float | None = None) -> _Result:
        del timeout
        self.commands.append(command)
        if "then printf 'active\\n'; fi" in command:
            return _Result(stdout="active\n")
        if command.startswith("nohup "):
            words = shlex.split(command)
            marker = words[words.index("--marker") + 1]
            marker_file = words[words.index("--marker-file") + 1]
            pidfile = words[words.index("--pidfile") + 1]
            self.remote_files[marker_file] = marker + "\n"
            self.remote_files[pidfile] = "4242\n"
        if "kill -TERM" in command:
            for path in list(self.remote_files):
                if path.endswith((".marker", ".pid")):
                    self.remote_files.pop(path, None)
        return _Result()

    def close(self) -> None:
        self.closed = True
        self.transport.active = False


class _FakeSSHFactory:
    def __init__(self) -> None:
        self.remote_files: dict[str, str] = {}
        self.sessions: list[_FakeSSH] = []

    def __call__(self, _account: AccountConfig) -> _FakeSSH:
        session = _FakeSSH(self.remote_files)
        self.sessions.append(session)
        return session


def _account() -> AccountConfig:
    return AccountConfig(
        name="relay-account",
        host="gate2",
        port=22,
        username="cluster-user",
        private_key_path="C:/keys/cluster-user",
        remote_workspace="/work/cluster-user",
    )


def test_supervisor_reestablishes_tunnel_and_owned_relay_after_health_failure() -> None:
    factory = _FakeSSHFactory()
    probe_results = iter((True, False, True))
    published: list[str] = []
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path="/home/cluster-user/.cache/slurm/control-plane-relay.py",
        local_port=8000,
        ssh_factory=factory,
        probe=lambda _url, _timeout: next(probe_results),
        publish_url=published.append,
    )

    first = relay.tick()
    assert first["state"] == "up"
    assert len(factory.sessions) == 1
    assert factory.sessions[0].transport.requests[0][:2] == (
        "127.0.0.1",
        relay.tunnel_port,
    )

    second = relay.tick()
    assert second["state"] == "up"
    assert len(factory.sessions) == 2
    old = factory.sessions[0]
    assert old.closed
    assert old.transport.cancels == [("127.0.0.1", relay.tunnel_port)]
    assert any("kill -TERM" in command for command in old.commands)
    assert published == ["http://gate2:18790", "", "http://gate2:18790"]
    relay.stop()


def test_supervisor_never_kills_relay_when_remote_marker_does_not_match() -> None:
    factory = _FakeSSHFactory()
    probe_results = iter((True, True))
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path="/home/cluster-user/.cache/slurm/control-plane-relay.py",
        local_port=8000,
        ssh_factory=factory,
        probe=lambda _url, _timeout: next(probe_results),
    )
    assert relay.tick()["state"] == "up"
    marker_path = relay.remote_path + ".marker"
    factory.remote_files[marker_path] = "some-other-process\n"

    # The fake launch represents a free public port, so a replacement can
    # start after the foreign metadata is left untouched by teardown.  If an
    # actual foreign process owned the port, bind-before-marker publication in
    # the deployed script would instead make this tick fail closed.
    assert relay.tick()["state"] == "up"
    assert not any("kill -TERM" in command for command in factory.sessions[0].commands)
    relay.stop()


def test_supervisor_reestablishes_without_trusting_health_when_owned_pid_disappears() -> None:
    factory = _FakeSSHFactory()
    probe_results = iter((True, True))
    published: list[str] = []
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path="/home/cluster-user/.cache/slurm/control-plane-relay.py",
        local_port=8000,
        ssh_factory=factory,
        probe=lambda _url, _timeout: next(probe_results),
        publish_url=published.append,
    )
    assert relay.tick()["state"] == "up"
    factory.remote_files.pop(relay.remote_path + ".marker")
    factory.remote_files.pop(relay.remote_path + ".pid")

    # A different process could now answer on the public port, so the
    # supervisor must validate its PID/marker before accepting HTTP health.
    assert relay.tick()["state"] == "up"
    assert len(factory.sessions) == 2
    assert factory.sessions[0].closed
    assert not any("kill -TERM" in command for command in factory.sessions[0].commands)
    assert published == ["http://gate2:18790", "", "http://gate2:18790"]
    relay.stop()


def test_supervisor_reuses_healthy_relay_left_by_previous_scheduler_generation() -> None:
    factory = _FakeSSHFactory()
    path = "/home/cluster-user/.cache/slurm/control-plane-relay.py"
    published: list[str] = []
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path=path,
        local_port=8000,
        ssh_factory=factory,
        probe=lambda _url, _timeout: True,
        publish_url=published.append,
    )
    factory.remote_files[path + ".marker"] = relay._marker_prefix + "previous-generation\n"
    factory.remote_files[path + ".pid"] = "31337\n"
    factory.remote_files[path + ".desired"] = relay._fingerprint + "\n"

    assert relay.tick()["state"] == "up"
    session = factory.sessions[0]
    assert not any(command.startswith("nohup ") for command in session.commands)
    assert not any("kill -TERM" in command for command in session.commands)
    assert published == ["http://gate2:18790"]
    relay.stop()
    assert not any("kill -TERM" in command for command in session.commands)


def test_supervisor_requests_self_retirement_for_stale_relay_generation() -> None:
    factory = _FakeSSHFactory()
    path = "/home/cluster-user/.cache/slurm/control-plane-relay.py"
    factory.remote_files[path + ".marker"] = (
        "slurm-scheduler-control-plane-relay:old-fingerprint:old-generation\n"
    )
    factory.remote_files[path + ".pid"] = "31337\n"
    factory.remote_files[path + ".desired"] = "old-fingerprint\n"
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path=path,
        local_port=8000,
        ssh_factory=factory,
        probe=lambda _url, _timeout: pytest.fail("stale relay must not be probed"),
    )

    status = relay.tick()
    assert status["state"] == "down"
    assert "configuration change" in status["last_error"]
    assert factory.remote_files[path + ".desired"].strip() == relay._fingerprint
    assert not any(command.startswith("nohup ") for command in factory.sessions[0].commands)
    assert not any("kill -TERM" in command for command in factory.sessions[0].commands)


def test_forwarded_channel_callback_does_not_wait_for_supervisor_state_lock() -> None:
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path="/home/cluster-user/relay.py",
        local_port=_free_port(),
    )

    class Channel:
        def close(self) -> None:
            return

    completed = threading.Event()

    def invoke() -> None:
        relay._handle_forwarded_channel(Channel(), None, None)
        completed.set()

    callback = threading.Thread(target=invoke)
    with relay._lock:
        callback.start()
        assert completed.wait(1), "forward callback deadlocked behind the health-probe lock"
    callback.join(timeout=2)


def test_loopback_tunnel_bridge_enforces_same_path_allowlist_before_local_connect() -> None:
    _DummyHandler.calls = []
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _DummyHandler)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()
    relay = ControlPlaneRelay(
        enabled=True,
        account=_account(),
        relay_port=18790,
        remote_path="/home/cluster-user/relay.py",
        local_port=upstream.server_port,
    )
    try:
        rejected = _BufferedChannel(
            b"GET /api/tasks HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        relay._bridge_channel(rejected)
        assert bytes(rejected.sent).startswith(b"HTTP/1.1 403 Forbidden")
        assert _DummyHandler.calls == []

        malformed_target = _BufferedChannel(
            b"GET //[ HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        relay._bridge_channel(malformed_target)
        assert bytes(malformed_target.sent).startswith(b"HTTP/1.1 400 Bad Request")
        assert _DummyHandler.calls == []

        pipelined = _BufferedChannel(
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n"
            b"GET /api/tasks HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        relay._bridge_channel(pipelined)
        assert bytes(pipelined.sent).startswith(b"HTTP/1.1 400 Bad Request")
        assert _DummyHandler.calls == []

        oversized = _BufferedChannel(
            b"POST /api/aedt-pool/leases HTTP/1.1\r\n"
            b"Host: localhost\r\nContent-Length: 16777217\r\n\r\n"
        )
        relay._bridge_channel(oversized)
        assert bytes(oversized.sent).startswith(b"HTTP/1.1 413 Payload Too Large")
        assert _DummyHandler.calls == []

        allowed = _BufferedChannel(
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        relay._bridge_channel(allowed)
        assert bytes(allowed.sent).startswith(b"HTTP/1.0 200 OK")
        assert _DummyHandler.calls == ["/healthz"]
    finally:
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join(timeout=5)


def test_disabled_relay_has_no_threads_ssh_probes_or_published_setting() -> None:
    side_effects: list[str] = []

    def forbidden_factory(_account: AccountConfig) -> object:
        side_effects.append("ssh")
        raise AssertionError("disabled relay opened SSH")

    relay = ControlPlaneRelay(
        enabled=False,
        account=_account(),
        relay_port=18790,
        remote_path="/home/cluster-user/relay.py",
        ssh_factory=forbidden_factory,
        probe=lambda _url, _timeout: side_effects.append("probe") or True,
        publish_url=lambda _url: side_effects.append("publish"),
    )
    relay.start()
    assert relay.tick()["state"] == "disabled"
    relay.stop()
    assert side_effects == []
    assert relay._thread is None


def test_remote_path_change_uses_different_stable_loopback_tunnel_port() -> None:
    common = {
        "enabled": True,
        "account": _account(),
        "relay_port": 18790,
        "local_port": 8000,
    }
    first = ControlPlaneRelay(remote_path="/home/cluster-user/relay-v1.py", **common)
    same = ControlPlaneRelay(remote_path="/home/cluster-user/relay-v1.py", **common)
    moved = ControlPlaneRelay(remote_path="/home/cluster-user/relay-v2.py", **common)
    assert first.tunnel_port == same.tunnel_port
    assert first.tunnel_port != moved.tunnel_port


def test_relay_config_and_app_factory_pass_through(tmp_path: Path, monkeypatch) -> None:
    accounts_path = tmp_path / "accounts.yaml"
    accounts_path.write_text(
        """
accounts:
  - name: relay-account
    host: gate2
    port: 22
    username: cluster-user
    private_key_path: C:/keys/cluster-user
    remote_workspace: /work/cluster-user
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "app.yaml"
    config_path.write_text(
        f"""
database_path: {str(tmp_path / 'scheduler.db').replace(os.sep, '/')}
accounts_path: {str(accounts_path).replace(os.sep, '/')}
bind_host: 0.0.0.0
bind_port: 8123
aedt_pool:
  session_host_enabled: true
  host_remote_cwd: /work/aedt
  host_bootstrap_token_file: /shared/aedt-bootstrap-token
control_plane_relay:
  enabled: true
  account: relay-account
  port: 18790
  remote_path: /home/cluster-user/.cache/slurm/control-plane-relay.py
  allowed_prefixes:
    - /api/aedt-pool/
    - /healthz
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_app_config(config_path)
    assert config.control_plane_relay_enabled is True
    assert config.control_plane_relay_account == "relay-account"
    assert config.control_plane_relay_port == 18790
    assert config.control_plane_relay_remote_path.endswith("control-plane-relay.py")
    assert config.control_plane_relay_allowed_prefixes == [
        "/api/aedt-pool/",
        "/healthz",
    ]

    # Importing app.py constructs its module-level app, so point that harmless
    # construction at this temporary disabled service configuration too.
    monkeypatch.setenv("SLURM_SCHEDULER_CONFIG", str(config_path))
    monkeypatch.setenv("SLURM_AEDT_POOL_BOOTSTRAP_TOKEN", "bootstrap-secret")
    from slurm_scheduler.app import create_app

    app = create_app(str(config_path))
    startup = app.router.on_startup[-1]
    shutdown = app.router.on_shutdown[-1]
    lifecycle: list[str] = []
    with (
        patch("slurm_scheduler.app.cleanup_local_temp_artifacts"),
        patch.object(
            app.state.scheduler, "start", side_effect=lambda: lifecycle.append("scheduler-start")
        ),
        patch.object(
            app.state.control_plane_relay,
            "start",
            side_effect=lambda: lifecycle.append("relay-start"),
        ),
        patch.object(
            app.state.aedt_pool_runtime,
            "start",
            side_effect=lambda: lifecycle.append("pool-start"),
        ),
        patch.object(
            app.state.aedt_pool_runtime,
            "stop",
            side_effect=lambda: lifecycle.append("pool-stop"),
        ),
        patch.object(
            app.state.control_plane_relay,
            "stop",
            side_effect=lambda: lifecycle.append("relay-stop"),
        ),
        patch.object(
            app.state.scheduler, "stop", side_effect=lambda: lifecycle.append("scheduler-stop")
        ),
    ):
        startup()
        shutdown()
    assert lifecycle == [
        "scheduler-start",
        "relay-start",
        "pool-start",
        "pool-stop",
        "relay-stop",
        "scheduler-stop",
    ]
    app.router.on_startup.clear()
    app.router.on_shutdown.clear()
    configured = app.state.control_plane_relay
    assert configured.enabled is True
    assert configured.account.name == "relay-account"
    assert configured.relay_port == 18790
    assert configured.local_host == "127.0.0.1"
    assert configured.local_port == 8123
    assert configured.allowed_prefixes == ("/api/aedt-pool/", "/healthz")
    assert app.state.aedt_pool_runtime.require_published_control_plane_url is True
    assert app.state.aedt_pool.config().adapter_ready is True
    with patch.object(
        app.state.aedt_pool,
        "config",
        return_value=SimpleNamespace(operational=True, control_plane_url=""),
    ):
        reason = app.state.scheduler.aedt_backend_block_reason(
            {"aedt_backend": "pooled"}
        )
    assert "relay is unavailable" in reason
    route_paths = {getattr(route, "path", "") for route in app.routes}
    assert "/healthz" in route_paths
    assert "/api/control-plane-relay" in route_paths


def test_relay_config_defaults_are_disabled_and_empty(tmp_path: Path) -> None:
    config_path = tmp_path / "app.yaml"
    config_path.write_text("{}\n", encoding="utf-8")
    config = load_app_config(config_path)
    assert config.control_plane_relay_enabled is False
    assert config.control_plane_relay_account == ""
    assert config.control_plane_relay_port == 18790
    assert config.control_plane_relay_remote_path == ""
    assert config.control_plane_relay_allowed_prefixes == [
        "/api/aedt-pool/",
        "/healthz",
    ]
