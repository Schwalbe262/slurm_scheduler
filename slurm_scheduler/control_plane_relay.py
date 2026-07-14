from __future__ import annotations

import hashlib
import logging
import posixpath
import shlex
import socket
import threading
import textwrap
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from .config import AccountConfig
from .slurm import SSHSession


LOGGER = logging.getLogger(__name__)

# The live API uses the hyphenated aedt-pool spelling.  Keep the trailing slash
# so similarly named scheduler endpoints are not accidentally exposed.
DEFAULT_ALLOWED_PREFIXES = ("/api/aedt-pool/", "/healthz")
MAX_REQUEST_LINE = 16384
MAX_REQUEST_HEADERS = 65536
MAX_REQUEST_BODY = 16 * 1024 * 1024


RELAY_SCRIPT = textwrap.dedent(
    r'''
    #!/usr/bin/env python3
    """Small allowlisted TCP relay used by slurm_scheduler.

    This file is generated and supervised by the scheduler.  It intentionally
    depends only on the Python standard library available on the login node.
    """

    import argparse
    import os
    import signal
    import socket
    import socketserver
    import threading
    import time
    import urllib.parse


    MAX_REQUEST_LINE = 16384
    MAX_REQUEST_HEADERS = 65536
    MAX_REQUEST_BODY = 16 * 1024 * 1024


    def _reply(sock, status, body):
        payload = body.encode("utf-8")
        response = (
            "HTTP/1.1 %s\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "Content-Length: %d\r\n"
            "Connection: close\r\n"
            "\r\n" % (status, len(payload))
        ).encode("ascii") + payload
        try:
            sock.sendall(response)
        except OSError:
            pass


    def _pump(source, destination):
        try:
            while True:
                data = source.recv(65536)
                if not data:
                    break
                destination.sendall(data)
        except OSError:
            pass
        finally:
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass


    class RelayHandler(socketserver.BaseRequestHandler):
        def handle(self):
            initial = bytearray()
            request_deadline = time.monotonic() + 60

            def receive(size):
                remaining_time = request_deadline - time.monotonic()
                if remaining_time <= 0:
                    raise TimeoutError("request deadline exceeded")
                self.request.settimeout(min(15, remaining_time))
                return self.request.recv(size)

            try:
                while b"\r\n" not in initial and len(initial) <= MAX_REQUEST_LINE:
                    chunk = receive(4096)
                    if not chunk:
                        return
                    initial.extend(chunk)
            except OSError:
                return
            if b"\r\n" not in initial or len(initial) > MAX_REQUEST_LINE:
                _reply(self.request, "400 Bad Request", "bad request\n")
                return
            request_line = bytes(initial).split(b"\r\n", 1)[0]
            try:
                method, target, version = request_line.decode("iso-8859-1").split()
            except (UnicodeDecodeError, ValueError):
                _reply(self.request, "400 Bad Request", "bad request\n")
                return
            if not method or not version.startswith("HTTP/") or not target.startswith("/"):
                _reply(self.request, "400 Bad Request", "bad request\n")
                return
            try:
                path = urllib.parse.urlsplit(target).path
            except ValueError:
                _reply(self.request, "400 Bad Request", "bad request target\n")
                return
            if not any(path.startswith(prefix) for prefix in self.server.allowed_prefixes):
                _reply(self.request, "403 Forbidden", "forbidden\n")
                return

            # This is deliberately a one-request connection.  Framing one
            # Content-Length body prevents an allowed first request from
            # smuggling a disallowed pipelined request through the byte stream.
            try:
                while b"\r\n\r\n" not in initial and len(initial) <= MAX_REQUEST_HEADERS:
                    chunk = receive(4096)
                    if not chunk:
                        return
                    initial.extend(chunk)
            except OSError:
                return
            if b"\r\n\r\n" not in initial or len(initial) > MAX_REQUEST_HEADERS:
                _reply(self.request, "431 Request Header Fields Too Large", "headers too large\n")
                return
            raw_head, initial_body = bytes(initial).split(b"\r\n\r\n", 1)
            header_lines = raw_head.split(b"\r\n")[1:]
            forwarded_headers = []
            content_lengths = []
            for raw_header in header_lines:
                name, separator, value = raw_header.partition(b":")
                if not separator or not name.strip():
                    _reply(self.request, "400 Bad Request", "bad request\n")
                    return
                lower_name = name.strip().lower()
                stripped_value = value.strip()
                if lower_name == b"transfer-encoding":
                    _reply(self.request, "501 Not Implemented", "chunked requests unsupported\n")
                    return
                if lower_name == b"expect":
                    _reply(self.request, "417 Expectation Failed", "expect unsupported\n")
                    return
                if lower_name == b"content-length":
                    try:
                        content_lengths.append(int(stripped_value))
                    except ValueError:
                        _reply(self.request, "400 Bad Request", "bad content length\n")
                        return
                if lower_name not in {b"connection", b"proxy-connection"}:
                    forwarded_headers.append(raw_header)
            if any(length < 0 for length in content_lengths) or len(set(content_lengths)) > 1:
                _reply(self.request, "400 Bad Request", "bad content length\n")
                return
            content_length = content_lengths[0] if content_lengths else 0
            if content_length > MAX_REQUEST_BODY:
                _reply(self.request, "413 Payload Too Large", "request body too large\n")
                return
            if len(initial_body) > content_length:
                _reply(self.request, "400 Bad Request", "pipelined requests forbidden\n")
                return
            forwarded_head = b"\r\n".join(
                [request_line, *forwarded_headers, b"Connection: close", b"", b""]
            )
            try:
                upstream = socket.create_connection(
                    (self.server.target_host, self.server.target_port), timeout=15
                )
            except OSError:
                _reply(self.request, "502 Bad Gateway", "upstream unavailable\n")
                return
            try:
                upstream.sendall(forwarded_head)
                if initial_body:
                    upstream.sendall(initial_body)
                remaining = content_length - len(initial_body)
                try:
                    while remaining:
                        chunk = receive(min(65536, remaining))
                        if not chunk:
                            return
                        upstream.sendall(chunk)
                        remaining -= len(chunk)
                except OSError:
                    return
                upstream.settimeout(30)
                _pump(upstream, self.request)
            finally:
                try:
                    upstream.close()
                except OSError:
                    pass


    class RelayServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address = True
        daemon_threads = True

        def __init__(self, *args, **kwargs):
            self._connection_slots = threading.BoundedSemaphore(64)
            super().__init__(*args, **kwargs)

        def process_request(self, request, client_address):
            if not self._connection_slots.acquire(blocking=False):
                _reply(request, "503 Service Unavailable", "relay busy\n")
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, client_address)
            except BaseException:
                self._connection_slots.release()
                raise

        def process_request_thread(self, request, client_address):
            try:
                super().process_request_thread(request, client_address)
            finally:
                self._connection_slots.release()


    def _atomic_write(path, value):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        temporary = "%s.%s.tmp" % (path, os.getpid())
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)


    def _remove_owned_files(marker_file, pidfile, marker):
        try:
            with open(marker_file, "r", encoding="utf-8") as handle:
                if handle.read().strip() != marker:
                    return
        except OSError:
            return
        for path in (pidfile, marker_file):
            try:
                os.unlink(path)
            except OSError:
                pass


    def main():
        parser = argparse.ArgumentParser()
        parser.add_argument("--listen-host", default="0.0.0.0")
        parser.add_argument("--listen-port", type=int, required=True)
        parser.add_argument("--target-host", default="127.0.0.1")
        parser.add_argument("--target-port", type=int, required=True)
        parser.add_argument("--allow-prefix", action="append", required=True)
        parser.add_argument("--pidfile", required=True)
        parser.add_argument("--marker-file", required=True)
        parser.add_argument("--marker", required=True)
        parser.add_argument("--fingerprint", required=True)
        parser.add_argument("--desired-fingerprint-file", required=True)
        args = parser.parse_args()

        # Bind before publishing ownership files.  If another process owns the
        # port, this process exits without overwriting its marker.
        server = RelayServer((args.listen_host, args.listen_port), RelayHandler)
        server.target_host = args.target_host
        server.target_port = args.target_port
        server.allowed_prefixes = tuple(args.allow_prefix)
        _atomic_write(args.marker_file, args.marker + "\n")
        _atomic_write(args.pidfile, str(os.getpid()) + "\n")

        stopping = threading.Event()

        def request_stop(_signum, _frame):
            if stopping.is_set():
                return
            stopping.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)

        def watch_desired_generation():
            while not stopping.wait(5):
                try:
                    with open(
                        args.desired_fingerprint_file, "r", encoding="utf-8"
                    ) as handle:
                        desired = handle.read().strip()
                except OSError:
                    continue
                if desired and desired != args.fingerprint:
                    request_stop(signal.SIGTERM, None)
                    return

        def watch_tunnel_target():
            failures = 0
            while not stopping.wait(10):
                try:
                    probe = socket.create_connection(
                        (args.target_host, args.target_port), timeout=2
                    )
                    probe.close()
                    failures = 0
                except OSError:
                    failures += 1
                    if failures >= 6:
                        request_stop(signal.SIGTERM, None)
                        return

        threading.Thread(target=watch_desired_generation, daemon=True).start()
        threading.Thread(target=watch_tunnel_target, daemon=True).start()
        try:
            server.serve_forever(poll_interval=0.5)
        finally:
            server.server_close()
            _remove_owned_files(args.marker_file, args.pidfile, args.marker)
        return 0


    if __name__ == "__main__":
        raise SystemExit(main())
    '''
).lstrip()


def relay_script_source() -> str:
    """Return the exact self-contained script deployed to the login node."""

    return RELAY_SCRIPT


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value is not None else None


def _default_probe(url: str, timeout: float) -> bool:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "Connection": "close"},
        method="GET",
    )
    # Do not let a scheduler-PC HTTP proxy turn this into a false-positive
    # health check.  The probe must traverse the login-node listener.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        return response.status == 200


class ControlPlaneRelay:
    """Supervise one login-node relay and its SSH reverse tunnel.

    The SSH transport is deliberately long lived and independent of the
    scheduler's per-tick SSH cache.  A remote TCP connection to the loopback
    tunnel is delivered as a Paramiko channel; a small local bridge connects
    that channel to the scheduler HTTP listener.
    """

    def __init__(
        self,
        *,
        enabled: bool = False,
        account: AccountConfig | None = None,
        relay_port: int = 18790,
        remote_path: str = "",
        allowed_prefixes: list[str] | tuple[str, ...] | None = None,
        local_host: str = "127.0.0.1",
        local_port: int = 8000,
        interval_seconds: int = 30,
        ssh_factory: Callable[[AccountConfig], Any] | None = None,
        probe: Callable[[str, float], bool] | None = None,
        publish_url: Callable[[str], None] | None = None,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        self.enabled = bool(enabled)
        self.account = account
        self.relay_port = int(relay_port)
        self.remote_path = str(remote_path or "").strip()
        prefix_source = (
            DEFAULT_ALLOWED_PREFIXES if allowed_prefixes is None else allowed_prefixes
        )
        configured_prefixes = [
            str(prefix).strip()
            for prefix in prefix_source
            if str(prefix).strip()
        ]
        self.allowed_prefixes = tuple(configured_prefixes)
        self.local_host = local_host.strip() or "127.0.0.1"
        self.local_port = int(local_port)
        self.interval_seconds = max(5, int(interval_seconds))
        self._ssh_factory = ssh_factory or (lambda item: SSHSession(item, default_timeout=30))
        self._probe = probe or _default_probe
        self._publish_url = publish_url
        self._now = now

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._channels_lock = threading.Lock()
        self._channel_slots = threading.BoundedSemaphore(64)
        self._session: Any = None
        self._transport: Any = None
        tunnel_identity = (
            f"{self.remote_path}\0"
            f"{self.account.username if self.account is not None else ''}"
        )
        self._tunnel_port = self._derived_tunnel_port(
            self.relay_port, tunnel_identity
        )
        self._resolved_remote_path = ""
        fingerprint_payload = repr(
            (
                RELAY_SCRIPT,
                self.relay_port,
                self._tunnel_port,
                self.allowed_prefixes,
            )
        ).encode("utf-8")
        self._fingerprint = hashlib.sha256(fingerprint_payload).hexdigest()
        self._marker_prefix = (
            f"slurm-scheduler-control-plane-relay:{self._fingerprint}:"
        )
        self._marker = self._marker_prefix + uuid.uuid4().hex
        self._relay_owned = False
        self._adopted_marker = ""
        self._expected_remote_marker = ""
        self._channels: set[Any] = set()

        now_value = self._now()
        self._state = "disabled" if not self.enabled else "down"
        self._status_since = now_value
        self._up_since: datetime | None = None
        self._down_since: datetime | None = now_value if self.enabled else None
        self._last_error = "" if not self.enabled else "relay has not started"
        self._published_value: str | None = None

    @staticmethod
    def _derived_tunnel_port(relay_port: int, identity: str = "") -> int:
        # Keep this stable across worker generations so an orphaned but valid
        # relay can attach to the replacement SSH tunnel.  Include remote-path
        # identity so moving the managed files does not keep an old relay's
        # watchdog artificially healthy forever.
        digest = hashlib.sha256(
            f"{relay_port}\0{identity}".encode("utf-8")
        ).digest()
        candidate = 20000 + int.from_bytes(digest[:4], "big") % 40000
        if candidate == relay_port:
            candidate = 20000 + ((candidate - 20000 + 1) % 40000)
        return candidate

    @property
    def relay_url(self) -> str:
        if self.account is None:
            return ""
        host = self.account.host.strip()
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self.relay_port}"

    @property
    def tunnel_port(self) -> int:
        return self._tunnel_port

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": self.enabled,
                "state": self._state,
                "since": _iso(self._status_since),
                "up_since": _iso(self._up_since),
                "down_since": _iso(self._down_since),
                "last_error": self._last_error,
                "relay_url": self.relay_url,
                "account": self.account.name if self.account is not None else "",
                "relay_port": self.relay_port,
                "tunnel_port": self._tunnel_port,
            }

    def start(self) -> None:
        # Disabled means a strict no-op: no DB write, thread, SSH connection,
        # upload, probe, or remote command.
        if not self.enabled:
            return
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._mark_down_locked("relay is starting")
            self._thread = threading.Thread(
                target=self._loop,
                name="control-plane-relay",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        if not self.enabled:
            return
        self._stop.set()
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=10)
        with self._lock:
            self._teardown_locked()
            self._mark_down_locked("relay stopped")
        self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            status = self.tick()
            delay = self.interval_seconds if status["state"] == "up" else 5
            self._stop.wait(delay)

    def tick(self) -> dict[str, Any]:
        if not self.enabled:
            return self.status()
        with self._lock:
            try:
                self._validate_configuration()
            except Exception as exc:
                self._mark_down_locked(str(exc))
                return self.status()

            failure = ""
            if self._session is not None:
                if self._transport is None or not self._transport.is_active():
                    failure = "SSH reverse tunnel is inactive"
                else:
                    active_marker = self._active_managed_relay_marker(self._session)
                    if (
                        not self._expected_remote_marker
                        or active_marker != self._expected_remote_marker
                    ):
                        failure = "remote relay PID/marker ownership check failed"
                    else:
                        try:
                            if self._probe(self.relay_url + "/healthz", self._probe_timeout):
                                self._mark_up_locked()
                                return self.status()
                            failure = "end-to-end relay health probe returned non-200"
                        except Exception as exc:
                            failure = f"end-to-end relay health probe failed: {exc}"
                self._mark_down_locked(failure)
                retirement_requested = self._retire_adopted_relay_locked()
                self._teardown_locked()
                if retirement_requested:
                    return self.status()

            try:
                self._establish_locked()
                if not self._probe(self.relay_url + "/healthz", self._probe_timeout):
                    raise RuntimeError("end-to-end relay health probe returned non-200")
                self._mark_up_locked()
            except Exception as exc:
                LOGGER.warning("control-plane relay unavailable: %s", exc)
                self._retire_adopted_relay_locked()
                self._teardown_locked()
                self._mark_down_locked(str(exc))
            return self.status()

    @property
    def _probe_timeout(self) -> float:
        return float(max(1, min(10, self.interval_seconds)))

    def _validate_configuration(self) -> None:
        if self.account is None:
            raise ValueError("control-plane relay account is not configured")
        if not self.remote_path:
            raise ValueError("control-plane relay remote path is not configured")
        if not 1 <= self.relay_port <= 65535:
            raise ValueError("control-plane relay port must be between 1 and 65535")
        if not 1 <= self.local_port <= 65535:
            raise ValueError("scheduler HTTP port must be between 1 and 65535")
        if not self.allowed_prefixes:
            raise ValueError("control-plane relay path allowlist is empty")
        if any(not prefix.startswith("/") for prefix in self.allowed_prefixes):
            raise ValueError("control-plane relay allowed prefixes must start with /")

    def _establish_locked(self) -> None:
        assert self.account is not None
        session = self._ssh_factory(self.account)
        try:
            session.ensure_connected()
            transport = session.client.get_transport()
            if transport is None or not transport.is_active():
                raise RuntimeError("SSH transport is not active")
            requested_port = self._tunnel_port
            assigned_port = transport.request_port_forward(
                "127.0.0.1",
                requested_port,
                handler=self._handle_forwarded_channel,
            )
            self._session = session
            self._transport = transport
            self._tunnel_port = int(assigned_port or requested_port)
            self._resolved_remote_path = self._resolve_remote_path(session)
            existing_marker = self._active_managed_relay_marker(session)
            if existing_marker:
                desired = self._remote_desired_fingerprint(session)
                if not existing_marker.startswith(self._marker_prefix):
                    self._prepare_remote_generation(session)
                    raise RuntimeError(
                        "previous remote relay generation is retiring after a configuration change"
                    )
                if desired and desired != self._fingerprint:
                    raise RuntimeError("previous remote relay generation is retiring")
                # A previous scheduler generation may have crashed after
                # launching the relay.  The deterministic loopback tunnel port
                # lets this generation safely reuse that process.  It remains
                # unowned unless the marker is our own, so shutdown will never
                # signal a process this supervisor did not launch.
                self._relay_owned = existing_marker == self._marker
                self._adopted_marker = "" if self._relay_owned else existing_marker
                self._expected_remote_marker = existing_marker
                self._prepare_remote_generation(session)
                return
            self._prepare_remote_generation(session)
            self._deploy_remote_relay_locked(session)
        except Exception:
            try:
                session.close()
            except Exception:
                pass
            if self._session is session:
                self._session = None
                self._transport = None
            raise

    def _prepare_remote_generation(self, session: Any) -> None:
        path = self._resolved_remote_path
        parent = posixpath.dirname(path) or "."
        result = session.run(f"mkdir -p -- {shlex.quote(parent)}")
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or "could not create relay directory")
        session.write_text_file(path + ".desired", self._fingerprint + "\n")

    def _remote_desired_fingerprint(self, session: Any) -> str:
        try:
            return session.read_text_file(
                self._resolved_remote_path + ".desired"
            ).strip()
        except Exception:
            return ""

    def _retire_adopted_relay_locked(self) -> bool:
        if not self._adopted_marker or self._session is None:
            return False
        try:
            self._session.write_text_file(
                self._resolved_remote_path + ".desired",
                f"retire:{uuid.uuid4().hex}\n",
            )
            return True
        except Exception:
            LOGGER.exception("could not request retirement of adopted remote relay")
            return False

    def _resolve_remote_path(self, session: Any) -> str:
        path = self.remote_path
        if not (path.startswith("~/") or path.startswith("$HOME/")):
            return path
        result = session.run("printf '%s\\n' \"$HOME\"")
        if result.exit_code != 0 or not result.stdout.strip():
            raise RuntimeError(result.stderr.strip() or "could not resolve remote home directory")
        suffix = path[2:] if path.startswith("~/") else path[len("$HOME/") :]
        return posixpath.join(result.stdout.strip(), suffix)

    def _deploy_remote_relay_locked(self, session: Any) -> None:
        path = self._resolved_remote_path
        session.write_text_file(path, RELAY_SCRIPT)
        result = session.run(f"chmod 700 -- {shlex.quote(path)}")
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or "could not make relay script executable")

        pidfile = path + ".pid"
        marker_file = path + ".marker"
        log_file = path + ".log"
        arguments = [
            "python3",
            path,
            "--listen-host",
            "0.0.0.0",
            "--listen-port",
            str(self.relay_port),
            "--target-host",
            "127.0.0.1",
            "--target-port",
            str(self._tunnel_port),
            "--pidfile",
            pidfile,
            "--marker-file",
            marker_file,
            "--marker",
            self._marker,
            "--fingerprint",
            self._fingerprint,
            "--desired-fingerprint-file",
            path + ".desired",
        ]
        for prefix in self.allowed_prefixes:
            arguments.extend(("--allow-prefix", prefix))
        command = " ".join(shlex.quote(item) for item in arguments)
        expected_marker = shlex.quote(self._marker)
        launch = (
            f"nohup {command} >> {shlex.quote(log_file)} 2>&1 < /dev/null & "
            "ready=0; "
            "for i in 1 2 3 4 5 6 7 8 9 10; do "
            f"test -f {shlex.quote(marker_file)} && "
            f"test \"$(cat {shlex.quote(marker_file)} 2>/dev/null)\" = {expected_marker} && "
            f"test -f {shlex.quote(pidfile)} && "
            f"kill -0 \"$(cat {shlex.quote(pidfile)} 2>/dev/null)\" 2>/dev/null && "
            "ready=1 && break; sleep 0.2; done; test \"$ready\" = 1"
        )
        result = session.run(launch, timeout=10)
        if result.exit_code != 0:
            if self._remote_owned_marker(session)[0]:
                self._relay_owned = True
                self._stop_remote_relay_locked()
            raise RuntimeError(result.stderr.strip() or "remote relay did not start")
        self._relay_owned = True
        self._adopted_marker = ""
        self._expected_remote_marker = self._marker

    def _active_managed_relay_marker(self, session: Any) -> str:
        path = self._resolved_remote_path
        if not path:
            return ""
        try:
            marker = session.read_text_file(path + ".marker").strip()
            pid = session.read_text_file(path + ".pid").strip()
        except Exception:
            return ""
        if (
            not marker.startswith("slurm-scheduler-control-plane-relay:")
            or not pid.isdigit()
            or int(pid) <= 0
        ):
            return ""
        quoted_marker = shlex.quote(marker)
        quoted_path = shlex.quote(path)
        command = (
            f"if test -r /proc/{pid}/cmdline && "
            f"tr '\\000' '\\n' < /proc/{pid}/cmdline | grep -Fqx -- {quoted_marker} && "
            f"tr '\\000' '\\n' < /proc/{pid}/cmdline | grep -Fqx -- {quoted_path}; "
            "then printf 'active\\n'; fi"
        )
        try:
            result = session.run(command, timeout=5)
        except Exception:
            return ""
        return marker if result.exit_code == 0 and result.stdout.strip() == "active" else ""

    def _remote_owned_marker(self, session: Any) -> tuple[str, str]:
        path = self._resolved_remote_path
        if not path:
            return "", ""
        try:
            marker = session.read_text_file(path + ".marker").strip()
            pid = session.read_text_file(path + ".pid").strip()
        except Exception:
            return "", ""
        if marker != self._marker or not pid.isdigit() or int(pid) <= 0:
            return "", ""
        return marker, pid

    def _stop_remote_relay_locked(self) -> None:
        session = self._session
        if session is None or not self._relay_owned:
            return
        marker, pid = self._remote_owned_marker(session)
        if not marker:
            # Marker mismatch means ownership is not proven.  Do not even send
            # a remote command containing a signal operation.
            self._relay_owned = False
            return
        path = self._resolved_remote_path
        marker_file = path + ".marker"
        pidfile = path + ".pid"
        quoted_pid = shlex.quote(pid)
        quoted_marker = shlex.quote(marker)
        quoted_path = shlex.quote(path)
        ownership_check = (
            f"test \"$(cat {shlex.quote(marker_file)} 2>/dev/null)\" = {quoted_marker} && "
            f"test -r /proc/{quoted_pid}/cmdline && "
            f"tr '\\000' '\\n' < /proc/{quoted_pid}/cmdline | grep -Fqx -- {quoted_marker} && "
            f"tr '\\000' '\\n' < /proc/{quoted_pid}/cmdline | grep -Fqx -- {quoted_path}"
        )
        command = (
            f"if {ownership_check}; then "
            f"kill -TERM {quoted_pid} 2>/dev/null || true; "
            "for i in 1 2 3 4 5 6 7 8 9 10; do "
            f"kill -0 {quoted_pid} 2>/dev/null || break; sleep 0.2; done; "
            f"if kill -0 {quoted_pid} 2>/dev/null && {ownership_check}; then "
            f"kill -KILL {quoted_pid} 2>/dev/null || true; fi; "
            f"test \"$(cat {shlex.quote(marker_file)} 2>/dev/null)\" != {quoted_marker} || "
            f"rm -f -- {shlex.quote(pidfile)} {shlex.quote(marker_file)}; "
            "fi"
        )
        try:
            session.run(command, timeout=10)
        except Exception:
            LOGGER.exception("could not stop owned remote control-plane relay")
        self._relay_owned = False

    def _teardown_locked(self) -> None:
        self._stop_remote_relay_locked()
        transport = self._transport
        if transport is not None:
            try:
                transport.cancel_port_forward("127.0.0.1", self._tunnel_port)
            except Exception:
                pass
        with self._channels_lock:
            channels = list(self._channels)
            self._channels.clear()
        for channel in channels:
            try:
                channel.close()
            except Exception:
                pass
        session = self._session
        self._session = None
        self._transport = None
        self._adopted_marker = ""
        self._expected_remote_marker = ""
        if session is not None:
            try:
                session.close()
            except Exception:
                pass

    def _handle_forwarded_channel(self, channel: Any, _origin: Any, _server: Any) -> None:
        # Paramiko invokes this callback while a health probe may hold the
        # supervisor state lock.  Channel bookkeeping therefore has its own
        # lock; otherwise the very probe that proves the tunnel would prevent
        # its forwarded channel from being bridged.
        if not self._channel_slots.acquire(blocking=False):
            self._channel_error(channel, "503 Service Unavailable", "relay busy\n")
            try:
                channel.close()
            except Exception:
                pass
            return
        with self._channels_lock:
            self._channels.add(channel)
        try:
            threading.Thread(
                target=self._bridge_channel,
                args=(channel, True),
                name="control-plane-relay-connection",
                daemon=True,
            ).start()
        except Exception:
            with self._channels_lock:
                self._channels.discard(channel)
            self._channel_slots.release()
            try:
                channel.close()
            except Exception:
                pass
            raise

    def _bridge_channel(self, channel: Any, release_slot: bool = False) -> None:
        local_socket: socket.socket | None = None
        try:
            request_deadline = time.monotonic() + 60

            def receive(size: int) -> bytes:
                remaining_time = request_deadline - time.monotonic()
                if remaining_time <= 0:
                    raise TimeoutError("request deadline exceeded")
                try:
                    channel.settimeout(min(15, remaining_time))
                except Exception:
                    pass
                return channel.recv(size)

            initial = bytearray()
            while b"\r\n" not in initial and len(initial) <= MAX_REQUEST_LINE:
                chunk = receive(4096)
                if not chunk:
                    return
                initial.extend(chunk)
            if b"\r\n" not in initial or len(initial) > MAX_REQUEST_LINE:
                self._channel_error(channel, "400 Bad Request", "bad request\n")
                return
            request_line = bytes(initial).split(b"\r\n", 1)[0]
            try:
                method, target, version = request_line.decode("iso-8859-1").split()
            except (UnicodeDecodeError, ValueError):
                self._channel_error(channel, "400 Bad Request", "bad request\n")
                return
            if not method or not version.startswith("HTTP/") or not target.startswith("/"):
                self._channel_error(channel, "400 Bad Request", "bad request\n")
                return
            try:
                path = urllib.parse.urlsplit(target).path
            except ValueError:
                self._channel_error(channel, "400 Bad Request", "bad request target\n")
                return
            if not any(path.startswith(prefix) for prefix in self.allowed_prefixes):
                self._channel_error(channel, "403 Forbidden", "forbidden\n")
                return
            while b"\r\n\r\n" not in initial and len(initial) <= MAX_REQUEST_HEADERS:
                chunk = receive(4096)
                if not chunk:
                    return
                initial.extend(chunk)
            if b"\r\n\r\n" not in initial or len(initial) > MAX_REQUEST_HEADERS:
                self._channel_error(
                    channel,
                    "431 Request Header Fields Too Large",
                    "headers too large\n",
                )
                return
            raw_head, initial_body = bytes(initial).split(b"\r\n\r\n", 1)
            forwarded_headers: list[bytes] = []
            content_lengths: list[int] = []
            for raw_header in raw_head.split(b"\r\n")[1:]:
                name, separator, value = raw_header.partition(b":")
                if not separator or not name.strip():
                    self._channel_error(channel, "400 Bad Request", "bad request\n")
                    return
                lower_name = name.strip().lower()
                if lower_name == b"transfer-encoding":
                    self._channel_error(
                        channel, "501 Not Implemented", "chunked requests unsupported\n"
                    )
                    return
                if lower_name == b"expect":
                    self._channel_error(
                        channel, "417 Expectation Failed", "expect unsupported\n"
                    )
                    return
                if lower_name == b"content-length":
                    try:
                        content_lengths.append(int(value.strip()))
                    except ValueError:
                        self._channel_error(
                            channel, "400 Bad Request", "bad content length\n"
                        )
                        return
                if lower_name not in {b"connection", b"proxy-connection"}:
                    forwarded_headers.append(raw_header)
            if any(length < 0 for length in content_lengths) or len(set(content_lengths)) > 1:
                self._channel_error(channel, "400 Bad Request", "bad content length\n")
                return
            content_length = content_lengths[0] if content_lengths else 0
            if content_length > MAX_REQUEST_BODY:
                self._channel_error(
                    channel, "413 Payload Too Large", "request body too large\n"
                )
                return
            if len(initial_body) > content_length:
                self._channel_error(
                    channel, "400 Bad Request", "pipelined requests forbidden\n"
                )
                return

            local_socket = socket.create_connection((self.local_host, self.local_port), timeout=10)
            local_socket.sendall(
                b"\r\n".join(
                    [request_line, *forwarded_headers, b"Connection: close", b"", b""]
                )
            )
            if initial_body:
                local_socket.sendall(initial_body)
            remaining = content_length - len(initial_body)
            while remaining:
                chunk = receive(min(65536, remaining))
                if not chunk:
                    return
                local_socket.sendall(chunk)
                remaining -= len(chunk)
            local_socket.settimeout(30)
            while not self._stop.is_set():
                data = local_socket.recv(65536)
                if not data:
                    break
                channel.sendall(data)
        except Exception:
            LOGGER.debug("forwarded relay channel closed", exc_info=True)
        finally:
            if local_socket is not None:
                try:
                    local_socket.close()
                except OSError:
                    pass
            try:
                channel.close()
            except Exception:
                pass
            with self._channels_lock:
                self._channels.discard(channel)
            if release_slot:
                self._channel_slots.release()

    @staticmethod
    def _channel_error(channel: Any, status: str, body: str) -> None:
        payload = body.encode("utf-8")
        response = (
            f"HTTP/1.1 {status}\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            f"Content-Length: {len(payload)}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii") + payload
        try:
            channel.sendall(response)
        except Exception:
            pass

    def _publish_locked(self, value: str) -> None:
        if self._publish_url is None or value == self._published_value:
            return
        self._publish_url(value)
        self._published_value = value

    def _mark_up_locked(self) -> None:
        self._publish_locked(self.relay_url)
        if self._state != "up":
            now = self._now()
            self._state = "up"
            self._status_since = now
            self._up_since = now
        self._last_error = ""

    def _mark_down_locked(self, error: str) -> None:
        try:
            self._publish_locked("")
        except Exception as exc:
            error = f"{error}; could not clear published relay URL: {exc}"
        if self._state != "down":
            now = self._now()
            self._state = "down"
            self._status_since = now
            self._down_since = now
        elif self._down_since is None:
            self._down_since = self._status_since
        self._last_error = error
