from __future__ import annotations

import argparse
import http.client
import json
import math
import os
import random
import secrets
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any


# Leave one HTTP-timeout/backoff margin beyond the required five-minute outage.
DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS = 360.0
CONTROL_PLANE_OUTAGE_ENV = "AEDT_SESSION_HOST_CONTROL_PLANE_OUTAGE_SECONDS"
DESKTOP_LAUNCH_ATTEMPTS = 3
DESKTOP_LAUNCH_RETRY_SECONDS = 2.0
TRANSIENT_HTTP_STATUSES = {408, 425, 429}
TERMINAL_REGISTRATION_CONFLICT_MARKERS = (
    "not owned",
    "no longer available",
    "session is failed",
    "session is closed",
    "session is cancelled",
    "session is expired",
)


def _install_pyaedt_psutil_cmdline_shim(psutil_module: Any | None = None) -> None:
    """Backport PyAEDT's guard for processes whose cmdline is unreadable."""

    if psutil_module is None:
        try:
            import psutil as psutil_module
        except ImportError:
            return
    original = psutil_module.process_iter
    if getattr(original, "_aedt_cmdline_none_shim", False):
        return

    def sanitized_process_iter(*args: Any, **kwargs: Any):
        for process in original(*args, **kwargs):
            info = getattr(process, "info", None)
            # PyAEDT 0.22 active_sessions() assumes cmdline is iterable.  Linux
            # psutil legitimately returns None for zombies/unreadable /proc
            # rows.  Remove this shim after upgrading beyond that upstream bug.
            if isinstance(info, dict) and info.get("cmdline") is None:
                info["cmdline"] = []
            yield process

    sanitized_process_iter._aedt_cmdline_none_shim = True  # type: ignore[attr-defined]
    if hasattr(original, "cache_clear"):
        sanitized_process_iter.cache_clear = original.cache_clear  # type: ignore[attr-defined]
    psutil_module.process_iter = sanitized_process_iter


class ControlPlaneClient:
    def __init__(self, base_url: str, *, bootstrap_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.bootstrap_token = bootstrap_token

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        host_token: str = "",
    ) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if method.upper() not in {"GET", "HEAD", "OPTIONS"}:
            headers["X-AEDT-Bootstrap-Token"] = self.bootstrap_token
        if host_token:
            headers["X-AEDT-Host-Token"] = host_token
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8") or "{}")


class AedtSessionHost:
    """Own exactly one AEDT process and at most two leased projects.

    Project workers may attach through the advertised gRPC endpoint, but they
    never own Desktop lifecycle.  This process alone closes projects and kills
    Desktop.  A solver timeout is deliberately session-scoped: it quarantines
    the session and waits for the sibling grace command before calling AEDT's
    global StopSimulations API.
    """

    def __init__(
        self,
        client: ControlPlaneClient,
        *,
        allocation_id: int,
        node_name: str,
        session_id: int = 0,
        heartbeat_seconds: int = 20,
        aedt_version: str = "",
        control_plane_outage_seconds: float = DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS,
    ) -> None:
        self.client = client
        self.allocation_id = int(allocation_id)
        self.node_name = node_name
        self.requested_session_id = max(0, int(session_id))
        self.heartbeat_seconds = max(5, int(heartbeat_seconds))
        self.aedt_version = aedt_version
        outage_seconds = float(control_plane_outage_seconds)
        if not math.isfinite(outage_seconds):
            raise ValueError("control-plane outage budget must be finite")
        self.control_plane_outage_seconds = max(0.0, outage_seconds)
        self.retry_initial_seconds = 1.0
        self.retry_max_seconds = 20.0
        self.host_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.session_id = 0
        self.host_token = ""
        self.desktop: Any = None
        self.stop_requested = False

    def request_stop(self, *_args: Any) -> None:
        self.stop_requested = True

    @staticmethod
    def _http_error_detail(exc: urllib.error.HTTPError) -> str:
        try:
            raw = exc.read()
        except Exception:
            return ""
        if not raw:
            return ""
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        try:
            payload = json.loads(text)
        except (TypeError, json.JSONDecodeError):
            return text.strip()
        if isinstance(payload, dict):
            return str(payload.get("detail") or "").strip()
        return text.strip()

    @classmethod
    def _registration_conflict_is_terminal(
        cls, exc: urllib.error.HTTPError
    ) -> bool:
        detail = cls._http_error_detail(exc).lower()
        return any(marker in detail for marker in TERMINAL_REGISTRATION_CONFLICT_MARKERS)

    def _control_plane_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        host_token: str = "",
        retry_registration_conflict: bool = False,
    ) -> dict[str, Any]:
        """Retry one logical control-plane operation within one outage budget.

        Registration keeps the three-attempt opaque-409 exception introduced by
        the startup-race fix.  Explicit ownership/allocation conflicts remain
        terminal, as do every other non-transient 4xx response.
        """

        outage_started_at = time.monotonic()
        retry_index = 0
        registration_conflicts = 0
        while True:
            if self.stop_requested:
                raise RuntimeError("session host stopped during control-plane retry")
            try:
                return self.client.request(
                    method,
                    path,
                    payload,
                    host_token=host_token,
                )
            except urllib.error.HTTPError as exc:
                retryable = exc.code in TRANSIENT_HTTP_STATUSES or 500 <= exc.code < 600
                if exc.code == 409 and retry_registration_conflict:
                    if self._registration_conflict_is_terminal(exc):
                        raise
                    registration_conflicts += 1
                    # Preserve commit 83f77ee's bounded registration-race
                    # behavior instead of treating an opaque conflict as a
                    # five-minute control-plane outage.
                    if registration_conflicts >= 3:
                        raise
                    retryable = True
                if not retryable:
                    raise
                last_error: BaseException = exc
            except (
                urllib.error.URLError,
                OSError,
                json.JSONDecodeError,
                http.client.HTTPException,
            ) as exc:
                last_error = exc

            elapsed = max(0.0, time.monotonic() - outage_started_at)
            remaining = self.control_plane_outage_seconds - elapsed
            if remaining <= 0:
                raise last_error
            base_delay = min(
                self.retry_max_seconds,
                self.retry_initial_seconds * (2 ** min(retry_index, 20)),
            )
            # Equal jitter keeps the capped steady-state cadence dispersed too:
            # exponential base/2 plus a random value in the other half.
            jitter = random.uniform(0.0, base_delay * 0.5)
            delay = min(remaining, base_delay * 0.5 + jitter)
            print(
                f"AEDT control-plane request {method} {path} failed; "
                f"retrying in {delay:.1f}s: {last_error}",
                file=sys.stderr,
            )
            time.sleep(delay)
            retry_index += 1

    def _create_desktop(self, *, new_desktop: bool, port: int) -> Any:
        try:
            from ansys.aedt.core import Desktop
        except ImportError as exc:
            raise RuntimeError("PyAEDT is required on the session-host node") from exc
        kwargs: dict[str, Any] = {
            "new_desktop": bool(new_desktop),
            "non_graphical": True,
            "close_on_exit": False,
            "port": int(port),
        }
        if self.aedt_version:
            kwargs["version"] = self.aedt_version
        return Desktop(**kwargs)

    @staticmethod
    def _find_free_desktop_port() -> int:
        # Select the port here so a failed constructor can be recovered by an
        # explicit-port attach without asking PyAEDT to rediscover sessions.
        for _attempt in range(100):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
                candidate.bind(("127.0.0.1", 0))
                port = int(candidate.getsockname()[1])
            if port not in range(50051, 50070):
                return port
        raise RuntimeError("could not select an AEDT gRPC port")

    @classmethod
    def _validate_desktop(
        cls, desktop: Any, *, expected_port: int | None = None
    ) -> Any:
        if desktop is None or getattr(desktop, "odesktop", None) is None:
            raise RuntimeError("PyAEDT returned an uninitialized Desktop")
        port = cls._desktop_port(desktop)
        if port <= 0:
            raise RuntimeError("PyAEDT returned an invalid AEDT gRPC port")
        if expected_port is not None and port != int(expected_port):
            raise RuntimeError(
                f"PyAEDT attached to gRPC port {port}, expected {expected_port}"
            )
        try:
            pid = int(cls._desktop_pid(desktop))
        except (TypeError, ValueError):
            pid = 0
        if pid <= 0:
            raise RuntimeError("PyAEDT returned an invalid AEDT process ID")
        return desktop

    @staticmethod
    def _desktop_port_is_listening(port: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.5)
                return probe.connect_ex(("127.0.0.1", int(port))) == 0
        except OSError:
            return False

    @staticmethod
    def _owned_desktop_pid_on_port(port: int, started_after: float) -> str:
        """Find only a newly launched, current-user ansysedt on an exact port."""

        try:
            import psutil

            current_user = str(psutil.Process(os.getpid()).username() or "").lower()
        except Exception:
            return ""
        matches: list[str] = []
        for process in psutil.process_iter(
            attrs=("pid", "name", "username", "cmdline", "create_time")
        ):
            try:
                info = process.info
                name = str(info.get("name") or "").lower()
                username = str(info.get("username") or "").lower()
                cmdline = info.get("cmdline") or []
                if not name.startswith("ansysedt") or username != current_user:
                    continue
                flag_index = cmdline.index("-grpcsrv")
                if int(cmdline[flag_index + 1]) != int(port):
                    continue
                # A small tolerance covers process timestamp resolution without
                # risking an older same-user Desktop that happened to use port.
                if float(info.get("create_time") or 0) < started_after - 5:
                    continue
                matches.append(str(int(info["pid"])))
            except Exception:
                continue
        return matches[0] if len(matches) == 1 else ""

    def _cleanup_failed_desktop_launch(
        self, desktop: Any, *, port: int, started_after: float
    ) -> None:
        pid_text = self._desktop_pid(desktop) if desktop is not None else ""
        try:
            pid = int(pid_text)
        except (TypeError, ValueError):
            pid = 0
        if pid <= 0:
            pid_text = self._owned_desktop_pid_on_port(port, started_after)
        try:
            pid = int(pid_text)
        except (TypeError, ValueError):
            return
        marker = self._process_marker(pid)
        self._force_kill_owned_desktop(pid_text, marker)

    def _start_desktop(self) -> Any:
        # Must run before importing/constructing Desktop because PyAEDT's
        # general_methods module retains the shared psutil module object.
        _install_pyaedt_psutil_cmdline_shim()
        last_error: BaseException | None = None
        for attempt in range(1, DESKTOP_LAUNCH_ATTEMPTS + 1):
            if self.stop_requested:
                raise RuntimeError("session host stopped during AEDT launch retry")
            port = self._find_free_desktop_port()
            launch_started_at = time.time()
            desktop: Any = None
            try:
                desktop = self._create_desktop(new_desktop=True, port=port)
                return self._validate_desktop(desktop)
            except Exception as launch_error:
                last_error = launch_error
                fallback_port = port
                try:
                    if desktop is not None:
                        fallback_port = self._desktop_port(desktop)
                    if not self._desktop_port_is_listening(fallback_port):
                        raise RuntimeError(
                            f"launched AEDT gRPC port {fallback_port} is not listening"
                        )
                    # PyAEDT 0.22 skips active_sessions() when an explicit port
                    # is occupied.  The probe prevents new_desktop=False from
                    # silently becoming a second launch when no server exists.
                    recovered_desktop = self._create_desktop(
                        new_desktop=False, port=fallback_port
                    )
                    recovered = self._validate_desktop(
                        recovered_desktop, expected_port=fallback_port
                    )
                    print(
                        f"Recovered AEDT Desktop on explicit gRPC port {fallback_port} "
                        f"after launch initialization failed: {launch_error}",
                        file=sys.stderr,
                    )
                    return recovered
                except Exception as attach_error:
                    last_error = attach_error
                    try:
                        self._cleanup_failed_desktop_launch(
                            desktop,
                            port=fallback_port,
                            started_after=launch_started_at,
                        )
                    except Exception as cleanup_error:
                        print(
                            f"Failed to clean AEDT Desktop launch on port "
                            f"{fallback_port}: {cleanup_error}",
                            file=sys.stderr,
                        )
                    print(
                        f"AEDT Desktop launch attempt {attempt}/{DESKTOP_LAUNCH_ATTEMPTS} "
                        f"failed on port {fallback_port}: {launch_error}; "
                        f"explicit-port attach failed: {attach_error}",
                        file=sys.stderr,
                    )
            if attempt < DESKTOP_LAUNCH_ATTEMPTS:
                time.sleep(DESKTOP_LAUNCH_RETRY_SECONDS)
        raise RuntimeError(
            f"AEDT Desktop launch failed after {DESKTOP_LAUNCH_ATTEMPTS} attempts: "
            f"{last_error}"
        ) from last_error

    @staticmethod
    def _desktop_port(desktop: Any) -> int:
        for owner in (desktop, getattr(desktop, "odesktop", None)):
            if owner is None:
                continue
            for name in ("port", "grpc_port", "_grpc_port"):
                value = getattr(owner, name, None)
                if value:
                    return int(value)
        raise RuntimeError("could not determine AEDT gRPC port")

    @staticmethod
    def _desktop_pid(desktop: Any) -> str:
        for name in ("aedt_process_id", "process_id", "pid"):
            value = getattr(desktop, name, None)
            if value:
                return str(value)
        return ""

    def _close_project(self, project_name: str) -> None:
        odesktop = getattr(self.desktop, "odesktop", None)
        if odesktop is not None:
            try:
                if project_name not in {str(name) for name in odesktop.GetProjectList()}:
                    return
            except Exception:
                pass
        close_project = getattr(self.desktop, "close_project", None)
        if callable(close_project):
            try:
                close_project(project_name, save_project=False)
            except TypeError:
                close_project(project_name)
            return
        if odesktop is None:
            raise RuntimeError("AEDT Desktop object has no project-close API")
        odesktop.CloseProject(project_name)

    def _global_stop(self) -> None:
        """Global by AEDT design; caller must honor the sibling grace gate."""
        odesktop = getattr(self.desktop, "odesktop", None)
        if odesktop is None:
            raise RuntimeError("AEDT Desktop object has no StopSimulations API")
        odesktop.StopSimulations(False)

    def _close_desktop(self) -> None:
        if self.desktop is None:
            return
        release = getattr(self.desktop, "release_desktop", None)
        if callable(release):
            try:
                release(close_projects=True, close_on_exit=True)
            except TypeError:
                release(True, True)
        self.desktop = None

    @staticmethod
    def _process_marker(pid: int) -> str | None:
        try:
            import psutil
        except Exception:
            psutil = None
        if psutil is not None:
            try:
                return f"psutil:{psutil.Process(pid).create_time():.6f}"
            except Exception:
                pass
        try:
            stat_text = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
            fields = stat_text.rsplit(")", 1)[1].strip().split()
            return f"proc:{fields[19]}"
        except Exception:
            return None

    @classmethod
    def _force_kill_owned_desktop(cls, pid_text: str, expected_marker: str | None) -> None:
        try:
            pid = int(pid_text)
        except (TypeError, ValueError):
            return
        if pid <= 0 or pid == os.getpid():
            return
        if expected_marker is not None and cls._process_marker(pid) != expected_marker:
            # Original AEDT is already gone and the numeric PID was reused.
            return
        try:
            import psutil
        except Exception:
            psutil = None
        if psutil is not None:
            try:
                process = psutil.Process(pid)
                children = process.children(recursive=True)
            except Exception:
                process = None
                children = []
            if process is not None:
                for child in reversed(children):
                    try:
                        child.terminate()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                try:
                    process.terminate()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                _gone, alive = psutil.wait_procs([*children, process], timeout=5)
                for item in alive:
                    try:
                        item.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                return
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            return

    @classmethod
    def _process_alive(cls, pid_text: str, expected_marker: str | None) -> bool | None:
        try:
            pid = int(pid_text)
        except (TypeError, ValueError):
            return None
        if pid <= 0:
            return None
        current_marker = cls._process_marker(pid)
        if expected_marker is not None:
            return current_marker == expected_marker
        if current_marker is not None:
            return True
        try:
            import psutil
        except Exception:
            psutil = None
        if psutil is not None:
            return psutil.pid_exists(pid)
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False

    def _bounded_close_desktop(self, *, global_stop: bool, timeout_seconds: int = 30) -> bool:
        """Bound a potentially wedged gRPC stop/release, then kill only owned AEDT."""
        desktop = self.desktop
        pid_text = self._desktop_pid(desktop) if desktop is not None else ""
        try:
            pid_value = int(pid_text)
        except (TypeError, ValueError):
            pid_value = 0
        expected_marker = self._process_marker(pid_value) if pid_value > 0 else None
        errors: list[BaseException] = []

        def graceful() -> None:
            try:
                if global_stop:
                    self._global_stop()
                self._close_desktop()
            except BaseException as exc:  # keep cleanup fail-safe even for gRPC runtime errors
                errors.append(exc)

        thread = threading.Thread(target=graceful, name="aedt-bounded-close", daemon=True)
        thread.start()
        thread.join(timeout=max(1, int(timeout_seconds)))
        alive = self._process_alive(pid_text, expected_marker)
        if thread.is_alive() or errors or alive is True:
            self._force_kill_owned_desktop(pid_text, expected_marker)
            self.desktop = None
            deadline = time.monotonic() + 5
            while (
                time.monotonic() < deadline
                and self._process_alive(pid_text, expected_marker) is True
            ):
                time.sleep(0.2)
            alive = self._process_alive(pid_text, expected_marker)
        if alive is None:
            return not thread.is_alive() and not errors
        return alive is False

    def _report_closed(self, *, success: bool, message: str = "", requeue: bool = True) -> None:
        if not self.session_id or not self.host_token:
            return
        try:
            self.client.request(
                "POST",
                f"/api/aedt-pool/sessions/{self.session_id}/closed",
                {
                    "success": success,
                    "failure_message": message,
                    "requeue_siblings": requeue,
                },
                host_token=self.host_token,
            )
        except Exception:
            # The durable control plane will mark the missing heartbeat
            # unhealthy; never hide the original host failure.
            pass

    def run(self) -> int:
        claim_payload = {
            "allocation_id": self.allocation_id,
            "node_name": self.node_name,
            "host_id": self.host_id,
            "session_id": self.requested_session_id,
        }
        claimed = self._control_plane_request(
            "POST",
            "/api/aedt-pool/hosts/claim-start",
            claim_payload,
        ).get("session")
        if not claimed:
            return 0
        self.session_id = int(claimed["id"])
        try:
            self.desktop = self._start_desktop()
            endpoint = f"{socket.getfqdn()}:{self._desktop_port(self.desktop)}"
            registration_token = secrets.token_urlsafe(32)
            registered = self._control_plane_request(
                "POST",
                f"/api/aedt-pool/sessions/{self.session_id}/register",
                {
                    "host_id": self.host_id,
                    "endpoint": endpoint,
                    "process_id": self._desktop_pid(self.desktop),
                },
                host_token=registration_token,
                retry_registration_conflict=True,
            )
            self.host_token = str(registered["host_token"])
            while not self.stop_requested:
                self._control_plane_request(
                    "POST",
                    f"/api/aedt-pool/sessions/{self.session_id}/heartbeat",
                    {},
                    host_token=self.host_token,
                )
                commands = self._control_plane_request(
                    "GET",
                    f"/api/aedt-pool/sessions/{self.session_id}/commands",
                    host_token=self.host_token,
                )
                for lease in commands.get("close_projects") or []:
                    success = True
                    failure = ""
                    try:
                        self._close_project(str(lease["project_name"]))
                    except Exception as exc:
                        success = False
                        failure = str(exc)
                    self._control_plane_request(
                        "POST",
                        f"/api/aedt-pool/sessions/{self.session_id}/leases/{int(lease['id'])}/release-complete",
                        {"success": success, "failure_message": failure},
                        host_token=self.host_token,
                    )
                if commands.get("global_stop_allowed"):
                    # Never use this path until the control plane says the
                    # sibling finished or its explicitly bounded grace elapsed.
                    confirmed = self._bounded_close_desktop(global_stop=True)
                    if confirmed:
                        self._report_closed(
                            success=False,
                            message="quarantined AEDT session globally stopped and recycled",
                            requeue=True,
                        )
                        return 2
                    print(
                        "AEDT recycle could not confirm process exit; session remains counted unhealthy",
                        file=sys.stderr,
                    )
                    return 3
                if commands.get("drain") and not commands.get("sibling_live_count"):
                    confirmed = self._bounded_close_desktop(global_stop=False)
                    if confirmed:
                        self._report_closed(success=True)
                        return 0
                    print(
                        "AEDT drain could not confirm process exit; session remains counted unhealthy",
                        file=sys.stderr,
                    )
                    return 3
                time.sleep(self.heartbeat_seconds)
        except Exception as exc:
            try:
                confirmed = self._bounded_close_desktop(global_stop=False)
            except Exception:
                confirmed = False
            if confirmed:
                if self.host_token:
                    self._report_closed(success=False, message=str(exc), requeue=True)
                elif self.session_id:
                    try:
                        self.client.request(
                            "POST",
                            f"/api/aedt-pool/sessions/{self.session_id}/start-failed",
                            {"host_id": self.host_id, "failure_message": str(exc)},
                        )
                    except Exception:
                        pass
            print(f"AEDT session host failed: {exc}", file=sys.stderr)
            return 1
        finally:
            if self.stop_requested and self.desktop is not None:
                # Slurm/task cancellation is a session drain.  It may affect
                # both projects, so mark both leases for retry.
                confirmed = False
                try:
                    confirmed = self._bounded_close_desktop(global_stop=False)
                finally:
                    if confirmed:
                        self._report_closed(
                            success=False,
                            message="session host received termination signal",
                            requeue=True,
                        )
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Own one pooled AEDT Desktop process")
    parser.add_argument("--scheduler-url", required=True)
    parser.add_argument("--allocation-id", required=True, type=int)
    parser.add_argument("--node-name", required=True)
    parser.add_argument("--session-id", type=int, default=0)
    parser.add_argument("--bootstrap-token-file", required=True)
    parser.add_argument("--heartbeat-seconds", type=int, default=20)
    parser.add_argument("--aedt-version", default="")
    return parser


def _control_plane_outage_seconds_from_env() -> float:
    value = os.environ.get(CONTROL_PLANE_OUTAGE_ENV, "").strip()
    if not value:
        return DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS
    try:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError
        return max(0.0, parsed)
    except ValueError:
        print(
            f"Ignoring invalid {CONTROL_PLANE_OUTAGE_ENV}={value!r}; "
            f"using {DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS:.0f}s",
            file=sys.stderr,
        )
        return DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bootstrap_token = Path(args.bootstrap_token_file).read_text(encoding="utf-8").strip()
    if not bootstrap_token:
        raise SystemExit("bootstrap token file is empty")
    host = AedtSessionHost(
        ControlPlaneClient(args.scheduler_url, bootstrap_token=bootstrap_token),
        allocation_id=args.allocation_id,
        node_name=args.node_name,
        session_id=args.session_id,
        heartbeat_seconds=args.heartbeat_seconds,
        aedt_version=args.aedt_version,
        control_plane_outage_seconds=_control_plane_outage_seconds_from_env(),
    )
    signal.signal(signal.SIGTERM, host.request_stop)
    signal.signal(signal.SIGINT, host.request_stop)
    return host.run()


if __name__ == "__main__":
    raise SystemExit(main())
