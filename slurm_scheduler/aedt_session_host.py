from __future__ import annotations

import argparse
import json
import os
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
    ) -> None:
        self.client = client
        self.allocation_id = int(allocation_id)
        self.node_name = node_name
        self.requested_session_id = max(0, int(session_id))
        self.heartbeat_seconds = max(5, int(heartbeat_seconds))
        self.aedt_version = aedt_version
        self.host_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.session_id = 0
        self.host_token = ""
        self.desktop: Any = None
        self.stop_requested = False

    def request_stop(self, *_args: Any) -> None:
        self.stop_requested = True

    def _start_desktop(self) -> Any:
        try:
            from ansys.aedt.core import Desktop
        except ImportError as exc:
            raise RuntimeError("PyAEDT is required on the session-host node") from exc
        kwargs: dict[str, Any] = {
            "new_desktop": True,
            "non_graphical": True,
            "close_on_exit": False,
        }
        if self.aedt_version:
            kwargs["version"] = self.aedt_version
        return Desktop(**kwargs)

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
        for attempt in range(3):
            try:
                claimed = self.client.request(
                    "POST",
                    "/api/aedt-pool/hosts/claim-start",
                    claim_payload,
                ).get("session")
                break
            except urllib.error.HTTPError as exc:
                if not 500 <= exc.code < 600 or attempt == 2:
                    raise
            except (urllib.error.URLError, OSError, json.JSONDecodeError):
                if attempt == 2:
                    raise
            if self.stop_requested:
                raise RuntimeError("session host stopped during start claim")
            time.sleep(1)
        if not claimed:
            return 0
        self.session_id = int(claimed["id"])
        try:
            self.desktop = self._start_desktop()
            endpoint = f"{socket.getfqdn()}:{self._desktop_port(self.desktop)}"
            registration_token = secrets.token_urlsafe(32)
            for attempt in range(3):
                try:
                    registered = self.client.request(
                        "POST",
                        f"/api/aedt-pool/sessions/{self.session_id}/register",
                        {
                            "host_id": self.host_id,
                            "endpoint": endpoint,
                            "process_id": self._desktop_pid(self.desktop),
                        },
                        host_token=registration_token,
                    )
                    break
                except urllib.error.HTTPError as exc:
                    retryable = exc.code == 409 or 500 <= exc.code < 600
                    if not retryable or attempt == 2:
                        raise
                except (urllib.error.URLError, OSError, json.JSONDecodeError):
                    if attempt == 2:
                        raise
                if self.stop_requested:
                    raise RuntimeError("session host stopped during registration")
                time.sleep(1)
            self.host_token = str(registered["host_token"])
            while not self.stop_requested:
                self.client.request(
                    "POST",
                    f"/api/aedt-pool/sessions/{self.session_id}/heartbeat",
                    {},
                    host_token=self.host_token,
                )
                commands = self.client.request(
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
                    self.client.request(
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
    )
    signal.signal(signal.SIGTERM, host.request_stop)
    signal.signal(signal.SIGINT, host.request_stop)
    return host.run()


if __name__ == "__main__":
    raise SystemExit(main())
