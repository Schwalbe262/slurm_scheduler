from __future__ import annotations

import argparse
import hashlib
import http.client
import importlib.resources
import json
import math
import os
import random
import re
import secrets
import signal
import socket
import stat
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from .aedt_automation_lock import (
    SessionAutomationLock,
    automation_lock_path,
    create_automation_lock_file,
)


# Leave one HTTP-timeout/backoff margin beyond the required five-minute outage.
DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS = 360.0
CONTROL_PLANE_OUTAGE_ENV = "AEDT_SESSION_HOST_CONTROL_PLANE_OUTAGE_SECONDS"
ARTIFACT_ROOT_ENV = "AEDT_SESSION_HOST_ARTIFACT_ROOT"
DSO_PROFILE_ENV = "AEDT_SESSION_HOST_DSO_PROFILE"
SESSION_PROFILE_ENV = "AEDT_SESSION_HOST_PROFILE"
SUPPORTED_DSO_PROFILE = "maxwell-2d3d-icepak-4c1e"
LEGACY_DSO_PROFILE = "maxwell-2d3d-4c1e"
SUPPORTED_DSO_PROFILES = {SUPPORTED_DSO_PROFILE, LEGACY_DSO_PROFILE}
DSO_TEMPLATE_PACKAGE = "ansys.aedt.core.misc"
DSO_TEMPLATE_FILENAME = "pyaedt_local_config.acf"
DSO_CONFIG_NAME = "pyaedt_config"
EXPECTED_SESSION_PROFILE = {
    "profile_version": 2,
    "aedt_version": "2025.2",
    "python_environment": "pyaedt2026v1",
    "pyaedt_version": "0.22.0",
    "filesystem": "gpfs-shared-v1",
    "desktop_dso": {
        "config_name": "pyaedt_config",
        "designs": {
            "Icepak": {
                "cores": 4,
                "tasks": 1,
                "gpus": 0,
                "use_auto_settings": False,
            },
            "Maxwell 2D": {
                "cores": 4,
                "tasks": 1,
                "gpus": 0,
                "use_auto_settings": True,
            },
            "Maxwell 3D": {
                "cores": 4,
                "tasks": 1,
                "gpus": 0,
                "use_auto_settings": True,
            },
        },
    },
}
EXPECTED_SESSION_PROFILE_JSON = json.dumps(
    EXPECTED_SESSION_PROFILE,
    ensure_ascii=False,
    sort_keys=True,
    separators=(",", ":"),
)
EXPECTED_AEDT_VERSION = str(EXPECTED_SESSION_PROFILE["aedt_version"])
EXPECTED_PYAEDT_VERSION = str(EXPECTED_SESSION_PROFILE["pyaedt_version"])
DESKTOP_LAUNCH_ATTEMPTS = 3
DESKTOP_LAUNCH_RETRY_SECONDS = 5.0
DESKTOP_LAUNCH_RETRY_MAX_SECONDS = 30.0
NATIVE_PROBE_FAILURES_BEFORE_RECYCLE = 3
NATIVE_PROBE_FAILURE_WINDOW_SECONDS = 60.0
NATIVE_PROBE_OK = "ok"
NATIVE_PROBE_FAILED = "failed"
NATIVE_PROBE_DEFERRED_BUSY = "deferred_busy"
NATIVE_PROBE_NOT_RUN = "not_run"
TRANSIENT_HTTP_STATUSES = {408, 425, 429}
TERMINAL_REGISTRATION_CONFLICT_MARKERS = (
    "not owned",
    "no longer available",
    "session is failed",
    "session is closed",
    "session is cancelled",
    "session is expired",
)


class ControlPlaneUnavailable(RuntimeError):
    """A retryable control-plane outage exhausted one bounded request window."""


def canonical_expected_session_profile(value: Any) -> str:
    """Validate and canonicalize the one attested Desktop-global profile."""

    candidate = value
    if isinstance(candidate, str):
        normalized = candidate.strip()
        if not normalized:
            raise ValueError("AEDT host session profile is required")
        try:
            candidate = json.loads(normalized)
        except json.JSONDecodeError as exc:
            raise ValueError("AEDT host session profile must be valid JSON") from exc
    if candidate != EXPECTED_SESSION_PROFILE:
        raise ValueError(
            "AEDT host session profile does not match the canonical "
            f"{SUPPORTED_DSO_PROFILE} contract"
        )
    return EXPECTED_SESSION_PROFILE_JSON


def is_expected_session_profile(value: Any) -> bool:
    try:
        canonical_expected_session_profile(value)
    except (TypeError, ValueError):
        return False
    return True


def normalize_aedt_version(value: Any) -> str:
    """Normalize AEDT's verbose GetVersion result to its year.release pair."""

    match = re.search(r"(?<!\d)(20\d{2}\.\d)(?!\d)", str(value or ""))
    return match.group(1) if match else ""


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


def _validate_pyaedt_dso_template(template: str) -> None:
    """Reject truncated ACF files before asking AEDT to load them."""

    stack: list[str] = []
    paths: set[tuple[str, ...]] = set()
    for line in template.splitlines():
        stripped = line.strip()
        begin = re.fullmatch(r"\$begin\s+'([^']+)'", stripped)
        if begin:
            stack.append(begin.group(1))
            paths.add(tuple(stack))
            continue
        end = re.fullmatch(r"\$end\s+'([^']+)'", stripped)
        if end:
            if not stack or stack[-1] != end.group(1):
                raise RuntimeError("PyAEDT DSO template has unbalanced ACF blocks")
            stack.pop()
            continue
        if stripped.startswith(("$begin", "$end")):
            raise RuntimeError("PyAEDT DSO template has malformed ACF blocks")
    if stack:
        raise RuntimeError("PyAEDT DSO template has unbalanced ACF blocks")

    dso_root = ("Configs", "Configs", "DSOConfig")
    required_paths = {
        ("Configs",),
        ("Configs", "Configs"),
        dso_root,
        dso_root + ("DSOMachineList",),
        dso_root + ("DSOMachineList", "DSOMachineInfo"),
        dso_root + ("DSOJobDistributionInfo",),
        dso_root + ("DSOMachineOptionsInfo",),
    }
    missing_blocks = sorted("/".join(path) for path in required_paths - paths)
    if missing_blocks:
        raise RuntimeError(
            "PyAEDT DSO template is incomplete; missing ACF blocks: "
            + ", ".join(missing_blocks)
        )
    for key in (
        "ConfigName",
        "DesignType",
        "NumEngines",
        "NumCores",
        "NumGPUs",
        "UseAutoSettings",
    ):
        matches = re.findall(rf"(?m)^[ \t]*{re.escape(key)}[ \t]*=", template)
        if len(matches) != 1:
            raise RuntimeError(
                f"PyAEDT DSO template must contain exactly one {key} setting"
            )


def _load_pyaedt_dso_template() -> str:
    try:
        resource = importlib.resources.files(DSO_TEMPLATE_PACKAGE).joinpath(
            DSO_TEMPLATE_FILENAME
        )
        template = resource.read_text(encoding="utf-8")
    except (ImportError, ModuleNotFoundError, FileNotFoundError, OSError, AttributeError) as exc:
        raise RuntimeError(
            f"PyAEDT bundled DSO template is unavailable: {DSO_TEMPLATE_FILENAME}"
        ) from exc
    _validate_pyaedt_dso_template(template)
    return template


def _render_dso_configuration(
    template: str, *, design_type: str, use_auto_settings: bool
) -> str:
    _validate_pyaedt_dso_template(template)
    rendered = template.replace("\r\n", "\n")
    values = {
        "ConfigName": f"'{DSO_CONFIG_NAME}'",
        "DesignType": f"'{design_type}'",
        "NumEngines": "1",
        "NumCores": "4",
        "NumGPUs": "0",
        "UseAutoSettings": "true" if use_auto_settings else "false",
    }
    for key, value in values.items():
        pattern = re.compile(rf"(?m)^([ \t]*{re.escape(key)}[ \t]*=)[^\r\n]*$")
        rendered, count = pattern.subn(lambda match: match.group(1) + value, rendered)
        if count != 1:
            raise RuntimeError(f"failed to render PyAEDT DSO setting {key}")
    _validate_pyaedt_dso_template(rendered)
    return rendered.rstrip("\n") + "\n"


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
    """Own exactly one AEDT process and at most three leased projects.

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
        artifact_root: str = "",
        dso_profile: str = "",
        session_profile: str = "",
        control_plane_outage_seconds: float = DEFAULT_CONTROL_PLANE_OUTAGE_SECONDS,
    ) -> None:
        self.client = client
        self.allocation_id = int(allocation_id)
        self.node_name = node_name
        self.requested_session_id = max(0, int(session_id))
        self.heartbeat_seconds = max(5, int(heartbeat_seconds))
        requested_aedt_version = str(aedt_version or "").strip()
        self.artifact_root = str(artifact_root or "").strip()
        self.dso_profile = str(dso_profile or "").strip().lower()
        if self.dso_profile and self.dso_profile not in SUPPORTED_DSO_PROFILES:
            raise ValueError(f"unsupported AEDT DSO profile: {self.dso_profile}")
        if self.dso_profile == SUPPORTED_DSO_PROFILE and not session_profile:
            raise ValueError(
                "canonical AEDT DSO profile requires the exact host session profile"
            )
        self.session_profile = (
            canonical_expected_session_profile(session_profile)
            if session_profile
            else ""
        )
        if self.session_profile:
            normalized_requested_version = normalize_aedt_version(
                requested_aedt_version
            )
            if requested_aedt_version and normalized_requested_version != EXPECTED_AEDT_VERSION:
                raise ValueError(
                    f"AEDT version must be {EXPECTED_AEDT_VERSION} for the canonical profile"
                )
            # Never let a blank CLI value silently select the installed default.
            self.aedt_version = EXPECTED_AEDT_VERSION
        else:
            self.aedt_version = requested_aedt_version
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
        self.desktop_process_id = ""
        self.desktop_process_marker: str | None = None
        self.desktop_port = 0
        self.native_probe_failures = 0
        self.native_probe_first_failure_at = 0.0
        self._native_probe_thread: threading.Thread | None = None
        self._native_probe_errors: list[BaseException] = []
        self._native_probe_outcome = NATIVE_PROBE_NOT_RUN
        self.last_native_probe_outcome = NATIVE_PROBE_NOT_RUN
        self.stop_requested = False
        self.artifact_dir = ""
        self.automation_lock_path = ""
        self._automation_lock: SessionAutomationLock | None = None
        self.error_log_path = ""
        self.journal_path = ""
        self.native_snapshot_path = ""
        self.runtime_metadata: dict[str, Any] = {}

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
                raise ControlPlaneUnavailable(
                    f"control plane unavailable for {elapsed:.1f}s: {last_error}"
                ) from last_error
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
            try:
                from ansys.aedt.core import settings
            except ImportError:
                from ansys.aedt.core.generic.settings import settings
        except ImportError as exc:
            raise RuntimeError("PyAEDT is required on the session-host node") from exc
        # PyAEDT 0.22 caches the first Desktop wrapper process-wide unless this
        # flag is set. If ownership validation causes a new-port retry, that
        # cache otherwise returns the first port forever.
        try:
            settings.use_multi_desktop = True
        except Exception as exc:
            raise RuntimeError(
                "failed to enable PyAEDT multi-desktop launch isolation"
            ) from exc
        if getattr(settings, "use_multi_desktop", None) is not True:
            raise RuntimeError("PyAEDT refused multi-desktop launch isolation")
        if self.error_log_path:
            # PyAEDT releases have moved these settings over time; keep this
            # best-effort and retain the host journal even if a field is absent.
            try:
                from ansys.aedt.core.generic.settings import settings

                if hasattr(settings, "enable_file_logs"):
                    settings.enable_file_logs = True
                if hasattr(settings, "logger_file_path"):
                    settings.logger_file_path = self.error_log_path
            except Exception as exc:
                self._journal("pyaedt_log_configuration_failed", error=str(exc))
        kwargs: dict[str, Any] = {
            "new_desktop": bool(new_desktop),
            "non_graphical": True,
            "close_on_exit": False,
            "port": int(port),
        }
        if self.aedt_version:
            kwargs["version"] = self.aedt_version
        return Desktop(**kwargs)

    def _prepare_artifacts(self, session: dict[str, Any]) -> None:
        if not self.artifact_root:
            return
        session_key = str(session.get("session_key") or f"session-{self.session_id}")
        safe_key = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in session_key
        )
        directory = Path(self.artifact_root) / f"session-{self.session_id}-{safe_key}"
        # Persist the intended paths in /start-failed even when mkdir itself is
        # the startup failure (permissions, GPFS outage, quota, and so on).
        self.artifact_dir = str(directory)
        self.automation_lock_path = automation_lock_path(self.artifact_dir)
        self.error_log_path = str(directory / "pyaedt.log")
        self.journal_path = str(directory / "session-events.jsonl")
        directory.mkdir(parents=True, exist_ok=True)
        create_automation_lock_file(self.automation_lock_path)
        self._automation_lock = SessionAutomationLock(
            self.automation_lock_path,
            timeout_seconds=max(300.0, self.control_plane_outage_seconds),
        )
        self._journal(
            "claim_accepted",
            allocation_id=self.allocation_id,
            expected_node=self.node_name,
            actual_node=socket.gethostname(),
            slurm_job_id=os.environ.get("SLURM_JOB_ID", ""),
        )

    def _attest_runtime_profile(self) -> dict[str, Any]:
        """Fail closed on Desktop/Python environment drift and save evidence."""

        odesktop = getattr(self.desktop, "odesktop", None)
        get_version = getattr(odesktop, "GetVersion", None)
        if not callable(get_version):
            raise RuntimeError("AEDT Desktop has no GetVersion runtime attestation API")
        raw_aedt_version = str(get_version() or "").strip()
        actual_aedt_version = normalize_aedt_version(raw_aedt_version)
        python_environment = str(os.environ.get("CONDA_DEFAULT_ENV", "")).strip()
        try:
            import ansys.aedt.core as pyaedt_core

            pyaedt_version = str(getattr(pyaedt_core, "__version__", "") or "")
        except Exception as exc:
            if self.session_profile:
                raise RuntimeError(
                    "could not attest the PyAEDT runtime version"
                ) from exc
            pyaedt_version = f"unavailable: {exc}"
        metadata = {
            "hostname": socket.gethostname(),
            "slurm_job_id": os.environ.get("SLURM_JOB_ID", ""),
            "host_process_id": os.getpid(),
            "python_executable": sys.executable,
            "python_version": sys.version,
            "python_environment": python_environment,
            "pyaedt_version": pyaedt_version,
            "aedt_version_raw": raw_aedt_version,
            "aedt_version": actual_aedt_version,
            "session_profile": self.session_profile,
            "dso_profile": self.dso_profile,
            "artifact_dir": self.artifact_dir,
            "automation_lock_path": self.automation_lock_path,
            "error_log_path": self.error_log_path,
            "journal_path": self.journal_path,
        }
        self.runtime_metadata = metadata
        if self.artifact_dir:
            evidence_path = Path(self.artifact_dir) / "runtime-attestation.json"
            evidence_path.write_text(
                json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2),
                encoding="utf-8",
            )
        self._journal("runtime_profile_attested", **metadata)
        if self.session_profile:
            expected_environment = str(
                EXPECTED_SESSION_PROFILE["python_environment"]
            )
            if actual_aedt_version != EXPECTED_AEDT_VERSION:
                raise RuntimeError(
                    "AEDT runtime version drift: "
                    f"expected {EXPECTED_AEDT_VERSION}, got {raw_aedt_version!r}"
                )
            if python_environment != expected_environment:
                raise RuntimeError(
                    "Python environment drift: "
                    f"expected {expected_environment!r}, got {python_environment!r}"
                )
            if pyaedt_version != EXPECTED_PYAEDT_VERSION:
                raise RuntimeError(
                    "PyAEDT runtime version drift: "
                    f"expected {EXPECTED_PYAEDT_VERSION!r}, got {pyaedt_version!r}"
                )
        return metadata

    def _initialize_dso_configuration(self) -> None:
        """Install Desktop-global 4-core/one-engine profiles exactly once."""

        if not self.dso_profile:
            return
        odesktop = getattr(self.desktop, "odesktop", None)
        if odesktop is None:
            raise RuntimeError("AEDT Desktop object has no native registry API")
        base = (
            Path(self.artifact_dir)
            if self.artifact_dir
            else Path(tempfile.gettempdir()) / f"aedt-session-{self.session_id}"
        )
        base.mkdir(parents=True, exist_ok=True)
        template = _load_pyaedt_dso_template()
        for design_type, registry_suffix, filename, use_auto_settings in (
            ("Maxwell 3D", "Maxwell 3D", "pyaedt_config_maxwell3d.acf", True),
            ("Maxwell 2D", "Maxwell 2D", "pyaedt_config_maxwell2d.acf", True),
            ("Icepak", "Icepak", "pyaedt_config_icepak.acf", False),
        ):
            acf_path = base / filename
            acf_path.write_text(
                _render_dso_configuration(
                    template,
                    design_type=design_type,
                    use_auto_settings=use_auto_settings,
                ),
                encoding="utf-8",
            )
            loaded = odesktop.SetRegistryFromFile(str(acf_path))
            if loaded is False:
                raise RuntimeError(f"SetRegistryFromFile failed for {design_type}")
            registry_key = f"Desktop/ActiveDSOConfigurations/{registry_suffix}"
            selected = odesktop.SetRegistryString(registry_key, DSO_CONFIG_NAME)
            if selected is False:
                raise RuntimeError(f"SetRegistryString failed for {design_type}")
            actual = str(odesktop.GetRegistryString(registry_key) or "").strip()
            if actual != DSO_CONFIG_NAME:
                raise RuntimeError(
                    f"DSO readback mismatch for {design_type}: {actual!r}"
                )
        self._journal("dso_profile_initialized", profile=self.dso_profile)

    def _journal(self, event: str, **fields: Any) -> None:
        if not self.journal_path:
            return
        record = {
            "timestamp": time.time(),
            "event": event,
            "session_id": self.session_id,
            "host_id": self.host_id,
            **fields,
        }
        try:
            with Path(self.journal_path).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        except OSError:
            pass

    def _capture_native_diagnostics(self, reason: str) -> str:
        """Save one bounded native-message/log snapshot per suspect episode."""

        if not self.artifact_dir:
            return ""
        if self.native_snapshot_path:
            try:
                if Path(self.native_snapshot_path).is_file():
                    return self.native_snapshot_path
            except OSError:
                pass
        messages: list[Any] = []
        errors: list[str] = []
        odesktop = getattr(self.desktop, "odesktop", None)
        get_messages = getattr(odesktop, "GetMessages", None)

        def collect_messages() -> None:
            if not callable(get_messages):
                errors.append("GetMessages API unavailable")
                return
            try:
                try:
                    value = get_messages("", "", 0)
                except TypeError:
                    value = get_messages()
                if isinstance(value, (list, tuple)):
                    messages.extend(str(item) for item in value[-200:])
                elif value is not None:
                    messages.append(str(value))
            except BaseException as exc:
                errors.append(str(exc))

        collector = threading.Thread(
            target=collect_messages,
            name="aedt-native-diagnostics",
            daemon=True,
        )
        collector.start()
        collector.join(timeout=2.0)
        if collector.is_alive():
            errors.append("GetMessages timed out after 2 seconds")
        log_tail = ""
        if self.error_log_path:
            try:
                with Path(self.error_log_path).open("rb") as handle:
                    handle.seek(0, os.SEEK_END)
                    size = handle.tell()
                    handle.seek(max(0, size - 65536), os.SEEK_SET)
                    log_tail = handle.read(65536).decode("utf-8", errors="replace")
            except OSError as exc:
                errors.append(f"PyAEDT log tail unavailable: {exc}")
        snapshot = Path(self.artifact_dir) / (
            f"native-liveness-{int(time.time() * 1000)}.json"
        )
        try:
            snapshot.write_text(
                json.dumps(
                    {
                        "timestamp": time.time(),
                        "reason": reason,
                        "process_id": self.desktop_process_id,
                        "port": self.desktop_port,
                        "messages": messages,
                        "pyaedt_log_tail": log_tail,
                        "errors": errors,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            snapshots = sorted(
                Path(self.artifact_dir).glob("native-liveness-*.json"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
            for stale in snapshots[10:]:
                try:
                    stale.unlink()
                except OSError:
                    pass
        except OSError as exc:
            self._journal("native_diagnostics_capture_failed", error=str(exc))
            return ""
        self.native_snapshot_path = str(snapshot)
        self._journal(
            "native_diagnostics_captured", path=self.native_snapshot_path
        )
        return self.native_snapshot_path

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

    @classmethod
    def _validate_owned_desktop(
        cls,
        desktop: Any,
        *,
        expected_port: int,
        started_after: float,
    ) -> Any:
        """Attest that PyAEDT returned the sole Desktop launched for this host.

        An explicit port alone is not an ownership boundary: another same-user
        AEDT (for example the motor workload's standalone Desktop) may already
        exist.  Accept the proxy only when its reported PID is exactly the one
        newly-created current-user ``ansysedt -grpcsrv <port>`` process.
        """

        validated = cls._validate_desktop(desktop, expected_port=expected_port)
        reported_pid = str(int(cls._desktop_pid(validated)))
        owned_pid = cls._owned_desktop_pid_on_port(expected_port, started_after)
        if not owned_pid:
            if cls._reported_pid_owns_desktop_port(
                reported_pid, expected_port, started_after
            ):
                return validated
            raise RuntimeError(
                "could not prove ownership of newly launched AEDT "
                f"PID {reported_pid} on gRPC port {expected_port}"
            )
        if reported_pid != owned_pid:
            raise RuntimeError(
                f"PyAEDT reported PID {reported_pid}, but the sole newly launched "
                f"AEDT on gRPC port {expected_port} is PID {owned_pid}"
            )
        return validated

    @staticmethod
    def _cmdline_has_grpc_port(cmdline: Any, port: int) -> bool:
        tokens = [str(item or "").strip() for item in (cmdline or [])]
        for index, token in enumerate(tokens):
            normalized = token.lower()
            if normalized == "-grpcsrv" and index + 1 < len(tokens):
                try:
                    return int(tokens[index + 1]) == int(port)
                except (TypeError, ValueError):
                    return False
            for separator in ("=", ":"):
                prefix = f"-grpcsrv{separator}"
                if normalized.startswith(prefix):
                    try:
                        return int(normalized[len(prefix) :]) == int(port)
                    except ValueError:
                        return False
        return False

    @classmethod
    def _reported_pid_owns_desktop_port(
        cls, reported_pid: str, port: int, started_after: float
    ) -> bool:
        """Attest a wrapper-reported PID without trusting the wrapper alone."""

        try:
            import psutil

            pid = int(reported_pid)
            current_process = psutil.Process(os.getpid())
            current_user = str(current_process.username() or "").lower()
            process = psutil.Process(pid)
            if not str(process.name() or "").lower().startswith("ansysedt"):
                return False
            executable = os.path.basename(str(process.exe() or "")).lower()
            if not executable.startswith("ansysedt"):
                return False
            try:
                if int(process.uids().effective) != int(current_process.uids().effective):
                    return False
            except (AttributeError, OSError, TypeError, ValueError):
                if str(process.username() or "").lower() != current_user:
                    return False
            if float(process.create_time() or 0) < started_after - 5:
                return False
            if cls._cmdline_has_grpc_port(process.cmdline(), port):
                return True
            connections = process.net_connections(kind="inet")
            listen_value = str(getattr(psutil, "CONN_LISTEN", "LISTEN")).upper()
            for connection in connections:
                status = str(getattr(connection, "status", "") or "").upper()
                local = getattr(connection, "laddr", None)
                local_port = getattr(local, "port", 0)
                if not local_port and isinstance(local, (tuple, list)) and len(local) > 1:
                    local_port = local[1]
                if int(local_port or 0) == int(port) and status in {"LISTEN", listen_value}:
                    return True
            # On Linux, AEDT can leave -grpcsrv and the listen socket on a
            # launcher/child. The wrapper came from this exact endpoint; after
            # independently proving PID executable, UID and creation time,
            # endpoint liveness completes the ownership proof.
            return cls._desktop_port_is_listening(port)
        except Exception:
            return False

    def _desktop_launch_retry_delay(self, failed_attempt: int) -> float:
        base = min(
            DESKTOP_LAUNCH_RETRY_MAX_SECONDS,
            DESKTOP_LAUNCH_RETRY_SECONDS * (2 ** max(0, int(failed_attempt) - 1)),
        )
        digest = hashlib.sha256(
            f"{self.host_id}:{int(failed_attempt)}".encode("utf-8")
        ).digest()
        fraction = int.from_bytes(digest[:8], "big") / float((1 << 64) - 1)
        return base * (0.75 + 0.5 * fraction)

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
                if not AedtSessionHost._cmdline_has_grpc_port(cmdline, port):
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
        # Never trust a wrapper-reported PID as authority to signal a process.
        # First prove exact port, creation-time, executable and current-user
        # ownership from the OS process table; then require wrapper agreement
        # when the wrapper did report a PID.
        owned_pid = self._owned_desktop_pid_on_port(port, started_after)
        reported_pid = self._desktop_pid(desktop) if desktop is not None else ""
        try:
            reported_value = int(reported_pid)
        except (TypeError, ValueError):
            reported_value = 0
        if not owned_pid:
            if reported_value <= 0 or not self._reported_pid_owns_desktop_port(
                str(reported_value), port, started_after
            ):
                return
            owned_pid = str(reported_value)
        elif reported_value > 0 and str(reported_value) != owned_pid:
            return
        marker = self._process_marker(int(owned_pid))
        self._force_kill_owned_desktop(owned_pid, marker)

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
                return self._validate_owned_desktop(
                    desktop,
                    expected_port=port,
                    started_after=launch_started_at,
                )
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
                    recovered = self._validate_owned_desktop(
                        recovered_desktop,
                        expected_port=fallback_port,
                        started_after=launch_started_at,
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
                time.sleep(self._desktop_launch_retry_delay(attempt))
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

    def _automation_guard(self):
        return self._automation_lock or nullcontext()

    def _close_project(self, project_name: str) -> None:
        with self._automation_guard():
            self._close_project_unlocked(project_name)

    def _close_project_unlocked(self, project_name: str) -> None:
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

    @staticmethod
    def _released_project_component(value: Any, label: str) -> str:
        """Validate one lease-owned filesystem component."""

        try:
            component = os.fsdecode(os.fspath(value)).strip()
        except TypeError as exc:
            raise RuntimeError(
                f"released AEDT {label} is unavailable"
            ) from exc
        if (
            not component
            or component in {".", ".."}
            or "\x00" in component
            or "/" in component
            or "\\" in component
        ):
            raise RuntimeError(
                f"unsafe released AEDT {label}: {component!r}"
            )
        return component

    @staticmethod
    def _plain_directory(path: str, label: str) -> os.stat_result:
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            raise RuntimeError(
                f"released AEDT {label} is unavailable: {path}"
            ) from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise RuntimeError(
                f"released AEDT {label} is not a plain directory: {path}"
            )
        return metadata

    @classmethod
    def _released_project_paths(
        cls, lease: dict[str, Any]
    ) -> tuple[str, str] | None:
        """Resolve the exact direct-child workspace owned by one v2 lease."""

        if int(lease.get("protocol_version") or 1) < 2:
            return None
        lease_id = int(lease.get("id") or 0)
        if lease_id <= 0:
            raise RuntimeError("released AEDT lease identity is unavailable")
        project_name = cls._released_project_component(
            lease.get("project_name"), "project name"
        )
        namespace_text = str(lease.get("project_namespace") or "").strip()
        if namespace_text:
            # Namespace is a logical collision domain, not a required filename
            # prefix (pyaedt_motor deliberately binds ``ipmsm-*`` projects in
            # the ``pyaedt_motor`` namespace).
            cls._released_project_component(
                namespace_text, "project namespace"
            )

        raw_workspace = str(lease.get("workspace_path") or "").strip()
        if not raw_workspace or not os.path.isabs(raw_workspace):
            raise RuntimeError(
                "released AEDT lease workspace must be an absolute path"
            )
        workspace = os.path.normpath(raw_workspace)
        if workspace != raw_workspace.rstrip("/\\") or workspace == os.path.abspath(
            os.path.sep
        ):
            raise RuntimeError(
                f"unsafe released AEDT lease workspace: {raw_workspace!r}"
            )
        cls._plain_directory(workspace, "lease workspace")

        project_path = os.path.join(workspace, project_name)
        if (
            os.path.dirname(project_path) != workspace
            or os.path.basename(project_path) != project_name
        ):
            raise RuntimeError(
                f"released AEDT project path escaped its workspace: {project_path!r}"
            )
        workspace_real = os.path.realpath(workspace)
        project_real = os.path.realpath(project_path)
        try:
            contained = (
                os.path.commonpath((workspace_real, project_real))
                == workspace_real
            )
        except ValueError:
            contained = False
        if not contained or project_real == workspace_real:
            raise RuntimeError(
                "released AEDT project path escaped its lease workspace: "
                f"project={project_path!r}, workspace={workspace!r}"
            )
        return workspace, project_path

    @staticmethod
    def _prepare_plain_directory_for_cross_account_delete(
        path: str,
        metadata: os.stat_result,
        host_uid: int,
        lease_owner_uid: int,
    ) -> bool:
        """Add delete traversal only to host-owned directories.

        On Linux the descriptor is opened with ``O_NOFOLLOW`` and the inode is
        rechecked before ``fchmod``.  The path fallback exists only for local
        non-POSIX unit tests; production session hosts are Linux processes.
        """

        mode = stat.S_IMODE(metadata.st_mode)
        owner_uid = int(metadata.st_uid)
        if owner_uid != int(host_uid):
            if (
                owner_uid != int(lease_owner_uid)
                and mode & stat.S_IRWXO != stat.S_IRWXO
            ):
                raise RuntimeError(
                    "released AEDT directory has an unexpected owner and is not "
                    f"cross-account removable: path={path}, mode={mode:04o}"
                )
            return False
        desired_mode = mode | stat.S_IRWXG | stat.S_IRWXO
        if desired_mode == mode:
            return False

        if os.name == "posix":
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            directory = getattr(os, "O_DIRECTORY", 0)
            if not nofollow or not directory:
                raise RuntimeError(
                    "released AEDT cleanup requires O_NOFOLLOW/O_DIRECTORY"
                )
            descriptor = os.open(
                path,
                os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                opened = os.fstat(descriptor)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or int(opened.st_dev) != int(metadata.st_dev)
                    or int(opened.st_ino) != int(metadata.st_ino)
                ):
                    raise RuntimeError(
                        f"released AEDT directory changed during attestation: {path}"
                    )
                os.fchmod(descriptor, desired_mode)
            finally:
                os.close(descriptor)
        else:
            current = os.lstat(path)
            if (
                stat.S_ISLNK(current.st_mode)
                or not stat.S_ISDIR(current.st_mode)
                or int(current.st_dev) != int(metadata.st_dev)
                or int(current.st_ino) != int(metadata.st_ino)
            ):
                raise RuntimeError(
                    f"released AEDT directory changed during attestation: {path}"
                )
            os.chmod(path, desired_mode)
        return True

    @classmethod
    def _prepare_released_project_workspace(
        cls, lease: dict[str, Any]
    ) -> dict[str, Any]:
        """Make one closed lease workspace removable by its client account.

        AEDT creates result/cache directories as the long-lived session-host
        UID with mode 0755.  The client intentionally waits for the host close
        ACK before deleting its disposable project, so the host prepares only
        directories inside that exact direct-child project.  It never removes
        data, changes file modes, or follows a symlink.
        """

        resolved = cls._released_project_paths(lease)
        if resolved is None:
            return {"state": "legacy", "directories_changed": 0}
        workspace, project_path = resolved
        if not os.path.lexists(project_path):
            return {
                "state": "absent",
                "project_path": project_path,
                "directories_changed": 0,
            }
        project_metadata = cls._plain_directory(
            project_path, "project workspace"
        )
        workspace_real = os.path.realpath(workspace)
        try:
            host_uid = int(os.geteuid())
        except AttributeError:  # Windows-only unit-test fallback.
            host_uid = int(os.lstat(project_path).st_uid)
        lease_owner_uid = int(project_metadata.st_uid)

        def fail_walk(error: OSError) -> None:
            raise RuntimeError(
                "released AEDT workspace traversal failed: "
                f"{getattr(error, 'filename', project_path)}"
            ) from error

        directories: list[tuple[str, os.stat_result]] = []
        for root, names, files in os.walk(
            project_path,
            topdown=True,
            onerror=fail_walk,
            followlinks=False,
        ):
            root_metadata = cls._plain_directory(root, "project directory")
            if (
                os.path.commonpath((workspace_real, os.path.realpath(root)))
                != workspace_real
            ):
                raise RuntimeError(
                    f"released AEDT directory escaped its workspace: {root}"
                )
            directories.append((root, root_metadata))
            for name in [*names, *files]:
                child = os.path.join(root, name)
                child_metadata = os.lstat(child)
                if stat.S_ISLNK(child_metadata.st_mode):
                    raise RuntimeError(
                        f"released AEDT workspace contains a symlink: {child}"
                    )
                if stat.S_ISDIR(child_metadata.st_mode):
                    if (
                        os.path.commonpath(
                            (workspace_real, os.path.realpath(child))
                        )
                        != workspace_real
                    ):
                        raise RuntimeError(
                            "released AEDT directory escaped its workspace: "
                            f"{child}"
                        )

        # Fail before any chmod if a third-party directory would remain
        # unreachable.  The host must never partially prepare an ambiguous
        # mixed-ownership tree.
        for path, metadata in directories:
            mode = stat.S_IMODE(metadata.st_mode)
            if (
                int(metadata.st_uid) not in {host_uid, lease_owner_uid}
                and mode & stat.S_IRWXO != stat.S_IRWXO
            ):
                raise RuntimeError(
                    "released AEDT directory has an unexpected owner and is not "
                    f"cross-account removable: path={path}, mode={mode:04o}"
                )

        changed = 0
        # Parents first: a host-owned 0700 parent must become traversable before
        # the lease client can reach any already-attested child directory.
        for path, metadata in directories:
            if cls._prepare_plain_directory_for_cross_account_delete(
                path, metadata, host_uid, lease_owner_uid
            ):
                changed += 1
        return {
            "state": "prepared",
            "project_path": project_path,
            "directories_seen": len(directories),
            "directories_changed": changed,
        }

    def _close_and_prepare_project_release(
        self, lease: dict[str, Any]
    ) -> dict[str, Any]:
        """Close one exact project before preparing its owned workspace."""

        self._close_project(str(lease["project_name"]))
        result = self._prepare_released_project_workspace(lease)
        self._journal(
            "released_project_workspace_prepared",
            lease_id=int(lease["id"]),
            **result,
        )
        return result

    def _global_stop(self) -> None:
        """Global by AEDT design; caller must honor the sibling grace gate."""
        with self._automation_guard():
            odesktop = getattr(self.desktop, "odesktop", None)
            if odesktop is None:
                raise RuntimeError("AEDT Desktop object has no StopSimulations API")
            odesktop.StopSimulations(False)

    def _close_desktop(self) -> None:
        with self._automation_guard():
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

    def _native_desktop_responds(self, timeout_seconds: float = 5.0) -> str:
        """Return an explicit native-probe outcome without racing a client.

        A probe is allowed to call ``GetVersion`` only after taking the same
        cross-process automation lock used by attached clients.  Lock
        contention is positive evidence that a client owns AEDT automation,
        not evidence that AEDT is unhealthy, so it is reported separately
        from a native call failure.
        """

        def consume_finished_probe() -> str:
            outcome = self._native_probe_outcome
            if outcome not in {
                NATIVE_PROBE_OK,
                NATIVE_PROBE_FAILED,
                NATIVE_PROBE_DEFERRED_BUSY,
            }:
                outcome = (
                    NATIVE_PROBE_FAILED
                    if self._native_probe_errors
                    else NATIVE_PROBE_OK
                )
            self._native_probe_thread = None
            self._native_probe_errors = []
            self._native_probe_outcome = NATIVE_PROBE_NOT_RUN
            return outcome

        existing = self._native_probe_thread
        if existing is not None:
            if existing.is_alive():
                # A live worker has already acquired the automation lock and
                # is blocked in GetVersion.  Never launch a second native call.
                return NATIVE_PROBE_FAILED
            return consume_finished_probe()
        odesktop = getattr(self.desktop, "odesktop", None)
        probe = getattr(odesktop, "GetVersion", None)
        if not callable(probe):
            return NATIVE_PROBE_FAILED
        self._native_probe_errors = []
        self._native_probe_outcome = NATIVE_PROBE_NOT_RUN

        def call() -> None:
            probe_lock: SessionAutomationLock | None = None
            probe_lock_acquired = False
            try:
                if self.automation_lock_path:
                    # A health probe must never wait behind a client and then
                    # unexpectedly enter AEDT after its caller timed out.  One
                    # non-blocking filesystem-lock attempt makes a busy client
                    # an explicit deferred outcome and guarantees GetVersion
                    # is not invoked in that case.
                    probe_lock = SessionAutomationLock(
                        self.automation_lock_path,
                        timeout_seconds=0.0,
                    )
                    try:
                        probe_lock.acquire()
                        probe_lock_acquired = True
                    except TimeoutError:
                        self._native_probe_outcome = NATIVE_PROBE_DEFERRED_BUSY
                        return
                probe()
                self._native_probe_outcome = NATIVE_PROBE_OK
            except BaseException as exc:
                self._native_probe_errors.append(exc)
                self._native_probe_outcome = NATIVE_PROBE_FAILED
            finally:
                if probe_lock is not None and probe_lock_acquired:
                    try:
                        probe_lock.release()
                    except BaseException as exc:
                        self._native_probe_errors.append(exc)
                        self._native_probe_outcome = NATIVE_PROBE_FAILED

        thread = threading.Thread(target=call, name="aedt-native-liveness", daemon=True)
        self._native_probe_thread = thread
        thread.start()
        thread.join(timeout=max(0.1, float(timeout_seconds)))
        if thread.is_alive():
            return NATIVE_PROBE_FAILED
        return consume_finished_probe()

    def _desktop_liveness_proof(self) -> tuple[bool, str]:
        """Require process identity, exact gRPC listener, and native response."""

        self.last_native_probe_outcome = NATIVE_PROBE_NOT_RUN
        if self.desktop is None:
            return False, "Desktop proxy is absent"
        pid_text = self.desktop_process_id or self._desktop_pid(self.desktop)
        if not pid_text:
            return False, "Desktop PID is unavailable"
        if self._process_alive(pid_text, self.desktop_process_marker) is not True:
            return False, f"Desktop PID {pid_text} is not alive or identity changed"
        port = self.desktop_port
        if port <= 0:
            try:
                port = self._desktop_port(self.desktop)
            except Exception:
                port = 0
        if port <= 0 or not self._desktop_port_is_listening(port):
            return False, f"Desktop gRPC port {port or '<unknown>'} is not listening"
        native_probe_outcome = self._native_desktop_responds()
        # Preserve compatibility with narrow test doubles while the production
        # probe always returns one of the explicit string outcomes above.
        if native_probe_outcome is True:
            native_probe_outcome = NATIVE_PROBE_OK
        elif native_probe_outcome is False:
            native_probe_outcome = NATIVE_PROBE_FAILED
        self.last_native_probe_outcome = str(native_probe_outcome)
        if native_probe_outcome == NATIVE_PROBE_DEFERRED_BUSY:
            # PID identity and the exact listening port still prove process
            # liveness.  Do not mutate the failure episode: this was not a
            # failed AEDT call and a later successful probe must clear any
            # earlier real failures.
            return True, ""
        if native_probe_outcome != NATIVE_PROBE_OK:
            now = time.monotonic()
            if not self.native_probe_failures:
                self.native_probe_first_failure_at = now
            self.native_probe_failures += 1
            elapsed = max(0.0, now - self.native_probe_first_failure_at)
            # A solve can serialize AEDT's scripting/native interface for many
            # minutes.  PID identity plus the exact listening gRPC socket are
            # authoritative process-liveness proofs; GetVersion timeout alone
            # only freezes admission and can never authorize a kill/recycle.
            return (
                True,
                "Desktop native GetVersion probe transiently failed "
                f"({self.native_probe_failures} consecutive, {elapsed:.1f}s)",
            )
        self.native_probe_failures = 0
        self.native_probe_first_failure_at = 0.0
        # A recovered episode must never cause a later real fault to reuse a
        # stale diagnostic snapshot.  The persisted path remains in control
        # plane history while the next episode creates a timestamped file.
        self.native_snapshot_path = ""
        return True, ""

    def _desktop_still_alive(self) -> bool:
        healthy, _reason = self._desktop_liveness_proof()
        return healthy

    def run(self) -> int:
        claim_payload = {
            "allocation_id": self.allocation_id,
            "node_name": self.node_name,
            "host_id": self.host_id,
            "session_id": self.requested_session_id,
            "actual_node_name": socket.gethostname(),
            "slurm_job_id": os.environ.get("SLURM_JOB_ID", ""),
            "host_process_id": str(os.getpid()),
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
            self._prepare_artifacts(claimed)
            self.desktop = self._start_desktop()
            self.desktop_process_id = self._desktop_pid(self.desktop)
            try:
                pid_value = int(self.desktop_process_id)
            except (TypeError, ValueError):
                pid_value = 0
            self.desktop_process_marker = (
                self._process_marker(pid_value) if pid_value > 0 else None
            )
            self.desktop_port = self._desktop_port(self.desktop)
            self._initialize_dso_configuration()
            self._attest_runtime_profile()
            self._journal(
                "desktop_started",
                process_id=self._desktop_pid(self.desktop),
                port=self._desktop_port(self.desktop),
            )
            endpoint = f"{socket.getfqdn()}:{self._desktop_port(self.desktop)}"
            registration_token = secrets.token_urlsafe(32)
            while True:
                try:
                    registered = self._control_plane_request(
                        "POST",
                        f"/api/aedt-pool/sessions/{self.session_id}/register",
                        {
                            "host_id": self.host_id,
                            "endpoint": endpoint,
                            "process_id": self._desktop_pid(self.desktop),
                            "artifact_dir": self.artifact_dir,
                            "error_log_path": self.error_log_path,
                            "journal_path": self.journal_path,
                            "session_profile": self.session_profile,
                            "runtime_metadata": self.runtime_metadata,
                        },
                        host_token=registration_token,
                        retry_registration_conflict=True,
                    )
                    break
                except ControlPlaneUnavailable as exc:
                    if not self._desktop_still_alive():
                        raise RuntimeError(
                            "AEDT died while registration control plane was unavailable"
                        ) from exc
                    self._journal("registration_deferred", error=str(exc))
                    time.sleep(min(5, self.heartbeat_seconds))
            self.host_token = str(registered["host_token"])
            self._journal("session_registered", endpoint=endpoint)
            while not self.stop_requested:
                try:
                    healthy, liveness_error = self._desktop_liveness_proof()
                    if not healthy:
                        snapshot_path = self._capture_native_diagnostics(
                            liveness_error
                        )
                        self._journal(
                            "desktop_liveness_failed", error=liveness_error
                        )
                        self._control_plane_request(
                            "POST",
                            f"/api/aedt-pool/sessions/{self.session_id}/fault",
                            {
                                "kind": "confirmed_aedt_death",
                                "failure_message": liveness_error,
                                "evidence": {
                                    "process_id": self.desktop_process_id,
                                    "port": self.desktop_port,
                                    "native_probe_failures": self.native_probe_failures,
                                    "artifact_dir": self.artifact_dir,
                                    "error_log_path": self.error_log_path,
                                    "native_snapshot_path": snapshot_path,
                                },
                            },
                            host_token=self.host_token,
                        )
                        raise RuntimeError(
                            f"AEDT Desktop liveness proof failed: {liveness_error}"
                        )
                    if liveness_error:
                        # One serialized native call can miss while two solvers
                        # are busy.  Freeze admission without claiming health;
                        # only an empty-error proof sends a recovery heartbeat.
                        self._journal(
                            "desktop_native_probe_suspect", error=liveness_error
                        )
                        snapshot_path = self._capture_native_diagnostics(
                            liveness_error
                        )
                        self._control_plane_request(
                            "POST",
                            f"/api/aedt-pool/sessions/{self.session_id}/fault",
                            {
                                "kind": "native_probe_suspect",
                                "failure_message": liveness_error,
                                "evidence": {
                                    "native_probe_failures": self.native_probe_failures,
                                    "process_id": self.desktop_process_id,
                                    "port": self.desktop_port,
                                    "artifact_dir": self.artifact_dir,
                                    "native_snapshot_path": snapshot_path,
                                },
                            },
                            host_token=self.host_token,
                        )
                        time.sleep(self.heartbeat_seconds)
                        continue
                    if (
                        self.last_native_probe_outcome
                        == NATIVE_PROBE_DEFERRED_BUSY
                    ):
                        self._journal(
                            "desktop_native_probe_deferred_busy",
                            automation_lock_path=self.automation_lock_path,
                        )
                    self._control_plane_request(
                        "POST",
                        f"/api/aedt-pool/sessions/{self.session_id}/heartbeat",
                        {
                            "liveness_confirmed": True,
                            "process_id": self.desktop_process_id,
                            "port": self.desktop_port,
                            "native_probe": (
                                "GetVersion"
                                if self.last_native_probe_outcome == NATIVE_PROBE_OK
                                else ""
                            ),
                            "native_probe_outcome": self.last_native_probe_outcome,
                        },
                        host_token=self.host_token,
                    )
                    commands = self._control_plane_request(
                        "GET",
                        f"/api/aedt-pool/sessions/{self.session_id}/commands",
                        host_token=self.host_token,
                    )
                except ControlPlaneUnavailable as exc:
                    if not self._desktop_still_alive():
                        raise RuntimeError(
                            "AEDT died while the control plane was unavailable"
                        ) from exc
                    # Offline-safe mode: freeze admission/commands locally and
                    # keep the existing Desktop and solves alive indefinitely.
                    self._journal("control_plane_offline_safe", error=str(exc))
                    time.sleep(min(5, self.heartbeat_seconds))
                    continue
                for lease in commands.get("close_projects") or []:
                    success = True
                    failure = ""
                    try:
                        self._close_and_prepare_project_release(lease)
                    except Exception as exc:
                        success = False
                        failure = str(exc)
                    try:
                        self._control_plane_request(
                            "POST",
                            f"/api/aedt-pool/sessions/{self.session_id}/leases/{int(lease['id'])}/release-complete",
                            {"success": success, "failure_message": failure},
                            host_token=self.host_token,
                        )
                    except ControlPlaneUnavailable as exc:
                        self._journal(
                            "release_ack_deferred",
                            lease_id=int(lease["id"]),
                            success=success,
                            error=str(exc),
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
            self._journal("session_host_failed", error=str(exc))
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
                            {
                                "host_id": self.host_id,
                                "failure_message": str(exc),
                                "artifact_dir": self.artifact_dir,
                                "error_log_path": self.error_log_path,
                                "journal_path": self.journal_path,
                                "runtime_metadata": {
                                    **self.runtime_metadata,
                                    "python_executable": sys.executable,
                                    "python_version": sys.version,
                                    "python_environment": os.environ.get(
                                        "CONDA_DEFAULT_ENV", ""
                                    ),
                                    "dso_profile": self.dso_profile,
                                    "session_profile": self.session_profile,
                                    "startup_failure": str(exc),
                                },
                            },
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
    parser.add_argument(
        "--artifact-root", default=os.environ.get(ARTIFACT_ROOT_ENV, "")
    )
    parser.add_argument(
        "--dso-profile", default=os.environ.get(DSO_PROFILE_ENV, "")
    )
    parser.add_argument(
        "--session-profile", default=os.environ.get(SESSION_PROFILE_ENV, "")
    )
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
        artifact_root=args.artifact_root,
        dso_profile=args.dso_profile,
        session_profile=args.session_profile,
        control_plane_outage_seconds=_control_plane_outage_seconds_from_env(),
    )
    signal.signal(signal.SIGTERM, host.request_stop)
    signal.signal(signal.SIGINT, host.request_stop)
    return host.run()


if __name__ == "__main__":
    raise SystemExit(main())
