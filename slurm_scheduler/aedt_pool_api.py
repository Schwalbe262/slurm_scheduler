from __future__ import annotations

from math import ceil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .aedt_pool import AedtPoolService


TEMPLATES = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


def _public_lease(lease: dict[str, Any]) -> dict[str, Any]:
    item = dict(lease)
    item.pop("client_token_hash", None)
    item["legacy_state"] = (
        "leased" if item.get("state") in {"offered", "attaching"} else item.get("state")
    )
    return item


def create_aedt_pool_router(service: AedtPoolService) -> APIRouter:
    router = APIRouter()

    def require_bootstrap(x_aedt_bootstrap_token: str = Header("")) -> None:
        try:
            service.authorize_bootstrap(x_aedt_bootstrap_token)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    bootstrap_guard = Depends(require_bootstrap)

    def require_lease_client(x_aedt_client_token: str = Header("")) -> None:
        try:
            service.authorize_lease_client(x_aedt_client_token)
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    lease_client_guard = Depends(require_lease_client)

    @router.get("/aedt-pool", response_class=HTMLResponse)
    def aedt_pool_page(request: Request) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(
            "aedt_pool.html",
            {"request": request, "summary": service.summary()},
        )

    @router.get("/api/aedt-pool")
    def get_aedt_pool() -> dict[str, Any]:
        return service.summary()

    @router.patch("/api/aedt-pool/config", dependencies=[bootstrap_guard])
    def set_aedt_pool_limit(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        allowed = {
            "concurrent_simulations",
            "max_aedt_sessions",
            "min_idle_aedt_sessions",
            "target_project_concurrency",
            "projects_per_aedt",
            "lease_ttl_seconds",
            "session_heartbeat_timeout_seconds",
            "idle_ttl_seconds",
            "allocation_max_age_seconds",
        }
        if not payload or set(payload) - allowed:
            raise HTTPException(
                status_code=422,
                detail=(
                    "only concurrent_simulations, max_aedt_sessions, "
                    "min_idle_aedt_sessions, "
                    "target_project_concurrency, projects_per_aedt, "
                    "lease_ttl_seconds, session_heartbeat_timeout_seconds, "
                    "idle_ttl_seconds, and allocation_max_age_seconds "
                    "are operator-configurable"
                ),
            )
        derived_keys = {"max_aedt_sessions", "target_project_concurrency"}
        if "concurrent_simulations" in payload and derived_keys & set(payload):
            raise HTTPException(
                status_code=422,
                detail=(
                    "concurrent_simulations cannot be combined with "
                    "max_aedt_sessions or target_project_concurrency"
                ),
            )
        try:
            limit_keys = {
                "concurrent_simulations",
                "max_aedt_sessions",
                "min_idle_aedt_sessions",
                "target_project_concurrency",
                "projects_per_aedt",
            }
            if limit_keys & set(payload):
                max_sessions = payload.get("max_aedt_sessions")
                target_projects = payload.get("target_project_concurrency")
                if "concurrent_simulations" in payload:
                    concurrent_simulations = payload["concurrent_simulations"]
                    if (
                        type(concurrent_simulations) is not int
                        or not 0 <= concurrent_simulations <= 1650
                    ):
                        raise ValueError(
                            "concurrent_simulations must be an integer between "
                            "0 and 1650"
                        )
                    projects_per_session = payload.get(
                        "projects_per_aedt",
                        service.config().projects_per_session,
                    )
                    if (
                        type(projects_per_session) is not int
                        or not 1 <= projects_per_session <= 3
                    ):
                        raise ValueError(
                            "projects_per_aedt must be an integer between 1 and 3"
                        )
                    max_sessions = ceil(
                        concurrent_simulations / projects_per_session
                    )
                    if max_sessions > 550:
                        raise ValueError(
                            "concurrent_simulations would require "
                            f"{max_sessions} max_aedt_sessions; maximum is 550"
                        )
                    target_projects = concurrent_simulations
                config = service.set_operator_limits(
                    max_sessions=max_sessions,
                    min_idle_sessions=payload.get("min_idle_aedt_sessions"),
                    target_projects=target_projects,
                    projects_per_session=payload.get("projects_per_aedt"),
                )
            timeout_keys = {
                "lease_ttl_seconds",
                "session_heartbeat_timeout_seconds",
                "idle_ttl_seconds",
                "allocation_max_age_seconds",
            }
            if timeout_keys & set(payload):
                config = service.set_operator_timeouts(
                    lease_ttl_seconds=payload.get("lease_ttl_seconds"),
                    session_heartbeat_timeout_seconds=payload.get(
                        "session_heartbeat_timeout_seconds"
                    ),
                    idle_ttl_seconds=payload.get("idle_ttl_seconds"),
                    allocation_max_age_seconds=payload.get(
                        "allocation_max_age_seconds"
                    ),
                )
            service.reconcile(execute=True)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "concurrent_simulations": config.target_projects,
            "max_aedt_sessions": config.max_sessions,
            "min_idle_aedt_sessions": config.min_idle_sessions,
            "target_project_concurrency": config.target_projects,
            "projects_per_aedt": config.projects_per_session,
            "project_cpus": config.project_cpus,
            "project_memory_mb": config.project_memory_mb,
            "session_reserved_cpus": (
                config.project_cpus * config.projects_per_session
            ),
            "session_reserved_memory_mb": (
                config.project_memory_mb * config.projects_per_session
            ),
            "lease_ttl_seconds": config.lease_ttl_seconds,
            "session_heartbeat_timeout_seconds": (
                config.session_heartbeat_timeout_seconds
            ),
            "idle_ttl_seconds": config.idle_ttl_seconds,
            "allocation_max_age_seconds": config.allocation_max_age_seconds,
            "enabled": config.enabled,
            "operational": config.operational,
        }

    @router.post("/api/aedt-pool/enable", dependencies=[bootstrap_guard])
    def set_aedt_pool_enabled(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        if type(payload.get("enabled")) is not bool:
            raise HTTPException(status_code=422, detail="enabled must be a boolean")
        try:
            config = service.set_enabled(bool(payload["enabled"]))
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "enabled": config.enabled,
            "operational": config.operational,
            "validation_passed": config.validation_passed,
            "adapter_ready": config.adapter_ready,
        }

    @router.post("/api/aedt-pool/validations", dependencies=[bootstrap_guard])
    def record_aedt_pool_validation(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        try:
            return service.record_validation(payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/mixed-canary-admissions",
        dependencies=[bootstrap_guard],
    )
    def create_mixed_canary_admission(
        payload: dict[str, Any] = Body(...),
    ) -> dict[str, Any]:
        allowed = {"session_id", "mft_projects", "ipmsm_projects", "ttl_seconds"}
        if not payload or set(payload) - allowed:
            raise HTTPException(
                status_code=422,
                detail=(
                    "only session_id, mft_projects, ipmsm_projects, and "
                    "ttl_seconds are accepted"
                ),
            )
        try:
            return service.create_mixed_canary_admission(
                session_id=payload.get("session_id"),
                mft_projects=payload.get("mft_projects", 2),
                ipmsm_projects=payload.get("ipmsm_projects", 1),
                ttl_seconds=payload.get("ttl_seconds", 1800),
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/api/aedt-pool/reconcile", dependencies=[bootstrap_guard])
    def reconcile_aedt_pool(dry_run: bool = True) -> dict[str, Any]:
        # Live reconciliation is still triple-gated inside the service.
        return service.dry_run() if dry_run else service.reconcile(execute=True)

    @router.post("/api/aedt-pool/leases", dependencies=[lease_client_guard])
    def request_aedt_lease(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        try:
            task_id = int(payload.get("task_id") or 0)
            allocation_id = int(payload.get("allocation_id") or 0)
            node_name = str(payload.get("node_name") or "")
            if allocation_id and not task_id:
                raise ValueError(
                    "allocation affinity requires task_id; affinity is "
                    "derived server-side from that task"
                )
            if task_id:
                task = service.db.get_task(task_id)
                if not task:
                    raise ValueError("task_id does not exist")
                if allocation_id:
                    task_allocation_id = int(task.get("allocation_id") or 0)
                    if allocation_id != task_allocation_id:
                        raise ValueError(
                            "requested allocation does not belong to task_id"
                        )
                    task_allocation = service.db.get_allocation(task_allocation_id)
                    task_node_name = str(
                        (task_allocation or {}).get("node_name") or ""
                    )
                    if (
                        node_name.strip()
                        and node_name.split(".", 1)[0].lower()
                        != task_node_name.split(".", 1)[0].lower()
                    ):
                        raise ValueError("requested node does not belong to task_id")
                    # Never trust the caller's node string for persisted affinity.
                    node_name = task_node_name
                else:
                    # Legacy clients report their worker node here.  It is not an
                    # AEDT-session placement request; task_id remains provenance.
                    node_name = ""
            elif not allocation_id:
                node_name = ""
            lease, token = service.request_lease(
                request_key=str(payload.get("request_key") or ""),
                project_name=str(payload.get("project_name") or ""),
                placement_group=payload.get("placement_group"),
                workload_family=str(payload.get("workload_family") or ""),
                session_profile=payload.get("session_profile") or "",
                project_namespace=str(payload.get("project_namespace") or ""),
                isolation_policy=str(payload.get("isolation_policy") or "family"),
                workspace_path=str(payload.get("workspace_path") or ""),
                protocol_version=int(payload.get("protocol_version") or 1),
                client_deadline_at=str(payload.get("client_deadline_at") or ""),
                admission_timeout_seconds=payload.get(
                    "admission_timeout_seconds"
                ),
                client_token=str(payload.get("client_token") or ""),
                task_id=task_id,
                allocation_id=allocation_id,
                node_name=node_name,
                exclusive_session=payload.get("exclusive_session", False),
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"lease": _public_lease(lease), "client_token": token}

    @router.get("/api/aedt-pool/leases/{lease_id}")
    def get_aedt_lease(
        lease_id: int,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.lease_status(lease_id, x_aedt_lease_token)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/heartbeat",
    )
    def heartbeat_aedt_lease(
        lease_id: int,
        payload: dict[str, Any] | None = Body(None),
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.heartbeat_lease(
                lease_id,
                x_aedt_lease_token,
                phase=str((payload or {}).get("phase") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/accept",
    )
    def accept_aedt_lease(
        lease_id: int,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.accept_lease(lease_id, x_aedt_lease_token)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/activate",
    )
    def activate_aedt_lease(
        lease_id: int,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.activate_lease(lease_id, x_aedt_lease_token)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/solve-permit",
    )
    def request_aedt_solve_permit(
        lease_id: int,
        payload: dict[str, Any] | None = Body(None),
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.request_solve_permit(
                lease_id,
                x_aedt_lease_token,
                seal_underfilled=(payload or {}).get(
                    "seal_underfilled", False
                ),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/cancel",
    )
    def cancel_aedt_lease(
        lease_id: int,
        payload: dict[str, Any] | None = Body(None),
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.cancel_lease(
                lease_id,
                x_aedt_lease_token,
                reason=str((payload or {}).get("reason") or "client cancelled lease"),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/release",
    )
    def release_aedt_lease(
        lease_id: int,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.release_lease(lease_id, x_aedt_lease_token)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @router.patch(
        "/api/aedt-pool/leases/{lease_id}/project-name",
    )
    def bind_aedt_lease_project_name(
        lease_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.bind_lease_project_name(
                lease_id,
                x_aedt_lease_token,
                str(payload.get("project_name") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/fault",
    )
    def report_aedt_lease_fault(
        lease_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.report_project_fault(
                lease_id,
                x_aedt_lease_token,
                fault_kind=str(payload.get("fault_kind") or ""),
                phase=str(payload.get("phase") or ""),
                evidence=(
                    payload.get("evidence")
                    if isinstance(payload.get("evidence"), dict)
                    else {}
                ),
                sibling_grace_seconds=int(payload.get("sibling_grace_seconds") or 900),
                failure_message=str(payload.get("failure_message") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/hosts/claim-start",
        dependencies=[bootstrap_guard],
    )
    def claim_aedt_start(
        payload: dict[str, Any] = Body(...),
        x_aedt_bootstrap_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            session = service.claim_start(
                session_id=int(payload.get("session_id") or 0),
                allocation_id=int(payload.get("allocation_id") or 0),
                node_name=str(payload.get("node_name") or ""),
                host_id=str(payload.get("host_id") or ""),
                actual_node_name=str(payload.get("actual_node_name") or ""),
                slurm_job_id=str(payload.get("slurm_job_id") or ""),
                host_process_id=str(payload.get("host_process_id") or ""),
                bootstrap_token=x_aedt_bootstrap_token,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"session": session}

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/register",
        dependencies=[bootstrap_guard],
    )
    def register_aedt_session(
        session_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_bootstrap_token: str = Header(""),
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            session, token = service.register_session(
                session_id=session_id,
                host_id=str(payload.get("host_id") or ""),
                endpoint=str(payload.get("endpoint") or ""),
                process_id=str(payload.get("process_id") or ""),
                artifact_dir=str(payload.get("artifact_dir") or ""),
                error_log_path=str(payload.get("error_log_path") or ""),
                journal_path=str(payload.get("journal_path") or ""),
                runtime_metadata=(
                    payload.get("runtime_metadata")
                    if isinstance(payload.get("runtime_metadata"), dict)
                    else {}
                ),
                session_profile=payload.get("session_profile") or "",
                bootstrap_token=x_aedt_bootstrap_token,
                host_token=x_aedt_host_token,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"session": session, "host_token": token}

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/fault",
        dependencies=[bootstrap_guard],
    )
    def report_aedt_session_fault(
        session_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.report_session_fault(
                session_id,
                x_aedt_host_token,
                kind=str(payload.get("kind") or ""),
                failure_message=str(payload.get("failure_message") or ""),
                evidence=(
                    payload.get("evidence")
                    if isinstance(payload.get("evidence"), dict)
                    else {}
                ),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/start-failed",
        dependencies=[bootstrap_guard],
    )
    def fail_aedt_session_start(
        session_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_bootstrap_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.fail_session_start(
                session_id=session_id,
                host_id=str(payload.get("host_id") or ""),
                bootstrap_token=x_aedt_bootstrap_token,
                failure_message=str(payload.get("failure_message") or ""),
                artifact_dir=str(payload.get("artifact_dir") or ""),
                error_log_path=str(payload.get("error_log_path") or ""),
                journal_path=str(payload.get("journal_path") or ""),
                runtime_metadata=(
                    payload.get("runtime_metadata")
                    if isinstance(payload.get("runtime_metadata"), dict)
                    else {}
                ),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/heartbeat",
        dependencies=[bootstrap_guard],
    )
    def heartbeat_aedt_session(
        session_id: int,
        payload: dict[str, Any] | None = Body(None),
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            heartbeat = payload if isinstance(payload, dict) else {}
            return service.heartbeat_session(
                session_id,
                x_aedt_host_token,
                liveness_confirmed=heartbeat.get("liveness_confirmed") is True,
                process_id=str(heartbeat.get("process_id") or ""),
                port=int(heartbeat.get("port") or 0),
                native_probe=str(heartbeat.get("native_probe") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.get("/api/aedt-pool/sessions/{session_id}/commands")
    def get_aedt_session_commands(
        session_id: int,
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.session_commands(session_id, x_aedt_host_token)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/leases/{lease_id}/release-complete",
        dependencies=[bootstrap_guard],
    )
    def complete_aedt_release(
        session_id: int,
        lease_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        if type(payload.get("success")) is not bool:
            raise HTTPException(status_code=422, detail="success must be a boolean")
        try:
            return service.complete_release(
                session_id,
                x_aedt_host_token,
                lease_id,
                success=payload["success"],
                failure_message=str(payload.get("failure_message") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session or lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/closed",
        dependencies=[bootstrap_guard],
    )
    def close_aedt_session(
        session_id: int,
        payload: dict[str, Any] = Body(...),
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        if type(payload.get("success")) is not bool:
            raise HTTPException(status_code=422, detail="success must be a boolean")
        if "requeue_siblings" in payload and type(payload["requeue_siblings"]) is not bool:
            raise HTTPException(status_code=422, detail="requeue_siblings must be a boolean")
        try:
            return service.close_session(
                session_id,
                x_aedt_host_token,
                success=payload["success"],
                failure_message=str(payload.get("failure_message") or ""),
                requeue_siblings=payload.get("requeue_siblings", True),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    return router
