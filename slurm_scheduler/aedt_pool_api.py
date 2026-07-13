from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .aedt_pool import AedtPoolService


TEMPLATES = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


def _public_lease(lease: dict[str, Any]) -> dict[str, Any]:
    item = dict(lease)
    item.pop("client_token_hash", None)
    return item


def create_aedt_pool_router(service: AedtPoolService) -> APIRouter:
    router = APIRouter()

    @router.get("/aedt-pool", response_class=HTMLResponse)
    def aedt_pool_page(request: Request) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(
            "aedt_pool.html",
            {"request": request, "summary": service.summary()},
        )

    @router.get("/api/aedt-pool")
    def get_aedt_pool() -> dict[str, Any]:
        return service.summary()

    @router.patch("/api/aedt-pool/config")
    async def set_aedt_pool_limit(request: Request) -> dict[str, Any]:
        payload = await request.json()
        allowed = {
            "max_aedt_sessions",
            "min_idle_aedt_sessions",
            "target_project_concurrency",
            "projects_per_aedt",
        }
        if not payload or set(payload) - allowed:
            raise HTTPException(
                status_code=422,
                detail=(
                    "only max_aedt_sessions, min_idle_aedt_sessions, "
                    "target_project_concurrency, and projects_per_aedt "
                    "are operator-configurable"
                ),
            )
        try:
            config = service.set_operator_limits(
                max_sessions=payload.get("max_aedt_sessions"),
                min_idle_sessions=payload.get("min_idle_aedt_sessions"),
                target_projects=payload.get("target_project_concurrency"),
                projects_per_session=payload.get("projects_per_aedt"),
            )
            service.reconcile(execute=True)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "max_aedt_sessions": config.max_sessions,
            "min_idle_aedt_sessions": config.min_idle_sessions,
            "target_project_concurrency": config.target_projects,
            "projects_per_aedt": config.projects_per_session,
            "enabled": config.enabled,
            "operational": config.operational,
        }

    @router.post("/api/aedt-pool/enable")
    async def set_aedt_pool_enabled(request: Request) -> dict[str, Any]:
        payload = await request.json()
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

    @router.post("/api/aedt-pool/validations")
    async def record_aedt_pool_validation(request: Request) -> dict[str, Any]:
        payload = await request.json()
        try:
            return service.record_validation(payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/api/aedt-pool/reconcile")
    def reconcile_aedt_pool(dry_run: bool = True) -> dict[str, Any]:
        # Live reconciliation is still triple-gated inside the service.
        return service.dry_run() if dry_run else service.reconcile(execute=True)

    @router.post("/api/aedt-pool/leases")
    async def request_aedt_lease(request: Request) -> dict[str, Any]:
        payload = await request.json()
        try:
            lease, token = service.request_lease(
                request_key=str(payload.get("request_key") or ""),
                project_name=str(payload.get("project_name") or ""),
                task_id=int(payload.get("task_id") or 0),
                allocation_id=int(payload.get("allocation_id") or 0),
                node_name=str(payload.get("node_name") or ""),
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

    @router.post("/api/aedt-pool/leases/{lease_id}/heartbeat")
    def heartbeat_aedt_lease(
        lease_id: int,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.heartbeat_lease(lease_id, x_aedt_lease_token)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post("/api/aedt-pool/leases/{lease_id}/release")
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

    @router.patch("/api/aedt-pool/leases/{lease_id}/project-name")
    async def bind_aedt_lease_project_name(
        lease_id: int,
        request: Request,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
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

    @router.post("/api/aedt-pool/leases/{lease_id}/fault")
    async def report_aedt_lease_fault(
        lease_id: int,
        request: Request,
        x_aedt_lease_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
        try:
            return service.report_project_fault(
                lease_id,
                x_aedt_lease_token,
                fault_kind=str(payload.get("fault_kind") or ""),
                sibling_grace_seconds=int(payload.get("sibling_grace_seconds") or 900),
                failure_message=str(payload.get("failure_message") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="lease not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post("/api/aedt-pool/hosts/claim-start")
    async def claim_aedt_start(
        request: Request,
        x_aedt_bootstrap_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
        try:
            session = service.claim_start(
                allocation_id=int(payload.get("allocation_id") or 0),
                node_name=str(payload.get("node_name") or ""),
                host_id=str(payload.get("host_id") or ""),
                bootstrap_token=x_aedt_bootstrap_token,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        return {"session": session}

    @router.post("/api/aedt-pool/sessions/{session_id}/register")
    async def register_aedt_session(
        session_id: int,
        request: Request,
        x_aedt_bootstrap_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
        try:
            session, token = service.register_session(
                session_id=session_id,
                host_id=str(payload.get("host_id") or ""),
                endpoint=str(payload.get("endpoint") or ""),
                process_id=str(payload.get("process_id") or ""),
                bootstrap_token=x_aedt_bootstrap_token,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"session": session, "host_token": token}

    @router.post("/api/aedt-pool/sessions/{session_id}/start-failed")
    async def fail_aedt_session_start(
        session_id: int,
        request: Request,
        x_aedt_bootstrap_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
        try:
            return service.fail_session_start(
                session_id=session_id,
                host_id=str(payload.get("host_id") or ""),
                bootstrap_token=x_aedt_bootstrap_token,
                failure_message=str(payload.get("failure_message") or ""),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="session not found") from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @router.post("/api/aedt-pool/sessions/{session_id}/heartbeat")
    def heartbeat_aedt_session(
        session_id: int,
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        try:
            return service.heartbeat_session(session_id, x_aedt_host_token)
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

    @router.post("/api/aedt-pool/sessions/{session_id}/leases/{lease_id}/release-complete")
    async def complete_aedt_release(
        session_id: int,
        lease_id: int,
        request: Request,
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
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

    @router.post("/api/aedt-pool/sessions/{session_id}/closed")
    async def close_aedt_session(
        session_id: int,
        request: Request,
        x_aedt_host_token: str = Header(""),
    ) -> dict[str, Any]:
        payload = await request.json()
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
