from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .aedt_pool import AedtPoolService


TEMPLATES = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))

NODE_LOCAL_AEDT_HOST_PROJECT = "_aedt_pool_hosts"
NODE_LOCAL_AEDT_HOST_PREFIX = "mft-aedt-pooled-"
NODE_LOCAL_AEDT_HOST_NAME = re.compile(
    rf"^{re.escape(NODE_LOCAL_AEDT_HOST_PREFIX)}([0-9a-fA-F]{{20}})-host$"
)
NODE_LOCAL_AEDT_HOST_STATUSES = ["queued", "attaching", "running"]
NODE_LOCAL_AEDT_CLIENT_STATUSES = ["attaching", "running"]


def _public_lease(lease: dict[str, Any]) -> dict[str, Any]:
    item = dict(lease)
    item.pop("client_token_hash", None)
    return item


def build_node_local_aedt_summary(
    db: Any,
    *,
    allocation_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the dashboard/detail view of task-backed node-local AEDT sessions."""
    host_rows = db.list_tasks(
        limit=None,
        project=NODE_LOCAL_AEDT_HOST_PROJECT,
        name_prefix=NODE_LOCAL_AEDT_HOST_PREFIX,
        statuses=NODE_LOCAL_AEDT_HOST_STATUSES,
    )
    parsed_hosts: list[tuple[dict[str, Any], str]] = []
    for host in host_rows:
        match = NODE_LOCAL_AEDT_HOST_NAME.fullmatch(str(host.get("name") or ""))
        if match:
            parsed_hosts.append((host, match.group(1)))

    if not parsed_hosts:
        return {
            "active_host_count": 0,
            "running_host_count": 0,
            "attached_client_count": 0,
            "hosts": [],
        }

    active_host_ids = {int(host["id"]) for host, _bundle_id in parsed_hosts}
    running_host_ids = {
        int(host["id"])
        for host, _bundle_id in parsed_hosts
        if str(host.get("status") or "") == "running"
    }
    clients_by_host: dict[int, list[dict[str, Any]]] = {
        host_id: [] for host_id in active_host_ids
    }
    for client in db.list_tasks_by_same_node_task_ids(
        active_host_ids,
        statuses=NODE_LOCAL_AEDT_CLIENT_STATUSES,
        aedt_backend="pooled",
    ):
        host_id = int(client.get("same_node_as_task_id") or 0)
        clients_by_host[host_id].append(
            {
                "id": int(client["id"]),
                "name": str(client.get("name") or ""),
                "project": str(client.get("project") or ""),
                "status": str(client.get("status") or ""),
            }
        )

    allocations = {
        int(allocation["id"]): allocation for allocation in (allocation_rows or [])
    }
    allocation_ids = {
        int(host.get("allocation_id") or host.get("requested_allocation_id") or 0)
        for host, _bundle_id in parsed_hosts
    }
    allocation_ids.discard(0)
    missing_allocation_ids = allocation_ids - set(allocations)
    if missing_allocation_ids:
        allocations.update(
            {
                int(allocation["id"]): allocation
                for allocation in db.list_allocations_by_ids(missing_allocation_ids)
            }
        )
    hosts: list[dict[str, Any]] = []
    for host, bundle_id in parsed_hosts:
        host_id = int(host["id"])
        allocation_id = int(host.get("allocation_id") or host.get("requested_allocation_id") or 0)
        allocation = allocations.get(allocation_id, {})
        hosts.append(
            {
                "id": host_id,
                "bundle_id": bundle_id,
                "bundle_id_short": bundle_id[:8],
                "node_name": str(
                    host.get("allocation_node_name")
                    or allocation.get("node_name")
                    or host.get("node_name")
                    or ""
                ),
                "account_name": str(
                    host.get("account_name")
                    or host.get("requested_account_name")
                    or allocation.get("account_name")
                    or ""
                ),
                "status": str(host.get("status") or ""),
                "started_at": host.get("started_at"),
                "clients": clients_by_host[host_id],
            }
        )

    return {
        "active_host_count": len(hosts),
        "running_host_count": len(running_host_ids),
        "attached_client_count": sum(
            len(clients_by_host[host_id]) for host_id in running_host_ids
        ),
        "hosts": hosts,
    }


def build_aedt_pool_summary(
    service: AedtPoolService,
    *,
    allocation_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    summary = dict(service.summary())
    summary["node_local"] = build_node_local_aedt_summary(
        service.db,
        allocation_rows=allocation_rows,
    )
    return summary


def create_aedt_pool_router(service: AedtPoolService) -> APIRouter:
    router = APIRouter()

    def require_bootstrap(x_aedt_bootstrap_token: str = Header("")) -> None:
        try:
            service.authorize_bootstrap(x_aedt_bootstrap_token)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    bootstrap_guard = Depends(require_bootstrap)

    @router.get("/aedt-pool", response_class=HTMLResponse)
    def aedt_pool_page(request: Request) -> HTMLResponse:
        return TEMPLATES.TemplateResponse(
            "aedt_pool.html",
            {"request": request, "summary": build_aedt_pool_summary(service)},
        )

    @router.get("/api/aedt-pool")
    def get_aedt_pool() -> dict[str, Any]:
        return build_aedt_pool_summary(service)

    @router.patch("/api/aedt-pool/config", dependencies=[bootstrap_guard])
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

    @router.post("/api/aedt-pool/enable", dependencies=[bootstrap_guard])
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

    @router.post("/api/aedt-pool/validations", dependencies=[bootstrap_guard])
    async def record_aedt_pool_validation(request: Request) -> dict[str, Any]:
        payload = await request.json()
        try:
            return service.record_validation(payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @router.post("/api/aedt-pool/reconcile", dependencies=[bootstrap_guard])
    def reconcile_aedt_pool(dry_run: bool = True) -> dict[str, Any]:
        # Live reconciliation is still triple-gated inside the service.
        return service.dry_run() if dry_run else service.reconcile(execute=True)

    @router.post("/api/aedt-pool/leases", dependencies=[bootstrap_guard])
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

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/heartbeat",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/release",
        dependencies=[bootstrap_guard],
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
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/leases/{lease_id}/fault",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/hosts/claim-start",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/register",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/start-failed",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/heartbeat",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/leases/{lease_id}/release-complete",
        dependencies=[bootstrap_guard],
    )
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

    @router.post(
        "/api/aedt-pool/sessions/{session_id}/closed",
        dependencies=[bootstrap_guard],
    )
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
