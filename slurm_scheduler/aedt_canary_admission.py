from __future__ import annotations

import json
from typing import Any


NODE_LOCAL_AEDT_CANARY_HOST_ENTRYPOINT = "aedt_node_canary_host"
NODE_LOCAL_AEDT_CANARY_CLIENT_ENTRYPOINT = "aedt_node_canary_client"
NODE_LOCAL_AEDT_CANARY_VALIDATED_MAX_PROJECTS = 2


def _json_mapping(value: object) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def node_local_aedt_canary_admission(db: Any, task: dict) -> tuple[bool, str]:
    """Explicit bounded exception while the central pool remains disabled."""
    if str(task.get("entrypoint") or "") != NODE_LOCAL_AEDT_CANARY_CLIENT_ENTRYPOINT:
        return False, "AEDT pooled backend is not operational"
    host_task_id = max(0, int(task.get("same_node_as_task_id") or 0))
    host = db.get_task(host_task_id) if host_task_id else None
    if not host or str(host.get("entrypoint") or "") != NODE_LOCAL_AEDT_CANARY_HOST_ENTRYPOINT:
        return False, "node-local AEDT canary host is missing"
    if str(host.get("status") or "") not in {"attaching", "running"}:
        return False, "node-local AEDT canary host is not active"
    client_payload = _json_mapping(task.get("payload_json"))
    host_payload = _json_mapping(host.get("payload_json"))
    bundle_id = str(client_payload.get("aedt_canary_bundle_id") or "")
    if not bundle_id or bundle_id != str(host_payload.get("aedt_canary_bundle_id") or ""):
        return False, "node-local AEDT canary bundle identity mismatch"
    expected = int(host_payload.get("aedt_canary_expected_projects") or 0)
    if not 1 <= expected <= NODE_LOCAL_AEDT_CANARY_VALIDATED_MAX_PROJECTS:
        return False, "node-local AEDT canary project count exceeds validated bound"
    if int(client_payload.get("aedt_canary_expected_projects") or 0) != expected:
        return False, "node-local AEDT canary project count mismatch"
    claimed = 0
    for other in db.list_tasks(limit=5000):
        if int(other.get("id") or 0) == int(task.get("id") or 0):
            continue
        if str(other.get("entrypoint") or "") != NODE_LOCAL_AEDT_CANARY_CLIENT_ENTRYPOINT:
            continue
        if int(other.get("same_node_as_task_id") or 0) != host_task_id:
            continue
        if str(other.get("status") or "") in {"queued", "cancelled"}:
            continue
        other_payload = _json_mapping(other.get("payload_json"))
        if str(other_payload.get("aedt_canary_bundle_id") or "") == bundle_id:
            claimed += 1
    if claimed >= expected:
        return False, "node-local AEDT canary already claimed its validated project slots"
    return True, "node-local AEDT canary admitted"
