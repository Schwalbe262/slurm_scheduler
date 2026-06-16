from __future__ import annotations

import fnmatch
import json

from .config import GitCredentialConfig


def find_git_credential(
    credentials: list[GitCredentialConfig],
    repo_url: str,
    credential_id: str = "",
) -> GitCredentialConfig | None:
    requested_id = (credential_id or "").strip()
    if requested_id:
        return next((item for item in credentials if item.id == requested_id), None)
    url = (repo_url or "").strip()
    for credential in credentials:
        for pattern in credential.url_patterns:
            if fnmatch.fnmatch(url, pattern) or pattern in url:
                return credential
    return None


def git_task_payload(
    repo_url: str,
    git_ref: str,
    entrypoint: str,
    arguments: str = "",
    credential: GitCredentialConfig | None = None,
) -> str:
    payload = {
        "type": "git_task",
        "repo_url": repo_url,
        "git_ref": git_ref,
        "entrypoint": entrypoint,
        "arguments": arguments,
    }
    if credential:
        payload["git_credential_id"] = credential.id
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def git_credential_id_from_payload(payload_json: str) -> str:
    if not payload_json:
        return ""
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("git_credential_id") or "").strip()
