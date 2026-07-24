from __future__ import annotations

from datetime import datetime
import json
import posixpath
import re
from typing import Any


WORKSPACE_PRUNE_PROTECTION_MARKER = ".slurm-scheduler-preserve.json"
WORKSPACE_PRUNE_PROTECTION_SCHEMA = "slurm-scheduler-prune-protection-v1"
WORKSPACE_PRUNE_PROTECTION_MAX_BYTES = 16 * 1024

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_MANIFEST_FIELDS = {
    "schema",
    "preserve",
    "created_at",
    "reason",
    "owner",
    "artifact_manifest",
}


def _normalized_remote_path(value: str) -> str:
    path = (value or "").strip()
    if (
        not path
        or "\x00" in path
        or "\n" in path
        or "\r" in path
        or "\t" in path
    ):
        raise ValueError("remote path is empty or contains control characters")
    normalized = posixpath.normpath(path)
    if normalized in {"", "."}:
        raise ValueError("remote path is empty after normalization")
    return normalized


def workspace_prune_protection_marker_paths(
    workspace: str, artifact: str
) -> tuple[str, ...]:
    """Return marker paths from an artifact up to its workspace root.

    The lexical containment check is deliberate: global pruning only accepts
    paths returned by ``find`` below the configured workspace, and deletion
    must never probe or act outside that same root.
    """

    root = _normalized_remote_path(workspace).rstrip("/") or "/"
    current = _normalized_remote_path(artifact).rstrip("/") or "/"
    prefix = "/" if root == "/" else root + "/"
    if current == root or not current.startswith(prefix):
        raise ValueError(
            f"artifact is outside workspace: {artifact!r} not below {workspace!r}"
        )
    markers: list[str] = []
    while True:
        markers.append(
            posixpath.join(current, WORKSPACE_PRUNE_PROTECTION_MARKER)
        )
        if current == root:
            break
        parent = posixpath.dirname(current)
        if parent == current or (
            root != "/" and parent != root and not parent.startswith(prefix)
        ):
            raise ValueError("artifact ancestor walk escaped workspace")
        current = parent
    return tuple(markers)


def parse_workspace_prune_protection_manifest(
    raw: str | bytes,
) -> dict[str, Any]:
    """Parse and strictly validate the v1 prune-protection manifest.

    The scheduler protects an artifact on marker *presence*, even when this
    validation fails. That fail-closed rule prevents a partial write or
    damaged marker from turning a sealed artifact into deletion input.
    """

    try:
        encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
        text = encoded.decode("utf-8")
    except UnicodeError as exc:
        raise ValueError(
            "prune-protection manifest is not valid UTF-8 JSON"
        ) from exc
    if len(encoded) > WORKSPACE_PRUNE_PROTECTION_MAX_BYTES:
        raise ValueError("prune-protection manifest exceeds 16 KiB")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("prune-protection manifest is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("prune-protection manifest must be a JSON object")
    unknown = set(value) - _MANIFEST_FIELDS
    if unknown:
        raise ValueError(
            "prune-protection manifest has unknown fields: "
            + ", ".join(sorted(unknown))
        )
    required = {"schema", "preserve", "created_at", "reason"}
    missing = required - set(value)
    if missing:
        raise ValueError(
            "prune-protection manifest is missing fields: "
            + ", ".join(sorted(missing))
        )
    if value["schema"] != WORKSPACE_PRUNE_PROTECTION_SCHEMA:
        raise ValueError("unsupported prune-protection manifest schema")
    if value["preserve"] is not True:
        raise ValueError("prune-protection manifest must set preserve=true")
    created_at = value["created_at"]
    if not isinstance(created_at, str) or not created_at.strip():
        raise ValueError("prune-protection created_at must be a timestamp")
    try:
        parsed_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("prune-protection created_at is not ISO-8601") from exc
    if parsed_at.tzinfo is None:
        raise ValueError("prune-protection created_at must include a timezone")
    reason = value["reason"]
    if (
        not isinstance(reason, str)
        or not reason.strip()
        or len(reason.strip()) > 1024
    ):
        raise ValueError(
            "prune-protection reason must contain 1 to 1024 characters"
        )
    owner = value.get("owner")
    if owner is not None and (
        not isinstance(owner, str)
        or not owner.strip()
        or len(owner.strip()) > 256
    ):
        raise ValueError(
            "prune-protection owner must contain 1 to 256 characters"
        )
    artifact_manifest = value.get("artifact_manifest")
    if artifact_manifest is not None:
        if (
            not isinstance(artifact_manifest, dict)
            or set(artifact_manifest) != {"path", "sha256"}
        ):
            raise ValueError(
                "artifact_manifest must contain exactly path and sha256"
            )
        path = artifact_manifest["path"]
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or "\\" in path
            or any(control in path for control in ("\x00", "\n", "\r", "\t"))
            or any(part in {"", ".", ".."} for part in path.split("/"))
            or posixpath.normpath(path) != path
        ):
            raise ValueError("artifact_manifest path must be safe and relative")
        digest = artifact_manifest["sha256"]
        if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
            raise ValueError(
                "artifact_manifest sha256 must be 64 lowercase hex characters"
            )
    return value
