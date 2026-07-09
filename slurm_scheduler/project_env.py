from __future__ import annotations

import json
import posixpath
import shlex
import threading
from typing import Any

from .config import AccountConfig
from .db import Database
from .slurm import SSHSession

# Root under each account's $HOME where project trees are cloned:
#   $HOME/<PROJECTS_ROOT>/<project-name>/<repo-dir>
# Kept separate from run artifacts (task-*/job-*/allocation-*, which live under
# <workspace>/runs/<date>/) so the workspace stays organized.
PROJECTS_ROOT = "slurm_scheduler/projects"

# Cloning/pulling several repos can take a while; keep a long but bounded deadline
# (mirrors the conda-sync pack/install timeout).
DEPLOY_SSH_TIMEOUT = 1800.0


def repo_dir_name(repo: dict[str, Any], index: int) -> str:
    """Folder name a repo is cloned into. Explicit ``subdir`` wins, else the
    repo basename (without ``.git``)."""
    subdir = str(repo.get("subdir") or "").strip().strip("/")
    if subdir:
        return subdir
    url = str(repo.get("url") or "").rstrip("/")
    base = url.split("/")[-1]
    if base.endswith(".git"):
        base = base[:-4]
    return base or f"repo{index}"


def _home_path(rel: str) -> str:
    """A shell token expanding to ``$HOME/<rel>`` with ``rel`` safely quoted."""
    return '"$HOME/"' + shlex.quote(rel.strip("/"))


def build_repo_sync_script(rel_dest: str, url: str, ref: str) -> str:
    """Idempotent clone-once / git-pull for a single repo (public https)."""
    dest = _home_path(rel_dest)
    branch = f"--branch {shlex.quote(ref)} " if ref else ""
    checkout = f'  git -C "$dest" checkout -q {shlex.quote(ref)}\n' if ref else ""
    return "\n".join(
        [
            "set -euo pipefail",
            f"dest={dest}",
            'if [ -d "$dest/.git" ]; then',
            '  git -C "$dest" fetch -q --all --prune',
            checkout + '  git -C "$dest" pull -q --ff-only || git -C "$dest" reset -q --hard "@{u}"',
            "else",
            '  rm -rf -- "$dest"',
            '  mkdir -p -- "$(dirname "$dest")"',
            f'  git clone -q --depth 1 {branch}{shlex.quote(url)} "$dest"',
            "fi",
        ]
    )


class ProjectEnvManager:
    """Deploys/updates project code bundles (git repos) into a fixed per-account
    folder ``$HOME/slurm_scheduler/<project>/``. Mirrors CondaEnvSyncManager:
    validation up front, a daemon thread per run, per-account fan-out, and
    status written to ``project_deployments``."""

    def __init__(self, db: Database, accounts: list[AccountConfig], projects_root: str = PROJECTS_ROOT):
        self.db = db
        self.accounts = {account.name: account for account in accounts}
        self.projects_root = (projects_root or PROJECTS_ROOT).strip("/") or PROJECTS_ROOT
        self._lock = threading.Lock()
        self._threads: dict[int, threading.Thread] = {}

    def project_rel_dir(self, project_name: str) -> str:
        """Path relative to $HOME for a project's tree."""
        return posixpath.join(self.projects_root, project_name)

    def _validate_targets(self, target_accounts: list[str]) -> list[str]:
        targets: list[str] = []
        for name in target_accounts:
            account_name = (name or "").strip()
            if not account_name:
                continue
            if account_name not in self.accounts:
                raise ValueError(f"account not found: {account_name}")
            if account_name not in targets:
                targets.append(account_name)
        if not targets:
            raise ValueError("target_accounts is required")
        return targets

    def deploy(self, project_name: str, target_accounts: list[str], update_only: bool = False) -> int:
        project = self.db.get_project_by_name(project_name)
        if not project:
            raise ValueError(f"project not found: {project_name}")
        targets = self._validate_targets(target_accounts)
        thread = threading.Thread(target=self._run, args=(project, targets, update_only), daemon=True)
        with self._lock:
            self._threads[int(project["id"])] = thread
        thread.start()
        return len(targets)

    def update(self, project_name: str, target_accounts: list[str] | None = None) -> int:
        project = self.db.get_project_by_name(project_name)
        if not project:
            raise ValueError(f"project not found: {project_name}")
        if target_accounts is None:
            target_accounts = [
                str(row["account_name"])
                for row in self.db.list_project_deployments(int(project["id"]))
                if row.get("status") == "deployed"
            ]
        if not target_accounts:
            raise ValueError("no deployed accounts to update")
        return self.deploy(project_name, target_accounts, update_only=True)

    def _run(self, project: dict[str, Any], targets: list[str], update_only: bool) -> None:
        account_threads = []
        for account_name in targets:
            thread = threading.Thread(
                target=self._deploy_account,
                args=(project, account_name, update_only),
                daemon=True,
            )
            account_threads.append(thread)
            thread.start()
        for thread in account_threads:
            thread.join()

    def _deploy_account(self, project: dict[str, Any], account_name: str, update_only: bool) -> None:
        account = self.accounts[account_name]
        project_id = int(project["id"])
        rel_dir = self.project_rel_dir(str(project["name"]))
        remote_dir = posixpath.join("$HOME", rel_dir)
        deployment_id = self.db.upsert_project_deployment(project_id, account_name)
        self.db.update_project_deployment(
            deployment_id,
            status="updating" if update_only else "deploying",
            remote_dir=remote_dir,
            failure_message="",
            started_at="CURRENT_TIMESTAMP",
        )
        try:
            repos = json.loads(project.get("repos") or "[]")
        except (TypeError, ValueError):
            repos = []
        try:
            refs: dict[str, str] = {}
            with SSHSession(account, default_timeout=DEPLOY_SSH_TIMEOUT) as ssh:
                mkdir = ssh.run(f"mkdir -p {_home_path(rel_dir)}")
                if mkdir.exit_code != 0:
                    raise RuntimeError(mkdir.stderr.strip() or "failed to create project directory")
                if not repos:
                    raise RuntimeError("project has no repos to deploy")
                for index, repo in enumerate(repos):
                    url = str(repo.get("url") or "").strip()
                    if not url:
                        continue
                    name = repo_dir_name(repo, index)
                    rel_dest = posixpath.join(rel_dir, name)
                    script = build_repo_sync_script(rel_dest, url, str(repo.get("ref") or "").strip())
                    result = ssh.run(f"bash -lc {shlex.quote(script)}")
                    if result.exit_code != 0:
                        raise RuntimeError(
                            f"repo {name}: {(result.stderr or result.stdout).strip()[:800] or 'clone/pull failed'}"
                        )
                    sha = ssh.run(f"git -C {_home_path(rel_dest)} rev-parse HEAD")
                    refs[name] = sha.stdout.strip() if sha.exit_code == 0 else ""
            self.db.update_project_deployment(
                deployment_id,
                status="deployed",
                deployed_refs=json.dumps(refs),
                finished_at="CURRENT_TIMESTAMP",
            )
        except Exception as exc:  # noqa: BLE001 - surface any failure into the row
            self.db.update_project_deployment(
                deployment_id,
                status="failed",
                failure_message=str(exc)[:2000],
                finished_at="CURRENT_TIMESTAMP",
            )
