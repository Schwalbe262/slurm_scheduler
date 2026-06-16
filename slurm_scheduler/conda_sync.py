from __future__ import annotations

import json
import posixpath
import shlex
import tempfile
import threading
import time
from pathlib import Path

from .config import AccountConfig
from .db import Database
from .slurm import SSHSession, command_failure_message


TERMINAL_SYNC_STATES = {"completed", "failed", "cancelled"}


def conda_bootstrap() -> str:
    return (
        "if [ -f \"$HOME/miniconda3/etc/profile.d/conda.sh\" ]; then source \"$HOME/miniconda3/etc/profile.d/conda.sh\"; "
        "elif [ -f \"$HOME/anaconda3/etc/profile.d/conda.sh\" ]; then source \"$HOME/anaconda3/etc/profile.d/conda.sh\"; "
        "elif [ -f \"/home1/$USER/miniconda3/etc/profile.d/conda.sh\" ]; then source \"/home1/$USER/miniconda3/etc/profile.d/conda.sh\"; "
        "elif [ -f \"/home1/$USER/anaconda3/etc/profile.d/conda.sh\" ]; then source \"/home1/$USER/anaconda3/etc/profile.d/conda.sh\"; fi"
    )


def env_prefix_lookup_command(env_name: str) -> str:
    code = (
        "import json,sys;"
        f"name={env_name!r};"
        "data=json.load(sys.stdin);"
        "matches=[p for p in data.get('envs',[]) if p.rstrip('/').split('/')[-1]==name];"
        "sys.exit(2) if not matches else print(matches[0])"
    )
    return f"conda env list --json | python3 -c {shlex.quote(code)}"


def shell_script(command: str) -> str:
    return f"bash -lc {shlex.quote(command)}"


class CondaEnvSyncManager:
    def __init__(self, db: Database, accounts: list[AccountConfig]):
        self.db = db
        self.accounts = {account.name: account for account in accounts}
        self._lock = threading.Lock()
        self._threads: dict[int, threading.Thread] = {}

    def start(self, reference_account: str, source_env_name: str, target_accounts: list[str]) -> int:
        reference = self.accounts.get(reference_account)
        if not reference:
            raise ValueError(f"reference account not found: {reference_account}")
        env_name = source_env_name.strip()
        if not env_name:
            raise ValueError("source_env_name is required")
        targets = []
        for name in target_accounts:
            account_name = name.strip()
            if not account_name:
                continue
            if account_name == reference_account:
                raise ValueError("target_accounts must not include reference_account")
            if account_name not in self.accounts:
                raise ValueError(f"target account not found: {account_name}")
            if account_name not in targets:
                targets.append(account_name)
        if not targets:
            raise ValueError("target_accounts is required")
        sync_job_id = self.db.create_env_sync_job(reference_account, env_name, env_name, targets)
        for account_name in targets:
            self.db.create_env_sync_target(sync_job_id, account_name)
        thread = threading.Thread(target=self._run_job, args=(sync_job_id,), daemon=True)
        with self._lock:
            self._threads[sync_job_id] = thread
        thread.start()
        return sync_job_id

    def cancel(self, sync_job_id: int) -> None:
        job = self.db.get_env_sync_job(sync_job_id)
        if not job:
            raise ValueError("sync job not found")
        if job["status"] in TERMINAL_SYNC_STATES:
            return
        self.db.update_env_sync_job(sync_job_id, status="cancelled", finished_at="CURRENT_TIMESTAMP")
        for target in self.db.list_env_sync_targets(sync_job_id):
            if target["status"] not in TERMINAL_SYNC_STATES:
                self.db.update_env_sync_target(target["id"], status="cancelled", finished_at="CURRENT_TIMESTAMP")

    def _cancelled(self, sync_job_id: int) -> bool:
        job = self.db.get_env_sync_job(sync_job_id)
        return not job or job["status"] == "cancelled"

    def _run_job(self, sync_job_id: int) -> None:
        job = self.db.get_env_sync_job(sync_job_id)
        if not job:
            return
        self.db.update_env_sync_job(sync_job_id, status="running", started_at="CURRENT_TIMESTAMP")
        local_archive = ""
        try:
            local_archive, source_archive = self._pack_reference_env(job)
            if self._cancelled(sync_job_id):
                return
            target_threads = []
            for target in self.db.list_env_sync_targets(sync_job_id):
                thread = threading.Thread(
                    target=self._run_target,
                    args=(sync_job_id, target, local_archive, source_archive),
                    daemon=True,
                )
                target_threads.append(thread)
                thread.start()
            for thread in target_threads:
                thread.join()
            if self._cancelled(sync_job_id):
                return
            targets = self.db.list_env_sync_targets(sync_job_id)
            failures = [target for target in targets if target["status"] != "completed"]
            if failures:
                self.db.update_env_sync_job(
                    sync_job_id,
                    status="failed",
                    failure_message=f"{len(failures)} target(s) failed",
                    finished_at="CURRENT_TIMESTAMP",
                )
            else:
                self.db.update_env_sync_job(sync_job_id, status="completed", finished_at="CURRENT_TIMESTAMP")
        except Exception as exc:
            if not self._cancelled(sync_job_id):
                self.db.update_env_sync_job(
                    sync_job_id,
                    status="failed",
                    failure_message=str(exc),
                    finished_at="CURRENT_TIMESTAMP",
                )
        finally:
            if local_archive:
                try:
                    Path(local_archive).unlink(missing_ok=True)
                except OSError:
                    pass

    def _pack_reference_env(self, job: dict) -> tuple[str, str]:
        account = self.accounts[str(job["reference_account"])]
        env_name = str(job["source_env_name"])
        remote_dir = posixpath.join(account.remote_workspace, "env-sync", f"job-{job['id']}", "reference")
        archive_path = posixpath.join(remote_dir, f"{env_name}.tar.gz")
        log_path = posixpath.join(remote_dir, "pack.log")
        command = "\n".join(
            [
                "set -euo pipefail",
                conda_bootstrap(),
                "command -v conda >/dev/null",
                "command -v conda-pack >/dev/null",
                f"mkdir -p {shlex.quote(remote_dir)}",
                f"prefix=$({env_prefix_lookup_command(env_name)})",
                f"conda-pack -p \"$prefix\" -o {shlex.quote(archive_path)} --force",
            ]
        )
        with SSHSession(account) as ssh:
            result = ssh.run(f"mkdir -p {shlex.quote(remote_dir)} && {shell_script(command)} > {shlex.quote(log_path)} 2>&1")
            if result.exit_code != 0:
                try:
                    log_text = ssh.read_text_file(log_path)
                except Exception:
                    log_text = command_failure_message(result, "reference conda-pack failed")
                raise RuntimeError(log_text.strip() or "reference conda-pack failed")
            local_file = tempfile.NamedTemporaryFile(prefix=f"conda-env-sync-{job['id']}-", suffix=".tar.gz", delete=False)
            local_file.close()
            ssh.download_file(archive_path, local_file.name)
        return local_file.name, archive_path

    def _run_target(self, sync_job_id: int, target: dict, local_archive: str, source_archive: str) -> None:
        if self._cancelled(sync_job_id):
            return
        job = self.db.get_env_sync_job(sync_job_id)
        if not job:
            return
        account_name = str(target["account_name"])
        account = self.accounts[account_name]
        env_name = str(job["target_env_name"])
        remote_dir = posixpath.join(account.remote_workspace, "env-sync", f"job-{sync_job_id}", account_name)
        archive_path = posixpath.join(remote_dir, f"{env_name}.tar.gz")
        log_path = posixpath.join(remote_dir, "install.log")
        self.db.update_env_sync_target(
            target["id"],
            status="running",
            started_at="CURRENT_TIMESTAMP",
            remote_dir=remote_dir,
            log_path=log_path,
            archive_path=archive_path,
        )
        try:
            with SSHSession(account) as ssh:
                mkdir = ssh.run(f"mkdir -p {shlex.quote(remote_dir)}")
                if mkdir.exit_code != 0:
                    raise RuntimeError(command_failure_message(mkdir, "failed to create target sync directory"))
                ssh.upload_file(local_archive, archive_path)
                install = self._install_command(env_name, archive_path)
                result = ssh.run(f"{shell_script(install)} > {shlex.quote(log_path)} 2>&1")
                if result.exit_code != 0:
                    try:
                        log_text = ssh.read_text_file(log_path)
                    except Exception:
                        log_text = command_failure_message(result, "target env install failed")
                    raise RuntimeError(log_text.strip() or "target env install failed")
                installed_prefix = ssh.read_text_file(posixpath.join(remote_dir, "installed_prefix.txt")).strip()
                backup_path = ""
                try:
                    backup_path = ssh.read_text_file(posixpath.join(remote_dir, "backup_path.txt")).strip()
                except Exception:
                    backup_path = ""
            self.db.upsert_account_env_overlay(account_name, env_name, installed_prefix, sync_job_id)
            self.db.update_env_sync_target(
                target["id"],
                status="completed",
                installed_prefix=installed_prefix,
                backup_path=backup_path,
                finished_at="CURRENT_TIMESTAMP",
            )
        except Exception as exc:
            if not self._cancelled(sync_job_id):
                self.db.update_env_sync_target(
                    target["id"],
                    status="failed",
                    failure_message=str(exc),
                    finished_at="CURRENT_TIMESTAMP",
                )

    def _install_command(self, env_name: str, archive_path: str) -> str:
        timestamp = int(time.time())
        return "\n".join(
            [
                "set -euo pipefail",
                conda_bootstrap(),
                "command -v conda >/dev/null",
                f"mkdir -p $(dirname {shlex.quote(archive_path)})",
                f"existing=$({env_prefix_lookup_command(env_name)} || true)",
                "base=$(conda info --base)",
                f"target=${{existing:-$base/envs/{env_name}}}",
                f"backup=\"${{target}}.bak.{timestamp}\"",
                "if [ -n \"$existing\" ] && [ -d \"$existing\" ]; then mv \"$existing\" \"$backup\"; echo \"$backup\" > "
                + shlex.quote(posixpath.join(posixpath.dirname(archive_path), "backup_path.txt"))
                + "; fi",
                "mkdir -p \"$target\"",
                f"tar -xzf {shlex.quote(archive_path)} -C \"$target\"",
                "if [ -x \"$target/bin/conda-unpack\" ]; then \"$target/bin/conda-unpack\"; fi",
                "echo \"$target\" > " + shlex.quote(posixpath.join(posixpath.dirname(archive_path), "installed_prefix.txt")),
            ]
        )
