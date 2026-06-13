from __future__ import annotations

import posixpath
import re
import shlex
import time
from dataclasses import dataclass

import paramiko

from .config import AccountConfig
from .models import AccountSnapshot, JobStatus


@dataclass(frozen=True)
class CommandResult:
    stdout: str
    stderr: str
    exit_code: int


class SSHSession:
    def __init__(self, account: AccountConfig):
        self.account = account
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def __enter__(self) -> "SSHSession":
        self.client.connect(
            hostname=self.account.host,
            port=self.account.port,
            username=self.account.username,
            key_filename=self.account.private_key_path,
            timeout=20,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.client.close()

    def run(self, command: str) -> CommandResult:
        stdin, stdout, stderr = self.client.exec_command(command)
        del stdin
        exit_code = stdout.channel.recv_exit_status()
        return CommandResult(
            stdout=stdout.read().decode("utf-8", errors="replace"),
            stderr=stderr.read().decode("utf-8", errors="replace"),
            exit_code=exit_code,
        )


def parse_squeue_counts(output: str) -> tuple[int, int]:
    running = 0
    pending = 0
    for line in output.splitlines():
        state = line.strip()
        if state in {"R", "RUNNING"}:
            running += 1
        elif state in {"PD", "PENDING"}:
            pending += 1
    return running, pending


def parse_sbatch_job_id(output: str) -> str:
    match = re.search(r"Submitted batch job\s+(\d+)", output)
    if not match:
        raise ValueError(f"could not parse sbatch output: {output!r}")
    return match.group(1)


def parse_du_gb(output: str) -> float:
    first = output.splitlines()[0].split()[0]
    return int(first) / 1024 / 1024


def map_slurm_state(state: str) -> JobStatus:
    if state in {"R", "RUNNING", "CF", "CONFIGURING", "CG", "COMPLETING"}:
        return JobStatus.RUNNING
    if state in {"PD", "PENDING"}:
        return JobStatus.SUBMITTED
    if state in {"CD", "COMPLETED"}:
        return JobStatus.COMPLETED
    if state in {"CA", "CANCELLED"}:
        return JobStatus.CANCELLED
    if state in {"F", "FAILED", "TO", "TIMEOUT", "NF", "NODE_FAIL", "OOM", "OUT_OF_MEMORY"}:
        return JobStatus.FAILED
    return JobStatus.SUBMITTED


def build_sbatch_script(job: dict, remote_job_dir: str) -> str:
    is_absolute = remote_job_dir.startswith("/")
    stdout_path = posixpath.join(remote_job_dir, "slurm-%j.out") if is_absolute else "slurm-%j.out"
    stderr_path = posixpath.join(remote_job_dir, "slurm-%j.err") if is_absolute else "slurm-%j.err"
    repo_path = posixpath.join(remote_job_dir, "repo") if is_absolute else "repo"
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        f"#SBATCH --job-name={job['job_name']}",
        f"#SBATCH --time={job['time_limit']}",
        f"#SBATCH --cpus-per-task={int(job['cpus'])}",
        f"#SBATCH --mem={job['memory']}",
        f"#SBATCH --output={stdout_path}",
        f"#SBATCH --error={stderr_path}",
    ]
    if job.get("partition"):
        lines.append(f"#SBATCH --partition={job['partition']}")
    if int(job.get("gpus") or 0) > 0:
        lines.append(f"#SBATCH --gres=gpu:{int(job['gpus'])}")
    lines.extend(
        [
            "",
            f"cd {shlex.quote(repo_path)}",
        ]
    )
    if job.get("env_setup"):
        lines.append(job["env_setup"])
    command = ["python", job["entrypoint"]]
    if job.get("arguments"):
        command.append(job["arguments"])
    lines.append(" ".join(command))
    return "\n".join(lines) + "\n"


class SlurmAccountClient:
    def __init__(self, account: AccountConfig):
        self.account = account

    def snapshot(self) -> AccountSnapshot:
        with SSHSession(self.account) as ssh:
            result = ssh.run("squeue -h -u \"$USER\" -o \"%T\"")
            storage_used_gb = None
            storage_path = self.account.storage_path or self.account.remote_workspace
            if self.account.storage_quota_gb:
                du = ssh.run(f"mkdir -p {shlex.quote(storage_path)} && du -sk {shlex.quote(storage_path)}")
                if du.exit_code == 0 and du.stdout.strip():
                    storage_used_gb = parse_du_gb(du.stdout)
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or "squeue failed")
        running, pending = parse_squeue_counts(result.stdout)
        return AccountSnapshot(
            account_name=self.account.name,
            running=running,
            pending=pending,
            max_running=self.account.max_running_jobs,
            max_pending=self.account.max_pending_jobs,
            max_total=self.account.max_total_jobs,
            storage_path=storage_path,
            storage_used_gb=storage_used_gb,
            storage_quota_gb=self.account.storage_quota_gb or None,
        )

    def submit(self, job: dict) -> dict[str, str]:
        stamp = int(time.time())
        remote_job_dir = posixpath.join(self.account.remote_workspace, f"job-{job['id']}-{stamp}")
        script = build_sbatch_script(job, remote_job_dir)
        quoted_script = shlex.quote(script)
        repo_url = shlex.quote(job["repo_url"])
        git_ref = shlex.quote(job["git_ref"])
        commands = [
            f"mkdir -p {shlex.quote(remote_job_dir)}",
            f"cd {shlex.quote(remote_job_dir)} && git clone {repo_url} repo",
            f"cd {shlex.quote(posixpath.join(remote_job_dir, 'repo'))} && git checkout {git_ref}",
            f"printf %s {quoted_script} > {shlex.quote(posixpath.join(remote_job_dir, 'run.sbatch'))}",
            f"cd {shlex.quote(remote_job_dir)} && sbatch run.sbatch",
        ]
        with SSHSession(self.account) as ssh:
            result = ssh.run(" && ".join(commands))
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "sbatch failed")
        slurm_job_id = parse_sbatch_job_id(result.stdout)
        return {
            "slurm_job_id": slurm_job_id,
            "remote_job_dir": remote_job_dir,
            "stdout_path": posixpath.join(remote_job_dir, f"slurm-{slurm_job_id}.out"),
            "stderr_path": posixpath.join(remote_job_dir, f"slurm-{slurm_job_id}.err"),
        }

    def state(self, slurm_job_id: str) -> JobStatus:
        command = f"squeue -h -j {shlex.quote(slurm_job_id)} -o \"%T\""
        with SSHSession(self.account) as ssh:
            result = ssh.run(command)
            if result.exit_code == 0 and result.stdout.strip():
                return map_slurm_state(result.stdout.splitlines()[0].strip())
            sacct = ssh.run(f"sacct -n -j {shlex.quote(slurm_job_id)} -X -o State")
        if sacct.exit_code == 0 and sacct.stdout.strip():
            return map_slurm_state(sacct.stdout.splitlines()[0].strip().split()[0])
        return JobStatus.SUBMITTED

    def cancel(self, slurm_job_id: str) -> None:
        with SSHSession(self.account) as ssh:
            result = ssh.run(f"scancel {shlex.quote(slurm_job_id)}")
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or "scancel failed")
