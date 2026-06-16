from __future__ import annotations

import posixpath
import re
import shlex
import time
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import paramiko

from .config import AccountConfig, GitCredentialConfig
from .git_auth import git_credential_id_from_payload
from .inventory import normalize_gpu_model
from .models import AccountSnapshot, JobStatus
from .task_commands import ACCOUNT_WORKSPACE_PLACEHOLDER, TASK_ID_PLACEHOLDER


@dataclass(frozen=True)
class CommandResult:
    stdout: str
    stderr: str
    exit_code: int


class RemoteExecutionError(RuntimeError):
    def __init__(self, message: str, result_fields: dict[str, Any] | None = None):
        super().__init__(message)
        self.result_fields = result_fields or {}


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

    def write_text_file(self, path: str, text: str) -> None:
        sftp = self.client.open_sftp()
        try:
            with sftp.file(path, "wb") as remote_file:
                remote_file.write(text.encode("utf-8"))
        finally:
            sftp.close()

    def read_text_file(self, path: str) -> str:
        sftp = self.client.open_sftp()
        try:
            with sftp.file(path, "rb") as remote_file:
                data = remote_file.read()
        finally:
            sftp.close()
        return data.decode("utf-8", errors="replace")

    def download_file(self, remote_path: str, local_path: str) -> None:
        sftp = self.client.open_sftp()
        try:
            sftp.get(remote_path, local_path)
        finally:
            sftp.close()

    def upload_file(self, local_path: str, remote_path: str) -> None:
        sftp = self.client.open_sftp()
        try:
            sftp.put(local_path, remote_path)
        finally:
            sftp.close()


def command_failure_message(result: CommandResult, fallback: str) -> str:
    message = result.stderr.strip() or result.stdout.strip()
    return message or fallback


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


def gpu_gres_value(model: str, count: int) -> str:
    gpus = int(count or 0)
    if gpus <= 0:
        return ""
    normalized = normalize_gpu_model(model)
    if normalized:
        return f"gpu:{normalized}:{gpus}"
    return f"gpu:{gpus}"


def build_sbatch_script(job: dict, remote_job_dir: str) -> str:
    if job.get("job_mode") == "packed_srun":
        return build_packed_srun_script(job, remote_job_dir)
    is_absolute = remote_job_dir.startswith("/")
    stdout_path = posixpath.join(remote_job_dir, "slurm-%j.out") if is_absolute else "slurm-%j.out"
    stderr_path = posixpath.join(remote_job_dir, "slurm-%j.err") if is_absolute else "slurm-%j.err"
    repo_path = posixpath.join(remote_job_dir, "repo") if is_absolute else "repo"
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={job['job_name']}",
        f"#SBATCH --time={job['time_limit']}",
        f"#SBATCH --cpus-per-task={int(job['cpus'])}",
        f"#SBATCH --mem={job['memory']}",
        f"#SBATCH --output={stdout_path}",
        f"#SBATCH --error={stderr_path}",
    ]
    if job.get("partition"):
        lines.append(f"#SBATCH --partition={job['partition']}")
    if job.get("node_name"):
        lines.append(f"#SBATCH --nodelist={job['node_name']}")
    gres = gpu_gres_value(str(job.get("gpu_model") or ""), int(job.get("gpus") or 0))
    if gres:
        lines.append(f"#SBATCH --gres={gres}")
    if int(job.get("exclusive_node") or 0):
        lines.append("#SBATCH --exclusive")
    lines.extend(
        [
            "",
            "set -euo pipefail",
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


def build_packed_srun_script(job: dict, remote_job_dir: str) -> str:
    is_absolute = remote_job_dir.startswith("/")
    stdout_path = posixpath.join(remote_job_dir, "slurm-%j.out") if is_absolute else "slurm-%j.out"
    stderr_path = posixpath.join(remote_job_dir, "slurm-%j.err") if is_absolute else "slurm-%j.err"
    sim_count = max(1, int(job.get("simulation_count") or job.get("simulations_per_job") or 1))
    cpus_per_sim = max(1, int(job.get("cpus_per_simulation") or 1))
    initial_workers = max(1, min(sim_count, int(job.get("initial_workers") or (int(job.get("cpus") or 1) // cpus_per_sim) or 1)))
    max_workers = max(initial_workers, min(sim_count, int(job.get("max_workers_per_job") or sim_count)))
    sim_start = max(1, int(job.get("simulation_start") or 1))
    remote_path = job.get("remote_path") or "."
    command = " ".join(["python", shlex.quote(job["entrypoint"]), job.get("arguments") or ""]).strip()
    allocated_cpus = max(cpus_per_sim, int(job.get("cpus") or (initial_workers * cpus_per_sim)))
    mem_per_sim_gb = float(job.get("mem_per_simulation_gb") or 1)
    load_target = float(job.get("load_target") or 0.75)
    ramp_interval = max(60, int(job.get("ramp_interval_seconds") or 900))
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={job['job_name']}",
        f"#SBATCH --time={job['time_limit']}",
        "#SBATCH --ntasks=1",
        f"#SBATCH --cpus-per-task={allocated_cpus}",
        f"#SBATCH --mem={job['memory']}",
        f"#SBATCH --output={stdout_path}",
        f"#SBATCH --error={stderr_path}",
    ]
    if job.get("partition"):
        lines.append(f"#SBATCH --partition={job['partition']}")
    if job.get("node_name"):
        lines.append(f"#SBATCH --nodelist={job['node_name']}")
    gres = gpu_gres_value(str(job.get("gpu_model") or ""), int(job.get("gpus") or 0))
    if gres:
        lines.append(f"#SBATCH --gres={gres}")
    if int(job.get("exclusive_node") or 0):
        lines.append("#SBATCH --exclusive")
    lines.extend(
        [
            "",
            "set -euo pipefail",
            f"cd {shlex.quote(remote_path)}",
            "mkdir -p simul_log log",
            f"export OMP_NUM_THREADS={cpus_per_sim}",
            f"export MKL_NUM_THREADS={cpus_per_sim}",
        ]
    )
    if job.get("env_setup"):
        lines.append(job["env_setup"])
    lines.extend(
        [
            "",
            "python - <<'PY'",
            "import os, subprocess, time",
            f"command = {command!r}",
            f"sim_start = {sim_start}",
            f"sim_count = {sim_count}",
            f"cpus_per_sim = {cpus_per_sim}",
            f"initial_limit = {initial_workers}",
            f"max_limit = {max_workers}",
            f"allocated_cpus = {allocated_cpus}",
            f"mem_per_sim_gb = {mem_per_sim_gb!r}",
            f"load_target = {load_target!r}",
            f"ramp_interval = {ramp_interval}",
            "pending = list(range(sim_start, sim_start + sim_count))",
            "running = []",
            "limit = initial_limit",
            "last_ramp = time.time()",
            "os.makedirs('simul_log', exist_ok=True)",
            "def mem_available_gb():",
            "    try:",
            "        with open('/proc/meminfo', 'r', encoding='utf-8') as f:",
            "            for line in f:",
            "                if line.startswith('MemAvailable:'):",
            "                    return int(line.split()[1]) / 1024 / 1024",
            "    except OSError:",
            "        return 0.0",
            "    return 0.0",
            "def maybe_ramp():",
            "    global limit, last_ramp",
            "    now = time.time()",
            "    if limit >= max_limit or now - last_ramp < ramp_interval:",
            "        return",
            "    load1, load5, load15 = os.getloadavg()",
            "    free_mem = mem_available_gb()",
            "    enough_cpu = load5 < allocated_cpus * load_target",
            "    enough_mem = free_mem > mem_per_sim_gb * 1.25",
            "    if enough_cpu and enough_mem:",
            "        limit += 1",
            "        print(f'[adaptive] increased worker limit to {limit}; load5={load5:.2f}; free_mem_gb={free_mem:.1f}', flush=True)",
            "    last_ramp = now",
            "def launch(sim_id):",
            "    env = os.environ.copy()",
            "    env['SIMULATION_ID'] = str(sim_id)",
            "    env['OMP_NUM_THREADS'] = str(cpus_per_sim)",
            "    env['MKL_NUM_THREADS'] = str(cpus_per_sim)",
            "    log = open(f'./simul_log/{os.environ.get(\"SLURM_JOB_ID\", \"local\")}_{sim_id}.log', 'w', encoding='utf-8')",
            "    proc = subprocess.Popen(command, shell=True, stdout=log, stderr=subprocess.STDOUT, env=env)",
            "    running.append((sim_id, proc, log))",
            "    print(f'[adaptive] started simulation {sim_id}; running={len(running)} limit={limit}', flush=True)",
            "while pending or running:",
            "    while pending and len(running) < limit:",
            "        launch(pending.pop(0))",
            "        time.sleep(2)",
            "    still = []",
            "    for sim_id, proc, log in running:",
            "        rc = proc.poll()",
            "        if rc is None:",
            "            still.append((sim_id, proc, log))",
            "        else:",
            "            log.write(f'\\nSimulation {sim_id} finished with return code {rc}\\n')",
            "            log.close()",
            "            print(f'[adaptive] finished simulation {sim_id}; rc={rc}', flush=True)",
            "            if rc != 0:",
            "                raise SystemExit(rc)",
            "    running = still",
            "    maybe_ramp()",
            "    time.sleep(10)",
            "PY",
        ]
    )
    return "\n".join(lines) + "\n"


def build_allocation_script(allocation: dict, time_limit: str) -> str:
    is_absolute = allocation["remote_dir"].startswith("/")
    stdout_path = allocation["stdout_path"] if is_absolute else "allocation-%j.out"
    stderr_path = allocation["stderr_path"] if is_absolute else "allocation-%j.err"
    lines = [
        "#!/usr/bin/env bash",
        "#SBATCH --job-name=pool",
        f"#SBATCH --time={time_limit}",
        "#SBATCH --nodes=1",
        "#SBATCH --ntasks=1",
        f"#SBATCH --cpus-per-task={int(allocation['total_cpus'])}",
        f"#SBATCH --output={stdout_path}",
        f"#SBATCH --error={stderr_path}",
    ]
    if allocation.get("total_memory_mb"):
        lines.append(f"#SBATCH --mem={int(allocation['total_memory_mb'])}M")
    if allocation.get("partition"):
        lines.append(f"#SBATCH --partition={allocation['partition']}")
    if allocation.get("node_name"):
        lines.append(f"#SBATCH --nodelist={allocation['node_name']}")
    gres = gpu_gres_value(str(allocation.get("gpu_model") or ""), int(allocation.get("total_gpus") or 0))
    if gres:
        lines.append(f"#SBATCH --gres={gres}")
    lines.extend(
        [
            "",
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(allocation['remote_dir'])}",
            f"cd {shlex.quote(allocation['remote_dir'])}",
            "echo ready > allocation.ready",
            "trap 'echo closing > allocation.ready; exit 0' TERM INT",
            "while true; do sleep 60 & wait $!; done",
        ]
    )
    return "\n".join(lines) + "\n"


def apply_env_profile(payload: dict, account: AccountConfig) -> dict:
    profile = str(payload.get("env_profile") or "").strip()
    if not profile:
        return payload
    setup = (account.env_profiles or {}).get(profile, "").strip()
    if not setup:
        return payload
    existing = str(payload.get("env_setup") or "").strip()
    merged = setup if not existing else f"{setup}\n{existing}"
    return {**payload, "env_setup": merged}


def shell_path(path: str) -> str:
    value = (path or ".").strip() or "."
    if value == "~":
        return "$HOME"
    if value.startswith("~/"):
        return "$HOME/" + shlex.quote(value[2:])
    return shlex.quote(value)


def remote_execution_path(path: str) -> str:
    value = (path or ".").strip() or "."
    if value.startswith("/"):
        return value
    if value == "~":
        return "$HOME"
    if value.startswith("~/"):
        return f"$HOME/{value[2:]}"
    return f"$HOME/{value}"


def shell_expandable_path(path: str) -> str:
    if path == "$HOME":
        return '"$HOME"'
    if path.startswith("$HOME/"):
        return "\"$HOME\"/" + shlex.quote(path[len("$HOME/") :])
    return shlex.quote(path)


def resolve_task_placeholders(task: dict, account: AccountConfig) -> dict:
    workspace = account.remote_workspace or "."
    task_id = str(task.get("id") or "")
    command = (
        str(task.get("command") or "")
        .replace(ACCOUNT_WORKSPACE_PLACEHOLDER, shell_path(workspace))
        .replace(TASK_ID_PLACEHOLDER, shlex.quote(task_id))
    )
    remote_cwd = str(task.get("remote_cwd") or "").replace(ACCOUNT_WORKSPACE_PLACEHOLDER, workspace)
    return {**task, "command": command, "remote_cwd": remote_cwd}


def build_task_script(task: dict) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
    ]
    if task.get("payload_json") and task.get("payload_path"):
        lines.extend(
            [
                "python - <<'PY'",
                "from pathlib import Path",
                f"path = Path({str(task['payload_path'])!r})",
                "path.parent.mkdir(parents=True, exist_ok=True)",
                f"path.write_text({str(task['payload_json'])!r}, encoding='utf-8')",
                "PY",
                f"export SLURM_SCHEDULER_PAYLOAD_PATH={shlex.quote(str(task['payload_path']))}",
            ]
        )
    if task.get("git_ssh_command"):
        lines.append("export GIT_TERMINAL_PROMPT=0")
        lines.append(f"export GIT_SSH_COMMAND={shlex.quote(str(task['git_ssh_command']))}")
    lines.append(f"cd {shell_path(task['remote_cwd'])}")
    if task.get("env_setup"):
        lines.append(task["env_setup"])
    lines.append(task["command"])
    return "\n".join(lines) + "\n"


def build_srun_attach_command(
    task: dict,
    allocation: dict,
    script_path: str,
    stdout_path: str,
    stderr_path: str,
    exit_code_path: str,
) -> str:
    srun_parts = [
        "srun",
        f"--jobid={shlex.quote(str(allocation['slurm_job_id']))}",
        "--nodes=1",
        "--ntasks=1",
        f"--cpus-per-task={int(task['cpus'])}",
        f"--mem={int(task['memory_mb'])}M",
    ]
    gres = gpu_gres_value(str(allocation.get("gpu_model") or task.get("gpu_model") or ""), int(task.get("gpus") or 0))
    if gres:
        srun_parts.append(f"--gres={shlex.quote(gres)}")
    cpu_shortage_gpu_task = (
        int(task.get("gpus") or 0) > 0
        and int(task.get("cpus") or 0) <= 4
        and int(allocation.get("free_cpus") or 0) < int(task.get("cpus") or 0)
    )
    if cpu_shortage_gpu_task:
        srun_parts.append("--overlap")
    else:
        srun_parts.append("--exclusive")
    srun_parts.extend(["bash", shell_expandable_path(script_path)])
    srun_command = " ".join(srun_parts)
    return (
        f"{srun_command} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}; "
        f"echo $? > {shlex.quote(exit_code_path)}"
    )


class SlurmAccountClient:
    def __init__(
        self,
        account: AccountConfig,
        git_credentials: list[GitCredentialConfig] | None = None,
        git_source_accounts: list[AccountConfig] | None = None,
    ):
        self.account = account
        self.git_credentials = git_credentials or []
        self.git_source_accounts = {item.name: item for item in (git_source_accounts or [])}

    def snapshot(self, storage_used_gb: float | None = None) -> AccountSnapshot:
        with SSHSession(self.account) as ssh:
            result = ssh.run("squeue -h -u \"$USER\" -o \"%T\"")
            storage_path = self.account.storage_path or self.account.remote_workspace
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

    def storage_used_gb(self) -> float | None:
        storage_path = self.account.storage_path or self.account.remote_workspace
        if not self.account.storage_quota_gb:
            return None
        with SSHSession(self.account) as ssh:
            du = ssh.run(f"mkdir -p {shlex.quote(storage_path)} && du -sk {shlex.quote(storage_path)}")
        if du.exit_code == 0 and du.stdout.strip():
            return parse_du_gb(du.stdout)
        return None

    def submit(self, job: dict) -> dict[str, str]:
        stamp = int(time.time())
        remote_job_dir = posixpath.join(self.account.remote_workspace, f"job-{job['id']}-{stamp}")
        run_script_path = posixpath.join(remote_job_dir, "run.sbatch")
        submit_stdout_path = posixpath.join(remote_job_dir, "submit.stdout.log")
        submit_stderr_path = posixpath.join(remote_job_dir, "submit.stderr.log")
        submit_stdout_file = "submit.stdout.log"
        submit_stderr_file = "submit.stderr.log"
        job = apply_env_profile(job, self.account)
        script = build_sbatch_script(job, remote_job_dir)
        if job.get("job_mode") == "packed_srun":
            commands = [
                f"mkdir -p {shlex.quote(remote_job_dir)}",
                f"cd {shlex.quote(remote_job_dir)} && sbatch run.sbatch > {shlex.quote(submit_stdout_file)} 2> {shlex.quote(submit_stderr_file)}",
            ]
        else:
            repo_url = shlex.quote(job["repo_url"])
            git_ref = shlex.quote(job["git_ref"])
            commands = [
                f"mkdir -p {shlex.quote(remote_job_dir)}",
                (
                    f"cd {shlex.quote(remote_job_dir)} "
                    f"&& git clone {repo_url} repo >> {shlex.quote(submit_stdout_file)} 2>> {shlex.quote(submit_stderr_file)}"
                ),
                (
                    f"cd {shlex.quote(posixpath.join(remote_job_dir, 'repo'))} "
                    f"&& git checkout {git_ref} >> {shlex.quote(posixpath.join('..', submit_stdout_file))} 2>> {shlex.quote(posixpath.join('..', submit_stderr_file))}"
                ),
                f"cd {shlex.quote(remote_job_dir)} && sbatch run.sbatch >> {shlex.quote(submit_stdout_file)} 2>> {shlex.quote(submit_stderr_file)}",
            ]
        with SSHSession(self.account) as ssh:
            mkdir = ssh.run(commands[0])
            if mkdir.exit_code != 0:
                raise RemoteExecutionError(
                    command_failure_message(mkdir, "failed to create remote job directory"),
                    {
                        "remote_job_dir": remote_job_dir,
                        "stdout_path": submit_stdout_path,
                        "stderr_path": submit_stderr_path,
                    },
                )
            ssh.write_text_file(run_script_path, script)
            submit_stdout = ""
            for command in commands[1:]:
                result = ssh.run(command)
                if result.exit_code != 0:
                    break
            else:
                submit_stdout = ssh.read_text_file(submit_stdout_path)
        if result.exit_code != 0:
            raise RemoteExecutionError(
                command_failure_message(result, "sbatch submission failed"),
                {
                    "remote_job_dir": remote_job_dir,
                    "stdout_path": submit_stdout_path,
                    "stderr_path": submit_stderr_path,
                },
            )
        slurm_job_id = parse_sbatch_job_id(result.stdout or submit_stdout)
        return {
            "slurm_job_id": slurm_job_id,
            "remote_job_dir": remote_job_dir,
            "stdout_path": posixpath.join(remote_job_dir, f"slurm-{slurm_job_id}.out"),
            "stderr_path": posixpath.join(remote_job_dir, f"slurm-{slurm_job_id}.err"),
        }

    def submit_allocation(self, allocation: dict, time_limit: str) -> dict[str, str]:
        remote_dir = posixpath.join(self.account.remote_workspace, f"allocation-{allocation['id']}-{int(time.time())}")
        allocation = {
            **allocation,
            "remote_dir": remote_dir,
            "stdout_path": posixpath.join(remote_dir, "allocation-%j.out"),
            "stderr_path": posixpath.join(remote_dir, "allocation-%j.err"),
        }
        script = build_allocation_script(allocation, time_limit)
        commands = [
            f"mkdir -p {shlex.quote(remote_dir)}",
            f"cd {shlex.quote(remote_dir)} && sbatch allocation.sbatch",
        ]
        with SSHSession(self.account) as ssh:
            result = ssh.run(commands[0])
            if result.exit_code != 0:
                raise RemoteExecutionError(
                    command_failure_message(result, "failed to create remote allocation directory"),
                    {
                        "remote_dir": remote_dir,
                        "stdout_path": allocation["stdout_path"],
                        "stderr_path": allocation["stderr_path"],
                    },
                )
            ssh.write_text_file(posixpath.join(remote_dir, "allocation.sbatch"), script)
            result = ssh.run(commands[1])
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "allocation sbatch failed")
        slurm_job_id = parse_sbatch_job_id(result.stdout)
        return {
            "slurm_job_id": slurm_job_id,
            "remote_dir": remote_dir,
            "stdout_path": posixpath.join(remote_dir, f"allocation-{slurm_job_id}.out"),
            "stderr_path": posixpath.join(remote_dir, f"allocation-{slurm_job_id}.err"),
        }

    def attach_task(self, task: dict, allocation: dict) -> dict[str, str]:
        stamp = int(time.time())
        remote_dir = posixpath.join(self.account.remote_workspace, f"task-{task['id']}-{stamp}")
        script_path = posixpath.join(remote_dir, "task.sh")
        script_exec_path = remote_execution_path(script_path)
        stdout_path = posixpath.join(remote_dir, "stdout.log")
        stderr_path = posixpath.join(remote_dir, "stderr.log")
        exit_code_path = posixpath.join(remote_dir, "exit_code")
        wrapper_path = posixpath.join(remote_dir, "wrapper.log")
        result_fields = {
            "remote_dir": remote_dir,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "exit_code_path": exit_code_path,
        }
        task = resolve_task_placeholders(apply_env_profile(task, self.account), self.account)
        task = {**task, "payload_path": posixpath.join(remote_dir, "payload.json")}
        credential = self.git_credential_for_task(task)
        if credential:
            key_path = posixpath.join(remote_dir, "git-auth", f"{credential.id}.key")
            known_hosts_path = posixpath.join(remote_dir, "git-auth", "known_hosts") if credential.known_hosts_path else ""
            ssh_options = [
                f"-i {shlex.quote(key_path)}",
                "-o IdentitiesOnly=yes",
                f"-o StrictHostKeyChecking={shlex.quote(credential.strict_host_key_checking or 'accept-new')}",
            ]
            if known_hosts_path:
                ssh_options.append(f"-o UserKnownHostsFile={shlex.quote(known_hosts_path)}")
            task = {**task, "git_ssh_command": "ssh " + " ".join(ssh_options)}
        script = build_task_script(task)
        wrapper = build_srun_attach_command(
            task,
            allocation,
            script_exec_path,
            stdout_path,
            stderr_path,
            exit_code_path,
        )
        commands = [
            f"mkdir -p {shlex.quote(remote_dir)}",
            f"chmod +x {shlex.quote(script_path)}",
            f"bash -lc {shlex.quote(f'nohup bash -lc {shlex.quote(wrapper)} > {shlex.quote(wrapper_path)} 2>&1 & echo $!')}",
        ]
        with SSHSession(self.account) as ssh:
            result = ssh.run(commands[0])
            if result.exit_code != 0:
                raise RemoteExecutionError(command_failure_message(result, "failed to create remote task directory"), result_fields)
            if credential:
                try:
                    self.write_git_credential_files(ssh, credential, remote_dir)
                except RemoteExecutionError as exc:
                    raise RemoteExecutionError(str(exc), result_fields) from exc
                except OSError as exc:
                    raise RemoteExecutionError(f"failed to read git credential {credential.id}: {exc}", result_fields) from exc
            ssh.write_text_file(script_path, script)
            result = ssh.run(" && ".join(commands[1:]))
        if result.exit_code != 0:
            raise RemoteExecutionError(command_failure_message(result, "srun attach failed"), result_fields)
        return {
            "remote_dir": remote_dir,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "exit_code_path": exit_code_path,
            "wrapper_pid": result.stdout.strip().splitlines()[-1],
        }

    def git_credential_for_task(self, task: dict) -> GitCredentialConfig | None:
        credential_id = git_credential_id_from_payload(str(task.get("payload_json") or ""))
        if not credential_id:
            return None
        return next((item for item in self.git_credentials if item.id == credential_id), None)

    def write_git_credential_files(self, ssh: SSHSession, credential: GitCredentialConfig, remote_dir: str) -> None:
        key_text = self.read_git_credential_text(credential, "private_key")
        key_dir = posixpath.join(remote_dir, "git-auth")
        key_path = posixpath.join(key_dir, f"{credential.id}.key")
        commands = [f"mkdir -p {shlex.quote(key_dir)}", f"chmod 700 {shlex.quote(key_dir)}"]
        result = ssh.run(" && ".join(commands))
        if result.exit_code != 0:
            raise RemoteExecutionError(command_failure_message(result, "failed to prepare git credential directory"))
        ssh.write_text_file(key_path, key_text)
        chmod = ssh.run(f"chmod 600 {shlex.quote(key_path)}")
        if chmod.exit_code != 0:
            raise RemoteExecutionError(command_failure_message(chmod, "failed to protect git credential key"))
        if credential.known_hosts_path or credential.source_known_hosts_path:
            known_hosts = self.read_git_credential_text(credential, "known_hosts")
            ssh.write_text_file(posixpath.join(key_dir, "known_hosts"), known_hosts)

    def read_git_credential_text(self, credential: GitCredentialConfig, kind: str) -> str:
        if kind == "private_key":
            local_path = credential.private_key_path
            remote_path = credential.source_private_key_path
        else:
            local_path = credential.known_hosts_path
            remote_path = credential.source_known_hosts_path
        if local_path:
            return Path(local_path).read_text(encoding="utf-8")
        if remote_path and credential.source_account:
            source = self.git_source_accounts.get(credential.source_account)
            if not source:
                raise FileNotFoundError(f"git credential source account not found: {credential.source_account}")
            with SSHSession(source) as source_ssh:
                result = source_ssh.run(f"cat {shell_path(remote_path)}")
            if result.exit_code != 0:
                raise FileNotFoundError(result.stderr.strip() or f"failed to read {remote_path}")
            return result.stdout
        raise FileNotFoundError(f"git credential {credential.id} has no {kind} path")

    def task_state(self, task: dict) -> JobStatus:
        exit_path = task.get("exit_code_path") or ""
        wrapper_pid = task.get("wrapper_pid") or ""
        if not exit_path:
            return JobStatus.RUNNING
        checks = [f"if test -f {shlex.quote(exit_path)}; then cat {shlex.quote(exit_path)}; exit 0; fi"]
        if wrapper_pid:
            checks.append(f"if ps -p {shlex.quote(str(wrapper_pid))} >/dev/null 2>&1; then echo RUNNING; exit 0; fi")
        checks.append("echo UNKNOWN")
        with SSHSession(self.account) as ssh:
            result = ssh.run("; ".join(checks))
        text = result.stdout.strip().splitlines()[0] if result.stdout.strip() else "UNKNOWN"
        if text == "RUNNING":
            return JobStatus.RUNNING
        try:
            return JobStatus.COMPLETED if int(text) == 0 else JobStatus.FAILED
        except ValueError:
            return JobStatus.RUNNING

    def task_exit_code(self, task: dict) -> int | None:
        exit_path = task.get("exit_code_path") or ""
        if not exit_path:
            return None
        try:
            text = self.read_text_file(exit_path).strip().splitlines()[0]
            return int(text)
        except Exception:
            return None

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

    def pending_reason(self, slurm_job_id: str) -> str:
        command = f"squeue -h -j {shlex.quote(slurm_job_id)} -o \"%R\""
        with SSHSession(self.account) as ssh:
            result = ssh.run(command)
        if result.exit_code != 0 or not result.stdout.strip():
            return ""
        return result.stdout.strip().splitlines()[0].strip()

    def cancel(self, slurm_job_id: str) -> None:
        with SSHSession(self.account) as ssh:
            result = ssh.run(f"scancel {shlex.quote(slurm_job_id)}")
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or "scancel failed")

    def cancel_task(self, task: dict) -> None:
        wrapper_pid = str(task.get("wrapper_pid") or "").strip()
        if not wrapper_pid:
            return
        command = (
            f"pkill -TERM -P {shlex.quote(wrapper_pid)} >/dev/null 2>&1 || true; "
            f"kill -TERM {shlex.quote(wrapper_pid)} >/dev/null 2>&1 || true"
        )
        with SSHSession(self.account) as ssh:
            result = ssh.run(command)
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "task cancel failed")

    def remove_tree(self, remote_path: str) -> None:
        with SSHSession(self.account) as ssh:
            result = ssh.run(f"rm -rf -- {shlex.quote(remote_path)}")
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"failed to remove {remote_path}")

    def read_text_file(self, path: str) -> str:
        with SSHSession(self.account) as ssh:
            result = ssh.run(f"test -f {shlex.quote(path)} && cat {shlex.quote(path)}")
        if result.exit_code != 0:
            raise FileNotFoundError(result.stderr.strip() or f"remote file not found: {path}")
        return result.stdout

    def list_files(self, root: str, pattern: str) -> list[str]:
        script = (
            "python - <<'PY'\n"
            "import glob, json, os\n"
            f"root = {root!r}\n"
            f"pattern = {pattern!r}\n"
            "matches = []\n"
            "for path in glob.glob(os.path.join(root, pattern), recursive=True):\n"
            "    if os.path.isfile(path):\n"
            "        matches.append(os.path.relpath(path, root))\n"
            "print(json.dumps(sorted(matches)))\n"
            "PY"
        )
        with SSHSession(self.account) as ssh:
            result = ssh.run(script)
        if result.exit_code != 0:
            raise FileNotFoundError(result.stderr.strip() or f"remote files not found: {root}/{pattern}")
        try:
            loaded = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            return []
        return [str(item) for item in loaded]
