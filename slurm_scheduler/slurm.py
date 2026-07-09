from __future__ import annotations

import posixpath
import re
import shlex
import time
import json
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import paramiko

from .config import AccountConfig, GitCredentialConfig
from .git_auth import git_credential_id_from_payload
from .inventory import normalize_gpu_model
from .models import AccountSnapshot, JobStatus, SchedulingProfile, normalize_scheduling_profile
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


class RemoteCommandTimeout(RemoteExecutionError, TimeoutError):
    """Remote command exceeded its deadline.

    Subclasses both RemoteExecutionError and TimeoutError so every existing
    except clause keeps catching it.
    """


DEFAULT_COMMAND_TIMEOUT = 30.0
SLOW_COMMAND_TIMEOUT = 300.0
_UNSET = object()


class SSHSession:
    def __init__(self, account: AccountConfig, default_timeout: float | None = DEFAULT_COMMAND_TIMEOUT):
        self.account = account
        self.default_timeout = default_timeout
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def __enter__(self) -> "SSHSession":
        self.ensure_connected()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def ensure_connected(self) -> None:
        transport = self.client.get_transport()
        if transport is not None and transport.is_active():
            return
        self.client.connect(
            hostname=self.account.host,
            port=self.account.port,
            username=self.account.username,
            key_filename=self.account.private_key_path,
            timeout=20,
        )

    def close(self) -> None:
        self.client.close()

    def force_close(self) -> None:
        """Close the transport from another thread; safe to call on a session
        whose owner is blocked in a read — the read raises and unblocks."""
        try:
            transport = self.client.get_transport()
            if transport is not None:
                transport.close()
        except Exception:
            pass

    def _resolve_timeout(self, timeout: float | None | object) -> float | None:
        if timeout is _UNSET:
            return self.default_timeout
        return timeout  # type: ignore[return-value]

    def run(self, command: str, timeout: float | None | object = _UNSET) -> CommandResult:
        timeout = self._resolve_timeout(timeout)
        stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        del stdin
        if timeout and timeout > 0:
            deadline = time.monotonic() + timeout
            channel = stdout.channel
            while not channel.exit_status_ready():
                if time.monotonic() >= deadline:
                    channel.close()
                    raise RemoteCommandTimeout(
                        f"remote command timed out after {timeout:g}s on {self.account.name}: {command[:120]}"
                    )
                time.sleep(0.05)
            exit_code = channel.recv_exit_status()
        else:
            exit_code = stdout.channel.recv_exit_status()
        return CommandResult(
            stdout=stdout.read().decode("utf-8", errors="replace"),
            stderr=stderr.read().decode("utf-8", errors="replace"),
            exit_code=exit_code,
        )

    def _open_sftp(self) -> paramiko.SFTPClient:
        sftp = self.client.open_sftp()
        channel = sftp.get_channel()
        if channel is not None:
            channel.settimeout(self.default_timeout or DEFAULT_COMMAND_TIMEOUT)
        return sftp

    def write_text_file(self, path: str, text: str) -> None:
        sftp = self._open_sftp()
        try:
            with sftp.file(path, "wb") as remote_file:
                remote_file.write(text.encode("utf-8"))
        finally:
            sftp.close()

    def read_text_file(self, path: str) -> str:
        sftp = self._open_sftp()
        try:
            with sftp.file(path, "rb") as remote_file:
                data = remote_file.read()
        finally:
            sftp.close()
        return data.decode("utf-8", errors="replace")

    def download_file(self, remote_path: str, local_path: str) -> None:
        sftp = self._open_sftp()
        try:
            sftp.get(remote_path, local_path)
        finally:
            sftp.close()

    def upload_file(self, local_path: str, remote_path: str) -> None:
        sftp = self._open_sftp()
        try:
            sftp.put(local_path, remote_path)
        finally:
            sftp.close()


def command_failure_message(result: CommandResult, fallback: str) -> str:
    message = result.stderr.strip() or result.stdout.strip()
    return message or fallback


def remote_text_command(path: str, tail_lines: int = 0, max_bytes: int = 0) -> str:
    quoted = shlex.quote(path)
    prefix = f"test -f {quoted} && "
    if tail_lines > 0:
        command = f"tail -n {int(tail_lines)} -- {quoted}"
        if max_bytes > 0:
            command = f"{command} | tail -c {int(max_bytes)}"
        return prefix + command
    if max_bytes > 0:
        return prefix + f"tail -c {int(max_bytes)} -- {quoted}"
    return prefix + f"cat {quoted}"


@dataclass(frozen=True)
class JobStateInfo:
    status: JobStatus
    raw_state: str = ""
    pending_reason: str = ""
    node_name: str = ""
    partition: str = ""


@dataclass(frozen=True)
class TaskProbe:
    status: JobStatus
    exit_code: int | None = None


def parse_squeue_table(output: str) -> dict[str, tuple[str, str, str, str]]:
    """Parse `squeue -h -o "%i|%T|%R|%N|%P"` into job_id -> (state, reason, node, partition)."""
    table: dict[str, tuple[str, str, str, str]] = {}
    for line in output.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 2 or not parts[0].strip():
            continue
        job_id = parts[0].strip()
        state = parts[1].strip()
        reason = parts[2].strip() if len(parts) > 2 else ""
        node = parts[3].strip() if len(parts) > 3 else ""
        partition = parts[4].strip() if len(parts) > 4 else ""
        if node in {"(None)", "N/A", "None"}:
            node = ""
        table[job_id] = (state, reason, node, partition)
    return table


def parse_sacct_states(output: str) -> dict[str, str]:
    """Parse `sacct -n -X -o JobID,State` into job_id -> first state word."""
    states: dict[str, str] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        job_id = parts[0].strip()
        if not job_id or "." in job_id:
            continue
        states[job_id] = parts[1].strip()
    return states


def _task_probe_from_value(value: str) -> TaskProbe:
    # Mirrors task_state(): RUNNING/garbage -> running, integer -> finished.
    if value == "RUNNING":
        return TaskProbe(status=JobStatus.RUNNING)
    try:
        exit_code = int(value)
    except ValueError:
        return TaskProbe(status=JobStatus.RUNNING)
    if exit_code == 0:
        return TaskProbe(status=JobStatus.COMPLETED, exit_code=0)
    return TaskProbe(status=JobStatus.FAILED, exit_code=exit_code)


def parse_task_probe_output(output: str) -> dict[int, str]:
    """Parse batched task probe lines of the form `<task_id>|<value>`."""
    probes: dict[int, str] = {}
    for line in output.splitlines():
        head, sep, value = line.partition("|")
        if not sep:
            continue
        try:
            task_id = int(head.strip())
        except ValueError:
            continue
        probes[task_id] = value.strip()
    return probes


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
            "probe_interval = 60",
            "def maybe_ramp():",
            "    global limit, last_ramp",
            "    now = time.time()",
            "    if now - last_ramp < probe_interval:",
            "        return",
            "    last_ramp = now",
            "    load1, load5, load15 = os.getloadavg()",
            "    free_mem = mem_available_gb()",
            "    # Simulations grow into their CPU/RAM late (mesh refinement), so",
            "    # reserve the full declared footprint for young workers instead of",
            "    # trusting the instantaneous load/free-memory readings.",
            "    young = sum(1 for _sid, _proc, _log, started in running if now - started < ramp_interval)",
            "    adj_load = load5 + young * cpus_per_sim",
            "    adj_free = free_mem - young * mem_per_sim_gb * 1.25",
            "    # Size the step from actual headroom instead of +1 per interval,",
            "    # capped at doubling per probe so load5 lag cannot over-launch.",
            "    cpu_headroom = (allocated_cpus * load_target - adj_load) / max(1, cpus_per_sim)",
            "    mem_headroom = (adj_free - mem_per_sim_gb * 1.25) / max(0.1, mem_per_sim_gb * 1.25)",
            "    step = int(min(cpu_headroom, mem_headroom, max(1, limit)))",
            "    if step > 0 and limit < max_limit:",
            "        limit = min(max_limit, limit + step)",
            "        print(f'[adaptive] increased worker limit to {limit}; load5={load5:.2f}; free_mem_gb={free_mem:.1f}; young={young}', flush=True)",
            "    elif limit > 1 and (load5 > allocated_cpus * max(1.0, load_target) * 1.3 or free_mem < mem_per_sim_gb):",
            "        limit = max(1, limit - 1)",
            "        print(f'[adaptive] reduced worker limit to {limit}; load5={load5:.2f}; free_mem_gb={free_mem:.1f}', flush=True)",
            "def launch(sim_id):",
            "    env = os.environ.copy()",
            "    env['SIMULATION_ID'] = str(sim_id)",
            "    env['OMP_NUM_THREADS'] = str(cpus_per_sim)",
            "    env['MKL_NUM_THREADS'] = str(cpus_per_sim)",
            "    log = open(f'./simul_log/{os.environ.get(\"SLURM_JOB_ID\", \"local\")}_{sim_id}.log', 'w', encoding='utf-8')",
            "    proc = subprocess.Popen(command, shell=True, stdout=log, stderr=subprocess.STDOUT, env=env)",
            "    running.append((sim_id, proc, log, time.time()))",
            "    print(f'[adaptive] started simulation {sim_id}; running={len(running)} limit={limit}', flush=True)",
            "while pending or running:",
            "    while pending and len(running) < limit:",
            "        launch(pending.pop(0))",
            "        time.sleep(2)",
            "    still = []",
            "    for sim_id, proc, log, started in running:",
            "        rc = proc.poll()",
            "        if rc is None:",
            "            still.append((sim_id, proc, log, started))",
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
    if task_attach_uses_overlap(task):
        srun_parts.append("--overlap")
    else:
        srun_parts.append("--exclusive")
    srun_parts.extend(["bash", shell_expandable_path(script_path)])
    srun_command = " ".join(srun_parts)
    return (
        f"{srun_command} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}; "
        f"echo $? > {shlex.quote(exit_code_path)}"
    )


def task_attach_uses_overlap(task: dict) -> bool:
    same_node_cpu_client = (
        int(task.get("same_node_as_task_id") or task.get("same_node_as") or 0) > 0
        and int(task.get("gpus") or 0) == 0
        and int(task.get("cpus") or 0) <= 4
    )
    fea_bursty_task = normalize_scheduling_profile(str(task.get("scheduling_profile") or "")) == SchedulingProfile.FEA_BURSTY.value
    return fea_bursty_task or same_node_cpu_client or task_is_vllm_service(task)


def task_is_vllm_service(task: dict) -> bool:
    if int(task.get("exclusive_node") or 0):
        return False
    if int(task.get("gpus") or 0) <= 0:
        return False
    name = str(task.get("name") or "").lower()
    command = str(task.get("command") or "").lower()
    if "vllm-service" in name:
        return True
    return "vllm" in name and "service_duration_seconds" in command


def background_wrapper_command(wrapper: str, wrapper_path: str) -> str:
    return f"nohup setsid bash -lc {shlex.quote(wrapper)} > {shlex.quote(wrapper_path)} 2>&1 & echo $!"


def workspace_runs_dir(workspace: str, stamp: int) -> str:
    """Run artifacts (task-/job-/allocation-*) live under <workspace>/runs/<date>/
    to keep the workspace organized and prunable by day, kept separate from the
    project trees under <workspace>/projects/."""
    date = time.strftime("%Y-%m-%d", time.localtime(stamp))
    return posixpath.join(workspace, "runs", date)


def cancel_process_group_command(wrapper_pid: str, term_grace_seconds: int = 3) -> str:
    pid = shlex.quote(str(wrapper_pid).strip())
    grace = max(0, int(term_grace_seconds))
    return (
        f"pid={pid}; "
        'if [ -n "$pid" ]; then '
        'kill -TERM -- "-$pid" >/dev/null 2>&1 || true; '
        'pkill -TERM -P "$pid" >/dev/null 2>&1 || true; '
        'kill -TERM "$pid" >/dev/null 2>&1 || true; '
        f"sleep {grace}; "
        'kill -KILL -- "-$pid" >/dev/null 2>&1 || true; '
        'pkill -KILL -P "$pid" >/dev/null 2>&1 || true; '
        'kill -KILL "$pid" >/dev/null 2>&1 || true; '
        "fi"
    )


class SlurmAccountClient:
    def __init__(
        self,
        account: AccountConfig,
        git_credentials: list[GitCredentialConfig] | None = None,
        git_source_accounts: list[AccountConfig] | None = None,
        *,
        command_timeout: float = DEFAULT_COMMAND_TIMEOUT,
        slow_command_timeout: float = SLOW_COMMAND_TIMEOUT,
    ):
        self.account = account
        self.git_credentials = git_credentials or []
        self.git_source_accounts = {item.name: item for item in (git_source_accounts or [])}
        self.command_timeout = command_timeout
        self.slow_command_timeout = slow_command_timeout
        self._shared_session: SSHSession | None = None

    def bind_shared_session(self, session: SSHSession) -> None:
        """Reuse one SSH session across calls (per scheduler tick). The owner
        of the session is responsible for closing it."""
        self._shared_session = session

    @contextmanager
    def _open_session(self) -> Iterator[SSHSession]:
        if self._shared_session is not None:
            self._shared_session.ensure_connected()
            yield self._shared_session
            return
        with SSHSession(self.account, default_timeout=self.command_timeout) as ssh:
            yield ssh

    def snapshot(self, storage_used_gb: float | None = None) -> AccountSnapshot:
        with self._open_session() as ssh:
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
        with self._open_session() as ssh:
            du = ssh.run(
                f"mkdir -p {shlex.quote(storage_path)} && du -sk {shlex.quote(storage_path)}",
                timeout=self.slow_command_timeout,
            )
        if du.exit_code == 0 and du.stdout.strip():
            return parse_du_gb(du.stdout)
        return None

    def submit(self, job: dict) -> dict[str, str]:
        stamp = int(time.time())
        remote_job_dir = posixpath.join(
            workspace_runs_dir(self.account.remote_workspace, stamp), f"job-{job['id']}-{stamp}"
        )
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
        with self._open_session() as ssh:
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
                result = ssh.run(command, timeout=self.slow_command_timeout)
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
        stamp = int(time.time())
        remote_dir = posixpath.join(
            workspace_runs_dir(self.account.remote_workspace, stamp), f"allocation-{allocation['id']}-{stamp}"
        )
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
        with self._open_session() as ssh:
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
        remote_dir = posixpath.join(
            workspace_runs_dir(self.account.remote_workspace, stamp), f"task-{task['id']}-{stamp}"
        )
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
            f"bash -lc {shlex.quote(background_wrapper_command(wrapper, wrapper_path))}",
        ]
        with self._open_session() as ssh:
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
        with self._open_session() as ssh:
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
        with self._open_session() as ssh:
            result = ssh.run(command)
            if result.exit_code == 0 and result.stdout.strip():
                return map_slurm_state(result.stdout.splitlines()[0].strip())
            sacct = ssh.run(f"sacct -n -j {shlex.quote(slurm_job_id)} -X -o State")
        if sacct.exit_code == 0 and sacct.stdout.strip():
            return map_slurm_state(sacct.stdout.splitlines()[0].strip().split()[0])
        return JobStatus.SUBMITTED

    def job_states(self, slurm_job_ids: list[str]) -> dict[str, JobStateInfo]:
        """Resolve state/reason/node for many jobs with one squeue and chunked sacct.

        squeue is user-scoped rather than `-j id,...` because one purged id makes
        squeue fail for the whole list on common Slurm versions. Jobs missing from
        both squeue and sacct stay SUBMITTED, matching state().
        """
        ids = [str(job_id).strip() for job_id in slurm_job_ids if str(job_id or "").strip()]
        if not ids:
            return {}
        out: dict[str, JobStateInfo] = {}
        with self._open_session() as ssh:
            result = ssh.run('squeue -h -u "$USER" -o "%i|%T|%R|%N|%P"')
            table = parse_squeue_table(result.stdout) if result.exit_code == 0 else {}
            for job_id in ids:
                row = table.get(job_id)
                if not row:
                    continue
                state, reason, node, partition = row
                out[job_id] = JobStateInfo(
                    status=map_slurm_state(state),
                    raw_state=state,
                    pending_reason=reason if state in {"PD", "PENDING"} else "",
                    node_name=node,
                    partition=partition,
                )
            missing = [job_id for job_id in ids if job_id not in out]
            for index in range(0, len(missing), 100):
                chunk = missing[index : index + 100]
                sacct = ssh.run(
                    "sacct -n -X -o JobID,State -j " + ",".join(shlex.quote(job_id) for job_id in chunk)
                )
                if sacct.exit_code != 0:
                    continue
                states = parse_sacct_states(sacct.stdout)
                for job_id in chunk:
                    if job_id in states:
                        out[job_id] = JobStateInfo(status=map_slurm_state(states[job_id]), raw_state=states[job_id])
        for job_id in ids:
            out.setdefault(job_id, JobStateInfo(status=JobStatus.SUBMITTED))
        return out

    def task_probes(self, tasks: list[dict]) -> dict[int, TaskProbe]:
        """Batched task_state + task_exit_code: one shell command per 50 tasks."""
        out: dict[int, TaskProbe] = {}
        remote: list[dict] = []
        for task in tasks:
            task_id = int(task["id"])
            if not str(task.get("exit_code_path") or ""):
                out[task_id] = TaskProbe(status=JobStatus.RUNNING)
            else:
                remote.append(task)
        if not remote:
            return out
        with self._open_session() as ssh:
            for index in range(0, len(remote), 50):
                chunk = remote[index : index + 50]
                pieces = []
                for task in chunk:
                    task_id = int(task["id"])
                    exit_path = shlex.quote(str(task["exit_code_path"]))
                    piece = f"printf '%s|' {task_id}; if test -f {exit_path}; then head -n1 -- {exit_path}"
                    wrapper_pid = str(task.get("wrapper_pid") or "").strip()
                    if wrapper_pid:
                        piece += f"; elif ps -p {shlex.quote(wrapper_pid)} >/dev/null 2>&1; then echo RUNNING"
                    piece += "; else echo UNKNOWN; fi; echo"
                    pieces.append(piece)
                result = ssh.run("; ".join(pieces))
                values = parse_task_probe_output(result.stdout) if result.exit_code == 0 else {}
                for task in chunk:
                    task_id = int(task["id"])
                    out[task_id] = _task_probe_from_value(values.get(task_id, "UNKNOWN"))
        return out

    def pending_reason(self, slurm_job_id: str) -> str:
        command = f"squeue -h -j {shlex.quote(slurm_job_id)} -o \"%R\""
        with self._open_session() as ssh:
            result = ssh.run(command)
        if result.exit_code != 0 or not result.stdout.strip():
            return ""
        return result.stdout.strip().splitlines()[0].strip()

    def allocation_node_name(self, slurm_job_id: str) -> str:
        command = f"squeue -h -j {shlex.quote(slurm_job_id)} -o \"%N\""
        with self._open_session() as ssh:
            result = ssh.run(command)
        if result.exit_code != 0 or not result.stdout.strip():
            return ""
        node_name = result.stdout.strip().splitlines()[0].strip()
        if node_name in {"(None)", "N/A", "None"}:
            return ""
        return node_name

    def cancel(self, slurm_job_id: str) -> None:
        with self._open_session() as ssh:
            result = ssh.run(f"scancel {shlex.quote(slurm_job_id)}", timeout=15)
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or "scancel failed")

    def cancel_task(self, task: dict) -> None:
        wrapper_pid = str(task.get("wrapper_pid") or "").strip()
        if not wrapper_pid:
            return
        command = cancel_process_group_command(wrapper_pid)
        with self._open_session() as ssh:
            result = ssh.run(command, timeout=15)
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "task cancel failed")

    def remove_tree(self, remote_path: str) -> None:
        with self._open_session() as ssh:
            result = ssh.run(f"rm -rf -- {shlex.quote(remote_path)}", timeout=120)
        if result.exit_code != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"failed to remove {remote_path}")

    def remove_trees(self, remote_paths: list[str]) -> None:
        paths = [str(path) for path in remote_paths if str(path or "").strip()]
        if not paths:
            return
        with self._open_session() as ssh:
            for index in range(0, len(paths), 100):
                chunk = paths[index : index + 100]
                result = ssh.run("rm -rf -- " + " ".join(shlex.quote(path) for path in chunk), timeout=120)
                if result.exit_code != 0:
                    message = result.stderr.strip() or result.stdout.strip() or "failed to remove remote artifacts"
                    raise RuntimeError(message)

    def read_text_file(
        self,
        path: str,
        tail_lines: int = 0,
        max_bytes: int = 0,
        timeout: float | None = None,
    ) -> str:
        with self._open_session() as ssh:
            result = ssh.run(
                remote_text_command(path, tail_lines=max(0, tail_lines), max_bytes=max(0, max_bytes)),
                timeout=timeout if timeout is not None else _UNSET,
            )
        if result.exit_code != 0:
            raise FileNotFoundError(result.stderr.strip() or f"remote file not found: {path}")
        return result.stdout

    def list_files(self, root: str, pattern: str, timeout: float | None = None) -> list[str]:
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
        with self._open_session() as ssh:
            result = ssh.run(script, timeout=timeout if timeout is not None else _UNSET)
        if result.exit_code != 0:
            raise FileNotFoundError(result.stderr.strip() or f"remote files not found: {root}/{pattern}")
        try:
            loaded = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            return []
        return [str(item) for item in loaded]
