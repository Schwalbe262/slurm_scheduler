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
    if int(job.get("gpus") or 0) > 0:
        lines.append(f"#SBATCH --gres=gpu:{int(job['gpus'])}")
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
    if int(job.get("gpus") or 0) > 0:
        lines.append(f"#SBATCH --gres=gpu:{int(job['gpus'])}")
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


class SlurmAccountClient:
    def __init__(self, account: AccountConfig):
        self.account = account

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
        script = build_sbatch_script(job, remote_job_dir)
        quoted_script = shlex.quote(script)
        if job.get("job_mode") == "packed_srun":
            commands = [
                f"mkdir -p {shlex.quote(remote_job_dir)}",
                f"printf %s {quoted_script} > {shlex.quote(posixpath.join(remote_job_dir, 'run.sbatch'))}",
                f"cd {shlex.quote(remote_job_dir)} && sbatch run.sbatch",
            ]
        else:
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

    def read_text_file(self, path: str) -> str:
        with SSHSession(self.account) as ssh:
            result = ssh.run(f"test -f {shlex.quote(path)} && cat {shlex.quote(path)}")
        if result.exit_code != 0:
            raise FileNotFoundError(result.stderr.strip() or f"remote file not found: {path}")
        return result.stdout
