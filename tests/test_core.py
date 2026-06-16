from __future__ import annotations

import tempfile
import unittest

from slurm_scheduler.config import AccountConfig
from slurm_scheduler.conda_sync import conda_bootstrap, env_prefix_lookup_command
from slurm_scheduler.db import Database
from slurm_scheduler.models import AccountSnapshot, AllocationStatus, JobCreate, JobStatus, TaskCreate, TaskStatus
from slurm_scheduler.scheduler import Scheduler
from slurm_scheduler.inventory import parse_scontrol_nodes, parse_sinfo_nodes, partition_rank
from slurm_scheduler.pestat import parse_pestat, plan_dynamic_allocations
from slurm_scheduler.task_commands import ACCOUNT_WORKSPACE_PLACEHOLDER, TASK_ID_PLACEHOLDER, build_git_task_command
from slurm_scheduler.slurm import (
    RemoteExecutionError,
    apply_env_profile,
    build_allocation_script,
    build_sbatch_script,
    build_srun_attach_command,
    build_task_script,
    parse_du_gb,
    parse_sbatch_job_id,
    parse_squeue_counts,
    remote_execution_path,
    resolve_task_placeholders,
)


class SlurmParsingTests(unittest.TestCase):
    def test_parse_squeue_counts(self) -> None:
        self.assertEqual(parse_squeue_counts("RUNNING\nPENDING\nR\nPD\nCOMPLETED\n"), (2, 2))

    def test_parse_sbatch_job_id(self) -> None:
        self.assertEqual(parse_sbatch_job_id("Submitted batch job 12345\n"), "12345")

    def test_parse_du_gb(self) -> None:
        self.assertAlmostEqual(parse_du_gb("1048576\t/path\n"), 1.0)

    def test_build_sbatch_script(self) -> None:
        job = {
            "job_name": "sleep-test",
            "time_limit": "00:10:00",
            "cpus": 4,
            "memory": "1G",
            "partition": "",
            "gpus": 0,
            "entrypoint": "scripts/run.py",
            "arguments": "--x 1",
            "env_setup": "module load python",
        }
        script = build_sbatch_script(job, "/tmp/job-1")
        self.assertIn("#SBATCH --cpus-per-task=4", script)
        self.assertIn("module load python", script)
        self.assertIn("python scripts/run.py --x 1", script)
        self.assertLess(script.index("#SBATCH --job-name"), script.index("set -euo pipefail"))

    def test_build_sbatch_script_with_relative_remote_dir(self) -> None:
        job = {
            "job_name": "relative-test",
            "time_limit": "00:10:00",
            "cpus": 4,
            "memory": "1G",
            "partition": "",
            "gpus": 0,
            "entrypoint": "run.py",
            "arguments": "",
            "env_setup": "",
        }
        script = build_sbatch_script(job, "slurm_scheduler/job-1")
        self.assertIn("#SBATCH --output=slurm-%j.out", script)
        self.assertIn("cd repo", script)

    def test_build_sbatch_script_with_gpu_model(self) -> None:
        job = {
            "job_name": "gpu-test",
            "time_limit": "00:10:00",
            "cpus": 8,
            "memory": "16G",
            "partition": "gpu3",
            "node_name": "gpu-node",
            "gpus": 1,
            "gpu_model": "a6000ada",
            "exclusive_node": 1,
            "entrypoint": "run.py",
            "arguments": "",
            "env_setup": "",
        }
        script = build_sbatch_script(job, "/tmp/job-1")
        self.assertIn("#SBATCH --gres=gpu:a6000ada:1", script)
        self.assertIn("#SBATCH --nodelist=gpu-node", script)
        self.assertIn("#SBATCH --exclusive", script)

    def test_build_packed_script_uses_adaptive_manager(self) -> None:
        job = {
            "job_mode": "packed_srun",
            "job_name": "packed",
            "time_limit": "12:00:00",
            "cpus": 44,
            "memory": "128G",
            "partition": "cpu1",
            "gpus": 0,
            "entrypoint": "run_simulation.py",
            "arguments": "",
            "env_setup": "module load app",
            "remote_path": "~/project",
            "simulation_count": 16,
            "simulation_start": 1,
            "cpus_per_simulation": 4,
            "initial_workers": 11,
            "max_workers_per_job": 16,
            "mem_per_simulation_gb": 8,
            "load_target": 0.75,
            "ramp_interval_seconds": 900,
        }
        script = build_sbatch_script(job, "slurm_scheduler/job-1")
        self.assertIn("#SBATCH --ntasks=1", script)
        self.assertIn("#SBATCH --cpus-per-task=44", script)
        self.assertIn("initial_limit = 11", script)
        self.assertIn("max_limit = 16", script)
        self.assertIn("[adaptive] increased worker limit", script)
        self.assertLess(script.index("#SBATCH --job-name"), script.index("set -euo pipefail"))

    def test_build_packed_script_includes_requested_node(self) -> None:
        job = {
            "job_mode": "packed_srun",
            "job_name": "packed",
            "time_limit": "12:00:00",
            "cpus": 4,
            "memory": "8G",
            "partition": "cpu2",
            "gpus": 0,
            "entrypoint": "run.py",
            "arguments": "",
            "env_setup": "",
            "remote_path": "~/project",
            "simulation_count": 1,
            "simulation_start": 1,
            "cpus_per_simulation": 4,
            "initial_workers": 1,
            "max_workers_per_job": 1,
            "node_name": "n110",
        }
        script = build_sbatch_script(job, "slurm_scheduler/job-1")
        self.assertIn("#SBATCH --partition=cpu2", script)
        self.assertIn("#SBATCH --nodelist=n110", script)

    def test_allocation_script_is_single_node_pool_job(self) -> None:
        allocation = {
            "id": 12,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/allocation-%j.out",
            "stderr_path": "/remote/allocation/allocation-%j.err",
            "total_cpus": 32,
            "total_memory_mb": 65536,
            "partition": "cpu2",
            "node_name": "n100",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --job-name=pool", script)
        self.assertNotIn("pool-12", script)
        self.assertIn("#SBATCH --nodes=1", script)
        self.assertIn("#SBATCH --ntasks=1", script)
        self.assertIn("#SBATCH --cpus-per-task=32", script)
        self.assertIn("#SBATCH --nodelist=n100", script)
        self.assertIn("while true; do sleep 60", script)

    def test_allocation_script_can_request_gpu_model(self) -> None:
        allocation = {
            "id": 13,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/allocation-%j.out",
            "stderr_path": "/remote/allocation/allocation-%j.err",
            "total_cpus": 48,
            "total_memory_mb": 65536,
            "total_gpus": 1,
            "gpu_model": "a6000ada",
            "partition": "gpu3",
            "node_name": "gpu-node",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --gres=gpu:a6000ada:1", script)

    def test_apply_env_profile_prepends_account_setup(self) -> None:
        account = AccountConfig(
            "a",
            "host",
            22,
            "a",
            "key",
            "/work",
            env_profiles={"pyaedt": "source ~/miniconda3/etc/profile.d/conda.sh\nconda activate pyaedt"},
        )
        payload = apply_env_profile({"env_profile": "pyaedt", "env_setup": "module load ansys"}, account)
        self.assertIn("conda activate pyaedt\nmodule load ansys", payload["env_setup"])

    def test_git_task_command_uses_account_workspace_placeholder(self) -> None:
        command = build_git_task_command("git@example.com:repo.git", "main", "run.py", "--x 1")
        self.assertIn(ACCOUNT_WORKSPACE_PLACEHOLDER, command)
        self.assertIn(TASK_ID_PLACEHOLDER, command)
        self.assertIn("git clone git@example.com:repo.git", command)
        self.assertIn("git checkout main", command)
        self.assertIn("python run.py --x 1", command)
        account = AccountConfig("a", "host", 22, "a", "key", "~/scheduler")
        task = resolve_task_placeholders({"id": 42, "remote_cwd": ACCOUNT_WORKSPACE_PLACEHOLDER, "command": command}, account)
        self.assertEqual(task["remote_cwd"], "~/scheduler")
        self.assertIn("$HOME/scheduler/git_tasks", task["command"])
        self.assertIn("task-42", task["command"])

    def test_task_script_uses_remote_cwd_and_command(self) -> None:
        task = {
            "remote_cwd": "~/case",
            "env_setup": "module load ansys",
            "command": "ansys -b -i input.dat",
        }
        script = build_task_script(task)
        self.assertIn("cd $HOME/case", script)
        self.assertIn("module load ansys", script)
        self.assertIn("ansys -b -i input.dat", script)

    def test_task_script_writes_payload_json(self) -> None:
        task = {
            "remote_cwd": "~/case",
            "env_setup": "",
            "command": "python run.py",
            "payload_json": '{"route":"ICN-SFO"}',
            "payload_path": "/remote/task-1/payload.json",
        }
        script = build_task_script(task)
        self.assertIn("SLURM_SCHEDULER_PAYLOAD_PATH=/remote/task-1/payload.json", script)
        self.assertIn("path.write_text", script)
        self.assertLess(script.index("path.write_text"), script.index("cd $HOME/case"))

    def test_srun_attach_command_targets_existing_allocation(self) -> None:
        task = {"cpus": 4, "memory_mb": 8192}
        allocation = {"slurm_job_id": "12345"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("srun --jobid=12345", command)
        self.assertIn("--nodes=1", command)
        self.assertIn("--ntasks=1", command)
        self.assertIn("--cpus-per-task=4", command)
        self.assertIn("--mem=8192M", command)
        self.assertIn("--exclusive", command)

    def test_srun_attach_command_overlaps_small_gpu_task_when_cpu_is_tight(self) -> None:
        task = {"cpus": 4, "memory_mb": 8192, "gpus": 1}
        allocation = {"slurm_job_id": "12345", "free_cpus": 0, "gpu_model": "a6000"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--gres=gpu:a6000:1", command)
        self.assertIn("--overlap", command)
        self.assertNotIn("--exclusive", command)

    def test_allocation_script_does_not_request_slurm_exclusive_for_scheduler_exclusive_pool(self) -> None:
        allocation = {
            "total_cpus": 12,
            "total_memory_mb": 98304,
            "partition": "cpu1",
            "node_name": "",
            "total_gpus": 0,
            "gpu_model": "",
            "exclusive_node": 1,
            "remote_dir": "/remote/allocation",
            "stdout_path": "/remote/allocation/out",
            "stderr_path": "/remote/allocation/err",
        }
        script = build_allocation_script(allocation, "48:00:00")
        self.assertIn("#SBATCH --cpus-per-task=12", script)
        self.assertNotIn("#SBATCH --exclusive", script)

    def test_remote_execution_path_promotes_relative_path_under_home(self) -> None:
        self.assertEqual(remote_execution_path("slurm_scheduler/task-1/task.sh"), "$HOME/slurm_scheduler/task-1/task.sh")
        self.assertEqual(remote_execution_path("~/slurm_scheduler/task-1/task.sh"), "$HOME/slurm_scheduler/task-1/task.sh")
        self.assertEqual(remote_execution_path("/tmp/task.sh"), "/tmp/task.sh")

    def test_srun_attach_command_can_request_gpu(self) -> None:
        task = {"cpus": 8, "memory_mb": 16384, "gpus": 1, "gpu_model": "a6000"}
        allocation = {"slurm_job_id": "12345", "gpu_model": "a6000"}
        command = build_srun_attach_command(
            task,
            allocation,
            "/remote/task.sh",
            "/remote/stdout.log",
            "/remote/stderr.log",
            "/remote/exit_code",
        )
        self.assertIn("--gres=gpu:a6000:1", command)

    def test_partition_rank_uses_cpu_and_gpu_profiles(self) -> None:
        nodes = parse_sinfo_nodes(
            "n040|cpu1|48|768000|(null)|idle\n"
            "n107|cpu2|256|1031519|(null)|mix\n"
            "n062|gpu3|56|1024000|gpu:a6000ada:4|mix\n"
            "n101|gpu5|64|1024000|gpu:a6000:4|mix\n"
        )
        rows = [node.__dict__ for node in nodes]
        self.assertEqual(partition_rank(rows, needs_gpu=False)[0]["partition"], "cpu2")
        self.assertEqual(partition_rank(rows, needs_gpu=True)[0]["partition"], "gpu3")

    def test_parse_scontrol_nodes_tracks_used_gpus(self) -> None:
        nodes = parse_scontrol_nodes(
            "NodeName=n062 Arch=x86_64 CPUTot=56 RealMemory=1024000 "
            "Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:3(IDX:0-2) "
            "State=MIXED Partitions=gpu3\n"
        )
        self.assertEqual(nodes[0].gpu_model, "a6000ada")
        self.assertEqual(nodes[0].gpu_count, 4)
        self.assertEqual(nodes[0].gpu_used_count, 3)

    def test_parse_scontrol_nodes_tracks_alloc_tres_gpus(self) -> None:
        nodes = parse_scontrol_nodes(
            "NodeName=n002 Arch=x86_64 CPUTot=48 RealMemory=768000 "
            "Gres=gpu:rtx3090:4 State=ALLOCATED Partitions=gpu1 "
            "CfgTRES=cpu=48,mem=750G,billing=48,gres/gpu=4,gres/gpu:rtx3090=4 "
            "AllocTRES=cpu=48,gres/gpu=2,gres/gpu:rtx3090=2\n"
        )
        self.assertEqual(nodes[0].gpu_model, "rtx3090")
        self.assertEqual(nodes[0].gpu_count, 4)
        self.assertEqual(nodes[0].gpu_used_count, 2)

    def test_parse_pestat_and_dynamic_plan(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "n001 cpu1 idle 0 48 0.25 768000 700000\n"
            "n002 cpu1 mix 40 48 42.0 768000 700000 1 user\n"
        )
        self.assertEqual(len(nodes), 2)
        plans = plan_dynamic_allocations(
            nodes,
            total_simulations=20,
            cpus_per_simulation=4,
            mem_per_simulation_gb=8,
            max_workers_per_allocation=32,
            max_allocations=2,
            partition="auto",
        )
        self.assertEqual(plans[0].node_name, "n001")
        self.assertEqual(plans[0].initial_workers, 11)
        self.assertEqual(plans[0].workers, 16)
        self.assertEqual(plans[0].total_cpus, 44)


class FakeClient:
    snapshots: dict[str, AccountSnapshot] = {}
    submitted: list[str] = []
    allocation_submits: list[str] = []
    allocation_states: dict[str, JobStatus] = {}
    task_states: dict[int, JobStatus] = {}
    attached_tasks: list[int] = []
    cancelled: list[str] = []
    cancelled_tasks: list[int] = []
    removed: list[str] = []

    def __init__(self, account: AccountConfig):
        self.account = account

    def snapshot(self, storage_used_gb: float | None = None) -> AccountSnapshot:
        return self.snapshots[self.account.name]

    def storage_used_gb(self) -> float | None:
        return None

    def submit(self, job: dict) -> dict[str, str]:
        self.submitted.append(self.account.name)
        return {
            "slurm_job_id": "777",
            "remote_job_dir": "/remote/job",
            "stdout_path": "/remote/job/slurm-777.out",
            "stderr_path": "/remote/job/slurm-777.err",
        }

    def submit_allocation(self, allocation: dict, time_limit: str) -> dict[str, str]:
        slurm_id = f"alloc-{allocation['id']}"
        self.allocation_submits.append(self.account.name)
        self.allocation_states[slurm_id] = JobStatus.RUNNING
        return {
            "slurm_job_id": slurm_id,
            "remote_dir": f"/remote/allocation-{allocation['id']}",
            "stdout_path": f"/remote/allocation-{allocation['id']}/out",
            "stderr_path": f"/remote/allocation-{allocation['id']}/err",
        }

    def attach_task(self, task: dict, allocation: dict) -> dict[str, str]:
        self.attached_tasks.append(int(task["id"]))
        self.task_states[task["id"]] = JobStatus.RUNNING
        return {
            "remote_dir": f"/remote/task-{task['id']}",
            "stdout_path": f"/remote/task-{task['id']}/stdout.log",
            "stderr_path": f"/remote/task-{task['id']}/stderr.log",
            "exit_code_path": f"/remote/task-{task['id']}/exit_code",
            "wrapper_pid": str(1000 + task["id"]),
        }

    def task_state(self, task: dict) -> JobStatus:
        return self.task_states.get(task["id"], JobStatus.RUNNING)

    def task_exit_code(self, task: dict) -> int | None:
        state = self.task_states.get(task["id"])
        if state == JobStatus.COMPLETED:
            return 0
        if state == JobStatus.FAILED:
            return 1
        return None

    def state(self, slurm_job_id: str) -> JobStatus:
        if slurm_job_id in self.allocation_states:
            return self.allocation_states[slurm_job_id]
        return JobStatus.COMPLETED

    def pending_reason(self, slurm_job_id: str) -> str:
        return self.pending_reasons.get(slurm_job_id, "")

    def cancel(self, slurm_job_id: str) -> None:
        self.cancelled.append(slurm_job_id)

    def cancel_task(self, task: dict) -> None:
        self.cancelled_tasks.append(int(task["id"]))

    def remove_tree(self, remote_path: str) -> None:
        self.removed.append(remote_path)


class AttachFailureClient(FakeClient):
    def attach_task(self, task: dict, allocation: dict) -> dict[str, str]:
        raise RemoteExecutionError(
            "ssh exec failed",
            {
                "remote_dir": f"/remote/task-{task['id']}",
                "stdout_path": f"/remote/task-{task['id']}/stdout.log",
                "stderr_path": f"/remote/task-{task['id']}/stderr.log",
                "exit_code_path": f"/remote/task-{task['id']}/exit_code",
            },
        )


class SubmitFailureClient(FakeClient):
    def submit(self, job: dict) -> dict[str, str]:
        raise RemoteExecutionError(
            "git clone failed",
            {
                "remote_job_dir": f"/remote/job-{job['id']}",
                "stdout_path": f"/remote/job-{job['id']}/submit.stdout.log",
                "stderr_path": f"/remote/job-{job['id']}/submit.stderr.log",
            },
        )


class PartialSnapshotFailureClient(FakeClient):
    def snapshot(self, storage_used_gb: float | None = None) -> AccountSnapshot:
        if self.account.name == "b":
            raise RuntimeError("squeue unavailable")
        return super().snapshot(storage_used_gb=storage_used_gb)


class SchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10),
            AccountConfig("b", "host", 22, "b", "key", "/work", 4, 10, 10),
        ]
        FakeClient.submitted = []
        FakeClient.allocation_submits = []
        FakeClient.allocation_states = {}
        FakeClient.pending_reasons = {}
        FakeClient.task_states = {}
        FakeClient.attached_tasks = []
        FakeClient.cancelled = []
        FakeClient.cancelled_tasks = []
        FakeClient.removed = []
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=3, pending=0, max_running=4, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=1, pending=1, max_running=4, max_pending=10, max_total=10),
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_choose_account_prefers_freer_account(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account().name, "b")

    def test_choose_account_respects_required_capability(self) -> None:
        accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10, capabilities=["conda:pyaedt"]),
            AccountConfig("b", "host", 22, "b", "key", "/work", 4, 10, 10),
        ]
        scheduler = Scheduler(self.db, accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account(required_capability="conda:pyaedt").name, "a")

    def test_choose_account_uses_synced_conda_overlay_capability(self) -> None:
        self.db.upsert_account_env_overlay("b", "pyaedt", "/work/miniconda3/envs/pyaedt", sync_job_id=1)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        account = scheduler.choose_account(required_capability="conda:pyaedt", env_profile="pyaedt")
        self.assertEqual(account.name, "b")

    def test_dynamic_env_profile_prepends_synced_overlay_setup(self) -> None:
        self.db.upsert_account_env_overlay("a", "pyaedt", "/work/miniconda3/envs/pyaedt", sync_job_id=1)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        payload = scheduler.apply_dynamic_env_profile({"env_profile": "pyaedt", "env_setup": "module load ansys"}, self.accounts[0])
        self.assertIn("conda activate pyaedt", payload["env_setup"])
        self.assertTrue(payload["env_setup"].endswith("module load ansys"))

    def test_conda_sync_command_helpers_reference_env_name(self) -> None:
        lookup = env_prefix_lookup_command("pyaedt")
        self.assertIn("conda env list --json", lookup)
        self.assertIn("pyaedt", lookup)
        self.assertIn("miniconda3", conda_bootstrap())

    def test_choose_account_respects_requested_account(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account(account_name="a").name, "a")

    def test_choose_account_accepts_ordered_account_candidates(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account(account_name="a,b").name, "a")
        self.assertEqual(scheduler.choose_account(account_name="b,a").name, "b")

    def test_submit_next_queued_job_updates_database(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.SUBMITTED.value)
        self.assertEqual(job["account_name"], "b")
        self.assertEqual(job["slurm_job_id"], "777")

    def test_submit_next_queued_job_uses_requested_account(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py", account_name="a"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.SUBMITTED.value)
        self.assertEqual(job["account_name"], "a")

    def test_submit_failure_keeps_remote_submit_log_paths(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py", account_name="a"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=SubmitFailureClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.FAILED.value)
        self.assertEqual(job["failure_message"], "git clone failed")
        self.assertEqual(job["remote_job_dir"], f"/remote/job-{job_id}")
        self.assertEqual(job["stdout_path"], f"/remote/job-{job_id}/submit.stdout.log")
        self.assertEqual(job["stderr_path"], f"/remote/job-{job_id}/submit.stderr.log")

    def test_choose_account_respects_total_job_limit(self) -> None:
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=5, pending=5, max_running=10, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=4, pending=5, max_running=10, max_pending=10, max_total=10),
        }
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account().name, "b")

    def test_token_usage_summary(self) -> None:
        self.db.create_token_usage("codex", "slurm_scheduler", input_tokens=10, output_tokens=5, reset_cycle="2026-W24")
        self.db.create_token_usage("codex", "slurm_scheduler", total_tokens=20, reset_cycle="2026-W24")
        summary = self.db.token_usage_summary()
        self.assertEqual(summary[0]["provider"], "codex")
        self.assertEqual(summary[0]["project"], "slurm_scheduler")
        self.assertEqual(summary[0]["total_tokens"], 35)

    def test_cleanup_removes_only_safe_finished_task_artifacts(self) -> None:
        safe_id = self.db.create_task(TaskCreate("safe", "/case", "run"))
        unsafe_id = self.db.create_task(TaskCreate("unsafe", "/case", "run"))
        self.db.update_task(
            safe_id,
            status=TaskStatus.COMPLETED.value,
            account_name="a",
            remote_dir="/work/task-1-1000",
            stdout_path="/work/task-1-1000/stdout.log",
            stderr_path="/work/task-1-1000/stderr.log",
            exit_code_path="/work/task-1-1000/exit_code",
            wrapper_pid="123",
            finished_at="2000-01-01 00:00:00",
        )
        self.db.update_task(
            unsafe_id,
            status=TaskStatus.FAILED.value,
            account_name="a",
            remote_dir="/work/project",
            stdout_path="/work/project/stdout.log",
            stderr_path="/work/project/stderr.log",
            finished_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cleanup_interval_seconds=0,
            cleanup_finished_task_ttl_seconds=0,
        )
        scheduler.cleanup_remote_artifacts_if_due()
        self.assertEqual(FakeClient.removed, ["/work/task-1-1000"])
        self.assertEqual(self.db.get_task(safe_id)["remote_dir"], "")
        self.assertEqual(self.db.get_task(unsafe_id)["remote_dir"], "/work/project")

    def test_cleanup_removes_finished_job_and_closed_allocation_artifacts(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py", account_name="a"))
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_job(
            job_id,
            status=JobStatus.FAILED.value,
            remote_job_dir="/work/job-1-1000",
            stdout_path="/work/job-1-1000/out",
            stderr_path="/work/job-1-1000/err",
            finished_at="2000-01-01 00:00:00",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.CLOSED.value,
            remote_dir="/work/allocation-1-1000",
            stdout_path="/work/allocation-1-1000/out",
            stderr_path="/work/allocation-1-1000/err",
            closed_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cleanup_interval_seconds=0,
            cleanup_finished_job_ttl_seconds=0,
            cleanup_closed_allocation_ttl_seconds=0,
        )
        scheduler.cleanup_remote_artifacts_if_due()
        self.assertEqual(FakeClient.removed, ["/work/job-1-1000", "/work/allocation-1-1000"])
        self.assertEqual(self.db.get_job(job_id)["remote_job_dir"], "")
        self.assertEqual(self.db.get_allocation(allocation_id)["remote_dir"], "")

    def test_cancel_task_marks_queued_task_cancelled(self) -> None:
        task_id = self.db.create_task(TaskCreate("queued", "~/case", "run"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.cancel_task(task_id)
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(FakeClient.cancelled_tasks, [])

    def test_list_tasks_by_statuses_keeps_running_visible_independent_of_recent_rows(self) -> None:
        running_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(running_id, status=TaskStatus.RUNNING.value)
        for index in range(20):
            done_id = self.db.create_task(TaskCreate(f"done-{index}", "~/case", "run"))
            self.db.update_task(done_id, status=TaskStatus.FAILED.value)
        recent = self.db.list_tasks(limit=5)
        self.assertNotIn(running_id, [int(task["id"]) for task in recent])
        running = self.db.list_tasks_by_statuses([TaskStatus.RUNNING.value], limit=5000)
        self.assertEqual([int(task["id"]) for task in running], [running_id])
        self.assertEqual(self.db.count_tasks_by_statuses([TaskStatus.FAILED.value]), 20)

    def test_gpu_task_attaches_before_higher_priority_cpu_backlog(self) -> None:
        self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=64,
            total_memory_mb=65536,
        )
        gpu_allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu1",
            node_name="g001",
            total_cpus=3,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="a6000",
        )
        self.db.update_allocation(
            gpu_allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="gpu-alloc-1",
            free_cpus=3,
            free_memory_mb=65536,
            free_gpus=2,
        )
        cpu_task_id = self.db.create_task(TaskCreate("cpu-backlog", "~/cpu", "run", cpus=16, memory_mb=32768, priority=70))
        gpu_task_id = self.db.create_task(
            TaskCreate("gpu-work", "~/gpu", "run", cpus=3, memory_mb=32768, gpus=1, gpu_model="a6000")
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        self.assertEqual(FakeClient.attached_tasks[0], gpu_task_id)
        self.assertEqual(self.db.get_task(gpu_task_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(cpu_task_id)["status"], TaskStatus.QUEUED.value)

    def test_snapshot_failure_for_one_account_does_not_block_other_accounts(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=PartialSnapshotFailureClient)
        snapshots = scheduler.snapshots()
        self.assertEqual([snapshot.account_name for snapshot in snapshots], ["a"])

    def test_cancel_task_kills_running_wrapper(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
            wrapper_pid="1234",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.cancel_task(task_id)
        task = self.db.get_task(task_id)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(task["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(FakeClient.cancelled_tasks, [task_id])
        self.assertEqual(allocation["free_cpus"], allocation["total_cpus"])

    def test_request_cancel_task_returns_fast_status_shape(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("running", "~/case", "run"))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
            allocation_id=allocation_id,
            wrapper_pid="1234",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        response = scheduler.request_cancel_task(task_id)
        self.assertEqual(response["ok"], True)
        self.assertEqual(response["id"], task_id)
        self.assertEqual(response["previous_status"], TaskStatus.RUNNING.value)
        self.assertEqual(response["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(self.db.get_task(task_id)["status"], TaskStatus.CANCELLED.value)

    def test_cancel_tasks_filters_by_name_and_status(self) -> None:
        first = self.db.create_task(TaskCreate("crypto-sweep-a", "~/case", "run"))
        second = self.db.create_task(TaskCreate("crypto-sweep-b", "~/case", "run"))
        third = self.db.create_task(TaskCreate("other", "~/case", "run"))
        self.db.update_task(second, status=TaskStatus.COMPLETED.value, finished_at="2000-01-01 00:00:00")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        cancelled = scheduler.cancel_tasks(name_contains="crypto-sweep")
        self.assertEqual(cancelled, [first])
        self.assertEqual(self.db.get_task(first)["status"], TaskStatus.CANCELLED.value)
        self.assertEqual(self.db.get_task(second)["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(self.db.get_task(third)["status"], TaskStatus.QUEUED.value)

    def test_task_create_stores_api_operational_fields(self) -> None:
        task_id = self.db.create_task(
            TaskCreate(
                "flight",
                "~/flight",
                "python worker.py",
                required_capability="flight-crawl",
                priority=7,
                timeout_seconds=30,
                dedupe_key="flight:ICN:SFO",
                max_workers_per_node=200,
                payload_json='{"from":"ICN","to":"SFO"}',
            )
        )
        task = self.db.get_task(task_id)
        self.assertEqual(task["required_capability"], "flight-crawl")
        self.assertEqual(task["priority"], 7)
        self.assertEqual(task["timeout_seconds"], 30)
        self.assertEqual(task["dedupe_key"], "flight:ICN:SFO")
        self.assertEqual(task["max_workers_per_node"], 200)
        self.assertEqual(task["payload_json"], '{"from":"ICN","to":"SFO"}')
        self.assertEqual(self.db.find_active_task_by_dedupe_key("flight:ICN:SFO")["id"], task_id)

    def test_maintains_minimum_warm_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=4)
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 1)
        self.assertEqual(allocations[0]["state"], AllocationStatus.PENDING.value)
        self.assertEqual(FakeClient.allocation_submits, ["b"])

    def test_warm_pool_can_prefer_configured_account(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            warm_pool_preferred_accounts=["a"],
        )
        scheduler.maintain_allocation_pool()
        self.assertEqual(FakeClient.allocation_submits, ["a"])

    def test_pending_allocation_becomes_warm(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["state"], AllocationStatus.WARM.value)

    def test_pending_allocation_reason_is_recorded(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        FakeClient.allocation_states[allocation["slurm_job_id"]] = JobStatus.SUBMITTED
        FakeClient.pending_reasons[allocation["slurm_job_id"]] = "(Resources)"
        scheduler.refresh_allocations()
        allocation = self.db.get_allocation(allocation["id"])
        self.assertEqual(allocation["state"], AllocationStatus.PENDING.value)
        self.assertEqual(allocation["pending_reason"], "(Resources)")

    def test_stale_pending_gpu_allocation_is_cancelled_and_backed_off(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:0 State=IDLE Partitions=gpu3\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            allocation_pending_timeout_seconds=1,
            allocation_pending_backoff_seconds=3600,
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        FakeClient.allocation_states[allocation["slurm_job_id"]] = JobStatus.SUBMITTED
        self.db.update_allocation(
            allocation["id"],
            submitted_at="2000-01-01 00:00:00",
            pending_reason="(Resources)",
        )
        scheduler.apply_allocation_lifecycle()
        closed = self.db.get_allocation(allocation["id"])
        self.assertEqual(closed["state"], AllocationStatus.CLOSED.value)
        self.assertIn("pending timeout", closed["drain_reason"])
        self.assertEqual(FakeClient.cancelled, [allocation["slurm_job_id"]])

        scheduler.maintain_allocation_pool()
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["resource_pool"], "gpu:a6000")

    def test_assigns_task_to_warm_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        task_id = self.db.create_task(TaskCreate("ansys", "~/case", "ansys -b", cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        allocation = self.db.get_allocation(task["allocation_id"])
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(allocation["free_cpus"], 4)

    def test_gpu_task_accepts_ordered_gpu_model_candidates(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu4",
            node_name="gpu-a6000",
            total_cpus=16,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="gpu-job")
        task_id = self.db.create_task(
            TaskCreate("gpu-task", "~/case", "run", cpus=4, memory_mb=2048, gpus=1, gpu_model="a6000ada,a6000")
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.RUNNING.value)
        self.assertEqual(task["allocation_id"], allocation_id)

    def test_attach_failure_keeps_remote_log_paths(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=AttachFailureClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        task_id = self.db.create_task(TaskCreate("large-payload", "~/case", "x" * 900_000, cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.FAILED.value)
        self.assertEqual(task["failure_message"], "ssh exec failed")
        self.assertEqual(task["remote_dir"], f"/remote/task-{task_id}")
        self.assertEqual(task["stdout_path"], f"/remote/task-{task_id}/stdout.log")
        self.assertEqual(task["stderr_path"], f"/remote/task-{task_id}/stderr.log")

    def test_assign_queued_tasks_skips_blocked_head_task(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        blocked_id = self.db.create_task(
            TaskCreate("blocked-gpu", "~/case", "run-gpu", cpus=4, memory_mb=2048, gpus=1, gpu_model="a6000ada")
        )
        ready_id = self.db.create_task(TaskCreate("ready-cpu", "~/case", "run-cpu", cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        blocked = self.db.get_task(blocked_id)
        ready = self.db.get_task(ready_id)
        self.assertEqual(blocked["status"], TaskStatus.QUEUED.value)
        self.assertEqual(ready["status"], TaskStatus.RUNNING.value)

    def test_assign_queued_tasks_prefers_higher_priority(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=4)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        low_id = self.db.create_task(TaskCreate("low", "~/case", "run", cpus=4, memory_mb=2048, priority=0))
        high_id = self.db.create_task(TaskCreate("high", "~/case", "run", cpus=4, memory_mb=2048, priority=10))
        scheduler.assign_queued_tasks()
        self.assertEqual(self.db.get_task(high_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(low_id)["status"], TaskStatus.QUEUED.value)

    def test_max_workers_per_node_limits_allocation_concurrency(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        first_id = self.db.create_task(TaskCreate("first", "~/case", "run", cpus=1, memory_mb=512, max_workers_per_node=1))
        second_id = self.db.create_task(TaskCreate("second", "~/case", "run", cpus=1, memory_mb=512, max_workers_per_node=1))
        scheduler.assign_queued_tasks()
        self.assertEqual(self.db.get_task(first_id)["status"], TaskStatus.RUNNING.value)
        self.assertEqual(self.db.get_task(second_id)["status"], TaskStatus.QUEUED.value)

    def test_half_used_cpu_pool_prewarms_spare_allocation(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            allocation_scale_out_usage_threshold=0.50,
        )
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        self.db.create_task(TaskCreate("heavy", "~/case", "run", cpus=4, memory_mb=2048))
        scheduler.assign_queued_tasks()
        scheduler.maintain_allocation_pool()
        live = [
            item
            for item in self.db.list_allocations()
            if item["state"] in {AllocationStatus.PENDING.value, AllocationStatus.WARM.value, AllocationStatus.ACTIVE.value}
        ]
        self.assertEqual(len(live), 2)

    def test_pending_allocation_counts_as_spare_capacity(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            allocation_scale_out_usage_threshold=0.50,
        )
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        self.db.create_task(TaskCreate("heavy", "~/case", "run", cpus=6, memory_mb=2048))
        scheduler.assign_queued_tasks()
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)

    def test_queued_demand_does_not_duplicate_fitting_pending_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.db.create_task(TaskCreate("queued", "~/case", "run", cpus=4, memory_mb=2048))
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 1)
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 1)

    def test_exclusive_task_opens_exclusive_allocation(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=4, memory_mb=2048, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["exclusive_node"], 1)

    def test_multiple_exclusive_tasks_wait_when_one_allocation_is_pending(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=8,
        )
        self.db.create_task(TaskCreate("exclusive-1", "~/case", "run", cpus=4, memory_mb=2048, exclusive_node=True))
        self.db.create_task(TaskCreate("exclusive-2", "~/case", "run", cpus=4, memory_mb=2048, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        self.assertEqual(len(allocations), 1)
        self.assertTrue(all(allocation["exclusive_node"] == 1 for allocation in allocations))
        scheduler.refresh_allocations()
        scheduler.maintain_allocation_pool()
        self.assertEqual(len(self.db.list_allocations()), 2)

    def test_exclusive_demand_allocation_uses_task_size(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
        )
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=98304, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["exclusive_node"], 1)
        self.assertEqual(allocation["total_cpus"], 12)
        self.assertEqual(allocation["total_memory_mb"], 98304)

    def test_non_exclusive_cpu_demand_uses_largest_available_cpu_pool(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-big cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu-small cpu1 idle 0 48 0.0 768000 700000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            allocation_memory="0",
        )
        self.db.create_task(TaskCreate("cpu-backlog", "~/case", "run", cpus=16, memory_mb=32768))
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["exclusive_node"], 0)
        self.assertEqual(allocation["total_cpus"], 64)
        self.assertGreater(allocation["total_memory_mb"], 32768)

    def test_exclusive_cpu_demand_avoids_busy_single_job_partition(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-free cpu1 idle 0 48 0.0 768000 700000\n"
                "cpu2-free cpu2 idle 0 256 0.0 1031519 1000000\n"
                "gpu-free gpu3 mix 4 56 0.0 1024000 900000\n"
            )
        )
        existing_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="cpu2-used",
            total_cpus=64,
            total_memory_mb=65536,
            resource_pool="cpu",
        )
        self.db.update_allocation(existing_id, state=AllocationStatus.WARM.value, slurm_job_id="cpu2-pool")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
        )
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=98304, exclusive_node=True))
        scheduler.maintain_allocation_pool()
        allocations = [allocation for allocation in self.db.list_allocations() if allocation["id"] != existing_id]
        self.assertEqual(len(allocations), 1)
        self.assertEqual(allocations[0]["partition"], "cpu1")
        self.assertEqual(allocations[0]["total_cpus"], 12)

    def test_pending_demand_allocation_closes_when_no_queued_task_needs_it(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-demand",
            drain_reason="queued CPU demand",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("pending-demand", FakeClient.cancelled)

    def test_undersized_shared_cpu_demand_allocation_closes_when_larger_pool_is_available(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-big cpu2 idle 0 256 0.0 1031519 1000000\n"
                "cpu-small cpu1 idle 0 48 0.0 768000 700000\n"
            )
        )
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="",
            total_cpus=16,
            total_memory_mb=32768,
            resource_pool="cpu",
            exclusive_node=False,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="small-pending-demand",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("cpu-backlog", "~/case", "run", cpus=16, memory_mb=32768))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("small-pending-demand", FakeClient.cancelled)

    def test_warm_demand_allocation_closes_when_no_queued_task_needs_it(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="warm-demand",
            drain_reason="queued CPU demand",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("warm-demand", FakeClient.cancelled)

    def test_warm_demand_allocation_stays_when_queued_task_needs_it(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="warm-demand",
            drain_reason="queued CPU demand",
        )
        self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=98304, exclusive_node=True))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.scale_in_idle_allocations()
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(allocation["state"], AllocationStatus.WARM.value)
        self.assertEqual(FakeClient.cancelled, [])

    def test_exclusive_task_closes_allocation_after_finish(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=12,
            total_memory_mb=98304,
            resource_pool="cpu",
            exclusive_node=True,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="exclusive-alloc")
        task_id = self.db.create_task(TaskCreate("exclusive", "~/case", "run", cpus=12, memory_mb=2048, exclusive_node=True))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_id,
            account_name="a",
        )
        FakeClient.task_states[task_id] = JobStatus.COMPLETED
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, min_warm_allocations=0)
        scheduler.refresh_tasks()
        task = self.db.get_task(task_id)
        allocation = self.db.get_allocation(allocation_id)
        self.assertEqual(task["status"], TaskStatus.COMPLETED.value)
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertIn("exclusive-alloc", FakeClient.cancelled)

    def test_running_task_timeout_marks_failed_with_exit_code(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="",
            total_cpus=4,
            total_memory_mb=8192,
            resource_pool="cpu",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.ACTIVE.value, slurm_job_id="alloc-timeout")
        task_id = self.db.create_task(TaskCreate("timeout", "~/case", "run", timeout_seconds=1))
        self.db.update_task(
            task_id,
            status=TaskStatus.RUNNING.value,
            allocation_id=allocation_id,
            account_name="a",
            wrapper_pid="1234",
            started_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.refresh_tasks()
        task = self.db.get_task(task_id)
        self.assertEqual(task["status"], TaskStatus.FAILED.value)
        self.assertEqual(task["exit_code"], 124)
        self.assertIn("timed out", task["failure_message"])
        self.assertEqual(FakeClient.cancelled_tasks, [task_id])

    def test_task_required_capability_uses_matching_allocation_account(self) -> None:
        accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10, capabilities=["conda:pyaedt"]),
            AccountConfig("b", "host", 22, "b", "key", "/work", 4, 10, 10),
        ]
        allocation_id = self.db.create_allocation(
            account_name="b",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(
            TaskCreate(
                "ansys",
                "~/case",
                "run",
                required_capability="conda:pyaedt",
                cpus=4,
                memory_mb=2048,
            )
        )
        scheduler = Scheduler(self.db, accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.assertIsNone(scheduler.best_allocation_for_task(self.db.get_task(task_id)))
        scheduler.maintain_allocation_pool()
        self.assertEqual(FakeClient.allocation_submits, ["a"])

    def test_task_requested_account_uses_matching_allocation_account(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="b",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        task_id = self.db.create_task(TaskCreate("ansys", "~/case", "run", account_name="a", cpus=4, memory_mb=2048))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        self.assertIsNone(scheduler.best_allocation_for_task(self.db.get_task(task_id)))
        scheduler.maintain_allocation_pool()
        self.assertEqual(FakeClient.allocation_submits, ["a"])

    def test_drained_allocation_is_cancelled_when_empty(self) -> None:
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=8,
            allocation_drain_after_seconds=1,
        )
        scheduler.maintain_allocation_pool()
        scheduler.refresh_allocations()
        allocation = self.db.list_allocations()[0]
        self.db.update_allocation(allocation["id"], started_at="2000-01-01 00:00:00")
        scheduler.apply_allocation_lifecycle()
        allocation = self.db.get_allocation(allocation["id"])
        self.assertEqual(allocation["state"], AllocationStatus.CLOSED.value)
        self.assertEqual(FakeClient.cancelled, [allocation["slurm_job_id"]])

    def test_allocation_prewarm_respects_account_job_limit(self) -> None:
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=10, pending=0, max_running=10, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=10, pending=0, max_running=10, max_pending=10, max_total=10),
        }
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_cpus=8)
        scheduler.maintain_allocation_pool()
        self.assertEqual(self.db.list_allocations(), [])

    def test_allocation_shape_uses_pestat_and_gpu_reserve(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "gpu-node gpu3 idle 0 56 0.0 1024000 900000\n"
            "cpu-node cpu2 idle 0 128 0.0 1031519 800000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_partition="gpu3",
            gpu_cpu_reserve=4,
        )
        shape = scheduler.choose_allocation_shape()
        self.assertEqual(shape["partition"], "gpu3")
        self.assertEqual(shape["node_name"], "")
        self.assertEqual(shape["cpus"], 52)

    def test_allocation_shape_prefers_larger_pestat_capacity(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "small cpu2 idle 0 32 0.0 128000 120000\n"
            "large cpu2 idle 0 128 0.0 512000 500000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_partition="cpu2")
        shape = scheduler.choose_allocation_shape()
        self.assertEqual(shape["node_name"], "large")
        self.assertEqual(shape["cpus"], 64)

    def test_shared_cpu_pool_can_use_mixed_single_job_partition_capacity(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "mixed-cpu cpu2 mix 4 256 0.0 1031519 1000000\n"
            "idle-gpu gpu3 mix 4 56 0.0 876000 800000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, allocation_partition="auto")
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "cpu2")
        self.assertEqual(shape["node_name"], "mixed-cpu")
        self.assertEqual(shape["cpus"], 64)

    def test_cpu_pool_can_use_gpu_partition_when_no_cpu_candidate_exists(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=cpu-old CPUTot=48 RealMemory=768000 Gres=(null) State=IDLE Partitions=cpu1\n"
            "NodeName=gpu-fast CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-fast gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "gpu5")
        self.assertEqual(shape["node_name"], "")
        self.assertEqual(shape["gpus"], 0)

    def test_cpu_pool_can_disable_gpu_partition_candidates(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=cpu-old CPUTot=48 RealMemory=768000 Gres=(null) State=IDLE Partitions=cpu1\n"
            "NodeName=gpu-fast CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "cpu-old cpu1 idle 0 48 0.0 768000 700000\n"
                "gpu-fast gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            cpu_pool_allow_gpu_partitions=False,
        )
        shape = scheduler.choose_allocation_shape(resource_pool="cpu")
        self.assertEqual(shape["partition"], "cpu1")

    def test_cpu2_single_job_partition_skips_occupied_node(self) -> None:
        nodes = parse_pestat(
            "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
            "busy cpu2 mix 64 128 64.0 512000 100000 busy_job\n"
            "free cpu2 idle 0 128 0.0 512000 500000\n"
        )
        self.db.replace_pestat_nodes(nodes)
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu2",
            node_name="busy",
            total_cpus=64,
            total_memory_mb=65536,
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_partition="cpu2",
            single_job_per_node_partitions=["cpu2"],
        )
        shape = scheduler.choose_allocation_shape()
        self.assertEqual(shape["node_name"], "free")

    def test_cpu2_direct_job_gets_free_node(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "busy cpu2 mix 64 128 64.0 512000 100000 busy_job\n"
                "free cpu2 idle 0 128 0.0 512000 500000\n"
            )
        )
        job_id = self.db.create_job(JobCreate("", "", "run.py", job_mode="packed_srun", partition="cpu2"))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            single_job_per_node_partitions=["cpu2"],
        )
        job = self.db.get_job(job_id)
        self.assertTrue(scheduler.prepare_single_job_node(job))
        self.assertEqual(self.db.get_job(job_id)["node_name"], "free")

    def test_cpu2_duplicate_queued_jobs_do_not_deadlock_before_first_submit(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "free cpu2 idle 0 128 0.0 512000 500000\n"
            )
        )
        first = self.db.create_job(JobCreate("", "", "run.py", job_mode="packed_srun", partition="cpu2", node_name="free"))
        self.db.create_job(JobCreate("", "", "run.py", job_mode="packed_srun", partition="cpu2", node_name="free"))
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            single_job_per_node_partitions=["cpu2"],
        )
        self.assertTrue(scheduler.prepare_single_job_node(self.db.get_job(first)))

    def test_gpu_prewarm_prefers_a6000ada(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:2(IDX:0-1) State=MIXED Partitions=gpu3\n"
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 mix 8 64 4.0 1024000 900000\n"
                "gpu-a6000 gpu5 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000ada", "a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocations = self.db.list_allocations()
        gpu_allocations = [item for item in allocations if item["resource_pool"].startswith("gpu:")]
        self.assertEqual(len(gpu_allocations), 1)
        self.assertEqual(gpu_allocations[0]["gpu_model"], "a6000ada")
        self.assertEqual(gpu_allocations[0]["total_gpus"], 2)

    def test_gpu_prewarm_uses_a6000_node_with_only_four_free_cpus(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=64 RealMemory=1024000 Gres=gpu:a6000:4 GresUsed=gpu:a6000:2(IDX:0-1) State=MIXED Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 mix 60 64 10.0 1024000 900000 busy_job\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["total_cpus"], 4)
        self.assertEqual(allocation["total_gpus"], 2)

    def test_gpu_fallback_caps_cpu_request_to_partition_node_capacity(self) -> None:
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 mix 4 56 0.0 876000 800000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_partition="auto",
            gpu_prewarm_gpus_per_allocation=2,
        )
        shape = scheduler.choose_allocation_shape(resource_pool="gpu:a6000ada", gpu_model="a6000ada", gpus=2)
        self.assertEqual(shape["partition"], "gpu3")
        self.assertLessEqual(shape["cpus"], 56)

    def test_gpu_prewarm_leaves_cpu_for_unclaimed_gpus(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-a6000 CPUTot=48 RealMemory=687626 Gres=gpu:a6000:4 GresUsed=gpu:a6000:0 State=IDLE Partitions=gpu5\n"
        )
        self.db.replace_node_inventory(inventory)
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-a6000 gpu5 idle 0 48 0.0 687626 680000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            allocation_cpus=64,
            gpu_cpu_reserve=4,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_preferred_models=["a6000"],
        )
        scheduler.maintain_allocation_pool()
        allocation = self.db.list_allocations()[0]
        self.assertEqual(allocation["resource_pool"], "gpu:a6000")
        self.assertEqual(allocation["total_gpus"], 2)
        self.assertEqual(allocation["total_cpus"], 44)

    def test_gpu_prewarm_opens_lower_fallback_when_preferred_is_only_pending(self) -> None:
        for model in ["a6000ada", "a6000"]:
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="gpu3",
                node_name=f"{model}-pending",
                total_cpus=32,
                total_memory_mb=65536,
                total_gpus=2,
                gpu_model=model,
                resource_pool=f"gpu:{model}",
            )
            self.db.update_allocation(allocation_id, state=AllocationStatus.PENDING.value, slurm_job_id=f"{model}-job")
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=gpu-rtx CPUTot=64 RealMemory=1024000 Gres=gpu:rtx3090:4 GresUsed=gpu:rtx3090:0 State=IDLE Partitions=gpu6\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-rtx gpu6 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000ada", "a6000"],
        )
        scheduler.maintain_allocation_pool()
        gpu_allocations = [item for item in self.db.list_allocations() if item["resource_pool"].startswith("gpu:")]
        self.assertEqual(len(gpu_allocations), 3)
        self.assertTrue(any(item["gpu_model"] == "rtx3090" for item in gpu_allocations))

    def test_gpu_prewarm_keeps_preferred_queue_when_fallback_is_ready(self) -> None:
        fallback_id = self.db.create_allocation(
            account_name="a",
            partition="gpu6",
            node_name="gpu-rtx",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="rtx3090",
            resource_pool="gpu:rtx3090",
        )
        self.db.update_allocation(fallback_id, state=AllocationStatus.WARM.value, slurm_job_id="rtx-job")
        self.db.replace_node_inventory(
            parse_scontrol_nodes(
                "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:0 State=IDLE Partitions=gpu3\n"
            )
        )
        self.db.replace_pestat_nodes(
            parse_pestat(
                "Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist\n"
                "gpu-ada gpu3 idle 0 64 0.0 1024000 900000\n"
            )
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            min_warm_allocations=0,
            gpu_prewarm_enabled=True,
            gpu_prewarm_min_warm_allocations=1,
            gpu_prewarm_max_warm_allocations=3,
            gpu_prewarm_preferred_models=["a6000ada", "a6000"],
        )
        scheduler.maintain_allocation_pool()
        gpu_allocations = [item for item in self.db.list_allocations() if item["resource_pool"].startswith("gpu:")]
        self.assertEqual(len(gpu_allocations), 2)
        self.assertTrue(any(item["gpu_model"] == "a6000ada" and item["state"] == AllocationStatus.PENDING.value for item in gpu_allocations))

    def test_cpu_task_can_borrow_idle_gpu_allocation_without_gpu_cpu_reserve(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="gpu-ada",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            gpu_prewarm_cpu_reserve_per_free_gpu=8,
        )
        fitting = {"cpus": 32, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        too_large = {"cpus": 33, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        self.assertIsNotNone(scheduler.best_allocation_for_task(fitting))
        self.assertIsNone(scheduler.best_allocation_for_task(too_large))

    def test_cpu_task_reserves_cpu_when_gpu_allocation_has_gpu_work(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="gpu-ada",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=2,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=24,
            free_gpus=1,
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            gpu_prewarm_cpu_reserve_per_free_gpu=8,
        )
        fitting = {"cpus": 16, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        too_large = {"cpus": 17, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        self.assertIsNotNone(scheduler.best_allocation_for_task(fitting))
        self.assertIsNone(scheduler.best_allocation_for_task(too_large))

    def test_gpu_task_can_attach_when_gpu_matches_but_cpu_is_tight(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-a6000",
            total_cpus=4,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            free_cpus=0,
            free_gpus=1,
        )
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = {"cpus": 4, "memory_mb": 2048, "gpus": 1, "gpu_model": "a6000", "partition": "auto", "node_name": ""}
        self.assertEqual(scheduler.best_allocation_for_task(task)["id"], allocation_id)
        too_large = {"cpus": 5, "memory_mb": 2048, "gpus": 1, "gpu_model": "a6000", "partition": "auto", "node_name": ""}
        self.assertIsNone(scheduler.best_allocation_for_task(too_large))
        capacity = scheduler.task_fit_capacity(task)
        self.assertEqual(capacity["fit_slots"], 1)

    def test_demand_prewarm_counts_inflight_slots_instead_of_binary_capacity(self) -> None:
        for name in ("n001", "n002"):
            allocation_id = self.db.create_allocation(
                account_name="a",
                partition="cpu1",
                node_name=name,
                total_cpus=64,
                total_memory_mb=65536,
            )
            self.db.update_allocation(
                allocation_id,
                state=AllocationStatus.ACTIVE.value,
                slurm_job_id=f"alloc-{name}",
                free_cpus=12,
                free_memory_mb=65536,
            )
        pending_id = self.db.create_allocation(
            account_name="a",
            partition="gpu1",
            node_name="g001",
            total_cpus=48,
            total_memory_mb=196608,
            total_gpus=2,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(
            pending_id,
            state=AllocationStatus.PENDING.value,
            slurm_job_id="pending-gpu",
            free_cpus=48,
            free_memory_mb=196608,
            free_gpus=2,
        )
        task_ids = [
            self.db.create_task(TaskCreate(f"cpu-{index}", "~/case", "run", cpus=16, memory_mb=32768))
            for index in range(4)
        ]
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = scheduler.next_queued_task_without_inflight_capacity()
        self.assertIsNotNone(task)
        self.assertEqual(task["id"], task_ids[3])

    def test_near_drain_allocation_does_not_accept_new_tasks(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name="n001",
            total_cpus=8,
            total_memory_mb=65536,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.WARM.value,
            slurm_job_id="alloc-1",
            started_at="2000-01-01 00:00:00",
        )
        scheduler = Scheduler(
            self.db,
            self.accounts,
            30,
            client_factory=FakeClient,
            allocation_drain_after_seconds=3600,
            allocation_attach_stop_before_drain_seconds=1800,
        )
        task = {"cpus": 1, "memory_mb": 2048, "gpus": 0, "partition": "auto", "node_name": ""}
        self.assertIsNone(scheduler.best_allocation_for_task(task))

    def test_gpu_task_requires_matching_model(self) -> None:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu5",
            node_name="gpu-a6000",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000",
            resource_pool="gpu:a6000",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        task = {"cpus": 8, "memory_mb": 2048, "gpus": 1, "gpu_model": "a6000ada", "partition": "auto", "node_name": ""}
        self.assertIsNone(scheduler.best_allocation_for_task(task))

    def test_allocation_usage_summary_excludes_pending_when_filtered(self) -> None:
        allocations = [
            {"state": "active", "total_cpus": 8, "free_cpus": 4, "total_gpus": 1, "free_gpus": 0, "total_memory_mb": 8192, "free_memory_mb": 4096},
            {"state": "pending", "total_cpus": 64, "free_cpus": 64, "total_gpus": 2, "free_gpus": 2, "total_memory_mb": 65536, "free_memory_mb": 65536},
        ]
        ready = [item for item in allocations if item["state"] in {"active", "warm", "draining", "closing"}]
        self.assertEqual(sum(item["total_cpus"] - item["free_cpus"] for item in ready), 4)
        self.assertEqual(sum(item["total_cpus"] for item in ready), 8)
        self.assertEqual(sum(item["total_gpus"] - item["free_gpus"] for item in ready), 1)
        self.assertEqual(sum(item["total_gpus"] for item in ready), 1)

    def test_gpu_capacity_summary_separates_cluster_and_scheduler_capacity(self) -> None:
        inventory = parse_scontrol_nodes(
            "NodeName=gpu-ada CPUTot=64 RealMemory=1024000 Gres=gpu:a6000ada:4 GresUsed=gpu:a6000ada:2(IDX:0-1) State=MIXED Partitions=gpu3\n"
        )
        self.db.replace_node_inventory(inventory)
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="gpu3",
            node_name="gpu-ada",
            total_cpus=32,
            total_memory_mb=65536,
            total_gpus=1,
            gpu_model="a6000ada",
            resource_pool="gpu:a6000ada",
        )
        self.db.update_allocation(allocation_id, state=AllocationStatus.WARM.value, slurm_job_id="alloc-1")
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        summary = scheduler.gpu_capacity_summary()[0]
        self.assertEqual(summary["gpu_model"], "a6000ada")
        self.assertEqual(summary["cluster_total_gpus"], 4)
        self.assertEqual(summary["cluster_used_gpus"], 2)
        self.assertEqual(summary["cluster_free_gpus"], 2)
        self.assertEqual(summary["scheduler_owned_gpus"], 1)
        self.assertEqual(summary["scheduler_free_gpus"], 1)


if __name__ == "__main__":
    unittest.main()
