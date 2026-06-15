from __future__ import annotations

import tempfile
import unittest

from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.models import AccountSnapshot, JobCreate, JobStatus
from slurm_scheduler.scheduler import Scheduler
from slurm_scheduler.inventory import parse_sinfo_nodes, partition_rank
from slurm_scheduler.pestat import parse_pestat, plan_dynamic_allocations
from slurm_scheduler.slurm import build_sbatch_script, parse_du_gb, parse_sbatch_job_id, parse_squeue_counts


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

    def state(self, slurm_job_id: str) -> JobStatus:
        return JobStatus.COMPLETED


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
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=3, pending=0, max_running=4, max_pending=10, max_total=10),
            "b": AccountSnapshot("b", running=1, pending=1, max_running=4, max_pending=10, max_total=10),
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_choose_account_prefers_freer_account(self) -> None:
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        self.assertEqual(scheduler.choose_account().name, "b")

    def test_submit_next_queued_job_updates_database(self) -> None:
        job_id = self.db.create_job(JobCreate("git@example.com:repo.git", "main", "run.py"))
        scheduler = Scheduler(self.db, self.accounts, 30, client_factory=FakeClient)
        scheduler.submit_next_queued_job()
        job = self.db.get_job(job_id)
        self.assertEqual(job["status"], JobStatus.SUBMITTED.value)
        self.assertEqual(job["account_name"], "b")
        self.assertEqual(job["slurm_job_id"], "777")

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


if __name__ == "__main__":
    unittest.main()
