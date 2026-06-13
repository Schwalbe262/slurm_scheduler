from __future__ import annotations

import tempfile
import unittest

from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.models import AccountSnapshot, JobCreate, JobStatus
from slurm_scheduler.scheduler import Scheduler
from slurm_scheduler.inventory import parse_sinfo_nodes, partition_rank
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


class FakeClient:
    snapshots: dict[str, AccountSnapshot] = {}
    submitted: list[str] = []

    def __init__(self, account: AccountConfig):
        self.account = account

    def snapshot(self) -> AccountSnapshot:
        return self.snapshots[self.account.name]

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
