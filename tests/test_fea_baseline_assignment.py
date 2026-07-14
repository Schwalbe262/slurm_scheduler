"""FEA baseline-first assignment policy.

Every allocation is entitled to one solver per requested-CPU share of its own
reservation (1x baseline) before any allocation is overcommitted; infra tasks
(_aedt_pool_hosts) do not consume baseline; baseline attaches draw from a
budget separate from the global overcommit cap; license scarcity flips the
under-baseline ordering from spread-first to fill-first.
"""
from __future__ import annotations

import tempfile
import unittest

from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.models import (
    AccountSnapshot,
    AllocationStatus,
    SchedulingProfile,
    TaskCreate,
    TaskStatus,
)
from slurm_scheduler.pestat import parse_pestat
from slurm_scheduler.scheduler import Scheduler

from test_core import FakeClient, days_ago


class FeaBaselineAssignmentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)]
        FakeClient.submitted = []
        FakeClient.allocation_submits = []
        FakeClient.allocation_states = {}
        FakeClient.task_states = {}
        FakeClient.attached_tasks = []
        FakeClient.snapshots = {
            "a": AccountSnapshot("a", running=1, pending=0, max_running=10, max_pending=10, max_total=20),
        }

    def make_scheduler(self, **kwargs) -> Scheduler:
        return Scheduler(self.db, self.accounts, 30, client_factory=FakeClient, **kwargs)

    def make_allocation(self, node: str, total_cpus: int) -> int:
        allocation_id = self.db.create_allocation(
            account_name="a",
            partition="cpu1",
            node_name=node,
            total_cpus=total_cpus,
            total_memory_mb=total_cpus * 4096,
        )
        self.db.update_allocation(
            allocation_id,
            state=AllocationStatus.ACTIVE.value,
            slurm_job_id=f"alloc-{allocation_id}",
        )
        return allocation_id

    def add_running(self, allocation_id: int, count: int, cpus: int = 4, project: str = "") -> None:
        for index in range(count):
            task_id = self.db.create_task(
                TaskCreate(
                    f"running-{allocation_id}-{project or 'solver'}-{index}",
                    "~/case",
                    "run",
                    cpus=cpus,
                    memory_mb=4096,
                    scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                    project=project,
                )
            )
            self.db.update_task(
                task_id,
                status=TaskStatus.RUNNING.value,
                allocation_id=allocation_id,
                account_name="a",
                # Mature workers: past the young-footprint window so pestat
                # readings are trusted instead of declared reservations.
                started_at=days_ago(1),
                attached_at=days_ago(1),
            )

    def queue_fea(self, name: str = "queued-fea", cpus: int = 4) -> int:
        return self.db.create_task(
            TaskCreate(
                name,
                "~/case",
                "run",
                cpus=cpus,
                memory_mb=4096,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
            )
        )

    def set_healthy_nodes(self, *nodes: tuple[str, int, int]) -> None:
        rows = ["Hostname  Partition Node Num_CPU CPUload Memsize Freemem Joblist"]
        for node, used, total in nodes:
            rows.append(f"{node} cpu1 mix {used} {total} 8.0 512000 400000 some_job")
        self.db.replace_pestat_nodes(parse_pestat("\n".join(rows)))

    def test_under_baseline_allocation_beats_lower_worker_count(self) -> None:
        big = self.make_allocation("n001", 64)
        small = self.make_allocation("n002", 32)
        self.add_running(big, 12)  # 48/64 = 0.75, 12 workers
        self.add_running(small, 8)  # 32/32 = 1.0, 8 workers
        self.set_healthy_nodes(("n001", 48, 64), ("n002", 32, 32))
        scheduler = self.make_scheduler()
        task_id = self.queue_fea()
        chosen = scheduler.best_allocation_for_task(self.db.get_task(task_id))
        self.assertIsNotNone(chosen)
        # The old node-worker-count ordering picked n002 (8 < 12 workers)
        # even though it already reached its 1x baseline.
        self.assertEqual(int(chosen["id"]), big)
        self.assertTrue(scheduler._fea_last_attach_baseline)

    def test_infra_host_task_does_not_consume_baseline(self) -> None:
        allocation_id = self.make_allocation("n001", 64)
        self.add_running(allocation_id, 15)  # 60 solver CPUs
        self.add_running(allocation_id, 1, cpus=1, project="_aedt_pool_hosts")
        self.set_healthy_nodes(("n001", 61, 64))
        scheduler = self.make_scheduler()
        allocation = self.db.get_allocation(allocation_id)
        # 60/64 < 1.0: the 1-cpu session host must not push the allocation
        # past baseline (floor((64-1)/4)=15 was the old, wrong ceiling).
        self.assertLess(scheduler.fea_baseline_ratio(allocation), 1.0)
        task_id = self.queue_fea()
        chosen = scheduler.best_allocation_for_task(self.db.get_task(task_id))
        self.assertIsNotNone(chosen)
        self.assertEqual(int(chosen["id"]), allocation_id)
        remaining = scheduler.fea_node_cpu_cap_remaining(
            self.db.get_allocation(allocation_id), self.db.get_task(task_id)
        )
        self.assertGreaterEqual(int(remaining), 1)

    def test_baseline_attaches_bypass_overcommit_cap(self) -> None:
        allocation_id = self.make_allocation("n001", 64)
        self.add_running(allocation_id, 4)  # 16/64, deeply under baseline
        self.set_healthy_nodes(("n001", 16, 64))
        scheduler = self.make_scheduler(fea_max_attach_per_loop=1)
        queued = [self.queue_fea(f"queued-{index}") for index in range(3)]
        scheduler.assign_ready_fea_tasks()
        statuses = [self.db.get_task(task_id)["status"] for task_id in queued]
        # All three are baseline attaches: the overcommit cap of 1 must not
        # throttle them.
        self.assertNotIn(TaskStatus.QUEUED.value, statuses)

    def test_overcommit_cap_still_bounds_at_or_above_baseline(self) -> None:
        allocation_id = self.make_allocation("n001", 64)
        self.add_running(allocation_id, 16)  # 64/64 = baseline reached
        self.set_healthy_nodes(("n001", 64, 64))
        # factor 2.0 leaves requested-CPU headroom past baseline, so the
        # overcommit budget (not the hard cap) is what bounds the attaches.
        scheduler = self.make_scheduler(
            fea_max_attach_per_loop=1, fea_node_requested_cpu_factor=2.0
        )
        queued = [self.queue_fea(f"queued-{index}") for index in range(3)]
        scheduler.assign_ready_fea_tasks()
        attached = [
            task_id
            for task_id in queued
            if self.db.get_task(task_id)["status"] != TaskStatus.QUEUED.value
        ]
        self.assertEqual(len(attached), 1)

    def test_license_scarcity_flips_to_fill_first(self) -> None:
        nearly_full = self.make_allocation("n001", 64)
        nearly_empty = self.make_allocation("n002", 64)
        self.add_running(nearly_full, 12)  # 0.75
        self.add_running(nearly_empty, 4)  # 0.25
        self.set_healthy_nodes(("n001", 48, 64), ("n002", 16, 64))
        scheduler = self.make_scheduler()
        task = self.db.get_task(self.queue_fea())
        chosen = scheduler.best_allocation_for_task(task)
        self.assertEqual(int(chosen["id"]), nearly_empty)  # spread-first
        scheduler.fea_license_admit_headroom = lambda: 1
        chosen = scheduler.best_allocation_for_task(task)
        self.assertEqual(int(chosen["id"]), nearly_full)  # fill-first


if __name__ == "__main__":
    unittest.main()
