from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from slurm_scheduler.aedt_pool_api import build_node_local_aedt_summary
from slurm_scheduler.db import Database
from slurm_scheduler.models import TaskCreate, TaskStatus


class NodeLocalAedtSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "scheduler.db"))
        self.db.init()
        self.allocation_id = self.db.create_allocation(
            account_name="allocation-account",
            partition="cpu",
            node_name="cpu-17",
            total_cpus=64,
            total_memory_mb=512 * 1024,
        )
        self.db.update_allocation(
            self.allocation_id,
            state="active",
            slurm_job_id="job-node-local",
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def create_host(
        self,
        bundle_id: str,
        status: str,
        *,
        account_name: str = "host-account",
    ) -> int:
        task_id = self.db.create_task(
            TaskCreate(
                f"mft-aedt-pooled-{bundle_id}-host",
                "~/aedt",
                "run-host",
                account_name=account_name,
                project="_aedt_pool_hosts",
                requested_allocation_id=self.allocation_id,
            )
        )
        fields: dict[str, object] = {"status": status}
        if status in {TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value}:
            fields.update(
                allocation_id=self.allocation_id,
                started_at="2026-07-14 01:02:03",
            )
        self.db.update_task(task_id, **fields)
        return task_id

    def create_client(
        self,
        host_id: int,
        status: str,
        *,
        name: str,
        project: str = "derived-project",
        aedt_backend: str = "pooled",
    ) -> int:
        task_id = self.db.create_task(
            TaskCreate(
                name,
                "~/project",
                "run-client",
                aedt_backend=aedt_backend,
                same_node_as_task_id=host_id,
                project=project,
            )
        )
        self.db.update_task(task_id, status=status, allocation_id=self.allocation_id)
        return task_id

    def test_builds_active_hosts_with_generic_attached_clients(self) -> None:
        running_host_id = self.create_host(
            "0123456789abcdefabcd",
            TaskStatus.RUNNING.value,
        )
        attaching_host_id = self.create_host(
            "11111111111111111111",
            TaskStatus.ATTACHING.value,
        )
        queued_host_id = self.create_host(
            "22222222222222222222",
            TaskStatus.QUEUED.value,
        )
        running_client_id = self.create_client(
            running_host_id,
            TaskStatus.RUNNING.value,
            name="arbitrary-running-client",
            project="not-the-production-project",
        )
        attaching_client_id = self.create_client(
            running_host_id,
            TaskStatus.ATTACHING.value,
            name="arbitrary-attaching-client",
        )
        attaching_host_client_id = self.create_client(
            attaching_host_id,
            TaskStatus.RUNNING.value,
            name="attaching-host-client",
        )
        self.create_client(
            running_host_id,
            TaskStatus.QUEUED.value,
            name="not-yet-attached-client",
        )
        self.create_client(
            running_host_id,
            TaskStatus.RUNNING.value,
            name="standalone-client",
            aedt_backend="standalone",
        )
        central_host_id = self.db.create_task(
            TaskCreate(
                "aedt-session-host-7",
                "~/aedt",
                "run-central-host",
                project="_aedt_pool_hosts",
            )
        )
        self.db.update_task(central_host_id, status=TaskStatus.RUNNING.value)

        summary = build_node_local_aedt_summary(self.db)

        self.assertEqual(summary["active_host_count"], 3)
        self.assertEqual(summary["running_host_count"], 1)
        self.assertEqual(summary["attached_client_count"], 2)
        hosts = {host["id"]: host for host in summary["hosts"]}
        self.assertEqual(set(hosts), {running_host_id, attaching_host_id, queued_host_id})
        running_host = hosts[running_host_id]
        self.assertEqual(running_host["bundle_id"], "0123456789abcdefabcd")
        self.assertEqual(running_host["bundle_id_short"], "01234567")
        self.assertEqual(running_host["node_name"], "cpu-17")
        self.assertEqual(running_host["account_name"], "host-account")
        self.assertEqual(running_host["status"], TaskStatus.RUNNING.value)
        self.assertEqual(running_host["started_at"], "2026-07-14 01:02:03")
        self.assertEqual(
            {client["id"] for client in running_host["clients"]},
            {running_client_id, attaching_client_id},
        )
        running_client = next(
            client for client in running_host["clients"] if client["id"] == running_client_id
        )
        self.assertEqual(running_client["name"], "arbitrary-running-client")
        self.assertEqual(running_client["project"], "not-the-production-project")
        self.assertEqual(running_client["status"], TaskStatus.RUNNING.value)
        self.assertEqual(
            [client["id"] for client in hosts[attaching_host_id]["clients"]],
            [attaching_host_client_id],
        )
        self.assertEqual(hosts[queued_host_id]["clients"], [])

    def test_empty_summary_is_graceful(self) -> None:
        summary = build_node_local_aedt_summary(self.db)

        self.assertEqual(
            summary,
            {
                "active_host_count": 0,
                "running_host_count": 0,
                "attached_client_count": 0,
                "hosts": [],
            },
        )

    def test_clients_of_terminal_hosts_are_excluded(self) -> None:
        active_host_id = self.create_host(
            "aaaaaaaaaaaaaaaaaaaa",
            TaskStatus.RUNNING.value,
        )
        completed_host_id = self.create_host(
            "bbbbbbbbbbbbbbbbbbbb",
            TaskStatus.COMPLETED.value,
        )
        failed_host_id = self.create_host(
            "cccccccccccccccccccc",
            TaskStatus.FAILED.value,
        )
        self.create_client(
            completed_host_id,
            TaskStatus.RUNNING.value,
            name="completed-host-client",
        )
        self.create_client(
            failed_host_id,
            TaskStatus.ATTACHING.value,
            name="failed-host-client",
        )

        summary = build_node_local_aedt_summary(self.db)

        self.assertEqual(summary["active_host_count"], 1)
        self.assertEqual(summary["running_host_count"], 1)
        self.assertEqual(summary["attached_client_count"], 0)
        self.assertEqual([host["id"] for host in summary["hosts"]], [active_host_id])
        self.assertEqual(summary["hosts"][0]["clients"], [])


if __name__ == "__main__":
    unittest.main()
