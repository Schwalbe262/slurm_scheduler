from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from slurm_scheduler.aedt_pool import AEDT_POOL_SCHEMA, AedtPoolService
from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.models import AedtBackend, TaskCreate, TaskStatus
from slurm_scheduler.scheduler import (
    TERMINAL_AEDT_WORKSPACE_DELETED_MARKER,
    Scheduler,
)
from slurm_scheduler.slurm import CommandResult


class FakeSSHSession:
    results: list[CommandResult] = []
    commands: list[str] = []
    accounts: list[str] = []

    def __init__(self, account: AccountConfig, default_timeout: float = 0):
        del default_timeout
        self.account = account

    def __enter__(self) -> "FakeSSHSession":
        self.accounts.append(self.account.name)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def run(self, command: str, timeout: float = 0) -> CommandResult:
        del timeout
        self.commands.append(command)
        if not self.results:
            raise AssertionError("no fake SSH result was configured")
        return self.results.pop(0)


class TerminalAedtWorkspaceCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "scheduler.db"))
        self.db.init()
        self.pool = AedtPoolService(self.db)
        self.pool.init()
        self.account = AccountConfig(
            "a", "host", 22, "a", "key", "/work", 10, 10, 20
        )
        self.scheduler = Scheduler(
            self.db,
            [self.account],
            30,
            cleanup_enabled=True,
        )
        FakeSSHSession.results = []
        FakeSSHSession.commands = []
        FakeSSHSession.accounts = []

    def tearDown(self) -> None:
        self.scheduler.stop()
        self.tmp.cleanup()

    def make_task(self, name: str = "mft-cleanup") -> int:
        task_id = self.db.create_task(
            TaskCreate(
                name,
                "/work/mft",
                "run",
                account_name="a",
                aedt_backend=AedtBackend.POOLED.value,
            )
        )
        self.db.update_task(
            task_id,
            status=TaskStatus.FAILED.value,
            account_name="a",
            finished_at="CURRENT_TIMESTAMP",
        )
        return task_id

    def add_lease(
        self,
        task_id: int,
        *,
        state: str = "failed",
        workspace_path: str | None = None,
        request_key: str | None = None,
    ) -> int:
        path = workspace_path or f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}"
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO aedt_project_leases (
                    request_key, project_name, workspace_path, protocol_version,
                    task_id, state, client_token_hash, last_heartbeat_at,
                    expires_at, finished_at
                ) VALUES (?, ?, ?, 2, ?, ?, 'hash', CURRENT_TIMESTAMP,
                          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    request_key or f"request-{task_id}",
                    f"mft-project-{task_id}",
                    path,
                    int(task_id),
                    state,
                ),
            )
            return int(cursor.lastrowid)

    def lease(self, lease_id: int) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM aedt_project_leases WHERE id = ?",
                (int(lease_id),),
            ).fetchone()
            assert row is not None
            return dict(row)

    def test_exact_same_account_cleanup_is_audited_and_idempotent(self) -> None:
        task_id = self.make_task()
        lease_id = self.add_lease(task_id)
        FakeSSHSession.results = [
            CommandResult(
                f"{TERMINAL_AEDT_WORKSPACE_DELETED_MARKER}\n", "", 0
            )
        ]

        with patch(
            "slurm_scheduler.scheduler.SSHSession", FakeSSHSession
        ):
            self.scheduler._cleanup_terminal_aedt_workspace_task(
                task_id, "failed"
            )
            # A completed audit state excludes the row from every retry.
            self.scheduler._cleanup_terminal_aedt_workspace_task(
                task_id, "periodic retry"
            )

        after = self.lease(lease_id)
        self.assertEqual(after["workspace_cleanup_state"], "deleted")
        self.assertEqual(after["workspace_cleanup_attempts"], 1)
        self.assertEqual(
            after["workspace_path"],
            f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}",
        )
        self.assertEqual(FakeSSHSession.accounts, ["a"])
        self.assertEqual(len(FakeSSHSession.commands), 1)
        command = FakeSSHSession.commands[0]
        self.assertIn(f"mft-{task_id}", command)
        self.assertIn("readlink -f", command)
        self.assertIn("stat -c %u", command)
        self.assertIn("find -P", command)
        self.assertIn("-type l", command)
        self.assertIn("! -uid", command)
        self.assertIn("rm -rf --one-file-system", command)
        self.assertNotIn("chmod", command)
        self.assertNotIn("aedt_session_logs", command)
        events = self.db.list_events(limit=20)
        self.assertTrue(
            any(event["kind"] == "aedt_workspace_cleanup" for event in events)
        )

    def test_permission_failure_is_retained_and_retried(self) -> None:
        task_id = self.make_task("permission-retry")
        lease_id = self.add_lease(task_id)
        FakeSSHSession.results = [
            CommandResult("", "AEDT workspace contains cross-account content", 86)
        ]
        with patch(
            "slurm_scheduler.scheduler.SSHSession", FakeSSHSession
        ):
            self.scheduler._cleanup_terminal_aedt_workspace_task(
                task_id, "failed"
            )

        failed = self.lease(lease_id)
        self.assertEqual(failed["workspace_cleanup_state"], "failed")
        self.assertEqual(failed["workspace_cleanup_attempts"], 1)
        self.assertIn("cross-account", failed["workspace_cleanup_error"])
        self.assertEqual(
            failed["workspace_path"],
            f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}",
        )

        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE aedt_project_leases
                SET workspace_cleanup_at = '2000-01-01 00:00:00'
                WHERE id = ?
                """,
                (lease_id,),
            )
        FakeSSHSession.results = [
            CommandResult(
                f"{TERMINAL_AEDT_WORKSPACE_DELETED_MARKER}\n", "", 0
            )
        ]
        with patch(
            "slurm_scheduler.scheduler.SSHSession", FakeSSHSession
        ):
            self.scheduler._cleanup_terminal_aedt_workspace_task(
                task_id, "periodic retry"
            )

        recovered = self.lease(lease_id)
        self.assertEqual(recovered["workspace_cleanup_state"], "deleted")
        self.assertEqual(recovered["workspace_cleanup_attempts"], 2)
        self.assertEqual(len(FakeSSHSession.commands), 2)
        events = self.db.list_events(limit=20)
        self.assertTrue(
            any(
                event["kind"] == "aedt_workspace_cleanup_failed"
                for event in events
            )
        )

    def test_active_lease_or_reference_blocks_claim_and_remote_delete(self) -> None:
        task_id = self.make_task("active-lease")
        lease_id = self.add_lease(task_id, state="active")

        with patch(
            "slurm_scheduler.scheduler.SSHSession", FakeSSHSession
        ):
            self.scheduler._cleanup_terminal_aedt_workspace_task(
                task_id, "failed"
            )
        self.assertEqual(FakeSSHSession.commands, [])
        self.assertEqual(self.lease(lease_id)["workspace_cleanup_state"], "pending")

        with self.db.connect() as conn:
            conn.execute(
                "UPDATE aedt_project_leases SET state = 'failed' WHERE id = ?",
                (lease_id,),
            )
        candidates = self.db.list_terminal_aedt_workspace_cleanup_candidates(
            task_id=task_id,
            retry_before="9999-12-31 23:59:59",
            stale_claim_before="9999-12-31 23:59:59",
        )
        self.assertEqual(len(candidates), 1)

        other_task_id = self.db.create_task(
            TaskCreate(
                "live-reference",
                "/work/mft",
                "run",
                account_name="a",
                aedt_backend=AedtBackend.POOLED.value,
            )
        )
        self.db.update_task(
            other_task_id,
            status=TaskStatus.RUNNING.value,
            account_name="a",
        )
        self.add_lease(
            other_task_id,
            state="active",
            workspace_path=f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}",
            request_key="active-shared-reference",
        )
        self.assertFalse(
            self.db.claim_terminal_aedt_workspace_cleanup(
                task_id,
                f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}",
                retry_before="9999-12-31 23:59:59",
                stale_claim_before="9999-12-31 23:59:59",
                claimed_at="2026-07-16 00:00:00",
            )
        )

    def test_non_exact_path_is_rejected_without_ssh(self) -> None:
        task_id = self.make_task("invalid-path")
        path = f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}/nested"
        lease_id = self.add_lease(task_id, workspace_path=path)

        with patch(
            "slurm_scheduler.scheduler.SSHSession", FakeSSHSession
        ):
            self.scheduler._cleanup_terminal_aedt_workspace_task(
                task_id, "failed"
            )

        after = self.lease(lease_id)
        self.assertEqual(after["workspace_cleanup_state"], "rejected")
        self.assertIn("exact task leaf", after["workspace_cleanup_error"])
        self.assertEqual(after["workspace_path"], path)
        self.assertEqual(FakeSSHSession.commands, [])

    def test_terminal_hook_schedules_pooled_cleanup_without_declared_globs(
        self,
    ) -> None:
        task_id = self.make_task("terminal-hook")
        task = self.db.get_task(task_id)
        assert task is not None
        self.assertEqual(task["cleanup_globs"], "")
        with patch.object(
            self.scheduler,
            "_schedule_terminal_aedt_workspace_cleanup",
            return_value=True,
        ) as schedule:
            self.scheduler.on_task_terminal(task, "failed")
        schedule.assert_called_once_with(task_id, "failed")

    def test_additive_migration_quarantines_only_preexisting_terminal_rows(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            db = Database(str(Path(root) / "legacy.db"))
            db.init()
            legacy_schema = AEDT_POOL_SCHEMA
            for line in (
                "    workspace_cleanup_state TEXT NOT NULL DEFAULT 'pending',\n",
                "    workspace_cleanup_attempts INTEGER NOT NULL DEFAULT 0,\n",
                "    workspace_cleanup_at TEXT,\n",
                "    workspace_cleanup_error TEXT NOT NULL DEFAULT '',\n",
            ):
                legacy_schema = legacy_schema.replace(line, "")
            with db.connect() as conn:
                conn.executescript(legacy_schema)
            terminal_task = db.create_task(
                TaskCreate(
                    "legacy-terminal",
                    "/work/mft",
                    "run",
                    aedt_backend=AedtBackend.POOLED.value,
                )
            )
            live_task = db.create_task(
                TaskCreate(
                    "legacy-live",
                    "/work/mft",
                    "run",
                    aedt_backend=AedtBackend.POOLED.value,
                )
            )
            db.update_task(terminal_task, status=TaskStatus.FAILED.value)
            db.update_task(live_task, status=TaskStatus.RUNNING.value)
            with db.connect() as conn:
                for task_id, state in (
                    (terminal_task, "failed"),
                    (live_task, "active"),
                ):
                    conn.execute(
                        """
                        INSERT INTO aedt_project_leases (
                            request_key, project_name, workspace_path,
                            protocol_version, task_id, state,
                            client_token_hash, last_heartbeat_at, expires_at
                        ) VALUES (?, ?, ?, 2, ?, ?, 'hash',
                                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (
                            f"legacy-{task_id}",
                            f"mft-project-{task_id}",
                            f"/gpfs/tmp_cpu2/mft_pool/mft-{task_id}",
                            task_id,
                            state,
                        ),
                    )

            AedtPoolService(db).init()
            with db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT task_id, workspace_cleanup_state,
                           workspace_cleanup_error
                    FROM aedt_project_leases ORDER BY task_id
                    """
                ).fetchall()
            states = {int(row["task_id"]): dict(row) for row in rows}
            self.assertEqual(
                states[terminal_task]["workspace_cleanup_state"], "legacy"
            )
            self.assertIn(
                "predates", states[terminal_task]["workspace_cleanup_error"]
            )
            self.assertEqual(
                states[live_task]["workspace_cleanup_state"], "pending"
            )


if __name__ == "__main__":
    unittest.main()
