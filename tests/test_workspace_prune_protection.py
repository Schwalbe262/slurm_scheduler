from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from slurm_scheduler.config import AccountConfig
from slurm_scheduler.retention import (
    WORKSPACE_PRUNE_PROTECTION_MARKER,
    parse_workspace_prune_protection_manifest,
    workspace_prune_protection_marker_paths,
)
from slurm_scheduler.scheduler import Scheduler
from slurm_scheduler.slurm import CommandResult


WORKSPACE = "/gpfs/home1/user/slurm_scheduler"
SEALED_ROOT = WORKSPACE + "/runs/sealed-source"
NESTED_RESULTS = (
    SEALED_ROOT
    + "/current/repo/simulation/design/design.aedtresults"
)
DIRECT_RESULTS = WORKSPACE + "/scratch/direct.aedtresults"
ORDINARY_RESULTS = WORKSPACE + "/scratch/ordinary.aedtresults"


def valid_manifest() -> str:
    return json.dumps(
        {
            "schema": "slurm-scheduler-prune-protection-v1",
            "preserve": True,
            "created_at": "2026-07-24T12:00:00Z",
            "reason": "sealed source",
            "owner": "workload",
            "artifact_manifest": {
                "path": "manifest.json",
                "sha256": "a" * 64,
            },
        }
    )


class WorkspacePruneProtectionContractTests(unittest.TestCase):
    def test_checked_in_example_matches_manifest_contract(self) -> None:
        example = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "workspace-prune-protection-v1.json"
        )
        parsed = parse_workspace_prune_protection_manifest(
            example.read_bytes()
        )
        self.assertTrue(parsed["preserve"])

    def test_manifest_contract_accepts_bound_artifact_manifest(self) -> None:
        parsed = parse_workspace_prune_protection_manifest(valid_manifest())
        self.assertTrue(parsed["preserve"])
        self.assertEqual(parsed["artifact_manifest"]["sha256"], "a" * 64)

    def test_manifest_contract_rejects_unsafe_or_ambiguous_values(self) -> None:
        base = json.loads(valid_manifest())
        invalid = [
            {**base, "schema": "future-schema"},
            {**base, "preserve": False},
            {**base, "created_at": "2026-07-24T12:00:00"},
            {**base, "reason": ""},
            {**base, "unknown": True},
            {
                **base,
                "artifact_manifest": {
                    "path": "../manifest.json",
                    "sha256": "a" * 64,
                },
            },
            {
                **base,
                "artifact_manifest": {
                    "path": "manifest.json",
                    "sha256": "A" * 64,
                },
            },
        ]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_workspace_prune_protection_manifest(json.dumps(value))
        with self.assertRaises(ValueError):
            parse_workspace_prune_protection_manifest(
                valid_manifest().encode("utf-16")
            )

    def test_marker_ancestry_covers_candidate_through_workspace_only(self) -> None:
        markers = workspace_prune_protection_marker_paths(
            WORKSPACE, NESTED_RESULTS
        )
        self.assertEqual(
            markers[0],
            NESTED_RESULTS + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER,
        )
        self.assertIn(
            SEALED_ROOT + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER,
            markers,
        )
        self.assertEqual(
            markers[-1],
            WORKSPACE + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER,
        )
        self.assertNotIn(
            "/gpfs/home1/user/" + WORKSPACE_PRUNE_PROTECTION_MARKER,
            markers,
        )

    def test_marker_ancestry_rejects_outside_workspace(self) -> None:
        with self.assertRaises(ValueError):
            workspace_prune_protection_marker_paths(
                WORKSPACE, "/gpfs/home1/other/design.aedtresults"
            )

    def test_delete_command_checks_direct_and_ancestor_markers_twice(self) -> None:
        command = Scheduler._workspace_prune_delete_command(
            WORKSPACE, [NESTED_RESULTS, DIRECT_RESULTS], 60
        )
        direct_marker = (
            DIRECT_RESULTS + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER
        )
        ancestor_marker = (
            SEALED_ROOT + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER
        )
        self.assertGreaterEqual(command.count(direct_marker), 2)
        self.assertGreaterEqual(command.count(ancestor_marker), 2)
        self.assertIn('rm -rf -- "$target"', command)
        self.assertIn('[ -L "$marker_path" ]', command)
        self.assertIn('printf "P\\t%s\\t%s\\n"', command)
        self.assertIn('printf "D\\t%s\\n"', command)

    @unittest.skipIf(os.name == "nt", "requires a native POSIX shell")
    def test_delete_command_posix_semantics_preserve_markers_and_prune_plain(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir) / "workspace"
            sealed_root = workspace / "runs" / "sealed-source"
            nested = (
                sealed_root
                / "current"
                / "repo"
                / "design.aedtresults"
            )
            direct = workspace / "scratch" / "direct.aedtresults"
            ordinary = workspace / "scratch" / "ordinary.aedtresults"
            for path in (nested, direct, ordinary):
                path.mkdir(parents=True)
                old = time.time() - 7200
                os.utime(path, (old, old))
            (sealed_root / WORKSPACE_PRUNE_PROTECTION_MARKER).write_text(
                valid_manifest(), encoding="utf-8"
            )
            (direct / WORKSPACE_PRUNE_PROTECTION_MARKER).write_text(
                "{partial", encoding="utf-8"
            )
            command = Scheduler._workspace_prune_delete_command(
                str(workspace),
                [str(nested), str(direct), str(ordinary)],
                60,
            )
            completed = subprocess.run(
                ["/bin/sh", "-c", command],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(nested.is_dir())
            self.assertTrue(direct.is_dir())
            self.assertFalse(ordinary.exists())
            self.assertEqual(
                [line[:1] for line in completed.stdout.splitlines()],
                ["P", "P", "D"],
            )


class WorkspacePruneProtectionSchedulerTests(unittest.TestCase):
    def make_scheduler(self) -> tuple[Scheduler, list[tuple]]:
        scheduler = object.__new__(Scheduler)
        scheduler.accounts = [
            AccountConfig(
                "user",
                "host",
                22,
                "user",
                "key",
                WORKSPACE,
                4,
                10,
                10,
            )
        ]
        events: list[tuple] = []
        scheduler.record_event = (  # type: ignore[method-assign]
            lambda *args, **kwargs: events.append((args, kwargs))
        )
        return scheduler, events

    def test_prune_preserves_direct_and_ancestor_markers_but_deletes_ordinary(
        self,
    ) -> None:
        direct_marker = (
            DIRECT_RESULTS + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER
        )
        ancestor_marker = (
            SEALED_ROOT + "/" + WORKSPACE_PRUNE_PROTECTION_MARKER
        )

        class SequenceSession:
            commands: list[str] = []

            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, command, timeout=None):
                type(self).commands.append(command)
                if command.startswith("find "):
                    return CommandResult(
                        "\n".join(
                            [
                                NESTED_RESULTS,
                                DIRECT_RESULTS,
                                ORDINARY_RESULTS,
                            ]
                        )
                        + "\n",
                        "",
                        0,
                    )
                if "rm -rf" in command:
                    return CommandResult(
                        "\n".join(
                            [
                                f"P\t{NESTED_RESULTS}\t{ancestor_marker}",
                                f"P\t{DIRECT_RESULTS}\t{direct_marker}",
                                f"D\t{ORDINARY_RESULTS}",
                            ]
                        )
                        + "\n",
                        "",
                        0,
                    )
                if ancestor_marker in command:
                    return CommandResult(valid_manifest(), "", 0)
                if direct_marker in command:
                    return CommandResult("{partial", "", 0)
                raise AssertionError(f"unexpected SSH command: {command}")

        scheduler, events = self.make_scheduler()
        with (
            mock.patch(
                "slurm_scheduler.scheduler.SSHSession", SequenceSession
            ),
            self.assertLogs(
                "slurm_scheduler.scheduler", level="WARNING"
            ) as captured,
        ):
            scheduler._prune_workspace_artifacts(["*.aedtresults"], 60)

        delete_commands = [
            command
            for command in SequenceSession.commands
            if "rm -rf" in command
        ]
        list_commands = [
            command
            for command in SequenceSession.commands
            if command.startswith("find ")
        ]
        self.assertEqual(len(list_commands), 1)
        self.assertNotIn("-mmin", list_commands[0])
        self.assertEqual(len(delete_commands), 1)
        self.assertIn("-mmin -60", delete_commands[0])
        self.assertIn(ancestor_marker, delete_commands[0])
        self.assertIn(direct_marker, delete_commands[0])
        self.assertIn(ORDINARY_RESULTS, delete_commands[0])
        self.assertTrue(
            any("invalid" in message.lower() for message in captured.output)
        )
        self.assertEqual(len(events), 1)
        message = events[0][0][1]
        self.assertIn("removed 1", message)
        self.assertIn("preserved 2", message)
        self.assertIn("1 invalid manifest", message)

    def test_nonzero_delete_command_fails_closed_without_claiming_removal(
        self,
    ) -> None:
        class FailedDeleteSession:
            def __init__(self, *_args, **_kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def run(self, command, timeout=None):
                if command.startswith("find "):
                    return CommandResult(ORDINARY_RESULTS + "\n", "", 0)
                return CommandResult("", "transport-side shell failure", 7)

        scheduler, events = self.make_scheduler()
        with mock.patch(
            "slurm_scheduler.scheduler.SSHSession", FailedDeleteSession
        ):
            scheduler._prune_workspace_artifacts(["*.aedtresults"], 60)
        self.assertEqual(len(events), 1)
        message = events[0][0][1]
        self.assertIn("removed 0", message)
        self.assertIn("failed closed on 1", message)


if __name__ == "__main__":
    unittest.main()
