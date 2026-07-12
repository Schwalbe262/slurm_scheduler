from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from slurm_scheduler.project_env import build_repo_sync_script


class ExactCommitRepoSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        git = shutil.which("git")
        if not git:
            self.skipTest("git is required")
        self.git = git
        self.bash = self._find_bash()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.remote = self.root / "remote.git"
        self.source = self.root / "source"

        self._git("init", "--bare", str(self.remote))
        self._git("init", str(self.source))
        self._git("-C", str(self.source), "config", "user.name", "Project Env Test")
        self._git("-C", str(self.source), "config", "user.email", "project-env@example.invalid")
        (self.source / "version.txt").write_text("one\n", encoding="utf-8")
        self._git("-C", str(self.source), "add", "version.txt")
        self._git("-C", str(self.source), "commit", "-m", "first")
        self.first = self._git("-C", str(self.source), "rev-parse", "HEAD").stdout.strip()
        (self.source / "version.txt").write_text("two\n", encoding="utf-8")
        self._git("-C", str(self.source), "commit", "-am", "second")
        self.tip = self._git("-C", str(self.source), "rev-parse", "HEAD").stdout.strip()
        self._git("-C", str(self.source), "branch", "-M", "main")
        self._git("-C", str(self.source), "remote", "add", "origin", self.remote.as_uri())
        self._git("-C", str(self.source), "push", "-u", "origin", "main")
        self._git("--git-dir", str(self.remote), "symbolic-ref", "HEAD", "refs/heads/main")

    def tearDown(self) -> None:
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    @staticmethod
    def _find_bash() -> str:
        candidates: list[Path] = []
        if os.name == "nt":
            for root_name in ("ProgramFiles", "ProgramFiles(x86)"):
                root = os.environ.get(root_name)
                if root:
                    candidates.append(Path(root) / "Git" / "bin" / "bash.exe")
        resolved = shutil.which("bash")
        if resolved:
            candidates.append(Path(resolved))
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
        raise unittest.SkipTest("bash is required")

    def _git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [self.git, *args],
            text=True,
            capture_output=True,
            check=False,
        )
        if check and result.returncode != 0:
            self.fail(f"git {' '.join(args)} failed: {result.stderr or result.stdout}")
        return result

    def _run_sync(self, rel_dest: str, ref: str) -> tuple[subprocess.CompletedProcess[str], str]:
        script = build_repo_sync_script(rel_dest, self.remote.as_uri(), ref)
        env = os.environ.copy()
        env["HOME"] = self.home.as_posix()
        result = subprocess.run(
            [self.bash, "-c", script],
            text=True,
            capture_output=True,
            check=False,
            env=env,
        )
        return result, script

    def _assert_exact_detached_commit(self, destination: Path, expected: str) -> None:
        head = self._git("-C", str(destination), "rev-parse", "HEAD").stdout.strip()
        self.assertEqual(head, expected)
        symbolic = self._git(
            "-C", str(destination), "symbolic-ref", "-q", "HEAD", check=False
        )
        self.assertNotEqual(symbolic.returncode, 0)
        status = self._git("-C", str(destination), "status", "--porcelain").stdout
        self.assertEqual(status, "")

    def test_branch_ref_keeps_clone_checkout_pull_behavior(self) -> None:
        script = build_repo_sync_script("projects/motor/repo", "https://example.test/repo.git", "main")

        self.assertIn("git -C \"$dest\" checkout -q main", script)
        self.assertIn("git -C \"$dest\" pull -q --ff-only", script)
        self.assertIn('git -C "$dest" reset -q --hard "@{u}"', script)
        self.assertIn("git clone -q --depth 1 --branch main", script)
        self.assertNotIn("--detach", script)

    def test_full_sha_fresh_clone_verifies_and_checks_out_exact_detached_commit(self) -> None:
        rel_dest = "projects/fresh/repo"
        result, script = self._run_sync(rel_dest, self.first.upper())

        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        self.assertNotIn("git pull", script)
        self.assertNotIn("@{u}", script)
        self.assertIn('rev-parse --verify "${target}^{commit}"', script)
        self.assertIn('checkout -q --detach "$target"', script)
        self.assertIn('reset -q --hard "$target"', script)
        destination = self.home / rel_dest
        self._assert_exact_detached_commit(destination, self.first)
        self.assertEqual((destination / "version.txt").read_text(encoding="utf-8"), "one\n")

    def test_full_sha_existing_shallow_clone_fetches_history_and_resets_exactly(self) -> None:
        rel_dest = "projects/existing/repo"
        destination = self.home / rel_dest
        destination.parent.mkdir(parents=True)
        self._git(
            "clone",
            "--depth",
            "1",
            "--branch",
            "main",
            self.remote.as_uri(),
            str(destination),
        )
        missing = self._git(
            "-C", str(destination), "cat-file", "-e", f"{self.first}^{{commit}}", check=False
        )
        self.assertNotEqual(missing.returncode, 0)

        result, script = self._run_sync(rel_dest, self.first)

        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        self.assertIn("fetch -q --unshallow", script)
        self.assertNotIn("git pull", script)
        self.assertNotIn("@{u}", script)
        self._assert_exact_detached_commit(destination, self.first)
        shallow = self._git(
            "-C", str(destination), "rev-parse", "--is-shallow-repository"
        ).stdout.strip()
        self.assertEqual(shallow, "false")

    def test_unknown_full_sha_fails_before_changing_existing_checkout(self) -> None:
        rel_dest = "projects/unknown-existing/repo"
        destination = self.home / rel_dest
        destination.parent.mkdir(parents=True)
        self._git("clone", "--branch", "main", self.remote.as_uri(), str(destination))

        result, script = self._run_sync(rel_dest, "f" * 40)

        self.assertNotEqual(result.returncode, 0)
        self.assertLess(
            script.index('rev-parse --verify "${target}^{commit}"'),
            script.index('checkout -q --detach "$target"'),
        )
        head = self._git("-C", str(destination), "rev-parse", "HEAD").stdout.strip()
        branch = self._git(
            "-C", str(destination), "symbolic-ref", "--short", "HEAD"
        ).stdout.strip()
        self.assertEqual(head, self.tip)
        self.assertEqual(branch, "main")
        self.assertEqual((destination / "version.txt").read_text(encoding="utf-8"), "two\n")

    def test_unknown_full_sha_fresh_clone_has_no_checked_out_source(self) -> None:
        rel_dest = "projects/unknown-fresh/repo"
        result, _ = self._run_sync(rel_dest, "e" * 40)

        self.assertNotEqual(result.returncode, 0)
        destination = self.home / rel_dest
        self.assertTrue((destination / ".git").is_dir())
        self.assertFalse((destination / "version.txt").exists())


if __name__ == "__main__":
    unittest.main()
