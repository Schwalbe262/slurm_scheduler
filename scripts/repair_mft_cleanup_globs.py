"""Remove the destructive shared simulation cleanup from active MFT tasks.

The repair waits for the scheduler's between-tick sleep, creates and verifies an
online SQLite backup, then changes the project default and matching active task
rows in one guarded transaction.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import time
from urllib.request import urlopen


PROJECT = "MFT_1MW_2026v1"
UNSAFE_CLEANUP = "*.aedtresults,simulation"
SAFE_CLEANUP = "*.aedtresults"
SAFE_OUTPUTS = (
    "simulation_results_*.csv,failed_samples_260706.jsonl,"
    "results_parts_260706/*.parquet"
)
ACTIVE_STATUSES = ("queued", "attaching", "running")


def snapshot(conn: sqlite3.Connection) -> dict:
    project = conn.execute(
        "SELECT id, cleanup_globs, output_globs FROM projects WHERE name = ?",
        (PROJECT,),
    ).fetchone()
    groups = conn.execute(
        """
        SELECT cleanup_globs, COUNT(*) AS count
        FROM tasks
        WHERE project = ? AND status IN (?, ?, ?)
        GROUP BY cleanup_globs
        ORDER BY cleanup_globs
        """,
        (PROJECT, *ACTIVE_STATUSES),
    ).fetchall()
    destructive = conn.execute(
        """
        SELECT COUNT(*)
        FROM tasks
        WHERE project = ? AND status IN (?, ?, ?)
          AND cleanup_globs = ?
        """,
        (PROJECT, *ACTIVE_STATUSES, UNSAFE_CLEANUP),
    ).fetchone()[0]
    return {
        "project": dict(project) if project else None,
        "active_cleanup_groups": [dict(row) for row in groups],
        "active_destructive": int(destructive),
    }


def wait_between_ticks(health_url: str, timeout_seconds: int) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last = None
    while time.monotonic() < deadline:
        with urlopen(health_url, timeout=10) as response:
            last = json.load(response)
        in_progress = last.get("tick_in_progress_seconds")
        if last.get("scheduler_thread_alive") and in_progress is None:
            return last
        time.sleep(0.5)
    raise TimeoutError(f"scheduler did not reach a between-tick gap: {last}")


def verified_backup(source: sqlite3.Connection, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"manual-pre-cleanup-glob-{stamp}.db"
    target = sqlite3.connect(backup_path)
    try:
        source.backup(target)
        integrity = target.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"backup integrity check failed: {integrity}")
    finally:
        target.close()
    if not backup_path.is_file() or backup_path.stat().st_size == 0:
        raise RuntimeError(f"backup was not created: {backup_path}")
    return backup_path


def apply_repair(conn: sqlite3.Connection) -> tuple[int, dict]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        before = snapshot(conn)
        project = before["project"]
        if project is None:
            raise RuntimeError(f"project not found: {PROJECT}")
        if project["cleanup_globs"] not in {UNSAFE_CLEANUP, SAFE_CLEANUP}:
            raise RuntimeError(f"unexpected project cleanup_globs: {project['cleanup_globs']!r}")
        unexpected = [
            group for group in before["active_cleanup_groups"]
            if group["cleanup_globs"] not in {UNSAFE_CLEANUP, SAFE_CLEANUP}
        ]
        if unexpected:
            raise RuntimeError(f"unexpected active task cleanup_globs: {unexpected}")

        cursor = conn.execute(
            """
            UPDATE tasks
            SET cleanup_globs = ?
            WHERE project = ? AND status IN (?, ?, ?)
              AND cleanup_globs = ?
            """,
            (SAFE_CLEANUP, PROJECT, *ACTIVE_STATUSES, UNSAFE_CLEANUP),
        )
        changed = int(cursor.rowcount)
        conn.execute(
            """
            UPDATE projects
            SET cleanup_globs = ?, output_globs = ?, updated_at = CURRENT_TIMESTAMP
            WHERE name = ?
            """,
            (SAFE_CLEANUP, SAFE_OUTPUTS, PROJECT),
        )
        conn.execute(
            """
            INSERT INTO scheduler_events(kind, message, entity_type, entity_id)
            VALUES(?, ?, ?, ?)
            """,
            (
                "maintenance",
                f"removed shared simulation cleanup from {changed} active {PROJECT} tasks",
                "project",
                str(project["id"]),
            ),
        )
        after = snapshot(conn)
        if after["active_destructive"] != 0:
            raise RuntimeError(f"destructive task rows remain: {after}")
        if after["project"]["cleanup_globs"] != SAFE_CLEANUP:
            raise RuntimeError(f"project cleanup readback failed: {after}")
        conn.commit()
        return changed, after
    except Exception:
        conn.rollback()
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/slurm_scheduler.db")
    parser.add_argument("--backup-dir", default="data/backups")
    parser.add_argument("--health-url", default="http://127.0.0.1:8000/api/health")
    parser.add_argument("--wait-seconds", type=int, default=900)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        before = snapshot(conn)
        print(json.dumps({"mode": "apply" if args.apply else "dry-run", "before": before}, indent=2))
        if not args.apply:
            return 0
        health = wait_between_ticks(args.health_url, args.wait_seconds)
        backup = verified_backup(conn, Path(args.backup_dir).resolve())
        changed, after = apply_repair(conn)
        print(json.dumps({
            "backup": str(backup),
            "scheduler_gap_after": health.get("last_tick_completed_at"),
            "changed_tasks": changed,
            "after": after,
        }, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
