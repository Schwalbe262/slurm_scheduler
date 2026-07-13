from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import AedtBackend, AllocationStatus, JobCreate, JobStatus, SchedulingProfile, TaskCreate, TaskStatus, normalize_aedt_backend


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_url TEXT NOT NULL,
    git_ref TEXT NOT NULL,
    entrypoint TEXT NOT NULL,
    arguments TEXT NOT NULL DEFAULT '',
    env_setup TEXT NOT NULL DEFAULT '',
    required_capability TEXT NOT NULL DEFAULT '',
    env_profile TEXT NOT NULL DEFAULT '',
    partition TEXT NOT NULL DEFAULT '',
    time_limit TEXT NOT NULL DEFAULT '01:00:00',
    cpus INTEGER NOT NULL DEFAULT 1,
    memory TEXT NOT NULL DEFAULT '4G',
    gpus INTEGER NOT NULL DEFAULT 0,
    gpu_model TEXT NOT NULL DEFAULT '',
    job_name TEXT NOT NULL DEFAULT 'web-job',
    job_mode TEXT NOT NULL DEFAULT 'python_git',
    remote_path TEXT NOT NULL DEFAULT '',
    simulations_per_job INTEGER NOT NULL DEFAULT 1,
    cpus_per_simulation INTEGER NOT NULL DEFAULT 1,
    simulation_start INTEGER NOT NULL DEFAULT 1,
    simulation_count INTEGER NOT NULL DEFAULT 1,
    node_name TEXT NOT NULL DEFAULT '',
    exclusive_node INTEGER NOT NULL DEFAULT 0,
    mem_per_simulation_gb REAL NOT NULL DEFAULT 1,
    max_workers_per_job INTEGER NOT NULL DEFAULT 32,
    initial_workers INTEGER NOT NULL DEFAULT 1,
    load_target REAL NOT NULL DEFAULT 0.75,
    ramp_interval_seconds INTEGER NOT NULL DEFAULT 900,
    status TEXT NOT NULL,
    account_name TEXT,
    slurm_job_id TEXT,
    remote_job_dir TEXT,
    stdout_path TEXT,
    stderr_path TEXT,
    failure_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    submitted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_slurm_job_id ON jobs(slurm_job_id);

CREATE TABLE IF NOT EXISTS token_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    project TEXT NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reset_cycle TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_token_usage_recorded_at ON token_usage(recorded_at);
CREATE INDEX IF NOT EXISTS idx_token_usage_provider_project ON token_usage(provider, project);

CREATE TABLE IF NOT EXISTS node_inventory (
    node_name TEXT PRIMARY KEY,
    partition TEXT NOT NULL,
    cpus INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    gpu_model TEXT NOT NULL DEFAULT '',
    gpu_count INTEGER NOT NULL DEFAULT 0,
    gpu_used_count INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    cpu_model TEXT NOT NULL DEFAULT '',
    sockets INTEGER NOT NULL DEFAULT 0,
    cores_per_socket INTEGER NOT NULL DEFAULT 0,
    threads_per_core INTEGER NOT NULL DEFAULT 0,
    cpu_score INTEGER NOT NULL DEFAULT 0,
    observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_node_inventory_partition ON node_inventory(partition);

CREATE TABLE IF NOT EXISTS pestat_nodes (
    hostname TEXT PRIMARY KEY,
    partition TEXT NOT NULL,
    state TEXT NOT NULL,
    cpu_used INTEGER NOT NULL,
    cpu_total INTEGER NOT NULL,
    cpu_load REAL NOT NULL,
    memory_mb INTEGER NOT NULL,
    free_memory_mb INTEGER NOT NULL,
    observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pestat_nodes_partition ON pestat_nodes(partition);

CREATE TABLE IF NOT EXISTS allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL,
    partition TEXT NOT NULL DEFAULT '',
    node_name TEXT NOT NULL DEFAULT '',
    slurm_job_id TEXT,
    state TEXT NOT NULL,
    total_cpus INTEGER NOT NULL DEFAULT 1,
    free_cpus INTEGER NOT NULL DEFAULT 1,
    total_memory_mb INTEGER NOT NULL DEFAULT 0,
    free_memory_mb INTEGER NOT NULL DEFAULT 0,
    total_gpus INTEGER NOT NULL DEFAULT 0,
    free_gpus INTEGER NOT NULL DEFAULT 0,
    gpu_model TEXT NOT NULL DEFAULT '',
    resource_pool TEXT NOT NULL DEFAULT 'cpu',
    exclusive_node INTEGER NOT NULL DEFAULT 0,
    remote_dir TEXT NOT NULL DEFAULT '',
    stdout_path TEXT NOT NULL DEFAULT '',
    stderr_path TEXT NOT NULL DEFAULT '',
    failure_message TEXT NOT NULL DEFAULT '',
    drain_reason TEXT NOT NULL DEFAULT '',
    pending_reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    submitted_at TEXT,
    started_at TEXT,
    last_active_at TEXT,
    drain_at TEXT,
    closed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_allocations_state ON allocations(state);
CREATE INDEX IF NOT EXISTS idx_allocations_slurm_job_id ON allocations(slurm_job_id);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    remote_cwd TEXT NOT NULL,
    command TEXT NOT NULL,
    env_setup TEXT NOT NULL DEFAULT '',
    required_capability TEXT NOT NULL DEFAULT '',
    env_profile TEXT NOT NULL DEFAULT '',
    cpus INTEGER NOT NULL DEFAULT 1,
    memory_mb INTEGER NOT NULL DEFAULT 4096,
    scheduling_profile TEXT NOT NULL DEFAULT 'standard',
    aedt_backend TEXT NOT NULL DEFAULT 'standalone',
    gpus INTEGER NOT NULL DEFAULT 0,
    gpu_model TEXT NOT NULL DEFAULT '',
    partition TEXT NOT NULL DEFAULT 'auto',
    node_name TEXT NOT NULL DEFAULT '',
    exclusive_node INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    timeout_seconds INTEGER NOT NULL DEFAULT 0,
    dedupe_key TEXT NOT NULL DEFAULT '',
    max_workers_per_node INTEGER NOT NULL DEFAULT 0,
    same_node_as_task_id INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '',
    cleanup_globs TEXT NOT NULL DEFAULT '',
    project TEXT NOT NULL DEFAULT '',
    entrypoint TEXT NOT NULL DEFAULT '',
    exit_code INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    allocation_id INTEGER,
    account_name TEXT,
    requested_account_name TEXT,
    remote_dir TEXT NOT NULL DEFAULT '',
    stdout_path TEXT NOT NULL DEFAULT '',
    stderr_path TEXT NOT NULL DEFAULT '',
    exit_code_path TEXT NOT NULL DEFAULT '',
    wrapper_pid TEXT NOT NULL DEFAULT '',
    attach_token TEXT NOT NULL DEFAULT '',
    launch_started_at TEXT,
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attached_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_allocation_id ON tasks(allocation_id);

CREATE TABLE IF NOT EXISTS env_sync_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reference_account TEXT NOT NULL,
    source_env_name TEXT NOT NULL,
    target_env_name TEXT NOT NULL,
    target_accounts TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_env_sync_jobs_status ON env_sync_jobs(status);

CREATE TABLE IF NOT EXISTS env_sync_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_job_id INTEGER NOT NULL,
    account_name TEXT NOT NULL,
    status TEXT NOT NULL,
    remote_dir TEXT NOT NULL DEFAULT '',
    remote_pid TEXT NOT NULL DEFAULT '',
    log_path TEXT NOT NULL DEFAULT '',
    archive_path TEXT NOT NULL DEFAULT '',
    installed_prefix TEXT NOT NULL DEFAULT '',
    backup_path TEXT NOT NULL DEFAULT '',
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_env_sync_targets_job ON env_sync_targets(sync_job_id);

CREATE TABLE IF NOT EXISTS account_env_overlays (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL,
    env_name TEXT NOT NULL,
    capability TEXT NOT NULL,
    env_profile TEXT NOT NULL,
    env_setup TEXT NOT NULL,
    installed_prefix TEXT NOT NULL DEFAULT '',
    sync_job_id INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_name, env_name)
);

CREATE INDEX IF NOT EXISTS idx_account_env_overlays_account ON account_env_overlays(account_name);

CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    repos TEXT NOT NULL DEFAULT '[]',
    setup TEXT NOT NULL DEFAULT '',
    entrypoints TEXT NOT NULL DEFAULT '[]',
    cleanup_globs TEXT NOT NULL DEFAULT '',
    output_globs TEXT NOT NULL DEFAULT '',
    sim_subdir TEXT NOT NULL DEFAULT 'simulation',
    auto_pull INTEGER NOT NULL DEFAULT 0,
    max_active_tasks INTEGER NOT NULL DEFAULT 0,
    aedt_backend TEXT NOT NULL DEFAULT 'standalone',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS project_deployments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    account_name TEXT NOT NULL,
    status TEXT NOT NULL,
    remote_dir TEXT NOT NULL DEFAULT '',
    deployed_refs TEXT NOT NULL DEFAULT '{}',
    log_path TEXT NOT NULL DEFAULT '',
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(project_id, account_name)
);

CREATE INDEX IF NOT EXISTS idx_project_deployments_project ON project_deployments(project_id);

CREATE TABLE IF NOT EXISTS scheduler_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scheduler_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    kind TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT '',
    entity_id TEXT NOT NULL DEFAULT '',
    account_name TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_scheduler_events_created_at ON scheduler_events(created_at);
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._ensure_columns(conn)

    def get_setting(self, key: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM scheduler_settings WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else None

    def set_setting(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO scheduler_settings(key, value, updated_at) VALUES(?, ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
                (key, value),
            )

    def allocation_has_aedt_pool_claim(self, allocation_id: int) -> bool:
        """Fail-safe ownership check for the opt-in AEDT session pool.

        The table is installed by the optional pool service, so legacy/test
        databases without it simply have no external claim.
        """
        with self.connect() as conn:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'aedt_sessions'"
            ).fetchone()
            if not table:
                return False
            row = conn.execute(
                """
                SELECT 1 FROM aedt_sessions
                WHERE allocation_id = ?
                  AND state IN ('starting','ready','busy','draining','unhealthy')
                LIMIT 1
                """,
                (int(allocation_id),),
            ).fetchone()
            return bool(row)

    def record_event(
        self,
        kind: str,
        message: str,
        entity_type: str = "",
        entity_id: str = "",
        account_name: str = "",
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO scheduler_events(kind, message, entity_type, entity_id, account_name) VALUES(?, ?, ?, ?, ?)",
                (kind, message, entity_type, str(entity_id), account_name),
            )

    def list_events(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM scheduler_events ORDER BY id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def backup_to(self, backup_path: str) -> None:
        """Consistent online backup via sqlite's backup API (WAL-safe)."""
        Path(backup_path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            target = sqlite3.connect(backup_path)
            try:
                conn.backup(target)
            finally:
                target.close()

    def list_referenced_remote_paths(self) -> set[str]:
        queries = (
            "SELECT remote_dir FROM tasks WHERE COALESCE(remote_dir, '') != ''",
            "SELECT remote_job_dir FROM jobs WHERE COALESCE(remote_job_dir, '') != ''",
            "SELECT remote_dir FROM allocations WHERE COALESCE(remote_dir, '') != ''",
            "SELECT remote_dir FROM env_sync_targets WHERE COALESCE(remote_dir, '') != ''",
        )
        paths: set[str] = set()
        with self.connect() as conn:
            for query in queries:
                for row in conn.execute(query):
                    paths.add(str(row[0]))
        return paths

    def prune_old_rows(
        self,
        row_cutoff: str,
        event_cutoff: str,
    ) -> dict[str, int]:
        """Delete terminal rows whose remote artifacts were already cleaned
        (remote dir columns emptied) and old events, then truncate the WAL."""
        deleted: dict[str, int] = {}
        with self.connect() as conn:
            deleted["tasks"] = conn.execute(
                "DELETE FROM tasks WHERE status IN ('completed','failed','cancelled') "
                "AND COALESCE(remote_dir, '') = '' AND COALESCE(finished_at, updated_at, created_at) < ?",
                (row_cutoff,),
            ).rowcount
            deleted["jobs"] = conn.execute(
                "DELETE FROM jobs WHERE status IN ('completed','failed','cancelled') "
                "AND COALESCE(remote_job_dir, '') = '' AND COALESCE(finished_at, updated_at, created_at) < ?",
                (row_cutoff,),
            ).rowcount
            deleted["allocations"] = conn.execute(
                "DELETE FROM allocations WHERE state IN ('closed','failed') "
                "AND COALESCE(remote_dir, '') = '' AND COALESCE(closed_at, updated_at, created_at) < ?",
                (row_cutoff,),
            ).rowcount
            deleted["events"] = conn.execute(
                "DELETE FROM scheduler_events WHERE created_at < ?",
                (event_cutoff,),
            ).rowcount
        with self.connect() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return deleted

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        table_columns = {
            "jobs": {
                "required_capability": "TEXT NOT NULL DEFAULT ''",
                "env_profile": "TEXT NOT NULL DEFAULT ''",
                "job_mode": "TEXT NOT NULL DEFAULT 'python_git'",
                "remote_path": "TEXT NOT NULL DEFAULT ''",
                "simulations_per_job": "INTEGER NOT NULL DEFAULT 1",
                "cpus_per_simulation": "INTEGER NOT NULL DEFAULT 1",
                "simulation_start": "INTEGER NOT NULL DEFAULT 1",
                "simulation_count": "INTEGER NOT NULL DEFAULT 1",
                "node_name": "TEXT NOT NULL DEFAULT ''",
                "gpu_model": "TEXT NOT NULL DEFAULT ''",
                "exclusive_node": "INTEGER NOT NULL DEFAULT 0",
                "mem_per_simulation_gb": "REAL NOT NULL DEFAULT 1",
                "max_workers_per_job": "INTEGER NOT NULL DEFAULT 32",
                "initial_workers": "INTEGER NOT NULL DEFAULT 1",
                "load_target": "REAL NOT NULL DEFAULT 0.75",
                "ramp_interval_seconds": "INTEGER NOT NULL DEFAULT 900",
            },
            "tasks": {
                "required_capability": "TEXT NOT NULL DEFAULT ''",
                "env_profile": "TEXT NOT NULL DEFAULT ''",
                "account_name": "TEXT NOT NULL DEFAULT ''",
                "requested_account_name": "TEXT",
                "scheduling_profile": f"TEXT NOT NULL DEFAULT '{SchedulingProfile.STANDARD.value}'",
                "aedt_backend": f"TEXT NOT NULL DEFAULT '{AedtBackend.STANDALONE.value}'",
                "gpus": "INTEGER NOT NULL DEFAULT 0",
                "gpu_model": "TEXT NOT NULL DEFAULT ''",
                "partition": "TEXT NOT NULL DEFAULT 'auto'",
                "node_name": "TEXT NOT NULL DEFAULT ''",
                "exclusive_node": "INTEGER NOT NULL DEFAULT 0",
                "priority": "INTEGER NOT NULL DEFAULT 0",
                "timeout_seconds": "INTEGER NOT NULL DEFAULT 0",
                "dedupe_key": "TEXT NOT NULL DEFAULT ''",
                "max_workers_per_node": "INTEGER NOT NULL DEFAULT 0",
                "same_node_as_task_id": "INTEGER NOT NULL DEFAULT 0",
                "payload_json": "TEXT NOT NULL DEFAULT ''",
                "cleanup_globs": "TEXT NOT NULL DEFAULT ''",
                "project": "TEXT NOT NULL DEFAULT ''",
                "entrypoint": "TEXT NOT NULL DEFAULT ''",
                "exit_code": "INTEGER",
                "attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "attach_token": "TEXT NOT NULL DEFAULT ''",
                "launch_started_at": "TEXT",
            },
            "allocations": {
                "total_gpus": "INTEGER NOT NULL DEFAULT 0",
                "free_gpus": "INTEGER NOT NULL DEFAULT 0",
                "gpu_model": "TEXT NOT NULL DEFAULT ''",
                "resource_pool": "TEXT NOT NULL DEFAULT 'cpu'",
                "exclusive_node": "INTEGER NOT NULL DEFAULT 0",
                "pending_reason": "TEXT NOT NULL DEFAULT ''",
            },
            "node_inventory": {
                "gpu_used_count": "INTEGER NOT NULL DEFAULT 0",
            },
            "projects": {
                "output_globs": "TEXT NOT NULL DEFAULT ''",
                "max_active_tasks": "INTEGER NOT NULL DEFAULT 0",
                "aedt_backend": f"TEXT NOT NULL DEFAULT '{AedtBackend.STANDALONE.value}'",
            },
        }
        for table, columns in table_columns.items():
            existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            for name, ddl in columns.items():
                if name not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    def create_job(self, job: JobCreate) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO jobs (
                    repo_url, git_ref, entrypoint, arguments, env_setup,
                    required_capability, env_profile, account_name, partition,
                    time_limit, cpus, memory, gpus, gpu_model, job_name, job_mode, remote_path,
                    simulations_per_job, cpus_per_simulation, simulation_start,
                    simulation_count, node_name, exclusive_node, mem_per_simulation_gb,
                    max_workers_per_job, initial_workers, load_target,
                    ramp_interval_seconds, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.repo_url,
                    job.git_ref,
                    job.entrypoint,
                    job.arguments,
                    job.env_setup,
                    job.required_capability,
                    job.env_profile,
                    job.account_name,
                    job.partition,
                    job.time_limit,
                    job.cpus,
                    job.memory,
                    job.gpus,
                    job.gpu_model,
                    job.job_name,
                    job.job_mode,
                    job.remote_path,
                    job.simulations_per_job,
                    job.cpus_per_simulation,
                    job.simulation_start,
                    job.simulation_count,
                    job.node_name,
                    int(job.exclusive_node),
                    job.mem_per_simulation_gb,
                    job.max_workers_per_job,
                    job.initial_workers,
                    job.load_target,
                    job.ramp_interval_seconds,
                    JobStatus.QUEUED.value,
                ),
            )
            return int(cursor.lastrowid)

    def create_task(self, task: TaskCreate) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO tasks (
                    name, remote_cwd, command, env_setup, required_capability, env_profile,
                    account_name, requested_account_name, cpus, memory_mb,
                    scheduling_profile, aedt_backend, gpus, gpu_model, partition, node_name, exclusive_node, priority, timeout_seconds, dedupe_key,
                    max_workers_per_node, same_node_as_task_id, payload_json, cleanup_globs, project, entrypoint, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.name,
                    task.remote_cwd,
                    task.command,
                    task.env_setup,
                    task.required_capability,
                    task.env_profile,
                    task.account_name,
                    task.account_name,
                    task.cpus,
                    task.memory_mb,
                    task.scheduling_profile,
                    normalize_aedt_backend(task.aedt_backend),
                    task.gpus,
                    task.gpu_model,
                    task.partition,
                    task.node_name,
                    int(task.exclusive_node),
                    task.priority,
                    task.timeout_seconds,
                    task.dedupe_key,
                    task.max_workers_per_node,
                    max(0, int(task.same_node_as_task_id or 0)),
                    task.payload_json,
                    task.cleanup_globs,
                    task.project,
                    task.entrypoint,
                    TaskStatus.QUEUED.value,
                ),
            )
            return int(cursor.lastrowid)

    def find_active_task_by_dedupe_key(self, dedupe_key: str) -> dict[str, Any] | None:
        if not dedupe_key:
            return None
        terminal = (TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM tasks
                WHERE dedupe_key = ? AND status NOT IN (?, ?, ?)
                ORDER BY id ASC
                LIMIT 1
                """,
                (dedupe_key, *terminal),
            ).fetchone()
            return dict(row) if row else None

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            return dict(row) if row else None

    def list_tasks(
        self,
        limit: int = 200,
        *,
        project: str = "",
        name_prefix: str = "",
        statuses: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if project:
            clauses.append("project = ?")
            params.append(project)
        if name_prefix:
            escaped = name_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("name LIKE ? ESCAPE '\\'")
            params.append(f"{escaped}%")
        if statuses is not None:
            if not statuses:
                return []
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks{where} ORDER BY id DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_tasks_with_active(self, limit: int = 200, active_limit: int = 5000) -> list[dict[str, Any]]:
        active_statuses = (TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value)
        placeholders = ",".join("?" for _ in active_statuses)
        with self.connect() as conn:
            recent_rows = conn.execute("SELECT * FROM tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            active_rows = conn.execute(
                f"SELECT * FROM tasks WHERE status IN ({placeholders}) ORDER BY id DESC LIMIT ?",
                (*active_statuses, active_limit),
            ).fetchall()
        by_id: dict[int, dict[str, Any]] = {}
        for row in [*recent_rows, *active_rows]:
            item = dict(row)
            by_id[int(item["id"])] = item
        return sorted(by_id.values(), key=lambda item: int(item["id"]), reverse=True)

    def _name_contains_filter(self, name_contains: str) -> tuple[str, tuple[str, ...]]:
        needle = (name_contains or "").strip()
        if not needle:
            return "", ()
        escaped = needle.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return " AND name LIKE ? ESCAPE '\\'", (f"%{escaped}%",)

    def list_tasks_by_statuses(
        self,
        statuses: list[str],
        limit: int = 200,
        name_contains: str = "",
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        name_filter, name_params = self._name_contains_filter(name_contains)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks WHERE status IN ({placeholders}){name_filter} ORDER BY id DESC LIMIT ? OFFSET ?",
                (*statuses, *name_params, limit, max(0, int(offset))),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_live_task_claims_for_allocation(self, allocation_id: int) -> list[dict[str, Any]]:
        """Return every task claim that still owns an allocation.

        This query is intentionally scoped by allocation and has no global
        LIMIT.  Allocation shutdown is a safety boundary: a busy cluster with
        more than the normal task-list limit must not hide an older live claim
        and make its parent Slurm allocation look idle.
        """
        active_statuses = (TaskStatus.ATTACHING.value, TaskStatus.RUNNING.value)
        placeholders = ",".join("?" for _ in active_statuses)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE allocation_id = ?
                  AND status IN ({placeholders})
                ORDER BY id ASC
                """,
                (int(allocation_id), *active_statuses),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_finished_tasks_for_cleanup(
        self,
        statuses: list[str],
        finished_before: str,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE status IN ({placeholders})
                  AND COALESCE(remote_dir, '') != ''
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                ORDER BY finished_at ASC, id ASC
                LIMIT ?
                """,
                (*statuses, finished_before, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def count_tasks_by_statuses(self, statuses: list[str], name_contains: str = "") -> int:
        if not statuses:
            return 0
        placeholders = ",".join("?" for _ in statuses)
        name_filter, name_params = self._name_contains_filter(name_contains)
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM tasks WHERE status IN ({placeholders}){name_filter}",
                (*statuses, *name_params),
            ).fetchone()
            return int(row["count"]) if row else 0

    def count_tasks_grouped_by_status(self, name_prefix: str = "") -> dict[str, int]:
        prefix_filter = ""
        params: tuple[Any, ...] = ()
        if name_prefix:
            escaped = name_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            prefix_filter = " WHERE name LIKE ? ESCAPE '\\'"
            params = (f"{escaped}%",)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT status, COUNT(*) AS count FROM tasks{prefix_filter} GROUP BY status",
                params,
            ).fetchall()
            return {str(row["status"]): int(row["count"]) for row in rows}

    def next_queued_task(self) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY id ASC LIMIT 1",
                (TaskStatus.QUEUED.value,),
            ).fetchone()
            return dict(row) if row else None

    def update_task(self, task_id: int, **fields: Any) -> None:
        self._update_row("tasks", task_id, fields)

    def update_task_if_status(self, task_id: int, expected_statuses: list[str], **fields: Any) -> bool:
        """Optimistic update: apply only while the task is still in one of the
        expected statuses. Returns False when a concurrent transition (requeue,
        cancel) won the race."""
        if not fields or not expected_statuses:
            return False
        fields["updated_at"] = fields.get("updated_at", "CURRENT_TIMESTAMP")
        assignments = []
        values: list[Any] = []
        for key, value in fields.items():
            if value == "CURRENT_TIMESTAMP":
                assignments.append(f"{key} = CURRENT_TIMESTAMP")
            else:
                assignments.append(f"{key} = ?")
                values.append(value)
        placeholders = ",".join("?" for _ in expected_statuses)
        values.append(task_id)
        values.extend(expected_statuses)
        with self.connect() as conn:
            cursor = conn.execute(
                f"UPDATE tasks SET {', '.join(assignments)} WHERE id = ? AND status IN ({placeholders})",
                values,
            )
            return cursor.rowcount > 0

    def update_task_if_attach_claim(
        self,
        task_id: int,
        expected_attach_token: str,
        *,
        require_launch_not_started: bool = False,
        **fields: Any,
    ) -> bool:
        """Update one exact attach attempt, protecting against requeue/reattach ABA races."""
        token = str(expected_attach_token or "")
        if not token or not fields:
            return False
        fields["updated_at"] = fields.get("updated_at", "CURRENT_TIMESTAMP")
        assignments = []
        values: list[Any] = []
        for key, value in fields.items():
            if value == "CURRENT_TIMESTAMP":
                assignments.append(f"{key} = CURRENT_TIMESTAMP")
            else:
                assignments.append(f"{key} = ?")
                values.append(value)
        values.extend((task_id, TaskStatus.ATTACHING.value, token))
        launch_guard = " AND launch_started_at IS NULL" if require_launch_not_started else ""
        with self.connect() as conn:
            cursor = conn.execute(
                f"UPDATE tasks SET {', '.join(assignments)} "
                "WHERE id = ? AND status = ? AND attach_token = ?"
                f"{launch_guard}",
                values,
            )
            return cursor.rowcount > 0

    def create_allocation(
        self,
        account_name: str,
        partition: str,
        node_name: str,
        total_cpus: int,
        total_memory_mb: int,
        total_gpus: int = 0,
        gpu_model: str = "",
        resource_pool: str = "cpu",
        exclusive_node: bool = False,
    ) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO allocations (
                    account_name, partition, node_name, state, total_cpus, free_cpus,
                    total_memory_mb, free_memory_mb, total_gpus, free_gpus,
                    gpu_model, resource_pool, exclusive_node
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_name,
                    partition,
                    node_name,
                    AllocationStatus.PENDING.value,
                    total_cpus,
                    total_cpus,
                    total_memory_mb,
                    total_memory_mb,
                    total_gpus,
                    total_gpus,
                    gpu_model,
                    resource_pool,
                    int(exclusive_node),
                ),
            )
            return int(cursor.lastrowid)

    def get_allocation(self, allocation_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM allocations WHERE id = ?", (allocation_id,)).fetchone()
            return dict(row) if row else None

    def list_allocations(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM allocations ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(row) for row in rows]

    def list_allocations_with_live(self, limit: int = 200, live_limit: int = 5000) -> list[dict[str, Any]]:
        live_states = (
            AllocationStatus.PENDING.value,
            AllocationStatus.WARM.value,
            AllocationStatus.ACTIVE.value,
            AllocationStatus.DRAINING.value,
            AllocationStatus.CLOSING.value,
        )
        placeholders = ",".join("?" for _ in live_states)
        with self.connect() as conn:
            recent_rows = conn.execute("SELECT * FROM allocations ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            live_rows = conn.execute(
                f"SELECT * FROM allocations WHERE state IN ({placeholders}) ORDER BY id DESC LIMIT ?",
                (*live_states, live_limit),
            ).fetchall()
        by_id: dict[int, dict[str, Any]] = {}
        for row in [*recent_rows, *live_rows]:
            item = dict(row)
            by_id[int(item["id"])] = item
        return sorted(by_id.values(), key=lambda item: int(item["id"]), reverse=True)

    def list_closed_allocations_for_cleanup(
        self,
        states: list[str],
        closed_before: str,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        if not states:
            return []
        placeholders = ",".join("?" for _ in states)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM allocations
                WHERE state IN ({placeholders})
                  AND COALESCE(remote_dir, '') != ''
                  AND closed_at IS NOT NULL
                  AND closed_at <= ?
                ORDER BY closed_at ASC, id ASC
                LIMIT ?
                """,
                (*states, closed_before, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_allocation(self, allocation_id: int, **fields: Any) -> None:
        self._update_row("allocations", allocation_id, fields)

    def get_job(self, job_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(row) for row in rows]

    def list_finished_jobs_for_cleanup(
        self,
        statuses: list[str],
        finished_before: str,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE status IN ({placeholders})
                  AND COALESCE(remote_job_dir, '') != ''
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                ORDER BY finished_at ASC, id ASC
                LIMIT ?
                """,
                (*statuses, finished_before, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def next_queued_job(self) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY id ASC LIMIT 1",
                (JobStatus.QUEUED.value,),
            ).fetchone()
            return dict(row) if row else None

    def update_job(self, job_id: int, **fields: Any) -> None:
        self._update_row("jobs", job_id, fields)

    def create_env_sync_job(
        self,
        reference_account: str,
        source_env_name: str,
        target_env_name: str,
        target_accounts: list[str],
    ) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO env_sync_jobs (
                    reference_account, source_env_name, target_env_name, target_accounts, status
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (reference_account, source_env_name, target_env_name, json_dumps(target_accounts), "queued"),
            )
            return int(cursor.lastrowid)

    def create_env_sync_target(self, sync_job_id: int, account_name: str) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO env_sync_targets (sync_job_id, account_name, status)
                VALUES (?, ?, ?)
                """,
                (sync_job_id, account_name, "queued"),
            )
            return int(cursor.lastrowid)

    def get_env_sync_job(self, sync_job_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM env_sync_jobs WHERE id = ?", (sync_job_id,)).fetchone()
            return dict(row) if row else None

    def list_env_sync_jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM env_sync_jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(row) for row in rows]

    def list_env_sync_targets(self, sync_job_id: int | None = None, limit: int = 500) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if sync_job_id is None:
                rows = conn.execute("SELECT * FROM env_sync_targets ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM env_sync_targets WHERE sync_job_id = ? ORDER BY id ASC",
                    (sync_job_id,),
                ).fetchall()
            return [dict(row) for row in rows]

    def update_env_sync_job(self, sync_job_id: int, **fields: Any) -> None:
        self._update_row("env_sync_jobs", sync_job_id, fields)

    def update_env_sync_target(self, target_id: int, **fields: Any) -> None:
        self._update_row("env_sync_targets", target_id, fields)

    def upsert_account_env_overlay(
        self,
        account_name: str,
        env_name: str,
        installed_prefix: str,
        sync_job_id: int,
    ) -> None:
        capability = f"conda:{env_name}"
        env_setup = (
            "if [ -f \"$HOME/miniconda3/etc/profile.d/conda.sh\" ]; then source \"$HOME/miniconda3/etc/profile.d/conda.sh\"; "
            "elif [ -f \"$HOME/anaconda3/etc/profile.d/conda.sh\" ]; then source \"$HOME/anaconda3/etc/profile.d/conda.sh\"; fi\n"
            f"conda activate {env_name}"
        )
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO account_env_overlays (
                    account_name, env_name, capability, env_profile, env_setup, installed_prefix, sync_job_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_name, env_name) DO UPDATE SET
                    capability = excluded.capability,
                    env_profile = excluded.env_profile,
                    env_setup = excluded.env_setup,
                    installed_prefix = excluded.installed_prefix,
                    sync_job_id = excluded.sync_job_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (account_name, env_name, capability, env_name, env_setup, installed_prefix, sync_job_id),
            )

    def list_account_env_overlays(self, account_name: str = "") -> list[dict[str, Any]]:
        with self.connect() as conn:
            if account_name:
                rows = conn.execute(
                    "SELECT * FROM account_env_overlays WHERE account_name = ? ORDER BY env_name",
                    (account_name,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM account_env_overlays ORDER BY account_name, env_name").fetchall()
            return [dict(row) for row in rows]

    def get_account_env_overlay(self, account_name: str, env_name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM account_env_overlays WHERE account_name = ? AND env_name = ?",
                (account_name, env_name),
            ).fetchone()
            return dict(row) if row else None

    def create_project(
        self,
        name: str,
        repos: list[dict[str, Any]] | None = None,
        setup: str = "",
        entrypoints: list[dict[str, Any]] | None = None,
        cleanup_globs: str = "",
        output_globs: str = "",
        sim_subdir: str = "simulation",
        auto_pull: bool = False,
        max_active_tasks: int = 0,
        aedt_backend: str = AedtBackend.STANDALONE.value,
    ) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO projects (
                    name, repos, setup, entrypoints, cleanup_globs, output_globs, sim_subdir, auto_pull,
                    max_active_tasks, aedt_backend
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    json_dumps(repos or []),
                    setup,
                    json_dumps(entrypoints or []),
                    cleanup_globs,
                    output_globs,
                    sim_subdir,
                    1 if auto_pull else 0,
                    max(0, int(max_active_tasks)),
                    normalize_aedt_backend(aedt_backend),
                ),
            )
            return int(cursor.lastrowid)

    def update_project(self, project_id: int, **fields: Any) -> None:
        self._update_row("projects", project_id, fields)

    def get_project(self, project_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
            return dict(row) if row else None

    def get_project_by_name(self, name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM projects WHERE name = ?", (name,)).fetchone()
            return dict(row) if row else None

    def count_tasks_by_project(self, project: str, statuses: list[str] | None = None) -> int:
        if not project:
            return 0
        with self.connect() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                row = conn.execute(
                    f"SELECT COUNT(*) FROM tasks WHERE project = ? AND status IN ({placeholders})",
                    (project, *statuses),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) FROM tasks WHERE project = ?", (project,)).fetchone()
            return int(row[0]) if row else 0

    def list_projects(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM projects ORDER BY name").fetchall()
            return [dict(row) for row in rows]

    def delete_project(self, project_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM project_deployments WHERE project_id = ?", (project_id,))
            conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))

    def upsert_project_deployment(self, project_id: int, account_name: str, status: str = "queued") -> int:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO project_deployments (project_id, account_name, status)
                VALUES (?, ?, ?)
                ON CONFLICT(project_id, account_name) DO UPDATE SET
                    status = excluded.status,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (project_id, account_name, status),
            )
            row = conn.execute(
                "SELECT id FROM project_deployments WHERE project_id = ? AND account_name = ?",
                (project_id, account_name),
            ).fetchone()
            return int(row["id"])

    def update_project_deployment(self, deployment_id: int, **fields: Any) -> None:
        self._update_row("project_deployments", deployment_id, fields)

    def list_project_deployments(self, project_id: int | None = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if project_id is None:
                rows = conn.execute("SELECT * FROM project_deployments ORDER BY id DESC").fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM project_deployments WHERE project_id = ? ORDER BY account_name",
                    (project_id,),
                ).fetchall()
            return [dict(row) for row in rows]

    def get_project_deployment(self, project_id: int, account_name: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM project_deployments WHERE project_id = ? AND account_name = ?",
                (project_id, account_name),
            ).fetchone()
            return dict(row) if row else None

    def _update_row(self, table: str, row_id: int, fields: dict[str, Any]) -> None:
        if not fields:
            return
        fields["updated_at"] = fields.get("updated_at", "CURRENT_TIMESTAMP")
        assignments = []
        values: list[Any] = []
        for key, value in fields.items():
            if value == "CURRENT_TIMESTAMP":
                assignments.append(f"{key} = CURRENT_TIMESTAMP")
            else:
                assignments.append(f"{key} = ?")
                values.append(value)
        values.append(row_id)
        with self.connect() as conn:
            conn.execute(f"UPDATE {table} SET {', '.join(assignments)} WHERE id = ?", values)

    def create_token_usage(
        self,
        provider: str,
        project: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        total_tokens: int | None = None,
        note: str = "",
        reset_cycle: str = "",
    ) -> int:
        total = total_tokens if total_tokens is not None else input_tokens + output_tokens
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO token_usage (
                    provider, project, input_tokens, output_tokens, total_tokens, note, reset_cycle
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (provider, project, input_tokens, output_tokens, total, note, reset_cycle),
            )
            return int(cursor.lastrowid)

    def list_token_usage(self, limit: int = 500) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM token_usage ORDER BY recorded_at ASC, id ASC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def token_usage_summary(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT provider, project, reset_cycle, SUM(total_tokens) AS total_tokens
                FROM token_usage
                GROUP BY provider, project, reset_cycle
                ORDER BY provider, project, reset_cycle
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def replace_node_inventory(self, nodes: list[Any]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM node_inventory")
            conn.executemany(
                """
                INSERT INTO node_inventory (
                    node_name, partition, cpus, memory_mb, gpu_model, gpu_count, gpu_used_count, state,
                    cpu_model, sockets, cores_per_socket, threads_per_core, cpu_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        node.node_name,
                        node.partition,
                        node.cpus,
                        node.memory_mb,
                        node.gpu_model,
                        node.gpu_count,
                        node.gpu_used_count,
                        node.state,
                        node.cpu_model,
                        node.sockets,
                        node.cores_per_socket,
                        node.threads_per_core,
                        node.cpu_score,
                    )
                    for node in nodes
                ],
            )

    def list_node_inventory(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM node_inventory ORDER BY partition, node_name"
            ).fetchall()
            return [dict(row) for row in rows]

    def replace_pestat_nodes(self, nodes: list[Any]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM pestat_nodes")
            conn.executemany(
                """
                INSERT INTO pestat_nodes (
                    hostname, partition, state, cpu_used, cpu_total, cpu_load,
                    memory_mb, free_memory_mb
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        node.hostname,
                        node.partition,
                        node.state,
                        node.cpu_used,
                        node.cpu_total,
                        node.cpu_load,
                        node.memory_mb,
                        node.free_memory_mb,
                    )
                    for node in nodes
                ],
            )

    def list_pestat_nodes(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pestat_nodes ORDER BY partition, hostname"
            ).fetchall()
            return [dict(row) for row in rows]
