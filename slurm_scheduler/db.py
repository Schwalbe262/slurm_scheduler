from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import AllocationStatus, JobCreate, JobStatus, TaskCreate, TaskStatus


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
    gpus INTEGER NOT NULL DEFAULT 0,
    gpu_model TEXT NOT NULL DEFAULT '',
    partition TEXT NOT NULL DEFAULT 'auto',
    node_name TEXT NOT NULL DEFAULT '',
    exclusive_node INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    timeout_seconds INTEGER NOT NULL DEFAULT 0,
    dedupe_key TEXT NOT NULL DEFAULT '',
    max_workers_per_node INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '',
    exit_code INTEGER,
    status TEXT NOT NULL,
    allocation_id INTEGER,
    account_name TEXT,
    remote_dir TEXT NOT NULL DEFAULT '',
    stdout_path TEXT NOT NULL DEFAULT '',
    stderr_path TEXT NOT NULL DEFAULT '',
    exit_code_path TEXT NOT NULL DEFAULT '',
    wrapper_pid TEXT NOT NULL DEFAULT '',
    failure_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attached_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_allocation_id ON tasks(allocation_id);
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._ensure_columns(conn)

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
                "gpus": "INTEGER NOT NULL DEFAULT 0",
                "gpu_model": "TEXT NOT NULL DEFAULT ''",
                "partition": "TEXT NOT NULL DEFAULT 'auto'",
                "node_name": "TEXT NOT NULL DEFAULT ''",
                "exclusive_node": "INTEGER NOT NULL DEFAULT 0",
                "priority": "INTEGER NOT NULL DEFAULT 0",
                "timeout_seconds": "INTEGER NOT NULL DEFAULT 0",
                "dedupe_key": "TEXT NOT NULL DEFAULT ''",
                "max_workers_per_node": "INTEGER NOT NULL DEFAULT 0",
                "payload_json": "TEXT NOT NULL DEFAULT ''",
                "exit_code": "INTEGER",
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
                    name, remote_cwd, command, env_setup, required_capability, env_profile, account_name, cpus, memory_mb,
                    gpus, gpu_model, partition, node_name, exclusive_node, priority, timeout_seconds, dedupe_key,
                    max_workers_per_node, payload_json, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.name,
                    task.remote_cwd,
                    task.command,
                    task.env_setup,
                    task.required_capability,
                    task.env_profile,
                    task.account_name,
                    task.cpus,
                    task.memory_mb,
                    task.gpus,
                    task.gpu_model,
                    task.partition,
                    task.node_name,
                    int(task.exclusive_node),
                    task.priority,
                    task.timeout_seconds,
                    task.dedupe_key,
                    task.max_workers_per_node,
                    task.payload_json,
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

    def list_tasks(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(row) for row in rows]

    def list_tasks_by_statuses(self, statuses: list[str], limit: int = 200) -> list[dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks WHERE status IN ({placeholders}) ORDER BY id DESC LIMIT ?",
                (*statuses, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def count_tasks_by_statuses(self, statuses: list[str]) -> int:
        if not statuses:
            return 0
        placeholders = ",".join("?" for _ in statuses)
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM tasks WHERE status IN ({placeholders})",
                tuple(statuses),
            ).fetchone()
            return int(row["count"]) if row else 0

    def next_queued_task(self) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY id ASC LIMIT 1",
                (TaskStatus.QUEUED.value,),
            ).fetchone()
            return dict(row) if row else None

    def update_task(self, task_id: int, **fields: Any) -> None:
        self._update_row("tasks", task_id, fields)

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

    def next_queued_job(self) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY id ASC LIMIT 1",
                (JobStatus.QUEUED.value,),
            ).fetchone()
            return dict(row) if row else None

    def update_job(self, job_id: int, **fields: Any) -> None:
        self._update_row("jobs", job_id, fields)

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
