from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import JobCreate, JobStatus


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_url TEXT NOT NULL,
    git_ref TEXT NOT NULL,
    entrypoint TEXT NOT NULL,
    arguments TEXT NOT NULL DEFAULT '',
    env_setup TEXT NOT NULL DEFAULT '',
    partition TEXT NOT NULL DEFAULT '',
    time_limit TEXT NOT NULL DEFAULT '01:00:00',
    cpus INTEGER NOT NULL DEFAULT 1,
    memory TEXT NOT NULL DEFAULT '4G',
    gpus INTEGER NOT NULL DEFAULT 0,
    job_name TEXT NOT NULL DEFAULT 'web-job',
    job_mode TEXT NOT NULL DEFAULT 'python_git',
    remote_path TEXT NOT NULL DEFAULT '',
    simulations_per_job INTEGER NOT NULL DEFAULT 1,
    cpus_per_simulation INTEGER NOT NULL DEFAULT 1,
    simulation_start INTEGER NOT NULL DEFAULT 1,
    simulation_count INTEGER NOT NULL DEFAULT 1,
    node_name TEXT NOT NULL DEFAULT '',
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
            self._ensure_job_columns(conn)

    def _ensure_job_columns(self, conn: sqlite3.Connection) -> None:
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        columns = {
            "job_mode": "TEXT NOT NULL DEFAULT 'python_git'",
            "remote_path": "TEXT NOT NULL DEFAULT ''",
            "simulations_per_job": "INTEGER NOT NULL DEFAULT 1",
            "cpus_per_simulation": "INTEGER NOT NULL DEFAULT 1",
            "simulation_start": "INTEGER NOT NULL DEFAULT 1",
            "simulation_count": "INTEGER NOT NULL DEFAULT 1",
            "node_name": "TEXT NOT NULL DEFAULT ''",
            "mem_per_simulation_gb": "REAL NOT NULL DEFAULT 1",
            "max_workers_per_job": "INTEGER NOT NULL DEFAULT 32",
            "initial_workers": "INTEGER NOT NULL DEFAULT 1",
            "load_target": "REAL NOT NULL DEFAULT 0.75",
            "ramp_interval_seconds": "INTEGER NOT NULL DEFAULT 900",
        }
        for name, ddl in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {ddl}")

    def create_job(self, job: JobCreate) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO jobs (
                    repo_url, git_ref, entrypoint, arguments, env_setup, partition,
                    time_limit, cpus, memory, gpus, job_name, job_mode, remote_path,
                    simulations_per_job, cpus_per_simulation, simulation_start,
                    simulation_count, node_name, mem_per_simulation_gb,
                    max_workers_per_job, initial_workers, load_target,
                    ramp_interval_seconds, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.repo_url,
                    job.git_ref,
                    job.entrypoint,
                    job.arguments,
                    job.env_setup,
                    job.partition,
                    job.time_limit,
                    job.cpus,
                    job.memory,
                    job.gpus,
                    job.job_name,
                    job.job_mode,
                    job.remote_path,
                    job.simulations_per_job,
                    job.cpus_per_simulation,
                    job.simulation_start,
                    job.simulation_count,
                    job.node_name,
                    job.mem_per_simulation_gb,
                    job.max_workers_per_job,
                    job.initial_workers,
                    job.load_target,
                    job.ramp_interval_seconds,
                    JobStatus.QUEUED.value,
                ),
            )
            return int(cursor.lastrowid)

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
        values.append(job_id)
        with self.connect() as conn:
            conn.execute(f"UPDATE jobs SET {', '.join(assignments)} WHERE id = ?", values)

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
                    node_name, partition, cpus, memory_mb, gpu_model, gpu_count, state,
                    cpu_model, sockets, cores_per_socket, threads_per_core, cpu_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        node.node_name,
                        node.partition,
                        node.cpus,
                        node.memory_mb,
                        node.gpu_model,
                        node.gpu_count,
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
