from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from .config import AccountConfig
from .db import Database
from .inventory import partition_rank
from .models import AccountSnapshot, JobStatus
from .slurm import SlurmAccountClient

LOGGER = logging.getLogger(__name__)
ClientFactory = Callable[[AccountConfig], SlurmAccountClient]


class Scheduler:
    def __init__(
        self,
        db: Database,
        accounts: list[AccountConfig],
        poll_interval_seconds: int,
        client_factory: ClientFactory = SlurmAccountClient,
    ):
        self.db = db
        self.accounts = accounts
        self.poll_interval_seconds = poll_interval_seconds
        self.client_factory = client_factory
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot_cache: tuple[float, list[AccountSnapshot]] | None = None
        self._storage_cache: dict[str, tuple[float, float | None]] = {}
        self._storage_refresh_interval_seconds = max(900, poll_interval_seconds * 20)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self.run_forever, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                LOGGER.exception("scheduler tick failed")
            self._stop.wait(self.poll_interval_seconds)

    def tick(self) -> None:
        self.refresh_submitted_jobs()
        self.submit_next_queued_job()

    def snapshots(self) -> list[AccountSnapshot]:
        now = time.time()
        if self._snapshot_cache and now - self._snapshot_cache[0] < self.poll_interval_seconds:
            return self._snapshot_cache[1]
        snapshots = []
        for account in self.accounts:
            client = self.client_factory(account)
            storage_used = self.cached_storage(account, client, now)
            snapshots.append(client.snapshot(storage_used_gb=storage_used))
        self._snapshot_cache = (now, snapshots)
        return snapshots

    def cached_storage(self, account: AccountConfig, client: SlurmAccountClient, now: float) -> float | None:
        cached = self._storage_cache.get(account.name)
        if cached and now - cached[0] < self._storage_refresh_interval_seconds:
            return cached[1]
        try:
            value = client.storage_used_gb()
        except Exception:
            value = cached[1] if cached else None
        self._storage_cache[account.name] = (now, value)
        return value

    def cached_snapshots(self) -> list[AccountSnapshot]:
        if not self._snapshot_cache:
            return []
        return self._snapshot_cache[1]

    def choose_account(self) -> AccountConfig | None:
        snapshots_by_name = {snapshot.account_name: snapshot for snapshot in self.snapshots()}
        candidates = [
            account
            for account in self.accounts
            if snapshots_by_name.get(account.name) and snapshots_by_name[account.name].available
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda account: snapshots_by_name[account.name].score)

    def submit_next_queued_job(self) -> None:
        job = self.db.next_queued_job()
        if not job:
            return
        account = self.choose_account()
        if not account:
            return
        partition = self.choose_partition(job)
        if partition and job.get("partition") != partition:
            job["partition"] = partition
            self.db.update_job(job["id"], partition=partition)
        self.db.update_job(job["id"], status=JobStatus.SUBMITTING.value, account_name=account.name)
        try:
            result = self.client_factory(account).submit(job)
        except Exception as exc:
            self.db.update_job(
                job["id"],
                status=JobStatus.FAILED.value,
                failure_message=str(exc),
                finished_at="CURRENT_TIMESTAMP",
            )
            return
        self.db.update_job(
            job["id"],
            status=JobStatus.SUBMITTED.value,
            submitted_at="CURRENT_TIMESTAMP",
            **result,
        )

    def choose_partition(self, job: dict) -> str:
        requested = (job.get("partition") or "").strip()
        if requested and requested.lower() != "auto":
            return requested
        ranked = partition_rank(self.db.list_node_inventory(), needs_gpu=int(job.get("gpus") or 0) > 0)
        if ranked:
            return ranked[0]["partition"]
        return "gpu3" if int(job.get("gpus") or 0) > 0 else "cpu2"

    def refresh_submitted_jobs(self) -> None:
        accounts_by_name = {account.name: account for account in self.accounts}
        for job in self.db.list_jobs(limit=500):
            if job["status"] not in {JobStatus.SUBMITTED.value, JobStatus.RUNNING.value}:
                continue
            if not job["account_name"] or not job["slurm_job_id"]:
                continue
            account = accounts_by_name.get(job["account_name"])
            if not account:
                continue
            try:
                status = self.client_factory(account).state(job["slurm_job_id"])
            except Exception as exc:
                LOGGER.warning("failed to refresh job %s: %s", job["id"], exc)
                continue
            updates = {"status": status.value}
            if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}:
                updates["finished_at"] = "CURRENT_TIMESTAMP"
            self.db.update_job(job["id"], **updates)

    def cancel(self, job_id: int) -> None:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError("job not found")
        if not job["account_name"] or not job["slurm_job_id"]:
            self.db.update_job(job_id, status=JobStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
            return
        account = next((item for item in self.accounts if item.name == job["account_name"]), None)
        if not account:
            raise ValueError("account not found")
        self.client_factory(account).cancel(job["slurm_job_id"])
        self.db.update_job(job_id, status=JobStatus.CANCELLED.value, finished_at="CURRENT_TIMESTAMP")
