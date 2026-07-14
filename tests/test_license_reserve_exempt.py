from __future__ import annotations

import tempfile
import time
import unittest

from slurm_scheduler.config import AccountConfig
from slurm_scheduler.db import Database
from slurm_scheduler.models import AedtBackend, SchedulingProfile, TaskCreate
from slurm_scheduler.scheduler import Scheduler


class LicenseReserveExemptTests(unittest.TestCase):
    FEATURE = "electronics_desktop"
    EXEMPT_PROJECT = "_aedt_pool_hosts"
    FLEET_PROJECT = "FEA_FLEET"

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(f"{self.tmp.name}/scheduler.db")
        self.db.init()
        self.accounts = [
            AccountConfig("a", "host", 22, "a", "key", "/work", 4, 10, 10)
        ]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_scheduler(
        self, reserve_exempt_projects: list[str] | None = None
    ) -> Scheduler:
        kwargs = {}
        if reserve_exempt_projects is not None:
            kwargs["license_admission_reserve_exempt_projects"] = (
                reserve_exempt_projects
            )
        return Scheduler(
            self.db,
            self.accounts,
            30,
            license_monitor_enabled=True,
            license_monitor_lmutil_path="/opt/lmutil",
            license_monitor_license_server="1055@license",
            license_admission_enabled=True,
            license_admission_snapshot_max_age_seconds=120,
            license_admission_reserve_by_feature={self.FEATURE: 10},
            license_admission_persistent_cost_by_project={
                self.EXEMPT_PROJECT: {self.FEATURE: 1},
                self.FLEET_PROJECT: {self.FEATURE: 1},
            },
            **kwargs,
        )

    def make_task(
        self,
        project: str,
        *,
        aedt_backend: str = AedtBackend.STANDALONE.value,
    ) -> int:
        return self.db.create_task(
            TaskCreate(
                f"licensed-{project}",
                "~/case",
                "run",
                cpus=4,
                memory_mb=8192,
                project=project,
                scheduling_profile=SchedulingProfile.FEA_BURSTY.value,
                aedt_backend=aedt_backend,
            )
        )

    def set_snapshot(self, scheduler: Scheduler, *, used: int, total: int) -> None:
        with scheduler._task_assignment_lock:
            scheduler._license_usage = {
                "checked_at": scheduler._now().isoformat(),
                "query_started_at": scheduler._now().isoformat(),
                "server": "1055@license",
                "server_up": True,
                "features": [
                    {"feature": self.FEATURE, "used": used, "total": total}
                ],
                "in_use": [],
                "error": "",
            }
            scheduler._license_snapshot_completed_monotonic = time.monotonic()
            scheduler._license_snapshot_query_started_at = scheduler._now()
            scheduler._license_last_successful_checked_at = scheduler._license_usage[
                "checked_at"
            ]

    def task_admission(
        self, scheduler: Scheduler, task_id: int
    ) -> tuple[bool, str]:
        with scheduler._task_assignment_lock:
            return scheduler._license_task_admitted_locked(self.db.get_task(task_id))

    def test_exempt_project_admits_in_reserve_band_while_fleet_is_blocked(self) -> None:
        scheduler = self.make_scheduler([self.EXEMPT_PROJECT])
        self.set_snapshot(scheduler, used=95, total=100)

        exempt_admitted, exempt_reason = self.task_admission(
            scheduler, self.make_task(self.EXEMPT_PROJECT)
        )
        fleet_admitted, fleet_reason = self.task_admission(
            scheduler, self.make_task(self.FLEET_PROJECT)
        )
        warm_spares, warm_spare_reason = scheduler.aedt_pool_warm_spare_admission(1)

        self.assertTrue(exempt_admitted)
        self.assertEqual(exempt_reason, "")
        self.assertFalse(fleet_admitted)
        self.assertIn("license capacity exhausted", fleet_reason)
        self.assertEqual(warm_spares, 1)
        self.assertEqual(warm_spare_reason, "")

    def test_exempt_project_is_blocked_when_candidate_exceeds_total(self) -> None:
        scheduler = self.make_scheduler([self.EXEMPT_PROJECT])
        self.set_snapshot(scheduler, used=100, total=100)

        admitted, reason = self.task_admission(
            scheduler, self.make_task(self.EXEMPT_PROJECT)
        )
        warm_spares, warm_spare_reason = scheduler.aedt_pool_warm_spare_admission(1)

        self.assertFalse(admitted)
        self.assertIn("license capacity exhausted", reason)
        self.assertEqual(warm_spares, 0)
        self.assertIn("license capacity exhausted", warm_spare_reason)

    def test_default_without_exempt_projects_keeps_reserved_capacity(self) -> None:
        scheduler = self.make_scheduler()
        self.set_snapshot(scheduler, used=95, total=100)

        admitted, reason = self.task_admission(
            scheduler, self.make_task(self.EXEMPT_PROJECT)
        )
        warm_spares, warm_spare_reason = scheduler.aedt_pool_warm_spare_admission(1)

        self.assertFalse(admitted)
        self.assertIn("license capacity exhausted", reason)
        self.assertEqual(warm_spares, 0)
        self.assertIn("license capacity exhausted", warm_spare_reason)

    def test_pooled_task_does_not_consume_a_desktop_seat(self) -> None:
        scheduler = self.make_scheduler()
        # The reserve leaves an admission capacity of 90, so another desktop
        # seat would exceed capacity even though a pooled client needs none.
        self.set_snapshot(scheduler, used=90, total=100)

        pooled_task_id = self.make_task(
            self.FLEET_PROJECT,
            aedt_backend=AedtBackend.POOLED.value,
        )
        pooled_admitted, pooled_reason = self.task_admission(
            scheduler,
            pooled_task_id,
        )
        standalone_admitted, standalone_reason = self.task_admission(
            scheduler,
            self.make_task(
                self.FLEET_PROJECT,
                aedt_backend=AedtBackend.STANDALONE.value,
            ),
        )

        self.assertTrue(pooled_admitted)
        self.assertEqual(pooled_reason, "")
        self.assertFalse(standalone_admitted)
        self.assertIn("license capacity exhausted", standalone_reason)

        scheduler.license_admission_persistent_cost_by_project[
            self.FLEET_PROJECT
        ]["solver_feature"] = 2
        pooled_costs, profile_error = scheduler._license_costs_for_task(
            self.db.get_task(pooled_task_id)
        )
        self.assertEqual(pooled_costs, {"solver_feature": 2})
        self.assertEqual(profile_error, "")
