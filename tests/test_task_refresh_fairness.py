from __future__ import annotations

from types import SimpleNamespace

from slurm_scheduler.models import SchedulingProfile, TaskStatus
from slurm_scheduler.scheduler import Scheduler


def _task(task_id: int, profile: str = "standard", *, expired: bool = False) -> dict:
    return {
        "id": task_id,
        "name": f"task-{task_id}",
        "status": TaskStatus.RUNNING.value,
        "scheduling_profile": profile,
        "account_name": "a",
        "exit_code_path": f"/remote/{task_id}.exit",
        "expired": expired,
    }


def _bare_scheduler(limit: int) -> Scheduler:
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.task_refresh_max_per_tick = limit
    scheduler._non_fea_task_refresh_cursor_id = 0
    scheduler._fea_task_refresh_cursor_id = 0
    scheduler._prefer_fea_for_single_task_refresh = False
    return scheduler


def test_refresh_selection_rotates_both_standard_and_fea_populations() -> None:
    scheduler = _bare_scheduler(8)
    standard = [_task(index) for index in range(1, 25)]
    fea = [
        _task(index, SchedulingProfile.FEA_BURSTY.value)
        for index in range(101, 109)
    ]

    first = scheduler.tasks_to_refresh(standard + fea)
    second = scheduler.tasks_to_refresh(standard + fea)

    assert len(first) == 8
    assert len(second) == 8
    assert sum(item["scheduling_profile"] == SchedulingProfile.FEA_BURSTY.value for item in first) == 2
    assert sum(item["scheduling_profile"] == SchedulingProfile.FEA_BURSTY.value for item in second) == 2
    assert {item["id"] for item in first}.isdisjoint(item["id"] for item in second)


def test_one_refresh_slot_alternates_between_standard_and_fea() -> None:
    scheduler = _bare_scheduler(1)
    tasks = [_task(100), _task(1, SchedulingProfile.FEA_BURSTY.value)]

    first = scheduler.tasks_to_refresh(tasks)
    second = scheduler.tasks_to_refresh(tasks)

    assert first[0]["scheduling_profile"] == "standard"
    assert second[0]["scheduling_profile"] == SchedulingProfile.FEA_BURSTY.value


class _TaskDb:
    def __init__(self, tasks: list[dict]):
        self.tasks = tasks

    def list_tasks_by_statuses(self, _statuses: list[str], limit: int = 5000) -> list[dict]:
        return self.tasks[:limit]


def test_expired_fea_is_cancelled_outside_probe_sample() -> None:
    scheduler = _bare_scheduler(4)
    expired_fea = _task(999, SchedulingProfile.FEA_BURSTY.value, expired=True)
    scheduler.accounts = [SimpleNamespace(name="a")]
    scheduler.db = _TaskDb([*[_task(index) for index in range(1, 20)], expired_fea])
    scheduler.task_timed_out = lambda task: bool(task.get("expired"))
    scheduler._timestamp = lambda _value: None
    scheduler._now = lambda: 0
    cancelled: list[int] = []
    scheduler.cancel_timed_out_task = lambda task: cancelled.append(int(task["id"]))
    scheduler._fan_out_by_account = lambda _items, _callback: {}
    scheduler.recalculate_allocation_capacity = lambda: None

    scheduler.refresh_tasks()

    assert cancelled == [999]
