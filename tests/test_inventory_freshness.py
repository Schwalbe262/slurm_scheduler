from datetime import datetime, timezone

from slurm_scheduler.inventory_freshness import summarize_freshness


def test_capacity_cache_reports_missing_and_old_snapshots() -> None:
    now = datetime(2026, 9, 27, 12, 0, tzinfo=timezone.utc)
    assert summarize_freshness([], 120, now)["stale"] is True
    old = summarize_freshness([{"observed_at": "2026-09-27 11:57:00"}], 120, now)
    assert old["age_seconds"] == 180
    assert old["stale"] is True
    fresh = summarize_freshness([{"observed_at": "2026-09-27 11:59:30"}], 120, now)
    assert fresh["stale"] is False
