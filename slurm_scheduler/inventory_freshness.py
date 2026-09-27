"""Age of cached Slurm inventory used by placement decisions."""

from datetime import datetime, timezone


def summarize_freshness(rows: list[dict], interval_seconds: int, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    observed = []
    for row in rows:
        raw = str(row.get("observed_at") or "").strip()
        if not raw:
            continue
        try:
            stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        observed.append(stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp)
    newest = max(observed) if observed else None
    age = max(0, int((now - newest).total_seconds())) if newest else None
    threshold = max(30, int(interval_seconds))
    return {
        "rows": len(rows),
        "observed_at": newest.isoformat() if newest else None,
        "age_seconds": age,
        "stale": age is None or age > threshold,
        "threshold_seconds": threshold,
    }
