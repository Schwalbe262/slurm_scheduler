# 2026-07-13 Windows WEB Listener Loss (WinError 64)

## Symptom

A burst of remote-log requests was followed by a Windows accept-path failure (`WinError 64`).
The Python/Uvicorn child PID remained alive, but the IPv4 port 8000 listener disappeared.  The
scheduler tick watchdog only observes scheduler-thread progress, so it did not restart this
half-alive WEB process.

## Containment

- Remote file reads remain bounded by `web_remote_read_concurrency` and the existing read cache.
- `python -m slurm_scheduler` now starts a stable parent supervisor and a replaceable Uvicorn child.
- The parent probes the local TCP listener independently of `/api/health` and scheduler tick state.
- After startup grace, three consecutive listener failures restart only the child.
- A child restart does not issue `scancel`, delete tasks, or close Slurm allocations.  SQLite and
  remote Slurm steps remain authoritative and the replacement worker resumes reconciliation.

Default policy:

```yaml
web_listener_watchdog_enabled: true
web_listener_probe_interval_seconds: 5
web_listener_startup_grace_seconds: 30
web_listener_failure_threshold: 3
web_listener_probe_timeout_seconds: 2
web_listener_restart_delay_seconds: 5
```

The existing `scripts/start_web.cmd` loop remains the outer recovery path if the supervisor itself
exits.  Set `SLURM_SCHEDULER_UVICORN_WORKER=1` only for a directly managed Uvicorn child or tests.

## Verification before deployment

1. Run the full test suite.
2. Start a disposable scheduler instance on another port.
3. Confirm parent and child PIDs are distinct.
4. Confirm killing only the child causes a replacement child and the listener returns.
5. Fault-inject a failed listener probe while the fake child remains alive; confirm thresholded
   restart, not a restart on a single transient failure.
6. Confirm pre-existing fake/live allocation rows and Slurm jobs are not cancelled.
7. Deploy only in a maintenance window; do not restart the active campaign scheduler during recovery.
