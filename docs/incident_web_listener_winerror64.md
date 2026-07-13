# 2026-07-13 Windows WEB Listener Loss (WinError 64)

## Symptom

A burst of remote-log requests was followed by a Windows accept-path failure (`WinError 64`).
The Python/Uvicorn child PID remained alive, but the IPv4 port 8000 listener disappeared.  The
scheduler tick watchdog only observes scheduler-thread progress, so it did not restart this
half-alive WEB process.

## Containment

- Remote file reads remain bounded by `web_remote_read_concurrency` and the existing read cache.
- `python -m slurm_scheduler` starts a parent listener monitor and one Uvicorn child.
- The parent probes the local TCP listener independently of `/api/health` and scheduler tick state.
- After startup grace, three consecutive listener failures stop the child and exit the parent.
- The external service launcher is the only restart authority, preventing a dying Python generation
  and `start_web.cmd` from both creating replacements. SQLite and remote Slurm steps remain
  authoritative when the new generation reconciles.

Default policy:

```yaml
web_listener_watchdog_enabled: true
web_listener_probe_interval_seconds: 5
web_listener_startup_grace_seconds: 30
web_listener_failure_threshold: 3
web_listener_probe_timeout_seconds: 2
```

The existing `scripts/start_web.cmd` loop is the recovery path after either process exits. It waits
while another process owns `0.0.0.0:8000`, and Python also refuses to start when its configured
listener is already live. The Uvicorn worker reserves its Windows socket before importing the app,
so a simultaneous losing generation exits before opening SQLite. Set
`SLURM_SCHEDULER_UVICORN_WORKER=1` only for a directly managed Uvicorn child or tests.

## Verification before deployment

1. Run the full test suite.
2. Start a disposable scheduler instance on another port.
3. Confirm parent and child PIDs are distinct.
4. Confirm killing only the child exits the parent and the external launcher starts one new generation.
5. Fault-inject a failed listener probe while the fake child remains alive; confirm thresholded
   parent exit, not an exit on a single transient failure.
6. Confirm pre-existing fake/live allocation rows and Slurm jobs are not cancelled.
7. Deploy only in a maintenance window; do not restart the active campaign scheduler during recovery.
