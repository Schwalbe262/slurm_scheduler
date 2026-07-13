# AEDT pooled attach handoff — 2026-07-13

## Frozen live state

- Do not start another canary until the next operator explicitly resumes it.
- Live scheduler source/launcher revision: `bf273f4121356c0175de106375c14d1ee01619f6`
- Scheduler branch/worktree: `experiment/aedt-node-canary-260713` / `Y:\git\slurm_scheduler_node_canary_260713`
- MFT pooled canary revision: `78d95110505315b8852c286994697844e22c214a`
- PyAEDT library revision: `e6b9b9d20a832ff5c3f7ca97218737a0b8650781`
- Previously validated shared-session revisions/evidence:
  - normal + pre-solve abort: scheduler `620a3019da3577bbe2ea2e10aafebd8c26717df7`, MFT `fd3b02c2a4c3bd2ef566cc6e79ce1291f5576a18`
  - active-solve timeout isolation: scheduler `f3a65eeabb0e1d4d0f7694035147b9c27e673ee1`, MFT `fd3b02c2a4c3bd2ef566cc6e79ce1291f5576a18`
- Central AEDT pool remains deliberately non-operational: `enabled=false`, `adapter_ready=false`, `validation_passed=false`, `operational=false`.
- WEB UI limits are present: max AEDT sessions `2`, target live projects `4`, projects/session `2`. The main dashboard displays hard sessions/max and live projects/target.
- Fresh license snapshot after cleanup at `2026-07-13T01:58:00.830902+00:00`: `electronics_desktop=333`, `electronics3d_gui=335`, admission snapshot valid.

## Tests

- `tests/test_core.py`: `347 passed`, `27 subtests passed` on scheduler revision `bf273f4`.
- The latest fix changes task payload materialization from `python` to base-image `python3`; payload generation happens before task conda activation and node `n046` has no `python` alias.
- Earlier generic node-local admission/UI focused suite passed before this one-line bootstrap fix.

## Last production canary attempt

- Host task `30484`, bundle `aedt-canary-20260713-105400`, allocation `8358`, node `n116`, Slurm job `732393`.
- The host successfully opened exactly one AEDT:
  - AEDT PID `1052278`
  - gRPC port `45193`
  - node-local scheduler URL `http://127.0.0.1:36359`
  - discovery emitted at `2026-07-13T01:53:04+00:00`
- Client tasks `30485` and `30486` were submitted as real full MFT runs (`--fixed --thermal --set keep_project=0`), pooled backend, `fea_bursty`, 4 CPUs, 64 GiB metadata, exact revisions above.
- Neither client launched. Both failed before assignment with `same_node_as task 30484 is failed`.

## Newly reproduced blocker

Host `30484` used `same_node_as_task_id=30371` only to obtain node `n116` quickly. At `01:54:33`, anchor task `30371` completed. `fail_stale_same_node_tasks()` then marked the already-running host failed with:

`same_node_as task 30371 is completed`

This shows `same_node_as_task_id` is currently both a placement relation and a lifetime dependency. It must not be used to anchor a long-lived AEDT host to an ordinary simulation that can finish at any moment.

For the next canary, the smallest no-redesign route is to submit the host without `same_node_as_task_id` (`0`) at high priority and let it attach to a warm allocation normally. Once the host is running, submit the two clients with `same_node_as_task_id=<host task id>`. A durable scheduler-owned anchor whose lifetime strictly exceeds the host is another option. Do not reuse a production simulation as the host anchor.

Longer-term, if `same_node_as` is intended as placement-only after successful attach, separate placement affinity from parent-lifetime semantics and add a regression test for an active child surviving reference-task completion. That behavior is not implemented in this frozen handoff.

## Cleanup evidence

Scheduler cancellation did not remove every descendant. Direct inspection through allocation `732393` showed:

- AEDT `1052278` with `SLURM_SCHED_TASK_ID=30484`
- host Python `1051935` with `SLURM_SCHED_TASK_ID=30484`
- persistence process `1052342` with `SLURM_SCHED_TASK_ID=30484`

Cleanup was restricted to processes carrying task marker `30484`. AEDT received TERM (KILL fallback), then the marked host/persistence descendants received TERM/KILL. Coordination files were removed:

- `/tmp/aedt-canary-20260713-105400.discovery.json`
- `/tmp/aedt-canary-20260713-105400.evidence.json`
- `/tmp/aedt-canary-20260713-105400.rollback`

Final verification on `n116` returned no PIDs `1051935`, `1052018`, `1052278`, or `1052342`, no process with `SLURM_SCHED_TASK_ID=30484`, no coordination files, and no listener on port `45193`.

## Related task ledger

- `30480`: cancelled stale queued host before replacement.
- `30481`: pre-launch failure on `n046`; payload bootstrap used missing `python` alias. Cancelled; no AEDT checkout.
- `30482`: cancelled queued attempt on overloaded `n046`.
- `30483`: cancelled queued attempt before immediate resubmission on ready `n116`.
- `30484`: opened AEDT successfully, then failed when its short-lived anchor completed; fully cleaned as above.
- `30485`, `30486`: failed before launch because host `30484` was already failed.

