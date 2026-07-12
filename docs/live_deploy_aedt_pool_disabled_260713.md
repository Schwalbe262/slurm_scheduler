# Disabled AEDT pool live deployment — 2026-07-13

Deployment code revision:
`6e3c85c130561991c0d6fa773e74d30e51a1c4dd`

This deployment adds the tested exclusive 1:1 attach plumbing while keeping
the production backend disabled.  It does not activate a pooled MFT campaign,
does not permit 1:2, and does not launch any AEDT pool session.

## Composition and validation

The deploy revision was composed on top of the current live scheduler line
(`dbd23ae`, immutable project refs) and includes the task-capacity project
admission fix.  Conflict resolution preserved the live scheduler's:

- license admission and fail-closed unknown-project policy;
- allocation CPU-utilization admission;
- node fill diagnostics;
- `_task_assignment_lock` allocation-close serialization;
- strict one-field project-cap route and its existing API contract.

The AEDT pool additions remained disabled-by-default.  A duplicate project-cap
route found during composed-branch testing was removed and a uniqueness
regression was added.

Final tests before deployment:

- scheduler: `379 passed`, `27 subtests passed`;
- compileall: passed;
- diff check: passed;
- targeted pool/project/OpenAPI group: `47 passed`, `14 subtests passed`.

The attach client, pool service/API, session host, and real 1:1 pilot harness
are byte-identical to revision
`099719ba75f9dd61f96d86abd81179dc7de5f6e5`, which passed real pilot task
`30089`.  See `mft_aedt_attach_1to1_result_30089.md`.

## Source deployment

- C live checkout: exact detached `6e3c85c...`
- Y scheduler checkout: exact detached `6e3c85c...`
- prior dirty Y state preserved as stash
  `pre-livecompose-y-20260713-0825`
- prior C live capacity patch preserved on local branch
  `deploy/pre-aedt-pool-live-20260713`

The C live watchdog was restarted once.  The resulting process tree used a
stable supervisor and Uvicorn worker; the active `0.0.0.0:8000` listener was
PID `180804`.  Local module import paths resolved to the exact C live checkout.

## Pre/post invariants

Pre-deploy snapshot at 08:20:30 KST:

- MFT logical active: 300 (`queued=93`, `running=207`)
- project cap: 300
- live allocations returned by API: 25
- scheduler health/thread: true/true, stalled=false
- license admission: enabled, unknown FEA project policy=`block`
- AEDT pool API: not yet present
- collector: alive; campaign controller intentionally paused at a mutation-lock
  safe point by the recovery workflow

Post-deploy/final snapshot at 08:27:22 KST:

- MFT logical active: 300 (`queued=69`, `running=231`)
- project cap: 300
- scheduler health/thread: true/true, stalled=false
- license admission: enabled, unknown FEA project policy=`block`
- configured MFT capacity probe: admitted (capacity-dependent pending state)
- empty-project FEA capacity probe: blocked by license admission
- pool `enabled=false`
- `adapter_ready=false`
- `validation_passed=false`
- `operational=false`
- pool sessions=0, leases=0, `start_needed=0`
- controller and collector: alive
- deployment-window cancel events: 0

The active status distribution changed through normal queued-to-running
attachment.  Three pre-existing 1K101 pilots (`30090`-`30092`) also reached
their own exit-1 terminal state and were replaced by the separate recovery
workflow; no deployment cancellation caused those terminals.  Allocation
closures in the window were recorded as pre-restart QOS-shape rejection or
normal "demand allocation no longer needed", not task cancellation.

## Activation decision

Production remains standalone.  Enabling the live pool still requires the
full 1:2 output/isolation/fault gate; the passing exclusive 1:1 lifecycle
pilot alone is not sufficient.
