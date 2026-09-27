# AEDT shared-session validation and rollback

The AEDT pool is an optional Slurm resource-management feature. One AEDT Desktop
may host several independent projects only after that exact project count has
passed validation. The default remains disabled. Validation jobs must use a
dedicated allocation and must never cancel work submitted by another project.

## Preflight

1. Record the scheduler commit, adapter commit, AEDT and PyAEDT versions, Slurm
   account, allocation, node, and the IDs of the validation jobs that **you**
   submit. Keep the production service and database unchanged during the pilot.
2. Use independent project workspaces and output directories. Each project must
   have a unique name and lease; a sibling may never reuse its result or lock.
3. Capture a baseline with N Desktops running N representative projects. Capture
   elapsed time, completed outputs, data rows, field solutions, and Desktop and
   solver license checkouts.
4. Run the same N projects on one Desktop with N project leases. Observe the
   Desktop PID, session generation, bound project names, lease acknowledgements,
   and license checkouts throughout the run.

Start with N=2. Increasing `projects_per_aedt` requires a new passing evidence
record for the **new** N; a prior 1:2 result does not authorize 1:3 or 1:4.
The configured maximum is eight projects per Desktop, not a validated operating
target. Do not increase production density beyond the largest live-tested N.

## Isolation and failure cases

For every tested N, verify normal completion for all projects, then repeat with
one project's pre-solve abort, solve timeout, cancellation, and process crash.
The faulted lease must be quarantined or released according to its state while
the other projects finish and retain their outputs. A quarantined Desktop must
not admit another project; recycle it only after all surviving siblings finish
and the host confirms AEDT and solver processes have exited. Test host restart
and temporary control-plane loss before considering production use.

Record a per-project result containing its exact project name, lease ID,
terminal output, data rows, and field-solution check. Reject missing, repeated,
or swapped project results. The pooled run must finish within the configured
runtime ratio gate and reduce Desktop checkout from N to one. A successful
loopback test is useful regression evidence but is not live license evidence.

## Activation

Submit the measured evidence through the pool validation API and verify the
response is `passed` for the configured N. Check that the host adapter is ready,
the control-plane tokens and node-local workspace are configured, and the
session-host startup is healthy. Only then enable the pool. Review live session
counts, lease counts, node capacity, scheduler events, and license usage after
each density increase. Keep standalone execution available as the rollback path.

## Rollback

Disable new pooled admission first. Let healthy siblings finish, then drain
only the dedicated sessions and allocations created for this validation or
pool generation. Confirm host close acknowledgements and zero remaining AEDT
and solver processes before releasing license accounting. A session whose
process exit cannot be proved remains unhealthy and counted. Return new tasks
to the standalone backend. Never cancel unrelated Slurm jobs or allocations.
