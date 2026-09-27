# Generic AEDT 1:N live pilot

`scripts/aedt_pool_live_pilot.py` runs a small Maxwell 3D electrostatic model,
independent of any product workload. Each worker creates its own AEDT project,
two conductor plates, a solution setup, a native solve, and a three-point
electric-field export. The voltage equals the worker index, so copied or
exchanged field data can be detected. The verifier checks project identity,
saved files, field rows and checksums, A/B field parity, Desktop and lease
identity, runtime, and incremental `electronics_desktop` checkouts.

This is a live qualification tool, not a pool enabler. It changes no pool
configuration. Run it on a staging scheduler with a **dedicated AEDT host
allocation** that belongs to this pilot. Record the scheduler commit, AEDT and
PyAEDT versions, Slurm account/allocation/job IDs, node, host session ID and
generation, and every submitted task ID. Never use the running production
service or another project's allocation. Start at N=2; repeat for each larger
N. The pilot's PyAEDT profile is the host's pinned profile (currently AEDT
2025.2 / PyAEDT 0.22.0); use the same environment for baseline and pooled
workers.

## Baseline: N independent Desktops

Create a fresh artifact root on the shared filesystem. Capture an idle
`lmutil lmstat -a` sample in `baseline_idle.txt` before starting the workers.
Launch N `worker --mode baseline` commands concurrently, each inside its own
Slurm step of the pilot's dedicated allocation. For example, worker 1:

```bash
python scripts/aedt_pool_live_pilot.py worker \
  --mode baseline --index 1 --artifact-root "$PILOT_ROOT" \
  --hold-before-solve-seconds 90
```

Use indices `1..N`; the other arguments are identical. During the 90-second
hold, capture repeated `lmutil lmstat -a` output in `baseline_lmstat.txt`.
Keep raw, timestamped output; the verifier uses the maximum checkout count.
The hold also makes it possible to observe all N Desktop processes alive
together. Verify each Slurm step exits zero and keep its stdout/stderr.

## Treatment: N projects on one host Desktop

Use a fresh host session with at least N free slots, on the pilot-owned
dedicated allocation. Before starting workers, create N real **pooled**
scheduler tasks and record their task IDs. Obtain the host's session ID,
generation, and exact session profile from the staging scheduler. As an
operator with the staging bootstrap credential, reserve those task IDs on that
session using `POST /api/aedt-pool/session-reservations`:

```json
{
  "reservation_key": "generic-pilot-unique-run-id",
  "session_id": 123,
  "session_generation": 1,
  "session_profile": "<exact host profile JSON>",
  "task_ids": [1001, 1002],
  "ttl_seconds": 1800
}
```

An unreserved `requested_session_id` is rejected by the scheduler. Capture
the reservation response. Capture `pooled_idle.txt` before workers start.
Launch all N pooled workers concurrently from the corresponding scheduler
tasks; each worker receives its own task ID and the same reserved session ID:

```bash
python scripts/aedt_pool_live_pilot.py worker \
  --mode pooled --index 1 --artifact-root "$PILOT_ROOT" \
  --scheduler-url "$STAGING_SCHEDULER_URL" \
  --token-file "$STAGING_BOOTSTRAP_TOKEN_FILE" \
  --task-id 1001 --session-id 123 --hold-before-solve-seconds 90
```

Capture timestamped `lmutil lmstat -a` samples in `pooled_lmstat.txt` during
the hold and solve. Keep a copy of the session/lease API status while all N
projects are active. Each pooled worker uses the client automation guard,
activates its lease before native analysis, waits for the native-pipeline
barrier, exports its own field, then waits for the host's project-close ACK.
It never closes the shared Desktop.

Run the verifier after all workers have exited:

```bash
python scripts/aedt_pool_live_pilot.py verify --projects 2 \
  --artifact-root "$PILOT_ROOT" \
  --baseline-idle-lmstat "$PILOT_ROOT/baseline_idle.txt" \
  --baseline-lmstat "$PILOT_ROOT/baseline_lmstat.txt" \
  --pooled-idle-lmstat "$PILOT_ROOT/pooled_idle.txt" \
  --pooled-lmstat "$PILOT_ROOT/pooled_lmstat.txt"
```

`verification.json` must say `passed: true`. The default field tolerance is
5% relative, with a 1.2 maximum wall-time ratio. The baseline must add N
Desktop checkouts above its idle count; the pooled run must add exactly one.
Keep the raw lmstat output because unrelated license activity can invalidate
an apparent delta. Also record solver-feature checkouts and verify they return
after the project close acknowledgements; the current automated verifier
checks the Desktop feature only.

## Failure isolation

Use a **second**, empty pilot session and a fresh artifact root. Reserve a new
N-task cohort. Give one pooled worker `--fault pre_solve`; run all others with
`--fault none`. The faulting worker reports a project-local error after model
creation, and the siblings should still solve and export fields. Run:

```bash
python scripts/aedt_pool_live_pilot.py verify-fault --projects 2 \
  --artifact-root "$FAULT_ROOT" --fault-index 1 --fault-kind pre_solve
```

Repeat with `--fault post_solve` and another fresh session/root. Keep API
session and lease snapshots proving the faulted Desktop was quarantined or
recycled according to its state, no new lease entered it, siblings finished,
and solver licenses returned. This verifier checks the reported fault and
surviving sibling outputs; it does **not** claim to verify those host-side
conditions, a real solve timeout, process crash, host restart, or temporary
control-plane loss. Execute those additional cases from
[`aedt_pool_runbook.md`](aedt_pool_runbook.md) before production activation.

Do not infer solver or Desktop license savings from PID counts alone. Keep
the raw license-server evidence and the dedicated allocation's job IDs. A
passing local unit test or loopback test does not replace this live evidence.

The model uses the documented PyAEDT
[`assign_voltage`](https://aedt.docs.pyansys.com/version/stable/API/_autosummary/ansys.aedt.core.maxwell.Maxwell3d.assign_voltage.html)
and
[`export_field_file`](https://aedt.docs.pyansys.com/version/stable/API/visualization/_autosummary/ansys.aedt.core.visualization.post.post_common_3d.PostProcessor3D.export_field_file.html)
APIs. Check the pinned 0.22.0 signatures and AEDT 2025.2 behavior on the
pilot host before relying on the resulting activation gate.
