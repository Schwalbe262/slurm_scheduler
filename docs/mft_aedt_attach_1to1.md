# MFT AEDT Attach 1:1 Pilot

Status on 2026-07-13: experimental, production-disabled, and limited to one
MFT project per AEDT Desktop.

The earlier one-Desktop/two-project experiment (`Slurm 732549`) did not
produce a valid terminal artifact for the healthy sibling.  It therefore did
not satisfy the isolation gate and does not authorize a 1:2 pool or the
250-AEDT/500-project target.

## What this revision changes

- A project lease can request `exclusive_session=true`.
- An exclusive lease is placed only on an empty Desktop and prevents any
  sibling lease from entering that Desktop.
- Exclusive demand counts one required Desktop per live project.
- The MFT runner has an explicit pooled backend, but it refuses to start unless
  `MFT_AEDT_EXCLUSIVE_1TO1=1` is also set.
- The session host remains the only owner allowed to close the project or
  Desktop.  The MFT process waits for the host's project-close ACK.
- Standalone MFT behavior remains the default.  No production pool setting is
  changed by this revision.

This is a plumbing and lifecycle gate only.  A passing 1:1 pilot is not a
performance claim and is not evidence that sibling isolation works at 1:2.

## MFT opt-in contract

The disposable runner must receive all of the following variables:

```bash
export MFT_AEDT_BACKEND=pooled
export MFT_AEDT_EXCLUSIVE_1TO1=1
export MFT_AEDT_SCHEDULER_URL=http://127.0.0.1:<pilot-port>
export MFT_SLURM_SCHEDULER_ROOT=/path/to/exact-scheduler-checkout
export MFT_PYAEDT_LIBRARY_ROOT=/path/to/exact-library-checkout
```

`--hold` is rejected in pooled mode.  The runner must not call
`release_desktop`, kill Desktop descendants, or delete project files before
the host reports the lease as `released`.

## Disposable real pilot

Submit exactly one Git task with:

- `project=MFT_1MW_2026v1` so license admission has a known profile;
- `priority=10000` so the pilot is not hidden behind ordinary refill work;
- `scheduling_profile=fea_bursty`;
- exact full 40-character Git SHAs for scheduler, MFT, and pyaedt_library;
- no cancellation of existing production tasks.

The task entrypoint is `scripts/aedt_pool_1to1_pilot.py`.  It starts a
loopback-only disposable control plane and one real `AedtSessionHost`, then
runs one MFT matrix case through the real attach client.  It never enables the
live AEDT pool.

Example entrypoint arguments:

```text
--mft-repo-url https://github.com/Schwalbe262/MFT_1MW_2026.git
--mft-revision <40-char-MFT-SHA>
--library-repo-url https://github.com/Schwalbe262/pyaedt_library.git
--library-revision <40-char-library-SHA>
--output-dir pilot_evidence_1to1
--lmutil /opt/ohpc/pub/Electronics/v242/Linux64/licensingclient/linx64/lmutil
--license-server 1055@172.16.10.81
```

## Pass gate

`pilot_evidence.json` must report every item below:

- runner exit code 0 and exactly one `RESULT_JSON`;
- `result_valid_em=1` and `matrix_solve_attempts=1`;
- `aedt_backend=pooled`, `aedt_exclusive_session=1`, and the expected lease ID;
- attach handshake and bound MFT project name;
- lease state `released` after a successful project-close ACK;
- session state `closed` after Desktop-close ACK;
- the exact Desktop PID no longer exists;
- the exact Desktop PID's `electronics_desktop` checkout was observed and
  subsequently returned;
- the disposable AEDT project workspace was removed;
- scheduler, MFT, and library revisions in the evidence match the submitted
  full SHAs.

Any missing terminal artifact, ACK, PID exit, license return, or cleanup item
is a failed gate.  On failure the disposable session is drained, the evidence
and logs are retained, and production remains on standalone AEDT.

## Explicit non-goals

- Do not set live `enabled=true` or `adapter_ready=true` from this pilot.
- Do not increase `projects_per_aedt` above one.
- Do not launch 250 AEDT or 500 project workers.
- Do not treat a 1:1 pass as remediation of the failed 1:2 isolation test.
