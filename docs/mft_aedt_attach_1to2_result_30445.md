# MFT shared AEDT 1:2 result: task 30445

Date: 2026-07-13

Status: **PASS for the isolated normal and pre-solve-abort cases only.** This
record does not approve or enable the production AEDT pool.

## Revisions and task

- scheduler: `620a3019da3577bbe2ea2e10aafebd8c26717df7`
- MFT: `fd3b02c2a4c3bd2ef566cc6e79ce1291f5576a18`
- pyaedt library: `e6b9b9d20a832ff5c3f7ca97218737a0b8650781`
- scheduler task: `30445`, Slurm job `732863`, node `n115`
- priority: `10000`
- production pool settings during the test: `enabled=false`,
  `adapter_ready=false`, `validation_passed=false`

The authoritative `pilot_evidence.json` reported top-level `passed=true`.
Both normal clients produced valid Matrix terminal rows with one solve and one
solution query. In the abort case A was stopped before solve, A alone received
a project-close ACK, and B independently produced a valid Matrix terminal
row. Each case ended with both required project-close ACKs, Desktop process
exit, and observed Desktop/Maxwell checkout return.

## What this proves

- one AEDT can host two distinct MFT projects concurrently;
- both concurrent projects can solve and query Matrix results;
- a project-local failure before solve can close only that project while the
  sibling remains attached and completes;
- client processes do not own or close the shared Desktop.

## What remains unproved

This task did not inject a timeout during an active solve. It also did not
prove normal cancel isolation, AEDT/session-host crash recovery, the
2-Desktop-versus-1-Desktop baseline runtime/parity contract, or the live pool
database and node adapter. Therefore no passing row was written to the live
`aedt_pool_validations` table and both readiness gates correctly remain false.

The next safe step is the timeout-only disposable pilot documented in
`mft_aedt_attach_1to2.md`. Directly killing a solver PID remains prohibited:
the prior experiment left a checkout/running state behind and did not produce
a valid sibling row or field solution.
